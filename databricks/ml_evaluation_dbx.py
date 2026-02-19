# Databricks notebook source
# MAGIC %pip install scikit-learn snowflake-connector-python

# COMMAND ----------

import numpy as np
import pandas as pd
import snowflake.connector
from datetime import datetime, UTC, date
from sklearn.metrics import roc_auc_score, average_precision_score, precision_score
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def _secret_or_raise(scope: str, key: str):
    try:
        return dbutils.secrets.get(scope, key)
    except Exception as exc:
        raise RuntimeError(
            f"Missing Databricks secret: scope='{scope}', key='{key}'. "
            f"Create it with: databricks secrets put-secret {scope} {key} --string-value <value>"
        ) from exc


def _sf_password(scope: str) -> str:
    try:
        return _secret_or_raise(scope, "SNOWFLAKE_PASSWORD")
    except RuntimeError:
        return _secret_or_raise(scope, "snowflake_password")


# Runtime params
dbutils.widgets.text("RAW_PATH", "/Volumes/main/default/retail_raw")
dbutils.widgets.text("SCOPE", "retail-secrets")
dbutils.widgets.text("CHURN_WINDOW_DAYS", "90")
dbutils.widgets.text("TOP_K_FRACTION", "0.10")

RAW_PATH = dbutils.widgets.get("RAW_PATH")
SCOPE = dbutils.widgets.get("SCOPE")
CHURN_WINDOW_DAYS = int(dbutils.widgets.get("CHURN_WINDOW_DAYS"))
TOP_K_FRACTION = float(dbutils.widgets.get("TOP_K_FRACTION"))


def sf_conn():
    return snowflake.connector.connect(
        account=_secret_or_raise(SCOPE, "SNOWFLAKE_ACCOUNT"),
        user=_secret_or_raise(SCOPE, "SNOWFLAKE_USER"),
        password=_sf_password(SCOPE),
        role=_secret_or_raise(SCOPE, "SNOWFLAKE_ROLE"),
        warehouse=_secret_or_raise(SCOPE, "SNOWFLAKE_WAREHOUSE"),
        database=_secret_or_raise(SCOPE, "SNOWFLAKE_DATABASE"),
    )


def ensure_metrics_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS RETAIL_DB.ML")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.MODEL_METRICS (
              MODEL_NAME STRING,
              MODEL_VERSION STRING,
              RUN_TS TIMESTAMP_NTZ,
              TRAIN_CUTOFF DATE,
              TEST_START DATE,
              TEST_END DATE,
              METRIC_NAME STRING,
              METRIC_VALUE FLOAT,
              NOTES STRING
            )
            """
        )
        conn.commit()
    finally:
        cur.close()


def _sf_value(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, np.datetime64):
        return pd.to_datetime(v).to_pydatetime().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return v


def append_df(conn, df: pd.DataFrame, table_full_name: str):
    cur = conn.cursor()
    try:
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        records = [tuple(_sf_value(v) for v in row) for row in df.itertuples(index=False, name=None)]
        cur.executemany(
            f"INSERT INTO {table_full_name} ({cols}) VALUES ({placeholders})",
            records,
        )
        conn.commit()
    finally:
        cur.close()


def build_orders_pd(spark: SparkSession) -> pd.DataFrame:
    orders = spark.read.csv(f"{RAW_PATH}/olist_orders_dataset.csv", header=True, inferSchema=True)
    customers = spark.read.csv(f"{RAW_PATH}/olist_customers_dataset.csv", header=True, inferSchema=True)

    orders = (
        orders.filter(col("order_status") == "delivered")
        .withColumn("order_date", to_date(col("order_purchase_timestamp")))
        .select("order_id", "customer_id", "order_date")
    )

    customers = customers.select("customer_id", "customer_unique_id")

    joined = orders.join(customers, "customer_id").select("order_id", "customer_unique_id", "order_date")
    df = joined.toPandas().rename(columns={"customer_unique_id": "customer_id"})
    df = df.dropna(subset=["customer_id", "order_date"])
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    return df


def build_eval_dataset(orders_df: pd.DataFrame, churn_window_days: int) -> tuple[pd.DataFrame, date, date, date]:
    all_dates = sorted(orders_df["order_date"].unique())
    cutoff = all_dates[int(len(all_dates) * 0.7)]
    test_start = cutoff
    test_end = max(all_dates)

    hist = orders_df[orders_df["order_date"] <= cutoff].copy()
    fut = orders_df[orders_df["order_date"] > cutoff].copy()

    agg = hist.groupby("customer_id").agg(
        last_order_date=("order_date", "max"),
        frequency=("order_id", "nunique"),
    ).reset_index()

    agg["recency_days"] = (pd.to_datetime(cutoff) - pd.to_datetime(agg["last_order_date"])) .dt.days

    # Future purchase within churn window => not churn (0), otherwise churn (1)
    fut_min = fut.groupby("customer_id").agg(next_order_date=("order_date", "min")).reset_index()
    ds = agg.merge(fut_min, on="customer_id", how="left")

    ds["days_to_next"] = (pd.to_datetime(ds["next_order_date"]) - pd.to_datetime(cutoff)).dt.days
    ds["is_churn"] = ((ds["days_to_next"].isna()) | (ds["days_to_next"] > churn_window_days)).astype(int)

    # Baseline score from recency + inverse frequency
    rec = ds["recency_days"].fillna(0)
    freq = ds["frequency"].fillna(0)

    def norm(s: pd.Series) -> pd.Series:
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series([0.0] * len(s), index=s.index)
        return (s - mn) / (mx - mn)

    rec_n = norm(rec)
    freq_n = norm(freq)
    ds["score"] = (0.75 * rec_n + 0.25 * (1 - freq_n)).clip(0, 1)

    return ds, cutoff, test_start, test_end


def evaluate(ds: pd.DataFrame, top_k_fraction: float) -> dict:
    y_true = ds["is_churn"].astype(int).values
    y_score = ds["score"].astype(float).values

    auc = float(roc_auc_score(y_true, y_score))
    pr_auc = float(average_precision_score(y_true, y_score))

    k = max(1, int(len(ds) * top_k_fraction))
    top_idx = np.argsort(-y_score)[:k]
    y_pred_topk = np.zeros(len(ds), dtype=int)
    y_pred_topk[top_idx] = 1
    p_at_k = float(precision_score(y_true, y_pred_topk, zero_division=0))

    churn_rate = float(np.mean(y_true))
    topk_churn_rate = float(np.mean(y_true[top_idx])) if len(top_idx) else 0.0
    lift_at_k = float((topk_churn_rate / churn_rate) if churn_rate > 0 else 0.0)

    return {
        "auc": auc,
        "pr_auc": pr_auc,
        "precision_at_k": p_at_k,
        "lift_at_k": lift_at_k,
        "sample_size": float(len(ds)),
        "churn_rate": churn_rate,
    }


def main():
    spark = SparkSession.builder.appName("retail-ml-evaluation").getOrCreate()

    orders_df = build_orders_pd(spark)
    ds, cutoff, test_start, test_end = build_eval_dataset(orders_df, CHURN_WINDOW_DAYS)
    metrics = evaluate(ds, TOP_K_FRACTION)

    run_ts = datetime.now(UTC)
    rows = []
    for k, v in metrics.items():
        rows.append(
            {
                "MODEL_NAME": "churn_baseline",
                "MODEL_VERSION": "v1",
                "RUN_TS": run_ts,
                "TRAIN_CUTOFF": cutoff,
                "TEST_START": test_start,
                "TEST_END": test_end,
                "METRIC_NAME": k,
                "METRIC_VALUE": float(v),
                "NOTES": f"window_days={CHURN_WINDOW_DAYS}, top_k_fraction={TOP_K_FRACTION}",
            }
        )

    metrics_df = pd.DataFrame(rows)

    conn = sf_conn()
    try:
        ensure_metrics_table(conn)
        append_df(conn, metrics_df, "RETAIL_DB.ML.MODEL_METRICS")
    finally:
        conn.close()

    print("ML evaluation completed")
    for k, v in metrics.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
