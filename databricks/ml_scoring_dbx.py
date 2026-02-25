# Databricks notebook source
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

# Databricks ML scoring notebook/script
# Produces:
#   RETAIL_DB.ML.CUSTOMER_CHURN_SCORE
#   RETAIL_DB.ML.CUSTOMER_CHURN_SCORE_LEGACY
#   RETAIL_DB.ML.DAILY_REVENUE_ANOMALY
import pandas as pd
import snowflake.connector
import numpy as np
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    date_format,
    countDistinct,
    sum as Fsum,
    max as Fmax,
    datediff,
    lit,
)


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
dbutils.widgets.text("MODEL_VERSION", "scoring_rfm_v1")

RAW_PATH = dbutils.widgets.get("RAW_PATH")
SCOPE = dbutils.widgets.get("SCOPE")
MODEL_VERSION = dbutils.widgets.get("MODEL_VERSION")


def sf_conn():
    return snowflake.connector.connect(
        account=_secret_or_raise(SCOPE, "SNOWFLAKE_ACCOUNT"),
        user=_secret_or_raise(SCOPE, "SNOWFLAKE_USER"),
        password=_sf_password(SCOPE),
        role=_secret_or_raise(SCOPE, "SNOWFLAKE_ROLE"),
        warehouse=_secret_or_raise(SCOPE, "SNOWFLAKE_WAREHOUSE"),
        database=_secret_or_raise(SCOPE, "SNOWFLAKE_DATABASE"),
    )


def ensure_ml_tables(conn):
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS RETAIL_DB.ML")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.CUSTOMER_CHURN_SCORE (
              CUSTOMER_ID STRING,
              SNAPSHOT_DATE DATE,
              CHURN_SCORE NUMBER(9,6),
              CHURN_LABEL STRING,
              MODEL_VERSION STRING,
              SCORED_AT TIMESTAMP_NTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.CUSTOMER_CHURN_SCORE_LEGACY (
              CUSTOMER_ID STRING,
              RECENCY_DAYS NUMBER,
              FREQUENCY NUMBER,
              MONETARY NUMBER(18,2),
              CHURN_SCORE NUMBER(9,6),
              CHURN_LABEL STRING,
              SCORED_AT TIMESTAMP_NTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.DAILY_REVENUE_ANOMALY (
              ORDER_DATE DATE,
              REVENUE NUMBER(18,2),
              ORDERS NUMBER,
              Z_SCORE NUMBER(18,6),
              IS_ANOMALY BOOLEAN,
              SCORED_AT TIMESTAMP_NTZ
            )
            """
        )
        # Backward-compatible column evolution if table was created by older versions.
        cur.execute("ALTER TABLE RETAIL_DB.ML.CUSTOMER_CHURN_SCORE ADD COLUMN IF NOT EXISTS SNAPSHOT_DATE DATE")
        cur.execute("ALTER TABLE RETAIL_DB.ML.CUSTOMER_CHURN_SCORE ADD COLUMN IF NOT EXISTS MODEL_VERSION STRING")
        cur.execute("ALTER TABLE RETAIL_DB.ML.CUSTOMER_CHURN_SCORE ADD COLUMN IF NOT EXISTS SCORED_AT TIMESTAMP_NTZ")
        conn.commit()
    finally:
        cur.close()


def write_df(conn, df: pd.DataFrame, table_full_name: str):
    def _sf_value(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, float) and pd.isna(v):
            return None
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

    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {table_full_name}")
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


def build_txn_base(spark: SparkSession):
    orders = spark.read.csv(f"{RAW_PATH}/olist_orders_dataset.csv", header=True, inferSchema=True)
    items = spark.read.csv(f"{RAW_PATH}/olist_order_items_dataset.csv", header=True, inferSchema=True)
    customers = spark.read.csv(f"{RAW_PATH}/olist_customers_dataset.csv", header=True, inferSchema=True)

    orders = (
        orders.filter(col("order_status") == "delivered")
        .withColumn("order_date", to_date(col("order_purchase_timestamp")))
        .select("order_id", "customer_id", "order_date")
    )

    items = items.withColumn("revenue", col("price") + col("freight_value")).select(
        "order_id", "revenue"
    )

    customers = customers.select("customer_id", "customer_unique_id")

    tx = items.join(orders, "order_id").join(customers, "customer_id")
    return tx


def score_churn(tx) -> tuple[pd.DataFrame, pd.DataFrame]:
    last_date = tx.agg(Fmax("order_date").alias("maxd")).collect()[0]["maxd"]

    rfm = (
        tx.groupBy("customer_unique_id")
        .agg(
            Fmax("order_date").alias("LAST_ORDER_DATE"),
            countDistinct("order_id").alias("FREQUENCY"),
            Fsum("revenue").alias("MONETARY"),
        )
        .withColumn("RECENCY_DAYS", datediff(lit(last_date), col("LAST_ORDER_DATE")))
        .select(
            col("customer_unique_id").alias("CUSTOMER_ID"),
            "RECENCY_DAYS",
            "FREQUENCY",
            "MONETARY",
        )
    )

    df = rfm.toPandas()

    # Simple deterministic churn scoring without external ML libs.
    rec = df["RECENCY_DAYS"].fillna(0)
    freq = df["FREQUENCY"].fillna(0)
    mon = df["MONETARY"].fillna(0)

    def safe_norm(s: pd.Series) -> pd.Series:
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series([0.0] * len(s), index=s.index)
        return (s - mn) / (mx - mn)

    rec_n = safe_norm(rec)
    freq_n = safe_norm(freq)
    mon_n = safe_norm(mon)

    # Higher recency increases churn risk; higher freq/monetary reduce risk.
    score = 0.6 * rec_n + 0.25 * (1 - freq_n) + 0.15 * (1 - mon_n)

    out = df.copy()
    out["CHURN_SCORE"] = score.clip(0, 1)
    out["CHURN_LABEL"] = out["CHURN_SCORE"].apply(
        lambda x: "HIGH" if x >= 0.67 else ("MEDIUM" if x >= 0.34 else "LOW")
    )
    out["SCORED_AT"] = datetime.utcnow()
    out["SNAPSHOT_DATE"] = pd.to_datetime(last_date).date() if last_date is not None else None
    out["MODEL_VERSION"] = MODEL_VERSION

    legacy_cols = ["CUSTOMER_ID", "RECENCY_DAYS", "FREQUENCY", "MONETARY", "CHURN_SCORE", "CHURN_LABEL", "SCORED_AT"]
    current_cols = ["CUSTOMER_ID", "SNAPSHOT_DATE", "CHURN_SCORE", "CHURN_LABEL", "MODEL_VERSION", "SCORED_AT"]
    return out[legacy_cols], out[current_cols]


def score_daily_anomaly(tx) -> pd.DataFrame:
    daily = (
        tx.groupBy("order_date")
        .agg(Fsum("revenue").alias("REVENUE"), countDistinct("order_id").alias("ORDERS"))
        .withColumn("YEAR_MONTH", date_format(col("order_date"), "yyyy-MM"))
        .orderBy("order_date")
    )

    df = daily.toPandas().rename(columns={"order_date": "ORDER_DATE"})

    rev = df["REVENUE"].fillna(0)
    std = rev.std(ddof=0)
    if std == 0 or pd.isna(std):
        df["Z_SCORE"] = 0.0
        df["IS_ANOMALY"] = False
    else:
        mean = rev.mean()
        df["Z_SCORE"] = (rev - mean) / std
        df["IS_ANOMALY"] = df["Z_SCORE"].abs() >= 3.0

    df["SCORED_AT"] = datetime.utcnow()
    return df[["ORDER_DATE", "REVENUE", "ORDERS", "Z_SCORE", "IS_ANOMALY", "SCORED_AT"]]


def main():
    spark = SparkSession.builder.appName("retail-ml-scoring").getOrCreate()

    tx = build_txn_base(spark)
    churn_legacy_df, churn_current_df = score_churn(tx)
    anomaly_df = score_daily_anomaly(tx)

    conn = sf_conn()
    try:
        ensure_ml_tables(conn)
        write_df(conn, churn_current_df, "RETAIL_DB.ML.CUSTOMER_CHURN_SCORE")
        write_df(conn, churn_legacy_df, "RETAIL_DB.ML.CUSTOMER_CHURN_SCORE_LEGACY")
        write_df(conn, anomaly_df, "RETAIL_DB.ML.DAILY_REVENUE_ANOMALY")
    finally:
        conn.close()

    print(
        f"ML scoring completed. churn_rows={len(churn_current_df)}, anomaly_rows={len(anomaly_df)}"
    )


if __name__ == "__main__":
    main()
