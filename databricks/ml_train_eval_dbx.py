# Databricks notebook source
# MAGIC %pip install scikit-learn snowflake-connector-python

# COMMAND ----------

import numpy as np
import pandas as pd
import snowflake.connector
from datetime import datetime, UTC, date
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, average_precision_score
from pyspark.sql import SparkSession


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
dbutils.widgets.text("CHURN_WINDOW_DAYS", "30")
dbutils.widgets.text("TOP_K_FRACTION", "0.10")
dbutils.widgets.text("ACTIVE_LOOKBACK_DAYS", "120")
dbutils.widgets.text("MODEL_VERSION", "repeat30_v5")
dbutils.widgets.text("MIN_ORDERS_LIFETIME", "2")
dbutils.widgets.text("HOLDOUT_SNAPSHOTS", "5")
dbutils.widgets.text("MIN_TRAIN_SNAPSHOTS", "5")

RAW_PATH = dbutils.widgets.get("RAW_PATH")
SCOPE = dbutils.widgets.get("SCOPE")
CHURN_WINDOW_DAYS = int(dbutils.widgets.get("CHURN_WINDOW_DAYS"))
TOP_K_FRACTION = float(dbutils.widgets.get("TOP_K_FRACTION"))
ACTIVE_LOOKBACK_DAYS = int(dbutils.widgets.get("ACTIVE_LOOKBACK_DAYS"))
MODEL_VERSION = dbutils.widgets.get("MODEL_VERSION")
MIN_ORDERS_LIFETIME = int(dbutils.widgets.get("MIN_ORDERS_LIFETIME"))
HOLDOUT_SNAPSHOTS = int(dbutils.widgets.get("HOLDOUT_SNAPSHOTS"))
MIN_TRAIN_SNAPSHOTS = int(dbutils.widgets.get("MIN_TRAIN_SNAPSHOTS"))

FEATURE_COLS = [
    "recency_days",
    "frequency_30d",
    "frequency_90d",
    "monetary_30d",
    "monetary_90d",
    "orders_lifetime",
    "monetary_lifetime",
    "avg_order_value_lifetime",
    "tenure_days",
]


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
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.MODEL_METRICS (
              MODEL_NAME STRING,
              MODEL_VERSION STRING,
              RUN_TS TIMESTAMP_NTZ,
              SPLIT_NAME STRING,
              SNAPSHOT_START DATE,
              SNAPSHOT_END DATE,
              METRIC_NAME STRING,
              METRIC_VALUE FLOAT,
              NOTES STRING
            )
            """
        )
        cur.execute("ALTER TABLE RETAIL_DB.ML.MODEL_METRICS ADD COLUMN IF NOT EXISTS SPLIT_NAME STRING")
        cur.execute("ALTER TABLE RETAIL_DB.ML.MODEL_METRICS ADD COLUMN IF NOT EXISTS SNAPSHOT_START DATE")
        cur.execute("ALTER TABLE RETAIL_DB.ML.MODEL_METRICS ADD COLUMN IF NOT EXISTS SNAPSHOT_END DATE")
        cur.execute("ALTER TABLE RETAIL_DB.ML.MODEL_METRICS ADD COLUMN IF NOT EXISTS NOTES STRING")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.MODEL_DECILE_DIAGNOSTICS (
              MODEL_NAME STRING,
              MODEL_VERSION STRING,
              RUN_TS TIMESTAMP_NTZ,
              SPLIT_NAME STRING,
              SNAPSHOT_START DATE,
              SNAPSHOT_END DATE,
              DECILE NUMBER,
              MIN_SCORE FLOAT,
              MAX_SCORE FLOAT,
              AVG_SCORE FLOAT,
              POSITIVE_RATE FLOAT,
              SAMPLE_SIZE NUMBER,
              NOTES STRING
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.MODEL_REGISTRY (
              MODEL_NAME STRING,
              MODEL_VERSION STRING,
              RUN_TS TIMESTAMP_NTZ,
              STATUS STRING,
              SELECTION_REASON STRING,
              HOLDOUT_AUC FLOAT,
              HOLDOUT_PR_AUC FLOAT,
              HOLDOUT_LIFT_AT_K FLOAT,
              HOLDOUT_PRECISION_AT_K FLOAT,
              HOLDOUT_POSITIVE_RATE FLOAT,
              HOLDOUT_SAMPLE_SIZE NUMBER,
              NOTES STRING
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.CUSTOMER_CHURN_SCORE (
              CUSTOMER_ID STRING,
              SNAPSHOT_DATE DATE,
              CHURN_SCORE FLOAT,
              CHURN_LABEL STRING,
              MODEL_VERSION STRING,
              SCORED_AT TIMESTAMP_NTZ
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
    if df.empty:
        return
    cur = conn.cursor()
    try:
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        records = [tuple(_sf_value(v) for v in row) for row in df.itertuples(index=False, name=None)]
        cur.executemany(f"INSERT INTO {table_full_name} ({cols}) VALUES ({placeholders})", records)
        conn.commit()
    finally:
        cur.close()


def replace_df(conn, df: pd.DataFrame, table_full_name: str):
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {table_full_name}")
        conn.commit()
    finally:
        cur.close()
    append_df(conn, df, table_full_name)


def build_customer_orders_pd_from_silver(conn) -> pd.DataFrame:
    query = """
        SELECT
          c.CUSTOMER_UNIQUE_ID AS CUSTOMER_ID,
          o.ORDER_ID,
          TO_DATE(o.ORDER_PURCHASE_TS) AS ORDER_DATE,
          SUM(COALESCE(i.REVENUE, 0)) AS ORDER_REVENUE
        FROM RETAIL_DB.SILVER.ORDERS o
        JOIN RETAIL_DB.SILVER.CUSTOMERS c ON c.CUSTOMER_ID = o.CUSTOMER_ID
        LEFT JOIN RETAIL_DB.SILVER.ORDER_ITEMS i ON i.ORDER_ID = o.ORDER_ID
        WHERE LOWER(o.ORDER_STATUS) = 'delivered'
          AND o.ORDER_PURCHASE_TS IS NOT NULL
          AND c.CUSTOMER_UNIQUE_ID IS NOT NULL
        GROUP BY 1,2,3
    """
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        cur.close()
    df = pd.DataFrame(rows, columns=cols)
    df = df.dropna(subset=["CUSTOMER_ID", "ORDER_DATE"]).copy()
    df["order_date"] = pd.to_datetime(df["ORDER_DATE"])
    df["customer_id"] = df["CUSTOMER_ID"].astype(str)
    df["order_id"] = df["ORDER_ID"].astype(str)
    df["order_revenue"] = pd.to_numeric(df["ORDER_REVENUE"], errors="coerce").fillna(0.0)
    return df[["customer_id", "order_id", "order_date", "order_revenue"]]


def build_snapshot_dataset(tx: pd.DataFrame, horizon_days: int, active_lookback_days: int, min_orders_lifetime: int) -> pd.DataFrame:
    tx = tx.copy()
    tx["order_date"] = pd.to_datetime(tx["order_date"])
    min_d = tx["order_date"].min()
    max_d = tx["order_date"].max()

    snapshot_start = min_d + pd.Timedelta(days=120)
    snapshot_end = max_d - pd.Timedelta(days=horizon_days)
    if snapshot_start >= snapshot_end:
        raise RuntimeError("Not enough date span for snapshot-based train/test setup")

    snapshots = pd.date_range(snapshot_start, snapshot_end, freq="MS")
    if len(snapshots) < 6:
        snapshots = pd.date_range(snapshot_start, snapshot_end, freq="14D")

    rows = []
    tx_sorted = tx.sort_values("order_date")

    for snap in snapshots:
        fut_end = snap + pd.Timedelta(days=horizon_days)
        hist = tx_sorted[tx_sorted["order_date"] <= snap]
        if hist.empty:
            continue
        fut = tx_sorted[(tx_sorted["order_date"] > snap) & (tx_sorted["order_date"] <= fut_end)]

        lifetime = hist.groupby("customer_id").agg(
            last_order_date=("order_date", "max"),
            first_order_date=("order_date", "min"),
            orders_lifetime=("order_id", "nunique"),
            monetary_lifetime=("order_revenue", "sum"),
        ).reset_index()

        d30 = snap - pd.Timedelta(days=30)
        d90 = snap - pd.Timedelta(days=90)
        win30 = hist[hist["order_date"] > d30].groupby("customer_id").agg(
            frequency_30d=("order_id", "nunique"),
            monetary_30d=("order_revenue", "sum"),
        ).reset_index()
        win90 = hist[hist["order_date"] > d90].groupby("customer_id").agg(
            frequency_90d=("order_id", "nunique"),
            monetary_90d=("order_revenue", "sum"),
        ).reset_index()

        base = lifetime.merge(win30, on="customer_id", how="left").merge(win90, on="customer_id", how="left")
        base["recency_days"] = (pd.to_datetime(snap) - pd.to_datetime(base["last_order_date"])).dt.days
        base["tenure_days"] = (pd.to_datetime(snap) - pd.to_datetime(base["first_order_date"])).dt.days
        base["avg_order_value_lifetime"] = np.where(
            base["orders_lifetime"] > 0,
            base["monetary_lifetime"] / base["orders_lifetime"],
            0,
        )

        base = base[(base["recency_days"] <= active_lookback_days) & (base["orders_lifetime"] >= min_orders_lifetime)].copy()
        if base.empty:
            continue

        active_future = set(fut["customer_id"].unique())
        base["target_1"] = base["customer_id"].isin(active_future).astype(int)
        base["snapshot_date"] = snap.date()
        rows.append(base)

    ds = pd.concat(rows, ignore_index=True)
    for c in ["frequency_30d", "monetary_30d", "frequency_90d", "monetary_90d"]:
        ds[c] = ds[c].fillna(0)
    return ds


def eval_metrics(y_true: np.ndarray, y_score: np.ndarray, top_k_fraction: float) -> dict:
    base_rate = float(np.mean(y_true))
    if len(np.unique(y_true)) < 2:
        auc = np.nan
        pr_auc = np.nan
    else:
        auc = float(roc_auc_score(y_true, y_score))
        pr_auc = float(average_precision_score(y_true, y_score))

    k = max(1, int(len(y_score) * top_k_fraction))
    top_idx = np.argsort(-y_score)[:k]
    top_rate = float(np.mean(y_true[top_idx])) if len(top_idx) else 0.0

    return {
        "auc": auc,
        "pr_auc": pr_auc,
        "precision_at_k": top_rate,
        "lift_at_k": (float(top_rate / base_rate) if base_rate > 0 else np.nan),
        "positive_rate": base_rate,
        "sample_size": float(len(y_true)),
    }


def choose_threshold(y_true: np.ndarray, y_score: np.ndarray) -> float:
    best_t, best_f1 = 0.5, -1.0
    for t in np.linspace(0.05, 0.95, 19):
        y_pred = (y_score >= t).astype(int)
        tp = np.sum((y_pred == 1) & (y_true == 1))
        fp = np.sum((y_pred == 1) & (y_true == 0))
        fn = np.sum((y_pred == 0) & (y_true == 1))
        p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * p * r / (p + r)) if (p + r) > 0 else 0.0
        if f1 > best_f1:
            best_f1, best_t = f1, float(t)
    return best_t


def build_model(model_name: str):
    if model_name == "logreg_calibrated":
        base = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("clf", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42)),
            ]
        )
        return CalibratedClassifierCV(base, method="sigmoid", cv=3)

    if model_name == "hgb_challenger":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    HistGradientBoostingClassifier(
                        learning_rate=0.05,
                        max_depth=6,
                        max_iter=200,
                        min_samples_leaf=50,
                        random_state=42,
                    ),
                ),
            ]
        )

    raise ValueError(f"Unknown model name: {model_name}")


def rolling_time_cv(ds_train: pd.DataFrame, min_train_snapshots: int, model_name: str):
    snaps = sorted(pd.to_datetime(ds_train["snapshot_date"]).dt.date.unique())
    if len(snaps) <= min_train_snapshots:
        raise RuntimeError("Not enough snapshots for rolling time CV")

    folds = []
    oof_rows = []

    for i in range(min_train_snapshots, len(snaps)):
        train_snaps = set(snaps[:i])
        val_snap = snaps[i]

        tr = ds_train[pd.to_datetime(ds_train["snapshot_date"]).dt.date.isin(train_snaps)].copy()
        va = ds_train[pd.to_datetime(ds_train["snapshot_date"]).dt.date == val_snap].copy()
        if tr.empty or va.empty or tr["target_1"].nunique() < 2:
            continue

        model = build_model(model_name)
        model.fit(tr[FEATURE_COLS], tr["target_1"].astype(int).values)

        va_score = model.predict_proba(va[FEATURE_COLS])[:, 1]
        va_y = va["target_1"].astype(int).values
        m = eval_metrics(va_y, va_score, TOP_K_FRACTION)

        folds.append(
            {
                "fold_index": len(folds) + 1,
                "split_name": f"cv_fold_{len(folds) + 1}",
                "snapshot_start": min(train_snaps),
                "snapshot_end": val_snap,
                "metrics": m,
            }
        )
        oof_rows.append(pd.DataFrame({"snapshot_date": va["snapshot_date"].values, "target_1": va_y, "score": va_score}))

    if not folds:
        raise RuntimeError("No valid CV folds generated.")

    return folds, pd.concat(oof_rows, ignore_index=True)


def decile_diagnostics(df_split: pd.DataFrame, y_score: np.ndarray) -> pd.DataFrame:
    tmp = df_split[["snapshot_date", "target_1"]].copy().reset_index(drop=True)
    tmp["score"] = y_score
    tmp["decile"] = pd.qcut(tmp["score"], 10, labels=False, duplicates="drop")
    tmp["decile"] = tmp["decile"].astype(int) + 1
    out = (
        tmp.groupby("decile", as_index=False)
        .agg(
            min_score=("score", "min"),
            max_score=("score", "max"),
            avg_score=("score", "mean"),
            positive_rate=("target_1", "mean"),
            sample_size=("target_1", "size"),
        )
        .sort_values("decile", ascending=False)
    )
    return out


def model_score_for_selection(cv_folds: list, holdout_metrics: dict) -> float:
    cv_lift = np.nanmean([f["metrics"].get("lift_at_k", np.nan) for f in cv_folds])
    holdout_lift = holdout_metrics.get("lift_at_k", np.nan)
    holdout_auc = holdout_metrics.get("auc", np.nan)
    return float((0.5 * cv_lift) + (0.4 * holdout_lift) + (0.1 * holdout_auc))


def train_evaluate_candidate(model_name: str, ds_train_cv: pd.DataFrame, ds_holdout: pd.DataFrame):
    cv_folds, oof = rolling_time_cv(ds_train_cv, MIN_TRAIN_SNAPSHOTS, model_name)
    threshold = choose_threshold(oof["target_1"].astype(int).values, oof["score"].astype(float).values)

    final_model = build_model(model_name)
    final_model.fit(ds_train_cv[FEATURE_COLS], ds_train_cv["target_1"].astype(int).values)
    holdout_score = final_model.predict_proba(ds_holdout[FEATURE_COLS])[:, 1]
    holdout_metrics = eval_metrics(ds_holdout["target_1"].astype(int).values, holdout_score, TOP_K_FRACTION)

    return {
        "model_name": model_name,
        "cv_folds": cv_folds,
        "threshold": threshold,
        "final_model": final_model,
        "holdout_score": holdout_score,
        "holdout_metrics": holdout_metrics,
        "selection_score": model_score_for_selection(cv_folds, holdout_metrics),
    }


def main():
    spark = SparkSession.builder.appName("retail-ml-train-eval").getOrCreate()
    _ = spark

    conn = sf_conn()
    try:
        ensure_ml_tables(conn)
        tx = build_customer_orders_pd_from_silver(conn)
    finally:
        conn.close()

    ds = build_snapshot_dataset(tx, CHURN_WINDOW_DAYS, ACTIVE_LOOKBACK_DAYS, MIN_ORDERS_LIFETIME)
    all_snaps = sorted(pd.to_datetime(ds["snapshot_date"]).dt.date.unique())

    holdout_n = min(max(1, HOLDOUT_SNAPSHOTS), max(1, len(all_snaps) - 2))
    holdout_snaps = set(all_snaps[-holdout_n:])
    train_cv_snaps = set(all_snaps[:-holdout_n])
    if len(train_cv_snaps) < MIN_TRAIN_SNAPSHOTS + 1:
        raise RuntimeError("Not enough snapshots left for CV after holdout split")

    ds_train_cv = ds[pd.to_datetime(ds["snapshot_date"]).dt.date.isin(train_cv_snaps)].copy()
    ds_holdout = ds[pd.to_datetime(ds["snapshot_date"]).dt.date.isin(holdout_snaps)].copy()

    candidates = ["logreg_calibrated", "hgb_challenger"]
    results = [train_evaluate_candidate(name, ds_train_cv, ds_holdout) for name in candidates]
    best = sorted(results, key=lambda x: x["selection_score"], reverse=True)[0]

    run_ts = datetime.now(UTC)
    notes = (
        f"horizon_days={CHURN_WINDOW_DAYS}, active_lookback_days={ACTIVE_LOOKBACK_DAYS}, "
        f"min_orders_lifetime={MIN_ORDERS_LIFETIME}, top_k_fraction={TOP_K_FRACTION}, "
        f"holdout_snapshots={holdout_n}"
    )

    metrics_rows = []
    decile_rows = []
    registry_rows = []

    def add_metrics_rows(candidate_result: dict):
        c_name = candidate_result["model_name"]
        for fold in candidate_result["cv_folds"]:
            for k, v in fold["metrics"].items():
                if pd.isna(v):
                    continue
                metrics_rows.append(
                    {
                        "MODEL_NAME": c_name,
                        "MODEL_VERSION": MODEL_VERSION,
                        "RUN_TS": run_ts,
                        "SPLIT_NAME": fold["split_name"],
                        "SNAPSHOT_START": fold["snapshot_start"],
                        "SNAPSHOT_END": fold["snapshot_end"],
                        "METRIC_NAME": k,
                        "METRIC_VALUE": float(v),
                        "NOTES": notes,
                    }
                )

        for k, v in candidate_result["holdout_metrics"].items():
            if pd.isna(v):
                continue
            metrics_rows.append(
                {
                    "MODEL_NAME": c_name,
                    "MODEL_VERSION": MODEL_VERSION,
                    "RUN_TS": run_ts,
                    "SPLIT_NAME": "holdout",
                    "SNAPSHOT_START": min(holdout_snaps),
                    "SNAPSHOT_END": max(holdout_snaps),
                    "METRIC_NAME": k,
                    "METRIC_VALUE": float(v),
                    "NOTES": notes,
                }
            )

        dec = decile_diagnostics(ds_holdout, candidate_result["holdout_score"])
        dec["MODEL_NAME"] = c_name
        dec["MODEL_VERSION"] = MODEL_VERSION
        dec["RUN_TS"] = run_ts
        dec["SPLIT_NAME"] = "holdout"
        dec["SNAPSHOT_START"] = min(holdout_snaps)
        dec["SNAPSHOT_END"] = max(holdout_snaps)
        dec["NOTES"] = notes
        decile_rows.append(
            dec[
                [
                    "MODEL_NAME",
                    "MODEL_VERSION",
                    "RUN_TS",
                    "SPLIT_NAME",
                    "SNAPSHOT_START",
                    "SNAPSHOT_END",
                    "decile",
                    "min_score",
                    "max_score",
                    "avg_score",
                    "positive_rate",
                    "sample_size",
                    "NOTES",
                ]
            ].rename(
                columns={
                    "decile": "DECILE",
                    "min_score": "MIN_SCORE",
                    "max_score": "MAX_SCORE",
                    "avg_score": "AVG_SCORE",
                    "positive_rate": "POSITIVE_RATE",
                    "sample_size": "SAMPLE_SIZE",
                }
            )
        )

        registry_rows.append(
            {
                "MODEL_NAME": c_name,
                "MODEL_VERSION": MODEL_VERSION,
                "RUN_TS": run_ts,
                "STATUS": "CHAMPION" if c_name == best["model_name"] else "CHALLENGER",
                "SELECTION_REASON": f"selection_score={candidate_result['selection_score']:.4f}",
                "HOLDOUT_AUC": candidate_result["holdout_metrics"].get("auc", np.nan),
                "HOLDOUT_PR_AUC": candidate_result["holdout_metrics"].get("pr_auc", np.nan),
                "HOLDOUT_LIFT_AT_K": candidate_result["holdout_metrics"].get("lift_at_k", np.nan),
                "HOLDOUT_PRECISION_AT_K": candidate_result["holdout_metrics"].get("precision_at_k", np.nan),
                "HOLDOUT_POSITIVE_RATE": candidate_result["holdout_metrics"].get("positive_rate", np.nan),
                "HOLDOUT_SAMPLE_SIZE": candidate_result["holdout_metrics"].get("sample_size", np.nan),
                "NOTES": notes,
            }
        )

    for r in results:
        add_metrics_rows(r)

    latest_snapshot = pd.to_datetime(ds["snapshot_date"]).max().date()
    latest_df = ds[pd.to_datetime(ds["snapshot_date"]).dt.date == latest_snapshot].copy()
    latest_score = best["final_model"].predict_proba(latest_df[FEATURE_COLS])[:, 1]

    pred_df = pd.DataFrame(
        {
            "CUSTOMER_ID": latest_df["customer_id"].values,
            "SNAPSHOT_DATE": latest_snapshot,
            "CHURN_SCORE": latest_score,
            "CHURN_LABEL": np.where(latest_score >= best["threshold"], "LIKELY_REPEAT", "UNLIKELY_REPEAT"),
            "MODEL_VERSION": MODEL_VERSION,
            "SCORED_AT": run_ts,
        }
    )

    conn = sf_conn()
    try:
        ensure_ml_tables(conn)
        append_df(conn, pd.DataFrame(metrics_rows), "RETAIL_DB.ML.MODEL_METRICS")
        append_df(conn, pd.concat(decile_rows, ignore_index=True), "RETAIL_DB.ML.MODEL_DECILE_DIAGNOSTICS")
        append_df(conn, pd.DataFrame(registry_rows), "RETAIL_DB.ML.MODEL_REGISTRY")
        replace_df(conn, pred_df, "RETAIL_DB.ML.CUSTOMER_CHURN_SCORE")
    finally:
        conn.close()

    print("ML train/eval completed (champion/challenger)")
    print(f"model_version={MODEL_VERSION}")
    print(f"winner={best['model_name']}")
    print(f"threshold={best['threshold']:.4f}")
    print(f"cv_folds={len(best['cv_folds'])}")
    print("winner holdout metrics:")
    for k, v in best["holdout_metrics"].items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()

# COMMAND ----------

# COMMAND ----------
# Extension Cell 1: noise reduction features + outlier handling + calibration metrics table

from sklearn.metrics import brier_score_loss

def add_noise_reduction_features(ds: pd.DataFrame) -> pd.DataFrame:
    x = ds.copy()

    # 1) Winsorize heavy-tail monetary fields (1st-99th percentile)
    for c in ["monetary_30d", "monetary_90d", "monetary_lifetime", "avg_order_value_lifetime"]:
        lo = x[c].quantile(0.01)
        hi = x[c].quantile(0.99)
        x[c] = x[c].clip(lo, hi)

    # 2) Log transforms to reduce skew
    x["log_monetary_30d"] = np.log1p(x["monetary_30d"])
    x["log_monetary_90d"] = np.log1p(x["monetary_90d"])
    x["log_monetary_lifetime"] = np.log1p(x["monetary_lifetime"])
    x["log_avg_order_value_lifetime"] = np.log1p(x["avg_order_value_lifetime"])

    # 3) Trend / velocity features
    x["freq_delta_30_vs_90"] = x["frequency_30d"] - (x["frequency_90d"] / 3.0)
    x["monetary_delta_30_vs_90"] = x["monetary_30d"] - (x["monetary_90d"] / 3.0)
    x["monetary_per_day_90d"] = x["monetary_90d"] / 90.0
    x["order_rate_lifetime"] = x["orders_lifetime"] / np.maximum(x["tenure_days"], 1)

    return x


def calibration_by_bin(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    tmp = pd.DataFrame({"y": y_true, "p": y_prob})
    tmp["bin"] = pd.qcut(tmp["p"], q=n_bins, labels=False, duplicates="drop") + 1
    out = (
        tmp.groupby("bin", as_index=False)
        .agg(
            avg_pred=("p", "mean"),
            actual_rate=("y", "mean"),
            sample_size=("y", "size"),
        )
        .sort_values("bin", ascending=False)
    )
    out["abs_gap"] = (out["avg_pred"] - out["actual_rate"]).abs()
    return out


def ensure_calibration_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS RETAIL_DB.ML")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS RETAIL_DB.ML.MODEL_CALIBRATION_DIAGNOSTICS (
              MODEL_NAME STRING,
              MODEL_VERSION STRING,
              RUN_TS TIMESTAMP_NTZ,
              SPLIT_NAME STRING,
              BIN NUMBER,
              AVG_PRED FLOAT,
              ACTUAL_RATE FLOAT,
              ABS_GAP FLOAT,
              SAMPLE_SIZE NUMBER,
              NOTES STRING
            )
        """)
        conn.commit()
    finally:
        cur.close()


# COMMAND ----------

# COMMAND ----------
# Extension Cell 2: run extended train/eval with new features, and log calibration diagnostics

# Build dataset using your existing pipeline functions
conn = sf_conn()
try:
    tx = build_customer_orders_pd_from_silver(conn)
finally:
    conn.close()

ds_base = build_snapshot_dataset(
    tx,
    CHURN_WINDOW_DAYS,
    ACTIVE_LOOKBACK_DAYS,
    MIN_ORDERS_LIFETIME,
)

ds_ext = add_noise_reduction_features(ds_base)

EXT_FEATURE_COLS = FEATURE_COLS + [
    "log_monetary_30d",
    "log_monetary_90d",
    "log_monetary_lifetime",
    "log_avg_order_value_lifetime",
    "freq_delta_30_vs_90",
    "monetary_delta_30_vs_90",
    "monetary_per_day_90d",
    "order_rate_lifetime",
]

# same holdout policy
all_snaps = sorted(pd.to_datetime(ds_ext["snapshot_date"]).dt.date.unique())
holdout_n = min(max(1, HOLDOUT_SNAPSHOTS), max(1, len(all_snaps) - 2))
holdout_snaps = set(all_snaps[-holdout_n:])
train_cv_snaps = set(all_snaps[:-holdout_n])

ds_train = ds_ext[pd.to_datetime(ds_ext["snapshot_date"]).dt.date.isin(train_cv_snaps)].copy()
ds_holdout = ds_ext[pd.to_datetime(ds_ext["snapshot_date"]).dt.date.isin(holdout_snaps)].copy()

# train calibrated logistic only (stable champion style)
ext_model = CalibratedClassifierCV(
    Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42)),
        ]
    ),
    method="sigmoid",
    cv=3,
)

ext_model.fit(ds_train[EXT_FEATURE_COLS], ds_train["target_1"].astype(int).values)
holdout_prob = ext_model.predict_proba(ds_holdout[EXT_FEATURE_COLS])[:, 1]
holdout_y = ds_holdout["target_1"].astype(int).values

ext_metrics = eval_metrics(holdout_y, holdout_prob, TOP_K_FRACTION)
brier = brier_score_loss(holdout_y, holdout_prob)

run_ts = datetime.now(UTC)
notes = "extension_cell: winsorize+log+trend+calibration"

# log extra metric rows
extra_metrics_df = pd.DataFrame([
    {
        "MODEL_NAME": "logreg_calibrated_ext",
        "MODEL_VERSION": MODEL_VERSION,
        "RUN_TS": run_ts,
        "SPLIT_NAME": "holdout_ext",
        "SNAPSHOT_START": min(holdout_snaps),
        "SNAPSHOT_END": max(holdout_snaps),
        "METRIC_NAME": k,
        "METRIC_VALUE": float(v),
        "NOTES": notes,
    }
    for k, v in ext_metrics.items()
] + [
    {
        "MODEL_NAME": "logreg_calibrated_ext",
        "MODEL_VERSION": MODEL_VERSION,
        "RUN_TS": run_ts,
        "SPLIT_NAME": "holdout_ext",
        "SNAPSHOT_START": min(holdout_snaps),
        "SNAPSHOT_END": max(holdout_snaps),
        "METRIC_NAME": "brier_score",
        "METRIC_VALUE": float(brier),
        "NOTES": notes,
    }
])

cal_df = calibration_by_bin(holdout_y, holdout_prob, n_bins=10)
cal_df["MODEL_NAME"] = "logreg_calibrated_ext"
cal_df["MODEL_VERSION"] = MODEL_VERSION
cal_df["RUN_TS"] = run_ts
cal_df["SPLIT_NAME"] = "holdout_ext"
cal_df["NOTES"] = notes
cal_df = cal_df.rename(columns={
    "bin": "BIN",
    "avg_pred": "AVG_PRED",
    "actual_rate": "ACTUAL_RATE",
    "abs_gap": "ABS_GAP",
    "sample_size": "SAMPLE_SIZE",
})[
    ["MODEL_NAME", "MODEL_VERSION", "RUN_TS", "SPLIT_NAME", "BIN", "AVG_PRED", "ACTUAL_RATE", "ABS_GAP", "SAMPLE_SIZE", "NOTES"]
]

conn = sf_conn()
try:
    ensure_ml_tables(conn)
    ensure_calibration_table(conn)
    append_df(conn, extra_metrics_df, "RETAIL_DB.ML.MODEL_METRICS")
    append_df(conn, cal_df, "RETAIL_DB.ML.MODEL_CALIBRATION_DIAGNOSTICS")
finally:
    conn.close()

print("Extension run completed")
print("holdout_ext metrics:", ext_metrics)
print("brier_score:", brier)


# COMMAND ----------

