import pandas as pd
import snowflake.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

def _secret_or_raise(scope: str, key: str):
    try:
        return dbutils.secrets.get(scope, key)
    except Exception as exc:
        raise RuntimeError(
            f"Missing Databricks secret: scope='{scope}', key='{key}'. "
            f"Create it with: databricks secrets put-secret {scope} {key} --string-value <value>"
        ) from exc

# Match the working gold notebook pattern.
dbutils.widgets.text("RAW_PATH", "/Volumes/main/default/retail_raw")
dbutils.widgets.text("SCOPE", "retail-secrets")

RAW_PATH = dbutils.widgets.get("RAW_PATH")
SCOPE = dbutils.widgets.get("SCOPE")

def sf_connect():
    scope = SCOPE

    account   = _secret_or_raise(scope, "SNOWFLAKE_ACCOUNT")
    user      = _secret_or_raise(scope, "SNOWFLAKE_USER")
    try:
        password = _secret_or_raise(scope, "SNOWFLAKE_PASSWORD")
    except RuntimeError:
        # Backward compatibility with older key naming in this workspace.
        password = _secret_or_raise(scope, "snowflake_password")
    role      = _secret_or_raise(scope, "SNOWFLAKE_ROLE")
    warehouse = _secret_or_raise(scope, "SNOWFLAKE_WAREHOUSE")
    database  = _secret_or_raise(scope, "SNOWFLAKE_DATABASE")

    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
    )

def truncate_table(conn, full_name: str):
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {full_name}")
    finally:
        cur.close()

def write_pd(conn, df: pd.DataFrame, full_name: str):
    cur = conn.cursor()
    try:
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        cur.executemany(
            f"INSERT INTO {full_name} ({cols}) VALUES ({placeholders})",
            df.itertuples(index=False, name=None),
        )
        conn.commit()
        return len(df)
    finally:
        cur.close()

def main():
    spark = SparkSession.builder.appName("retail-bronze-silver").getOrCreate()

    # ---- Read raw CSV from UC volume ----
    orders_raw = spark.read.csv(f"{RAW_PATH}/olist_orders_dataset.csv", header=True, inferSchema=True)
    items_raw  = spark.read.csv(f"{RAW_PATH}/olist_order_items_dataset.csv", header=True, inferSchema=True)
    cust_raw   = spark.read.csv(f"{RAW_PATH}/olist_customers_dataset.csv", header=True, inferSchema=True)

    # ---- Build SILVER ----
    orders_silver = (
        orders_raw
        .withColumn("ORDER_PURCHASE_TS", to_timestamp(col("order_purchase_timestamp")))
        .withColumn("ORDER_DATE", to_date(col("order_purchase_timestamp")))
        .select(
            col("order_id").alias("ORDER_ID"),
            col("customer_id").alias("CUSTOMER_ID"),
            col("order_status").alias("ORDER_STATUS"),
            col("ORDER_PURCHASE_TS"),
            col("ORDER_DATE"),
        )
    )

    items_silver = (
        items_raw
        .withColumn("ORDER_ITEM_ID", col("order_item_id").cast("int"))
        .withColumn("PRICE", col("price").cast("double"))
        .withColumn("FREIGHT_VALUE", col("freight_value").cast("double"))
        .withColumn("REVENUE", (col("price") + col("freight_value")).cast("double"))
        .select(
            col("order_id").alias("ORDER_ID"),
            col("ORDER_ITEM_ID"),
            col("product_id").alias("PRODUCT_ID"),
            col("PRICE"),
            col("FREIGHT_VALUE"),
            col("REVENUE"),
        )
    )

    cust_silver = (
        cust_raw.select(
            col("customer_id").alias("CUSTOMER_ID"),
            col("customer_unique_id").alias("CUSTOMER_UNIQUE_ID"),
            col("customer_zip_code_prefix").cast("string").alias("CUSTOMER_ZIP_CODE_PREFIX"),
            col("customer_city").alias("CUSTOMER_CITY"),
            col("customer_state").alias("CUSTOMER_STATE"),
        )
    )

    # ---- Push to Snowflake (BRONZE + SILVER) ----
    conn = sf_connect()
    try:
        # Bronze refresh (safe and repeatable)
        truncate_table(conn, "RETAIL_DB.BRONZE.ORDERS_RAW")
        truncate_table(conn, "RETAIL_DB.BRONZE.ORDER_ITEMS_RAW")
        truncate_table(conn, "RETAIL_DB.BRONZE.CUSTOMERS_RAW")

        # Write bronze as strings (raw)
        write_pd(conn, orders_raw.toPandas().astype(str), "RETAIL_DB.BRONZE.ORDERS_RAW")
        write_pd(conn, items_raw.toPandas().astype(str),  "RETAIL_DB.BRONZE.ORDER_ITEMS_RAW")
        write_pd(conn, cust_raw.toPandas().astype(str),   "RETAIL_DB.BRONZE.CUSTOMERS_RAW")

        # Silver refresh (safe for now)
        truncate_table(conn, "RETAIL_DB.SILVER.ORDERS")
        truncate_table(conn, "RETAIL_DB.SILVER.ORDER_ITEMS")
        truncate_table(conn, "RETAIL_DB.SILVER.CUSTOMERS")

        write_pd(conn, orders_silver.toPandas(), "RETAIL_DB.SILVER.ORDERS")
        write_pd(conn, items_silver.toPandas(),  "RETAIL_DB.SILVER.ORDER_ITEMS")
        write_pd(conn, cust_silver.toPandas(),   "RETAIL_DB.SILVER.CUSTOMERS")

        print("✅ Loaded BRONZE + SILVER into Snowflake.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
