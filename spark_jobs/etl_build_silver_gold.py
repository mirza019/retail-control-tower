import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def _conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )

def write_df(df: pd.DataFrame, table_full_name: str):
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {table_full_name}")
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        cur.executemany(
            f"INSERT INTO {table_full_name} ({cols}) VALUES ({placeholders})",
            df.itertuples(index=False, name=None),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, sum as Fsum, countDistinct, max as Fmax, datediff, lit

RAW = "/Users/iqubal.ewu@gmail.com/retail-control-tower/raw"

def main():
    spark = SparkSession.builder.master("local[*]").appName("retail-etl").getOrCreate()

    orders = spark.read.csv(f"{RAW}/olist_orders_dataset.csv", header=True, inferSchema=True)
    items = spark.read.csv(f"{RAW}/olist_order_items_dataset.csv", header=True, inferSchema=True)
    customers = spark.read.csv(f"{RAW}/olist_customers_dataset.csv", header=True, inferSchema=True)

    orders = (orders
              .filter(col("order_status") == "delivered")
              .withColumn("order_date", to_date(col("order_purchase_timestamp")))
              .select("order_id", "customer_id", "order_date"))

    items = items.withColumn("revenue", col("price") + col("freight_value")) \
                 .select("order_id", "product_id", "revenue")

    customers = customers.select("customer_id", "customer_unique_id")

    tx = items.join(orders, "order_id").join(customers, "customer_id") \
              .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))

    # KPI 1: Monthly revenue
    monthly = (tx.groupBy("year_month")
               .agg(Fsum("revenue").alias("REVENUE"),
                    countDistinct("order_id").alias("ORDERS"),
                    countDistinct("customer_unique_id").alias("UNIQUE_CUSTOMERS"))
               .orderBy("year_month"))
    monthly_pd = monthly.toPandas().rename(columns={"year_month": "YEAR_MONTH"})
    write_df(monthly_pd[["YEAR_MONTH","REVENUE","ORDERS","UNIQUE_CUSTOMERS"]],
             "RETAIL_DB.GOLD.KPI_MONTHLY_REVENUE")

    # KPI 2: Daily revenue
    daily = (tx.groupBy("order_date")
             .agg(Fsum("revenue").alias("REVENUE"),
                  countDistinct("order_id").alias("ORDERS"))
             .orderBy("order_date"))
    daily_pd = daily.toPandas().rename(columns={"order_date": "ORDER_DATE"})
    write_df(daily_pd[["ORDER_DATE","REVENUE","ORDERS"]],
             "RETAIL_DB.GOLD.KPI_DAILY_REVENUE")

    # KPI 3: Customer RFM
    last_date = tx.agg(Fmax("order_date").alias("maxd")).collect()[0]["maxd"]
    rfm = (tx.groupBy("customer_unique_id")
           .agg(Fmax("order_date").alias("LAST_ORDER_DATE"),
                countDistinct("order_id").alias("FREQUENCY"),
                Fsum("revenue").alias("MONETARY"))
           .withColumn("RECENCY_DAYS", datediff(lit(last_date), col("LAST_ORDER_DATE")))
           .select(col("customer_unique_id").alias("CUSTOMER_ID"),
                   "RECENCY_DAYS","FREQUENCY","MONETARY","LAST_ORDER_DATE"))
    rfm_pd = rfm.toPandas()
    write_df(rfm_pd[["CUSTOMER_ID","RECENCY_DAYS","FREQUENCY","MONETARY","LAST_ORDER_DATE"]],
             "RETAIL_DB.GOLD.KPI_CUSTOMER_RFM")

    print("✅ ETL finished: GOLD tables loaded into Snowflake.")
    spark.stop()

if __name__ == "__main__":
    main()