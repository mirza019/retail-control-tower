# Databricks notebook source
# MAGIC %pip install snowflake-connector-python dotenv

# COMMAND ----------

import snowflake.connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format,
    sum as Fsum, countDistinct, max as Fmax,
    datediff, lit
)

spark = SparkSession.builder.appName("retail-etl").getOrCreate()


# COMMAND ----------

dbutils.widgets.text("RAW_PATH", "/Volumes/main/default/retail_raw")
dbutils.widgets.text("SCOPE", "retail-secrets")

RAW_PATH = dbutils.widgets.get("RAW_PATH")
SCOPE    = dbutils.widgets.get("SCOPE")

SNOWFLAKE_ACCOUNT   = dbutils.secrets.get(SCOPE, "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = dbutils.secrets.get(SCOPE, "SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = dbutils.secrets.get(SCOPE, "SNOWFLAKE_PASSWORD")  # do not print
SNOWFLAKE_ROLE      = dbutils.secrets.get(SCOPE, "SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = dbutils.secrets.get(SCOPE, "SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = dbutils.secrets.get(SCOPE, "SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = dbutils.secrets.get(SCOPE, "SNOWFLAKE_SCHEMA")

print(f"RAW_PATH={RAW_PATH}")
print(f"SCOPE={SCOPE}")
print(f"SF account/user/role/wh/db/schema: {SNOWFLAKE_ACCOUNT}, {SNOWFLAKE_USER}, {SNOWFLAKE_ROLE}, {SNOWFLAKE_WAREHOUSE}, {SNOWFLAKE_DATABASE}, {SNOWFLAKE_SCHEMA}")


# COMMAND ----------

# DBTITLE 1,Cell 2
def sf_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

def write_df(df: pd.DataFrame, table_full_name: str):
    conn = sf_conn()
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
        print(f"Loaded {len(df)} rows into {table_full_name}")
    finally:
        cur.close()
        conn.close()


# COMMAND ----------

display(dbutils.fs.ls("/Volumes/main/default/retail_raw"))
