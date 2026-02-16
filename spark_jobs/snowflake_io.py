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
