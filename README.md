# Retail Control Tower

End-to-end retail analytics engineering project using Databricks, Snowflake, and Python.

## Architecture

Raw CSV files -> Snowflake BRONZE -> typed SILVER -> MODEL star schema -> GOLD KPI views/tables -> BI/ML consumption.

Core schemas:
- `RETAIL_DB.BRONZE`: raw landed data
- `RETAIL_DB.SILVER`: cleaned and typed operational data
- `RETAIL_DB.MODEL`: dimensional/fact model
- `RETAIL_DB.GOLD`: KPI outputs for dashboards
- `RETAIL_DB.ML`: ML features/scores (planned)

## Repository Layout

- `infra/dev`: canonical SQL pipeline for development
- `infra/prod`: production SQL placeholders
- `databricks`: Databricks notebooks/scripts and job JSON
- `spark_jobs`: local PySpark + Snowflake write path
- `ml`: starter ML modules (feature build, churn, forecast, anomaly)
- `scripts`: local automation scripts
- `data/raw`: local raw CSVs for local runs

## Prerequisites

- Python 3.9+
- Snowflake CLI (`snow`)
- Databricks CLI (`databricks`) for workspace/job operations
- Snowflake connection profile `retail-dev`

## Snowflake Pipeline Runbook (dev)

Run from repo root.

1. Bootstrap schemas/layers/tables

```bash
snow sql -c retail-dev -f infra/dev/01_create_db_schema.sql
snow sql -c retail-dev -f infra/dev/04_create_layers.sql
snow sql -c retail-dev -f infra/dev/05_create_bronze_silver_model.sql
```

2. Upload raw files to stage (if needed)

```bash
snow sql -c retail-dev -q "PUT 'file://$PWD/data/raw/olist_orders_dataset.csv' @RETAIL_DB.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
snow sql -c retail-dev -q "PUT 'file://$PWD/data/raw/olist_order_items_dataset.csv' @RETAIL_DB.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
snow sql -c retail-dev -q "PUT 'file://$PWD/data/raw/olist_customers_dataset.csv' @RETAIL_DB.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
```

3. Copy staged files to BRONZE tables

```bash
snow sql -c retail-dev -q "
COPY INTO RETAIL_DB.BRONZE.ORDERS_RAW
  (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_PURCHASE_TIMESTAMP, ORDER_APPROVED_AT,
   ORDER_DELIVERED_CARRIER_DATE, ORDER_DELIVERED_CUSTOMER_DATE, ORDER_ESTIMATED_DELIVERY_DATE)
FROM (SELECT t.\$1, t.\$2, t.\$3, t.\$4, t.\$5, t.\$6, t.\$7, t.\$8
      FROM @RETAIL_DB.BRONZE.RAW_STAGE/olist_orders_dataset.csv.gz t)
FILE_FORMAT = (FORMAT_NAME = RETAIL_DB.BRONZE.FF_CSV)
ON_ERROR = 'ABORT_STATEMENT';"

snow sql -c retail-dev -q "
COPY INTO RETAIL_DB.BRONZE.ORDER_ITEMS_RAW
  (ORDER_ID, ORDER_ITEM_ID, PRODUCT_ID, SELLER_ID, SHIPPING_LIMIT_DATE, PRICE, FREIGHT_VALUE)
FROM (SELECT t.\$1, t.\$2, t.\$3, t.\$4, t.\$5, t.\$6, t.\$7
      FROM @RETAIL_DB.BRONZE.RAW_STAGE/olist_order_items_dataset.csv.gz t)
FILE_FORMAT = (FORMAT_NAME = RETAIL_DB.BRONZE.FF_CSV)
ON_ERROR = 'ABORT_STATEMENT';"

snow sql -c retail-dev -q "
COPY INTO RETAIL_DB.BRONZE.CUSTOMERS_RAW
  (CUSTOMER_ID, CUSTOMER_UNIQUE_ID, CUSTOMER_ZIP_CODE_PREFIX, CUSTOMER_CITY, CUSTOMER_STATE)
FROM (SELECT t.\$1, t.\$2, t.\$3, t.\$4, t.\$5
      FROM @RETAIL_DB.BRONZE.RAW_STAGE/olist_customers_dataset.csv.gz t)
FILE_FORMAT = (FORMAT_NAME = RETAIL_DB.BRONZE.FF_CSV)
ON_ERROR = 'ABORT_STATEMENT';"
```

4. Build SILVER/MODEL/GOLD

```bash
snow sql -c retail-dev -f infra/dev/07_merge_model.sql
snow sql -c retail-dev -f infra/dev/06_create_gold_views_model_based.sql
```

5. Data quality checks

```bash
snow sql -c retail-dev -f infra/dev/08_dq_checks_enterprise.sql
snow sql -c retail-dev -f infra/dev/03_dq_checks.sql
```

## Local Python Setup

```bash
bash scripts/setup_venv.sh
source venv/bin/activate
```

## Databricks Notes

- Serverless compute can block Spark connector DML to Snowflake.
- Current Databricks scripts use Python Snowflake connector writes for compatibility.
- Use Databricks secrets scope (`retail-secrets` or `jdbc`) for credentials.

## Current Output

- Bronze landing tables populated
- Silver and model layers built with merge logic
- Gold views available:
  - `GOLD.V_KPI_MONTHLY_REVENUE`
  - `GOLD.V_KPI_DAILY_REVENUE`
  - `GOLD.V_KPI_CUSTOMER_RFM`

## Next Productization Steps

- Fill `infra/prod` with production equivalents of dev SQL
- Add Power BI dashboard on GOLD views
- Add Alteryx workflow consuming Snowflake model/gold data
- Write ML scores to `RETAIL_DB.ML` and expose in BI

## GitHub CI/CD

- CI: `.github/workflows/ci.yml` (syntax and shell checks)
- CD: `.github/workflows/deploy.yml`
  - Push to `main` deploys to dev
  - Manual dispatch with `target=prod` deploys to prod

Required GitHub Secrets:
- Dev:
  - `DATABRICKS_HOST_DEV`, `DATABRICKS_TOKEN_DEV`, `DATABRICKS_WORKSPACE_USER_DEV`, `DATABRICKS_JOB_ID_DEV`
  - `SNOWFLAKE_ACCOUNT_DEV`, `SNOWFLAKE_USER_DEV`, `SNOWFLAKE_PASSWORD_DEV`, `SNOWFLAKE_ROLE_DEV`, `SNOWFLAKE_WAREHOUSE_DEV`, `SNOWFLAKE_DATABASE_DEV`
- Prod:
  - `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD`, `DATABRICKS_WORKSPACE_USER_PROD`, `DATABRICKS_JOB_ID_PROD`
  - `SNOWFLAKE_ACCOUNT_PROD`, `SNOWFLAKE_USER_PROD`, `SNOWFLAKE_PASSWORD_PROD`, `SNOWFLAKE_ROLE_PROD`, `SNOWFLAKE_WAREHOUSE_PROD`, `SNOWFLAKE_DATABASE_PROD`
