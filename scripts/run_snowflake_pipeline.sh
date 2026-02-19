#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-retail-dev}"
DB="RETAIL_DB"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1"
    exit 1
  }
}

require_file() {
  [[ -f "$1" ]] || {
    echo "Missing required file: $1"
    exit 1
  }
}

run_sql_file() {
  local file="$1"
  echo "Running SQL file: $file"
  snow sql -c "$PROFILE" -f "$file"
}

run_sql_query() {
  local query="$1"
  snow sql -c "$PROFILE" -q "$query"
}

require_cmd snow
require_file "$ROOT_DIR/infra/dev/01_create_db_schema.sql"
require_file "$ROOT_DIR/infra/dev/04_create_layers.sql"
require_file "$ROOT_DIR/infra/dev/05_create_bronze_silver_model.sql"
require_file "$ROOT_DIR/infra/dev/07_merge_model.sql"
require_file "$ROOT_DIR/infra/dev/06_create_gold_views_model_based.sql"
require_file "$ROOT_DIR/infra/dev/08_dq_checks_enterprise.sql"

# Optional local raw files for PUT
orders_csv="$ROOT_DIR/data/raw/olist_orders_dataset.csv"
items_csv="$ROOT_DIR/data/raw/olist_order_items_dataset.csv"
customers_csv="$ROOT_DIR/data/raw/olist_customers_dataset.csv"

run_sql_file "$ROOT_DIR/infra/dev/01_create_db_schema.sql"
run_sql_file "$ROOT_DIR/infra/dev/04_create_layers.sql"
run_sql_file "$ROOT_DIR/infra/dev/05_create_bronze_silver_model.sql"

if [[ -f "$orders_csv" && -f "$items_csv" && -f "$customers_csv" ]]; then
  echo "Uploading local raw CSVs to Snowflake stage"
  run_sql_query "PUT 'file://$orders_csv' @${DB}.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
  run_sql_query "PUT 'file://$items_csv' @${DB}.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
  run_sql_query "PUT 'file://$customers_csv' @${DB}.BRONZE.RAW_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
else
  echo "Skipping PUT: local raw CSV files not found under data/raw"
fi

echo "Copying staged files into BRONZE tables"
run_sql_query "
COPY INTO ${DB}.BRONZE.ORDERS_RAW
  (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_PURCHASE_TIMESTAMP, ORDER_APPROVED_AT,
   ORDER_DELIVERED_CARRIER_DATE, ORDER_DELIVERED_CUSTOMER_DATE, ORDER_ESTIMATED_DELIVERY_DATE)
FROM (
  SELECT t.\$1, t.\$2, t.\$3, t.\$4, t.\$5, t.\$6, t.\$7, t.\$8
  FROM @${DB}.BRONZE.RAW_STAGE/olist_orders_dataset.csv.gz t
)
FILE_FORMAT = (FORMAT_NAME = ${DB}.BRONZE.FF_CSV)
ON_ERROR = 'ABORT_STATEMENT';
"

run_sql_query "
COPY INTO ${DB}.BRONZE.ORDER_ITEMS_RAW
  (ORDER_ID, ORDER_ITEM_ID, PRODUCT_ID, SELLER_ID, SHIPPING_LIMIT_DATE, PRICE, FREIGHT_VALUE)
FROM (
  SELECT t.\$1, t.\$2, t.\$3, t.\$4, t.\$5, t.\$6, t.\$7
  FROM @${DB}.BRONZE.RAW_STAGE/olist_order_items_dataset.csv.gz t
)
FILE_FORMAT = (FORMAT_NAME = ${DB}.BRONZE.FF_CSV)
ON_ERROR = 'ABORT_STATEMENT';
"

run_sql_query "
COPY INTO ${DB}.BRONZE.CUSTOMERS_RAW
  (CUSTOMER_ID, CUSTOMER_UNIQUE_ID, CUSTOMER_ZIP_CODE_PREFIX, CUSTOMER_CITY, CUSTOMER_STATE)
FROM (
  SELECT t.\$1, t.\$2, t.\$3, t.\$4, t.\$5
  FROM @${DB}.BRONZE.RAW_STAGE/olist_customers_dataset.csv.gz t
)
FILE_FORMAT = (FORMAT_NAME = ${DB}.BRONZE.FF_CSV)
ON_ERROR = 'ABORT_STATEMENT';
"

run_sql_file "$ROOT_DIR/infra/dev/07_merge_model.sql"
run_sql_file "$ROOT_DIR/infra/dev/06_create_gold_views_model_based.sql"
run_sql_file "$ROOT_DIR/infra/dev/08_dq_checks_enterprise.sql"

echo "Snowflake pipeline completed for profile: $PROFILE"
