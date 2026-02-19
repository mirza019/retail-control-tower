#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

PROFILE="${SNOWFLAKE_PROFILE_PROD:-retail-prod}"
RUN_ML="${RUN_ML_AFTER_SQL_PROD:-true}"

if ! command -v snow >/dev/null 2>&1; then
  echo "Missing command: snow"
  exit 1
fi

echo "[1/5] Ensure prod schema/layers/tables"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/01_create_db_schema.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/04_create_layers.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/05_create_bronze_silver_model.sql"

echo "[2/5] Build SILVER + MODEL"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/07_merge_model.sql"

echo "[3/5] Build GOLD"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/06_create_gold_views_model_based.sql"

echo "[4/5] Run DQ checks"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/08_dq_checks_enterprise.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/03_dq_checks.sql"

if [[ "$RUN_ML" == "true" ]]; then
  echo "[5/5] Run Databricks job"
  DATABRICKS_PROFILE="${DATABRICKS_PROFILE_PROD:-${DATABRICKS_PROFILE:-retail-dev}}" \
  DATABRICKS_JOB_ID="${DATABRICKS_JOB_ID_PROD:-${DATABRICKS_JOB_ID:-}}" \
  bash "$ROOT_DIR/databricks/run_now.sh"
else
  echo "[5/5] Skip Databricks job (RUN_ML_AFTER_SQL_PROD=$RUN_ML)"
fi

echo "Prod pipeline completed after Bronze change."
