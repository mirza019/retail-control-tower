#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

PROFILE="${SNOWFLAKE_PROFILE:-retail-dev}"
RUN_ML="${RUN_ML_AFTER_SQL:-true}"

if ! command -v snow >/dev/null 2>&1; then
  echo "Missing command: snow"
  exit 1
fi

echo "[1/4] Build SILVER + MODEL"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/dev/07_merge_model.sql"

echo "[2/4] Build GOLD"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/dev/06_create_gold_views_model_based.sql"

echo "[3/4] Run DQ checks"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/dev/08_dq_checks_enterprise.sql"

if [[ "$RUN_ML" == "true" ]]; then
  echo "[4/4] Run Databricks job"
  bash "$ROOT_DIR/databricks/run_now.sh"
else
  echo "[4/4] Skip Databricks job (RUN_ML_AFTER_SQL=$RUN_ML)"
fi

echo "Pipeline completed after Bronze change."
