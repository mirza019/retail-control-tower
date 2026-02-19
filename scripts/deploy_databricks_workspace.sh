#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-${DATABRICKS_PROFILE:-retail-dev}}"
WORKSPACE_USER="${DATABRICKS_WORKSPACE_USER:-}"
WORKSPACE_DIR="/Users/${WORKSPACE_USER}/retail-control-tower"

if [[ -z "$WORKSPACE_USER" ]]; then
  echo "Set DATABRICKS_WORKSPACE_USER"
  exit 1
fi

databricks workspace mkdirs "$WORKSPACE_DIR" --profile "$PROFILE"

databricks workspace import "${WORKSPACE_DIR}/etl_build_silver_gold_dbx" \
  --file "${ROOT_DIR}/databricks/etl_build_silver_gold_dbx.py" \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"

databricks workspace import "${WORKSPACE_DIR}/ml_scoring_dbx" \
  --file "${ROOT_DIR}/databricks/ml_scoring_dbx.py" \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"

databricks workspace import "${WORKSPACE_DIR}/ml_train_eval_dbx" \
  --file "${ROOT_DIR}/databricks/ml_train_eval_dbx.py" \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"

echo "Databricks workspace deploy completed: ${WORKSPACE_DIR}"
