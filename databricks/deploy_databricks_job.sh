#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

PROFILE="${DATABRICKS_PROFILE:-retail-dev}"
WORKSPACE_USER="${DATABRICKS_WORKSPACE_USER:-}"
RAW_PATH="${DATABRICKS_RAW_PATH:-/Volumes/main/default/retail_raw}"
SECRET_SCOPE="${DATABRICKS_SECRET_SCOPE:-retail-secrets}"
JOB_NAME="${DATABRICKS_JOB_NAME:-retail-control-tower-etl-ml-serverless}"
ENABLE_ML="${DATABRICKS_ENABLE_ML:-false}"

if [[ -z "$WORKSPACE_USER" ]]; then
  echo "Set DATABRICKS_WORKSPACE_USER (example: iqubal.ewu@gmail.com)"
  exit 1
fi

ETL_NOTEBOOK_PATH="/Users/${WORKSPACE_USER}/retail-control-tower/etl_build_silver_gold_dbx"
ML_NOTEBOOK_PATH="/Users/${WORKSPACE_USER}/retail-control-tower/ml_scoring_dbx"

TMP_JSON="$(mktemp)"
if [[ "$ENABLE_ML" == "true" ]]; then
  cat > "$TMP_JSON" <<JSON
{
  "name": "${JOB_NAME}",
  "tasks": [
    {
      "task_key": "etl_gold_to_snowflake",
      "notebook_task": {
        "notebook_path": "${ETL_NOTEBOOK_PATH}",
        "base_parameters": {
          "RAW_PATH": "${RAW_PATH}",
          "SCOPE": "${SECRET_SCOPE}"
        }
      }
    },
    {
      "task_key": "ml_scoring_to_snowflake",
      "depends_on": [{"task_key": "etl_gold_to_snowflake"}],
      "notebook_task": {
        "notebook_path": "${ML_NOTEBOOK_PATH}",
        "base_parameters": {
          "RAW_PATH": "${RAW_PATH}",
          "SCOPE": "${SECRET_SCOPE}"
        }
      }
    }
  ]
}
JSON
else
  cat > "$TMP_JSON" <<JSON
{
  "name": "${JOB_NAME}",
  "tasks": [
    {
      "task_key": "etl_gold_to_snowflake",
      "notebook_task": {
        "notebook_path": "${ETL_NOTEBOOK_PATH}",
        "base_parameters": {
          "RAW_PATH": "${RAW_PATH}",
          "SCOPE": "${SECRET_SCOPE}"
        }
      }
    }
  ]
}
JSON
fi

JOB_ID="$(databricks jobs list --profile "$PROFILE" --output json | python3 -c "import json,sys; data=json.load(sys.stdin); name='${JOB_NAME}';
for j in data:
    if j.get('settings',{}).get('name')==name:
        print(j.get('job_id')); break")"

if [[ -n "$JOB_ID" ]]; then
  echo "Updating existing job: $JOB_ID"
  databricks jobs reset "$JOB_ID" --json @"$TMP_JSON" --profile "$PROFILE"
  echo "JOB_ID=$JOB_ID"
else
  echo "Creating new job: $JOB_NAME"
  databricks jobs create --json @"$TMP_JSON" --profile "$PROFILE"
fi

rm -f "$TMP_JSON"
