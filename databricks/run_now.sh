#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

PROFILE="${DATABRICKS_PROFILE:-retail-dev}"
JOB_ID="${1:-${DATABRICKS_JOB_ID:-}}"

if [[ -z "$JOB_ID" ]]; then
  echo "Usage: bash databricks/run_now.sh <job_id>"
  echo "or set DATABRICKS_JOB_ID in environment"
  exit 1
fi

databricks jobs run-now "$JOB_ID" --profile "$PROFILE" --performance-target STANDARD
