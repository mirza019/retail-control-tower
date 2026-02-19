#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -d "$ROOT_DIR/venv" ]]; then
  echo "Missing venv. Run: bash scripts/setup_venv.sh"
  exit 1
fi

source "$ROOT_DIR/venv/bin/activate"

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing .env with Snowflake credentials."
  exit 1
fi

set -a
source "$ROOT_DIR/.env"
set +a

required=(
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE
)

for key in "${required[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "Missing env var: $key"
    exit 1
  fi
done

python "$ROOT_DIR/spark_jobs/etl_build_silver_gold.py"

echo "Local pipeline completed."
