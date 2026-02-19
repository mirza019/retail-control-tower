#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-retail-prod}"

command -v snow >/dev/null 2>&1 || {
  echo "Missing required command: snow"
  exit 1
}

snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/01_create_db_schema.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/04_create_layers.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/05_create_bronze_silver_model.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/07_merge_model.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/06_create_gold_views_model_based.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/08_dq_checks_enterprise.sql"
snow sql -c "$PROFILE" -f "$ROOT_DIR/infra/prod/03_dq_checks.sql"

echo "Prod SQL pipeline completed for profile: $PROFILE"
