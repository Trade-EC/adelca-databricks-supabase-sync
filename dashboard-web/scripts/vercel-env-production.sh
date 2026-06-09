#!/usr/bin/env bash
# One-off: add Production env to linked Vercel project (sensitive; do not commit values).
set -euo pipefail
cd "$(dirname "$0")/.."
SCOPE="${VERCEL_SCOPE:-adelca-lineas-transportistas}"

set -a
# shellcheck source=../transportistas_sync/.env
source "../transportistas_sync/.env"
set +a
AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)"
export AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key)"
export AWS_SECRET_ACCESS_KEY
export LAMBDA_NAME="${LAMBDA_NAME:-patek-philippe}"
export AWS_REGION="${AWS_REGION:-$(aws configure get region)}"

add_one() {
  local name=$1
  local value=$2
  echo "Adding $name ..."
  vercel env add "$name" production --value "$value" --yes --force --scope "$SCOPE" --cwd "$(pwd -P)"
}

add_one AWS_REGION "$AWS_REGION"
add_one LAMBDA_NAME "$LAMBDA_NAME"
add_one SUPABASE_URL "$SUPABASE_URL"
add_one SUPABASE_SERVICE_ROLE_KEY "$SUPABASE_SERVICE_ROLE_KEY"
add_one DATABRICKS_PRD_HOST "$DATABRICKS_PRD_HOST"
add_one DATABRICKS_PRD_HTTP_PATH "$DATABRICKS_PRD_HTTP_PATH"
add_one DATABRICKS_PRD_CLIENT_ID "$DATABRICKS_PRD_CLIENT_ID"
add_one DATABRICKS_PRD_CLIENT_SECRET "$DATABRICKS_PRD_CLIENT_SECRET"
add_one DATABRICKS_QAS_HOST "${DATABRICKS_QAS_HOST:-}"
add_one DATABRICKS_QAS_HTTP_PATH "${DATABRICKS_QAS_HTTP_PATH:-}"
add_one DATABRICKS_QAS_TOKEN "${DATABRICKS_QAS_TOKEN:-}"
add_one SUPABASE_SECONDARY_URL "${SUPABASE_SECONDARY_URL:-}"
add_one SUPABASE_SECONDARY_SERVICE_ROLE_KEY "${SUPABASE_SECONDARY_SERVICE_ROLE_KEY:-}"
add_one SUPABASE_BASE_SOCIO_URL "${SUPABASE_BASE_SOCIO_URL:-}"
add_one SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY "${SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY:-}"
add_one AWS_ACCESS_KEY_ID "$AWS_ACCESS_KEY_ID"
add_one AWS_SECRET_ACCESS_KEY "$AWS_SECRET_ACCESS_KEY"
# S3 checkpoint for last-run (same bucket as Lambda logs; default from template account)
add_one ETL_LOGS_BUCKET "${ETL_LOGS_BUCKET:-patek-philippe-etl-logs-052124708820}"
add_one ETL_SUCCESS_PREFIX "${ETL_SUCCESS_PREFIX:-etl-success}"
echo "OK"
