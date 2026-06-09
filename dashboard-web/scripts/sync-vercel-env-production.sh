#!/usr/bin/env bash
# Sync Production env from transportistas_sync/.env using the Vercel CLI (same as before).
# Uses the pinned CLI in node_modules — no VERCEL_TOKEN and no "npx vercel" per variable.
#
# Prerequisite: `vercel login` once on this machine (team access to adelca-lineas-transportistas).
#
# Run from repo root:
#   bash dashboard-web/scripts/sync-vercel-env-production.sh
# Or from dashboard-web:
#   npm run vercel:sync-env
#
set -euo pipefail

# `vercel` prefers VERCEL_TOKEN over `vercel login`. A placeholder like "…" in the env
# causes: "Invalid token value ... must not contain: …" — clear it for CLI session auth.
unset VERCEL_TOKEN 2>/dev/null || true

export CI=1
export VERCEL_NONINTERACTIVE=1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DASHBOARD_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCOPE="${VERCEL_SCOPE:-adelca-lineas-transportistas}"

VERCEL_BIN="$DASHBOARD_ROOT/node_modules/.bin/vercel"
if [ ! -x "$VERCEL_BIN" ]; then
  echo "Installing dashboard-web deps (vercel CLI)…"
  (cd "$DASHBOARD_ROOT" && npm install)
fi

set -a
# shellcheck disable=SC1091
source "$REPO_ROOT/transportistas_sync/.env"
set +a

unset VERCEL_TOKEN 2>/dev/null || true

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id 2>/dev/null || true)}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key 2>/dev/null || true)}"
export AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || echo us-east-1)}"
export LAMBDA_NAME="${LAMBDA_NAME:-patek-philippe}"
export ETL_LOGS_BUCKET="${ETL_LOGS_BUCKET:-patek-philippe-etl-logs-052124708820}"
export ETL_SUCCESS_PREFIX="${ETL_SUCCESS_PREFIX:-etl-success}"

vc() {
  local name="$1"
  local value="$2"
  if [ -z "$value" ]; then
    echo "⊘ skip $name (empty)"
    return 0
  fi
  echo "→ $name"
  "$VERCEL_BIN" env add "$name" production \
    --value "$value" \
    --yes --force \
    --scope "$SCOPE" \
    --cwd "$DASHBOARD_ROOT"
}

echo "Using: $VERCEL_BIN"
"$VERCEL_BIN" --version

vc AWS_REGION "$AWS_REGION"
vc LAMBDA_NAME "$LAMBDA_NAME"
vc SUPABASE_URL "$SUPABASE_URL"
vc SUPABASE_SERVICE_ROLE_KEY "$SUPABASE_SERVICE_ROLE_KEY"
vc DATABRICKS_PRD_HOST "$DATABRICKS_PRD_HOST"
vc DATABRICKS_PRD_HTTP_PATH "$DATABRICKS_PRD_HTTP_PATH"
vc DATABRICKS_PRD_CLIENT_ID "$DATABRICKS_PRD_CLIENT_ID"
vc DATABRICKS_PRD_CLIENT_SECRET "$DATABRICKS_PRD_CLIENT_SECRET"
vc DATABRICKS_QAS_HOST "${DATABRICKS_QAS_HOST:-}"
vc DATABRICKS_QAS_HTTP_PATH "${DATABRICKS_QAS_HTTP_PATH:-}"
vc DATABRICKS_QAS_TOKEN "${DATABRICKS_QAS_TOKEN:-}"
vc SUPABASE_SECONDARY_URL "${SUPABASE_SECONDARY_URL:-}"
vc SUPABASE_SECONDARY_SERVICE_ROLE_KEY "${SUPABASE_SECONDARY_SERVICE_ROLE_KEY:-}"
vc SUPABASE_BASE_SOCIO_URL "${SUPABASE_BASE_SOCIO_URL:-}"
vc SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY "${SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY:-}"
vc AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID:-}"
vc AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY:-}"
vc ETL_LOGS_BUCKET "$ETL_LOGS_BUCKET"
vc ETL_SUCCESS_PREFIX "$ETL_SUCCESS_PREFIX"

echo "Done. Redeploy from repo root:"
echo "  cd \"$REPO_ROOT\" && npx vercel deploy --prod --yes --scope \"$SCOPE\""
