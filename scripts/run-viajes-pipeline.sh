#!/usr/bin/env bash
# Invoca pipeline viajes (PRD fact → default.ct_freights, insert_only + baseline S3).
#
# Pre-requisitos (una vez, post carga histórica):
#   node scripts/bootstrapViajesBaseline.js --upload
#   ./deploy.sh
#
# Uso:
#   source scripts/aws-env-cuenta-8820.sh
#   bash scripts/run-viajes-pipeline.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/aws-env-cuenta-8820.sh
source "$REPO_ROOT/scripts/aws-env-cuenta-8820.sh"

PIPELINE="viajes"
FUNCTION_NAME="${LAMBDA_NAME:-patek-philippe}"
OUT="${REPO_ROOT}/.lambda-invoke-${PIPELINE}.json"

echo "Invoking ${PIPELINE} on ${FUNCTION_NAME}…"
echo "  PRD → default.ct_freights | insert_only | baseline S3 etl-success/viajes/prd_baseline_codigo_viaje.json"
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"pipeline_name\":\"${PIPELINE}\"}" \
  --cli-read-timeout 900 \
  "$OUT"

python3 -m json.tool "$OUT"
