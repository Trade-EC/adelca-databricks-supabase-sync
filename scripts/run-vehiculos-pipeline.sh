#!/usr/bin/env bash
# Invoca pipeline vehiculos (PRD dim → default.vehicles, insert_only + baseline S3).
#
# Pre-requisitos (una vez):
#   node scripts/bootstrapVehiculosBaseline.js --upload
#   ./deploy.sh
#
# Dry-run local (sin inserts):
#   node scripts/evaluateVehiculosSnapshot.js
#
# Uso:
#   source scripts/aws-env-cuenta-8820.sh
#   bash scripts/run-vehiculos-pipeline.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/aws-env-cuenta-8820.sh
source "$REPO_ROOT/scripts/aws-env-cuenta-8820.sh"

PIPELINE="vehiculos"
FUNCTION_NAME="${LAMBDA_NAME:-patek-philippe}"
OUT="${REPO_ROOT}/.lambda-invoke-${PIPELINE}.json"

echo "Invoking ${PIPELINE} on ${FUNCTION_NAME}…"
echo "  PRD → default.vehicles | insert_only | baseline S3 etl-success/vehiculos/prd_baseline_placas.json"
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"pipeline_name\":\"${PIPELINE}\"}" \
  --cli-read-timeout 900 \
  "$OUT"

python3 -m json.tool "$OUT"
