#!/usr/bin/env bash
# Ejecuta el batch secuencial Base Socio (4 pipelines) en Lambda patek-philippe.
# Secuencia: grupos → ferreterías → facturas → cartera (materiales excluido).
#
# Uso:
#   source scripts/aws-env-cuenta-8820.sh
#   bash scripts/run-base-socio-batch.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/aws-env-cuenta-8820.sh
source "$REPO_ROOT/scripts/aws-env-cuenta-8820.sh"

FUNCTION_NAME="${LAMBDA_NAME:-patek-philippe}"
OUT="${REPO_ROOT}/.lambda-invoke-base-socio-batch.json"

echo "Invoking domain batch base_socio on ${FUNCTION_NAME}…"
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"domain_batch":"base_socio"}' \
  --cli-read-timeout 900 \
  "$OUT"

python3 -m json.tool "$OUT"
echo ""
echo "Checkpoint S3: s3://\${ETL_LOGS_BUCKET:-patek-philippe-etl-logs-*}/etl-success/base_socio/batch/latest.json"
