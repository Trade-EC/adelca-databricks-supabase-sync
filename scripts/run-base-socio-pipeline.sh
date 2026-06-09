#!/usr/bin/env bash
# Invoca un pipeline Base Socio individual en Lambda (prueba tabla a tabla).
#
# Uso:
#   source scripts/aws-env-cuenta-8820.sh
#   bash scripts/run-base-socio-pipeline.sh socio_adelca_grupos
#
set -euo pipefail

PIPELINE="${1:?pipeline_name required (e.g. socio_adelca_grupos)}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/aws-env-cuenta-8820.sh
source "$REPO_ROOT/scripts/aws-env-cuenta-8820.sh"

FUNCTION_NAME="${LAMBDA_NAME:-patek-philippe}"
OUT="${REPO_ROOT}/.lambda-invoke-${PIPELINE}.json"

echo "Invoking pipeline ${PIPELINE} on ${FUNCTION_NAME}…"
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"pipeline_name\":\"${PIPELINE}\"}" \
  --cli-read-timeout 900 \
  "$OUT"

python3 -m json.tool "$OUT"
