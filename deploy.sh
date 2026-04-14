#!/bin/bash
set -euo pipefail

# ============================================================
# Deploy Databricks PRD → Supabase sync Lambda via SAM
# ============================================================
#
# Prerequisites:
#   1. AWS CLI configured:  aws configure
#   2. SAM CLI installed:   pip install aws-sam-cli
#   3. Docker running (for sam build --use-container)
#
# Usage:
#   First deploy (interactive):  ./deploy.sh --guided
#   Subsequent deploys:          ./deploy.sh
#
# IMPORTANT: Before first deploy, update samconfig.toml:
#   - DatabricksClientSecret=<your actual secret>
#   - SupabaseServiceRoleKey=<your actual key>
# ============================================================

FUNCTION_NAME="databricks-supabase-sync-prd"
GUIDED=""
if [[ "${1:-}" == "--guided" ]]; then
    GUIDED="--guided"
fi

echo "=== Building Lambda package ==="
sam build --use-container

echo ""
echo "=== Deploying to AWS ==="
if [[ -n "$GUIDED" ]]; then
    sam deploy --guided
else
    sam deploy
fi

echo ""
echo "=== Done ==="
echo "Check logs:  sam logs -n ${FUNCTION_NAME} --tail"
echo "Test now:    aws lambda invoke --function-name ${FUNCTION_NAME} /dev/stdout"
