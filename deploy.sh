#!/bin/bash
set -euo pipefail

# ============================================================
# Deploy Databricks → Supabase sync Lambda via SAM
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
# ============================================================

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
echo "Check logs:  sam logs -n databricks-supabase-sync --tail"
echo "Test now:    aws lambda invoke --function-name databricks-supabase-sync /dev/stdout"
