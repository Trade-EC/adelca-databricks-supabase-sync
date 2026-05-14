#!/usr/bin/env bash
# Pause EventBridge rules that invoke the sync Lambda (no redeploy).
# Prereq: AWS CLI, same account/region as the stack.
#
#   export AWS_REGION=us-east-1
#   export STACK_NAME=databricks-supabase-sync-prd   # default below
#   bash scripts/pause-lambda-schedules.sh
#
# Optional: export LAMBDA_NAME=patek-philippe  (default) if the function name differs.

set -euo pipefail
STACK_NAME="${STACK_NAME:-databricks-supabase-sync-prd}"
REGION="${AWS_REGION:-us-east-1}"
LAMBDA_NAME="${LAMBDA_NAME:-patek-philippe}"

PHYSICAL="$(aws cloudformation describe-stack-resources \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "StackResources[?LogicalResourceId=='SyncFunction'].PhysicalResourceId" \
  --output text 2>/dev/null || true)"

if [[ -z "$PHYSICAL" || "$PHYSICAL" == "None" ]]; then
  echo "Stack '$STACK_NAME' not found in $REGION (or no SyncFunction). Set STACK_NAME / AWS_REGION."
  exit 1
fi

ARN="$(aws lambda get-function --function-name "$PHYSICAL" --region "$REGION" --query 'Configuration.FunctionArn' --output text)"
RULES="$(aws events list-rule-names-by-target --target-arn "$ARN" --region "$REGION" --query 'RuleNames' --output text | tr '\t' '\n' | sed '/^$/d' || true)"

if [[ -z "$RULES" ]]; then
  echo "No EventBridge rules target $ARN — nothing to disable."
  exit 0
fi

while IFS= read -r rule; do
  [[ -z "$rule" ]] && continue
  aws events disable-rule --name "$rule" --region "$REGION"
  echo "Disabled: $rule"
done <<< "$RULES"

echo "Done. Invocación manual de la Lambda sigue permitida."
