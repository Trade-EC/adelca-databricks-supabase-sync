#!/usr/bin/env bash
# Re-enable EventBridge rules that invoke the sync Lambda (pair of pause-lambda-schedules.sh).
#
#   export AWS_REGION=us-east-1
#   export STACK_NAME=databricks-supabase-sync-prd
#   bash scripts/resume-lambda-schedules.sh

set -euo pipefail
STACK_NAME="${STACK_NAME:-databricks-supabase-sync-prd}"
REGION="${AWS_REGION:-us-east-1}"

PHYSICAL="$(aws cloudformation describe-stack-resources \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "StackResources[?LogicalResourceId=='SyncFunction'].PhysicalResourceId" \
  --output text 2>/dev/null || true)"

if [[ -z "$PHYSICAL" || "$PHYSICAL" == "None" ]]; then
  echo "Stack '$STACK_NAME' not found in $REGION."
  exit 1
fi

ARN="$(aws lambda get-function --function-name "$PHYSICAL" --region "$REGION" --query 'Configuration.FunctionArn' --output text)"
RULES="$(aws events list-rule-names-by-target --target-arn "$ARN" --region "$REGION" --query 'RuleNames' --output text | tr '\t' '\n' | sed '/^$/d' || true)"

if [[ -z "$RULES" ]]; then
  echo "No rules found for $ARN."
  exit 0
fi

while IFS= read -r rule; do
  [[ -z "$rule" ]] && continue
  aws events enable-rule --name "$rule" --region "$REGION"
  echo "Enabled: $rule"
done <<< "$RULES"

echo "Done."
