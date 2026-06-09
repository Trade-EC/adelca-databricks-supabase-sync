# Cuenta AWS 052124708820 (últimos dígitos 8820) — Lambda patek-philippe y stack de sync.
# Uso (en la misma terminal):
#   source scripts/aws-env-cuenta-8820.sh
# Luego: sam deploy, bash scripts/resume-lambda-schedules.sh, aws lambda invoke, etc.

export AWS_PROFILE="${AWS_PROFILE:-david-etl-sync}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export STACK_NAME="${STACK_NAME:-databricks-supabase-sync-prd}"
