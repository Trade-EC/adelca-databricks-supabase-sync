#!/usr/bin/env bash
# Push Production env vars to Vercel — REST API (no npx hang).
#
# 1. Create a token: https://vercel.com/account/tokens
# 2. From dashboard-web:
#      export VERCEL_TOKEN=...
#      npm run vercel:push-env
#
# Legacy CLI loop removed (was hanging on repeated npx vercel calls).

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec node scripts/push-vercel-env.mjs
