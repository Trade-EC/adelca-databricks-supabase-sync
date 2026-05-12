#!/usr/bin/env bash
# Production deploy: Vercel project has Root Directory = dashboard-web, so the CLI
# must run from the git repo root (not from this folder) or it resolves …/dashboard-web/dashboard-web.
set -euo pipefail
DASHBOARD_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
REPO_ROOT="$(cd "$DASHBOARD_ROOT/.." && pwd -P)"
mkdir -p "$REPO_ROOT/.vercel"
cp "$DASHBOARD_ROOT/.vercel/project.json" "$REPO_ROOT/.vercel/project.json"
cd "$REPO_ROOT"
exec npx vercel deploy --prod --yes
