#!/usr/bin/env node
/**
 * Push env vars to Vercel Production via REST API (no npx / no CLI hang).
 *
 *   cd dashboard-web
 *   export VERCEL_TOKEN=...   # https://vercel.com/account/tokens (scope: full account or team)
 *   npm run vercel:push-env
 *
 * Loads secrets from ../../transportistas_sync/.env (same as the bash sync script).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dashboardRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(dashboardRoot, "..");

dotenv.config({ path: path.join(repoRoot, "transportistas_sync", ".env") });

const projectJsonPath = path.join(dashboardRoot, ".vercel", "project.json");
if (!fs.existsSync(projectJsonPath)) {
  console.error("Missing dashboard-web/.vercel/project.json — run `vercel link` in dashboard-web.");
  process.exit(1);
}

const { projectId: PROJECT_ID, orgId: TEAM_ID } = JSON.parse(
  fs.readFileSync(projectJsonPath, "utf8")
);

const TOKEN = process.env.VERCEL_TOKEN;
if (!TOKEN) {
  console.error(
    "Set VERCEL_TOKEN (create at https://vercel.com/account/tokens), then:\n  npm run vercel:push-env"
  );
  process.exit(1);
}

/** Same keys as scripts/sync-vercel-env-production.sh — order preserved */
const KEYS = [
  "AWS_REGION",
  "LAMBDA_NAME",
  "SUPABASE_URL",
  "SUPABASE_SERVICE_ROLE_KEY",
  "DATABRICKS_PRD_HOST",
  "DATABRICKS_PRD_HTTP_PATH",
  "DATABRICKS_PRD_CLIENT_ID",
  "DATABRICKS_PRD_CLIENT_SECRET",
  "DATABRICKS_QAS_HOST",
  "DATABRICKS_QAS_HTTP_PATH",
  "DATABRICKS_QAS_TOKEN",
  "SUPABASE_SECONDARY_URL",
  "SUPABASE_SECONDARY_SERVICE_ROLE_KEY",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "ETL_LOGS_BUCKET",
  "ETL_SUCCESS_PREFIX",
];

function applyDefaults() {
  if (!process.env.AWS_REGION) process.env.AWS_REGION = "us-east-1";
  if (!process.env.LAMBDA_NAME) process.env.LAMBDA_NAME = "patek-philippe";
  if (!process.env.ETL_LOGS_BUCKET) {
    process.env.ETL_LOGS_BUCKET = "patek-philippe-etl-logs-052124708820";
  }
  if (!process.env.ETL_SUCCESS_PREFIX) process.env.ETL_SUCCESS_PREFIX = "etl-success";
}

async function api(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = text;
  }
  if (!res.ok) {
    const snippet =
      typeof data === "string" ? data.slice(0, 400) : JSON.stringify(data).slice(0, 400);
    throw new Error(`${method} ${url} → ${res.status}: ${snippet}`);
  }
  return data;
}

async function listEnvs() {
  const url = `https://api.vercel.com/v9/projects/${PROJECT_ID}/env?teamId=${encodeURIComponent(TEAM_ID)}`;
  return api("GET", url);
}

async function deleteEnv(envId) {
  const url = `https://api.vercel.com/v9/projects/${PROJECT_ID}/env/${envId}?teamId=${encodeURIComponent(TEAM_ID)}`;
  return api("DELETE", url);
}

async function createEnv(key, value) {
  const url = `https://api.vercel.com/v9/projects/${PROJECT_ID}/env?teamId=${encodeURIComponent(TEAM_ID)}`;
  return api("POST", url, {
    key,
    value,
    type: "encrypted",
    target: ["production"],
  });
}

function targetsProduction(entry) {
  const t = entry.target;
  if (!t) return false;
  return Array.isArray(t) ? t.includes("production") : t === "production";
}

applyDefaults();

console.log(`Project ${PROJECT_ID} (team ${TEAM_ID})`);
console.log("Listing existing Production env keys…");

const listed = await listEnvs();
const envs = listed.envs || [];

for (const key of KEYS) {
  const value = process.env[key];
  if (value === undefined || value === "") {
    console.log(`⊘ skip ${key} (empty)`);
    continue;
  }

  const duplicates = envs.filter((e) => e.key === key && targetsProduction(e));
  for (const e of duplicates) {
    console.log(`  remove old ${key} (${e.id})`);
    await deleteEnv(e.id);
  }

  console.log(`→ set ${key}`);
  await createEnv(key, value);
}

console.log("\nDone. Redeploy production from repo root:");
console.log(
  `  cd "${repoRoot}" && npx vercel deploy --prod --yes --scope adelca-lineas-transportistas`
);
