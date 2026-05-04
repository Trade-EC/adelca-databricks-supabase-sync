#!/usr/bin/env node
/**
 * Push env vars to Vercel Production via REST API (no npx / no CLI hang).
 *
 *   cd dashboard-web
 *   export VERCEL_TOKEN="<paste token>"   # https://vercel.com/account/tokens
 *   npm run vercel:push-env
 *
 * Prefer `npm run vercel:sync-env` if you use `vercel login` (no token; same as before).
 * Optional: VERCEL_TEAM_ID=team_xxx  (override team from .vercel/project.json if 403)
 *
 * Loads secrets from ../../transportistas_sync/.env; merges AWS keys from `aws configure`
 * when missing in .env (same behaviour as the old bash script).
 */
import { execSync } from "child_process";
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

const projectJson = JSON.parse(fs.readFileSync(projectJsonPath, "utf8"));
const PROJECT_ID = projectJson.projectId;
/** Prefer explicit override if token only sees one team / 403 with linked project orgId */
const TEAM_ID = process.env.VERCEL_TEAM_ID || projectJson.orgId;

const TOKEN = process.env.VERCEL_TOKEN;
if (!TOKEN) {
  console.error(
    "Set VERCEL_TOKEN (create at https://vercel.com/account/tokens), then:\n  npm run vercel:push-env"
  );
  process.exit(1);
}

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

function awsConfigureGet(name) {
  try {
    return execSync(`aws configure get ${name}`, { encoding: "utf8" }).trim() || "";
  } catch {
    return "";
  }
}

function applyDefaults() {
  if (!process.env.AWS_REGION) {
    process.env.AWS_REGION = awsConfigureGet("region") || "us-east-1";
  }
  if (!process.env.LAMBDA_NAME) process.env.LAMBDA_NAME = "patek-philippe";
  if (!process.env.ETL_LOGS_BUCKET) {
    process.env.ETL_LOGS_BUCKET = "patek-philippe-etl-logs-052124708820";
  }
  if (!process.env.ETL_SUCCESS_PREFIX) process.env.ETL_SUCCESS_PREFIX = "etl-success";
  if (!process.env.AWS_ACCESS_KEY_ID) {
    process.env.AWS_ACCESS_KEY_ID = awsConfigureGet("aws_access_key_id");
  }
  if (!process.env.AWS_SECRET_ACCESS_KEY) {
    process.env.AWS_SECRET_ACCESS_KEY = awsConfigureGet("aws_secret_access_key");
  }
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
      typeof data === "string" ? data.slice(0, 500) : JSON.stringify(data).slice(0, 500);
    const err = new Error(`${method} → ${res.status}: ${snippet}`);
    err.status = res.status;
    err.body = data;
    throw err;
  }
  return data;
}

async function listAllEnvs() {
  const q = new URLSearchParams({ teamId: TEAM_ID, decrypt: "false" });
  const url = `https://api.vercel.com/v9/projects/${encodeURIComponent(PROJECT_ID)}/env?${q}`;
  const data = await api("GET", url);
  return data.envs || [];
}

async function deleteEnv(envId) {
  const url = `https://api.vercel.com/v9/projects/${encodeURIComponent(PROJECT_ID)}/env/${encodeURIComponent(envId)}?teamId=${encodeURIComponent(TEAM_ID)}`;
  return api("DELETE", url);
}

async function createEnv(key, value) {
  const url = `https://api.vercel.com/v9/projects/${encodeURIComponent(PROJECT_ID)}/env?teamId=${encodeURIComponent(TEAM_ID)}`;
  return api("POST", url, {
    key,
    value,
    type: "encrypted",
    target: ["production"],
  });
}

async function patchEnv(envId, key, value) {
  const url = `https://api.vercel.com/v9/projects/${encodeURIComponent(PROJECT_ID)}/env/${encodeURIComponent(envId)}?teamId=${encodeURIComponent(TEAM_ID)}`;
  return api("PATCH", url, {
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

function hint403(err) {
  const body = err.body;
  const invalid =
    body &&
    typeof body === "object" &&
    body.error?.invalidToken === true;
  console.error("\n---");
  if (err.status === 403 || invalid) {
    console.error(
      "Token inválido o sin acceso al equipo del proyecto. Prueba:\n" +
        "  - Crear un token nuevo en https://vercel.com/account/tokens (Full Account).\n" +
        "  - Asegurarte de que tu usuario pertenezca al equipo del proyecto.\n" +
        "  - Si sigue 403: export VERCEL_TEAM_ID=team_… (ID del equipo en Vercel → Settings → Team ID)."
    );
  }
  console.error("---\n");
}

async function main() {
  applyDefaults();

  console.log(`Project ${PROJECT_ID} (team ${TEAM_ID})`);
  console.log("Listing env vars…");

  let envs;
  try {
    envs = await listAllEnvs();
  } catch (e) {
    hint403(e);
    throw e;
  }

  for (const key of KEYS) {
    const value = process.env[key];
    if (value === undefined || value === "") {
      console.log(`⊘ skip ${key} (empty)`);
      continue;
    }

    const matches = envs.filter((e) => e.key === key && targetsProduction(e));
    if (matches.length > 1) {
      for (const extra of matches.slice(1)) {
        console.log(`  dedupe ${key} (${extra.id})`);
        await deleteEnv(extra.id);
      }
    }

    const existing = matches[0];
    if (existing) {
      console.log(`↻ patch ${key}`);
      await patchEnv(existing.id, key, value);
    } else {
      console.log(`→ create ${key}`);
      await createEnv(key, value);
    }
  }

  console.log("\nDone. Redeploy production from repo root:");
  console.log(
    `  cd "${repoRoot}" && npx vercel deploy --prod --yes --scope adelca-lineas-transportistas`
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
