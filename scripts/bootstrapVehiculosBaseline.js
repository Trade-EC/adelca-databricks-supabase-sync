/**
 * Snapshot de placas en prod.gldlogistica.db_trade_dim_vehiculos para corte incremental.
 * audit_created_at no sirve (misma fecha de ingesta masiva); el pipeline vehiculos diff contra este baseline.
 *
 * Uso:
 *   node scripts/bootstrapVehiculosBaseline.js
 *   node scripts/bootstrapVehiculosBaseline.js --upload
 *   CUTOVER_DATE=2026-06-09 node scripts/bootstrapVehiculosBaseline.js --upload
 *
 * Local: scripts/data/vehiculos_prd_baseline_placas.json
 * S3 (Lambda): s3://{ETL_LOGS_BUCKET}/etl-success/vehiculos/prd_baseline_placas.json
 */
require("dotenv").config({ path: require("path").join(__dirname, "../transportistas_sync/.env") });

const fs = require("fs");
const path = require("path");

const TABLE = "prod.gldlogistica.db_trade_dim_vehiculos";
const S3_KEY = "etl-success/vehiculos/prd_baseline_placas.json";
const LOCAL_OUT = path.join(__dirname, "data", "vehiculos_prd_baseline_placas.json");
const CUTOVER = process.env.CUTOVER_DATE || "2026-06-09";

function normalizePlate(raw) {
  if (raw == null) return "";
  const compact = String(raw).trim().toUpperCase().replace(/[\s-]/g, "");
  if (!compact) return "";
  const m = compact.match(/^([A-Z]+)([0-9]+)$/);
  if (m) return `${m[1]}-${m[2]}`;
  return compact;
}

async function oauthToken(host) {
  const id = process.env.DATABRICKS_PRD_CLIENT_ID;
  const sec = process.env.DATABRICKS_PRD_CLIENT_SECRET;
  const auth = Buffer.from(`${id}:${sec}`).toString("base64");
  const res = await fetch(`https://${host}/oidc/v1/token`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({ grant_type: "client_credentials", scope: "all-apis" }),
  });
  if (!res.ok) throw new Error(`OAuth ${res.status}: ${(await res.text()).slice(0, 300)}`);
  return (await res.json()).access_token;
}

async function fetchPlacas() {
  const host = process.env.DATABRICKS_PRD_HOST;
  const httpPath = process.env.DATABRICKS_PRD_HTTP_PATH;
  const token = await oauthToken(host);
  const warehouseId = httpPath.split("/").pop();
  let res = await fetch(`https://${host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement: `SELECT DISTINCT placa FROM ${TABLE} WHERE placa IS NOT NULL AND TRIM(placa) <> ''`,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
  });
  let p = await res.json();
  const sid = p.statement_id;
  const t0 = Date.now();
  while (["PENDING", "RUNNING"].includes(p.status?.state || "") && Date.now() - t0 < 240000) {
    await new Promise((r) => setTimeout(r, 2000));
    res = await fetch(`https://${host}/api/2.0/sql/statements/${sid}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    p = await res.json();
  }
  if (p.status?.state !== "SUCCEEDED") {
    throw new Error(JSON.stringify(p.status?.error || p.status).slice(0, 600));
  }
  return (p.result?.data_array || []).map((r) => normalizePlate(r[0])).filter(Boolean);
}

async function main() {
  const upload = process.argv.includes("--upload");
  console.log(`Corte: ${CUTOVER}`);
  console.log(`Origen: ${TABLE}\n`);

  const keys = [...new Set(await fetchPlacas())].sort();
  const payload = {
    cutover_date: CUTOVER,
    updated_at: new Date().toISOString(),
    source_table: TABLE,
    keys,
  };

  fs.mkdirSync(path.dirname(LOCAL_OUT), { recursive: true });
  fs.writeFileSync(LOCAL_OUT, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  console.log(`Local: ${LOCAL_OUT} (${keys.length} placas)`);

  if (upload) {
    const bucket = process.env.ETL_LOGS_BUCKET || "patek-philippe-etl-logs-052124708820";
    const body = `${JSON.stringify(payload, null, 2)}\n`;
    try {
      const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
      const s3 = new S3Client({});
      await s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: S3_KEY,
          Body: body,
          ContentType: "application/json",
        })
      );
    } catch (e) {
      if (e.code !== "MODULE_NOT_FOUND") throw e;
      const { execFileSync } = require("child_process");
      const tmp = path.join(__dirname, ".vehiculos_baseline_upload.json");
      fs.writeFileSync(tmp, body, "utf8");
      execFileSync(
        "aws",
        ["s3", "cp", tmp, `s3://${bucket}/${S3_KEY}`, "--content-type", "application/json"],
        { stdio: "inherit" }
      );
      fs.unlinkSync(tmp);
    }
    console.log(`S3: s3://${bucket}/${S3_KEY}`);
  } else {
    console.log("\nPase --upload para publicar el baseline que usa la Lambda.");
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
