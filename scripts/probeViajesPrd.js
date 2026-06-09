/**
 * Verifica acceso OAuth PRD a prod.gldlogistica.db_trade_fact_transportistas_viajes
 * y que existan las columnas del pipeline `viajes`.
 *
 * Uso: node scripts/probeViajesPrd.js
 */
require("dotenv").config({ path: require("path").join(__dirname, "../transportistas_sync/.env") });

const TABLE = "prod.gldlogistica.db_trade_fact_transportistas_viajes";
const PIPELINE_COLS = [
  "codigo_viaje",
  "fecha",
  "codigo_unico_transportista",
  "distancia_km",
  "placa",
  "peso_ton",
  "valor_neto",
  "nombre_conductor",
  "texto_condicion_expedicion_viaje",
  "RUTA",
];

async function oauthToken(host) {
  const id = process.env.DATABRICKS_PRD_CLIENT_ID;
  const sec = process.env.DATABRICKS_PRD_CLIENT_SECRET;
  if (!host || !id || !sec) {
    throw new Error("Faltan DATABRICKS_PRD_HOST, DATABRICKS_PRD_CLIENT_ID, DATABRICKS_PRD_CLIENT_SECRET");
  }
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

async function runSql(statement) {
  const host = process.env.DATABRICKS_PRD_HOST;
  const httpPath = process.env.DATABRICKS_PRD_HTTP_PATH;
  if (!httpPath) throw new Error("Falta DATABRICKS_PRD_HTTP_PATH");
  const warehouseId = httpPath.split("/").pop();
  const token = await oauthToken(host);

  let res = await fetch(`https://${host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
  });
  if (!res.ok) throw new Error(`Submit ${res.status}: ${(await res.text()).slice(0, 400)}`);
  let p = await res.json();
  const sid = p.statement_id;
  const t0 = Date.now();
  while (["PENDING", "RUNNING"].includes(p.status?.state || "") && Date.now() - t0 < 180000) {
    await new Promise((r) => setTimeout(r, 2000));
    res = await fetch(`https://${host}/api/2.0/sql/statements/${sid}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    p = await res.json();
  }
  if (p.status?.state !== "SUCCEEDED") {
    throw new Error(JSON.stringify(p.status?.error || p.status).slice(0, 600));
  }
  return p.result?.data_array || [];
}

async function main() {
  console.log(`Tabla: ${TABLE}`);
  console.log(`Host PRD: ${process.env.DATABRICKS_PRD_HOST || "(no set)"}\n`);

  const [row] = await runSql(`
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT codigo_viaje) AS distinct_codigo,
  MIN(fecha) AS min_fecha,
  MAX(fecha) AS max_fecha,
  SUM(CASE WHEN codigo_viaje IS NULL OR TRIM(codigo_viaje) = '' THEN 1 ELSE 0 END) AS null_codigo
FROM ${TABLE}
`);
  const [rows, distinct, minF, maxF, nullCod] = row;

  const desc = await runSql(`DESCRIBE TABLE ${TABLE}`);
  const cols = new Set(desc.map((r) => r[0]));
  const missing = PIPELINE_COLS.filter((c) => !cols.has(c));

  console.log({
    rows: Number(rows),
    distinct_codigo_viaje: Number(distinct),
    fecha_min: minF,
    fecha_max: maxF,
    filas_sin_codigo_viaje: Number(nullCod),
    columnas_pipeline_ok: missing.length === 0,
    columnas_faltantes: missing,
  });

  if (missing.length) process.exit(1);
  console.log("\nConexión PRD OK — columnas del pipeline viajes presentes.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
