/**
 * Compara nombre_conductor (Databricks fact viajes) vs driver (Supabase ct_freights)
 * unidos por codigo_viaje = freight_code.
 *
 * Uso: node scripts/compare_viajes_driver_ct_freights.js
 * Carga env desde transportistas_sync/.env (mismo patrón que el dashboard).
 */
const path = require("path");
const fs = require("fs");
require("dotenv").config({ path: path.join(__dirname, "..", "transportistas_sync", ".env") });

const DBX_TABLE = "qas.gldlogistica.db_trade_fact_transportistas_viajes";
const WAIT = "50s";

function normDriver(v) {
  if (v == null) return null;
  const s = String(v).trim().replace(/\s+/g, " ");
  return s === "" ? null : s;
}

async function databricksQueryAll(statement) {
  const host = process.env.DATABRICKS_QAS_HOST;
  const httpPath = process.env.DATABRICKS_QAS_HTTP_PATH;
  const token = process.env.DATABRICKS_QAS_TOKEN;
  if (!host || !httpPath || !token) {
    throw new Error("Faltan DATABRICKS_QAS_HOST, DATABRICKS_QAS_HTTP_PATH, DATABRICKS_QAS_TOKEN");
  }
  const warehouseId = httpPath.split("/").pop();
  const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };

  let res = await fetch(`https://${host}/api/2.0/sql/statements`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: WAIT,
      disposition: "INLINE",
    }),
  });
  if (!res.ok) throw new Error(`Databricks submit ${res.status}: ${(await res.text()).slice(0, 400)}`);
  let payload = await res.json();
  let state = payload.status?.state || "UNKNOWN";
  const statementId = payload.statement_id;
  const started = Date.now();
  while ((state === "PENDING" || state === "RUNNING") && statementId) {
    if (Date.now() - started > 240000) throw new Error(`Databricks timeout, state=${state}`);
    await new Promise((r) => setTimeout(r, 2000));
    res = await fetch(`https://${host}/api/2.0/sql/statements/${statementId}`, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) throw new Error(`Databricks poll ${res.status}`);
    payload = await res.json();
    state = payload.status?.state || "UNKNOWN";
  }
  if (state !== "SUCCEEDED") {
    throw new Error(`Databricks FAILED: ${JSON.stringify(payload.status?.error || state).slice(0, 600)}`);
  }

  const rows = [...(payload.result?.data_array || [])];
  const fetched = new Set([payload.result?.chunk_index ?? 0]);
  const totalChunks = payload.manifest?.total_chunk_count || 1;
  for (let chunkIndex = 1; chunkIndex < totalChunks; chunkIndex += 1) {
    if (fetched.has(chunkIndex)) continue;
    const chunkRes = await fetch(
      `https://${host}/api/2.0/sql/statements/${statementId}/result/chunks/${chunkIndex}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (!chunkRes.ok) throw new Error(`chunk ${chunkRes.status}`);
    const chunkPayload = await chunkRes.json();
    const chunkRows = chunkPayload.result?.data_array || chunkPayload.data_array || [];
    rows.push(...chunkRows);
    fetched.add(chunkPayload.result?.chunk_index ?? chunkPayload.chunk_index ?? chunkIndex);
  }
  let next = payload.result?.next_chunk_internal_link || null;
  while (next) {
    const chunkRes = await fetch(`https://${host}${next}`, { headers: { Authorization: `Bearer ${token}` } });
    if (!chunkRes.ok) throw new Error(`chain chunk ${chunkRes.status}`);
    const chunkPayload = await chunkRes.json();
    const ci = chunkPayload.result?.chunk_index ?? chunkPayload.chunk_index ?? -1;
    if (!fetched.has(ci)) {
      rows.push(...(chunkPayload.result?.data_array || chunkPayload.data_array || []));
      fetched.add(ci);
    }
    next = chunkPayload.result?.next_chunk_internal_link || null;
  }
  return rows;
}

async function supabaseFetchAllFreightDrivers() {
  const url = (process.env.SUPABASE_SECONDARY_URL || "").replace(/\/$/, "");
  const key = process.env.SUPABASE_SECONDARY_SERVICE_ROLE_KEY || "";
  if (!url || !key) {
    throw new Error("Faltan SUPABASE_SECONDARY_URL o SUPABASE_SECONDARY_SERVICE_ROLE_KEY");
  }
  const h = { apikey: key, Authorization: `Bearer ${key}` };
  const out = [];
  const batch = 1000;
  let from = 0;
  while (true) {
    const to = from + batch - 1;
    const res = await fetch(`${url}/rest/v1/ct_freights?select=freight_code,driver&order=freight_code.asc`, {
      headers: { ...h, Range: `${from}-${to}` },
      cache: "no-store",
    });
    if (![200, 206].includes(res.status)) {
      throw new Error(`Supabase ${res.status}: ${(await res.text()).slice(0, 300)}`);
    }
    const chunk = await res.json();
    if (!chunk.length) break;
    for (const row of chunk) out.push(row);
    if (chunk.length < batch) break;
    from += batch;
  }
  return out;
}

async function main() {
  console.log("Databricks:", DBX_TABLE, "(nombre_conductor)");
  console.log("Supabase:  ct_freights (driver), join freight_code = codigo_viaje\n");

  const dbxRows = await databricksQueryAll(
    `SELECT codigo_viaje, nombre_conductor FROM ${DBX_TABLE}`
  );
  /** Map: última fila gana por codigo_viaje (alineado con dedupe del ETL por codigo_viaje). */
  const byCode = new Map();
  for (const [code, nombre] of dbxRows) {
    const k = code != null ? String(code).trim() : "";
    if (k) byCode.set(k, nombre);
  }
  console.log("Filas Databricks (array):", dbxRows.length);
  console.log("Códigos únicos (codigo_viaje):", byCode.size);

  const supRows = await supabaseFetchAllFreightDrivers();
  console.log("Filas Supabase ct_freights:", supRows.length);

  let match = 0;
  let mismatch = 0;
  let bothNull = 0;
  let dbxNullSupVal = 0;
  let dbxValSupNull = 0;
  let bothValDiff = 0;
  const missingInDbx = [];
  const samples = [];

  const supKeys = new Set();
  for (const { freight_code: fc, driver } of supRows) {
    const code = fc != null ? String(fc).trim() : "";
    if (!code) continue;
    supKeys.add(code);
    if (!byCode.has(code)) {
      missingInDbx.push(code);
      continue;
    }
    const n = normDriver(byCode.get(code));
    const d = normDriver(driver);
    if (n === null && d === null) {
      bothNull += 1;
      match += 1;
      continue;
    }
    if (n === null && d !== null) {
      dbxNullSupVal += 1;
      mismatch += 1;
      if (samples.length < 15) samples.push({ code, databricks: n, supabase: d, kind: "dbx_null_sup_value" });
      continue;
    }
    if (n !== null && d === null) {
      dbxValSupNull += 1;
      mismatch += 1;
      if (samples.length < 15) samples.push({ code, databricks: n, supabase: d, kind: "dbx_value_sup_null" });
      continue;
    }
    if (n === d) {
      match += 1;
      continue;
    }
    bothValDiff += 1;
    mismatch += 1;
    if (samples.length < 15) samples.push({ code, databricks: n, supabase: d, kind: "both_strings_differ" });
  }

  let inDbxNotSup = 0;
  for (const k of byCode.keys()) {
    if (!supKeys.has(k)) inDbxNotSup += 1;
  }

  console.log("\n--- Resultado comparación (normalizado trim + espacios) ---");
  console.log(JSON.stringify({
    supabase_rows_used: supRows.length,
    databricks_unique_codes: byCode.size,
    coinciden_match: match,
    no_coinciden_mismatch: mismatch,
    detalle_mismatch: {
      ambos_null_cuenta_como_match: bothNull,
      databricks_null_supabase_con_valor: dbxNullSupVal,
      databricks_con_valor_supabase_null: dbxValSupNull,
      ambos_con_valor_texto_distinto: bothValDiff,
    },
    freight_code_en_supabase_no_en_databricks: missingInDbx.length,
    codigo_viaje_en_databricks_no_en_supabase: inDbxNotSup,
    muestras_discrepancia: samples,
  }, null, 2));

  if (missingInDbx.length) {
    console.log("\nPrimeros freight_code sin match en Databricks:", missingInDbx.slice(0, 10));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
