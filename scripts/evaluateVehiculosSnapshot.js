/**
 * Dry-run del pipeline `vehiculos`: simula qué insertaría la Lambda sin escribir en Supabase.
 *
 * Lógica (igual que handler + pipelines.json):
 *   1. PRD prod.gldlogistica.db_trade_dim_vehiculos
 *   2. Filtro baseline S3/local: placa NO estaba en el corte (2026-06-09)
 *   3. insert_only: placa NO está en default public.vehicles (rqlzsdziohmqanalmgyx)
 *
 * Uso:
 *   node scripts/evaluateVehiculosSnapshot.js
 *   node scripts/evaluateVehiculosSnapshot.js --s3
 *   node scripts/evaluateVehiculosSnapshot.js --out scripts/data/vehiculos_would_insert.csv
 *
 * Env: transportistas_sync/.env
 *   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (default)
 *   DATABRICKS_PRD_*, ETL_LOGS_BUCKET (solo con --s3 o baseline local ausente)
 */
require("dotenv").config({ path: require("path").join(__dirname, "../transportistas_sync/.env") });

const fs = require("fs");
const path = require("path");

const PROD_TABLE = "prod.gldlogistica.db_trade_dim_vehiculos";
const BASELINE_S3_KEY = "etl-success/vehiculos/prd_baseline_placas.json";
const BASELINE_LOCAL = path.join(__dirname, "data", "vehiculos_prd_baseline_placas.json");
const COLS = ["placa", "tipo_transporte", "capacidad_ton", "codigo_transportista", "flg_rutero"];

function normalizePlate(raw) {
  if (raw == null) return "";
  const compact = String(raw).trim().toUpperCase().replace(/[\s-]/g, "");
  if (!compact) return "";
  const m = compact.match(/^([A-Z]+)([0-9]+)$/);
  if (m) return `${m[1]}-${m[2]}`;
  return compact;
}

function normBoolRutero(v) {
  return v === true || v === 1 || String(v).trim() === "1";
}

function prodSnapshot(rec) {
  return {
    license_plate: rec.placa_norm,
    type: rec.tipo_transporte ?? null,
    weight_average_capacity: rec.capacidad_ton ?? 0,
    fleet_owner_unique_code: rec.codigo_transportista ?? "",
    is_route_based: normBoolRutero(rec.flg_rutero),
  };
}

function sbSnapshot(row) {
  return {
    license_plate: normalizePlate(row.license_plate),
    type: row.type ?? null,
    weight_average_capacity: row.weight_average_capacity ?? 0,
    fleet_owner_unique_code: row.fleet_owner_unique_code ?? "",
    is_route_based: !!row.is_route_based,
  };
}

function snapshotEqual(a, b) {
  if (Number(a.weight_average_capacity) !== Number(b.weight_average_capacity)) return false;
  if (Boolean(a.is_route_based) !== Boolean(b.is_route_based)) return false;
  if (String(a.type ?? "") !== String(b.type ?? "")) return false;
  if (String(a.fleet_owner_unique_code ?? "") !== String(b.fleet_owner_unique_code ?? "")) return false;
  return true;
}

function parseArgs(argv) {
  const out = { preferS3: false, outPath: null };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--s3") out.preferS3 = true;
    else if (argv[i] === "--out" && argv[i + 1]) out.outPath = argv[++i];
  }
  return out;
}

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

async function runDatabricksSql(statement) {
  const host = process.env.DATABRICKS_PRD_HOST;
  const httpPath = process.env.DATABRICKS_PRD_HTTP_PATH;
  if (!httpPath) throw new Error("Falta DATABRICKS_PRD_HTTP_PATH");
  const token = await oauthToken(host);
  const warehouseId = httpPath.split("/").pop();

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
  if (!res.ok) throw new Error(`Databricks ${res.status}: ${(await res.text()).slice(0, 400)}`);
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

  const rows = [...(p.result?.data_array || [])];
  let next = p.result?.next_chunk_internal_link;
  while (next) {
    res = await fetch(`https://${host}${next}`, { headers: { Authorization: `Bearer ${token}` } });
    const cp = await res.json();
    rows.push(...(cp.result?.data_array || cp.data_array || []));
    next = cp.result?.next_chunk_internal_link || null;
  }
  return rows;
}

async function fetchProdVehiculos() {
  const sql = `SELECT ${COLS.join(", ")} FROM ${PROD_TABLE} WHERE placa IS NOT NULL AND TRIM(placa) <> ''`;
  const rows = await runDatabricksSql(sql);
  const map = new Map();
  for (const r of rows) {
    const rec = {
      placa_raw: String(r[0]).trim(),
      placa_norm: normalizePlate(r[0]),
      tipo_transporte: r[1],
      capacidad_ton: r[2],
      codigo_transportista: r[3],
      flg_rutero: r[4],
    };
    if (!rec.placa_norm) continue;
    map.set(rec.placa_norm, rec);
  }
  return map;
}

async function fetchDefaultVehicles() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) throw new Error("Faltan SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY");

  const map = new Map();
  let offset = 0;
  while (true) {
    const q = new URL(`${url}/rest/v1/vehicles`);
    q.searchParams.set(
      "select",
      "license_plate,type,weight_average_capacity,fleet_owner_unique_code,is_route_based"
    );
    const res = await fetch(q, {
      headers: { apikey: key, Authorization: `Bearer ${key}`, Range: `${offset}-${offset + 999}` },
    });
    if (!res.ok) throw new Error(`Supabase vehicles ${res.status}: ${(await res.text()).slice(0, 400)}`);
    const batch = await res.json();
    if (!batch.length) break;
    for (const row of batch) {
      const pk = normalizePlate(row.license_plate);
      if (pk) map.set(pk, row);
    }
    if (batch.length < 1000) break;
    offset += 1000;
  }
  return { host: new URL(url).hostname, map };
}

async function loadBaseline(preferS3) {
  const tryLocal = () => {
    if (!fs.existsSync(BASELINE_LOCAL)) return null;
    const payload = JSON.parse(fs.readFileSync(BASELINE_LOCAL, "utf8"));
    if (!Array.isArray(payload.keys)) return null;
    return { source: BASELINE_LOCAL, payload, keys: new Set(payload.keys.map(String)) };
  };

  const tryS3 = async () => {
    const bucket = process.env.ETL_LOGS_BUCKET;
    if (!bucket) return null;
    const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
    const s3 = new S3Client({});
    try {
      const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: BASELINE_S3_KEY }));
      const payload = JSON.parse(await res.Body.transformToString());
      if (!Array.isArray(payload.keys)) return null;
      return {
        source: `s3://${bucket}/${BASELINE_S3_KEY}`,
        payload,
        keys: new Set(payload.keys.map(String)),
      };
    } catch (e) {
      if (e.name === "NoSuchKey" || e.$metadata?.httpStatusCode === 404) return null;
      throw e;
    }
  };

  if (preferS3) {
    const s3 = await tryS3();
    if (s3) return s3;
    return tryLocal();
  }
  const local = tryLocal();
  if (local) return local;
  return tryS3();
}

function escCsv(v) {
  const s = String(v ?? "");
  return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const { preferS3, outPath } = parseArgs(process.argv);

  console.log("=== evaluateVehiculosSnapshot (dry-run, sin inserts) ===\n");
  console.log("Origen PRD:", PROD_TABLE);

  const baseline = await loadBaseline(preferS3);
  if (!baseline) {
    console.error(
      "\nERROR: baseline no encontrado (local ni S3).\n" +
        "  node scripts/bootstrapVehiculosBaseline.js\n" +
        "  node scripts/bootstrapVehiculosBaseline.js --upload\n"
    );
    process.exit(1);
  }

  const [{ host: sbHost, map: defaultMap }, prodMap] = await Promise.all([
    fetchDefaultVehicles(),
    fetchProdVehiculos(),
  ]);

  console.log("Destino:", `default ${sbHost} → public.vehicles`);
  console.log("Baseline:", baseline.source, `(${baseline.keys.size} placas, corte ${baseline.payload.cutover_date || "?"})\n`);

  const wouldInsert = [];
  const newInProdAlreadyInDefault = [];
  const inBaselineInDefaultOk = [];
  const inBaselineInDefaultChanged = [];
  const inBaselineNotInDefault = [];
  const onlyInDefault = [];

  for (const [pk, prodRec] of prodMap) {
    const inBaseline = baseline.keys.has(pk);
    const sbRow = defaultMap.get(pk);
    const prodSnap = prodSnapshot(prodRec);

    if (!inBaseline) {
      if (!sbRow) {
        wouldInsert.push({ ...prodSnap, placa_raw: prodRec.placa_raw, accion: "INSERT" });
      } else {
        newInProdAlreadyInDefault.push({ placa: pk, prod: prodSnap, default: sbSnapshot(sbRow) });
      }
      continue;
    }

    if (sbRow) {
      const sbSnap = sbSnapshot(sbRow);
      if (snapshotEqual(prodSnap, sbSnap)) inBaselineInDefaultOk.push(pk);
      else inBaselineInDefaultChanged.push({ placa: pk, prod: prodSnap, default: sbSnap });
    } else {
      inBaselineNotInDefault.push(pk);
    }
  }

  for (const [pk] of defaultMap) {
    if (!prodMap.has(pk)) onlyInDefault.push(pk);
  }

  const naiveInsertOnly = [...prodMap.keys()].filter((pk) => !defaultMap.has(pk)).length;

  const summary = {
    prod_placas: prodMap.size,
    default_vehicles: defaultMap.size,
    baseline_placas: baseline.keys.size,
    filtro1_nuevas_en_prod_desde_baseline: wouldInsert.length + newInProdAlreadyInDefault.length,
    lambda_insertaria: wouldInsert.length,
    nuevas_en_prod_ya_en_default: newInProdAlreadyInDefault.length,
    en_baseline_y_default_ok: inBaselineInDefaultOk.length,
    en_baseline_y_default_datos_distintos: inBaselineInDefaultChanged.length,
    en_baseline_no_en_default_sin_accion: inBaselineNotInDefault.length,
    solo_en_default_no_en_prod: onlyInDefault.length,
    riesgo_si_corrieras_sin_baseline: naiveInsertOnly,
  };

  console.log("Resumen:");
  console.log(JSON.stringify(summary, null, 2));

  if (wouldInsert.length) {
    console.log(`\n--- INSERTARÍA (${wouldInsert.length}) — muestra max 15 ---`);
    wouldInsert.slice(0, 15).forEach((r, i) => {
      console.log(
        `${i + 1}. ${r.license_plate} | ${r.type || "-"} | fleet ${r.fleet_owner_unique_code || "∅"} | ${r.weight_average_capacity}t`
      );
    });
    if (wouldInsert.length > 15) console.log(`   … +${wouldInsert.length - 15} más`);
  } else {
    console.log("\nINSERTARÍA: 0 filas (comportamiento esperado si PRD no creció post-corte).");
  }

  if (inBaselineInDefaultChanged.length) {
    console.log(`\n--- Misma placa, snapshot distinto (${inBaselineInDefaultChanged.length}) — insert_only NO actualiza — max 8 ---`);
    inBaselineInDefaultChanged.slice(0, 8).forEach((r, i) => {
      console.log(`${i + 1}. ${r.placa}`);
      if (String(r.prod.type ?? "") !== String(r.default.type ?? "")) {
        console.log(`     type: prod=${JSON.stringify(r.prod.type)} vs default=${JSON.stringify(r.default.type)}`);
      }
      if (Number(r.prod.weight_average_capacity) !== Number(r.default.weight_average_capacity)) {
        console.log(
          `     weight_average_capacity: prod=${r.prod.weight_average_capacity} vs default=${r.default.weight_average_capacity}`
        );
      }
      if (String(r.prod.fleet_owner_unique_code ?? "") !== String(r.default.fleet_owner_unique_code ?? "")) {
        console.log(
          `     fleet_owner_unique_code: prod=${JSON.stringify(r.prod.fleet_owner_unique_code)} vs default=${JSON.stringify(r.default.fleet_owner_unique_code)}`
        );
      }
      if (Boolean(r.prod.is_route_based) !== Boolean(r.default.is_route_based)) {
        console.log(`     is_route_based: prod=${r.prod.is_route_based} vs default=${r.default.is_route_based}`);
      }
    });
  }

  if (onlyInDefault.length) {
    console.log(`\nSolo en default, no en PRD (${onlyInDefault.length}):`, onlyInDefault.join(", "));
  }

  if (naiveInsertOnly > 100 && wouldInsert.length === 0) {
    console.log(
      `\n⚠ Sin baseline, insert_only insertaría ${naiveInsertOnly} filas (backfill). ` +
        "El baseline evita eso; no desactives incremental_new_keys."
    );
  }

  if (outPath) {
    const hdr = [
      "license_plate",
      "type",
      "weight_average_capacity",
      "fleet_owner_unique_code",
      "is_route_based",
      "placa_raw",
      "accion",
    ];
    const lines = wouldInsert.map((r) => hdr.map((k) => escCsv(r[k])).join(","));
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, `${hdr.join(",")}\n${lines.join("\n")}\n`, "utf8");
    console.log(`\nCSV candidatos insert: ${outPath}`);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
