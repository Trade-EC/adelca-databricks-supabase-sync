const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { v5: uuidv5 } = require("uuid");

const DATABRICKS_HOST = process.env.DATABRICKS_HOST || "";
const DATABRICKS_HTTP_PATH = process.env.DATABRICKS_HTTP_PATH || "";
const DATABRICKS_CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || "";
const DATABRICKS_CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || "";
const DATABRICKS_QAS_HOST = process.env.DATABRICKS_QAS_HOST || "";
const DATABRICKS_QAS_HTTP_PATH = process.env.DATABRICKS_QAS_HTTP_PATH || "";
const DATABRICKS_QAS_TOKEN = process.env.DATABRICKS_QAS_TOKEN || "";

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const SUPABASE_SECONDARY_URL = (process.env.SUPABASE_SECONDARY_URL || "").replace(/\/$/, "");
const SUPABASE_SECONDARY_KEY = process.env.SUPABASE_SECONDARY_SERVICE_ROLE_KEY || "";

const WATERMARK_TABLE = "etl_watermarks";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "500", 10);
const ETL_LOGS_BUCKET = process.env.ETL_LOGS_BUCKET || "";
const UUID_NAMESPACE = "b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b";
const PIPELINES_CONFIG_PATH =
  process.env.PIPELINES_CONFIG_PATH || path.join(__dirname, "pipelines.json");

const s3Client = new S3Client({});

function logInfo(message, extra) {
  if (extra !== undefined) {
    console.log(`${message}: ${JSON.stringify(extra)}`);
  } else {
    console.log(message);
  }
}

function resolveSupabaseForPipeline(cfg) {
  const profile = cfg.supabase_profile || "default";
  if (profile === "secondary") {
    if (!SUPABASE_SECONDARY_URL || !SUPABASE_SECONDARY_KEY) {
      throw new Error(
        "Pipeline uses supabase_profile=secondary: set SUPABASE_SECONDARY_URL and SUPABASE_SECONDARY_SERVICE_ROLE_KEY on the Lambda"
      );
    }
    return { baseUrl: SUPABASE_SECONDARY_URL, serviceKey: SUPABASE_SECONDARY_KEY };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Default Supabase (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY) not configured");
  }
  return { baseUrl: SUPABASE_URL, serviceKey: SUPABASE_KEY };
}

function makeSupabaseClient(baseUrl, serviceKey) {
  const u = (baseUrl || "").replace(/\/$/, "");
  const getHeaders = (prefer) => ({
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    "Content-Type": "application/json",
    ...(prefer ? { Prefer: prefer } : {}),
  });
  return {
    get(table, params = {}, extraHeaders = {}) {
      const url = new URL(`${u}/rest/v1/${table}`);
      Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
      return fetch(url, {
        headers: { ...getHeaders(), ...extraHeaders },
      });
    },
    post(table, rows, prefer) {
      return fetch(`${u}/rest/v1/${table}`, {
        method: "POST",
        headers: getHeaders(`resolution=ignore-duplicates,return=minimal${prefer ? `,${prefer}` : ""}`),
        body: JSON.stringify(rows),
      });
    },
    upsert(table, rows, conflictKey) {
      const url = new URL(`${u}/rest/v1/${table}`);
      if (conflictKey) {
        url.searchParams.set("on_conflict", conflictKey);
      }
      return fetch(url, {
        method: "POST",
        headers: getHeaders("resolution=merge-duplicates"),
        body: JSON.stringify(rows),
      });
    },
  };
}

function generateTransportistaId(codigoTransportista) {
  return uuidv5(String(codigoTransportista).trim(), UUID_NAMESPACE);
}

function loadPipelinesConfig() {
  const raw = fs.readFileSync(PIPELINES_CONFIG_PATH, "utf8");
  return JSON.parse(raw);
}

function getPipelineConfig(event) {
  const pipelineName = event?.pipeline_name || process.env.DEFAULT_PIPELINE || "transportistas";
  const all = loadPipelinesConfig();
  const cfg = all[pipelineName];
  if (!cfg) {
    throw new Error(`Unknown pipeline_name '${pipelineName}'`);
  }
  return cfg;
}

function cleanUniqueField(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s || null;
}

function sourceIndexMap(pipelineConfig) {
  return Object.fromEntries(
    pipelineConfig.column_mapping.map((m, i) => [m.source, i])
  );
}

function conflictSourceColumnIndex(pipelineConfig) {
  const src = pipelineConfig.column_mapping.find((m) => m.target === pipelineConfig.conflict_key);
  if (!src) return 0;
  return pipelineConfig.column_mapping.findIndex((m) => m.source === src.source);
}

function deduplicateRows(rows, pipelineConfig) {
  const idx = conflictSourceColumnIndex(pipelineConfig);
  const seen = new Map();
  for (const row of rows) {
    const k = row[idx] != null ? String(row[idx]).trim() : null;
    if (k) {
      seen.set(k, row);
    }
  }
  const dupes = rows.length - seen.size;
  if (dupes > 0) {
    console.warn(`Removed ${dupes} duplicate rows on conflict key from Databricks.`);
  }
  return [...seen.values()];
}

function splitRowsByMode(rows, pipelineConfig, existingKeys, now) {
  const rowMode = pipelineConfig.row_mode || "transportistas";
  if (rowMode === "generic") {
    return splitRowsGeneric(rows, pipelineConfig, existingKeys, now);
  }
  return splitRowsTransportistas(rows, pipelineConfig, existingKeys, now);
}

function splitRowsGeneric(rows, pipelineConfig, existingKeys, now) {
  const sourceIndex = sourceIndexMap(pipelineConfig);
  const conflictKey = pipelineConfig.conflict_key;
  const srcForConflict = pipelineConfig.column_mapping.find(
    (m) => m.target === conflictKey
  )?.source;
  const defaultRequire = srcForConflict ? [srcForConflict] : [];
  const requireNonNull = pipelineConfig.require_non_null || defaultRequire;
  const includeIngested = pipelineConfig.include_ingested_at !== false;

  const newRows = [];
  const allRows = [];

  for (const row of rows) {
    let skip = false;
    for (const src of requireNonNull) {
      const vi = sourceIndex[src];
      const v = vi !== undefined ? row[vi] : undefined;
      if (v === null || v === undefined || String(v).trim() === "") {
        skip = true;
        break;
      }
    }
    if (skip) continue;

    const record = {};
    for (const mapping of pipelineConfig.column_mapping) {
      const i = sourceIndex[mapping.source];
      record[mapping.target] = i !== undefined ? row[i] : null;
    }
    if (pipelineConfig.defaults) {
      Object.assign(record, pipelineConfig.defaults);
    }
    if (pipelineConfig.null_coalesce) {
      for (const [k, v] of Object.entries(pipelineConfig.null_coalesce)) {
        if (record[k] == null || record[k] === "") {
          record[k] = v;
        }
      }
    }
    const idS = pipelineConfig.id_strategy;
    if (idS?.type === "uuid5_from_source" && idS.source_column && idS.column) {
      const si = sourceIndex[idS.source_column];
      const ns = idS.namespace || UUID_NAMESPACE;
      if (si !== undefined && row[si] != null) {
        record[idS.column] = uuidv5(String(row[si]).trim(), ns);
      }
    }
    if (includeIngested) {
      const tsCol = pipelineConfig.sync_timestamp_column || "_ingested_at";
      record[tsCol] = now;
    }

    const ck = record[conflictKey];
    const ckStr = ck != null ? String(ck) : "";
    allRows.push(record);
    if (ckStr && !existingKeys.has(ckStr)) {
      newRows.push(record);
    }
  }
  return { newRows, allRows };
}

function splitRowsTransportistas(rows, pipelineConfig, existingKeys, now) {
  const newRows = [];
  const allRows = [];
  const seenEmails = new Set();
  const seenPhones = new Set();
  const sourceIndex = sourceIndexMap(pipelineConfig);
  const conflictKey = pipelineConfig.conflict_key;

  for (const row of rows) {
    const codigo = row[sourceIndex.codigo_transportista]
      ? String(row[sourceIndex.codigo_transportista]).trim()
      : null;
    const ruc = row[sourceIndex.ruc] ? String(row[sourceIndex.ruc]).trim() : null;
    if (!codigo || !ruc) continue;

    let email =
      sourceIndex.email !== undefined ? cleanUniqueField(row[sourceIndex.email]) : null;
    let telefono =
      sourceIndex.telefono !== undefined
        ? cleanUniqueField(row[sourceIndex.telefono])
        : null;

    if (email && seenEmails.has(email)) email = null;
    if (telefono && seenPhones.has(telefono)) telefono = null;

    if (email) seenEmails.add(email);
    if (telefono) seenPhones.add(telefono);

    const record = {};
    for (const mapping of pipelineConfig.column_mapping) {
      const idx = sourceIndex[mapping.source];
      let value = idx !== undefined ? row[idx] : null;
      if (mapping.target === "email") value = email;
      if (mapping.target === "telefono") value = telefono;
      record[mapping.target] = value;
    }
    if (pipelineConfig.defaults) {
      Object.assign(record, pipelineConfig.defaults);
    }
    if (pipelineConfig.id_strategy?.type === "uuid5_codigo_transportista") {
      record[pipelineConfig.id_strategy.column] = generateTransportistaId(codigo);
    }
    record._ingested_at = now;

    allRows.push(record);
    const ck = record[conflictKey] != null ? String(record[conflictKey]) : "";
    if (ck && !existingKeys.has(ck)) {
      newRows.push(record);
    }
  }
  return { newRows, allRows };
}

function resolveDatabricksContext(pipelineConfig) {
  const profile = pipelineConfig.databricks_profile || "prd";
  if (profile === "qas") {
    if (!DATABRICKS_QAS_HOST || !DATABRICKS_QAS_HTTP_PATH || !DATABRICKS_QAS_TOKEN) {
      throw new Error(
        "Pipeline uses databricks_profile=qas: set DATABRICKS_QAS_HOST, DATABRICKS_QAS_HTTP_PATH, DATABRICKS_QAS_TOKEN on the Lambda"
      );
    }
    return {
      host: DATABRICKS_QAS_HOST,
      httpPath: DATABRICKS_QAS_HTTP_PATH,
      auth: "pat",
      pat: DATABRICKS_QAS_TOKEN
    };
  }
  if (!DATABRICKS_HOST || !DATABRICKS_HTTP_PATH) {
    throw new Error("Databricks PRD (DATABRICKS_HOST / DATABRICKS_HTTP_PATH) not configured");
  }
  return { host: DATABRICKS_HOST, httpPath: DATABRICKS_HTTP_PATH, auth: "oauth" };
}

async function getDatabricksOAuthToken() {
  const tokenUrl = `https://${DATABRICKS_HOST}/oidc/v1/token`;
  const auth = Buffer.from(`${DATABRICKS_CLIENT_ID}:${DATABRICKS_CLIENT_SECRET}`).toString(
    "base64"
  );
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    scope: "all-apis",
  });

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Databricks OAuth failed (${res.status}): ${txt.slice(0, 300)}`);
  }

  const data = await res.json();
  return data.access_token;
}

async function fetchDatabricks(pipelineConfig) {
  const sourceColumns = pipelineConfig.column_mapping.map((m) => m.source).join(", ");
  const statement = `SELECT ${sourceColumns} FROM ${pipelineConfig.source_table}`;
  const ctx = resolveDatabricksContext(pipelineConfig);
  const warehouseId = ctx.httpPath.split("/").pop();

  logInfo(`Querying Databricks`, { table: pipelineConfig.source_table, profile: ctx.auth });
  const token =
    ctx.auth === "pat" ? ctx.pat : await getDatabricksOAuthToken();
  const res = await fetch(`https://${ctx.host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Databricks query failed (${res.status}): ${txt.slice(0, 300)}`);
  }

  const payload = await res.json();
  if (payload.status?.state !== "SUCCEEDED") {
    throw new Error(`Databricks statement status: ${payload.status?.state || "UNKNOWN"}`);
  }

  const rows = payload.result?.data_array || [];
  logInfo(`Fetched ${rows.length} row(s) from Databricks.`);
  return rows;
}

async function getExistingKeys(sb, targetTable, conflictKey) {
  const values = new Set();
  let offset = 0;
  const batch = 1000;

  while (true) {
    const res = await sb.get(
      targetTable,
      { select: conflictKey },
      { Range: `${offset}-${offset + batch - 1}` }
    );

    if (![200, 206].includes(res.status)) {
      throw new Error(`Failed to read existing keys: ${res.status}`);
    }

    const data = await res.json();
    if (!data.length) break;
    for (const row of data) {
      const v = row[conflictKey];
      if (v != null) values.add(String(v));
    }
    if (data.length < batch) break;
    offset += batch;
  }
  return values;
}

async function saveEtlLog(logData) {
  if (!ETL_LOGS_BUCKET) {
    console.warn("ETL_LOGS_BUCKET not set, skipping S3 log.");
    return null;
  }

  const ts = logData.sync_timestamp || new Date().toISOString();
  const datePrefix = ts.slice(0, 10);
  const pipelineName = logData.pipeline_name || "unknown";
  const key = `${pipelineName}/${datePrefix}/${ts.replace(/:/g, "-")}.json`;

  try {
    await s3Client.send(
      new PutObjectCommand({
        Bucket: ETL_LOGS_BUCKET,
        Key: key,
        Body: JSON.stringify(logData, null, 2),
        ContentType: "application/json",
      })
    );
    const uri = `s3://${ETL_LOGS_BUCKET}/${key}`;
    logInfo("ETL log saved", { uri });
    return uri;
  } catch (err) {
    console.error("Failed to save ETL log to S3", err);
    return null;
  }
}

async function createRunRecord(sb, pipelineName, status, payload = {}) {
  const row = {
    pipeline_name: pipelineName,
    status,
    source_table: payload.source_table || null,
    target_table: payload.dest_table || null,
    inserted_count: payload.inserted || 0,
    updated_count: payload.updated || 0,
    skipped_count: payload.skipped || 0,
    error_message: payload.error_message || null,
    started_at: payload.started_at || new Date().toISOString(),
    finished_at: payload.finished_at || new Date().toISOString(),
    duration_ms: payload.duration_ms || null,
    s3_log_uri: payload.s3_log_uri || null
  };
  try {
    const res = await sb.post("etl_runs", [row], null);
    if (![200, 201].includes(res.status)) {
      const txt = await res.text();
      console.warn(`etl_runs insert skipped (${res.status}): ${txt.slice(0, 200)}`);
    }
  } catch (err) {
    console.warn(`etl_runs insert skipped: ${String(err.message || err)}`);
  }
}

async function runSyncForPipeline(event) {
  const startedAt = new Date();
  const pipelineConfig = getPipelineConfig(event);
  const pipelineName = pipelineConfig.pipeline_name;
  const syncStart = new Date();
  const now = syncStart.toISOString();
  const sourceTable = pipelineConfig.source_table;
  const targetTable = pipelineConfig.target_table;
  const conflictKey = pipelineConfig.conflict_key;
  const batchSize = pipelineConfig.batch_size || BATCH_SIZE;
  const writeMode = pipelineConfig.write_mode || "insert_only";

  const { baseUrl, serviceKey } = resolveSupabaseForPipeline(pipelineConfig);
  const sb = makeSupabaseClient(baseUrl, serviceKey);

  const tableRes = await sb.get(targetTable, {
    select: conflictKey,
    limit: "1",
  });
  if (tableRes.status !== 200) {
    throw new Error(`Cannot access '${targetTable}': ${tableRes.status}`);
  }

  const wmRes = await sb.get(WATERMARK_TABLE, {
    select: "table_name",
    limit: "1",
  });
  if (wmRes.status !== 200) {
    throw new Error(
      `Cannot access '${WATERMARK_TABLE}': ${wmRes.status}. Create ETL tables in this Supabase project.`
    );
  }

  const existing = await getExistingKeys(sb, targetTable, conflictKey);
  const countBefore = existing.size;
  logInfo(`Found ${countBefore} existing records in Supabase.`, { targetTable, pipelineName });

  let dbxRows = await fetchDatabricks(pipelineConfig);
  if (!dbxRows.length) {
    return { status: "no_data", inserted: 0, total: countBefore, pipeline_name: pipelineName };
  }
  dbxRows = deduplicateRows(dbxRows, pipelineConfig);

  const { newRows, allRows } = splitRowsByMode(dbxRows, pipelineConfig, existing, now);
  logInfo("Sync split", {
    pipeline: pipelineName,
    new: newRows.length,
    skipped: dbxRows.length - newRows.length
  });

  let inserted = 0;
  let updated = 0;
  const rowsToWrite = writeMode === "upsert" ? allRows : newRows;
  for (let i = 0; i < rowsToWrite.length; i += batchSize) {
    const batch = rowsToWrite.slice(i, i + batchSize);
    const res =
      writeMode === "upsert"
        ? await sb.upsert(targetTable, batch, conflictKey)
        : await sb.post(targetTable, batch, null);
    if (![200, 201].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Write failed: ${res.status} ${txt.slice(0, 300)}`);
    }
    if (writeMode === "upsert") {
      const insertedInBatch = batch.filter(
        (r) => r[conflictKey] != null && !existing.has(String(r[conflictKey]))
      ).length;
      inserted += insertedInBatch;
      updated += batch.length - insertedInBatch;
      for (const r of batch) {
        if (r[conflictKey] != null) {
          existing.add(String(r[conflictKey]));
        }
      }
    } else {
      inserted += batch.length;
    }
    logInfo(`Written batch ${Math.floor(i / batchSize) + 1}`, { mode: writeMode, size: batch.length });
  }

  const postKeys = await getExistingKeys(sb, targetTable, conflictKey);
  const countAfter = postKeys.size;
  const syncTime = new Date().toISOString();

  const upsertRes = await sb.upsert(
    WATERMARK_TABLE,
    [{ table_name: pipelineName, last_timestamp: syncTime }],
    "table_name"
  );
  if (![200, 201].includes(upsertRes.status)) {
    console.warn(`Watermark update failed: ${upsertRes.status}`);
  }

  const detail = {
    pipeline_name: pipelineName,
    status: "success",
    inserted,
    updated,
    total: countAfter,
    source_table: sourceTable,
    dest_table: targetTable,
    write_mode: writeMode,
    conflict_key: conflictKey,
    source_count: dbxRows.length,
    dest_count_before: countBefore,
    dest_count_after: countAfter,
    skipped: dbxRows.length - rowsToWrite.length,
    sync_timestamp: syncTime,
    inserted_records: newRows
      .slice(0, 200)
      .map((r) => ({ [conflictKey]: r[conflictKey] })),
  };
  const s3LogUri = await saveEtlLog(detail);
  const finishedAt = new Date();
  await createRunRecord(sb, pipelineName, "success", {
    ...detail,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    s3_log_uri: s3LogUri
  });

  return {
    pipeline_name: pipelineName,
    status: "success",
    inserted,
    updated,
    total: countAfter,
    sync_timestamp: syncTime,
  };
}

exports.lambdaHandler = async (event) => {
  logInfo("Event received", event);
  try {
    const result = await runSyncForPipeline(event || {});
    logInfo("Sync complete", result);
    return {
      statusCode: 200,
      body: JSON.stringify(result),
    };
  } catch (error) {
    console.error("Sync failed", error);
    try {
      const pipelineName = event?.pipeline_name || process.env.DEFAULT_PIPELINE || "transportistas";
      const cfg = loadPipelinesConfig()[pipelineName];
      if (cfg) {
        const { baseUrl, serviceKey } = resolveSupabaseForPipeline(cfg);
        const sb = makeSupabaseClient(baseUrl, serviceKey);
        await createRunRecord(sb, pipelineName, "error", {
          error_message: String(error.message || error)
        });
      }
    } catch (inner) {
      console.error("Failed writing etl_runs error row", inner);
    }
    return {
      statusCode: 500,
      body: JSON.stringify({ status: "error", message: String(error.message || error) }),
    };
  }
};
