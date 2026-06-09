const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { v5: uuidv5 } = require("uuid");
const {
  transformMaterialsWeightTotals,
  fetchSaMaterialsCatalog,
} = require("./sa_invoice_zod_transform");

const DATABRICKS_HOST = process.env.DATABRICKS_HOST || process.env.DATABRICKS_PRD_HOST || "";
const DATABRICKS_HTTP_PATH =
  process.env.DATABRICKS_HTTP_PATH || process.env.DATABRICKS_PRD_HTTP_PATH || "";
const DATABRICKS_CLIENT_ID =
  process.env.DATABRICKS_CLIENT_ID || process.env.DATABRICKS_PRD_CLIENT_ID || "";
const DATABRICKS_CLIENT_SECRET =
  process.env.DATABRICKS_CLIENT_SECRET || process.env.DATABRICKS_PRD_CLIENT_SECRET || "";
const DATABRICKS_QAS_HOST = process.env.DATABRICKS_QAS_HOST || "";
const DATABRICKS_QAS_HTTP_PATH = process.env.DATABRICKS_QAS_HTTP_PATH || "";
const DATABRICKS_QAS_TOKEN = process.env.DATABRICKS_QAS_TOKEN || "";

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const SUPABASE_SECONDARY_URL = (process.env.SUPABASE_SECONDARY_URL || "").replace(/\/$/, "");
const SUPABASE_SECONDARY_KEY = process.env.SUPABASE_SECONDARY_SERVICE_ROLE_KEY || "";
const SUPABASE_TERTIARY_URL = (process.env.SUPABASE_TERTIARY_URL || "").replace(/\/$/, "");
const SUPABASE_TERTIARY_KEY = process.env.SUPABASE_TERTIARY_SERVICE_ROLE_KEY || "";
const SUPABASE_BASE_SOCIO_URL = (process.env.SUPABASE_BASE_SOCIO_URL || "").replace(/\/$/, "");
const SUPABASE_BASE_SOCIO_KEY = process.env.SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY || "";

const WATERMARK_TABLE = "etl_watermarks";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "500", 10);
const ETL_LOGS_BUCKET = process.env.ETL_LOGS_BUCKET || "";
const SKIP_ETL_WATERMARK =
  process.env.SKIP_ETL_WATERMARK === "1" || process.env.SKIP_ETL_WATERMARK === "true";
const ETL_SUCCESS_PREFIX = (process.env.ETL_SUCCESS_PREFIX || "etl-success").replace(/\/$/, "");
const UUID_NAMESPACE = "b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b";
const DBX_WAIT_TIMEOUT = process.env.DBX_WAIT_TIMEOUT || "50s";
const DBX_POLL_INTERVAL_MS = parseInt(process.env.DBX_POLL_INTERVAL_MS || "3000", 10);
const DBX_MAX_WAIT_MS = parseInt(process.env.DBX_MAX_WAIT_MS || "240000", 10);
const PIPELINES_CONFIG_PATH =
  process.env.PIPELINES_CONFIG_PATH || path.join(__dirname, "pipelines.json");
const DOMAIN_BATCHES_CONFIG_PATH =
  process.env.DOMAIN_BATCHES_CONFIG_PATH || path.join(__dirname, "domain_batches.json");

const s3Client = new S3Client({});

function logInfo(message, extra) {
  if (extra !== undefined) {
    console.log(`${message}: ${JSON.stringify(extra)}`);
  } else {
    console.log(message);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** PostgREST bulk writes require identical keys on every object (PGRST102). */
function alignBatchObjectKeys(rows) {
  if (!rows.length) return rows;
  const keys = new Set();
  for (const r of rows) {
    for (const k of Object.keys(r)) keys.add(k);
  }
  const list = [...keys];
  return rows.map((r) => {
    const out = { ...r };
    for (const k of list) {
      if (!Object.prototype.hasOwnProperty.call(out, k)) {
        out[k] = null;
      }
    }
    return out;
  });
}

function resolveSupabaseByProfile(profile) {
  const p = profile || "default";
  if (p === "secondary") {
    if (!SUPABASE_SECONDARY_URL || !SUPABASE_SECONDARY_KEY) {
      throw new Error(
        "supabase_profile=secondary: set SUPABASE_SECONDARY_URL and SUPABASE_SECONDARY_SERVICE_ROLE_KEY on the Lambda"
      );
    }
    return { baseUrl: SUPABASE_SECONDARY_URL, serviceKey: SUPABASE_SECONDARY_KEY };
  }
  if (p === "tertiary") {
    if (!SUPABASE_TERTIARY_URL || !SUPABASE_TERTIARY_KEY) {
      throw new Error(
        "supabase_profile=tertiary: set SUPABASE_TERTIARY_URL and SUPABASE_TERTIARY_SERVICE_ROLE_KEY on the Lambda"
      );
    }
    return { baseUrl: SUPABASE_TERTIARY_URL, serviceKey: SUPABASE_TERTIARY_KEY };
  }
  if (p === "base_socio") {
    if (!SUPABASE_BASE_SOCIO_URL || !SUPABASE_BASE_SOCIO_KEY) {
      throw new Error(
        "supabase_profile=base_socio: set SUPABASE_BASE_SOCIO_URL and SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY on the Lambda"
      );
    }
    return { baseUrl: SUPABASE_BASE_SOCIO_URL, serviceKey: SUPABASE_BASE_SOCIO_KEY };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Default Supabase (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY) not configured");
  }
  return { baseUrl: SUPABASE_URL, serviceKey: SUPABASE_KEY };
}

function resolveSupabaseForPipeline(cfg) {
  return resolveSupabaseByProfile(cfg.supabase_profile || "default");
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
    deleteWhere(table, filters = {}) {
      const url = new URL(`${u}/rest/v1/${table}`);
      Object.entries(filters).forEach(([k, v]) => url.searchParams.set(k, v));
      return fetch(url, {
        method: "DELETE",
        headers: getHeaders("return=minimal"),
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

function loadDomainBatchesConfig() {
  if (!fs.existsSync(DOMAIN_BATCHES_CONFIG_PATH)) {
    return {};
  }
  const raw = fs.readFileSync(DOMAIN_BATCHES_CONFIG_PATH, "utf8");
  return JSON.parse(raw);
}

function getDomainBatchConfig(batchId) {
  const id = batchId != null ? String(batchId).trim() : "";
  if (!id) {
    throw new Error("domain_batch is required (e.g. base_socio)");
  }
  const all = loadDomainBatchesConfig();
  const cfg = all[id];
  if (!cfg) {
    throw new Error(`Unknown domain_batch '${id}'`);
  }
  if (!Array.isArray(cfg.sequence) || !cfg.sequence.length) {
    throw new Error(`domain_batch '${id}' has empty sequence`);
  }
  return { batch_id: id, ...cfg };
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

/** Ecuador mobile: 0988634965 → +593988634965 (prefix +593, drop leading 0). */
function normalizeEcuadorPhone593(raw) {
  if (raw == null || raw === "") return null;
  let digits = String(raw).replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("593")) return `+${digits}`;
  if (digits.startsWith("0")) digits = digits.slice(1);
  return `+593${digits}`;
}

function isBlankValue(value) {
  return value === null || value === undefined || String(value).trim() === "";
}

function buildJsonbSpecsList(pipelineConfig) {
  if (Array.isArray(pipelineConfig.build_jsonb_targets) && pipelineConfig.build_jsonb_targets.length) {
    return pipelineConfig.build_jsonb_targets;
  }
  if (pipelineConfig.build_jsonb_object) {
    return [pipelineConfig.build_jsonb_object];
  }
  return [];
}

/** Source column names referenced by build-jsonb specs (for Databricks SELECT). */
function collectBuildJsonbSourceColumnRefs(spec) {
  const cols = [];
  if (!spec || typeof spec !== "object") return cols;
  if (spec.root_object_from_columns && typeof spec.root_object_from_columns === "object" && !Array.isArray(spec.root_object_from_columns)) {
    for (const sc of Object.values(spec.root_object_from_columns)) {
      if (typeof sc === "string" && sc) cols.push(sc);
    }
  }
  if (Array.isArray(spec.properties)) {
    for (const p of spec.properties) {
      if (p.object_from_columns && typeof p.object_from_columns === "object" && !Array.isArray(p.object_from_columns)) {
        for (const sc of Object.values(p.object_from_columns)) {
          if (typeof sc === "string" && sc) cols.push(sc);
        }
      }
      if (typeof p.source_column === "string" && p.source_column) cols.push(p.source_column);
    }
  }
  return cols;
}

/** One spec: either `root_object_from_columns` → flat object on `target`, or `properties` → keyed object. */
function applyOneBuildJsonbFromRow(record, row, sourceIndex, spec) {
  if (!spec || typeof spec !== "object" || !spec.target) return;
  const { target, wrap_in_array: wrapInArray } = spec;
  if (spec.root_object_from_columns && typeof spec.root_object_from_columns === "object" && !Array.isArray(spec.root_object_from_columns)) {
    const nested = {};
    for (const [nestedKey, srcCol] of Object.entries(spec.root_object_from_columns)) {
      if (typeof srcCol !== "string") continue;
      const si = sourceIndex[srcCol];
      const raw = si !== undefined ? row[si] : undefined;
      nested[nestedKey] = raw == null ? "" : String(raw).trim();
    }
    const wrapRoot = wrapInArray === true;
    record[target] = wrapRoot ? [nested] : nested;
    return;
  }
  if (!Array.isArray(spec.properties)) return;
  const obj = {};
  for (const p of spec.properties) {
    const key = p.key;
    if (!key) continue;
    if (Object.prototype.hasOwnProperty.call(p, "literal")) {
      obj[key] = p.literal;
      continue;
    }
    if (p.object_from_columns && typeof p.object_from_columns === "object" && !Array.isArray(p.object_from_columns)) {
      const nested = {};
      for (const [nestedKey, srcCol] of Object.entries(p.object_from_columns)) {
        if (typeof srcCol !== "string") continue;
        const si = sourceIndex[srcCol];
        const raw = si !== undefined ? row[si] : undefined;
        nested[nestedKey] = raw == null ? "" : String(raw).trim();
      }
      obj[key] = nested;
      continue;
    }
    const sc = p.source_column;
    if (!sc) continue;
    const si = sourceIndex[sc];
    const raw = si !== undefined ? row[si] : undefined;
    obj[key] = raw == null ? "" : String(raw).trim();
  }
  const wrap = wrapInArray !== false;
  record[target] = wrap ? [obj] : obj;
}

function applyBuildJsonbFromPipelineConfig(record, row, sourceIndex, pipelineConfig) {
  for (const spec of buildJsonbSpecsList(pipelineConfig)) {
    applyOneBuildJsonbFromRow(record, row, sourceIndex, spec);
  }
}

/** PostgREST `on_conflict` accepts comma-separated columns; we use the same string in pipelines.json */
function parseConflictColumns(conflictKey) {
  if (!conflictKey || typeof conflictKey !== "string") {
    return conflictKey ? [String(conflictKey)] : [];
  }
  const parts = conflictKey
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return parts.length ? parts : [];
}

function compositeConflictKey(record, targetColumns) {
  const parts = targetColumns.map((c) =>
    record[c] != null && record[c] !== "" ? String(record[c]).trim() : ""
  );
  if (parts.some((p) => p === "")) return null;
  return parts.join("\x1f");
}

function recordConflictKey(record, conflictKeyParam) {
  const cols = parseConflictColumns(conflictKeyParam);
  if (!cols.length) return "";
  if (cols.length === 1) {
    const v = record[cols[0]];
    return v != null && v !== "" ? String(v) : "";
  }
  return compositeConflictKey(record, cols) || "";
}

function normalizeBoolean(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  const s = String(value).trim().toLowerCase();
  return ["1", "true", "t", "yes", "y", "si", "s", "rutero"].includes(s);
}

function sourceColumnsList(pipelineConfig) {
  const out = [];
  const seen = new Set();
  for (const m of pipelineConfig.column_mapping) {
    if (!seen.has(m.source)) {
      out.push(m.source);
      seen.add(m.source);
    }
  }
  for (const c of pipelineConfig.extra_source_columns || []) {
    if (typeof c === "string" && c && !seen.has(c)) {
      out.push(c);
      seen.add(c);
    }
  }
  for (const spec of buildJsonbSpecsList(pipelineConfig)) {
    for (const c of collectBuildJsonbSourceColumnRefs(spec)) {
      if (c && !seen.has(c)) {
        out.push(c);
        seen.add(c);
      }
    }
  }
  return out;
}

function sourceIndexMap(pipelineConfig) {
  const cols = sourceColumnsList(pipelineConfig);
  return Object.fromEntries(cols.map((name, i) => [name, i]));
}

function conflictSourceColumnIndex(pipelineConfig) {
  const idxMap = sourceIndexMap(pipelineConfig);
  const cols = parseConflictColumns(pipelineConfig.conflict_key);
  if (cols.length === 1) {
    const src = pipelineConfig.column_mapping.find((m) => m.target === cols[0]);
    if (!src) return 0;
    const i = idxMap[src.source];
    return i !== undefined ? i : 0;
  }
  if (cols.length > 1) {
    const first = pipelineConfig.column_mapping.find((m) => m.target === cols[0]);
    if (!first) return 0;
    const i = idxMap[first.source];
    return i !== undefined ? i : 0;
  }
  return 0;
}

function deduplicateRows(rows, pipelineConfig) {
  const conflictKeyParam = pipelineConfig.conflict_key;
  const cols = parseConflictColumns(conflictKeyParam);
  const sourceIndex = sourceIndexMap(pipelineConfig);

  if (cols.length > 1) {
    const sources = cols
      .map((t) => pipelineConfig.column_mapping.find((m) => m.target === t)?.source)
      .filter(Boolean);
    if (sources.length !== cols.length) {
      console.warn("deduplicateRows: composite conflict_key targets missing from column_mapping; skipping dedupe.");
      return rows;
    }
    const seen = new Map();
    for (const row of rows) {
      const parts = sources.map((src) => {
        const i = sourceIndex[src];
        const v = i !== undefined ? row[i] : undefined;
        return v != null ? String(v).trim() : "";
      });
      if (parts.some((p) => !p)) continue;
      const k = parts.join("\x1f");
      seen.set(k, row);
    }
    const dupes = rows.length - seen.size;
    if (dupes > 0) {
      console.warn(`Removed ${dupes} duplicate rows on composite conflict key from Databricks.`);
    }
    return [...seen.values()];
  }

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

function buildSourceFieldStats(rows, pipelineConfig) {
  const sourceIndex = sourceIndexMap(pipelineConfig);
  const stats = {};
  for (const mapping of pipelineConfig.column_mapping) {
    stats[mapping.source] = { total: rows.length, null_or_empty: 0 };
  }
  for (const row of rows) {
    for (const mapping of pipelineConfig.column_mapping) {
      const i = sourceIndex[mapping.source];
      const value = i !== undefined ? row[i] : undefined;
      if (isBlankValue(value)) {
        stats[mapping.source].null_or_empty += 1;
      }
    }
  }
  return stats;
}

function buildRecordFieldStats(records, fields) {
  const stats = {};
  for (const field of fields) {
    stats[field] = { total: records.length, null_or_empty: 0 };
  }
  for (const row of records) {
    for (const field of fields) {
      if (isBlankValue(row[field])) {
        stats[field].null_or_empty += 1;
      }
    }
  }
  return stats;
}

function splitRowsByMode(rows, pipelineConfig, existingKeys, now) {
  const rowMode = pipelineConfig.row_mode || "transportistas";
  if (rowMode === "generic") {
    return splitRowsGeneric(rows, pipelineConfig, existingKeys, now);
  }
  return splitRowsTransportistas(rows, pipelineConfig, existingKeys, now);
}

/** Same logical RUC with/without leading zeros must share one dedupe bucket (unique tax_id in Postgres). */
function normalizeTaxIdClusterKey(raw) {
  const s = raw != null ? String(raw).trim() : "";
  if (!s) return "";
  if (!/^\d+$/.test(s)) return s;
  const stripped = s.replace(/^0+/, "");
  return stripped === "" ? "0" : stripped;
}

/**
 * Collapse records sharing the same value for all listed target columns.
 * `strategy`: "last" (default) or "first" for which row wins per dedupe key.
 * Rows missing any dedupe key stay unmerged (appended as-is).
 * For `tax_id`, the dedupe bucket uses normalized digits (leading zeros stripped) so "011…" and "11…" merge.
 */
/** Ecuador plate display: letters + hyphen + digits (PWR0840 → PWR-0840). */
function normalizeLicensePlateEcDisplay(raw) {
  if (raw == null) return "";
  const compact = String(raw).trim().toUpperCase().replace(/[\s-]/g, "");
  if (!compact) return "";
  const m = compact.match(/^([A-Z]+)([0-9]+)$/);
  if (m) return `${m[1]}-${m[2]}`;
  return compact;
}

function licensePlateDedupeKey(raw) {
  return normalizeLicensePlateEcDisplay(raw).replace(/-/g, "");
}

function normalizePlateValue(raw) {
  return normalizeLicensePlateEcDisplay(raw);
}

function applyLicensePlateNormalization(record, pipelineConfig) {
  if (!pipelineConfig?.normalize_license_plates_ec) return;
  if (Object.prototype.hasOwnProperty.call(record, "license_plate")) {
    const v = normalizeLicensePlateEcDisplay(record.license_plate);
    record.license_plate = v || record.license_plate;
  }
  if (Object.prototype.hasOwnProperty.call(record, "current_license_plate")) {
    const v = normalizeLicensePlateEcDisplay(record.current_license_plate);
    record.current_license_plate = v || null;
  }
  if (Array.isArray(record.license_plates)) {
    record.license_plates = record.license_plates
      .map((p) => normalizeLicensePlateEcDisplay(p))
      .filter(Boolean);
  }
}

function parseAggTimestamp(value) {
  if (value == null || value === "") return 0;
  const t = Date.parse(String(value));
  return Number.isFinite(t) ? t : 0;
}

/** Union plate strings case-insensitively, preserving first-seen order. */
function mergeUniquePlateList(existingArr, incomingPlates) {
  const seen = new Set();
  const out = [];
  const add = (raw) => {
    const plate = normalizePlateValue(raw);
    if (!plate) return;
    const key = licensePlateDedupeKey(raw);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(plate);
  };
  for (const p of incomingPlates || []) add(p);
  for (const p of existingArr || []) add(p);
  return out;
}

/** Same logical national_id with/without leading zeros share one aggregation bucket. */
function normalizeNationalIdClusterKey(raw) {
  const s = raw != null ? String(raw).trim() : "";
  if (!s) return "";
  if (!/^\d+$/.test(s)) return s;
  const stripped = s.replace(/^0+/, "");
  return stripped === "" ? "0" : stripped;
}

function conductorAggregateGroupKey(record, spec) {
  const nationalField = spec.group_by_national_id;
  if (typeof nationalField === "string" && nationalField.trim() && !isBlankValue(record[nationalField])) {
    return `national_id:${normalizeNationalIdClusterKey(record[nationalField])}`;
  }
  const id = record.id != null ? String(record.id).trim() : "";
  return id ? `id:${id}` : "";
}

function collectExistingPlatesForAggregateGroup(groupKey, winnerId, existingById, existingByNationalId) {
  if (groupKey.startsWith("national_id:")) {
    const nidKey = groupKey.slice("national_id:".length);
    const bucket = existingByNationalId.get(nidKey) || [];
    const plates = [];
    for (const row of bucket) {
      if (Array.isArray(row.license_plates)) plates.push(...row.license_plates);
    }
    return plates;
  }
  const existing = existingById.get(String(winnerId)) || {};
  return Array.isArray(existing.license_plates) ? existing.license_plates : [];
}

/**
 * Group mapped rows by driver id, or by national_id when configured; accumulate plates jsonb
 * and set current plate from the row with the latest timestamp_source.
 */
function aggregateConductorPlateRecords(
  records,
  spec,
  existingById = new Map(),
  existingByNationalId = new Map()
) {
  if (!spec || typeof spec !== "object" || !Array.isArray(records) || !records.length) {
    return records;
  }
  const platesTarget = spec.license_plates_target || "license_plates";
  const currentTarget = spec.current_plate_target || "current_license_plate";
  const groups = new Map();

  for (const record of records) {
    const groupKey = conductorAggregateGroupKey(record, spec);
    if (!groupKey) continue;
    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey).push(record);
  }

  const out = [];
  for (const [groupKey, rows] of groups) {
    rows.sort((a, b) => {
      const tb = parseAggTimestamp(b.__agg_ts);
      const ta = parseAggTimestamp(a.__agg_ts);
      if (tb !== ta) return tb - ta;
      return normalizePlateValue(b.__agg_plate).localeCompare(normalizePlateValue(a.__agg_plate));
    });
    const winner = rows[0];
    const sourcePlates = rows
      .slice()
      .sort((a, b) => parseAggTimestamp(a.__agg_ts) - parseAggTimestamp(b.__agg_ts))
      .map((r) => normalizePlateValue(r.__agg_plate))
      .filter(Boolean);
    const currentPlate = normalizePlateValue(winner.__agg_plate) || null;
    const existingPlates = collectExistingPlatesForAggregateGroup(
      groupKey,
      winner.id,
      existingById,
      existingByNationalId
    );
    const licensePlates = mergeUniquePlateList(existingPlates, sourcePlates);

    const merged = { ...winner };
    delete merged.__agg_plate;
    delete merged.__agg_ts;
    merged[platesTarget] = licensePlates;
    merged[currentTarget] = currentPlate;
    applyLicensePlateNormalization(merged, { normalize_license_plates_ec: spec.normalize_license_plates_ec !== false });
    out.push(merged);
  }
  return out;
}

async function fetchExistingDriversByNationalId(sb, targetTable, nationalIdValues, nationalIdField = "national_id") {
  const exactValues = [
    ...new Set(
      nationalIdValues
        .map((raw) => (raw != null ? String(raw).trim() : ""))
        .filter(Boolean)
    ),
  ];
  const out = new Map();
  if (!exactValues.length) return out;

  const chunkSize = 100;
  for (let i = 0; i < exactValues.length; i += chunkSize) {
    const chunk = exactValues.slice(i, i + chunkSize);
    const inList = chunk.map((id) => encodeURIComponent(id)).join(",");
    const res = await sb.get(targetTable, {
      select: `id,${nationalIdField},license_plates,current_license_plate`,
      [nationalIdField]: `in.(${inList})`,
    });
    if (![200, 206].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Failed to read existing drivers by national_id: ${res.status} ${txt.slice(0, 300)}`);
    }
    const data = await res.json();
    if (!Array.isArray(data)) continue;
    for (const row of data) {
      const rawNid = row?.[nationalIdField];
      if (rawNid == null || String(rawNid).trim() === "") continue;
      const key = normalizeNationalIdClusterKey(rawNid);
      if (!out.has(key)) out.set(key, []);
      out.get(key).push(row);
    }
  }
  return out;
}

async function deleteStaleDriversByNationalId(sb, targetTable, records, spec) {
  const nationalIdField = spec.group_by_national_id || "national_id";
  if (!spec.dedupe_destination_by_national_id) return 0;
  let deleted = 0;
  for (const record of records) {
    const nid = record[nationalIdField];
    const winnerId = record.id;
    if (isBlankValue(nid) || isBlankValue(winnerId)) continue;
    const res = await sb.deleteWhere(targetTable, {
      [nationalIdField]: `eq.${encodeURIComponent(String(nid).trim())}`,
      id: `neq.${winnerId}`,
    });
    if (![200, 204].includes(res.status)) {
      const txt = await res.text();
      throw new Error(
        `Failed to dedupe drivers by ${nationalIdField}: ${res.status} ${txt.slice(0, 300)}`
      );
    }
    deleted += 1;
  }
  return deleted;
}

async function fetchExistingDriverPlateFields(sb, targetTable, ids) {
  const uniqueIds = [...new Set(ids.map((id) => (id != null ? String(id).trim() : "")).filter(Boolean))];
  const out = new Map();
  if (!uniqueIds.length) return out;

  const chunkSize = 100;
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const inList = chunk.map((id) => encodeURIComponent(id)).join(",");
    const res = await sb.get(targetTable, {
      select: "id,license_plates,current_license_plate",
      id: `in.(${inList})`,
    });
    if (![200, 206].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Failed to read existing driver plates: ${res.status} ${txt.slice(0, 300)}`);
    }
    const data = await res.json();
    if (!Array.isArray(data)) continue;
    for (const row of data) {
      if (row?.id != null) out.set(String(row.id), row);
    }
  }
  return out;
}

function dedupeRecordsByTargetFields(records, targetKeys, opts = {}) {
  const strategy = opts.strategy === "first" ? "first" : "last";
  const keys = (Array.isArray(targetKeys) ? targetKeys : [targetKeys]).filter(
    (k) => typeof k === "string" && k.trim() !== ""
  );
  if (!keys.length || !Array.isArray(records) || !records.length) return records;
  const merged = new Map();
  const rest = [];
  for (const r of records) {
    const parts = keys.map((k) => {
      if (k === "tax_id") return normalizeTaxIdClusterKey(String(r.tax_id != null ? r.tax_id : "").trim());
      const v = r[k];
      return v != null && v !== "" ? String(v).trim() : "";
    });
    if (parts.some((p) => !p)) {
      rest.push(r);
      continue;
    }
    const k = keys.length === 1 ? parts[0] : parts.join("\x1f");
    if (strategy === "first" && merged.has(k)) continue;
    merged.set(k, r);
  }
  return rest.concat([...merged.values()]);
}

function normalizeSourceIdToUuid(raw) {
  const s = String(raw).trim().toLowerCase();
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(s)) {
    return s;
  }
  const compact = s.replace(/-/g, "");
  if (!/^[0-9a-f]{32}$/.test(compact)) return null;
  return `${compact.slice(0, 8)}-${compact.slice(8, 12)}-${compact.slice(12, 16)}-${compact.slice(16, 20)}-${compact.slice(20)}`;
}

function resolveUuid5PrimaryOrFallbackKey(row, sourceIndex, primaryCol, fallbackCol) {
  const pi = sourceIndex[primaryCol];
  const primary = pi !== undefined ? row[pi] : undefined;
  const hasPrimary = primary != null && String(primary).trim() !== "";
  const useFallback = typeof fallbackCol === "string" && fallbackCol.trim() !== "";

  if (hasPrimary) {
    const val = String(primary).trim();
    return useFallback ? `national_id:${val}` : val;
  }
  if (useFallback) {
    const fi = sourceIndex[fallbackCol.trim()];
    const fallback = fi !== undefined ? row[fi] : undefined;
    if (fallback != null && String(fallback).trim() !== "") {
      return `person_id:${String(fallback).trim()}`;
    }
  }
  return null;
}

function splitRowsGeneric(rows, pipelineConfig, existingKeys, now) {
  const sourceIndex = sourceIndexMap(pipelineConfig);
  const conflictKey = pipelineConfig.conflict_key;
  const conflictTargets = parseConflictColumns(conflictKey);
  let defaultRequire = [];
  if (conflictTargets.length === 1) {
    const srcForConflict = pipelineConfig.column_mapping.find(
      (m) => m.target === conflictTargets[0]
    )?.source;
    defaultRequire = srcForConflict ? [srcForConflict] : [];
  } else if (conflictTargets.length > 1) {
    defaultRequire = conflictTargets
      .map((t) => pipelineConfig.column_mapping.find((m) => m.target === t)?.source)
      .filter(Boolean);
  }
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
    if (pipelineConfig.phone_normalize_ec593 && record.phone != null && String(record.phone).trim() !== "") {
      record.phone = normalizeEcuadorPhone593(record.phone);
    }
    if (Array.isArray(pipelineConfig.boolean_fields)) {
      for (const field of pipelineConfig.boolean_fields) {
        if (Object.prototype.hasOwnProperty.call(record, field)) {
          record[field] = normalizeBoolean(record[field]);
        }
      }
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
    if (Array.isArray(pipelineConfig.json_parse_targets)) {
      for (const field of pipelineConfig.json_parse_targets) {
        const v = record[field];
        if (typeof v === "string" && v.trim() !== "") {
          try {
            record[field] = JSON.parse(v);
          } catch (e) {
            console.warn(
              `json_parse_targets: invalid JSON for ${field}: ${String(e.message || e).slice(0, 160)}`
            );
          }
        }
      }
    }
    if (pipelineConfig.transform_materials_weight_zod && pipelineConfig._saMaterialsCatalog) {
      transformMaterialsWeightTotals(record, pipelineConfig._saMaterialsCatalog);
    }
    if (Array.isArray(pipelineConfig.fill_missing_iso_timestamps)) {
      for (const field of pipelineConfig.fill_missing_iso_timestamps) {
        if (record[field] == null || record[field] === "") {
          record[field] = now;
        }
      }
    }
    if (Array.isArray(pipelineConfig.derived_uuid5)) {
      for (const spec of pipelineConfig.derived_uuid5) {
        const targetCol = spec.target_column;
        const sourceCol = spec.source_column;
        if (!targetCol || !sourceCol) continue;
        const cur = record[targetCol];
        if (cur != null && String(cur).trim() !== "") continue;
        const si = sourceIndex[sourceCol];
        if (si === undefined || row[si] == null || String(row[si]).trim() === "") continue;
        const ns = spec.namespace || UUID_NAMESPACE;
        record[targetCol] = uuidv5(String(row[si]).trim(), ns);
      }
    }
    const idS = pipelineConfig.id_strategy;
    if (idS?.type === "uuid5_from_concat" && Array.isArray(idS.source_columns) && idS.column) {
      const sep = idS.separator != null ? String(idS.separator) : "|";
      const ns = idS.namespace || UUID_NAMESPACE;
      const parts = idS.source_columns.map((col) => {
        const si = sourceIndex[col];
        const v = si !== undefined ? row[si] : undefined;
        return v != null ? String(v).trim() : "";
      });
      if (parts.length && parts.every((p) => p !== "")) {
        const generatedUuid = uuidv5(parts.join(sep), ns);
        record[idS.column] = generatedUuid;
        const dup = idS.duplicate_uuid_to;
        if (dup) {
          const cols = Array.isArray(dup) ? dup : [dup];
          for (const c of cols) {
            if (c && typeof c === "string") record[c] = generatedUuid;
          }
        }
      }
    } else if (idS?.type === "direct_from_source" && idS.source_column && idS.column) {
      const si = sourceIndex[idS.source_column];
      const raw = si !== undefined ? row[si] : undefined;
      if (raw != null && String(raw).trim() !== "") {
        const normalized =
          idS.normalize_to_uuid === false
            ? String(raw).trim()
            : normalizeSourceIdToUuid(raw);
        if (normalized) {
          record[idS.column] = normalized;
        }
      }
    } else if (idS?.type === "uuid5_from_source" && idS.source_column && idS.column) {
      const ns = idS.namespace || UUID_NAMESPACE;
      const key = idS.fallback_source_column
        ? resolveUuid5PrimaryOrFallbackKey(
            row,
            sourceIndex,
            idS.source_column,
            idS.fallback_source_column
          )
        : (() => {
            const si = sourceIndex[idS.source_column];
            const raw = si !== undefined ? row[si] : undefined;
            return raw != null && String(raw).trim() !== "" ? String(raw).trim() : null;
          })();
      if (key) {
        const generatedUuid = uuidv5(key, ns);
        record[idS.column] = generatedUuid;
        const dup = idS.duplicate_uuid_to;
        if (dup) {
          const cols = Array.isArray(dup) ? dup : [dup];
          for (const c of cols) {
            if (c && typeof c === "string") record[c] = generatedUuid;
          }
        }
      }
    }
    if (pipelineConfig.id_strategy?.column && isBlankValue(record[pipelineConfig.id_strategy.column])) {
      continue;
    }
    applyLicensePlateNormalization(record, pipelineConfig);
    if (buildJsonbSpecsList(pipelineConfig).length) {
      applyBuildJsonbFromPipelineConfig(record, row, sourceIndex, pipelineConfig);
    }
    if (Array.isArray(pipelineConfig.json_array_from_source)) {
      for (const spec of pipelineConfig.json_array_from_source) {
        if (!spec || typeof spec !== "object" || !spec.target || !spec.source) continue;
        const si = sourceIndex[spec.source];
        const raw = si !== undefined ? row[si] : undefined;
        if (raw == null || String(raw).trim() === "") continue;
        record[spec.target] = [String(raw).trim()];
      }
    }
    if (pipelineConfig.aggregate_conductor_plates) {
      const aggSpec = pipelineConfig.aggregate_conductor_plates;
      const plateSource = aggSpec.plate_source || "placa";
      const tsSource = aggSpec.timestamp_source || "visitor_updated_at";
      const plateIdx = sourceIndex[plateSource];
      const tsIdx = sourceIndex[tsSource];
      record.__agg_plate = plateIdx !== undefined ? row[plateIdx] : null;
      record.__agg_ts = tsIdx !== undefined ? row[tsIdx] : null;
    }
    if (includeIngested) {
      const tsCol = pipelineConfig.sync_timestamp_column || "_ingested_at";
      record[tsCol] = now;
    }

    if (Array.isArray(pipelineConfig.require_non_null_targets)) {
      let skipTargets = false;
      for (const t of pipelineConfig.require_non_null_targets) {
        if (isBlankValue(record[t])) {
          skipTargets = true;
          break;
        }
      }
      if (skipTargets) continue;
    }

    const ckStr = recordConflictKey(record, conflictKey);
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
    if (telefono && pipelineConfig.phone_normalize_ec593) {
      telefono = normalizeEcuadorPhone593(telefono);
    }

    if (email && seenEmails.has(email)) email = null;
    if (telefono && seenPhones.has(telefono)) telefono = null;

    if (email) seenEmails.add(email);
    if (telefono) seenPhones.add(telefono);

    const record = {};
    for (const mapping of pipelineConfig.column_mapping) {
      const idx = sourceIndex[mapping.source];
      let value = idx !== undefined ? row[idx] : null;
      if (mapping.target === "email") value = email;
      if (mapping.target === "telefono" || mapping.target === "phone") value = telefono;
      record[mapping.target] = value;
    }
    if (pipelineConfig.defaults) {
      Object.assign(record, pipelineConfig.defaults);
    }
    if (pipelineConfig.id_strategy?.type === "uuid5_codigo_transportista") {
      record[pipelineConfig.id_strategy.column] = generateTransportistaId(codigo);
    }
    if (pipelineConfig.include_ingested_at !== false) {
      const tsCol = pipelineConfig.sync_timestamp_column || "_ingested_at";
      record[tsCol] = now;
    }
    if (!record.created_at) {
      record.created_at = now;
    }

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
    throw new Error(
      "Databricks PRD: set DATABRICKS_HOST + DATABRICKS_HTTP_PATH (SAM) or DATABRICKS_PRD_HOST + DATABRICKS_PRD_HTTP_PATH (transportistas_sync/.env)"
    );
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
  const sourceColumns = sourceColumnsList(pipelineConfig).join(", ");
  const whereRaw = pipelineConfig.source_where;
  const whereClause =
    typeof whereRaw === "string" && whereRaw.trim() ? ` WHERE ${whereRaw.trim()} ` : "";
  const statement = `SELECT ${sourceColumns} FROM ${pipelineConfig.source_table}${whereClause}`;
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
      wait_timeout: DBX_WAIT_TIMEOUT,
      disposition: "INLINE",
    }),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Databricks query failed (${res.status}): ${txt.slice(0, 300)}`);
  }

  let payload = await res.json();
  const startedAt = Date.now();
  let state = payload.status?.state || "UNKNOWN";
  let statementId = payload.statement_id;

  // Large fact tables can stay PENDING after initial wait_timeout; poll until terminal state.
  while ((state === "PENDING" || state === "RUNNING") && statementId) {
    if (Date.now() - startedAt > DBX_MAX_WAIT_MS) {
      throw new Error(`Databricks statement timeout after ${DBX_MAX_WAIT_MS}ms (last state: ${state})`);
    }
    await sleep(DBX_POLL_INTERVAL_MS);
    const pollRes = await fetch(`https://${ctx.host}/api/2.0/sql/statements/${statementId}`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    if (!pollRes.ok) {
      const txt = await pollRes.text();
      throw new Error(`Databricks poll failed (${pollRes.status}): ${txt.slice(0, 300)}`);
    }
    payload = await pollRes.json();
    state = payload.status?.state || "UNKNOWN";
  }

  if (state !== "SUCCEEDED") {
    throw new Error(`Databricks statement status: ${state}`);
  }

  const rows = [...(payload.result?.data_array || [])];
  const fetchedChunkIndexes = new Set([payload.result?.chunk_index ?? 0]);

  // Prefer explicit chunk count from manifest when present.
  const totalChunks = payload.manifest?.total_chunk_count || 1;
  for (let chunkIndex = 1; chunkIndex < totalChunks; chunkIndex += 1) {
    if (fetchedChunkIndexes.has(chunkIndex)) continue;
    const chunkPath = `/api/2.0/sql/statements/${statementId}/result/chunks/${chunkIndex}`;
    const chunkRes = await fetch(`https://${ctx.host}${chunkPath}`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    if (!chunkRes.ok) {
      const txt = await chunkRes.text();
      throw new Error(`Databricks chunk fetch failed (${chunkRes.status}): ${txt.slice(0, 300)}`);
    }
    const chunkPayload = await chunkRes.json();
    const chunkRows = chunkPayload.result?.data_array || chunkPayload.data_array || [];
    rows.push(...chunkRows);
    fetchedChunkIndexes.add(chunkPayload.result?.chunk_index ?? chunkPayload.chunk_index ?? chunkIndex);
  }

  // Fallback in case API returns a chain link but missing manifest chunk metadata.
  let nextChunkLink = payload.result?.next_chunk_internal_link || null;
  while (nextChunkLink) {
    const chunkRes = await fetch(`https://${ctx.host}${nextChunkLink}`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    if (!chunkRes.ok) {
      const txt = await chunkRes.text();
      throw new Error(`Databricks chunk fetch failed (${chunkRes.status}): ${txt.slice(0, 300)}`);
    }
    const chunkPayload = await chunkRes.json();
    const chunkIndex = chunkPayload.result?.chunk_index ?? chunkPayload.chunk_index ?? -1;
    if (!fetchedChunkIndexes.has(chunkIndex)) {
      const chunkRows = chunkPayload.result?.data_array || chunkPayload.data_array || [];
      rows.push(...chunkRows);
      fetchedChunkIndexes.add(chunkIndex);
    }
    nextChunkLink = chunkPayload.result?.next_chunk_internal_link || null;
  }

  const expected = payload.manifest?.total_row_count;
  if (typeof expected === "number" && expected !== rows.length) {
    console.warn(
      `Databricks returned ${rows.length} row(s) but manifest reported ${expected}.`
    );
  }
  logInfo(`Fetched ${rows.length} row(s) from Databricks.`);
  return rows;
}

/**
 * Read source rows via PostgREST (same shape as Databricks data_array: array of arrays).
 * Requires `source_supabase_profile` (default|secondary|tertiary) and `source_table` (view/table name).
 * Optional `source_supabase_accept_profile` (e.g. "api") for non-public schemas.
 */
async function fetchSupabaseSourceRows(pipelineConfig) {
  const readProfile = pipelineConfig.source_supabase_profile;
  if (!readProfile || typeof readProfile !== "string") {
    throw new Error(
      "source_kind=supabase requires source_supabase_profile (default | secondary | tertiary)"
    );
  }
  const { baseUrl, serviceKey } = resolveSupabaseByProfile(readProfile);
  const sb = makeSupabaseClient(baseUrl, serviceKey);
  const table = String(pipelineConfig.source_table || "").trim();
  if (!table) {
    throw new Error("source_table is required for Supabase source");
  }
  const cols = sourceColumnsList(pipelineConfig);
  if (!cols.length) {
    throw new Error("column_mapping must list at least one source column");
  }

  const acceptProfile = pipelineConfig.source_supabase_accept_profile;
  const profileHeaders = {};
  if (typeof acceptProfile === "string" && acceptProfile.trim()) {
    const ap = acceptProfile.trim();
    profileHeaders["Accept-Profile"] = ap;
    profileHeaders["Content-Profile"] = ap;
  }

  const rawPage = parseInt(String(pipelineConfig.supabase_source_page_size || "1000"), 10);
  const pageSize = Math.min(Math.max(Number.isFinite(rawPage) ? rawPage : 1000, 1), 5000);
  const orderCol = cols[0];
  const orderParam =
    typeof pipelineConfig.supabase_source_order === "string"
      ? pipelineConfig.supabase_source_order.trim()
      : "";
  const order = orderParam || `${orderCol}.asc`;
  const out = [];
  let offset = 0;

  logInfo(`Querying Supabase source`, { table, profile: readProfile, columns: cols, order });

  while (true) {
    const res = await sb.get(
      table,
      {
        select: cols.join(","),
        order,
      },
      {
        ...profileHeaders,
        Range: `${offset}-${offset + pageSize - 1}`,
      }
    );
    if (![200, 206].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Supabase source read failed (${res.status}): ${txt.slice(0, 400)}`);
    }
    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) {
      break;
    }
    for (const obj of data) {
      out.push(cols.map((c) => (Object.prototype.hasOwnProperty.call(obj, c) ? obj[c] : null)));
    }
    if (data.length < pageSize) {
      break;
    }
    offset += pageSize;
  }

  logInfo(`Fetched ${out.length} row(s) from Supabase.`, { table, profile: readProfile });
  return out;
}

async function getExistingKeys(sb, targetTable, conflictKey) {
  const columns = parseConflictColumns(conflictKey);
  if (!columns.length) {
    return new Set();
  }
  const selectParam = columns.join(",");
  const values = new Set();
  let offset = 0;
  const batch = 1000;

  while (true) {
    const res = await sb.get(
      targetTable,
      { select: selectParam },
      { Range: `${offset}-${offset + batch - 1}` }
    );

    if (![200, 206].includes(res.status)) {
      throw new Error(`Failed to read existing keys: ${res.status}`);
    }

    const data = await res.json();
    if (!data.length) break;
    for (const row of data) {
      if (columns.length === 1) {
        const v = row[columns[0]];
        if (v != null && v !== "") values.add(String(v));
      } else {
        const k = compositeConflictKey(row, columns);
        if (k != null) values.add(k);
      }
    }
    if (data.length < batch) break;
    offset += batch;
  }
  return values;
}

async function loadS3Json(key) {
  if (!ETL_LOGS_BUCKET || !key) return null;
  try {
    const res = await s3Client.send(
      new GetObjectCommand({ Bucket: ETL_LOGS_BUCKET, Key: key })
    );
    const body = await res.Body.transformToString();
    return JSON.parse(body);
  } catch (err) {
    const code = err?.name || err?.Code;
    const status = err?.$metadata?.httpStatusCode;
    if (code === "NoSuchKey" || status === 404) return null;
    throw err;
  }
}

async function saveS3Json(key, data) {
  if (!ETL_LOGS_BUCKET || !key) {
    throw new Error("ETL_LOGS_BUCKET not set; cannot persist baseline snapshot");
  }
  await s3Client.send(
    new PutObjectCommand({
      Bucket: ETL_LOGS_BUCKET,
      Key: key,
      Body: JSON.stringify(data, null, 2),
      ContentType: "application/json",
    })
  );
  return `s3://${ETL_LOGS_BUCKET}/${key}`;
}

/** Normalize business key for incremental_new_keys baseline diff (optional EC plate format). */
function normalizeIncrementalSourceKey(raw, spec) {
  let s = raw != null ? String(raw).trim() : "";
  if (!s) return "";
  if (spec?.normalize_plate) {
    s = normalizeLicensePlateEcDisplay(s);
  }
  return s;
}

async function loadIncrementalBaselineKeys(pipelineConfig) {
  const spec = pipelineConfig.incremental_new_keys;
  if (!spec?.baseline_s3_key || !spec.source_column) {
    return null;
  }
  const payload = await loadS3Json(spec.baseline_s3_key);
  if (!payload || !Array.isArray(payload.keys)) {
    const msg =
      `Missing baseline at s3://${ETL_LOGS_BUCKET}/${spec.baseline_s3_key}. ` +
      "Run the matching scripts/bootstrap*Baseline.js --upload before incremental sync.";
    throw new Error(msg);
  }
  return {
    spec,
    payload,
    keys: new Set(payload.keys.map((k) => String(k))),
  };
}

function filterDatabricksRowsNotInBaseline(rows, pipelineConfig, baselineKeys, spec) {
  const sourceIndex = sourceIndexMap(pipelineConfig);
  const colIdx = sourceIndex[spec.source_column];
  if (colIdx === undefined) {
    throw new Error(`incremental_new_keys.source_column not in SELECT: ${spec.source_column}`);
  }
  const before = rows.length;
  const filtered = rows.filter((row) => {
    const key = normalizeIncrementalSourceKey(row[colIdx], spec);
    return key && !baselineKeys.has(key);
  });
  logInfo("incremental_new_keys baseline filter", {
    source_column: spec.source_column,
    baseline_size: baselineKeys.size,
    rows_before: before,
    rows_after: filtered.length,
    cutover_date: spec.cutover_date || pipelineConfig._baselinePayload?.cutover_date,
  });
  return filtered;
}

async function appendInsertedKeysToBaseline(baselineState, newRows, pipelineConfig, conflictKey) {
  const spec = baselineState.spec;
  if (!spec?.baseline_s3_key || !newRows.length) return null;
  const targetCol =
    pipelineConfig.column_mapping.find((m) => m.source === spec.source_column)?.target ||
    parseConflictColumns(conflictKey)[0];
  let added = 0;
  for (const row of newRows) {
    const raw = targetCol ? row[targetCol] : recordConflictKey(row, conflictKey);
    const key = normalizeIncrementalSourceKey(raw, spec);
    if (key && !baselineState.keys.has(key)) {
      baselineState.keys.add(key);
      added += 1;
    }
  }
  if (!added) return null;
  const updated = {
    cutover_date: spec.cutover_date || baselineState.payload.cutover_date,
    updated_at: new Date().toISOString(),
    keys: [...baselineState.keys].sort(),
  };
  const uri = await saveS3Json(spec.baseline_s3_key, updated);
  logInfo("incremental_new_keys baseline updated", { added, total: updated.keys.length, uri });
  return uri;
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

/** Overwrites one JSON per pipeline — quick proof that last upsert succeeded. */
async function saveLatestPipelineSuccess(logData, fullLogUri) {
  if (!ETL_LOGS_BUCKET) {
    return null;
  }

  const pipelineName = logData.pipeline_name || "unknown";
  const key = `${ETL_SUCCESS_PREFIX}/${pipelineName}/latest.json`;
  const payload = {
    pipeline_name: logData.pipeline_name,
    status: logData.status,
    sync_timestamp: logData.sync_timestamp,
    inserted: logData.inserted,
    updated: logData.updated,
    total: logData.total,
    source_table: logData.source_table,
    dest_table: logData.dest_table,
    write_mode: logData.write_mode,
    conflict_key: logData.conflict_key,
    source_count: logData.source_count,
    dest_count_before: logData.dest_count_before,
    dest_count_after: logData.dest_count_after,
    skipped: logData.skipped,
    quality_stats: logData.quality_stats || null,
    full_log_uri: fullLogUri || null,
  };

  try {
    await s3Client.send(
      new PutObjectCommand({
        Bucket: ETL_LOGS_BUCKET,
        Key: key,
        Body: JSON.stringify(payload, null, 2),
        ContentType: "application/json",
      })
    );
    const uri = `s3://${ETL_LOGS_BUCKET}/${key}`;
    logInfo("Latest success checkpoint saved", { uri });
    return uri;
  } catch (err) {
    console.error("Failed to save latest pipeline checkpoint to S3", err);
    return null;
  }
}

async function saveDomainBatchLog(batchId, logData) {
  if (!ETL_LOGS_BUCKET) {
    console.warn("ETL_LOGS_BUCKET not set, skipping domain batch S3 log.");
    return { detail_uri: null, latest_uri: null };
  }

  const ts = logData.sync_timestamp || new Date().toISOString();
  const datePrefix = ts.slice(0, 10);
  const prefix = (logData.s3_checkpoint_prefix || batchId).replace(/\/$/, "");
  const detailKey = `${prefix}-batch/${datePrefix}/${ts.replace(/:/g, "-")}.json`;
  const latestKey = `${ETL_SUCCESS_PREFIX}/${prefix}/batch/latest.json`;

  const payload = {
    batch_id: batchId,
    label: logData.label || batchId,
    status: logData.status,
    sync_timestamp: ts,
    stop_on_failure: logData.stop_on_failure !== false,
    steps: logData.steps || [],
    warnings: logData.warnings || [],
    failed_step: logData.failed_step || null,
    duration_ms: logData.duration_ms || null,
  };

  try {
    await s3Client.send(
      new PutObjectCommand({
        Bucket: ETL_LOGS_BUCKET,
        Key: detailKey,
        Body: JSON.stringify(payload, null, 2),
        ContentType: "application/json",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: ETL_LOGS_BUCKET,
        Key: latestKey,
        Body: JSON.stringify({ ...payload, full_log_uri: `s3://${ETL_LOGS_BUCKET}/${detailKey}` }, null, 2),
        ContentType: "application/json",
      })
    );
    const detailUri = `s3://${ETL_LOGS_BUCKET}/${detailKey}`;
    const latestUri = `s3://${ETL_LOGS_BUCKET}/${latestKey}`;
    logInfo("Domain batch log saved", { batchId, detailUri, latestUri, status: payload.status });
    return { detail_uri: detailUri, latest_uri: latestUri };
  } catch (err) {
    console.error("Failed to save domain batch log to S3", err);
    return { detail_uri: null, latest_uri: null };
  }
}

function evaluateBatchStepWarnings(stepCfg, pipelineName, result, batchWarningsConfig) {
  const warnings = [];
  const rules = batchWarningsConfig?.[pipelineName];
  if (!rules || typeof rules !== "object") return warnings;

  if (typeof rules.min_total_after === "number" && (result.total ?? 0) < rules.min_total_after) {
    warnings.push({
      pipeline_name: pipelineName,
      target_table: stepCfg.target_table || null,
      code: "min_total_after",
      message: rules.message || `total ${result.total} below min_total_after ${rules.min_total_after}`,
    });
  }
  return warnings;
}

async function runDomainBatch(event) {
  const batchId = event?.domain_batch || event?.batch_name;
  const batchCfg = getDomainBatchConfig(batchId);
  const stopOnFailure = event?.stop_on_failure !== undefined ? !!event.stop_on_failure : batchCfg.stop_on_failure !== false;
  const startedAt = new Date();
  const steps = [];
  const warnings = [];
  let failedStep = null;

  logInfo("Domain batch start", {
    batch_id: batchCfg.batch_id,
    label: batchCfg.label,
    steps: batchCfg.sequence.length,
    stop_on_failure: stopOnFailure,
  });

  for (const step of batchCfg.sequence) {
    const pipelineName = step.pipeline_name;
    if (!pipelineName) {
      const msg = `Batch step missing pipeline_name (order ${step.order ?? "?"})`;
      console.warn(msg);
      warnings.push({ pipeline_name: null, code: "invalid_step", message: msg });
      continue;
    }

    const stepStarted = Date.now();
    logInfo("Domain batch step start", {
      batch_id: batchCfg.batch_id,
      order: step.order,
      pipeline_name: pipelineName,
      target_table: step.target_table,
      reason: step.reason,
    });

    try {
      const result = await runSyncForPipeline({ pipeline_name: pipelineName });
      const stepWarnings = evaluateBatchStepWarnings(step, pipelineName, result, batchCfg.warnings);
      for (const w of stepWarnings) {
        console.warn(`[batch:${batchCfg.batch_id}] ${w.message}`);
        warnings.push(w);
      }

      steps.push({
        order: step.order,
        pipeline_name: pipelineName,
        target_table: step.target_table || result.dest_table || null,
        status: result.status || "success",
        inserted: result.inserted ?? 0,
        updated: result.updated ?? 0,
        total: result.total ?? null,
        duration_ms: Date.now() - stepStarted,
        s3_log_uri: result.s3_log_uri || null,
        s3_latest_uri: result.s3_latest_uri || null,
        warnings: stepWarnings,
      });

      logInfo("Domain batch step complete", {
        batch_id: batchCfg.batch_id,
        pipeline_name: pipelineName,
        status: result.status,
        inserted: result.inserted,
        updated: result.updated,
        total: result.total,
      });
    } catch (error) {
      const message = String(error.message || error);
      console.warn(`[batch:${batchCfg.batch_id}] step failed: ${pipelineName} — ${message}`);
      failedStep = {
        order: step.order,
        pipeline_name: pipelineName,
        target_table: step.target_table || null,
        status: "error",
        error: message,
        duration_ms: Date.now() - stepStarted,
      };
      steps.push(failedStep);

      try {
        const cfg = loadPipelinesConfig()[pipelineName];
        if (cfg) {
          const { baseUrl, serviceKey } = resolveSupabaseForPipeline(cfg);
          const sb = makeSupabaseClient(baseUrl, serviceKey);
          await createRunRecord(sb, pipelineName, "error", { error_message: message });
        }
      } catch (inner) {
        console.warn(`Failed writing etl_runs for batch step ${pipelineName}`, inner);
      }

      if (stopOnFailure) {
        break;
      }
    }
  }

  const finishedAt = new Date();
  const allOk = !failedStep && steps.every((s) => s.status !== "error");
  const batchStatus = allOk ? "success" : failedStep ? "failed" : "partial";

  const summary = {
    batch_id: batchCfg.batch_id,
    label: batchCfg.label,
    status: batchStatus,
    sync_timestamp: finishedAt.toISOString(),
    stop_on_failure: stopOnFailure,
    s3_checkpoint_prefix: batchCfg.s3_checkpoint_prefix || batchCfg.batch_id,
    steps,
    warnings,
    failed_step: failedStep,
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
  };

  const s3 = await saveDomainBatchLog(batchCfg.batch_id, summary);

  if (batchStatus === "failed") {
    const err = new Error(
      `Domain batch '${batchCfg.batch_id}' failed at step ${failedStep.pipeline_name}: ${failedStep.error}`
    );
    err.batchSummary = { ...summary, s3_batch_uri: s3.detail_uri, s3_batch_latest_uri: s3.latest_uri };
    throw err;
  }

  if (warnings.length) {
    console.warn(`[batch:${batchCfg.batch_id}] completed with ${warnings.length} warning(s)`);
  }

  return {
    ...summary,
    s3_batch_uri: s3.detail_uri,
    s3_batch_latest_uri: s3.latest_uri,
  };
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

  if (!SKIP_ETL_WATERMARK) {
    const wmRes = await sb.get(WATERMARK_TABLE, {
      select: "table_name",
      limit: "1",
    });
    if (wmRes.status !== 200) {
      throw new Error(
        `Cannot access '${WATERMARK_TABLE}': ${wmRes.status}. Create ETL tables in this Supabase project (or set SKIP_ETL_WATERMARK=true).`
      );
    }
  }

  const existing = await getExistingKeys(sb, targetTable, conflictKey);
  const countBefore = existing.size;
  logInfo(`Found ${countBefore} existing records in Supabase.`, { targetTable, pipelineName });

  const sourceKind = pipelineConfig.source_kind || "databricks";
  let dbxRows;
  if (sourceKind === "supabase") {
    dbxRows = await fetchSupabaseSourceRows(pipelineConfig);
  } else {
    dbxRows = await fetchDatabricks(pipelineConfig);
  }
  if (!dbxRows.length) {
    return { status: "no_data", inserted: 0, total: countBefore, pipeline_name: pipelineName };
  }
  if (!pipelineConfig.skip_source_deduplicate) {
    dbxRows = deduplicateRows(dbxRows, pipelineConfig);
  }

  let baselineState = null;
  if (pipelineConfig.incremental_new_keys) {
    baselineState = await loadIncrementalBaselineKeys(pipelineConfig);
    pipelineConfig._baselinePayload = baselineState.payload;
    dbxRows = filterDatabricksRowsNotInBaseline(
      dbxRows,
      pipelineConfig,
      baselineState.keys,
      baselineState.spec
    );
    if (!dbxRows.length) {
      logInfo("No new source keys since baseline cutover", {
        pipelineName,
        baseline_size: baselineState.keys.size,
      });
      return {
        status: "no_new_since_baseline",
        inserted: 0,
        total: countBefore,
        pipeline_name: pipelineName,
      };
    }
  }

  let pipelineConfigForSplit = pipelineConfig;
  if (pipelineConfig.transform_materials_weight_zod) {
    const materialsCatalog = await fetchSaMaterialsCatalog(sb);
    logInfo("sa_materials catalog loaded for Zod transform", { size: materialsCatalog.size });
    pipelineConfigForSplit = { ...pipelineConfig, _saMaterialsCatalog: materialsCatalog };
  }

  let { newRows, allRows } = splitRowsByMode(dbxRows, pipelineConfigForSplit, existing, now);
  if (pipelineConfig.aggregate_conductor_plates) {
    const aggSpec = pipelineConfig.aggregate_conductor_plates;
    const beforeAgg = allRows.length;
    let existingById = new Map();
    let existingByNationalId = new Map();
    if (aggSpec.merge_existing !== false) {
      existingById = await fetchExistingDriverPlateFields(
        sb,
        targetTable,
        allRows.map((r) => r.id)
      );
      if (aggSpec.group_by_national_id) {
        existingByNationalId = await fetchExistingDriversByNationalId(
          sb,
          targetTable,
          allRows.map((r) => r[aggSpec.group_by_national_id]),
          aggSpec.group_by_national_id
        );
      }
    }
    allRows = aggregateConductorPlateRecords(allRows, aggSpec, existingById, existingByNationalId);
    newRows = allRows.filter((r) => {
      const k = recordConflictKey(r, conflictKey);
      return k !== "" && !existing.has(k);
    });
    logInfo("aggregate_conductor_plates applied", {
      before: beforeAgg,
      after: allRows.length,
      merge_existing: aggSpec.merge_existing !== false,
      group_by_national_id: aggSpec.group_by_national_id || null,
    });
  }
  if (pipelineConfig.dedupe_records_by) {
    const keys = pipelineConfig.dedupe_records_by;
    const strat = pipelineConfig.dedupe_records_strategy === "first" ? "first" : "last";
    const beforeAll = allRows.length;
    allRows = dedupeRecordsByTargetFields(allRows, keys, { strategy: strat });
    newRows = dedupeRecordsByTargetFields(newRows, keys, { strategy: strat });
    if (allRows.length < beforeAll) {
      logInfo("dedupe_records_by applied", { keys, strategy: strat, before: beforeAll, after: allRows.length });
    }
  }
  logInfo("Sync split", {
    pipeline: pipelineName,
    new: newRows.length,
    skipped: dbxRows.length - newRows.length
  });

  let inserted = 0;
  let updated = 0;
  const rowsToWrite = writeMode === "upsert" ? allRows : newRows;
  const mappedTargets = pipelineConfig.column_mapping.map((m) => m.target);
  const qualityStats = {
    source_fields: buildSourceFieldStats(dbxRows, pipelineConfig),
    rows_to_write_fields: buildRecordFieldStats(rowsToWrite, mappedTargets),
    new_rows_fields: buildRecordFieldStats(newRows, mappedTargets),
  };
  for (let i = 0; i < rowsToWrite.length; i += batchSize) {
    const batch = alignBatchObjectKeys(rowsToWrite.slice(i, i + batchSize));
    const res =
      writeMode === "upsert"
        ? await sb.upsert(targetTable, batch, conflictKey)
        : await sb.post(targetTable, batch, null);
    if (![200, 201].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Write failed: ${res.status} ${txt.slice(0, 300)}`);
    }
    if (writeMode === "upsert") {
      const insertedInBatch = batch.filter((r) => {
        const k = recordConflictKey(r, conflictKey);
        return k !== "" && !existing.has(k);
      }).length;
      inserted += insertedInBatch;
      updated += batch.length - insertedInBatch;
      for (const r of batch) {
        const k = recordConflictKey(r, conflictKey);
        if (k !== "") {
          existing.add(k);
        }
      }
    } else {
      inserted += batch.length;
    }
    logInfo(`Written batch ${Math.floor(i / batchSize) + 1}`, { mode: writeMode, size: batch.length });
  }

  if (pipelineConfig.aggregate_conductor_plates?.dedupe_destination_by_national_id) {
    const dedupeBatches = await deleteStaleDriversByNationalId(
      sb,
      targetTable,
      rowsToWrite,
      pipelineConfig.aggregate_conductor_plates
    );
    logInfo("dedupe_destination_by_national_id applied", { records: rowsToWrite.length, calls: dedupeBatches });
  }

  const postKeys = await getExistingKeys(sb, targetTable, conflictKey);
  const countAfter = postKeys.size;
  const syncTime = new Date().toISOString();

  let baselineS3Uri = null;
  if (baselineState && inserted > 0) {
    baselineS3Uri = await appendInsertedKeysToBaseline(
      baselineState,
      newRows,
      pipelineConfig,
      conflictKey
    );
  }

  if (!SKIP_ETL_WATERMARK) {
    const upsertRes = await sb.upsert(
      WATERMARK_TABLE,
      [{ table_name: pipelineName, last_timestamp: syncTime }],
      "table_name"
    );
    if (![200, 201].includes(upsertRes.status)) {
      console.warn(`Watermark update failed: ${upsertRes.status}`);
    }
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
    quality_stats: qualityStats,
    sync_timestamp: syncTime,
    baseline_s3_uri: baselineS3Uri,
    inserted_records: newRows.slice(0, 200).map((r) => ({
      conflict_key: recordConflictKey(r, conflictKey),
      id: r.id,
    })),
  };
  const s3LogUri = await saveEtlLog(detail);
  const s3LatestUri = await saveLatestPipelineSuccess(detail, s3LogUri);
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
    s3_log_uri: s3LogUri,
    s3_latest_uri: s3LatestUri,
  };
}

exports.lambdaHandler = async (event) => {
  logInfo("Event received", event);
  try {
    if (event?.domain_batch || event?.batch_name) {
      const result = await runDomainBatch(event || {});
      logInfo("Domain batch complete", {
        batch_id: result.batch_id,
        status: result.status,
        steps: result.steps?.length,
        warnings: result.warnings?.length,
      });
      return {
        statusCode: result.status === "success" ? 200 : 207,
        body: JSON.stringify(result),
      };
    }

    const result = await runSyncForPipeline(event || {});
    logInfo("Sync complete", result);
    return {
      statusCode: 200,
      body: JSON.stringify(result),
    };
  } catch (error) {
    console.error("Sync failed", error);
    if (error.batchSummary) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          status: "error",
          message: String(error.message || error),
          batch: error.batchSummary,
        }),
      };
    }
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
