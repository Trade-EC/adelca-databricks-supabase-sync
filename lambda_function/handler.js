const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
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
    } else if (idS?.type === "uuid5_from_source" && idS.source_column && idS.column) {
      const si = sourceIndex[idS.source_column];
      const ns = idS.namespace || UUID_NAMESPACE;
      if (si !== undefined && row[si] != null) {
        const generatedUuid = uuidv5(String(row[si]).trim(), ns);
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

  let pipelineConfigForSplit = pipelineConfig;
  if (pipelineConfig.transform_materials_weight_zod) {
    const materialsCatalog = await fetchSaMaterialsCatalog(sb);
    logInfo("sa_materials catalog loaded for Zod transform", { size: materialsCatalog.size });
    pipelineConfigForSplit = { ...pipelineConfig, _saMaterialsCatalog: materialsCatalog };
  }

  let { newRows, allRows } = splitRowsByMode(dbxRows, pipelineConfigForSplit, existing, now);
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

  const postKeys = await getExistingKeys(sb, targetTable, conflictKey);
  const countAfter = postKeys.size;
  const syncTime = new Date().toISOString();

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
