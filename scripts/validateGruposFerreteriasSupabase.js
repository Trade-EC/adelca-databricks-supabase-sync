/**
 * Valida un CSV maestro grupo→ferreterías contra Supabase (sin Databricks).
 *
 * CSV esperado (cabecera flexible):
 *   CODIGO_GRUPO,NOMBRE_GRUPO,CODIGO_CLIENTE,NOMBRE_CLIENTE,RUC,...
 *
 * Uso (desde la raíz del repo, con transportistas_sync/.env):
 *   node scripts/validateGruposFerreteriasSupabase.js [ruta/al/archivo.csv]
 *   VALIDATE_SUPABASE_PROFILE=base_socio node scripts/validateGruposFerreteriasSupabase.js data.csv
 *   node scripts/validateGruposFerreteriasSupabase.js data.csv --out scripts/data/validacion_mismatch.csv
 *
 * Perfil Supabase: VALIDATE_SUPABASE_PROFILE=base_socio|secondary (default: base_socio)
 *
 * Reglas por fila:
 * - Ferretería: uuid5(codigo_cliente) = sa_hardware_stores.id, o fallback RUC normalizado, o nombre (trim lower).
 * - Grupo: nombre en sa_hardware_store_groups (vía store_group_id) vs NOMBRE_GRUPO (trim lower).
 * - CODIGO_GRUPO: uuid5(codigo_grupo) debe coincidir con store_group_id si la tienda existe.
 */
require("dotenv").config({ path: require("path").join(__dirname, "../transportistas_sync/.env") });

const fs = require("fs");
const path = require("path");
const { v5: uuidv5 } = require("uuid");

const UUID_NAMESPACE = "b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b";

const PROFILE = (process.env.VALIDATE_SUPABASE_PROFILE || "base_socio").trim();

function credsForProfile(profile) {
  if (profile === "secondary") {
    return {
      url: process.env.SUPABASE_SECONDARY_URL,
      key: process.env.SUPABASE_SECONDARY_SERVICE_ROLE_KEY,
    };
  }
  if (profile === "base_socio") {
    return {
      url: process.env.SUPABASE_BASE_SOCIO_URL,
      key: process.env.SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY,
    };
  }
  throw new Error(`VALIDATE_SUPABASE_PROFILE no soportado: ${profile} (use base_socio o secondary)`);
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inq = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      inq = !inq;
      continue;
    }
    if (!inq && c === ",") {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += c;
  }
  out.push(cur);
  return out;
}

function normTaxId(raw) {
  const s = raw != null ? String(raw).trim() : "";
  if (!s) return "";
  if (!/^\d+$/.test(s)) return s;
  const stripped = s.replace(/^0+/, "");
  return stripped === "" ? "0" : stripped;
}

function normName(raw) {
  return String(raw ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normKey(raw) {
  return String(raw ?? "").trim();
}

function headerIndex(header, aliases) {
  const h = header.map((x) => normName(x));
  for (const a of aliases) {
    const i = h.indexOf(normName(a));
    if (i >= 0) return i;
  }
  return -1;
}

async function fetchAllSupabase(baseUrl, key, table, select) {
  const rows = [];
  let offset = 0;
  const pageSize = 1000;
  while (true) {
    const q = new URL(`${baseUrl}/rest/v1/${table}`);
    q.searchParams.set("select", select);
    const res = await fetch(q, {
      headers: {
        apikey: key,
        Authorization: `Bearer ${key}`,
        Range: `${offset}-${offset + pageSize - 1}`,
      },
    });
    if (!res.ok) {
      throw new Error(`${table} HTTP ${res.status}: ${(await res.text()).slice(0, 400)}`);
    }
    const batch = await res.json();
    if (!Array.isArray(batch) || batch.length === 0) break;
    rows.push(...batch);
    if (batch.length < pageSize) break;
    offset += pageSize;
  }
  return rows;
}

function buildSupabaseIndexes(stores, groups) {
  const groupById = new Map(groups.map((g) => [g.id, g]));
  const byStoreId = new Map();
  const byTax = new Map();
  const byName = new Map();

  for (const s of stores) {
    if (s.deleted_at != null) continue;
    byStoreId.set(s.id, s);
    const tk = normTaxId(s.tax_id);
    if (tk) {
      if (!byTax.has(tk)) byTax.set(tk, []);
      byTax.get(tk).push(s);
    }
    const nk = normName(s.name);
    if (nk) {
      if (!byName.has(nk)) byName.set(nk, []);
      byName.get(nk).push(s);
    }
  }

  return { groupById, byStoreId, byTax, byName };
}

function resolveStore(row, indexes) {
  const { byStoreId, byTax, byName } = indexes;
  const expectedId = row.codigo_cliente
    ? uuidv5(normKey(row.codigo_cliente), UUID_NAMESPACE)
    : null;
  const expectedGroupId = row.codigo_grupo
    ? uuidv5(normKey(row.codigo_grupo), UUID_NAMESPACE)
    : null;

  if (expectedId && byStoreId.has(expectedId)) {
    return {
      store: byStoreId.get(expectedId),
      match: "codigo_cliente_uuid",
      expectedId,
      expectedGroupId,
    };
  }

  const tk = normTaxId(row.ruc);
  if (tk && byTax.has(tk)) {
    const candidates = byTax.get(tk);
    if (candidates.length === 1) {
      return {
        store: candidates[0],
        match: "ruc",
        expectedId,
        expectedGroupId,
      };
    }
    if (expectedId) {
      const byId = candidates.find((s) => s.id === expectedId);
      if (byId) {
        return { store: byId, match: "ruc+uuid", expectedId, expectedGroupId };
      }
    }
    return {
      store: candidates[0],
      match: "ruc_ambiguo",
      expectedId,
      expectedGroupId,
      altCount: candidates.length,
    };
  }

  const nk = normName(row.nombre_cliente);
  if (nk && byName.has(nk)) {
    const candidates = byName.get(nk);
    return {
      store: candidates[0],
      match: candidates.length > 1 ? "nombre_ambiguo" : "nombre",
      expectedId,
      expectedGroupId,
      altCount: candidates.length,
    };
  }

  return { store: null, match: "ninguno", expectedId, expectedGroupId };
}

function classifyRow(row, indexes, resolved) {
  const { store, match, expectedId, expectedGroupId, altCount } = resolved;
  if (!store) {
    return {
      resultado: "NO_EN_SUPABASE",
      detalle: "sin match por codigo_cliente, RUC ni nombre",
      match,
    };
  }

  const g = indexes.groupById.get(store.store_group_id);
  const sbGroupName = g?.name ?? "";
  const csvGroup = normName(row.nombre_grupo);
  const sbGroupNorm = normName(sbGroupName);

  const pieces = [];

  const tkCsv = normTaxId(row.ruc);
  const tkSb = normTaxId(store.tax_id);
  if (tkCsv && tkSb && tkCsv !== tkSb) {
    pieces.push("ruc_distinto");
  }

  const groupNameOk = !csvGroup || !sbGroupNorm || csvGroup === sbGroupNorm;
  const storeIdOk = !expectedId || store.id === expectedId;
  const codigoGrupoUuidOk =
    !expectedGroupId || store.store_group_id === expectedGroupId;

  if (!groupNameOk) {
    return {
      resultado: "GRUPO_NOMBRE_DISTINTO",
      detalle: `CSV "${row.nombre_grupo}" vs SB "${sbGroupName}"`,
      match,
      sb_group_name: sbGroupName,
      sb_store_name: store.name,
    };
  }

  if (!storeIdOk && match !== "codigo_cliente_uuid") {
    pieces.push("id_distinto_del_codigo_cliente");
  }
  if (!codigoGrupoUuidOk) {
    pieces.push("codigo_grupo_csv_no_es_id_grupo_mtz");
  }
  if (match === "ruc" || match === "ruc_ambiguo") {
    pieces.push("match_solo_por_ruc");
  }
  if (match === "nombre" || match === "nombre_ambiguo") {
    pieces.push("match_solo_por_nombre");
  }
  if (altCount && altCount > 1) {
    pieces.push(`candidatos_${altCount}`);
  }

  if (storeIdOk && groupNameOk && pieces.length === 0) {
    return {
      resultado: "OK",
      detalle: match,
      match,
      sb_group_name: sbGroupName,
      sb_store_name: store.name,
    };
  }

  if (storeIdOk && groupNameOk && pieces.every((p) => p === "codigo_grupo_csv_no_es_id_grupo_mtz")) {
    return {
      resultado: "OK",
      detalle: `${match}; codigo_grupo CSV ≠ id_grupo MTZ (nombre grupo OK)`,
      match,
      sb_group_name: sbGroupName,
      sb_store_name: store.name,
    };
  }

  if (pieces.length) {
    return {
      resultado: "EN_SB_CON_RESERVAS",
      detalle: pieces.join("; "),
      match,
      sb_group_name: sbGroupName,
      sb_store_name: store.name,
    };
  }

  return {
    resultado: "OK",
    detalle: match,
    match,
    sb_group_name: sbGroupName,
    sb_store_name: store.name,
  };
}

function parseArgs(argv) {
  const positional = [];
  let outPath = null;
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--out" && argv[i + 1]) {
      outPath = argv[++i];
      continue;
    }
    positional.push(argv[i]);
  }
  return {
    csvPath: positional[0] || path.join(__dirname, "data", "maestro_grupos_ferreterias.csv"),
    outPath,
  };
}

async function main() {
  const { csvPath, outPath } = parseArgs(process.argv);
  const { url, key } = credsForProfile(PROFILE);
  if (!url || !key) {
    throw new Error(
      `Faltan credenciales para perfil ${PROFILE} (SUPABASE_${PROFILE === "secondary" ? "SECONDARY" : "BASE_SOCIO"}_URL / _SERVICE_ROLE_KEY)`
    );
  }

  if (!fs.existsSync(csvPath)) {
    throw new Error(
      `No existe el CSV: ${csvPath}\nGuarde su export como scripts/data/maestro_grupos_ferreterias.csv o pase la ruta como argumento.`
    );
  }

  const host = new URL(url).hostname;
  console.log(`Supabase perfil: ${PROFILE} (${host})`);
  console.log(`CSV: ${csvPath}`);

  const [groups, stores] = await Promise.all([
    fetchAllSupabase(url, key, "sa_hardware_store_groups", "id,name,deleted_at"),
    fetchAllSupabase(url, key, "sa_hardware_stores", "id,store_group_id,name,tax_id,deleted_at"),
  ]);

  const activeGroups = groups.filter((g) => g.deleted_at == null);
  const activeStores = stores.filter((s) => s.deleted_at == null);
  const indexes = buildSupabaseIndexes(activeStores, activeGroups);

  console.log(
    `Cargado: ${activeGroups.length} grupos, ${activeStores.length} ferreterías activas en Supabase\n`
  );

  const raw = fs.readFileSync(csvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length < 2) throw new Error("CSV vacío o sin datos");

  const header = parseCsvLine(lines[0]);
  const iGrupo = headerIndex(header, ["codigo_grupo", "código_grupo", "codigo grupo"]);
  const iNomGrupo = headerIndex(header, ["nombre_grupo", "nombre grupo", "socio adelca"]);
  const iCliente = headerIndex(header, ["codigo_cliente", "código_cliente", "codigo cliente", "codigo cliente sap"]);
  const iNomCliente = headerIndex(header, ["nombre_cliente", "nombre cliente", "nombre cliente sap"]);
  const iRuc = headerIndex(header, ["ruc"]);

  if (iGrupo < 0 || iCliente < 0 || iRuc < 0) {
    throw new Error(
      `Cabecera no reconocida. Se requiere CODIGO_GRUPO, CODIGO_CLIENTE y RUC. Cabecera: ${header.join(",")}`
    );
  }

  const summary = {};
  const results = [];
  const rucToCsvRows = new Map();

  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length < Math.max(iGrupo, iCliente, iRuc) + 1) continue;

    const row = {
      line: i + 1,
      codigo_grupo: normKey(cols[iGrupo]),
      nombre_grupo: iNomGrupo >= 0 ? normKey(cols[iNomGrupo]) : "",
      codigo_cliente: normKey(cols[iCliente]),
      nombre_cliente: iNomCliente >= 0 ? normKey(cols[iNomCliente]) : "",
      ruc: normKey(cols[iRuc]),
    };

    const tk = normTaxId(row.ruc);
    if (tk) {
      if (!rucToCsvRows.has(tk)) rucToCsvRows.set(tk, []);
      rucToCsvRows.get(tk).push(row.line);
    }

    const resolved = resolveStore(row, indexes);
    const cls = classifyRow(row, indexes, resolved);
    summary[cls.resultado] = (summary[cls.resultado] || 0) + 1;

    results.push({
      line: row.line,
      codigo_grupo: row.codigo_grupo,
      nombre_grupo: row.nombre_grupo,
      codigo_cliente: row.codigo_cliente,
      nombre_cliente: row.nombre_cliente,
      ruc: row.ruc,
      resultado: cls.resultado,
      match: cls.match,
      detalle: cls.detalle,
      sb_store_name: cls.sb_store_name || "",
      sb_group_name: cls.sb_group_name || "",
      expected_store_id: resolved.expectedId || "",
      sb_store_id: resolved.store?.id || "",
    });
  }

  let csvDupRuc = 0;
  for (const [, linesWithRuc] of rucToCsvRows) {
    if (linesWithRuc.length > 1) csvDupRuc++;
  }

  console.log("Resumen filas CSV:", summary);
  console.log(`RUC repetidos en CSV (varios codigo_cliente): ${csvDupRuc} RUC distintos`);

  const bad = results.filter((r) => r.resultado !== "OK");
  if (bad.length) {
    console.log(`\nFilas con problema: ${bad.length} (muestra max 25):`);
    for (const r of bad.slice(0, 25)) {
      console.log(
        `  L${r.line} ${r.resultado} | ${r.codigo_grupo} / ${r.codigo_cliente} | ${r.detalle}`
      );
      if (r.sb_store_name) {
        console.log(`       SB: "${r.sb_store_name}" → grupo "${r.sb_group_name}"`);
      }
    }
  } else {
    console.log("\nTodas las filas CSV OK contra Supabase.");
  }

  if (outPath) {
    const hdr = [
      "line",
      "codigo_grupo",
      "nombre_grupo",
      "codigo_cliente",
      "nombre_cliente",
      "ruc",
      "resultado",
      "match",
      "detalle",
      "sb_store_name",
      "sb_group_name",
      "expected_store_id",
      "sb_store_id",
    ];
    const esc = (v) => {
      const s = String(v ?? "");
      return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const body = results.map((r) => hdr.map((k) => esc(r[k])).join(",")).join("\n");
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, `${hdr.join(",")}\n${body}\n`, "utf8");
    console.log(`\nReporte escrito: ${outPath}`);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
