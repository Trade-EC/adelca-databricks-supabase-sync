/**
 * Valida filas CSV (Código de Socio, Socio Adelca, RUC, Código cliente SAP, Nombre Cliente SAP)
 * contra qas.aplicaciones.dim_ferreterias_mtz + dim_grupos_ferreteros_mtz (Databricks QAS).
 *
 * Uso: desde la raíz del repo, con transportistas_sync/.env cargado:
 *   node scripts/validateSocioExcelAgainstMtz.js [ruta/al/archivo.csv]
 *
 * Reglas:
 * - RUC: comparación numérica (se ignoran ceros a la izquierda).
 * - codigo_cliente SAP: comparación numérica con codigo_cliente MTZ.
 * - Si Código cliente SAP viene vacío: match solo por RUC + nombre cliente (trim, lower).
 * - Socio Adelca vs nombre_grupo: lower(trim); no validamos "Código de Socio" (no existe en MTZ).
 */
require("dotenv").config({ path: require("path").join(__dirname, "../transportistas_sync/.env") });

const fs = require("fs");
const path = require("path");

const host = process.env.DATABRICKS_QAS_HOST;
const pat = process.env.DATABRICKS_QAS_TOKEN;
const warehouseId = process.env.DATABRICKS_QAS_HTTP_PATH.split("/").pop();

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

function sqlEsc(s) {
  return String(s ?? "")
    .trim()
    .replace(/'/g, "''");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function runSql(statement) {
  const res = await fetch(`https://${host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${pat}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
  });
  if (!res.ok) throw new Error(await res.text());
  let p = await res.json();
  let st = p.status?.state;
  const id = p.statement_id;
  const t0 = Date.now();
  while ((st === "PENDING" || st === "RUNNING") && id && Date.now() - t0 < 180000) {
    await sleep(2000);
    const pr = await fetch(`https://${host}/api/2.0/sql/statements/${id}`, {
      headers: { Authorization: `Bearer ${pat}` },
    });
    p = await pr.json();
    st = p.status?.state;
  }
  if (st !== "SUCCEEDED") throw new Error(`Databricks: ${st} ${JSON.stringify(p.status)}`);
  const cols = (p.manifest?.schema?.columns || []).map((c) => c.name);
  const rows = p.result?.data_array || [];
  return { cols, rows };
}

async function main() {
  const csvPath = path.join(
    __dirname,
    process.argv[2] || "validate_socio_excel.csv"
  );
  const raw = fs.readFileSync(csvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim() !== "");
  const header = parseCsvLine(lines[0]);
  if (header.length < 5) {
    throw new Error("CSV debe tener al menos 5 columnas en la cabecera");
  }
  const tuples = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length < 5) {
      console.warn("Línea omitida (columnas < 5):", lines[i].slice(0, 120));
      continue;
    }
    const [codSocio, socio, ruc, codSap, nomSap] = cols;
    const codSapTrim = (codSap || "").trim();
    const hasSap = codSapTrim !== "";
    tuples.push({
      line: i + 1,
      cod_socio_excel: (codSocio || "").trim(),
      socio: (socio || "").trim(),
      ruc: (ruc || "").trim(),
      cod_sap: hasSap ? codSapTrim : null,
      nom_sap: (nomSap || "").trim(),
    });
  }

  const valueRows = tuples
    .map(
      (t) =>
        `(${t.line}, '${sqlEsc(t.cod_socio_excel)}', '${sqlEsc(t.socio)}', '${sqlEsc(
          t.ruc
        )}', ${t.cod_sap == null ? "NULL" : `CAST('${sqlEsc(t.cod_sap)}' AS STRING)`}, '${sqlEsc(
          t.nom_sap
        )}')`
    )
    .join(",\n");

  const statement = `
WITH excel(line_no, cod_socio_excel, socio_excel, ruc_excel, cod_sap_excel, nom_sap_excel) AS (
  SELECT * FROM VALUES
${valueRows}
  AS t(line_no, cod_socio_excel, socio_excel, ruc_excel, cod_sap_excel, nom_sap_excel)
),
joined AS (
  SELECT
    e.*,
    f.codigo_cliente AS mtz_codigo_cliente,
    f.ruc AS mtz_ruc,
    f.nombre_cliente AS mtz_nombre_cliente,
    f.id_grupo AS mtz_id_grupo,
    g.nombre_grupo AS mtz_nombre_grupo
  FROM excel e
  LEFT JOIN qas.aplicaciones.dim_ferreterias_mtz f
    ON TRY_CAST(REGEXP_REPLACE(TRIM(f.ruc), '^0+', '') AS BIGINT)
     = TRY_CAST(REGEXP_REPLACE(TRIM(e.ruc_excel), '^0+', '') AS BIGINT)
   AND (
        (e.cod_sap_excel IS NOT NULL AND TRIM(e.cod_sap_excel) <> ''
          AND TRY_CAST(TRIM(f.codigo_cliente) AS BIGINT) = TRY_CAST(TRIM(e.cod_sap_excel) AS BIGINT))
     OR ((e.cod_sap_excel IS NULL OR TRIM(e.cod_sap_excel) = '')
          AND LOWER(TRIM(f.nombre_cliente)) = LOWER(TRIM(e.nom_sap_excel)))
   )
  LEFT JOIN qas.aplicaciones.dim_grupos_ferreteros_mtz g
    ON g.id_grupo = f.id_grupo
)
SELECT
  line_no,
  cod_socio_excel,
  socio_excel,
  ruc_excel,
  cod_sap_excel,
  nom_sap_excel,
  mtz_codigo_cliente,
  mtz_ruc,
  mtz_nombre_cliente,
  mtz_id_grupo,
  mtz_nombre_grupo,
  CASE
    WHEN mtz_codigo_cliente IS NULL THEN 'NO_MATCH_FERRETERIA'
    WHEN LOWER(TRIM(mtz_nombre_grupo)) <> LOWER(TRIM(socio_excel)) THEN 'GRUPO_NOMBRE_DISTINTO'
    ELSE 'OK'
  END AS resultado
FROM joined
ORDER BY line_no
`.trim();

  const { cols, rows } = await runSql(statement);
  const summary = { OK: 0, NO_MATCH_FERRETERIA: 0, GRUPO_NOMBRE_DISTINTO: 0 };
  for (const r of rows) {
    const res = r[r.length - 1];
    summary[res] = (summary[res] || 0) + 1;
  }
  console.log("Resumen (filas CSV):", summary);
  console.log("Columnas:", cols.join(" | "));
  const bad = rows.filter((r) => r[r.length - 1] !== "OK");
  if (bad.length) {
    console.log("\nDetalle filas con problema (max 40):");
    bad.slice(0, 40).forEach((r) => console.log(JSON.stringify(r)));
  } else {
    console.log("\nTodas las filas OK según reglas (ferretería + nombre grupo).");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
