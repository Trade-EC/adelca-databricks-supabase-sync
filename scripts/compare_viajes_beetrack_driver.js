/**
 * Compara conductor entre:
 *   - qas.gldlogistica.db_trade_fact_transportistas_viajes.nombre_conductor
 *   - qas.gldqas.beetrack_routes.driver_name
 *
 * Regla de negocio: el id de expedición / envío NO es comparable entre MTZ viajes y Beetrack.
 * Solo pueden relacionar: (1) placa — placa vs truck_identifier — y (2) driver_identifier en Beetrack
 * (en el snapshot QAS no hay columna en viajes que iguala driver_identifier; se reportan conteos 0).
 *
 * Cruce implementado para métricas: placa normalizada + fecha = dispatch_date (heurístico; puede
 * haber varias rutas Beetrack o varios viajes el mismo día con la misma placa).
 *
 * Uso: node scripts/compare_viajes_beetrack_driver.js
 */
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", "transportistas_sync", ".env") });

const V = "qas.gldlogistica.db_trade_fact_transportistas_viajes";
const B = "qas.gldqas.beetrack_routes";

async function runStatement(statement) {
  const host = process.env.DATABRICKS_QAS_HOST;
  const token = process.env.DATABRICKS_QAS_TOKEN;
  const httpPath = process.env.DATABRICKS_QAS_HTTP_PATH;
  if (!host || !token || !httpPath) {
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
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
  });
  if (!res.ok) throw new Error(await res.text());
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
    throw new Error(JSON.stringify(p.status?.error || p.status));
  }
  return p.result?.data_array || [];
}

async function main() {
  const joinDriverIdCodigo = await runStatement(`
    SELECT COUNT(*) AS c FROM ${V} v
    INNER JOIN ${B} b ON lower(trim(b.driver_identifier)) = lower(trim(v.codigo_unico_transportista))
  `);
  const joinDriverIdRuc = await runStatement(`
    SELECT COUNT(*) AS c FROM ${V} v
    INNER JOIN ${B} b ON lower(trim(b.driver_identifier)) = lower(trim(v.ruc))
  `);
  const joinPlateDateAndDriverId = await runStatement(`
    SELECT COUNT(*) AS c FROM ${V} v
    INNER JOIN ${B} b
      ON upper(replace(trim(v.placa), '-', '')) = upper(replace(trim(b.truck_identifier), '-', ''))
     AND v.fecha = b.dispatch_date
     AND lower(trim(b.driver_identifier)) = lower(trim(v.codigo_unico_transportista))
  `);

  const summarySql = `
WITH v AS (
  SELECT codigo_viaje, nombre_conductor, fecha,
    upper(replace(trim(placa), '-', '')) AS plate_norm
  FROM ${V}
),
b AS (
  SELECT route_id, driver_name, dispatch_date,
    upper(replace(trim(truck_identifier), '-', '')) AS plate_norm
  FROM ${B}
),
j AS (
  SELECT v.codigo_viaje, v.nombre_conductor, b.route_id, b.driver_name
  FROM v INNER JOIN b ON v.plate_norm = b.plate_norm AND v.fecha = b.dispatch_date
),
cmp AS (
  SELECT codigo_viaje, nombre_conductor, driver_name,
    lower(trim(regexp_replace(coalesce(nombre_conductor, ''), '\\\\s+', ' '))) AS n_viajes,
    lower(trim(regexp_replace(coalesce(driver_name, ''), '\\\\s+', ' '))) AS n_beetrack
  FROM j
),
hasj AS (
  SELECT DISTINCT v.codigo_viaje
  FROM v INNER JOIN b ON v.plate_norm = b.plate_norm AND v.fecha = b.dispatch_date
)
SELECT
  (SELECT COUNT(*) FROM ${V}) AS viajes_rows,
  (SELECT COUNT(*) FROM ${B}) AS beetrack_rows,
  (SELECT COUNT(*) FROM j) AS join_pairs_plate_date,
  (SELECT COUNT(DISTINCT codigo_viaje) FROM j) AS viajes_distinct_in_join,
  (SELECT COUNT(DISTINCT route_id) FROM j) AS beetrack_distinct_in_join,
  (SELECT COUNT(*) FROM cmp WHERE n_viajes IS NOT NULL AND n_viajes != '' AND n_beetrack IS NOT NULL AND n_beetrack != '' AND n_viajes = n_beetrack) AS pairs_both_names_equal,
  (SELECT COUNT(*) FROM cmp WHERE n_viajes IS NOT NULL AND n_viajes != '' AND n_beetrack IS NOT NULL AND n_beetrack != '' AND n_viajes <> n_beetrack) AS pairs_both_names_differ,
  (SELECT COUNT(*) FROM cmp WHERE n_viajes IS NULL OR n_viajes = '') AS pairs_viajes_name_blank,
  (SELECT COUNT(*) FROM cmp WHERE n_beetrack IS NULL OR n_beetrack = '') AS pairs_beetrack_name_blank,
  (SELECT SUM(CASE WHEN trim(coalesce(v.nombre_conductor, '')) != '' AND h.codigo_viaje IS NULL THEN 1 ELSE 0 END)
     FROM ${V} v LEFT JOIN hasj h ON v.codigo_viaje = h.codigo_viaje) AS viajes_con_nombre_sin_match_heuristic
`;

  const rows = await runStatement(summarySql);
  const [
    viajes_rows,
    beetrack_rows,
    join_pairs,
    viajes_in_join,
    beetrack_in_join,
    equal_,
    differ_,
    blank_v,
    blank_b,
    nombre_sin_match,
  ] = rows[0];

  const out = {
    business_rule:
      "No hay id de expedición común entre tablas; solo placa (placa ↔ truck_identifier) o driver_identifier en Beetrack pueden vincular; en QAS viajes no expone un campo que iguala driver_identifier (pruebas codigo_unico_transportista / ruc = 0 filas).",
    join_rule:
      "upper(replace(trim(placa),'-','')) + fecha = upper(replace(trim(truck_identifier),'-','')) + dispatch_date",
    driver_identifier_equality_join_rows: {
      driver_identifier_eq_codigo_unico_transportista: Number(joinDriverIdCodigo[0][0]),
      driver_identifier_eq_ruc: Number(joinDriverIdRuc[0][0]),
      plate_and_date_and_driver_identifier_eq_codigo_unico: Number(joinPlateDateAndDriverId[0][0]),
    },
    warning:
      "Cruce por placa+fecha es heurístico: una misma placa+día puede generar varias filas Beetrack o varios viajes; join_pairs puede ser > viajes.",
    viajes_total: Number(viajes_rows),
    beetrack_total: Number(beetrack_rows),
    join_pairs_plate_date: Number(join_pairs),
    viajes_distinct_in_join: Number(viajes_in_join),
    beetrack_distinct_in_join: Number(beetrack_in_join),
    conductor_both_nonnull_normalized_equal: Number(equal_),
    conductor_both_nonnull_normalized_differ: Number(differ_),
    join_pairs_viajes_name_blank: Number(blank_v),
    join_pairs_beetrack_name_blank: Number(blank_b),
    viajes_with_nombre_conductor_but_no_plate_date_match: Number(nombre_sin_match),
  };

  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
