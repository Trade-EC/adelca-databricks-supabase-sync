/**
 * Transform sa_invoices JSONB for Zod contracts (socio_adelca_facturas_rebate).
 *
 * Consulta 1 — materials_weight_totals[] (materialsWeightValidation):
 *   material_name (enum Title Case), material_code, kg, subtotal, taxes, total
 *
 * Consulta 2 — materials_categories_weight_totals[] (sin cambio):
 *   category (DB: TREFILADOS_PESADOS), total_weight_kg, total_amount
 */

const REBATE_CATEGORY_TO_ZOD = {
  LAMINADOS: "Laminados",
  PANELADOS: "Panelados",
  PERFILES: "Perfiles",
  TUBOS: "Tubos",
  TREFILADOS_LIVIANOS: "Trefilados Livianos",
  TREFILADOS_PESADOS: "Trefilados Pesados",
  VARILLAS: "Varillas",
};

function dbRebateCategoryToZodName(dbCat) {
  if (dbCat == null || String(dbCat).trim() === "") return null;
  const key = String(dbCat).trim().toUpperCase().replace(/\s+/g, "_");
  return REBATE_CATEGORY_TO_ZOD[key] || null;
}

function aggregateLinesByMaterial(lines) {
  const agg = {};
  for (const ln of lines || []) {
    const code = ln?.material_code;
    if (!code) continue;
    if (!agg[code]) {
      agg[code] = { subtotal: 0, taxes: 0, total: 0, kg: 0 };
    }
    const row = agg[code];
    row.subtotal += Number(ln.base_imponible) || 0;
    row.taxes += Number(ln.iva) || 0;
    row.total += Number(ln.total) || 0;
    row.kg += Number(ln.weight_kg) || 0;
  }
  return agg;
}

function isLegacyMaterialsWeightItem(item) {
  if (!item || typeof item !== "object") return false;
  return (
    Object.prototype.hasOwnProperty.call(item, "material_description") ||
    Object.prototype.hasOwnProperty.call(item, "total_weight_kg") ||
    Object.prototype.hasOwnProperty.call(item, "total_amount")
  );
}

function materialsWeightField(record) {
  if (Object.prototype.hasOwnProperty.call(record, "materials_weight_totals")) {
    return "materials_weight_totals";
  }
  if (Object.prototype.hasOwnProperty.call(record, "materials_totals")) {
    return "materials_totals";
  }
  return "materials_weight_totals";
}

/**
 * @param {Record<string, unknown>} record - sa_invoices row after json_parse
 * @param {Map<string, string>} materialsCatalog - material code → cashback_category (DB)
 */
function transformMaterialsWeightTotals(record, materialsCatalog) {
  const field = materialsWeightField(record);
  const lines = record.lines;
  const fromLines = aggregateLinesByMaterial(Array.isArray(lines) ? lines : []);
  const source = Array.isArray(record[field]) ? record[field] : [];
  const out = [];

  for (const item of source) {
    if (!item || typeof item !== "object") continue;

    // Idempotent: already written in Zod shape
    if (!isLegacyMaterialsWeightItem(item) && item.material_name && item.material_code) {
      out.push({
        material_name: item.material_name,
        material_code: item.material_code,
        kg: Number(item.kg) || 0,
        subtotal: Number(item.subtotal) || 0,
        taxes: Number(item.taxes) || 0,
        total: Number(item.total) || 0,
      });
      continue;
    }

    const code = item.material_code;
    if (!code) continue;
    const dbCat = materialsCatalog.get(String(code));
    const materialName = dbRebateCategoryToZodName(dbCat);
    if (!materialName) {
      console.warn(
        `transformMaterialsWeightTotals: sin categoría para material_code=${code} (invoice ${record.invoice_number || record.id || "?"})`
      );
      continue;
    }
    const line = fromLines[String(code)] || {};
    out.push({
      material_name: materialName,
      material_code: String(code),
      kg: Number(item.total_weight_kg ?? line.kg ?? 0),
      subtotal: Number(line.subtotal ?? 0),
      taxes: Number(line.taxes ?? 0),
      total: Number(item.total_amount ?? line.total ?? 0),
    });
  }

  record[field] = out;
  return record;
}

async function fetchSaMaterialsCatalog(sb, pageSize = 1000) {
  const catalog = new Map();
  let offset = 0;
  while (true) {
    const res = await sb.get("sa_materials", {
      select: "code,cashback_category",
      limit: String(pageSize),
      offset: String(offset),
    });
    if (res.status !== 200) {
      if (res.status === 404) {
        console.warn("fetchSaMaterialsCatalog: sa_materials not found; Zod transform will skip uncategorized materials");
        return catalog;
      }
      throw new Error(`Cannot load sa_materials for Zod transform: ${res.status}`);
    }
    const batch = await res.json();
    if (!Array.isArray(batch) || !batch.length) break;
    for (const row of batch) {
      if (row?.code) {
        catalog.set(String(row.code), row.cashback_category != null ? String(row.cashback_category) : "");
      }
    }
    if (batch.length < pageSize) break;
    offset += pageSize;
  }
  return catalog;
}

module.exports = {
  REBATE_CATEGORY_TO_ZOD,
  dbRebateCategoryToZodName,
  aggregateLinesByMaterial,
  isLegacyMaterialsWeightItem,
  transformMaterialsWeightTotals,
  fetchSaMaterialsCatalog,
};
