import pipelinesRaw from "../../pipelines.json";

export type ColumnMappingEntry = { source: string; target: string };

export type IdStrategyConfig =
  | { type: "uuid5_codigo_transportista"; column: string }
  | { type: "uuid5_from_source"; column: string; source_column: string; namespace: string };

export type PipelineConfig = {
  pipeline_name: string;
  source_table: string;
  /** Appended as `WHERE ...` on the Databricks SELECT (trusted config only). */
  source_where?: string;
  target_table: string;
  write_mode: "insert_only" | "upsert";
  conflict_key: string;
  batch_size: number;
  schedule: string;
  column_mapping?: ColumnMappingEntry[];
  /** Skip row early if any listed source field is blank (Lambda generic mode). */
  require_non_null?: string[];
  supabase_profile?: "default" | "secondary";
  databricks_profile?: "prd" | "qas";
  row_mode?: "transportistas" | "generic";
  id_strategy?: IdStrategyConfig;
  /** Coalesce null/empty mapped values before further transforms (generic mode). */
  null_coalesce?: Record<string, string | number | boolean | unknown[]>;
  include_ingested_at?: boolean;
  sync_timestamp_column?: string;
  /** Map truthy/falsy from Databricks into booleans (generic mode). */
  boolean_fields?: string[];
  /** Column for MAX() in dashboard (default _ingested_at) */
  datamart_timestamp_column?: string;
  /** When true, do not collapse duplicate keys from Databricks before load (handler). */
  skip_source_deduplicate?: boolean;
  /** After mapping: collapse rows sharing the same value(s) on these target column(s); last wins (generic mode). */
  dedupe_records_by?: string | string[];
  /** With `dedupe_records_by`: "last" (default) or "first" row wins per dedupe key. */
  dedupe_records_strategy?: "last" | "first";
  /** Merged into each row after mapping (Lambda generic mode). */
  defaults?: Record<string, string | number | boolean | unknown[]>;
  /** After null_coalesce: parse these record fields from JSON string to object/array (generic mode). */
  json_parse_targets?: string[];
  /** After json_parse: set ISO timestamp from sync run when field is null/empty (generic mode). */
  fill_missing_iso_timestamps?: string[];
  /** Appended to Databricks SELECT (generic mode); not written unless mapped or derived. */
  extra_source_columns?: string[];
  /** After fill_missing_iso: set target_column to uuid5(row[source]) when target empty (generic mode). */
  derived_uuid5?: { source_column: string; target_column: string; namespace?: string }[];
  /** After full record build: skip row if any listed target field is blank (generic mode). */
  require_non_null_targets?: string[];
  /** After id_strategy: set target jsonb from row columns (generic mode). */
  build_jsonb_object?: BuildJsonbSpec;
  /** Multiple jsonb targets; takes precedence over `build_jsonb_object` when non-empty. */
  build_jsonb_targets?: BuildJsonbSpec[];
};

type BuildJsonbPropertyEntry =
  | { key: string; source_column: string }
  | { key: string; literal?: string | number | boolean | null }
  | { key: string; object_from_columns: Record<string, string> };

export type BuildJsonbSpec = {
  target: string;
  wrap_in_array?: boolean;
  /** Flat object: each value is a Databricks column name. */
  root_object_from_columns?: Record<string, string>;
  properties?: Array<BuildJsonbPropertyEntry>;
};

type PipelinesMap = Record<string, PipelineConfig>;

/** Display order in the dashboard table (unknown names sort last, alphabetically). */
const PIPELINE_ORDER: Record<string, number> = {
  transportistas: 0,
  vehiculos: 1,
  viajes: 2,
  socio_adelca_grupos: 3,
  socio_adelca_ferreterias: 4,
  socio_adelca_facturas_rebate: 5,
  socio_adelca_materiales: 6,
};

/**
 * Bundled at build time so `/api/dashboard` works on Vercel: `process.cwd()` there is often
 * the monorepo root, not `dashboard-web`, so `fs.readFileSync("pipelines.json")` was missing the file.
 */
export function readPipelines(): PipelineConfig[] {
  const list = Object.values(pipelinesRaw as PipelinesMap);
  return list.sort((a, b) => {
    const oa = PIPELINE_ORDER[a.pipeline_name] ?? 100;
    const ob = PIPELINE_ORDER[b.pipeline_name] ?? 100;
    if (oa !== ob) return oa - ob;
    return a.pipeline_name.localeCompare(b.pipeline_name);
  });
}

