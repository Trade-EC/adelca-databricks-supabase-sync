import pipelinesRaw from "../../pipelines.json";

export type PipelineConfig = {
  pipeline_name: string;
  source_table: string;
  target_table: string;
  write_mode: "insert_only" | "upsert";
  conflict_key: string;
  batch_size: number;
  schedule: string;
  supabase_profile?: "default" | "secondary";
  databricks_profile?: "prd" | "qas";
  row_mode?: "transportistas" | "generic";
  /** Column for MAX() in dashboard (default _ingested_at) */
  datamart_timestamp_column?: string;
  /** When true, do not collapse duplicate keys from Databricks before load (handler). */
  skip_source_deduplicate?: boolean;
};

type PipelinesMap = Record<string, PipelineConfig>;

/** Display order in the dashboard table (unknown names sort last, alphabetically). */
const PIPELINE_ORDER: Record<string, number> = {
  transportistas: 0,
  vehiculos: 1,
  viajes: 2,
  socio_adelca_ferreterias: 3,
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

