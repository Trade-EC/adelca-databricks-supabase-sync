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

/**
 * Bundled at build time so `/api/dashboard` works on Vercel: `process.cwd()` there is often
 * the monorepo root, not `dashboard-web`, so `fs.readFileSync("pipelines.json")` was missing the file.
 */
export function readPipelines(): PipelineConfig[] {
  return Object.values(pipelinesRaw as PipelinesMap);
}

