import fs from "fs";
import path from "path";

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
};

type PipelinesMap = Record<string, PipelineConfig>;

export function readPipelines(): PipelineConfig[] {
  const filePath = path.resolve(process.cwd(), "pipelines.json");
  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as PipelinesMap;
  return Object.values(parsed);
}

