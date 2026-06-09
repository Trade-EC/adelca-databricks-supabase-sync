import path from "path";
import { config as loadDotenv } from "dotenv";

loadDotenv({
  path: path.resolve(process.cwd(), "../transportistas_sync/.env"),
});

/**
 * Dashboard must never throw during module init: Vercel may only set SUPABASE_SECONDARY_*,
 * omit PRD defaults, SUPABASE_TERTIARY_* for tertiary-only setups, or lack AWS_* — failures belong in route handlers / per-pipeline.
 */
export const appConfig = {
  region: process.env.AWS_REGION || "us-east-1",
  lambdaName: process.env.LAMBDA_NAME || "patek-philippe",
  /** Same bucket as Lambda ETL_LOGS_BUCKET; enables last-run from S3 if etl_runs is missing. */
  etlLogsBucket: process.env.ETL_LOGS_BUCKET || "",
  etlSuccessPrefix: (process.env.ETL_SUCCESS_PREFIX || "etl-success").replace(/\/$/, ""),
  supabaseUrl: (process.env.SUPABASE_URL || "").replace(/\/$/, ""),
  supabaseKey: process.env.SUPABASE_SERVICE_ROLE_KEY || "",
  /** Optional: second Supabase project for pipelines with supabase_profile=secondary */
  supabaseSecondaryUrl: process.env.SUPABASE_SECONDARY_URL?.replace(/\/$/, "") || "",
  supabaseSecondaryKey: process.env.SUPABASE_SECONDARY_SERVICE_ROLE_KEY || "",
  /** Optional: third Supabase project for pipelines with supabase_profile=tertiary */
  supabaseTertiaryUrl: process.env.SUPABASE_TERTIARY_URL?.replace(/\/$/, "") || "",
  supabaseTertiaryKey: process.env.SUPABASE_TERTIARY_SERVICE_ROLE_KEY || "",
  /** Base Socio — producción socio Adelca + cartera (pipelines con supabase_profile=base_socio) */
  supabaseBaseSocioUrl: process.env.SUPABASE_BASE_SOCIO_URL?.replace(/\/$/, "") || "",
  supabaseBaseSocioKey: process.env.SUPABASE_BASE_SOCIO_SERVICE_ROLE_KEY || "",
  databricksHost: process.env.DATABRICKS_PRD_HOST || "",
  databricksHttpPath: process.env.DATABRICKS_PRD_HTTP_PATH || "",
  databricksClientId: process.env.DATABRICKS_PRD_CLIENT_ID || "",
  databricksClientSecret: process.env.DATABRICKS_PRD_CLIENT_SECRET || "",
  /** QAS (PAT) — optional; required for pipelines with databricks_profile=qas */
  databricksQasHost: process.env.DATABRICKS_QAS_HOST || "",
  databricksQasHttpPath: process.env.DATABRICKS_QAS_HTTP_PATH || "",
  databricksQasToken: process.env.DATABRICKS_QAS_TOKEN || "",
};
