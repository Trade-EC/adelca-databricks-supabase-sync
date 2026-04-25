import path from "path";
import { config as loadDotenv } from "dotenv";

loadDotenv({
  path: path.resolve(process.cwd(), "../transportistas_sync/.env"),
});

function req(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env var: ${name}`);
  return value;
}

export const appConfig = {
  region: process.env.AWS_REGION || "us-east-1",
  lambdaName: process.env.LAMBDA_NAME || "patek-philippe",
  supabaseUrl: req("SUPABASE_URL").replace(/\/$/, ""),
  supabaseKey: req("SUPABASE_SERVICE_ROLE_KEY"),
  /** Optional: second Supabase project for pipelines with supabase_profile=secondary */
  supabaseSecondaryUrl: process.env.SUPABASE_SECONDARY_URL?.replace(/\/$/, "") || "",
  supabaseSecondaryKey: process.env.SUPABASE_SECONDARY_SERVICE_ROLE_KEY || "",
  databricksHost: req("DATABRICKS_PRD_HOST"),
  databricksHttpPath: req("DATABRICKS_PRD_HTTP_PATH"),
  databricksClientId: req("DATABRICKS_PRD_CLIENT_ID"),
  databricksClientSecret: req("DATABRICKS_PRD_CLIENT_SECRET"),
  /** QAS (PAT) — optional; required for pipelines with databricks_profile=qas */
  databricksQasHost: process.env.DATABRICKS_QAS_HOST || "",
  databricksQasHttpPath: process.env.DATABRICKS_QAS_HTTP_PATH || "",
  databricksQasToken: process.env.DATABRICKS_QAS_TOKEN || "",
};

