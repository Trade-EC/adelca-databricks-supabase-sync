const path = require("path");
const dotenv = require("dotenv");

dotenv.config({ path: path.resolve(process.cwd(), "transportistas_sync/.env") });

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function getConfig() {
  return {
    databricksHost: requireEnv("DATABRICKS_PRD_HOST"),
    databricksHttpPath: requireEnv("DATABRICKS_PRD_HTTP_PATH"),
    databricksClientId: requireEnv("DATABRICKS_PRD_CLIENT_ID"),
    databricksClientSecret: requireEnv("DATABRICKS_PRD_CLIENT_SECRET"),
    supabaseUrl: requireEnv("SUPABASE_URL").replace(/\/$/, ""),
    supabaseKey: requireEnv("SUPABASE_SERVICE_ROLE_KEY"),
    dbxTable:
      process.env.DBX_TABLE || "prod.gldlogistica.db_trade_dim_app_transportistas",
    supabaseTable: process.env.SUPABASE_TABLE || "fleet_owners",
    batchSize: parseInt(process.env.FETCH_SIZE || "500", 10),
    lambdaName: process.env.LAMBDA_NAME || "patek-philippe"
  };
}

module.exports = { getConfig };
