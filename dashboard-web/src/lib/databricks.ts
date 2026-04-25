import { appConfig } from "./config";

export type DatabricksProfile = "prd" | "qas";

async function getPrdToken() {
  const auth = Buffer.from(
    `${appConfig.databricksClientId}:${appConfig.databricksClientSecret}`
  ).toString("base64");
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    scope: "all-apis",
  });

  const res = await fetch(`https://${appConfig.databricksHost}/oidc/v1/token`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`Databricks OAuth failed (${res.status})`);
  return (await res.json()).access_token as string;
}

async function runStatement(
  statement: string,
  profile: DatabricksProfile
): Promise<{ ok: boolean; payload: unknown }> {
  let host: string;
  let httpPath: string;
  let token: string;

  if (profile === "qas") {
    if (!appConfig.databricksQasHost || !appConfig.databricksQasHttpPath || !appConfig.databricksQasToken) {
      throw new Error("Configure DATABRICKS_QAS_HOST, DATABRICKS_QAS_HTTP_PATH, DATABRICKS_QAS_TOKEN");
    }
    host = appConfig.databricksQasHost;
    httpPath = appConfig.databricksQasHttpPath;
    token = appConfig.databricksQasToken;
  } else {
    host = appConfig.databricksHost;
    httpPath = appConfig.databricksHttpPath;
    token = await getPrdToken();
  }

  const warehouseId = httpPath.split("/").pop();

  const res = await fetch(`https://${host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
    cache: "no-store",
  });
  if (!res.ok) {
    return { ok: false, payload: await res.text() };
  }
  const payload = await res.json();
  return { ok: true, payload };
}

export async function countSourceRows(
  sourceTable: string,
  profile: DatabricksProfile = "prd"
): Promise<number> {
  const { ok, payload } = await runStatement(`SELECT COUNT(*) FROM ${sourceTable}`, profile);
  if (!ok || !payload || typeof payload !== "object") {
    throw new Error(`Databricks SQL failed (${profile})`);
  }
  const p = payload as {
    status?: { state?: string };
    result?: { data_array?: unknown[][] };
  };
  if (p.status?.state !== "SUCCEEDED") {
    throw new Error(`Databricks status: ${p.status?.state || "UNKNOWN"}`);
  }
  return Number(p.result?.data_array?.[0]?.[0] || 0);
}

export async function maxSourceAuditCreatedAt(
  sourceTable: string,
  profile: DatabricksProfile = "prd"
): Promise<string | null> {
  try {
    const { ok, payload } = await runStatement(
      `SELECT MAX(audit_created_at) FROM ${sourceTable}`,
      profile
    );
    if (!ok || !payload || typeof payload !== "object") return null;
    const p = payload as {
      status?: { state?: string };
      result?: { data_array?: unknown[][] };
    };
    if (p.status?.state !== "SUCCEEDED") return null;
    const v = p.result?.data_array?.[0]?.[0];
    return v != null ? String(v) : null;
  } catch {
    return null;
  }
}
