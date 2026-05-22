import { appConfig } from "./config";

export type DatabricksProfile = "prd" | "qas";

/** Keep dashboard waits bounded for typical serverless limits; polling covers cold warehouses after POST returns. */
const POST_WAIT_TIMEOUT = "30s";
const POLL_INTERVAL_MS = 2500;
const MAX_POLL_MS = 90_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

type StatementPayload = {
  statement_id?: string;
  status?: { state?: string; error?: unknown };
  result?: { data_array?: unknown[][] };
};

function resolveConnection(profile: DatabricksProfile): { host: string; warehouseId: string; token: string } {
  if (profile === "qas") {
    if (!appConfig.databricksQasHost || !appConfig.databricksQasHttpPath || !appConfig.databricksQasToken) {
      throw new Error("Configure DATABRICKS_QAS_HOST, DATABRICKS_QAS_HTTP_PATH, DATABRICKS_QAS_TOKEN");
    }
    const warehouseId = appConfig.databricksQasHttpPath.split("/").pop() || "";
    return {
      host: appConfig.databricksQasHost,
      warehouseId,
      token: appConfig.databricksQasToken,
    };
  }
  if (
    !appConfig.databricksHost ||
    !appConfig.databricksHttpPath ||
    !appConfig.databricksClientId ||
    !appConfig.databricksClientSecret
  ) {
    throw new Error(
      "Configure DATABRICKS_PRD_HOST, DATABRICKS_PRD_HTTP_PATH, DATABRICKS_PRD_CLIENT_ID, DATABRICKS_PRD_CLIENT_SECRET"
    );
  }
  const warehouseId = appConfig.databricksHttpPath.split("/").pop() || "";
  return {
    host: appConfig.databricksHost,
    warehouseId,
    token: "", // filled by caller with OAuth
  };
}

/** POST + poll until terminal state (matches Lambda behavior; avoids false failures when status stays PENDING/RUNNING). */
async function executeStatement(
  statement: string,
  profile: DatabricksProfile
): Promise<StatementPayload> {
  const conn = resolveConnection(profile);
  const token =
    profile === "qas" ? conn.token : await getPrdToken();

  const postRes = await fetch(`https://${conn.host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: conn.warehouseId,
      statement,
      wait_timeout: POST_WAIT_TIMEOUT,
      disposition: "INLINE",
    }),
    cache: "no-store",
  });

  const postBody = await postRes.text();
  if (!postRes.ok) {
    throw new Error(
      `Databricks HTTP ${postRes.status} (${profile}): ${postBody.slice(0, 800)}`
    );
  }

  let payload: StatementPayload;
  try {
    payload = JSON.parse(postBody) as StatementPayload;
  } catch {
    throw new Error(`Databricks: invalid JSON (${profile}): ${postBody.slice(0, 300)}`);
  }

  let state = payload.status?.state || "UNKNOWN";
  const statementId = payload.statement_id;
  const pollStarted = Date.now();

  while ((state === "PENDING" || state === "RUNNING") && statementId) {
    if (Date.now() - pollStarted > MAX_POLL_MS) {
      throw new Error(`Databricks statement timeout (${profile}), last state: ${state}`);
    }
    await sleep(POLL_INTERVAL_MS);
    const pollRes = await fetch(`https://${conn.host}/api/2.0/sql/statements/${statementId}`, {
      headers: { Authorization: `Bearer ${token}` },
      cache: "no-store",
    });
    const pollTxt = await pollRes.text();
    if (!pollRes.ok) {
      throw new Error(`Databricks poll HTTP ${pollRes.status} (${profile}): ${pollTxt.slice(0, 600)}`);
    }
    try {
      payload = JSON.parse(pollTxt) as StatementPayload;
    } catch {
      throw new Error(`Databricks poll: invalid JSON (${profile}): ${pollTxt.slice(0, 300)}`);
    }
    state = payload.status?.state || "UNKNOWN";
  }

  if (state !== "SUCCEEDED") {
    const err = payload.status?.error;
    const detail =
      err !== undefined
        ? JSON.stringify(err).slice(0, 800)
        : JSON.stringify(payload.status ?? {}).slice(0, 600);
    throw new Error(`Databricks SQL ${state} (${profile}): ${detail}`);
  }

  return payload;
}

export async function countSourceRows(
  sourceTable: string,
  profile: DatabricksProfile = "prd"
): Promise<number> {
  const payload = await executeStatement(`SELECT COUNT(*) FROM ${sourceTable}`, profile);
  return Number(payload.result?.data_array?.[0]?.[0] || 0);
}

export async function maxSourceAuditCreatedAt(
  sourceTable: string,
  profile: DatabricksProfile = "prd"
): Promise<string | null> {
  try {
    const payload = await executeStatement(
      `SELECT MAX(audit_created_at) FROM ${sourceTable}`,
      profile
    );
    const v = payload.result?.data_array?.[0]?.[0];
    return v != null ? String(v) : null;
  } catch {
    return null;
  }
}
