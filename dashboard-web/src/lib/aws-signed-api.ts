/** Lambda / CloudWatch / S3 via SigV4 fetch — no @aws-sdk. */
import { awsSigv4Fetch, resolveAwsCredentialsFromEnv } from "./aws-sigv4";

export type LambdaDashboardInfo = {
  Runtime?: string;
  MemorySize?: number;
  Timeout?: number;
  State?: string;
};

export async function fetchLambdaDashboardInfo(region: string, functionName: string): Promise<{
  ok: true;
  data: LambdaDashboardInfo | null;
} | { ok: false; status: number; text: string }> {
  const creds = resolveAwsCredentialsFromEnv();
  if (!creds) return { ok: false, status: 0, text: "missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY" };

  const path = `/2015-03-31/functions/${encodeURIComponent(functionName).replace(/%2F/g, "/")}/configuration`;
  const url = `https://lambda.${region}.amazonaws.com${path}`;
  const res = await awsSigv4Fetch({
    method: "GET",
    url,
    region,
    service: "lambda",
    credentials: creds,
  });
  const text = await res.text();
  if (!res.ok) return { ok: false, status: res.status, text: text.slice(0, 400) };

  try {
    const j = JSON.parse(text) as Record<string, unknown>;
    const data: LambdaDashboardInfo = {
      Runtime: j.Runtime != null ? String(j.Runtime) : undefined,
      MemorySize: typeof j.MemorySize === "number" ? j.MemorySize : undefined,
      Timeout: typeof j.Timeout === "number" ? j.Timeout : undefined,
      State: j.State != null ? String(j.State) : undefined,
    };
    return { ok: true, data };
  } catch {
    return { ok: false, status: res.status, text: "Lambda config: invalid JSON" };
  }
}

export type LogStreamSummary = {
  logStreamName?: string;
  lastEventTimestamp?: number;
};

export async function fetchRecentLambdaLogStreams(
  region: string,
  functionName: string,
  limit = 10
): Promise<{ ok: true; streams: LogStreamSummary[] } | { ok: false; status: number; text: string }> {
  const creds = resolveAwsCredentialsFromEnv();
  if (!creds) return { ok: false, status: 0, text: "missing AWS credentials" };

  const url = `https://logs.${region}.amazonaws.com/`;
  const body = JSON.stringify({
    logGroupName: `/aws/lambda/${functionName}`,
    orderBy: "LastEventTime",
    descending: true,
    limit,
  });

  const res = await awsSigv4Fetch({
    method: "POST",
    url,
    region,
    service: "logs",
    credentials: creds,
    body,
    extraHeaders: {
      "Content-Type": "application/x-amz-json-1.1",
      "X-Amz-Target": "Logs_20140328.DescribeLogStreams",
    },
  });
  const text = await res.text();
  if (!res.ok) return { ok: false, status: res.status, text: text.slice(0, 400) };

  try {
    const j = JSON.parse(text) as { logStreams?: LogStreamSummary[] };
    const streams = (j.logStreams || []).map((s) => ({
      logStreamName: s.logStreamName ? String(s.logStreamName) : undefined,
      lastEventTimestamp:
        typeof s.lastEventTimestamp === "number" ? s.lastEventTimestamp : undefined,
    }));
    return { ok: true, streams };
  } catch {
    return { ok: false, status: res.status, text: "CW Logs: invalid JSON" };
  }
}

export async function invokeLambdaPipeline(
  region: string,
  functionName: string,
  payload: Record<string, unknown>
): Promise<{ ok: true; body: unknown; functionError: boolean } | { ok: false; status: number; text: string }> {
  const creds = resolveAwsCredentialsFromEnv();
  if (!creds) return { ok: false, status: 0, text: "missing AWS credentials" };

  const path = `/2015-03-31/functions/${encodeURIComponent(functionName).replace(/%2F/g, "/")}/invocations`;
  const url = `https://lambda.${region}.amazonaws.com${path}`;
  const bodyStr = JSON.stringify(payload);

  const res = await awsSigv4Fetch({
    method: "POST",
    url,
    region,
    service: "lambda",
    credentials: creds,
    body: bodyStr,
    extraHeaders: {
      "Content-Type": "application/json",
      "X-Amz-Invocation-Type": "RequestResponse",
    },
  });
  const txt = await res.text();
  if (!res.ok) return { ok: false, status: res.status, text: txt.slice(0, 500) };

  let parsed: unknown = txt;
  try {
    parsed = JSON.parse(txt) as unknown;
  } catch {
    /* Lambda may return opaque string payload */
  }
  const functionError = !!res.headers.get("X-Amz-Function-Error");
  return { ok: true, body: parsed, functionError };
}

export async function s3GetText(region: string, bucket: string, key: string): Promise<{
  ok: true;
  text: string;
} | { ok: false; status: number }> {
  const creds = resolveAwsCredentialsFromEnv();
  if (!creds) return { ok: false, status: 0 };

  const encKey = key
    .split("/")
    .map((seg) => encodeURIComponent(seg).replace(/%2F/g, "/"))
    .join("/");

  /** Virtual-hosted endpoint (Regional). */
  const host = `${bucket}.s3.${region}.amazonaws.com`;
  const url = `https://${host}/${encKey}`;
  const res = await awsSigv4Fetch({
    method: "GET",
    url,
    region,
    service: "s3",
    credentials: creds,
  });

  const text = await res.text();
  if (!res.ok) return { ok: false, status: res.status };
  return { ok: true, text };
}
