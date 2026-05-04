/**
 * Minimal AWS SigV4 signing for Node fetch — avoids @aws-sdk (ERR_REQUIRE_ESM on Vercel / Turbopack).
 * @see https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
 */
import { createHash, createHmac } from "node:crypto";

export type AwsCredentials = {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
};

export function resolveAwsCredentialsFromEnv(): AwsCredentials | null {
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID?.trim();
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY?.trim();
  if (!accessKeyId || !secretAccessKey) return null;
  const sessionToken = process.env.AWS_SESSION_TOKEN?.trim();
  return { accessKeyId, secretAccessKey, ...(sessionToken ? { sessionToken } : {}) };
}

function sha256Hex(data: Buffer | string): string {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data, "utf8");
  return createHash("sha256").update(buf).digest("hex");
}

function hmacSha256(key: Buffer | Uint8Array, data: string): Buffer {
  return createHmac("sha256", key).update(data, "utf8").digest();
}

function signingKey(secret: string, dateStamp: string, region: string, service: string): Buffer {
  const kDate = hmacSha256(Buffer.from(`AWS4${secret}`, "utf8"), dateStamp);
  const kRegion = hmacSha256(kDate, region);
  const kService = hmacSha256(kRegion, service);
  return hmacSha256(kService, "aws4_request");
}

function amzDatetime(now: Date): { amz: string; dateStamp: string } {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const min = String(now.getUTCMinutes()).padStart(2, "0");
  const s = String(now.getUTCSeconds()).padStart(2, "0");
  const dateStamp = `${y}${m}${d}`;
  const amz = `${dateStamp}T${h}${min}${s}Z`;
  return { amz, dateStamp };
}

function encodeRfc3986(s: string): string {
  return encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function canonicalPathSegments(pathname: string): string {
  const trimmed = pathname.startsWith("/") ? pathname.slice(1) : pathname;
  if (!trimmed) return "/";
  const encoded = trimmed
    .split("/")
    .map((p) => encodeRfc3986(p))
    .join("/");
  return `/${encoded}`;
}

function canonicalQuery(searchParams: URLSearchParams): string {
  const pairs: [string, string][] = [];
  for (const [k, v] of searchParams.entries()) {
    pairs.push([encodeRfc3986(k), encodeRfc3986(v)]);
  }
  pairs.sort((a, b) => {
    const c = a[0].localeCompare(b[0]);
    return c !== 0 ? c : a[1].localeCompare(b[1]);
  });
  return pairs.map(([k, v]) => `${k}=${v}`).join("&");
}

/**
 * Signed fetch to an AWS HTTPS endpoint using SigV4.
 */
export async function awsSigv4Fetch(opts: {
  method: string;
  url: string;
  region: string;
  service: "lambda" | "logs" | "s3";
  credentials: AwsCredentials;
  /** Raw body UTF-8; omit for GET after hashing "" */
  body?: string;
  extraHeaders?: Record<string, string>;
}): Promise<Response> {
  const u = new URL(opts.url);
  const now = amzDatetime(new Date());

  const payload = opts.method.toUpperCase() === "GET" || opts.method.toUpperCase() === "HEAD" ? "" : opts.body ?? "";
  const payloadHash = sha256Hex(payload);

  const headersLower: Record<string, string> = {
    host: u.host.toLowerCase(),
    "x-amz-date": now.amz,
    "x-amz-content-sha256": payloadHash,
  };
  if (opts.credentials.sessionToken) {
    headersLower["x-amz-security-token"] = opts.credentials.sessionToken;
  }
  if (opts.extraHeaders) {
    for (const [k, v] of Object.entries(opts.extraHeaders)) {
      headersLower[k.toLowerCase()] = v;
    }
  }

  const signedHeaderKeys = Object.keys(headersLower).sort();
  const canonicalHeaders = signedHeaderKeys.map((k) => `${k}:${headersLower[k].trim()}\n`).join("");
  const signedHeaders = signedHeaderKeys.join(";");

  const cPath = canonicalPathSegments(u.pathname || "/");
  const cQuery = canonicalQuery(u.searchParams);

  const canonicalRequest = [
    opts.method.toUpperCase(),
    cPath,
    cQuery,
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");

  const hashedCanonical = sha256Hex(canonicalRequest);
  const scope = `${now.dateStamp}/${opts.region}/${opts.service}/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", now.amz, scope, hashedCanonical].join("\n");

  const sigKey = signingKey(opts.credentials.secretAccessKey, now.dateStamp, opts.region, opts.service);
  const signature = createHmac("sha256", sigKey).update(stringToSign, "utf8").digest("hex");

  const auth =
    `AWS4-HMAC-SHA256 Credential=${opts.credentials.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const outHeaders = new Headers();
  outHeaders.set("Authorization", auth);
  for (const k of signedHeaderKeys) {
    outHeaders.set(k, headersLower[k]);
  }

  const method = opts.method.toUpperCase();
  const init: RequestInit = {
    method,
    headers: outHeaders,
    ...(method !== "GET" && method !== "HEAD" ? { body: payload } : {}),
  };

  return fetch(opts.url, init);
}
