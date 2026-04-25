import { appConfig } from "./config";

export type SupabaseProfile = "default" | "secondary";

function credsForProfile(profile: SupabaseProfile = "default") {
  if (profile === "secondary") {
    if (!appConfig.supabaseSecondaryUrl || !appConfig.supabaseSecondaryKey) {
      return null;
    }
    return {
      supabaseUrl: appConfig.supabaseSecondaryUrl,
      supabaseKey: appConfig.supabaseSecondaryKey,
    };
  }
  return { supabaseUrl: appConfig.supabaseUrl, supabaseKey: appConfig.supabaseKey };
}

function headersForProfile(profile: SupabaseProfile, extra?: Record<string, string>) {
  const c = credsForProfile(profile);
  if (!c) throw new Error("Secondary Supabase not configured (SUPABASE_SECONDARY_*)");
  return {
    apikey: c.supabaseKey,
    Authorization: `Bearer ${c.supabaseKey}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

export async function supabaseCount(table: string, profile: SupabaseProfile = "default"): Promise<number | null> {
  const c = credsForProfile(profile);
  if (!c) return null;
  const res = await fetch(
    `${c.supabaseUrl}/rest/v1/${table}?select=${encodeURIComponent("*")}`,
    {
      headers: headersForProfile(profile, { Prefer: "count=exact", Range: "0-0" }),
      cache: "no-store",
    }
  );
  const range = res.headers.get("content-range") || "*/0";
  return Number(range.split("/")[1] || 0);
}

export async function supabaseLastWatermark(pipelineName: string, profile: SupabaseProfile = "default") {
  if (!credsForProfile(profile)) return null;
  const c = credsForProfile(profile)!;
  const res = await fetch(
    `${c.supabaseUrl}/rest/v1/etl_watermarks?select=last_timestamp&table_name=eq.${encodeURIComponent(pipelineName)}&limit=1`,
    { headers: headersForProfile(profile), cache: "no-store" }
  );
  if (!res.ok) return null;
  const data = (await res.json()) as Array<{ last_timestamp: string }>;
  return data[0]?.last_timestamp || null;
}

export async function supabaseLastRun(pipelineName: string, profile: SupabaseProfile = "default") {
  if (!credsForProfile(profile)) return null;
  const c = credsForProfile(profile)!;
  const res = await fetch(
    `${c.supabaseUrl}/rest/v1/etl_runs?select=pipeline_name,status,inserted_count,updated_count,skipped_count,error_message,started_at,finished_at,duration_ms,s3_log_uri&pipeline_name=eq.${encodeURIComponent(pipelineName)}&order=created_at.desc&limit=1`,
    { headers: headersForProfile(profile), cache: "no-store" }
  );
  if (!res.ok) return null;
  const data = await res.json();
  return data[0] || null;
}

export async function supabaseDatamartWatermark(
  targetTable: string,
  profile: SupabaseProfile = "default",
  timestampColumn: string = "_ingested_at"
): Promise<string | null> {
  if (!credsForProfile(profile)) return null;
  const c = credsForProfile(profile)!;
  const res = await fetch(
    `${c.supabaseUrl}/rest/v1/${targetTable}?select=${encodeURIComponent(timestampColumn)}&order=${encodeURIComponent(`${timestampColumn}.desc`)}&limit=1`,
    { headers: headersForProfile(profile), cache: "no-store" }
  );
  if (!res.ok) return null;
  const data = (await res.json()) as Record<string, string | undefined>[];
  return data[0]?.[timestampColumn] || null;
}
