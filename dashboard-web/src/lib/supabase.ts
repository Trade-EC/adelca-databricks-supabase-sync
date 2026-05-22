import { appConfig } from "./config";

export type SupabaseProfile = "default" | "secondary" | "tertiary";

const PROFILE_LABEL: Record<SupabaseProfile, string> = {
  default: "Default",
  secondary: "Secondary",
  tertiary: "Tertiary",
};

const PROFILE_ENV_HINT: Record<SupabaseProfile, string> = {
  default: "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY",
  secondary: "SUPABASE_SECONDARY_URL / SUPABASE_SECONDARY_SERVICE_ROLE_KEY",
  tertiary: "SUPABASE_TERTIARY_URL / SUPABASE_TERTIARY_SERVICE_ROLE_KEY",
};

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
  if (profile === "tertiary") {
    if (!appConfig.supabaseTertiaryUrl || !appConfig.supabaseTertiaryKey) {
      return null;
    }
    return {
      supabaseUrl: appConfig.supabaseTertiaryUrl,
      supabaseKey: appConfig.supabaseTertiaryKey,
    };
  }
  if (!appConfig.supabaseUrl || !appConfig.supabaseKey) {
    return null;
  }
  return { supabaseUrl: appConfig.supabaseUrl, supabaseKey: appConfig.supabaseKey };
}

function headersForProfile(profile: SupabaseProfile, extra?: Record<string, string>) {
  const c = credsForProfile(profile);
  if (!c)
    throw new Error(
      `${PROFILE_LABEL[profile]} Supabase not configured (${PROFILE_ENV_HINT[profile]})`
    );
  return {
    apikey: c.supabaseKey,
    Authorization: `Bearer ${c.supabaseKey}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

export async function supabaseCount(
  table: string,
  profile: SupabaseProfile = "default",
  opts?: { acceptProfile?: string | null }
): Promise<number | null> {
  const c = credsForProfile(profile);
  if (!c) return null;
  const extra: Record<string, string> = { Prefer: "count=exact", Range: "0-0" };
  const ap = opts?.acceptProfile;
  if (typeof ap === "string" && ap.trim()) {
    const s = ap.trim();
    extra["Accept-Profile"] = s;
    extra["Content-Profile"] = s;
  }
  const res = await fetch(
    `${c.supabaseUrl}/rest/v1/${encodeURIComponent(table)}?select=${encodeURIComponent("*")}`,
    {
      headers: headersForProfile(profile, extra),
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

/** Map pipeline JSON `supabase_profile` to API profile (unknown values → default). */
export function resolveSupabaseProfileFromPipeline(raw?: string): SupabaseProfile {
  if (raw === "secondary") return "secondary";
  if (raw === "tertiary") return "tertiary";
  return "default";
}
