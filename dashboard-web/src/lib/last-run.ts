import type { SupabaseProfile } from "./supabase";
import { supabaseLastRun } from "./supabase";

/**
 * Prefer `etl_runs` in Supabase; if missing or empty, use Lambda S3 checkpoint
 * `s3://{bucket}/{etl-success}/{pipeline}/latest.json`.
 *
 * S3 checkpoints use SigV4 fetch (no AWS SDK) in `s3-checkpoint`, loaded only when needed via dynamic import.
 */
export async function resolveLastRun(pipelineName: string, profile: SupabaseProfile) {
  const fromDb = await supabaseLastRun(pipelineName, profile);
  if (fromDb) {
    return { ...fromDb, run_source: "supabase" as const };
  }
  const mod = await import("./s3-checkpoint");
  const cp = await mod.fetchCheckpointFromS3(pipelineName);
  if (cp) {
    return mod.s3CheckpointToLastRun(cp);
  }
  return null;
}
