import { s3GetText } from "./aws-signed-api";
import { appConfig } from "./config";

export type S3EtlCheckpoint = {
  pipeline_name?: string;
  status?: string;
  sync_timestamp?: string;
  inserted?: number;
  updated?: number;
  total?: number;
  skipped?: number;
  source_count?: number;
  dest_count_before?: number;
  dest_count_after?: number;
  full_log_uri?: string | null;
};

export async function fetchCheckpointFromS3(
  pipelineName: string
): Promise<S3EtlCheckpoint | null> {
  if (!appConfig.etlLogsBucket) return null;

  const key = `${appConfig.etlSuccessPrefix}/${pipelineName}/latest.json`;

  try {
    const res = await s3GetText(appConfig.region, appConfig.etlLogsBucket, key);
    if (!res.ok) {
      if (res.status === 404 || res.status === 403) return null;
      console.warn("S3 ETL checkpoint read failed", pipelineName, res.status);
      return null;
    }
    return JSON.parse(res.text) as S3EtlCheckpoint;
  } catch (e) {
    console.warn("S3 ETL checkpoint parse failed", pipelineName, e);
    return null;
  }
}

export function s3CheckpointToLastRun(cp: S3EtlCheckpoint) {
  return {
    status: cp.status || "success",
    inserted_count: cp.inserted ?? 0,
    updated_count: cp.updated ?? 0,
    skipped_count: cp.skipped ?? 0,
    error_message: null as string | null,
    started_at: cp.sync_timestamp ?? null,
    finished_at: cp.sync_timestamp ?? null,
    duration_ms: null as number | null,
    s3_log_uri: cp.full_log_uri ?? null,
    run_source: "s3" as const,
  };
}
