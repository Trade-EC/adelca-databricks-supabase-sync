import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { appConfig } from "./config";

export type S3EtlCheckpoint = {
  pipeline_name?: string;
  status?: string;
  sync_timestamp?: string;
  inserted?: number;
  updated?: number;
  total?: number;
  skipped?: number;
  full_log_uri?: string | null;
};

export async function fetchCheckpointFromS3(
  pipelineName: string
): Promise<S3EtlCheckpoint | null> {
  if (!appConfig.etlLogsBucket) return null;

  const key = `${appConfig.etlSuccessPrefix}/${pipelineName}/latest.json`;
  const s3 = new S3Client({ region: appConfig.region });
  try {
    const res = await s3.send(
      new GetObjectCommand({
        Bucket: appConfig.etlLogsBucket,
        Key: key,
      })
    );
    const raw = await res.Body?.transformToString();
    if (!raw) return null;
    return JSON.parse(raw) as S3EtlCheckpoint;
  } catch (e: unknown) {
    const err = e as { name?: string; $metadata?: { httpStatusCode?: number } };
    if (err.name === "NoSuchKey" || err.$metadata?.httpStatusCode === 404) {
      return null;
    }
    console.warn("S3 ETL checkpoint read failed", pipelineName, e);
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
