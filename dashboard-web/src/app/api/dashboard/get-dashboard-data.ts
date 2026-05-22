import {
  fetchLambdaDashboardInfo,
  fetchRecentLambdaLogStreams,
} from "@/lib/aws-signed-api";
import { appConfig } from "@/lib/config";
import { formatMappingSummary, readPipelines } from "@/lib/pipelines";
import { fetchCheckpointFromS3, s3CheckpointToLastRun } from "@/lib/s3-checkpoint";
import {
  resolveSupabaseProfileFromPipeline,
  supabaseCount,
  supabaseDatamartWatermark,
  supabaseLastRun,
  supabaseLastWatermark,
} from "@/lib/supabase";

/** Separated into its own chunk so `route.ts` can dynamic-import and always return JSON on load failure (Vercel HTML 500 otherwise). */
export async function buildDashboardPayload() {
  const pipelines = readPipelines();

  let lambdaInfo: {
    Runtime?: string;
    MemorySize?: number;
    Timeout?: number;
    State?: string;
  } | null = null;
  let executions: { stream?: string; last_event?: number }[] = [];
  let awsError: string | null = null;

  try {
    const [lc, ls] = await Promise.all([
      fetchLambdaDashboardInfo(appConfig.region, appConfig.lambdaName),
      fetchRecentLambdaLogStreams(appConfig.region, appConfig.lambdaName, 10),
    ]);

    const parts: string[] = [];
    if (!lc.ok) parts.push(`Lambda: ${lc.text} (${lc.status})`);
    if (!ls.ok) parts.push(`Logs: ${ls.text} (${ls.status})`);
    awsError = parts.length ? parts.join(" | ") : null;

    if (lc.ok && lc.data) lambdaInfo = lc.data;
    if (ls.ok) {
      executions = ls.streams.map((s) => ({
        stream: s.logStreamName,
        last_event: s.lastEventTimestamp,
      }));
    }
  } catch (e) {
    awsError = e instanceof Error ? e.message : "AWS Lambda / CloudWatch error";
    console.error("dashboard AWS metadata failed:", e);
  }

  const rows = await Promise.all(
    pipelines.map(async (p) => {
      const profile = resolveSupabaseProfileFromPipeline(p.supabase_profile);
      const datamartTs = p.datamart_timestamp_column || "_ingested_at";
      try {
        const checkpoint = await fetchCheckpointFromS3(p.pipeline_name);
        const fromDb = await supabaseLastRun(p.pipeline_name, profile);
        const lastRun = fromDb
          ? { ...fromDb, run_source: "supabase" as const }
          : checkpoint
            ? s3CheckpointToLastRun(checkpoint)
            : null;

        const sourceCount =
          checkpoint?.source_count ??
          (typeof checkpoint?.total === "number" ? checkpoint.total : null);
        let destCount =
          checkpoint?.dest_count_after ??
          (typeof checkpoint?.total === "number" ? checkpoint.total : null);

        if (destCount == null) {
          destCount = await supabaseCount(p.target_table, profile);
        }

        const pending =
          sourceCount != null && destCount != null
            ? Math.max(0, sourceCount - destCount)
            : typeof checkpoint?.skipped === "number"
              ? checkpoint.skipped
              : null;

        const [watermark, datamartWatermark] = await Promise.all([
          supabaseLastWatermark(p.pipeline_name, profile),
          supabaseDatamartWatermark(p.target_table, profile, datamartTs),
        ]);

        return {
          ...p,
          mapping_summary: formatMappingSummary(p),
          source_count: sourceCount,
          dest_count: destCount,
          pending,
          watermark,
          datamart_watermark: datamartWatermark,
          source_sync_at: checkpoint?.sync_timestamp ?? lastRun?.finished_at ?? null,
          metrics_source: checkpoint ? ("s3" as const) : ("supabase" as const),
          last_run: lastRun,
          pipeline_error: null,
        };
      } catch (error) {
        return {
          ...p,
          mapping_summary: formatMappingSummary(p),
          source_count: null,
          dest_count: null,
          pending: null,
          watermark: null,
          datamart_watermark: null,
          source_sync_at: null,
          metrics_source: null,
          last_run: null,
          pipeline_error: error instanceof Error ? error.message : "Pipeline metrics error",
        };
      }
    })
  );

  return {
    timestamp: new Date().toISOString(),
    lambda: {
      name: appConfig.lambdaName,
      runtime: lambdaInfo?.Runtime ?? "—",
      memory: lambdaInfo?.MemorySize ?? 0,
      timeout: lambdaInfo?.Timeout ?? 0,
      state: lambdaInfo?.State ?? (awsError ? "unavailable" : "unknown"),
    },
    aws_error: awsError,
    executions,
    pipelines: rows,
  };
}
