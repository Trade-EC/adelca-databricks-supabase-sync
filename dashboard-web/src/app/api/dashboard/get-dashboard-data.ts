import {
  fetchLambdaDashboardInfo,
  fetchRecentLambdaLogStreams,
} from "@/lib/aws-signed-api";
import { appConfig } from "@/lib/config";
import { readPipelines } from "@/lib/pipelines";
import { countSourceRows, maxSourceAuditCreatedAt } from "@/lib/databricks";
import { supabaseCount, supabaseDatamartWatermark, supabaseLastWatermark } from "@/lib/supabase";
import { resolveLastRun } from "@/lib/last-run";

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
      const profile = p.supabase_profile === "secondary" ? "secondary" : "default";
      const dbProfile = p.databricks_profile === "qas" ? "qas" : "prd";
      const datamartTs = p.datamart_timestamp_column || "_ingested_at";
      try {
        const [sourceCount, destCount, watermark, datamartWatermark, sourceAuditCreatedAt, lastRun] =
          await Promise.all([
            countSourceRows(p.source_table, dbProfile),
            supabaseCount(p.target_table, profile),
            supabaseLastWatermark(p.pipeline_name, profile),
            supabaseDatamartWatermark(p.target_table, profile, datamartTs),
            maxSourceAuditCreatedAt(p.source_table, dbProfile),
            resolveLastRun(p.pipeline_name, profile),
          ]);

        const destN = destCount ?? 0;
        return {
          ...p,
          source_count: sourceCount,
          dest_count: destCount,
          pending: destCount == null ? null : Math.max(0, sourceCount - destN),
          watermark,
          datamart_watermark: datamartWatermark,
          source_audit_created_at_max: sourceAuditCreatedAt,
          last_run: lastRun,
          pipeline_error: null,
        };
      } catch (error) {
        return {
          ...p,
          source_count: 0,
          dest_count: null,
          pending: null,
          watermark: null,
          datamart_watermark: null,
          source_audit_created_at_max: null,
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
