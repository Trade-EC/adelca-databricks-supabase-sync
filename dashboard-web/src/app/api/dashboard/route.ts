import { NextResponse } from "next/server";
import { LambdaClient, GetFunctionConfigurationCommand } from "@aws-sdk/client-lambda";
import {
  CloudWatchLogsClient,
  DescribeLogStreamsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import { appConfig } from "@/lib/config";
import { readPipelines } from "@/lib/pipelines";
import { countSourceRows, maxSourceAuditCreatedAt } from "@/lib/databricks";
import { supabaseCount, supabaseDatamartWatermark, supabaseLastWatermark } from "@/lib/supabase";
import { resolveLastRun } from "@/lib/last-run";

export const dynamic = "force-dynamic";
/** Vercel: default ~10s on Hobby; Databricks COUNT×pipelines often needs more. Pro/Enterprise can use 60+. */
export const maxDuration = 60;

const noStore = { "Cache-Control": "no-store, must-revalidate" as const };

export async function GET() {
  try {
    const pipelines = readPipelines();
    const lambda = new LambdaClient({ region: appConfig.region });
    const logs = new CloudWatchLogsClient({ region: appConfig.region });

    let lambdaInfo: {
      Runtime?: string;
      MemorySize?: number;
      Timeout?: number;
      State?: string;
    } | null = null;
    let executions: { stream?: string; last_event?: number }[] = [];
    let awsError: string | null = null;
    try {
      const [li, exec] = await Promise.all([
        lambda.send(new GetFunctionConfigurationCommand({ FunctionName: appConfig.lambdaName })),
        logs.send(
          new DescribeLogStreamsCommand({
            logGroupName: `/aws/lambda/${appConfig.lambdaName}`,
            orderBy: "LastEventTime",
            descending: true,
            limit: 10,
          })
        ),
      ]);
      lambdaInfo = li;
      executions =
        exec.logStreams?.map((s) => ({
          stream: s.logStreamName,
          last_event: s.lastEventTimestamp,
        })) || [];
    } catch (e) {
      awsError = e instanceof Error ? e.message : "AWS Lambda / CloudWatch error";
      console.error("dashboard /api/dashboard AWS metadata failed:", e);
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

    return NextResponse.json(
      {
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
      },
      { headers: noStore }
    );
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500, headers: noStore }
    );
  }
}
