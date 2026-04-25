import { NextResponse } from "next/server";
import { LambdaClient, GetFunctionConfigurationCommand } from "@aws-sdk/client-lambda";
import {
  CloudWatchLogsClient,
  DescribeLogStreamsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import { appConfig } from "@/lib/config";
import { readPipelines } from "@/lib/pipelines";
import { countSourceRows, maxSourceAuditCreatedAt } from "@/lib/databricks";
import {
  supabaseCount,
  supabaseDatamartWatermark,
  supabaseLastRun,
  supabaseLastWatermark,
} from "@/lib/supabase";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const pipelines = readPipelines();
    const lambda = new LambdaClient({ region: appConfig.region });
    const logs = new CloudWatchLogsClient({ region: appConfig.region });

    const [lambdaInfo, executions] = await Promise.all([
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

    const rows = await Promise.all(
      pipelines.map(async (p) => {
        const profile = p.supabase_profile === "secondary" ? "secondary" : "default";
        const dbProfile = p.databricks_profile === "qas" ? "qas" : "prd";
        const datamartTs = p.datamart_timestamp_column || "_ingested_at";
        const [sourceCount, destCount, watermark, datamartWatermark, sourceAuditCreatedAt, lastRun] =
          await Promise.all([
          countSourceRows(p.source_table, dbProfile),
          supabaseCount(p.target_table, profile),
          supabaseLastWatermark(p.pipeline_name, profile),
          supabaseDatamartWatermark(p.target_table, profile, datamartTs),
          maxSourceAuditCreatedAt(p.source_table, dbProfile),
          supabaseLastRun(p.pipeline_name, profile),
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
        };
      })
    );

    return NextResponse.json({
      timestamp: new Date().toISOString(),
      lambda: {
        name: appConfig.lambdaName,
        runtime: lambdaInfo.Runtime,
        memory: lambdaInfo.MemorySize,
        timeout: lambdaInfo.Timeout,
        state: lambdaInfo.State,
      },
      executions:
        executions.logStreams?.map((s) => ({
          stream: s.logStreamName,
          last_event: s.lastEventTimestamp,
        })) || [],
      pipelines: rows,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}

