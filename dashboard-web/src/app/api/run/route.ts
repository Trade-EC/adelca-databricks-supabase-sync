import { NextResponse } from "next/server";
import { invokeLambdaPipeline } from "@/lib/aws-signed-api";
import { appConfig } from "@/lib/config";

export const dynamic = "force-dynamic";
export const maxDuration = 120;

const noStore = { "Cache-Control": "no-store, must-revalidate" as const };

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const pipelineName = body?.pipeline_name;
    if (!pipelineName) {
      return NextResponse.json(
        { error: "pipeline_name is required" },
        { status: 400, headers: noStore }
      );
    }

    const result = await invokeLambdaPipeline(appConfig.region, appConfig.lambdaName, {
      pipeline_name: pipelineName,
    });
    if (!result.ok) {
      return NextResponse.json(
        { status: "error", message: `${result.text} (${result.status})` },
        { status: 500, headers: noStore }
      );
    }
    if (result.functionError) {
      return NextResponse.json(
        { status: "error", message: "Lambda returned an error", lambda_response: result.body },
        { status: 502, headers: noStore }
      );
    }
    return NextResponse.json(
      { status: "ok", lambda_response: result.body },
      { headers: noStore }
    );
  } catch (error) {
    return NextResponse.json(
      { status: "error", message: error instanceof Error ? error.message : "Unknown error" },
      { status: 500, headers: noStore }
    );
  }
}
