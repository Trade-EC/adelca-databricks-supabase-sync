import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

const noStore = { "Cache-Control": "no-store, must-revalidate" as const };

/**
 * Thin route: dashboard logic loads in a separate chunk. If Lambda/S3/AWS SDK fails at module evaluation,
 * this catch returns JSON instead of Next's HTML error page (fixes client "Respuesta no JSON").
 */
export async function GET() {
  try {
    const { buildDashboardPayload } = await import("./get-dashboard-data");
    const body = await buildDashboardPayload();
    return NextResponse.json(body, { headers: noStore });
  } catch (error) {
    console.error("/api/dashboard load failed:", error);
    const message = error instanceof Error ? error.message : "Unknown server error";
    return NextResponse.json(
      {
        error: message,
        hint: "Revisa logs Vercel y credenciales AWS (Lambda/Logs/S3 usan SigV4 vía fetch).",
      },
      { status: 500, headers: noStore }
    );
  }
}
