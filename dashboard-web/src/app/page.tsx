"use client";

import { useEffect, useState } from "react";

type RunInfo = {
  status: string;
  inserted_count: number;
  updated_count: number;
  skipped_count: number;
  error_message?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  s3_log_uri?: string | null;
};

type PipelineRow = {
  pipeline_name: string;
  source_table: string;
  target_table: string;
  write_mode: "insert_only" | "upsert";
  conflict_key: string;
  schedule: string;
  source_count: number;
  dest_count: number | null;
  pending: number | null;
  watermark: string | null;
  datamart_watermark: string | null;
  source_audit_created_at_max: string | null;
  last_run: RunInfo | null;
};

type DashboardPayload = {
  timestamp: string;
  lambda: { name: string; runtime: string; memory: number; timeout: number; state: string };
  pipelines: PipelineRow[];
};

export default function Home() {
  const [data, setData] = useState<DashboardPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [runningPipeline, setRunningPipeline] = useState<string | null>(null);
  const [runFeedback, setRunFeedback] = useState<
    Record<
      string,
      {
        ok: boolean;
        message: string;
      }
    >
  >({});

  const load = async () => {
    const res = await fetch("/api/dashboard");
    const json = await res.json();
    setData(json);
    setLoading(false);
  };

  const runPipeline = async (pipelineName: string) => {
    try {
      setRunningPipeline(pipelineName);
      setRunFeedback((prev) => ({ ...prev, [pipelineName]: { ok: true, message: "Running..." } }));

      const res = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pipeline_name: pipelineName }),
      });
      const json = await res.json();

      if (!res.ok || json.status === "error") {
        const msg = json?.message || "Execution failed";
        setRunFeedback((prev) => ({ ...prev, [pipelineName]: { ok: false, message: msg } }));
        return;
      }

      const lambdaResp = json.lambda_response || {};
      const body =
        typeof lambdaResp.body === "string" ? JSON.parse(lambdaResp.body) : lambdaResp.body || {};
      const msg = `Status: ${body.status ?? "unknown"} | inserted: ${body.inserted ?? 0} | updated: ${body.updated ?? 0} | total: ${body.total ?? "-"}`;
      setRunFeedback((prev) => ({ ...prev, [pipelineName]: { ok: true, message: msg } }));
      await load();
    } catch (error) {
      const msg = error instanceof Error ? error.message : "Execution failed";
      setRunFeedback((prev) => ({ ...prev, [pipelineName]: { ok: false, message: msg } }));
    } finally {
      setRunningPipeline(null);
    }
  };

  useEffect(() => {
    load().catch(console.error);
    const id = setInterval(() => load().catch(console.error), 30000);
    return () => clearInterval(id);
  }, []);

  return (
    <main className="min-h-screen p-8">
      <div className="mx-auto max-w-7xl space-y-6">
        <header className="rounded-xl border border-zinc-700 bg-zinc-900 p-5">
          <h1 className="text-2xl font-bold">Databricks → Supabase Pipelines</h1>
          <p className="mt-1 text-sm text-zinc-400">
            Updated: {data?.timestamp ?? "loading..."} | Lambda: {data?.lambda?.name ?? "-"} (
            {data?.lambda?.runtime ?? "-"})
          </p>
        </header>

        <section className="rounded-xl border border-zinc-700 bg-zinc-900 p-4 overflow-x-auto">
          {loading && <div className="text-zinc-400">Loading pipelines...</div>}
          {!loading && (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-zinc-400 border-b border-zinc-700">
                  <th className="py-2 pr-4">Pipeline</th>
                  <th className="py-2 pr-4">Mode</th>
                  <th className="py-2 pr-4">Source</th>
                  <th className="py-2 pr-4">Target</th>
                  <th className="py-2 pr-4">Counts</th>
                  <th className="py-2 pr-4">Watermarks</th>
                  <th className="py-2 pr-4">Databricks Audit</th>
                  <th className="py-2 pr-4">Last Run</th>
                  <th className="py-2 pr-4">Action</th>
                </tr>
              </thead>
              <tbody>
                {data?.pipelines?.map((p) => (
                  <tr key={p.pipeline_name} className="border-b border-zinc-800 align-top">
                    <td className="py-3 pr-4 font-medium">{p.pipeline_name}</td>
                    <td className="py-3 pr-4">{p.write_mode}</td>
                    <td className="py-3 pr-4 text-zinc-300">{p.source_table}</td>
                    <td className="py-3 pr-4 text-zinc-300">{p.target_table}</td>
                    <td className="py-3 pr-4">
                      src {p.source_count} / dst {p.dest_count ?? "—"}
                      <div className="text-xs text-zinc-400">
                        pending {p.pending ?? "—"}
                      </div>
                    </td>
                    <td className="py-3 pr-4 text-xs text-zinc-300">
                      <div>pipeline: {p.watermark ?? "—"}</div>
                      <div className="text-zinc-400">datamart: {p.datamart_watermark ?? "—"}</div>
                    </td>
                    <td className="py-3 pr-4 text-xs text-zinc-300">
                      {p.source_audit_created_at_max ?? "—"}
                    </td>
                    <td className="py-3 pr-4">
                      <div
                        className={`inline-block rounded px-2 py-1 text-xs ${
                          p.last_run?.status === "success"
                            ? "bg-emerald-900/40 text-emerald-300"
                            : p.last_run?.status === "error"
                              ? "bg-red-900/40 text-red-300"
                              : "bg-zinc-800 text-zinc-300"
                        }`}
                      >
                        {p.last_run?.status ?? "unknown"}
                      </div>
                      <div className="mt-1 text-xs text-zinc-400">
                        ins {p.last_run?.inserted_count ?? 0} | upd {p.last_run?.updated_count ?? 0}
                      </div>
                    </td>
                    <td className="py-3 pr-4">
                      <button
                        onClick={() => runPipeline(p.pipeline_name)}
                        disabled={runningPipeline === p.pipeline_name}
                        className="rounded bg-blue-600 px-3 py-1.5 text-xs font-semibold hover:bg-blue-500 disabled:opacity-60"
                      >
                        {runningPipeline === p.pipeline_name ? "Running..." : "Run now"}
                      </button>
                      {runFeedback[p.pipeline_name] && (
                        <div
                          className={`mt-2 max-w-xs rounded px-2 py-1 text-[11px] ${
                            runFeedback[p.pipeline_name].ok
                              ? "bg-emerald-900/30 text-emerald-300"
                              : "bg-red-900/30 text-red-300"
                          }`}
                        >
                          {runFeedback[p.pipeline_name].message}
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      </div>
    </main>
  );
}
