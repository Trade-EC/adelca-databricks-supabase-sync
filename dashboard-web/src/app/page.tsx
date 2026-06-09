"use client";

import { useCallback, useState } from "react";

type RunInfo = {
  status: string;
  inserted_count: number;
  updated_count: number;
  skipped_count: number;
  error_message?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  s3_log_uri?: string | null;
  /** Where the row came from when etl_runs is missing (S3 checkpoint from Lambda). */
  run_source?: "supabase" | "s3";
};

type PipelineRow = {
  pipeline_name: string;
  source_kind?: "databricks" | "supabase";
  source_supabase_profile?: "default" | "secondary" | "tertiary";
  source_supabase_accept_profile?: string;
  source_table: string;
  target_table: string;
  write_mode: "insert_only" | "upsert";
  conflict_key: string;
  schedule: string;
  mapping_summary?: string;
  databricks_profile?: "prd" | "qas";
  supabase_profile?: "default" | "secondary" | "tertiary" | "base_socio";
  source_count: number | null;
  dest_count: number | null;
  pending: number | null;
  watermark: string | null;
  datamart_watermark: string | null;
  source_sync_at: string | null;
  metrics_source: "s3" | "supabase" | null;
  last_run: RunInfo | null;
  pipeline_error?: string | null;
};

type DashboardPayload = {
  timestamp: string;
  lambda: { name: string; runtime: string; memory: number; timeout: number; state: string };
  aws_error?: string | null;
  executions?: { stream?: string; last_event?: number }[];
  pipelines: PipelineRow[];
};

export default function Home() {
  const [data, setData] = useState<DashboardPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
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

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/dashboard", { cache: "no-store" });
      const text = await res.text();
      let json: unknown;
      try {
        json = JSON.parse(text);
      } catch {
        throw new Error(
          `Respuesta no JSON (${res.status}); suele ser HTML de error del servidor. Revisa variables en Vercel.`
        );
      }
      const body = json as { error?: string; hint?: string };
      if (!res.ok) {
        throw new Error(
          [body?.error, body?.hint].filter(Boolean).join(" — ") || "Failed to load dashboard"
        );
      }
      setData(body as DashboardPayload);
      setLoadError(null);
    } catch (error) {
      const msg = error instanceof Error ? error.message : "Failed to load dashboard";
      setLoadError(msg);
    } finally {
      setLoading(false);
    }
  }, []);

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

  return (
    <main className="min-h-screen p-8">
      <div className="mx-auto max-w-7xl space-y-6">
        <header className="rounded-xl border border-zinc-700 bg-zinc-900 p-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h1 className="text-2xl font-bold">Databricks → Supabase Pipelines</h1>
              <p className="mt-1 text-sm text-zinc-400">
                Updated: {data?.timestamp ?? "—"} | Lambda: {data?.lambda?.name ?? "-"} (
                {data?.lambda?.runtime ?? "-"}) | Pipelines: {data?.pipelines?.length ?? "—"}
              </p>
              <p className="mt-1 text-xs text-zinc-500">
                Sin auto-refresh ni consultas Databricks: métricas desde S3/Supabase solo al pulsar Actualizar.
              </p>
            </div>
            <button
              type="button"
              onClick={() => load().catch(console.error)}
              disabled={loading}
              className="rounded bg-zinc-100 px-4 py-2 text-sm font-semibold text-zinc-900 hover:bg-white disabled:opacity-60"
            >
              {loading ? "Actualizando..." : "Actualizar datos"}
            </button>
          </div>
          {data?.aws_error ? (
            <p className="mt-2 text-xs text-amber-300">
              AWS (metadata): {data.aws_error} — tabla de pipelines puede seguir cargando; revisa
              AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / IAM y LAMBDA_NAME en Vercel.
            </p>
          ) : null}
        </header>

        <section className="rounded-xl border border-zinc-700 bg-zinc-900 p-4 overflow-x-auto">
          {loading && <div className="text-zinc-400">Cargando pipelines...</div>}
          {!loading && loadError && <div className="text-red-300 text-sm">{loadError}</div>}
          {!loading && !data && !loadError && (
            <div className="text-zinc-400 text-sm">
              Pulsa &quot;Actualizar datos&quot; para cargar métricas (S3 checkpoint, Supabase, Lambda).
            </div>
          )}
          {!loading && data && (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-zinc-400 border-b border-zinc-700">
                  <th className="py-2 pr-4">Pipeline</th>
                  <th className="py-2 pr-4">Entorno</th>
                  <th className="py-2 pr-4">Mode</th>
                  <th className="py-2 pr-4">Source</th>
                  <th className="py-2 pr-4">Target / mapping</th>
                  <th className="py-2 pr-4">Counts</th>
                  <th className="py-2 pr-4">Watermarks</th>
                  <th className="py-2 pr-4">Última sync ETL</th>
                  <th className="py-2 pr-4">Last Run</th>
                  <th className="py-2 pr-4">Action</th>
                </tr>
              </thead>
              <tbody>
                {data?.pipelines?.map((p) => (
                  <tr key={p.pipeline_name} className="border-b border-zinc-800 align-top">
                    <td className="py-3 pr-4 font-medium">{p.pipeline_name}</td>
                    <td className="py-3 pr-4 text-xs text-zinc-300">
                      <div className="flex flex-wrap gap-1">
                        <span
                          className="rounded bg-violet-900/50 px-2 py-0.5 text-violet-200"
                          title="Origen de datos"
                        >
                          src{" "}
                          {p.source_kind === "supabase"
                            ? `SB ${p.source_supabase_profile ?? "default"}${
                                p.source_supabase_accept_profile
                                  ? ` · ${p.source_supabase_accept_profile}`
                                  : ""
                              }`
                            : `DBX ${p.databricks_profile ?? "prd"}`}
                        </span>
                        <span
                          className="rounded bg-slate-800 px-2 py-0.5 text-slate-200"
                          title="Destino PostgREST"
                        >
                          dst SB {p.supabase_profile ?? "default"}
                        </span>
                        <span className="rounded bg-zinc-800/80 px-2 py-0.5 text-zinc-400" title="Schedule en config">
                          {p.schedule === "manual" ? "manual" : "cron"}
                        </span>
                      </div>
                    </td>
                    <td className="py-3 pr-4">{p.write_mode}</td>
                    <td className="py-3 pr-4 text-zinc-300">{p.source_table}</td>
                    <td className="py-3 pr-4 text-zinc-300">
                      <div>{p.target_table}</div>
                      <div className="mt-1 text-[11px] text-zinc-500 max-w-md">
                        {p.write_mode} · {p.conflict_key}
                        {p.mapping_summary ? (
                          <>
                            <br />
                            {p.mapping_summary}
                          </>
                        ) : null}
                      </div>
                    </td>
                    <td className="py-3 pr-4">
                      src {p.source_count ?? "—"} / dst {p.dest_count ?? "—"}
                      <div className="text-xs text-zinc-400">
                        pending {p.pending ?? "—"}
                        {p.metrics_source === "s3" ? (
                          <span className="ml-1 text-zinc-500">(S3)</span>
                        ) : null}
                      </div>
                    </td>
                    <td className="py-3 pr-4 text-xs text-zinc-300">
                      <div>pipeline: {p.watermark ?? "—"}</div>
                      <div className="text-zinc-400">datamart: {p.datamart_watermark ?? "—"}</div>
                    </td>
                    <td className="py-3 pr-4 text-xs text-zinc-300">
                      {p.source_sync_at ?? "—"}
                    </td>
                    <td className="py-3 pr-4">
                      {p.pipeline_error && (
                        <div className="mb-2 rounded bg-amber-900/30 px-2 py-1 text-[11px] text-amber-300">
                          {p.pipeline_error}
                        </div>
                      )}
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
                        {p.last_run?.run_source === "s3" && (
                          <span className="ml-2 text-zinc-500"> (última corrida desde S3)</span>
                        )}
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
