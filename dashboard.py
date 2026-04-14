#!/usr/bin/env python3
"""Lambda Sync Health Dashboard — Grafana-style monitoring."""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta

import httpx
from flask import Flask, jsonify, render_template_string, request

sys.path.insert(0, "transportistas_sync")
sys.path.insert(0, "lambda_function")
from dotenv import load_dotenv
from databricks_connector import DatabricksConnector

load_dotenv("transportistas_sync/.env")

app = Flask(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
GMT5 = timezone(timedelta(hours=-5))

LAMBDA_NAME = "patek-philippe"
SOURCE_TABLE = "prod.gldlogistica.db_trade_dim_app_transportistas"
DEST_TABLE = "transportistas"


def supabase_count(table):
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}?select=codigo_transportista",
        headers={**HEADERS, "Prefer": "count=exact", "Range": "0-0"},
        timeout=15,
    )
    return int(r.headers.get("content-range", "*/0").split("/")[1])


def supabase_watermark(table):
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/etl_watermarks?select=last_timestamp&table_name=eq.{table}",
        headers=HEADERS,
        timeout=15,
    )
    data = r.json()
    return data[0].get("last_timestamp") if data else None


def supabase_latest_record(table):
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}?select=codigo_transportista,nombre_transportista,email,_ingested_at&order=_ingested_at.desc&limit=1",
        headers=HEADERS,
        timeout=15,
    )
    data = r.json()
    return data[0] if data else None


def supabase_email_stats(table):
    total = supabase_count(table)
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}?select=email&email=not.is.null&email=neq.",
        headers={**HEADERS, "Prefer": "count=exact", "Range": "0-0"},
        timeout=15,
    )
    with_email = int(r.headers.get("content-range", "*/0").split("/")[1])
    return total, with_email


def databricks_source_info():
    try:
        conn = DatabricksConnector(env="prd")
        with conn._connection() as c:
            with c.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
                count = cur.fetchone()[0]

                cur.execute(
                    f"SELECT codigo_transportista, nombre_transportista, email "
                    f"FROM {SOURCE_TABLE} ORDER BY codigo_transportista DESC LIMIT 1"
                )
                row = cur.fetchone()
                latest = {
                    "codigo_transportista": row[0],
                    "nombre_transportista": row[1],
                    "email": row[2],
                } if row else None

        return {"count": count, "latest": latest, "status": "ok"}
    except Exception as e:
        return {"count": "?", "latest": None, "status": "error", "error": str(e)[:150]}


def lambda_last_executions(limit=5):
    try:
        result = subprocess.run(
            ["aws", "logs", "describe-log-streams",
             "--log-group-name", f"/aws/lambda/{LAMBDA_NAME}",
             "--order-by", "LastEventTime", "--descending",
             "--max-items", str(limit), "--output", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return []
        streams = json.loads(result.stdout).get("logStreams", [])
        return [{
            "timestamp": datetime.fromtimestamp(
                s.get("lastEventTimestamp", s.get("firstEventTimestamp", 0)) / 1000,
                tz=timezone.utc
            ).astimezone(GMT5).strftime("%Y-%m-%d %H:%M:%S"),
            "stream": s["logStreamName"],
        } for s in streams]
    except Exception:
        return []


def lambda_last_log(stream_name):
    try:
        result = subprocess.run(
            ["aws", "logs", "get-log-events",
             "--log-group-name", f"/aws/lambda/{LAMBDA_NAME}",
             "--log-stream-name", stream_name, "--output", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return None
        events = json.loads(result.stdout).get("events", [])
        status, inserted, duration, error_msg = "unknown", "?", "?", None
        for e in events:
            msg = e.get("message", "")
            if '"status": "success"' in msg:
                status = "success"
            if "Sync failed" in msg or "ERROR" in msg:
                status = "error"
                error_msg = msg.strip()[:200]
            if '"inserted":' in msg:
                try:
                    inserted = msg[msg.index('"inserted":'):].split(":")[1].split(",")[0].strip()
                except Exception:
                    pass
            if "Duration:" in msg:
                try:
                    duration = f"{float(msg.split('Duration:')[1].split('ms')[0].strip()) / 1000:.1f}s"
                except Exception:
                    pass
        return {"status": status, "inserted": inserted, "duration": duration, "error": error_msg}
    except Exception:
        return None


def lambda_info():
    try:
        result = subprocess.run(
            ["aws", "lambda", "get-function-configuration",
             "--function-name", LAMBDA_NAME, "--output", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return {}
        data = json.loads(result.stdout)
        return {
            "last_modified": data.get("LastModified", ""),
            "runtime": data.get("Runtime", ""),
            "memory": data.get("MemorySize", ""),
            "timeout": data.get("Timeout", ""),
            "state": data.get("State", ""),
        }
    except Exception:
        return {}


@app.route("/api/data")
def api_data():
    now = datetime.now(GMT5).strftime("%Y-%m-%d %H:%M:%S GMT-5")

    dest_count = supabase_count(DEST_TABLE)
    watermark = supabase_watermark(DEST_TABLE)
    latest_supa = supabase_latest_record(DEST_TABLE)
    total, with_email = supabase_email_stats(DEST_TABLE)

    source = databricks_source_info()

    if watermark:
        wm_dt = datetime.fromisoformat(watermark).replace(tzinfo=timezone.utc).astimezone(GMT5)
        watermark_fmt = wm_dt.strftime("%Y-%m-%d %H:%M:%S")
        delta = datetime.now(timezone.utc) - wm_dt.astimezone(timezone.utc)
        hours_ago = delta.total_seconds() / 3600
        watermark_ago = f"{hours_ago:.1f}h ago" if hours_ago < 48 else f"{hours_ago / 24:.1f}d ago"
    else:
        watermark_fmt, watermark_ago = "Never", ""

    executions = lambda_last_executions(5)
    last_log = lambda_last_log(executions[0]["stream"]) if executions else None
    info = lambda_info()

    health = "healthy"
    if last_log and last_log.get("status") == "error":
        health = "error"
    elif not executions:
        health = "unknown"

    pending = 0
    if isinstance(source.get("count"), int):
        pending = max(0, source["count"] - dest_count)

    return jsonify({
        "timestamp": now, "health": health,
        "source_table": SOURCE_TABLE, "dest_table": DEST_TABLE, "lambda_name": LAMBDA_NAME,
        "source_count": source.get("count", "?"),
        "source_latest": source.get("latest"),
        "source_status": source.get("status", "unknown"),
        "dest_count": dest_count, "pending": pending,
        "watermark": watermark_fmt, "watermark_ago": watermark_ago,
        "latest_record": latest_supa,
        "email_stats": {"total": total, "with_email": with_email, "pct": round(with_email / total * 100, 1) if total else 0},
        "executions": executions, "last_log": last_log, "lambda_info": info,
    })


@app.route("/api/trigger", methods=["POST"])
def api_trigger():
    try:
        result = subprocess.run(
            ["aws", "lambda", "invoke", "--function-name", LAMBDA_NAME, "/tmp/dash_invoke.json"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            return jsonify({"status": "error", "message": result.stderr[:300]}), 500

        with open("/tmp/dash_invoke.json") as f:
            payload = json.load(f)
        return jsonify({"status": "ok", "lambda_response": payload})
    except subprocess.TimeoutExpired:
        return jsonify({"status": "error", "message": "Lambda invocation timed out (120s)"}), 504
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)[:300]}), 500


@app.route("/")
def index():
    return render_template_string(TEMPLATE)


TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lambda Sync Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
  :root {
    --bg-canvas: #111217;
    --bg-primary: #181b1f;
    --bg-secondary: #22252b;
    --bg-hover: #2c2f36;
    --border: #2c2f36;
    --text-primary: #e0e0e0;
    --text-secondary: #8b8fa3;
    --text-muted: #585c68;
    --accent-blue: #3b82f6;
    --accent-green: #22c55e;
    --accent-orange: #f59e0b;
    --accent-red: #ef4444;
    --accent-purple: #a855f7;
    --accent-cyan: #06b6d4;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg-canvas); color: var(--text-primary); min-height: 100vh;
  }
  .topbar {
    background: var(--bg-primary); border-bottom: 1px solid var(--border);
    padding: 12px 24px; display:flex; align-items:center; justify-content:space-between;
  }
  .topbar-left { display:flex; align-items:center; gap:12px; }
  .topbar-logo {
    width:32px; height:32px; background:var(--accent-blue); border-radius:6px;
    display:flex; align-items:center; justify-content:center;
    font-weight:700; font-size:16px; color:#fff;
  }
  .topbar-title { font-size:18px; font-weight:600; }
  .topbar-right { display:flex; align-items:center; gap:12px; }
  .refresh-time { font-size:12px; color:var(--text-muted); }
  .health-badge {
    padding:4px 12px; border-radius:12px; font-size:12px; font-weight:600;
    text-transform:uppercase; letter-spacing:0.5px;
  }
  .health-healthy { background:rgba(34,197,94,0.15); color:var(--accent-green); }
  .health-error { background:rgba(239,68,68,0.15); color:var(--accent-red); }
  .health-unknown { background:rgba(245,158,11,0.15); color:var(--accent-orange); }

  .btn-sync {
    padding:6px 16px; border-radius:6px; font-size:13px; font-weight:600;
    border:none; cursor:pointer; display:flex; align-items:center; gap:6px;
    background:var(--accent-blue); color:#fff; transition:all 0.2s;
  }
  .btn-sync:hover { background:#2563eb; transform:translateY(-1px); }
  .btn-sync:active { transform:translateY(0); }
  .btn-sync:disabled { opacity:0.5; cursor:not-allowed; transform:none; }
  .btn-sync.running { background:var(--accent-orange); }
  .btn-sync.done { background:var(--accent-green); }
  .btn-sync.fail { background:var(--accent-red); }

  .container { padding:20px 24px; max-width:1400px; margin:0 auto; }
  .grid { display:grid; grid-template-columns:repeat(5,1fr); gap:16px; margin-bottom:20px; }
  .grid-2 { grid-template-columns:1fr 1fr; }
  .grid-3 { grid-template-columns:1fr 1fr 1fr; }

  .card {
    background:var(--bg-primary); border:1px solid var(--border);
    border-radius:8px; padding:20px;
  }
  .card-header {
    font-size:12px; font-weight:500; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:0.5px; margin-bottom:12px;
  }
  .card-value { font-size:32px; font-weight:700; line-height:1.1; }
  .card-sub { font-size:13px; color:var(--text-secondary); margin-top:6px; }
  .card-value.green { color:var(--accent-green); }
  .card-value.blue { color:var(--accent-blue); }
  .card-value.orange { color:var(--accent-orange); }
  .card-value.purple { color:var(--accent-purple); }
  .card-value.cyan { color:var(--accent-cyan); }
  .card-value.red { color:var(--accent-red); }

  .panel { margin-bottom:20px; }
  .panel-title {
    font-size:14px; font-weight:600; margin-bottom:12px;
    padding-bottom:8px; border-bottom:1px solid var(--border);
    display:flex; align-items:center; gap:8px;
  }
  .info-row {
    display:flex; justify-content:space-between; align-items:center;
    padding:10px 0; border-bottom:1px solid var(--border);
  }
  .info-row:last-child { border-bottom:none; }
  .info-label { font-size:13px; color:var(--text-secondary); }
  .info-value { font-size:13px; font-weight:500; font-family:'JetBrains Mono',monospace; }

  .exec-row {
    display:grid; grid-template-columns:180px 80px 80px 1fr;
    padding:8px 0; border-bottom:1px solid var(--border);
    font-size:13px; align-items:center;
  }
  .exec-row:last-child { border-bottom:none; }
  .exec-header { color:var(--text-muted); font-weight:600; font-size:11px; text-transform:uppercase; }

  .status-dot { width:8px; height:8px; border-radius:50%; display:inline-block; margin-right:6px; }
  .status-dot.success { background:var(--accent-green); }
  .status-dot.error { background:var(--accent-red); }
  .status-dot.unknown { background:var(--text-muted); }

  .progress-bar { background:var(--bg-secondary); border-radius:4px; height:8px; margin-top:10px; overflow:hidden; }
  .progress-fill { height:100%; border-radius:4px; transition:width 0.5s; }

  .error-box {
    background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.2);
    border-radius:6px; padding:12px; margin-top:12px; font-size:12px;
    color:var(--accent-red); font-family:monospace; word-break:break-all;
  }
  .sync-result {
    margin-top:12px; padding:10px; border-radius:6px; font-size:12px;
    font-family:'JetBrains Mono',monospace;
  }
  .sync-result.ok { background:rgba(34,197,94,0.08); border:1px solid rgba(34,197,94,0.2); color:var(--accent-green); }
  .sync-result.err { background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.2); color:var(--accent-red); }

  .loading { text-align:center; padding:40px; color:var(--text-muted); }
  .pulse { animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.5} }
  @keyframes spin { to{transform:rotate(360deg)} }
  .spinner { display:inline-block; width:14px; height:14px; border:2px solid rgba(255,255,255,0.3);
    border-top-color:#fff; border-radius:50%; animation:spin 0.8s linear infinite; }

  @media (max-width:1100px) { .grid { grid-template-columns:repeat(3,1fr); } }
  @media (max-width:768px) {
    .grid { grid-template-columns:repeat(2,1fr); }
    .grid-2,.grid-3 { grid-template-columns:1fr; }
    .exec-row { grid-template-columns:1fr 1fr; }
  }
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-left">
    <div class="topbar-logo">&#955;</div>
    <span class="topbar-title">Databricks &rarr; Supabase Sync</span>
  </div>
  <div class="topbar-right">
    <span class="refresh-time" id="refreshTime">Loading...</span>
    <button class="btn-sync" id="syncBtn" onclick="triggerSync()">
      <span id="syncIcon">&#9654;</span> <span id="syncText">Run Sync</span>
    </button>
    <span class="health-badge health-unknown" id="healthBadge">CHECKING</span>
  </div>
</div>

<div class="container">
  <!-- KPI Cards -->
  <div class="grid">
    <div class="card">
      <div class="card-header">Source (Databricks)</div>
      <div class="card-value orange" id="sourceCount">&mdash;</div>
      <div class="card-sub" id="sourceTableSub">&mdash;</div>
    </div>
    <div class="card">
      <div class="card-header">Destination (Supabase)</div>
      <div class="card-value blue" id="destCount">&mdash;</div>
      <div class="card-sub" id="destTable">&mdash;</div>
    </div>
    <div class="card">
      <div class="card-header">Pending Sync</div>
      <div class="card-value" id="pendingCount" style="color:var(--text-muted)">&mdash;</div>
      <div class="card-sub">new records to insert</div>
    </div>
    <div class="card">
      <div class="card-header">Last Sync</div>
      <div class="card-value green" id="lastSync" style="font-size:20px">&mdash;</div>
      <div class="card-sub" id="lastSyncAgo">&mdash;</div>
    </div>
    <div class="card">
      <div class="card-header">Email Coverage</div>
      <div class="card-value cyan" id="emailPct">&mdash;</div>
      <div class="card-sub" id="emailDetail">&mdash;</div>
      <div class="progress-bar"><div class="progress-fill" id="emailBar" style="width:0%;background:var(--accent-cyan)"></div></div>
    </div>
  </div>

  <!-- Config + Latest Records -->
  <div class="grid grid-3">
    <div class="card panel">
      <div class="panel-title">&#9881; Pipeline Configuration</div>
      <div class="info-row"><span class="info-label">Lambda Function</span><span class="info-value" id="lambdaName">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Source (Databricks)</span><span class="info-value" id="sourceTable">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Destination (Supabase)</span><span class="info-value" id="destTableName">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Runtime</span><span class="info-value" id="runtime">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Memory / Timeout</span><span class="info-value" id="memTimeout">&mdash;</span></div>
      <div class="info-row"><span class="info-label">State</span><span class="info-value" id="lambdaState">&mdash;</span></div>
    </div>

    <div class="card panel">
      <div class="panel-title">&#9660; Latest from Databricks</div>
      <div class="info-row"><span class="info-label">Codigo</span><span class="info-value" id="dbxCodigo">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Nombre</span><span class="info-value" id="dbxNombre">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Email</span><span class="info-value" id="dbxEmail">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Connection</span><span class="info-value" id="dbxStatus">&mdash;</span></div>
    </div>

    <div class="card panel">
      <div class="panel-title">&#9650; Latest in Supabase</div>
      <div class="info-row"><span class="info-label">Codigo</span><span class="info-value" id="latCodigo">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Nombre</span><span class="info-value" id="latNombre">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Email</span><span class="info-value" id="latEmail">&mdash;</span></div>
      <div class="info-row"><span class="info-label">Ingested At</span><span class="info-value" id="latIngested">&mdash;</span></div>
      <div id="errorBox"></div>
      <div id="syncResult"></div>
    </div>
  </div>

  <!-- Executions -->
  <div class="card panel">
    <div class="panel-title">&#9776; Recent Executions</div>
    <div class="exec-row exec-header">
      <span>Timestamp (GMT-5)</span><span>Status</span><span>Inserted</span><span>Duration</span>
    </div>
    <div id="execList"><div class="loading pulse">Loading execution history...</div></div>
  </div>
</div>

<script>
let syncing = false;

async function fetchData() {
  try {
    const res = await fetch('/api/data');
    const d = await res.json();

    document.getElementById('refreshTime').textContent = 'Updated: ' + d.timestamp;

    const badge = document.getElementById('healthBadge');
    badge.className = 'health-badge health-' + d.health;
    badge.textContent = d.health === 'healthy' ? 'HEALTHY' : d.health === 'error' ? 'ERROR' : 'UNKNOWN';

    document.getElementById('sourceCount').textContent = typeof d.source_count === 'number' ? d.source_count.toLocaleString() : d.source_count;
    document.getElementById('sourceTableSub').textContent = d.source_table.split('.').pop();
    document.getElementById('destCount').textContent = d.dest_count.toLocaleString();
    document.getElementById('destTable').textContent = d.dest_table;

    const pc = document.getElementById('pendingCount');
    pc.textContent = d.pending;
    pc.style.color = d.pending > 0 ? 'var(--accent-orange)' : 'var(--accent-green)';

    document.getElementById('lastSync').textContent = d.watermark;
    document.getElementById('lastSyncAgo').textContent = d.watermark_ago;

    const es = d.email_stats;
    document.getElementById('emailPct').textContent = es.pct + '%';
    document.getElementById('emailDetail').textContent = es.with_email + ' / ' + es.total;
    document.getElementById('emailBar').style.width = es.pct + '%';

    document.getElementById('lambdaName').textContent = d.lambda_name;
    document.getElementById('sourceTable').textContent = d.source_table;
    document.getElementById('destTableName').textContent = d.dest_table;

    if (d.lambda_info) {
      document.getElementById('runtime').textContent = d.lambda_info.runtime || '';
      document.getElementById('memTimeout').textContent = (d.lambda_info.memory||'?') + 'MB / ' + (d.lambda_info.timeout||'?') + 's';
      document.getElementById('lambdaState').innerHTML =
        '<span class="status-dot ' + (d.lambda_info.state==='Active'?'success':'error') + '"></span>' + (d.lambda_info.state||'');
    }

    if (d.source_latest) {
      document.getElementById('dbxCodigo').textContent = d.source_latest.codigo_transportista || '';
      document.getElementById('dbxNombre').textContent = d.source_latest.nombre_transportista || '';
      document.getElementById('dbxEmail').textContent = d.source_latest.email || 'NULL';
    }
    document.getElementById('dbxStatus').innerHTML =
      '<span class="status-dot ' + (d.source_status==='ok'?'success':'error') + '"></span>' + d.source_status;

    if (d.latest_record) {
      document.getElementById('latCodigo').textContent = d.latest_record.codigo_transportista || '';
      document.getElementById('latNombre').textContent = d.latest_record.nombre_transportista || '';
      document.getElementById('latEmail').textContent = d.latest_record.email || 'NULL';
      document.getElementById('latIngested').textContent = d.latest_record._ingested_at || '';
    }

    const eb = document.getElementById('errorBox');
    eb.innerHTML = d.last_log && d.last_log.error
      ? '<div class="error-box">Last Error: ' + d.last_log.error + '</div>' : '';

    const el = document.getElementById('execList');
    if (d.executions && d.executions.length) {
      el.innerHTML = d.executions.map((ex, i) => {
        const log = i===0 && d.last_log ? d.last_log : {};
        const sc = log.status==='success'?'success':log.status==='error'?'error':'unknown';
        return '<div class="exec-row"><span>'+ex.timestamp+'</span>' +
          '<span><span class="status-dot '+sc+'"></span>'+(log.status||'—')+'</span>' +
          '<span>'+(log.inserted||'—')+'</span><span>'+(log.duration||'—')+'</span></div>';
      }).join('');
    } else {
      el.innerHTML = '<div style="padding:12px;color:var(--text-muted)">No executions found</div>';
    }
  } catch(e) { console.error('Fetch error:', e); }
}

async function triggerSync() {
  if (syncing) return;
  syncing = true;
  const btn = document.getElementById('syncBtn');
  const icon = document.getElementById('syncIcon');
  const text = document.getElementById('syncText');
  const sr = document.getElementById('syncResult');

  btn.disabled = true;
  btn.className = 'btn-sync running';
  icon.innerHTML = '<span class="spinner"></span>';
  text.textContent = 'Running...';
  sr.innerHTML = '';

  try {
    const res = await fetch('/api/trigger', { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      const lr = data.lambda_response;
      const body = typeof lr.body === 'string' ? JSON.parse(lr.body) : lr.body;
      btn.className = 'btn-sync done';
      icon.innerHTML = '&#10003;';
      text.textContent = 'Done!';
      sr.innerHTML = '<div class="sync-result ok">Inserted: ' + body.inserted +
        ' | Total: ' + body.total + ' | Status: ' + body.status + '</div>';
      setTimeout(fetchData, 1000);
    } else {
      btn.className = 'btn-sync fail';
      icon.innerHTML = '&#10007;';
      text.textContent = 'Failed';
      sr.innerHTML = '<div class="sync-result err">' + (data.message||'Unknown error') + '</div>';
    }
  } catch(e) {
    btn.className = 'btn-sync fail';
    icon.innerHTML = '&#10007;';
    text.textContent = 'Error';
    sr.innerHTML = '<div class="sync-result err">' + e.message + '</div>';
  }

  setTimeout(() => {
    btn.className = 'btn-sync';
    btn.disabled = false;
    icon.innerHTML = '&#9654;';
    text.textContent = 'Run Sync';
    syncing = false;
  }, 5000);
}

fetchData();
setInterval(fetchData, 30000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Dashboard: http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)
