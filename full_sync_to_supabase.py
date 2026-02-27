#!/usr/bin/env python3
"""
Incremental sync: Databricks → Supabase (public.transportistas_final).

Uses Supabase REST API (HTTPS) — no direct Postgres connection needed.

Steps:
  1. Verify target table exists in Supabase via REST.
  2. Fetch all rows from Databricks.
  3. Upsert into Supabase via PostgREST (ON CONFLICT codigo_transportista).
  4. Record sync timestamp in public.etl_watermarks.

Usage:
  python full_sync_to_supabase.py
"""
import os
import sys
from datetime import datetime, date, timezone

import httpx

# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------

def _load_env():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(this_dir, "transportistas_sync", ".env")
    if not os.path.isfile(env_path):
        return False
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                key = k.strip()
                if key and not key.startswith("#"):
                    os.environ[key] = v.strip()
    return True


if not _load_env():
    print("ERROR: transportistas_sync/.env not found.", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_TABLE = "transportistas_final"
WATERMARK_TABLE = "etl_watermarks"
DBX_TABLE = os.environ.get("DBX_TABLE", "qas.aplicaciones.transportistas_final")
BATCH_SIZE = int(os.environ.get("FETCH_SIZE", "500"))

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

COLUMN_NAMES = [
    "codigo_transportista", "nombre", "documento", "telefono", "email",
    "direccion", "ciudad", "codigo_postal", "pais",
    "datos_vehiculo", "datos_dueno_vehiculo", "estado",
    "total_transportes", "ultima_fecha_transporte",
]

# ---------------------------------------------------------------------------
# SUPABASE REST HELPERS
# ---------------------------------------------------------------------------

def supabase_get(table: str, params: dict | None = None) -> httpx.Response:
    return httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params=params or {},
        timeout=30,
    )


def supabase_upsert(table: str, rows: list[dict]) -> httpx.Response:
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={
            **HEADERS,
            "Prefer": "resolution=merge-duplicates",
        },
        json=rows,
        timeout=60,
    )

# ---------------------------------------------------------------------------
# STEP 1 — verify table exists
# ---------------------------------------------------------------------------

def verify_table():
    r = supabase_get(SUPABASE_TABLE, {"select": "codigo_transportista", "limit": "1"})
    if r.status_code == 200:
        print(f"[OK] Table '{SUPABASE_TABLE}' exists and is accessible.")
        return True
    print(f"[ERROR] Cannot access '{SUPABASE_TABLE}': {r.status_code} {r.text[:200]}", file=sys.stderr)
    sys.exit(1)


def verify_watermark_table():
    r = supabase_get(WATERMARK_TABLE, {"select": "table_name", "limit": "1"})
    if r.status_code == 200:
        print(f"[OK] Table '{WATERMARK_TABLE}' exists and is accessible.")
        return True
    print(f"[WARN] '{WATERMARK_TABLE}' not accessible: {r.status_code}. Watermark tracking disabled.")
    return False

# ---------------------------------------------------------------------------
# STEP 2 — read last sync watermark
# ---------------------------------------------------------------------------

def get_last_sync() -> str | None:
    r = supabase_get(WATERMARK_TABLE, {
        "select": "last_timestamp",
        "table_name": f"eq.{SUPABASE_TABLE}",
    })
    if r.status_code == 200:
        data = r.json()
        if data and data[0].get("last_timestamp"):
            return data[0]["last_timestamp"]
    return None

# ---------------------------------------------------------------------------
# STEP 3 — fetch from Databricks
# ---------------------------------------------------------------------------

def fetch_databricks():
    from transportistas_sync.databricks_connector import DatabricksConnector

    query = f"""
    SELECT
      codigo_transportista, nombre, documento, telefono, email,
      direccion, ciudad, codigo_postal, pais,
      datos_vehiculo, datos_dueno_vehiculo, estado,
      total_transportes, ultima_fecha_transporte
    FROM {DBX_TABLE}
    """
    dbx = DatabricksConnector()
    print(f"[INFO] Querying Databricks: {DBX_TABLE} (full table)")
    cols, rows = dbx.execute_query(query)
    print(f"[INFO] Fetched {len(rows)} row(s) from Databricks.")
    return cols, rows

# ---------------------------------------------------------------------------
# STEP 4 — upsert via REST API
# ---------------------------------------------------------------------------

def _serialize_value(v):
    if v is None:
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def rows_to_dicts(rows: list[tuple]) -> list[dict]:
    result = []
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        d = {}
        for i, col in enumerate(COLUMN_NAMES):
            d[col] = _serialize_value(row[i])
        d["_ingested_at"] = now
        result.append(d)
    return result


def upsert_to_supabase(rows: list[tuple]) -> int:
    dicts = rows_to_dicts(rows)
    total = 0

    for i in range(0, len(dicts), BATCH_SIZE):
        batch = dicts[i : i + BATCH_SIZE]
        r = supabase_upsert(SUPABASE_TABLE, batch)
        if r.status_code in (200, 201):
            total += len(batch)
            print(f"  Upserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows")
        else:
            print(f"  [ERROR] Batch {i // BATCH_SIZE + 1} failed: {r.status_code} {r.text[:300]}", file=sys.stderr)
            sys.exit(1)

    return total

# ---------------------------------------------------------------------------
# STEP 5 — update watermark
# ---------------------------------------------------------------------------

def update_watermark(sync_time: str, has_watermark_table: bool):
    if not has_watermark_table:
        return
    r = supabase_upsert(WATERMARK_TABLE, [{
        "table_name": SUPABASE_TABLE,
        "last_timestamp": sync_time,
    }])
    if r.status_code in (200, 201):
        print(f"[OK] Watermark updated: {SUPABASE_TABLE} → {sync_time}")
    else:
        print(f"[WARN] Watermark update failed: {r.status_code} {r.text[:200]}")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Databricks → Supabase sync (REST API)")
    print("=" * 60)

    verify_table()
    has_wm = verify_watermark_table()

    if has_wm:
        last = get_last_sync()
        if last:
            print(f"[INFO] Last successful sync: {last}")
        else:
            print("[INFO] No previous sync recorded — first run.")

    _, rows = fetch_databricks()
    if not rows:
        print("[INFO] No rows returned from Databricks. Nothing to sync.")
        return

    total = upsert_to_supabase(rows)
    sync_time = datetime.now(timezone.utc).isoformat()
    update_watermark(sync_time, has_wm)

    print("-" * 60)
    print(f"[DONE] Upserted {total} rows. Sync timestamp: {sync_time}")


if __name__ == "__main__":
    main()
