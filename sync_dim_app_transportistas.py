#!/usr/bin/env python3
"""
Full sync: Databricks PRD → Supabase (public.dim_app_transportistas).

Uses Supabase REST API (HTTPS) with OAuth M2M (Service Principal) for Databricks.

Steps:
  1. Verify target table exists in Supabase via REST.
  2. Fetch all rows from prod.gldprd.dim_app_transportistas (Databricks PRD).
  3. Upsert into Supabase via PostgREST (ON CONFLICT codigo_transportista).
  4. Record sync timestamp in public.etl_watermarks.

Usage:
  python sync_dim_app_transportistas.py
"""
import os
import sys
from datetime import datetime, timezone

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
SUPABASE_TABLE = "dim_app_transportistas"
WATERMARK_TABLE = "etl_watermarks"
DBX_TABLE = "prod.gldprd.dim_app_transportistas"
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
    "codigo_transportista", "ruc", "nombre_transportista",
    "telefono", "email", "estado_transportista",
    "placas", "tipos_vehiculo",
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
    print(
        f"[ERROR] Cannot access '{SUPABASE_TABLE}': {r.status_code}\n"
        f"  Create it first in Supabase SQL Editor (see supabase_create_tables.sql)",
        file=sys.stderr,
    )
    sys.exit(1)


def verify_watermark_table():
    r = supabase_get(WATERMARK_TABLE, {"select": "table_name", "limit": "1"})
    if r.status_code == 200:
        print(f"[OK] Table '{WATERMARK_TABLE}' exists and is accessible.")
        return True
    print(f"[WARN] '{WATERMARK_TABLE}' not accessible. Watermark tracking disabled.")
    return False

# ---------------------------------------------------------------------------
# STEP 2 — fetch from Databricks PRD
# ---------------------------------------------------------------------------

def fetch_databricks():
    from transportistas_sync.databricks_connector import DatabricksConnector

    query = f"""
    SELECT
      codigo_transportista, ruc, nombre_transportista,
      telefono, email, estado_transportista,
      placas, tipos_vehiculo
    FROM {DBX_TABLE}
    """
    dbx = DatabricksConnector(env="prd")
    print(f"[INFO] Connecting to Databricks PRD (OAuth M2M)...")
    print(f"[INFO] Querying: {DBX_TABLE}")
    cols, rows = dbx.execute_query(query)
    print(f"[INFO] Fetched {len(rows)} row(s) from Databricks PRD.")
    return cols, rows

# ---------------------------------------------------------------------------
# STEP 3 — upsert via REST API
# ---------------------------------------------------------------------------

def _serialize_value(v):
    if v is None:
        return None
    if hasattr(v, "tolist"):
        v = v.tolist()
    if isinstance(v, (list, tuple)):
        return [str(item).strip() if item is not None else None for item in v]
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
            print(
                f"  [ERROR] Batch {i // BATCH_SIZE + 1} failed: "
                f"{r.status_code} {r.text[:300]}",
                file=sys.stderr,
            )
            sys.exit(1)

    return total

# ---------------------------------------------------------------------------
# STEP 4 — update watermark
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
    print("  Databricks PRD → Supabase sync")
    print(f"  {DBX_TABLE} → public.{SUPABASE_TABLE}")
    print("=" * 60)

    verify_table()
    has_wm = verify_watermark_table()

    if has_wm:
        r = supabase_get(WATERMARK_TABLE, {
            "select": "last_timestamp",
            "table_name": f"eq.{SUPABASE_TABLE}",
        })
        if r.status_code == 200:
            data = r.json()
            if data and data[0].get("last_timestamp"):
                print(f"[INFO] Last successful sync: {data[0]['last_timestamp']}")
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
