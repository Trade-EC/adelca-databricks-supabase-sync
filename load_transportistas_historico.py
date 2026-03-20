#!/usr/bin/env python3
"""
One-time historical load: Databricks PRD → Supabase (public.transportistas).

Reads ALL rows from prod.gldprd.dim_app_transportistas and inserts them
into Supabase. Every record gets a new UUID and estado='pendiente'.

Run ONCE on an empty table, then use sync_transportistas_incremental.py
for ongoing syncs.

Usage:
  python load_transportistas_historico.py
"""
import os
import sys
import uuid
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
SUPABASE_TABLE = "transportistas"
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

# ---------------------------------------------------------------------------
# SUPABASE HELPERS
# ---------------------------------------------------------------------------

def supabase_get(table: str, params: dict | None = None) -> httpx.Response:
    return httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params=params or {},
        timeout=30,
    )


def supabase_insert(table: str, rows: list[dict]) -> httpx.Response:
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**HEADERS, "Prefer": "return=minimal"},
        json=rows,
        timeout=60,
    )

# ---------------------------------------------------------------------------
# SAFETY CHECK
# ---------------------------------------------------------------------------

def check_table_empty():
    r = supabase_get(SUPABASE_TABLE, {
        "select": "transportista_id",
        "limit": "1",
    })
    if r.status_code != 200:
        print(
            f"[ERROR] Cannot access '{SUPABASE_TABLE}': {r.status_code}\n"
            f"  Create it first in Supabase SQL Editor.",
            file=sys.stderr,
        )
        sys.exit(1)
    data = r.json()
    if data:
        print(
            f"[ERROR] Table '{SUPABASE_TABLE}' is NOT empty.\n"
            f"  This script is for the initial historical load only.\n"
            f"  Use sync_transportistas_incremental.py for ongoing syncs.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"[OK] Table '{SUPABASE_TABLE}' exists and is empty.")

# ---------------------------------------------------------------------------
# FETCH FROM DATABRICKS PRD
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
    print(f"[INFO] Fetched {len(rows)} row(s).")
    return cols, rows

# ---------------------------------------------------------------------------
# TRANSFORM + INSERT
# ---------------------------------------------------------------------------

def _serialize_array(v):
    if v is None:
        return None
    if hasattr(v, "tolist"):
        v = v.tolist()
    if isinstance(v, (list, tuple)):
        return [str(item).strip() if item is not None else None for item in v]
    return v


def deduplicate_rows(rows: list[tuple]) -> list[tuple]:
    """Keep only the last occurrence of each codigo_transportista."""
    seen: dict[str, tuple] = {}
    for row in rows:
        codigo = str(row[0]).strip() if row[0] else None
        if codigo:
            seen[codigo] = row
    dupes = len(rows) - len(seen)
    if dupes:
        print(f"[WARN] Removed {dupes} duplicate codigo_transportista from Databricks data.")
    return list(seen.values())


def transform_rows(rows: list[tuple]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    result = []
    for row in rows:
        codigo = str(row[0]).strip() if row[0] else None
        ruc = str(row[1]).strip() if row[1] else None

        if not codigo or not ruc:
            continue

        result.append({
            "transportista_id": str(uuid.uuid4()),
            "codigo_transportista": codigo,
            "ruc": ruc,
            "nombre_transportista": row[2],
            "telefono": row[3],
            "email": str(row[4]).strip() if row[4] else None,
            "estado_transportista": "pendiente",
            "placas": _serialize_array(row[6]),
            "tipos_vehiculo": _serialize_array(row[7]),
            "_ingested_at": now,
        })
    return result


def insert_to_supabase(dicts: list[dict]) -> int:
    total = 0
    for i in range(0, len(dicts), BATCH_SIZE):
        batch = dicts[i : i + BATCH_SIZE]
        r = supabase_insert(SUPABASE_TABLE, batch)
        if r.status_code in (200, 201):
            total += len(batch)
            print(f"  Inserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows")
        else:
            print(
                f"  [ERROR] Batch {i // BATCH_SIZE + 1} failed: "
                f"{r.status_code} {r.text[:300]}",
                file=sys.stderr,
            )
            sys.exit(1)
    return total

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  HISTORICAL LOAD: Databricks PRD → Supabase")
    print(f"  {DBX_TABLE} → public.{SUPABASE_TABLE}")
    print("=" * 60)

    check_table_empty()

    _, rows = fetch_databricks()
    if not rows:
        print("[INFO] No rows returned from Databricks. Nothing to load.")
        return

    rows = deduplicate_rows(rows)

    dicts = transform_rows(rows)
    skipped = len(rows) - len(dicts)
    if skipped:
        print(f"[WARN] Skipped {skipped} rows with NULL codigo_transportista or ruc.")

    print(f"[INFO] Inserting {len(dicts)} rows (all with estado='pendiente')...")
    total = insert_to_supabase(dicts)

    # Post-insert verification
    r = supabase_get(SUPABASE_TABLE, {"select": "transportista_id"})
    if r.status_code in (200, 206):
        actual = len(r.json())
        if actual >= total:
            print(f"[OK] Post-insert verification: {actual} rows in Supabase.")
        else:
            print(
                f"[WARN] Post-insert mismatch: expected {total}, found {actual}.",
                file=sys.stderr,
            )
    else:
        print(f"[WARN] Could not verify post-insert count: {r.status_code}")

    print("-" * 60)
    print(f"[DONE] Inserted {total} rows. Historical load complete.")
    print(f"  Next: use sync_transportistas_incremental.py for ongoing syncs.")


if __name__ == "__main__":
    main()
