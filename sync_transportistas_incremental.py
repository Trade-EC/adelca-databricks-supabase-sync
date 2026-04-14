#!/usr/bin/env python3
"""
Incremental sync: Databricks PRD → Supabase (public.transportistas).

Only inserts NEW records (codigo_transportista not yet in Supabase).
Existing records get their data updated but estado_transportista is preserved.

Flow:
  1. Fetch all codigo_transportista currently in Supabase.
  2. Fetch all rows from Databricks PRD.
  3. Split into NEW (insert with estado='pendiente') and EXISTING (update data only).
  4. Record sync timestamp in etl_watermarks.

Usage:
  python sync_transportistas_incremental.py
"""
import os
import sys
from datetime import datetime, timezone

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lambda_function"))
from id_generator import generate_transportista_id

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
WATERMARK_TABLE = "etl_watermarks"
DBX_TABLE = "prod.gldlogistica.db_trade_dim_app_transportistas"
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


def supabase_post(table: str, rows: list[dict], prefer: str = "return=minimal") -> httpx.Response:
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**HEADERS, "Prefer": prefer},
        json=rows,
        timeout=60,
    )


def supabase_upsert(table: str, rows: list[dict]) -> httpx.Response:
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=rows,
        timeout=60,
    )

# ---------------------------------------------------------------------------
# STEP 1 — Get existing codigo_transportista from Supabase
# ---------------------------------------------------------------------------

def get_existing_codigos() -> set[str]:
    """Fetch all codigo_transportista already in Supabase."""
    codigos = set()
    offset = 0
    batch = 1000
    while True:
        r = httpx.get(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
            headers={**HEADERS, "Range": f"{offset}-{offset + batch - 1}"},
            params={"select": "codigo_transportista"},
            timeout=30,
        )
        if r.status_code not in (200, 206):
            print(f"[ERROR] Failed to read existing codes: {r.status_code}", file=sys.stderr)
            sys.exit(1)
        data = r.json()
        if not data:
            break
        for row in data:
            codigos.add(row["codigo_transportista"])
        if len(data) < batch:
            break
        offset += batch
    return codigos

# ---------------------------------------------------------------------------
# STEP 2 — Fetch from Databricks PRD
# ---------------------------------------------------------------------------

def fetch_databricks() -> list[tuple]:
    from transportistas_sync.databricks_connector import DatabricksConnector

    query = f"""
    SELECT
      codigo_transportista, ruc, nombre_transportista,
      telefono, email, estado_transportista
    FROM {DBX_TABLE}
    """
    dbx = DatabricksConnector(env="prd")
    print(f"[INFO] Connecting to Databricks PRD (OAuth M2M)...")
    print(f"[INFO] Querying: {DBX_TABLE}")
    _, rows = dbx.execute_query(query)
    print(f"[INFO] Fetched {len(rows)} row(s) from Databricks.")
    return rows

# ---------------------------------------------------------------------------
# STEP 3 — Transform and split
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


def split_new_and_existing(
    rows: list[tuple], existing_codigos: set[str]
) -> tuple[list[dict], list[dict]]:
    now = datetime.now(timezone.utc).isoformat()
    new_rows = []
    update_rows = []

    for row in rows:
        codigo = str(row[0]).strip() if row[0] else None
        ruc = str(row[1]).strip() if row[1] else None

        if not codigo or not ruc:
            continue

        record = {
            "codigo_transportista": codigo,
            "ruc": ruc,
            "nombre_transportista": row[2],
            "telefono": row[3],
            "email": str(row[4]).strip() if row[4] else None,
            "_ingested_at": now,
        }

        if codigo not in existing_codigos:
            record["transportista_id"] = generate_transportista_id(codigo)
            record["estado_transportista"] = "pendiente"
            new_rows.append(record)

    return new_rows

# ---------------------------------------------------------------------------
# STEP 4 — Insert new + update existing
# ---------------------------------------------------------------------------

def insert_new(rows: list[dict]) -> int:
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        r = supabase_post(SUPABASE_TABLE, batch)
        if r.status_code in (200, 201):
            total += len(batch)
            print(f"  [NEW] Inserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows")
        else:
            print(
                f"  [ERROR] Insert batch {i // BATCH_SIZE + 1} failed: "
                f"{r.status_code} {r.text[:300]}",
                file=sys.stderr,
            )
            sys.exit(1)
    return total


# ---------------------------------------------------------------------------
# STEP 5 — Watermark
# ---------------------------------------------------------------------------

def update_watermark(sync_time: str):
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
    print("  INCREMENTAL SYNC: Databricks PRD → Supabase")
    print(f"  {DBX_TABLE} → public.{SUPABASE_TABLE}")
    print("=" * 60)

    # Verify tables accessible
    r = supabase_get(SUPABASE_TABLE, {"select": "transportista_id", "limit": "1"})
    if r.status_code != 200:
        print(f"[ERROR] Cannot access '{SUPABASE_TABLE}': {r.status_code}", file=sys.stderr)
        sys.exit(1)
    print(f"[OK] Table '{SUPABASE_TABLE}' is accessible.")

    r_wm = supabase_get(WATERMARK_TABLE, {"select": "table_name", "limit": "1"})
    if r_wm.status_code != 200:
        print(f"[ERROR] Cannot access '{WATERMARK_TABLE}': {r_wm.status_code}", file=sys.stderr)
        sys.exit(1)
    print(f"[OK] Table '{WATERMARK_TABLE}' is accessible.")

    # Step 1: get existing codes and count
    print("[INFO] Fetching existing codigos from Supabase...")
    existing = get_existing_codigos()
    count_before = len(existing)
    print(f"[INFO] Found {count_before} existing records in Supabase.")

    # Step 2: fetch from Databricks
    dbx_rows = fetch_databricks()
    if not dbx_rows:
        print("[INFO] No rows returned from Databricks. Nothing to sync.")
        return

    # Step 2b: deduplicate
    dbx_rows = deduplicate_rows(dbx_rows)

    # Step 3: filter new only
    new_rows = split_new_and_existing(dbx_rows, existing)
    skipped = len(dbx_rows) - len(new_rows)
    print(f"[INFO] New records to insert: {len(new_rows)}")
    print(f"[INFO] Already in Supabase (skipped): {skipped}")

    # Step 4: insert new
    inserted = 0

    if new_rows:
        print(f"\n[INFO] Inserting {len(new_rows)} new records (estado='pendiente')...")
        inserted = insert_new(new_rows)
    else:
        print("[INFO] No new records to insert.")

    # Step 5: post-sync verification
    post_codigos = get_existing_codigos()
    count_after = len(post_codigos)
    expected = count_before + inserted
    if count_after == expected:
        print(f"[OK] Post-sync verification: {count_after} rows in Supabase (was {count_before}, +{inserted} new).")
    else:
        print(
            f"[WARN] Post-sync count mismatch: expected {expected}, found {count_after}.",
            file=sys.stderr,
        )

    # Step 6: watermark
    sync_time = datetime.now(timezone.utc).isoformat()
    update_watermark(sync_time)

    print("-" * 60)
    print(f"[DONE] Inserted: {inserted} | Total in Supabase: {count_after} | Sync: {sync_time}")


if __name__ == "__main__":
    main()
