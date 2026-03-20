"""
AWS Lambda handler: Databricks PRD → Supabase incremental sync.

Triggered by EventBridge schedule. Uses OAuth M2M (Service Principal)
for Databricks and REST API for Supabase.

New records get estado='pendiente'; existing records get data updated
without touching estado_transportista.
"""
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# CONFIG (from Lambda environment variables)
# ---------------------------------------------------------------------------

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

DBX_TABLE = os.environ.get("DBX_TABLE", "prod.gldprd.dim_app_transportistas")
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "dim_transportistas")
WATERMARK_TABLE = "etl_watermarks"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# DATABRICKS (OAuth M2M)
# ---------------------------------------------------------------------------

def _get_connection():
    host = DATABRICKS_HOST
    cid = DATABRICKS_CLIENT_ID
    csecret = DATABRICKS_CLIENT_SECRET

    def _credential_provider():
        config = Config(
            host=f"https://{host}",
            client_id=cid,
            client_secret=csecret,
        )
        return oauth_service_principal(config)

    return sql.connect(
        server_hostname=host,
        http_path=DATABRICKS_HTTP_PATH,
        credentials_provider=_credential_provider,
    )


def fetch_databricks() -> list[tuple]:
    query = f"""
    SELECT
      codigo_transportista, ruc, nombre_transportista,
      telefono, email, estado_transportista,
      placas, tipos_vehiculo
    FROM {DBX_TABLE}
    """
    logger.info("Querying Databricks PRD: %s", DBX_TABLE)
    with _get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            row_tuples = [tuple(r) if not isinstance(r, tuple) else r for r in rows]
    logger.info("Fetched %d row(s) from Databricks.", len(row_tuples))
    return row_tuples

# ---------------------------------------------------------------------------
# SUPABASE REST
# ---------------------------------------------------------------------------

def supabase_get(table: str, params: dict | None = None) -> httpx.Response:
    return httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params=params or {},
        timeout=30,
    )


def supabase_post(table: str, rows: list[dict]) -> httpx.Response:
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**HEADERS, "Prefer": "return=minimal"},
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
# HELPERS
# ---------------------------------------------------------------------------

def get_existing_codigos() -> set[str]:
    codigos: set[str] = set()
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
            raise RuntimeError(f"Failed to read existing codes: {r.status_code}")
        data = r.json()
        if not data:
            break
        for row in data:
            codigos.add(row["codigo_transportista"])
        if len(data) < batch:
            break
        offset += batch
    return codigos


def _serialize_array(v: Any):
    if v is None:
        return None
    if hasattr(v, "tolist"):
        v = v.tolist()
    if isinstance(v, str):
        v = v.strip()
        if v.startswith("["):
            try:
                v = json.loads(v)
            except (json.JSONDecodeError, ValueError):
                return [v]
        elif v.startswith("{") and v.endswith("}"):
            v = [item.strip().strip('"') for item in v[1:-1].split(",") if item.strip()]
        else:
            return [v] if v else None
    if isinstance(v, (list, tuple)):
        return [str(item).strip() if item is not None else None for item in v]
    return [v] if v else None


def deduplicate_rows(rows: list[tuple]) -> list[tuple]:
    seen: dict[str, tuple] = {}
    for row in rows:
        codigo = str(row[0]).strip() if row[0] else None
        if codigo:
            seen[codigo] = row
    dupes = len(rows) - len(seen)
    if dupes:
        logger.warning("Removed %d duplicate codigo_transportista from Databricks data.", dupes)
    return list(seen.values())


def split_new_and_existing(
    rows: list[tuple], existing_codigos: set[str], now: str
) -> tuple[list[dict], list[dict]]:
    new_rows: list[dict] = []
    update_rows: list[dict] = []

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
            "placas": _serialize_array(row[6]),
            "tipos_vehiculo": _serialize_array(row[7]),
            "_ingested_at": now,
        }

        if codigo not in existing_codigos:
            record["transportista_id"] = str(uuid.uuid4())
            record["estado_transportista"] = "pendiente"
            new_rows.append(record)

    return new_rows

# ---------------------------------------------------------------------------
# SYNC LOGIC
# ---------------------------------------------------------------------------

def run_sync() -> dict:
    sync_start = datetime.now(timezone.utc)
    now = sync_start.isoformat()

    # Validate Supabase tables
    r = supabase_get(SUPABASE_TABLE, {"select": "transportista_id", "limit": "1"})
    if r.status_code != 200:
        raise RuntimeError(f"Cannot access '{SUPABASE_TABLE}': {r.status_code}")
    logger.info("Table '%s' accessible.", SUPABASE_TABLE)

    r_wm = supabase_get(WATERMARK_TABLE, {"select": "table_name", "limit": "1"})
    if r_wm.status_code != 200:
        raise RuntimeError(f"Cannot access '{WATERMARK_TABLE}': {r_wm.status_code}")

    # Get existing codes
    existing = get_existing_codigos()
    count_before = len(existing)
    logger.info("Found %d existing records in Supabase.", count_before)

    # Fetch from Databricks
    dbx_rows = fetch_databricks()
    if not dbx_rows:
        logger.info("No rows from Databricks. Nothing to sync.")
        return {"status": "no_data", "inserted": 0, "total": count_before}

    # Deduplicate
    dbx_rows = deduplicate_rows(dbx_rows)

    # Filter new only
    new_rows = split_new_and_existing(dbx_rows, existing, now)
    logger.info("New: %d | Already in Supabase: %d (skipped)", len(new_rows), len(dbx_rows) - len(new_rows))

    # Insert new
    inserted = 0
    if new_rows:
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i : i + BATCH_SIZE]
            r = supabase_post(SUPABASE_TABLE, batch)
            if r.status_code not in (200, 201):
                raise RuntimeError(f"Insert failed: {r.status_code} {r.text[:300]}")
            inserted += len(batch)
            logger.info("[NEW] Batch %d: %d rows", i // BATCH_SIZE + 1, len(batch))
    else:
        logger.info("No new records to insert.")

    # Post-sync verification
    post_codigos = get_existing_codigos()
    count_after = len(post_codigos)
    expected = count_before + inserted
    if count_after != expected:
        logger.warning("Post-sync mismatch: expected %d, found %d.", expected, count_after)
    else:
        logger.info("Post-sync OK: %d rows (was %d, +%d new).", count_after, count_before, inserted)

    # Watermark
    sync_time = datetime.now(timezone.utc).isoformat()
    r = supabase_upsert(WATERMARK_TABLE, [{
        "table_name": SUPABASE_TABLE,
        "last_timestamp": sync_time,
    }])
    if r.status_code in (200, 201):
        logger.info("Watermark updated: %s", sync_time)
    else:
        logger.warning("Watermark update failed: %s", r.status_code)

    return {
        "status": "success",
        "inserted": inserted,
        "total": count_after,
        "sync_timestamp": sync_time,
    }

# ---------------------------------------------------------------------------
# LAMBDA HANDLER
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event, default=str))

    try:
        result = run_sync()
        logger.info("Sync complete: %s", json.dumps(result))
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }
    except Exception as e:
        logger.error("Sync failed: %s", e, exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)}),
        }
