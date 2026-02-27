"""
AWS Lambda handler: Databricks → Supabase sync.

Triggered by EventBridge (hourly schedule). All config comes from
Lambda environment variables — no .env file needed.
"""
import os
import json
import logging
from datetime import datetime, date, timezone
from typing import Any

import httpx
from databricks import sql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# CONFIG (from Lambda environment variables)
# ---------------------------------------------------------------------------

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

DBX_TABLE = os.environ.get("DBX_TABLE", "qas.aplicaciones.transportistas_final")
SUPABASE_TABLE = "transportistas_final"
WATERMARK_TABLE = "etl_watermarks"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))

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
# DATABRICKS
# ---------------------------------------------------------------------------

def fetch_databricks() -> tuple[list[str], list[tuple]]:
    query = f"""
    SELECT
      codigo_transportista, nombre, documento, telefono, email,
      direccion, ciudad, codigo_postal, pais,
      datos_vehiculo, datos_dueno_vehiculo, estado,
      total_transportes, ultima_fecha_transporte
    FROM {DBX_TABLE}
    """
    logger.info(f"Querying Databricks: {DBX_TABLE}")
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            cols = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            row_tuples = [tuple(r) if not isinstance(r, tuple) else r for r in rows]
    logger.info(f"Fetched {len(row_tuples)} row(s) from Databricks.")
    return cols, row_tuples

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


def supabase_upsert(table: str, rows: list[dict]) -> httpx.Response:
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=rows,
        timeout=60,
    )


def _serialize(v: Any):
    if v is None:
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def rows_to_dicts(rows: list[tuple], ingested_at: str) -> list[dict]:
    result = []
    for row in rows:
        d = {col: _serialize(row[i]) for i, col in enumerate(COLUMN_NAMES)}
        d["_ingested_at"] = ingested_at
        result.append(d)
    return result

# ---------------------------------------------------------------------------
# SYNC LOGIC
# ---------------------------------------------------------------------------

def run_sync() -> dict:
    """Run the full sync and return a summary dict."""
    sync_start = datetime.now(timezone.utc)

    # Verify Supabase tables
    r = supabase_get(SUPABASE_TABLE, {"select": "codigo_transportista", "limit": "1"})
    if r.status_code != 200:
        raise RuntimeError(f"Cannot access {SUPABASE_TABLE}: {r.status_code} {r.text[:200]}")
    logger.info(f"Table '{SUPABASE_TABLE}' accessible.")

    has_watermark = supabase_get(WATERMARK_TABLE, {"select": "table_name", "limit": "1"}).status_code == 200

    # Read last sync
    last_sync = None
    if has_watermark:
        r = supabase_get(WATERMARK_TABLE, {
            "select": "last_timestamp",
            "table_name": f"eq.{SUPABASE_TABLE}",
        })
        if r.status_code == 200:
            data = r.json()
            if data and data[0].get("last_timestamp"):
                last_sync = data[0]["last_timestamp"]
        logger.info(f"Last sync: {last_sync or 'never'}")

    # Fetch from Databricks
    _, rows = fetch_databricks()
    if not rows:
        logger.info("No rows from Databricks. Nothing to sync.")
        return {"status": "no_data", "rows": 0, "last_sync": last_sync}

    # Upsert to Supabase
    ingested_at = sync_start.isoformat()
    dicts = rows_to_dicts(rows, ingested_at)
    total = 0
    for i in range(0, len(dicts), BATCH_SIZE):
        batch = dicts[i : i + BATCH_SIZE]
        r = supabase_upsert(SUPABASE_TABLE, batch)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Upsert failed: {r.status_code} {r.text[:300]}")
        total += len(batch)
        logger.info(f"Upserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows")

    # Update watermark
    sync_time = datetime.now(timezone.utc).isoformat()
    if has_watermark:
        r = supabase_upsert(WATERMARK_TABLE, [{
            "table_name": SUPABASE_TABLE,
            "last_timestamp": sync_time,
        }])
        if r.status_code in (200, 201):
            logger.info(f"Watermark updated: {sync_time}")
        else:
            logger.warning(f"Watermark update failed: {r.status_code}")

    return {
        "status": "success",
        "rows_upserted": total,
        "sync_timestamp": sync_time,
        "previous_sync": last_sync,
    }

# ---------------------------------------------------------------------------
# LAMBDA HANDLER
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """Entry point for AWS Lambda."""
    logger.info(f"Event: {json.dumps(event, default=str)}")

    try:
        result = run_sync()
        logger.info(f"Sync complete: {json.dumps(result)}")
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }
    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)}),
        }
