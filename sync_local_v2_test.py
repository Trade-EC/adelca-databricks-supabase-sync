"""
Local test sync: QAS dim_app_transportistas_v2 → Supabase transportistas_duplicate
NOT for production use. Does not modify the Lambda or AWS deployment.
"""
import json
import os
import sys
from datetime import datetime, timezone

import httpx

sys.path.insert(0, "transportistas_sync")
sys.path.insert(0, "lambda_function")
from dotenv import load_dotenv
from databricks_connector import DatabricksConnector
from id_generator import generate_transportista_id

load_dotenv("transportistas_sync/.env")

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_TABLE = "transportistas_duplicate"
DBX_TABLE = "qas.gldqas.dim_app_transportistas_v2"
BATCH_SIZE = 500

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


def fetch_databricks():
    print(f"[1/5] Conectando a QAS Databricks → {DBX_TABLE}")
    conn = DatabricksConnector(env="qas")
    with conn._connection() as c:
        with c.cursor() as cur:
            cur.execute(f"""
                SELECT codigo_transportista, ruc, nombre_transportista,
                       telefono, email, estado_transportista
                FROM {DBX_TABLE}
            """)
            rows = [tuple(r) for r in cur.fetchall()]
    print(f"  Obtenidos: {len(rows)} registros")
    return rows


def get_existing_codigos():
    print(f"[2/5] Leyendo codigos existentes en Supabase → {SUPABASE_TABLE}")
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
            raise RuntimeError(f"Error leyendo Supabase: {r.status_code}")
        data = r.json()
        if not data:
            break
        for row in data:
            if row.get("codigo_transportista"):
                codigos.add(row["codigo_transportista"])
        if len(data) < batch:
            break
        offset += batch
    print(f"  Existentes en Supabase: {len(codigos)}")
    return codigos


def deduplicate(rows):
    seen = {}
    for row in rows:
        codigo = str(row[0]).strip() if row[0] else None
        if codigo:
            seen[codigo] = row
    dupes = len(rows) - len(seen)
    if dupes:
        print(f"  Duplicados eliminados: {dupes}")
    return list(seen.values())


def build_new_records(rows, existing, now):
    new_rows = []
    for row in rows:
        codigo = str(row[0]).strip() if row[0] else None
        ruc = str(row[1]).strip() if row[1] else None
        if not codigo or not ruc:
            continue
        if codigo not in existing:
            new_rows.append({
                "transportista_id": generate_transportista_id(codigo),
                "codigo_transportista": codigo,
                "ruc": ruc,
                "nombre_transportista": row[2],
                "telefono": row[3],
                "email": str(row[4]).strip() if row[4] else None,
                "estado_transportista": "pendiente",
                "placas": None,
                "tipos_vehiculo": None,
                "_ingested_at": now,
            })
    return new_rows


def insert_batch(rows):
    r = httpx.post(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
        headers={**HEADERS, "Prefer": "return=minimal"},
        json=rows,
        timeout=60,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Insert failed: {r.status_code} {r.text[:300]}")
    return len(rows)


def main():
    now = datetime.now(timezone.utc).isoformat()
    print("=" * 60)
    print("SYNC LOCAL: QAS v2 → Supabase transportistas_duplicate")
    print("=" * 60)

    dbx_rows = fetch_databricks()
    existing = get_existing_codigos()

    dbx_rows = deduplicate(dbx_rows)
    print(f"  Registros únicos de Databricks: {len(dbx_rows)}")

    new_rows = build_new_records(dbx_rows, existing, now)
    print(f"\n[3/5] Nuevos registros a insertar: {len(new_rows)}")
    print(f"  Ya existentes (skip): {len(dbx_rows) - len(new_rows)}")

    if new_rows:
        emails_con_valor = sum(1 for r in new_rows if r["email"] and r["email"].strip())
        print(f"  Nuevos con email: {emails_con_valor}")

    inserted = 0
    if new_rows:
        print(f"\n[4/5] Insertando en batches de {BATCH_SIZE}...")
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i: i + BATCH_SIZE]
            inserted += insert_batch(batch)
            print(f"  Batch {i // BATCH_SIZE + 1}: {len(batch)} insertados")
    else:
        print("\n[4/5] No hay registros nuevos. Nada que insertar.")

    post_count = len(get_existing_codigos())
    print(f"\n[5/5] Verificación post-sync:")
    print(f"  Total en Supabase: {post_count}")
    print(f"  Insertados: {inserted}")
    print("=" * 60)
    print("COMPLETADO")
    print("=" * 60)


if __name__ == "__main__":
    main()
