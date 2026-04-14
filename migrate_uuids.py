#!/usr/bin/env python3
"""One-time migration: regenerate transportista_id using deterministic uuid5."""
import os
import sys

sys.path.insert(0, "transportistas_sync")
sys.path.insert(0, "lambda_function")

import httpx
from dotenv import load_dotenv
from id_generator import generate_transportista_id

load_dotenv("transportistas_sync/.env")

URL = os.environ["SUPABASE_URL"].rstrip("/")
KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}

TABLES_WITH_IDS = ["transportistas", "dim_transportistas", "transportistas_duplicate"]
FK_TABLES = [
    ("facturas", "transportista_id"),
    ("carrier_deletion_feedbacks", "carrier_id"),
    ("purchases", "carrier_id"),
]


def fetch_all(table):
    records = []
    offset = 0
    while True:
        r = httpx.get(
            f"{URL}/rest/v1/{table}?select=transportista_id,codigo_transportista&order=codigo_transportista.asc",
            headers={**H, "Range": f"{offset}-{offset+999}"},
            timeout=30,
        )
        data = r.json()
        if not data:
            break
        records.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    return records


def main():
    print("=" * 70, flush=True)
    print("MIGRACION: Regenerar UUIDs con uuid5 + Actualizar FKs", flush=True)
    print("=" * 70, flush=True)

    for table in TABLES_WITH_IDS:
        print(f"\n--- Tabla: {table} ---", flush=True)

        records = fetch_all(table)
        print(f"  Registros: {len(records)}", flush=True)

        changes = []
        for rec in records:
            codigo = rec["codigo_transportista"]
            old_id = rec["transportista_id"]
            new_id = generate_transportista_id(codigo)
            if old_id != new_id:
                changes.append((old_id, new_id, codigo))

        print(f"  Ya correctos: {len(records) - len(changes)}", flush=True)
        print(f"  A actualizar: {len(changes)}", flush=True)

        if not changes:
            continue

        updated = 0
        errors = 0
        for old_id, new_id, codigo in changes:
            r = httpx.patch(
                f"{URL}/rest/v1/{table}?transportista_id=eq.{old_id}",
                headers={**H, "Prefer": "return=minimal"},
                json={"transportista_id": new_id},
                timeout=30,
            )
            if r.status_code in (200, 204):
                updated += 1

                if table == "transportistas":
                    for fk_table, fk_col in FK_TABLES:
                        r_fk = httpx.patch(
                            f"{URL}/rest/v1/{fk_table}?{fk_col}=eq.{old_id}",
                            headers={**H, "Prefer": "return=minimal"},
                            json={fk_col: new_id},
                            timeout=30,
                        )
                        if r_fk.status_code in (200, 204):
                            pass
            else:
                errors += 1
                if errors <= 5:
                    print(f"  ERROR {codigo}: {r.status_code} {r.text[:200]}", flush=True)

            if updated % 100 == 0 and updated > 0:
                print(f"  Progreso: {updated}/{len(changes)}", flush=True)

        print(f"  Actualizados: {updated} | Errores: {errors}", flush=True)

    print("\n" + "=" * 70, flush=True)
    print("MIGRACION DE IDs COMPLETADA", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
