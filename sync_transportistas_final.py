#!/usr/bin/env python3
"""
Sync transportistas_final from Databricks to Supabase

Synchronizes data from qas.club_transportistas.transportistas_final (Databricks)
to public.transportistas_final (Supabase) using watermark-based incremental sync.

Run after testing Supabase: python test_supabase_connection.py
Then: python sync_transportistas_final.py
"""
import os
import sys

# Load transportistas_sync/.env before importing sync_module (which needs Supabase/DB env)
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
    print("No transportistas_sync/.env found. Create it from .env.example.", file=sys.stderr)
    sys.exit(1)

try:
    from sync_module import DatabricksSupabaseSynchronizer
except ImportError:
    from .sync_module import DatabricksSupabaseSynchronizer


def main():
    """Main synchronization function for transportistas_final table."""
    # Configuration
    TABLE_NAME = "transportistas_final"
    OVERLAP_DAYS = int(os.environ.get("OVERLAP_DAYS", "7"))
    FETCH_SIZE = int(os.environ.get("FETCH_SIZE", "5000"))
    # Full Databricks table: catalog.schema.table (set in .env if different from default)
    DBR_TABLE = os.environ.get("DBX_TABLE", "qas.club_transportistas.transportistas_final")

    # Databricks query - using ultima_fecha_transporte for incremental sync
    DBR_QUERY = f"""
    SELECT
      codigo_transportista,
      nombre,
      documento,
      telefono,
      email,
      direccion,
      ciudad,
      codigo_postal,
      pais,
      datos_vehiculo,
      datos_dueno_vehiculo,
      estado,
      total_transportes,
      ultima_fecha_transporte
    FROM {DBR_TABLE}
    WHERE ultima_fecha_transporte >= ?
    ORDER BY ultima_fecha_transporte ASC, codigo_transportista ASC
    """
    
    # Supabase upsert SQL - using codigo_transportista as primary key
    UPSERT_SQL = """
    INSERT INTO public.transportistas_final (
      codigo_transportista,
      nombre,
      documento,
      telefono,
      email,
      direccion,
      ciudad,
      codigo_postal,
      pais,
      datos_vehiculo,
      datos_dueno_vehiculo,
      estado,
      total_transportes,
      ultima_fecha_transporte,
      _ingested_at
    )
    VALUES %s
    ON CONFLICT (codigo_transportista)
    DO UPDATE SET
      nombre = EXCLUDED.nombre,
      documento = EXCLUDED.documento,
      telefono = EXCLUDED.telefono,
      email = EXCLUDED.email,
      direccion = EXCLUDED.direccion,
      ciudad = EXCLUDED.ciudad,
      codigo_postal = EXCLUDED.codigo_postal,
      pais = EXCLUDED.pais,
      datos_vehiculo = EXCLUDED.datos_vehiculo,
      datos_dueno_vehiculo = EXCLUDED.datos_dueno_vehiculo,
      estado = EXCLUDED.estado,
      total_transportes = EXCLUDED.total_transportes,
      ultima_fecha_transporte = EXCLUDED.ultima_fecha_transporte,
      _ingested_at = NOW();
    """
    
    # Initialize synchronizer
    synchronizer = DatabricksSupabaseSynchronizer(
        table_name=TABLE_NAME,
        overlap_days=OVERLAP_DAYS,
        fetch_size=FETCH_SIZE
    )
    
    # Run synchronization
    # ultima_fecha_transporte is at index 13 in SELECT (0-based)
    # 14 columns from Databricks + NOW() for _ingested_at in Supabase
    UPSERT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
    total, max_date = synchronizer.sync(
        databricks_query=DBR_QUERY,
        upsert_sql=UPSERT_SQL,
        date_column_index=13,
        upsert_template=UPSERT_TEMPLATE,
    )
    
    print(f"\n[SUMMARY] Synchronized {total} rows. Max timestamp: {max_date}")


if __name__ == "__main__":
    main()
