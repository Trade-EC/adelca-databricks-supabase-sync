"""
Sync module: incremental sync from Databricks to Supabase (Postgres).

Uses transportistas_sync.DatabricksConnector for Databricks and psycopg2
for Supabase (SUPABASE_DB_* or SUPABASE_CONNECTION_STRING in env).
"""
import os
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

# Import after env is loaded (caller must load transportistas_sync/.env first)
def _get_supabase_conn():
    """Build Supabase Postgres connection from env."""
    conn_str = os.environ.get("SUPABASE_CONNECTION_STRING")
    if conn_str:
        return psycopg2.connect(conn_str)
    host = os.environ.get("SUPABASE_DB_HOST")
    user = os.environ.get("SUPABASE_DB_USER")
    password = os.environ.get("SUPABASE_DB_PASSWORD")
    dbname = os.environ.get("SUPABASE_DB_NAME", "postgres")
    port = int(os.environ.get("SUPABASE_DB_PORT", "6543"))
    if not all([host, user, password]):
        raise ValueError(
            "Missing Supabase config. Set SUPABASE_DB_HOST, SUPABASE_DB_USER, "
            "SUPABASE_DB_PASSWORD (and optionally SUPABASE_DB_NAME, SUPABASE_DB_PORT) "
            "or SUPABASE_CONNECTION_STRING."
        )
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode="require",
    )


class DatabricksSupabaseSynchronizer:
    """Incremental sync from Databricks (parameterized query) to Supabase (upsert)."""

    def __init__(
        self,
        table_name: str,
        overlap_days: int = 7,
        fetch_size: int = 5000,
    ):
        self.table_name = table_name
        self.overlap_days = overlap_days
        self.fetch_size = fetch_size

    def _get_watermark(self, conn) -> datetime:
        """Return (max date in Supabase table) - overlap_days, or far past if empty."""
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(MAX(ultima_fecha_transporte) - (%s::int * INTERVAL '1 day'),
                                '1900-01-01'::timestamp)
                FROM public.{self.table_name}
                """,
                (self.overlap_days,),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else datetime(1900, 1, 1)

    def sync(
        self,
        databricks_query: str,
        upsert_sql: str,
        date_column_index: int,
        upsert_template: str | None = None,
    ) -> tuple[int, datetime | None]:
        """
        Run incremental sync: watermark from Supabase, fetch from Databricks, upsert to Supabase.

        - databricks_query: SQL with a single ? for the watermark date (e.g. WHERE ultima_fecha_transporte >= ?)
        - upsert_sql: INSERT ... VALUES %s ON CONFLICT ... (one %s for execute_values)
        - date_column_index: 0-based index of the date column in the SELECT (for max_date)
        - upsert_template: optional template for execute_values, e.g. "(%s, %s, ..., NOW())" for 14 cols + NOW()

        Returns (total_rows_upserted, max_date_from_fetch).
        """
        from transportistas_sync.databricks_connector import DatabricksConnector

        pg_conn = _get_supabase_conn()
        try:
            watermark = self._get_watermark(pg_conn)
        finally:
            pg_conn.close()

        dbx = DatabricksConnector()
        cols, rows = dbx.execute_query(databricks_query, [watermark])
        if not rows:
            return 0, None

        total = 0
        max_date = None
        # Rows are 14-tuples; upsert expects 15 columns (last is _ingested_at = NOW())
        if upsert_template is None:
            # 14 columns from Databricks + NOW() for _ingested_at
            upsert_template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"

        for i in range(0, len(rows), self.fetch_size):
            batch = rows[i : i + self.fetch_size]
            for r in batch:
                if date_column_index < len(r) and r[date_column_index] is not None:
                    d = r[date_column_index]
                    if max_date is None or (d and d > max_date):
                        max_date = d
            conn = _get_supabase_conn()
            try:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        upsert_sql,
                        batch,
                        template=upsert_template,
                        page_size=self.fetch_size,
                    )
                conn.commit()
                total += len(batch)
            finally:
                conn.close()

        return total, max_date
