#!/usr/bin/env python3
"""
Test Supabase (Postgres) connection before running sync.

Loads transportistas_sync/.env and connects using SUPABASE_DB_* or
SUPABASE_CONNECTION_STRING. Exits 0 on success, 1 on failure.
"""
import os
import sys


def _load_env():
    """Load transportistas_sync/.env into os.environ."""
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


def main():
    if not _load_env():
        print("No transportistas_sync/.env found. Create it from .env.example.", file=sys.stderr)
        sys.exit(1)

    conn_str = os.environ.get("SUPABASE_CONNECTION_STRING")
    if conn_str:
        try:
            import psycopg2
            conn = psycopg2.connect(conn_str)
        except Exception as e:
            print(f"Supabase connection failed (connection string): {e}", file=sys.stderr)
            sys.exit(1)
    else:
        host = os.environ.get("SUPABASE_DB_HOST")
        user = os.environ.get("SUPABASE_DB_USER")
        password = os.environ.get("SUPABASE_DB_PASSWORD")
        dbname = os.environ.get("SUPABASE_DB_NAME", "postgres")
        port = int(os.environ.get("SUPABASE_DB_PORT", "6543"))
        if not all([host, user, password]):
            print(
                "Set SUPABASE_DB_HOST, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD in .env "
                "(or SUPABASE_CONNECTION_STRING).",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                sslmode="require",
            )
        except Exception as e:
            print(f"Supabase connection failed: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            row = cur.fetchone()
            print("Supabase connection OK.")
            print(f"  Server: {row[0][:80]}..." if row and len(row[0]) > 80 else f"  Server: {row[0]}")
        # Optional: check if target table exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'transportistas_final'
                );
            """)
            exists = cur.fetchone()[0]
            if exists:
                print("  Table public.transportistas_final: exists.")
            else:
                print("  Table public.transportistas_final: not found (create it before sync).")
    finally:
        conn.close()

    sys.exit(0)


if __name__ == "__main__":
    main()
