"""
Shim: re-export DatabricksConnector from transportistas_sync for scripts run from project root.

Usage:
    python connector.py                  # uses DATABRICKS_ENV from .env (default: qas)
    python connector.py --env qas        # force QAS environment
    python connector.py --env prd        # force PRD environment
"""
import argparse
import os
import sys

from transportistas_sync.databricks_connector import DatabricksConnector

__all__ = ["DatabricksConnector"]

DEFAULT_TABLES = {
    "qas": "qas.aplicaciones.transportistas_final",
    "prd": "prod.gldprd.fact_transporte",
}


def _load_env():
    """Load transportistas_sync/.env into os.environ when run from project root."""
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Databricks connectivity test")
    parser.add_argument(
        "--env", choices=["qas", "prd"], default=None,
        help="Environment to connect to (default: from .env DATABRICKS_ENV)",
    )
    parser.add_argument(
        "--table", default=None,
        help="Table to query (default: auto per environment)",
    )
    args = parser.parse_args()

    if not _load_env():
        print(
            "No transportistas_sync/.env found. Create it from .env.example.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.env:
        os.environ["DATABRICKS_ENV"] = args.env

    env = args.env or os.environ.get("DATABRICKS_ENV", "qas")
    c = DatabricksConnector(env=env)
    table = args.table or os.environ.get("DBX_TABLE") or DEFAULT_TABLES.get(env, DEFAULT_TABLES["qas"])

    print(f"Environment: {env.upper()}")
    print(f"Connector:   {c}")
    print(f"Table:       {table}")
    print("-" * 60)

    query = f"SELECT * FROM {table} LIMIT 5"
    try:
        cols, rows = c.execute_query(query)
        if not cols:
            print("(no columns)")
        else:
            print("Columns:", ", ".join(cols))
            for i, row in enumerate(rows, 1):
                print(f"  Row {i}:", row)
        print("-" * 60)
        print(f"Total: {len(rows)} row(s) (LIMIT 5)")
    except Exception as e:
        print(f"Error reading table: {e}", file=sys.stderr)
        sys.exit(1)
