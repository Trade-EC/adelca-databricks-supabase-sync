"""
Shim: re-export DatabricksConnector from transportistas_sync for scripts run from project root.
Run as script to load .env and read the default table: python connector.py
"""
import os
import sys

from transportistas_sync.databricks_connector import DatabricksConnector

__all__ = ["DatabricksConnector"]

# Default table to read when run as __main__
DEFAULT_TABLE = os.environ.get("DBX_TABLE", "qas.aplicaciones.transportistas_final")


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
    if not _load_env():
        print("No transportistas_sync/.env found. Create it from .env.example and set DATABRICKS_*", file=sys.stderr)
        sys.exit(1)
    c = DatabricksConnector()
    table = DEFAULT_TABLE
    query = f"SELECT * FROM {table}"
    try:
        cols, rows = c.execute_query(query)
        print(f"Table: {table} (all rows)")
        print("-" * 60)
        if not cols:
            print("(no columns)")
        else:
            print("Columns:", ", ".join(cols))
            for i, row in enumerate(rows, 1):
                print(f"  Row {i}:", row)
        print("-" * 60)
        print(f"Total: {len(rows)} row(s)")
    except Exception as e:
        print(f"Error reading table: {e}", file=sys.stderr)
        sys.exit(1)
