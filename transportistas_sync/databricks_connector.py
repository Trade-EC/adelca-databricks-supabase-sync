"""
Databricks SQL connector using personal access token auth.
Expects env: DATABRICKS_HOST (or DATABRICKS_SERVER_HOSTNAME), DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
"""
import os
from typing import Any

from databricks import sql


def _get_host() -> str:
    return (
        os.environ.get("DATABRICKS_HOST")
        or os.environ.get("DATABRICKS_SERVER_HOSTNAME")
        or ""
    ).strip()


class DatabricksConnector:
    """Connect to Databricks SQL using PAT and run queries."""

    def __init__(
        self,
        server_hostname: str | None = None,
        http_path: str | None = None,
        access_token: str | None = None,
    ):
        self.server_hostname = (
            server_hostname or _get_host()
        ).strip()
        self.http_path = (
            http_path or os.environ.get("DATABRICKS_HTTP_PATH", "")
        ).strip()
        self.access_token = (
            access_token or os.environ.get("DATABRICKS_TOKEN", "")
        ).strip()

    def _connection(self):
        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError(
                "Missing Databricks config. Set DATABRICKS_HOST, "
                "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN (or pass to constructor)."
            )
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        )

    def execute_query(
        self, query: str, parameters: list[Any] | None = None
    ) -> tuple[list[str], list[tuple]]:
        """
        Run a SQL query and return (column_names, rows).
        Rows are tuples in order of the selected columns.
        """
        with self._connection() as conn:
            with conn.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                cols = [d[0] for d in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                # Normalize to tuples (connector may return Row-like objects)
                row_tuples = [
                    tuple(r) if not isinstance(r, tuple) else r for r in rows
                ]
                return cols, row_tuples
