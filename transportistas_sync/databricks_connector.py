"""
Databricks SQL connector with dual-environment support.

Environments (selected via DATABRICKS_ENV or constructor):
  - qas: Personal Access Token (PAT) auth
  - prd: OAuth M2M (Service Principal) auth via databricks-sdk

QAS env vars: DATABRICKS_QAS_HOST, DATABRICKS_QAS_HTTP_PATH, DATABRICKS_QAS_TOKEN
PRD env vars: DATABRICKS_PRD_HOST, DATABRICKS_PRD_HTTP_PATH,
              DATABRICKS_PRD_CLIENT_ID, DATABRICKS_PRD_CLIENT_SECRET

Legacy env vars (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.) still work as
fallback for QAS when the prefixed versions are not set.
"""
import os
from typing import Any

from databricks import sql

VALID_ENVS = ("qas", "prd")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _resolve_env() -> str:
    env = _env("DATABRICKS_ENV", "qas").lower()
    if env not in VALID_ENVS:
        raise ValueError(f"DATABRICKS_ENV must be one of {VALID_ENVS}, got '{env}'")
    return env


class DatabricksConnector:
    """Connect to Databricks SQL (QAS via PAT, PRD via OAuth M2M)."""

    def __init__(
        self,
        env: str | None = None,
        *,
        server_hostname: str | None = None,
        http_path: str | None = None,
        access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ):
        self.env = (env or _resolve_env()).lower()
        if self.env not in VALID_ENVS:
            raise ValueError(f"env must be one of {VALID_ENVS}, got '{self.env}'")

        if self.env == "qas":
            self.server_hostname = (
                server_hostname
                or _env("DATABRICKS_QAS_HOST")
                or _env("DATABRICKS_HOST")
                or _env("DATABRICKS_SERVER_HOSTNAME")
            )
            self.http_path = (
                http_path
                or _env("DATABRICKS_QAS_HTTP_PATH")
                or _env("DATABRICKS_HTTP_PATH")
            )
            self.access_token = (
                access_token
                or _env("DATABRICKS_QAS_TOKEN")
                or _env("DATABRICKS_TOKEN")
            )
            self._auth_mode = "pat"
        else:
            self.server_hostname = (
                server_hostname
                or _env("DATABRICKS_PRD_HOST")
            )
            self.http_path = (
                http_path
                or _env("DATABRICKS_PRD_HTTP_PATH")
            )
            self._client_id = (
                client_id
                or _env("DATABRICKS_PRD_CLIENT_ID")
            )
            self._client_secret = (
                client_secret
                or _env("DATABRICKS_PRD_CLIENT_SECRET")
            )
            self.access_token = None
            self._auth_mode = "oauth_m2m"

    def _connection(self):
        if not self.server_hostname or not self.http_path:
            raise ValueError(
                f"Missing DATABRICKS_{self.env.upper()}_HOST or "
                f"DATABRICKS_{self.env.upper()}_HTTP_PATH."
            )

        if self._auth_mode == "pat":
            if not self.access_token:
                raise ValueError(
                    "Missing Databricks PAT. Set DATABRICKS_QAS_TOKEN "
                    "(or DATABRICKS_TOKEN)."
                )
            return sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
            )

        # OAuth M2M (PRD)
        if not self._client_id or not self._client_secret:
            raise ValueError(
                "Missing OAuth credentials. Set DATABRICKS_PRD_CLIENT_ID "
                "and DATABRICKS_PRD_CLIENT_SECRET."
            )
        from databricks.sdk.core import Config, oauth_service_principal

        host = self.server_hostname
        cid = self._client_id
        csecret = self._client_secret

        def _credential_provider():
            config = Config(
                host=f"https://{host}",
                client_id=cid,
                client_secret=csecret,
            )
            return oauth_service_principal(config)

        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            credentials_provider=_credential_provider,
        )

    def execute_query(
        self, query: str, parameters: list[Any] | None = None
    ) -> tuple[list[str], list[tuple]]:
        """Run a SQL query and return (column_names, rows)."""
        with self._connection() as conn:
            with conn.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                cols = [d[0] for d in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                row_tuples = [
                    tuple(r) if not isinstance(r, tuple) else r for r in rows
                ]
                return cols, row_tuples

    def __repr__(self) -> str:
        return (
            f"DatabricksConnector(env='{self.env}', "
            f"host='{self.server_hostname}', auth='{self._auth_mode}')"
        )
