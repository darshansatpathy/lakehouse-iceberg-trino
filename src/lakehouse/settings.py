"""Connection settings, from the environment. Defaults match docker-compose.yml."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    trino_host: str = "localhost"
    trino_port: int = 8080
    trino_user: str = "lakehouse"
    trino_catalog: str = "lakehouse"
    catalog_uri: str = "postgresql+psycopg://iceberg:iceberg@localhost:5432/iceberg_catalog"
    catalog_name: str = "lakehouse"
    warehouse: str = "s3://lakehouse/warehouse"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "lakehouse"
    s3_secret_key: str = "lakehouse-secret"  # noqa: S105 - the local stack's default, overridden by LH_S3_SECRET_KEY
    s3_region: str = "us-east-1"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        base = cls()
        return cls(
            trino_host=env.get("LH_TRINO_HOST", base.trino_host),
            trino_port=int(env.get("LH_TRINO_PORT", base.trino_port)),
            trino_user=env.get("LH_TRINO_USER", base.trino_user),
            trino_catalog=env.get("LH_TRINO_CATALOG", base.trino_catalog),
            catalog_uri=env.get("LH_CATALOG_URI", base.catalog_uri),
            catalog_name=env.get("LH_CATALOG_NAME", base.catalog_name),
            warehouse=env.get("LH_WAREHOUSE", base.warehouse),
            s3_endpoint=env.get("LH_S3_ENDPOINT", base.s3_endpoint),
            s3_access_key=env.get("LH_S3_ACCESS_KEY", base.s3_access_key),
            s3_secret_key=env.get("LH_S3_SECRET_KEY", base.s3_secret_key),
            s3_region=env.get("LH_S3_REGION", base.s3_region),
        )

    def dbt_env(self) -> dict[str, str]:
        return {
            "LH_TRINO_HOST": self.trino_host,
            "LH_TRINO_PORT": str(self.trino_port),
            "LH_TRINO_USER": self.trino_user,
            "LH_TRINO_CATALOG": self.trino_catalog,
        }
