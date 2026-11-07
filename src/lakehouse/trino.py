"""A small Trino client: rows plus the engine's own statistics for each query."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import trino

from lakehouse.settings import Settings


@dataclass
class QueryResult:
    rows: list[list[Any]]
    columns: list[str]
    elapsed_ms: int
    stats: dict[str, Any]

    @property
    def physical_input_bytes(self) -> int:
        return int(self.stats.get("physicalInputBytes", 0))

    @property
    def splits(self) -> int:
        return int(self.stats.get("totalSplits", 0))


class Trino:
    def __init__(self, settings: Settings, schema: str | None = None) -> None:
        self.settings = settings
        self.conn = trino.dbapi.connect(  # type: ignore[no-untyped-call]
            host=settings.trino_host,
            port=settings.trino_port,
            user=settings.trino_user,
            catalog=settings.trino_catalog,
            schema=schema,
            http_scheme="http",
        )

    def query(self, sql: str, params: list[Any] | None = None) -> QueryResult:
        cur = self.conn.cursor()
        started = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        elapsed = int((time.perf_counter() - started) * 1000)
        columns = [d[0] for d in cur.description or []]
        return QueryResult(rows, columns, elapsed, dict(cur.stats or {}))

    def scalar(self, sql: str, params: list[Any] | None = None) -> Any:
        rows = self.query(sql, params).rows
        return rows[0][0] if rows else None

    def close(self) -> None:
        self.conn.close()
