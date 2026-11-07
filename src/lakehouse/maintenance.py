"""Table health and the Iceberg maintenance procedures, run through Trino."""

from __future__ import annotations

from dataclasses import dataclass

from lakehouse.trino import Trino


@dataclass
class Health:
    table: str
    data_files: int
    snapshots: int
    rows: int
    bytes: int
    delete_files: int = 0

    @property
    def avg_file_kb(self) -> float:
        return self.bytes / self.data_files / 1024 if self.data_files else 0.0


def _split(table: str) -> tuple[str, str]:
    schema, _, name = table.partition(".")
    return schema, name


def health(trino: Trino, table: str) -> Health:
    schema, name = _split(table)
    # content 0 is data; 1 and 2 are position and equality delete files written by MERGE.
    files = trino.query(
        "SELECT count_if(content = 0), count_if(content <> 0), "
        "coalesce(sum(record_count) FILTER (WHERE content = 0), 0), coalesce(sum(file_size_in_bytes) FILTER (WHERE content = 0), 0) "
        f'FROM {schema}."{name}$files"'
    ).rows[0]
    snapshots = trino.scalar(f'SELECT count(*) FROM {schema}."{name}$snapshots"')
    return Health(table, int(files[0]), int(snapshots), int(files[2]), int(files[3]), int(files[1]))


def optimize(trino: Trino, table: str, file_size_threshold: str = "128MB") -> None:
    trino.query(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')")


def expire_snapshots(trino: Trino, table: str, retention: str = "7d") -> None:
    trino.query(f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')")


def remove_orphan_files(trino: Trino, table: str, retention: str = "7d") -> None:
    trino.query(f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')")


def maintain(trino: Trino, tables: list[str], retention: str = "7d") -> list[tuple[Health, Health]]:
    """Compact, expire and clean each table; return its health before and after."""
    out = []
    for table in tables:
        before = health(trino, table)
        optimize(trino, table)
        expire_snapshots(trino, table, retention)
        remove_orphan_files(trino, table, retention)
        out.append((before, health(trino, table)))
    return out
