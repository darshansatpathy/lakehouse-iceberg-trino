"""A measured benchmark: partition pruning, and what compaction does to small files.

Events are written the way a streaming loader writes them: in time order, in many small
appends, so each daily partition ends up with several small files. Each query is warmed up
twice, then run five times; the median is reported with Trino's own statistics for splits and
bytes read.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import platform
import random
import statistics
from dataclasses import dataclass, field
from decimal import Decimal

import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DecimalType, LongType, NestedField, StringType, TimestampType

from lakehouse.ingest import catalog
from lakehouse.maintenance import Health, health, optimize
from lakehouse.settings import Settings
from lakehouse.trino import QueryResult, Trino

TABLE = "bench.events"
DAYS = 120
START = dt.datetime(2025, 1, 1)
SCHEMA = Schema(
    NestedField(1, "event_id", LongType(), required=True),
    NestedField(2, "commit_ts", TimestampType(), required=True),
    NestedField(3, "order_ts", TimestampType(), required=True),
    NestedField(4, "customer_id", LongType(), required=True),
    NestedField(5, "status", StringType(), required=True),
    NestedField(6, "channel", StringType(), required=True),
    NestedField(7, "amount", DecimalType(12, 2), required=True),
)
SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="commit_ts_day"))


@dataclass
class Measurement:
    name: str
    stage: str
    median_ms: int
    splits: int
    bytes_read: int
    rows: int


@dataclass
class BenchReport:
    events: int
    appends: int
    before: Health | None = None
    after: Health | None = None
    measurements: list[Measurement] = field(default_factory=list)

    def markdown(self) -> str:
        cpu = os.cpu_count()
        lines = [
            "# Benchmark",
            "",
            f"{self.events:,} events over {DAYS} days, written in {self.appends} time-ordered appends to a table partitioned by day.",
            f"Single Trino node, {cpu} CPUs, {platform.system()} {platform.machine()}. "
            "Each query is warmed up twice, then the median of five runs is reported.",
            "",
        ]
        if self.before and self.after:
            lines += [
                f"Compaction (`optimize`): {self.before.data_files} files averaging {self.before.avg_file_kb:.0f} KB "
                f"→ {self.after.data_files} files averaging {self.after.avg_file_kb:.0f} KB.",
                "",
            ]
        lines += ["| Query | Layout | Splits | Bytes read | Rows scanned | Median |", "|---|---|---|---|---|---|"]
        for m in self.measurements:
            lines.append(f"| {m.name} | {m.stage} | {m.splits:,} | {m.bytes_read:,} | {m.rows:,} | {m.median_ms:,} ms |")
        return "\n".join(lines) + "\n"


def generate(events: int, seed: int = 11) -> pa.Table:
    rng = random.Random(seed)
    span = DAYS * 86400
    commits = sorted(rng.randrange(span) for _ in range(events))
    channels = ["web", "app", "store", "marketplace"]
    statuses = ["pending", "paid", "shipped", "delivered", "cancelled"]
    data = {
        "event_id": list(range(1, events + 1)),
        "commit_ts": [START + dt.timedelta(seconds=s) for s in commits],
        "order_ts": [START + dt.timedelta(seconds=rng.randrange(span)) for _ in range(events)],
        "customer_id": [rng.randint(1, 50_000) for _ in range(events)],
        "status": [rng.choice(statuses) for _ in range(events)],
        "channel": [rng.choice(channels) for _ in range(events)],
        "amount": [Decimal(rng.randint(500, 250_000)) / 100 for _ in range(events)],
    }
    return pa.Table.from_pydict(data, schema=SCHEMA.as_arrow())


def _measure(trino: Trino, name: str, stage: str, sql: str, runs: int = 5) -> Measurement:
    results: list[QueryResult] = [trino.query(sql) for _ in range(runs)]
    last = results[-1]
    return Measurement(
        name,
        stage,
        int(statistics.median(r.elapsed_ms for r in results)),
        last.splits,
        last.physical_input_bytes,
        int(last.stats.get("processedRows", 0)),
    )


def run(settings: Settings, events: int = 1_000_000, appends: int = 200) -> BenchReport:
    cat = catalog(settings)
    with contextlib.suppress(NamespaceAlreadyExistsError):
        cat.create_namespace("bench")
    with contextlib.suppress(NoSuchTableError):
        cat.purge_table(TABLE)
    table = cat.create_table(TABLE, SCHEMA, partition_spec=SPEC, properties={"format-version": "2"})
    data = generate(events)
    size = -(-events // appends)
    for offset in range(0, events, size):
        table.append(data.slice(offset, size))

    trino = Trino(settings)
    day = START + dt.timedelta(days=45)
    lo, hi = day.strftime("%Y-%m-%d %H:%M:%S"), (day + dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    def within(column: str) -> str:
        return f"{column} >= TIMESTAMP '{lo}' AND {column} < TIMESTAMP '{hi}'"

    queries = {
        "one day, filtered on the partition column": f"SELECT count(*), sum(amount) FROM {TABLE} WHERE {within('commit_ts')}",
        "one day, filtered on an unpartitioned column": f"SELECT count(*), sum(amount) FROM {TABLE} WHERE {within('order_ts')}",
        "whole table, grouped by channel": f"SELECT channel, count(*), sum(amount) FROM {TABLE} GROUP BY channel",
    }
    report = BenchReport(events, appends)
    report.before = health(trino, TABLE)
    for stage in ("before compaction", "after compaction"):
        if stage == "after compaction":
            optimize(trino, TABLE)
            report.after = health(trino, TABLE)
        for _ in range(2):  # warm-up: JIT, metadata caches, connections
            for sql in queries.values():
                trino.query(sql)
        for name, sql in queries.items():
            report.measurements.append(_measure(trino, name, stage, sql))
    trino.close()
    return report
