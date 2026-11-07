"""End-to-end verification: every table is compared with the state the CDC events imply.

The run starts from empty storage and, for each batch: lands the events with pyiceberg, runs
dbt through Trino, and compares silver and gold with `Reference`. It then replays a batch,
reads old snapshots, checks the schema and partition changes, and runs maintenance.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from pyiceberg.catalog import Catalog
from trino.exceptions import TrinoQueryError

from lakehouse import dbt
from lakehouse.generator import Batch, Generator, Reference
from lakehouse.ingest import CUSTOMERS, EVENTS, catalog, drop_all, ingest
from lakehouse.maintenance import Health, health, maintain
from lakehouse.settings import Settings
from lakehouse.trino import Trino

ORDER_COLUMNS = ["order_id", "customer_id", "order_ts", "status", "amount", "channel", "coupon_code", "is_deleted", "last_lsn"]
DBT_SCHEMAS = ["gold", "silver", "staging"]


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Verification:
    checks: list[Check] = field(default_factory=list)
    maintenance: list[tuple[Health, Health]] = field(default_factory=list)
    batches: list[tuple[int, int, int, int]] = field(default_factory=list)  # batch, delivered, distinct, orders

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append(Check(name, passed, detail))

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed]

    def markdown(self) -> str:
        ok = sum(c.passed for c in self.checks)
        lines = ["# Verification", "", f"**{ok} / {len(self.checks)} checks passed.**", ""]
        lines += ["## Batches", "", "| Batch | Events delivered | Distinct events | Orders in silver |", "|---|---|---|---|"]
        lines += [f"| {b} | {d:,} | {u:,} | {o:,} |" for b, d, u, o in self.batches]
        lines += ["", "## Checks", "", "| Check | Result | Detail |", "|---|---|---|"]
        lines += [f"| {c.name} | {'pass' if c.passed else '**FAIL**'} | {c.detail} |" for c in self.checks]
        if self.maintenance:
            lines += [
                "",
                "## Maintenance",
                "",
                "| Table | Data files | Delete files | Snapshots | Rows in data files | Average data file |",
                "|---|---|---|---|---|---|",
            ]
            for before, after in self.maintenance:
                lines.append(
                    f"| {before.table} | {before.data_files} → {after.data_files} | {before.delete_files} → {after.delete_files} | "
                    f"{before.snapshots} → {after.snapshots} | {before.rows:,} → {after.rows:,} | "
                    f"{before.avg_file_kb:.0f} KB → {after.avg_file_kb:.0f} KB |"
                )
        return "\n".join(lines) + "\n"


def reset(trino: Trino, cat: Catalog) -> None:
    for schema in DBT_SCHEMAS:
        for (table,) in trino.query(f"SHOW TABLES FROM {schema}").rows if _schema_exists(trino, schema) else []:
            kind = trino.scalar("SELECT table_type FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", [schema, table])
            trino.query(f'DROP {"VIEW" if kind == "VIEW" else "TABLE"} {schema}."{table}"')
        if _schema_exists(trino, schema):
            trino.query(f"DROP SCHEMA {schema}")
    drop_all(cat)


def _schema_exists(trino: Trino, schema: str) -> bool:
    return bool(trino.scalar("SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?", [schema]))


def silver_rows(trino: Trino, version: int | None = None) -> set[tuple[Any, ...]]:
    sql = "SELECT * FROM silver.orders" + (f" FOR VERSION AS OF {int(version)}" if version is not None else "")
    result = trino.query(sql)
    idx = {c: i for i, c in enumerate(result.columns)}
    return {tuple(row[idx[c]] if c in idx else None for c in ORDER_COLUMNS) for row in result.rows}


def expected_rows(ref: Reference) -> set[tuple[Any, ...]]:
    return {
        (o.order_id, o.customer_id, o.order_ts, o.status, o.amount, o.channel, o.coupon_code, o.is_deleted, o.last_lsn) for o in ref.orders.values()
    }


def gold_matches(trino: Trino, ref: Reference) -> tuple[bool, str]:
    daily = {
        (r[0], r[1], r[2]): (int(r[3]), Decimal(r[4]), int(r[5]))
        for r in trino.query("SELECT order_date, channel, country, orders, revenue, cancelled_orders FROM gold.daily_sales").rows
    }
    value = {int(r[0]): (int(r[1]), Decimal(r[2])) for r in trino.query("SELECT customer_id, orders, revenue FROM gold.customer_value").rows}
    expected_daily, expected_value = ref.daily_sales(), ref.customer_value()
    ok = daily == expected_daily and value == expected_value
    return ok, f"{len(daily):,} daily rows, {len(value):,} customers" if ok else _diff(daily, expected_daily) + "; " + _diff(value, expected_value)


def _diff(got: dict[Any, Any] | set[Any], expected: dict[Any, Any] | set[Any]) -> str:
    g, e = (set(got.items()), set(expected.items())) if isinstance(got, dict) and isinstance(expected, dict) else (set(got), set(expected))
    return f"{len(e - g)} missing or different, {len(g - e)} unexpected"


def latest_snapshot(trino: Trino, table: str) -> int:
    schema, _, name = table.partition(".")
    return int(trino.scalar(f'SELECT snapshot_id FROM {schema}."{name}$snapshots" ORDER BY committed_at DESC LIMIT 1'))


def run(
    settings: Settings,
    batches: int = 5,
    appends: int = 8,
    generator: Generator | None = None,
    daily_partitions_from: int = 4,
    progress: Callable[[str], None] = lambda _: None,
) -> Verification:
    v = Verification()
    trino = Trino(settings)
    cat = catalog(settings)
    reset(trino, cat)
    gen = generator or Generator()
    ref = Reference()
    delivered: dict[int, Batch] = {}
    snapshots: dict[int, int] = {}
    states: dict[int, set[tuple[Any, ...]]] = {}

    for n in range(1, batches + 1):
        batch = gen.batch(n)
        delivered[n] = batch
        result = ingest(cat, batch, appends, daily_partitions=n >= daily_partitions_from)
        ref.apply(batch)
        progress(f"batch {n}: {result.events} events landed in {result.appends} appends; running dbt")
        dbt.build(settings)
        got, expected = silver_rows(trino), expected_rows(ref)
        v.check(
            f"batch {n}: silver.orders equals the reference",
            got == expected,
            f"{len(got):,} orders, {sum(1 for r in got if r[7]):,} deleted" if got == expected else _diff(got, expected),
        )
        ok, detail = gold_matches(trino, ref)
        v.check(f"batch {n}: gold equals the reference", ok, detail)
        snapshots[n], states[n] = latest_snapshot(trino, "silver.orders"), expected
        v.batches.append((n, len(batch.events), batch.distinct_events, len(expected)))

    # Replays: a batch of events delivered again changes nothing.
    progress("replaying batch 2")
    ingest(cat, delivered[2], appends, customers_snapshot=False)
    dbt.build(settings)
    v.check("replayed batch 2: silver unchanged", silver_rows(trino) == expected_rows(ref), "events at or below the applied LSN are ignored")
    v.check("replayed batch 2: gold unchanged", gold_matches(trino, ref)[0])

    # Time travel: each batch's snapshot still reads as the state after that batch.
    for n, snap in snapshots.items():
        same = silver_rows(trino, snap) == states[n]
        v.check(f"time travel to the snapshot after batch {n}", same, f"FOR VERSION AS OF {snap}")

    # Schema evolution: coupon_code was added at the source in batch 3.
    silver_cols = [r[0] for r in trino.query("SHOW COLUMNS FROM silver.orders").rows]
    coupons_by_batch = trino.query("SELECT last_batch_id, count(coupon_code) FROM silver.orders GROUP BY 1 ORDER BY 1").rows
    before = sum(c for b, c in coupons_by_batch if b < gen.coupons_from)
    after = sum(c for b, c in coupons_by_batch if b >= gen.coupons_from)
    v.check(
        "schema evolution: coupon_code reached silver without a model change",
        "coupon_code" in silver_cols and before == 0 and after > 0,
        f"{after:,} orders with a coupon from batch {gen.coupons_from} on, none before",
    )

    # Partition evolution: monthly files and daily files in one table, both pruned.
    specs = trino.query('SELECT spec_id, count(*) FROM raw."order_events$files" GROUP BY 1 ORDER BY 1').rows
    v.check(
        "partition evolution: raw.order_events holds monthly and daily files",
        [s for s, _ in specs] == [0, 1],
        ", ".join(f"spec {s}: {c} files" for s, c in specs),
    )
    day = dt.datetime(2025, 1, 1) + dt.timedelta(days=30 * (daily_partitions_from - 1) + 3)
    one_day = trino.query("SELECT count(*) FROM raw.order_events WHERE commit_ts >= ? AND commit_ts < ?", [day, day + dt.timedelta(days=1)])
    everything = trino.query("SELECT count(*) FROM raw.order_events WHERE op IS NOT NULL")
    expected_day = sum(1 for b in [*delivered.values(), delivered[2]] for e in b.events if day <= e.commit_ts < day + dt.timedelta(days=1))
    v.check(
        "partition pruning on the daily spec",
        one_day.rows[0][0] == expected_day and one_day.splits < everything.splits,
        f"one day: {one_day.splits} splits, {one_day.physical_input_bytes:,} bytes; whole table: {everything.splits} splits, "
        f"{everything.physical_input_bytes:,} bytes",
    )

    # Maintenance: compaction keeps results identical; expiry removes old snapshots.
    probes = {
        EVENTS: "SELECT count(*), sum(lsn), sum(amount) FROM raw.order_events",
        "silver.orders": "SELECT count(*), sum(last_lsn), sum(amount), count_if(is_deleted) FROM silver.orders",
    }
    sums = {t: trino.query(q).rows for t, q in probes.items()}
    progress("maintenance: optimize, expire_snapshots, remove_orphan_files")
    v.maintenance = maintain(trino, list(probes), retention="0s")
    for (before, after), (table, query) in zip(v.maintenance, probes.items(), strict=True):
        v.check(
            f"maintenance on {table}: fewer files, same results",
            after.data_files + after.delete_files < before.data_files + before.delete_files and trino.query(query).rows == sums[table],
            f"{before.data_files} data and {before.delete_files} delete files → {after.data_files} and {after.delete_files}",
        )
        v.check(f"maintenance on {table}: snapshots expired", after.snapshots == 1, f"{before.snapshots} → {after.snapshots}")
    try:
        trino.query(f"SELECT count(*) FROM silver.orders FOR VERSION AS OF {snapshots[1]}")
        expired = False
    except TrinoQueryError as exc:
        expired = "snapshot" in str(exc).lower()
    v.check("an expired snapshot can no longer be read", expired)
    v.check("silver still equals the reference after maintenance", silver_rows(trino) == expected_rows(ref))
    v.check("raw customers table readable by Trino after pyiceberg overwrites", health(trino, CUSTOMERS).rows == len(ref.customers))
    trino.close()
    return v
