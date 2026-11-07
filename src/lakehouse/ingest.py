"""Land CDC batches in Iceberg with pyiceberg, through the same JDBC catalog Trino uses.

The raw tables are append-only. Events are written in several small appends per batch, as a
streaming or micro-batch loader would, which is what makes compaction worth showing.
"""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, MonthTransform
from pyiceberg.types import DateType, DecimalType, IntegerType, LongType, NestedField, StringType, TimestampType

from lakehouse.generator import Batch, Customer, Event
from lakehouse.settings import Settings

RAW = "raw"
EVENTS = f"{RAW}.order_events"
CUSTOMERS = f"{RAW}.customers"

EVENT_SCHEMA = Schema(
    NestedField(1, "lsn", LongType(), required=True),
    NestedField(2, "op", StringType(), required=True),
    NestedField(3, "commit_ts", TimestampType(), required=True),
    NestedField(4, "order_id", LongType(), required=True),
    NestedField(5, "customer_id", LongType(), required=True),
    NestedField(6, "order_ts", TimestampType(), required=True),
    NestedField(7, "status", StringType(), required=True),
    NestedField(8, "amount", DecimalType(12, 2), required=True),
    NestedField(9, "channel", StringType(), required=True),
    NestedField(10, "batch_id", IntegerType(), required=True),
)
MONTHLY = PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=MonthTransform(), name="commit_ts_month"))

CUSTOMER_SCHEMA = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "country", StringType(), required=True),
    NestedField(3, "segment", StringType(), required=True),
    NestedField(4, "signup_date", DateType(), required=True),
)


def catalog(settings: Settings) -> SqlCatalog:
    return SqlCatalog(
        settings.catalog_name,
        uri=settings.catalog_uri,
        warehouse=settings.warehouse,
        **{
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key,
            "s3.secret-access-key": settings.s3_secret_key,
            "s3.region": settings.s3_region,
            "s3.path-style-access": "true",
        },
    )


@dataclass
class IngestResult:
    batch: int
    events: int
    appends: int
    schema_changed: bool
    spec_changed: bool


def ensure_tables(cat: Catalog) -> tuple[Table, Table]:
    with contextlib.suppress(NamespaceAlreadyExistsError):
        cat.create_namespace(RAW)
    try:
        events = cat.load_table(EVENTS)
    except NoSuchTableError:
        events = cat.create_table(EVENTS, EVENT_SCHEMA, partition_spec=MONTHLY, properties={"format-version": "2"})
    try:
        customers = cat.load_table(CUSTOMERS)
    except NoSuchTableError:
        customers = cat.create_table(CUSTOMERS, CUSTOMER_SCHEMA, properties={"format-version": "2"})
    return events, customers


def evolve(events: Table, with_coupons: bool, daily_partitions: bool) -> tuple[bool, bool]:
    """Schema evolution (a new source column) and partition evolution (monthly to daily)."""
    schema_changed = spec_changed = False
    if with_coupons and "coupon_code" not in events.schema().column_names:
        with events.update_schema() as update:
            update.add_column("coupon_code", StringType(), doc="Added by the source system in a later release")
        schema_changed = True
    if daily_partitions and all(f.name != "commit_ts_day" for f in events.spec().fields):
        with events.update_spec() as update:
            update.add_field("commit_ts", DayTransform(), "commit_ts_day")
            update.remove_field("commit_ts_month")
        spec_changed = True
    return schema_changed, spec_changed


def events_table(events: Sequence[Event], batch: int, schema: Schema) -> pa.Table:
    arrow_schema = schema.as_arrow()
    columns: dict[str, list[object]] = {name: [] for name in arrow_schema.names}
    for e in events:
        row = {
            "lsn": e.lsn,
            "op": e.op,
            "commit_ts": e.commit_ts,
            "order_id": e.order_id,
            "customer_id": e.customer_id,
            "order_ts": e.order_ts,
            "status": e.status,
            "amount": e.amount,
            "channel": e.channel,
            "batch_id": batch,
            "coupon_code": e.coupon_code,
        }
        for name in arrow_schema.names:
            columns[name].append(row[name])
    return pa.Table.from_pydict(columns, schema=arrow_schema)


def customers_table(customers: Sequence[Customer], schema: Schema) -> pa.Table:
    return pa.Table.from_pylist(
        [{"customer_id": c.customer_id, "country": c.country, "segment": c.segment, "signup_date": c.signup_date} for c in customers],
        schema=schema.as_arrow(),
    )


def ingest(cat: Catalog, batch: Batch, appends: int = 8, daily_partitions: bool = False, customers_snapshot: bool = True) -> IngestResult:
    events, customers = ensure_tables(cat)
    schema_changed, spec_changed = evolve(events, batch.with_coupons, daily_partitions)
    events = cat.load_table(EVENTS)
    size = max(1, -(-len(batch.events) // appends))
    chunks = [batch.events[i : i + size] for i in range(0, len(batch.events), size)]
    for chunk in chunks:
        events.append(events_table(chunk, batch.number, events.schema()))
    if customers_snapshot:
        with warnings.catch_warnings():  # overwriting an empty table warns that nothing was deleted
            warnings.simplefilter("ignore", UserWarning)
            customers.overwrite(customers_table(batch.customers, customers.schema()))
    return IngestResult(batch.number, len(batch.events), len(chunks), schema_changed, spec_changed)


def drop_all(cat: Catalog, namespaces: Sequence[str] = (RAW,)) -> None:
    """Remove the raw tables (used to start a verification run from nothing)."""
    for ns in namespaces:
        try:
            tables = cat.list_tables(ns)
        except NoSuchNamespaceError:
            continue
        for ident in tables:
            cat.purge_table(ident)
        cat.drop_namespace(ns)
