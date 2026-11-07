# Lakehouse on Iceberg and Trino

[![CI](https://github.com/darshansatpathy/lakehouse-iceberg-trino/actions/workflows/ci.yml/badge.svg)](https://github.com/darshansatpathy/lakehouse-iceberg-trino/actions/workflows/ci.yml)
[![CodeQL](https://github.com/darshansatpathy/lakehouse-iceberg-trino/actions/workflows/codeql.yml/badge.svg)](https://github.com/darshansatpathy/lakehouse-iceberg-trino/actions/workflows/codeql.yml)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
![Trino](https://img.shields.io/badge/Trino-476-dd00a1)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v2-blue)
![Verification](https://img.shields.io/badge/verification-27%2F27-success)
![License](https://img.shields.io/badge/license-MIT-green)

**An open lakehouse you can run on a laptop and check end to end: change-data-capture events
landed in Apache Iceberg on S3-compatible storage, one catalog shared by two engines, dbt models
on Trino that merge the changes, and every table compared with an answer computed independently.**

```
 orders database (CDC)                                       PostgreSQL: Iceberg JDBC catalog
        │  create / update / delete events,                    ▲                     ▲
        │  unordered, some delivered twice                     │ commits             │ commits
        ▼                                                      │                     │
   pyiceberg ──append──▶ raw.order_events (Iceberg) ◀── reads ─┴── Trino 476 ── dbt ─┘
                         raw.customers                                │
                              │ MinIO (S3 API)                        ▼
                              │                     staging (views) ─▶ silver.orders (MERGE, tombstones)
                              │                                       silver.dim_customers
                              │                                     ─▶ gold.daily_sales, gold.customer_value
                              ▼                                                       │
              optimize · expire_snapshots · remove_orphan_files              Superset (optional)
```

Two engines write to the same tables through the same catalog: Python (pyiceberg) lands raw
data and Trino builds everything downstream. Neither owns the data; the table format does.

## What is verified

`lh verify` starts from empty storage and lands five batches of CDC events. After each batch it
runs `dbt build` through Trino and compares `silver.orders` and both gold tables, row for row,
with the state computed by applying the same events in plain Python
([`Reference`](src/lakehouse/generator.py)). Then it exercises the parts of Iceberg that matter in
operation. From [the latest run](docs/verification.md), 27 of 27 checks pass:

| Behaviour | How it is checked | Result |
|---|---|---|
| CDC merge | silver and gold equal the reference after every batch: 10,000 orders, 100 deleted, 4,175 daily rows, 560 customers | 10 / 10 |
| At-least-once delivery | 2% of events are delivered twice and batch 2 is replayed whole; nothing changes | 2 / 2 |
| Time travel | `FOR VERSION AS OF` each batch's snapshot reads exactly that batch's expected state | 5 / 5 |
| Schema evolution | `coupon_code` is added at the source in batch 3 and reaches silver with no model change; earlier rows stay null | pass |
| Partition evolution | `raw.order_events` moves from monthly to daily partitions in batch 4; both layouts coexist and are queried together | pass |
| Partition pruning | one day on the daily layout reads 11 splits and 38,772 bytes, against 763 splits and 3,934,440 bytes for the table | pass |
| Compaction | `optimize`: raw 760 → 150 data files; silver 16 data and 23 delete files → 5 and 0; results identical | 2 / 2 |
| Snapshot expiry | `expire_snapshots` leaves one snapshot per table, and an expired snapshot can no longer be read | 3 / 3 |
| Interoperability | tables written by pyiceberg, including overwrites, are read by Trino; silver still matches after maintenance | 2 / 2 |

dbt adds its own tests on every build: uniqueness, accepted statuses, relationships, and a
check that every order in silver carries the highest LSN seen for it. That last test also
catches events that arrive below the watermark, so late data fails the build instead of
disappearing.

## How the CDC merge works

[`silver.orders`](transform/models/silver/orders.sql) is an incremental dbt model with the merge
strategy:

- Events are deduplicated by LSN in staging, so a re-delivered event is the same event.
- Only events above the highest LSN already applied are read, so replays and re-runs are no-ops.
- Within those, the last event per order wins, whatever order the files arrived in.
- A delete becomes a tombstone (`is_deleted`), kept for audit; gold excludes it.
- Columns are taken from the source relation, and `on_schema_change: append_new_columns` adds a
  new source column to the table, so schema evolution needs no SQL change.

## Benchmark

`lh bench` writes 1,000,000 events over 120 days the way a streaming loader does, in 200 small
time-ordered appends to a daily-partitioned table, then measures with Trino's own statistics
([full report](docs/benchmark.md); single node, 2 CPUs, MinIO on the same machine; medians of
five runs after two warm-ups):

| Query | Before compaction | After compaction |
|---|---|---|
| One day, on the partition column | 6 splits, 217 KB, 42 ms | 4 splits, 139 KB, 35 ms |
| One day, on an unpartitioned column | 321 splits, 25.7 MB, 1,058 ms | 123 splits, 16.6 MB, 138 ms |
| Whole table, grouped | 323 splits, 25.7 MB, 1,148 ms | 124 splits, 16.6 MB, 133 ms |

Partition pruning cuts the bytes read for a one-day query by about 120 times. Compaction took the
table from 319 files averaging 79 KB to 120 averaging 135 KB, and the whole-table query from about
1.1 s to 0.13 s. Part of that is file count and part is the rewrite itself: `optimize` rewrote
files written by pyiceberg with Trino's writer, which is why bytes read also fell by a third.
These are single-machine numbers for comparing layouts, not throughput claims.

## Run it

```bash
docker compose up -d && make wait           # PostgreSQL catalog, MinIO, Trino 476
pip install -e ".[dev]"

lh verify --report verification.md          # the end-to-end run above, about a minute
lh bench --report benchmark.md
lh health raw.order_events silver.orders    # files, delete files, snapshots
lh maintain silver.orders --retention 7d    # optimize, expire_snapshots, remove_orphan_files
```

Query the tables from the Trino CLI or any client at `localhost:8080`, catalog `lakehouse`:

```sql
SELECT * FROM silver.orders FOR TIMESTAMP AS OF TIMESTAMP '2026-11-08 10:00:00 UTC';
SELECT spec_id, count(*) FROM raw."order_events$files" GROUP BY 1;
SELECT * FROM silver."orders$snapshots";
```

`docker compose --profile bi up -d` adds Superset on `localhost:8088` (admin / admin), with the
lakehouse registered as a database. It is a convenience for exploring the gold tables and is
not exercised in CI.

MinIO stopped publishing community container images in 2025. The stack uses
`pgsty/minio`, a community-maintained build of the same server, pinned to a release. Nothing in
the project depends on MinIO specifically: point `LH_S3_ENDPOINT` and the Trino catalog at any
S3-compatible store.

## Engineering

- 13 tests, 99% line coverage with the stack up; the integration tests run a smaller verification,
  the benchmark and the CLI against the real services.
- `ruff`, `mypy --strict`, Python 3.11 to 3.13.
- CI:
  - lint, types and unit tests on three Python versions;
  - the whole stack in Docker Compose, all tests with a 90% coverage gate, `lh verify --check` and
    the benchmark, with both reports in the job summary;
  - a wheel install outside the repository that carries the dbt project;
  - CodeQL, pip-audit, and Dependabot for Python, Actions and the Compose images.
- Design notes: [architecture](docs/architecture.md) and [decision records](docs/adr).

## Layout

```
docker-compose.yml          PostgreSQL (catalog), MinIO, bucket creation, Trino; Superset profile
infra/trino/etc/            Trino configuration and the Iceberg catalog (JDBC catalog, native S3)
infra/postgres/             the Iceberg JDBC catalog tables
transform/                  dbt project: staging views, silver (incremental MERGE), gold
src/lakehouse/
  generator.py              deterministic CDC batches and the independent Reference
  ingest.py                 pyiceberg landing, schema evolution, partition evolution
  verify.py                 the end-to-end verification
  maintenance.py            health, optimize, expire_snapshots, remove_orphan_files
  bench.py                  the benchmark
  trino.py, dbt.py, cli.py  Trino client with query statistics, dbt runner, lh command
```

## Known limits

- One Trino node. The configuration is for a laptop or a CI runner, not a cluster.
- The JDBC catalog has no access control or credential vending of its own; every engine that
  uses it holds storage credentials. A REST catalog (Apache Polaris, Lakekeeper, Nessie) adds
  both and is the next step for anything shared. See [ADR-0001](docs/adr/0001-jdbc-catalog.md).
- Events below the watermark (an LSN lower than one already applied) are not merged; they fail
  the dbt test instead. A source with independent LSN sequences would need a per-source watermark.
- The data is synthetic, and the benchmark measures layouts on one machine.

## License

MIT. See [LICENSE](LICENSE).
