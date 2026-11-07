"""Command line: verify, ingest, transform, health, maintain, bench."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from lakehouse import __version__
from lakehouse.settings import Settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lh", description="Iceberg lakehouse on Trino: ingest, transform, verify, maintain.")
    parser.add_argument("--version", action="version", version=f"lh {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("verify", help="end-to-end run from empty storage, compared with an independent reference")
    p.add_argument("--batches", type=int, default=5)
    p.add_argument("--appends", type=int, default=8, help="appends per batch (small files on purpose)")
    p.add_argument("--report", type=Path)
    p.add_argument("--check", action="store_true", help="exit 1 if any check fails")

    p = sub.add_parser("ingest", help="land one generated batch (batches are generated in order from 1)")
    p.add_argument("--batches", type=int, default=1)

    sub.add_parser("transform", help="dbt build through Trino")

    p = sub.add_parser("health", help="files, snapshots and file sizes")
    p.add_argument("tables", nargs="+")

    p = sub.add_parser("maintain", help="optimize, expire_snapshots and remove_orphan_files")
    p.add_argument("tables", nargs="+")
    p.add_argument("--retention", default="7d")

    p = sub.add_parser("bench", help="partition pruning and compaction, measured")
    p.add_argument("--events", type=int, default=1_000_000)
    p.add_argument("--appends", type=int, default=200)
    p.add_argument("--report", type=Path)

    args = parser.parse_args(argv)
    settings = Settings.from_env()
    return int(COMMANDS[args.command](args, settings))


def _verify(args: argparse.Namespace, settings: Settings) -> int:
    from lakehouse.verify import run

    result = run(settings, args.batches, args.appends, progress=lambda m: print(m, file=sys.stderr))
    text = result.markdown()
    if args.report:
        args.report.write_text(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write(text)
    for c in result.checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name}  {c.detail}")
    return 1 if args.check and result.failures else 0


def _ingest(args: argparse.Namespace, settings: Settings) -> int:
    from lakehouse.generator import Generator
    from lakehouse.ingest import catalog, ingest

    gen = Generator()
    cat = catalog(settings)
    for n in range(1, args.batches + 1):
        print(ingest(cat, gen.batch(n), daily_partitions=n >= 4))
    return 0


def _transform(args: argparse.Namespace, settings: Settings) -> int:
    from lakehouse import dbt

    print(dbt.build(settings, quiet=False))
    return 0


def _health(args: argparse.Namespace, settings: Settings) -> int:
    from lakehouse.maintenance import health
    from lakehouse.trino import Trino

    trino = Trino(settings)
    for table in args.tables:
        h = health(trino, table)
        print(
            f"{h.table}: {h.data_files} data files, {h.delete_files} delete files, {h.snapshots} snapshots, {h.rows:,} rows, "
            f"{h.avg_file_kb:.0f} KB average data file"
        )
    return 0


def _maintain(args: argparse.Namespace, settings: Settings) -> int:
    from lakehouse.maintenance import maintain
    from lakehouse.trino import Trino

    for before, after in maintain(Trino(settings), args.tables, args.retention):
        print(f"{before.table}: files {before.data_files} -> {after.data_files}, snapshots {before.snapshots} -> {after.snapshots}")
    return 0


def _bench(args: argparse.Namespace, settings: Settings) -> int:
    from lakehouse.bench import run

    report = run(settings, args.events, args.appends)
    text = report.markdown()
    if args.report:
        args.report.write_text(text)
    print(text)
    return 0


COMMANDS = {"verify": _verify, "ingest": _ingest, "transform": _transform, "health": _health, "maintain": _maintain, "bench": _bench}
