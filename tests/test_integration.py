"""Against a running stack: PostgreSQL catalog, S3-compatible storage, Trino."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse import bench, verify
from lakehouse.cli import main
from lakehouse.generator import Generator
from lakehouse.settings import Settings


def test_end_to_end_verification(settings: Settings) -> None:
    small = Generator(seed=21, orders_per_batch=300, updates_per_batch=200, deletes_per_batch=6, customers=60, coupons_from=2)
    result = verify.run(settings, batches=3, appends=4, generator=small, daily_partitions_from=3)
    assert not result.failures, result.markdown()
    assert len(result.checks) == 21


def test_benchmark_runs(settings: Settings) -> None:
    report = bench.run(settings, events=20_000, appends=40)
    assert len(report.measurements) == 6
    assert report.before is not None and report.after is not None and report.after.data_files < report.before.data_files
    pruned, full = report.measurements[0], report.measurements[1]
    assert pruned.splits < full.splits and pruned.bytes_read < full.bytes_read


def test_cli(settings: Settings, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["transform"]) == 0
    assert "Completed successfully" in capsys.readouterr().out
    assert main(["health", "raw.order_events", "silver.orders"]) == 0
    assert "raw.order_events:" in capsys.readouterr().out
    assert main(["maintain", "gold.daily_sales", "--retention", "7d"]) == 0
    assert "gold.daily_sales: files" in capsys.readouterr().out
    assert main(["bench", "--events", "5000", "--appends", "10", "--report", str(tmp_path / "b.md")]) == 0
    assert (tmp_path / "b.md").read_text().startswith("# Benchmark")
