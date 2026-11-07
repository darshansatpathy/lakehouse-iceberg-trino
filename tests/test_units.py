from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from lakehouse import dbt
from lakehouse.bench import BenchReport, Measurement, generate
from lakehouse.generator import Customer, Event
from lakehouse.ingest import CUSTOMER_SCHEMA, EVENT_SCHEMA, customers_table, events_table
from lakehouse.maintenance import Health
from lakehouse.resources import resource
from lakehouse.settings import Settings
from lakehouse.verify import Verification, _diff


def test_settings_from_env() -> None:
    s = Settings.from_env({"LH_TRINO_PORT": "18080", "LH_S3_ENDPOINT": "http://127.0.0.1:9000"})
    assert s.trino_port == 18080 and s.s3_endpoint == "http://127.0.0.1:9000"
    assert s.dbt_env()["LH_TRINO_PORT"] == "18080"
    assert Settings.from_env({}) == Settings()


def test_arrow_tables_follow_the_iceberg_schema() -> None:
    ts = dt.datetime(2025, 1, 1)
    events = [Event(1, "c", ts, 1, 2, ts, "pending", Decimal("1.50"), "web", "VIP20")]
    table = events_table(events, 3, EVENT_SCHEMA)
    assert table.column_names == [f.name for f in EVENT_SCHEMA.fields] and table["batch_id"][0].as_py() == 3
    assert table.schema.field("amount").type == pa.decimal128(12, 2)
    customers = customers_table([Customer(1, "US", "consumer", dt.date(2024, 1, 1))], CUSTOMER_SCHEMA)
    assert customers.num_rows == 1


def test_reports_render() -> None:
    v = Verification()
    v.check("a", True, "fine")
    v.check("b", False)
    v.batches.append((1, 10, 9, 5))
    v.maintenance.append((Health("t", 10, 4, 100, 10240), Health("t", 2, 1, 100, 10240)))
    text = v.markdown()
    assert "**1 / 2 checks passed.**" in text and "**FAIL**" in text and "10 → 2" in text and "1 KB → 5 KB" in text and "| 0 → 0 |" in text
    assert [c.name for c in v.failures] == ["b"]
    assert Health("t", 0, 0, 0, 0).avg_file_kb == 0
    assert _diff({1: 1}, {1: 2, 3: 3}) == "2 missing or different, 1 unexpected"
    report = BenchReport(100, 2, Health("t", 4, 1, 100, 4096), Health("t", 1, 1, 100, 4096), [Measurement("q", "before compaction", 5, 4, 4096, 100)])
    assert "4 files averaging 1 KB → 1 files averaging 4 KB" in report.markdown()


def test_bench_data_is_time_ordered() -> None:
    data = generate(500, seed=1)
    commits = data["commit_ts"].to_pylist()
    assert data.num_rows == 500 and commits == sorted(commits)


def test_dbt_project_is_found_and_errors_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    assert (resource("transform") / "dbt_project.yml").exists()
    assert dbt.dbt_executable().endswith("dbt")

    class Failed:
        returncode, stdout, stderr = 2, "boom", "trace"

    monkeypatch.setattr(dbt.subprocess, "run", lambda *a, **k: Failed())
    with pytest.raises(dbt.DbtError, match="boom"):
        dbt.build(Settings(), select="orders")


def test_verify_command_reports_and_fails_on_a_failed_check(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from lakehouse import cli, verify

    result = Verification()
    result.check("something", False, "broken")
    monkeypatch.setattr(verify, "run", lambda *a, **k: result)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert cli.main(["verify", "--report", str(tmp_path / "v.md"), "--check"]) == 1
    assert cli.main(["verify"]) == 0
    assert "**FAIL**" in (tmp_path / "v.md").read_text() and summary.exists()


def test_ingest_command(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from lakehouse import cli, ingest

    calls: list[tuple[int, bool]] = []
    monkeypatch.setattr(ingest, "catalog", lambda settings: object())
    monkeypatch.setattr(ingest, "ingest", lambda cat, batch, daily_partitions: calls.append((batch.number, daily_partitions)) or batch.number)
    assert cli.main(["ingest", "--batches", "4"]) == 0
    assert calls == [(1, False), (2, False), (3, False), (4, True)]
    assert capsys.readouterr().out.split() == ["1", "2", "3", "4"]
