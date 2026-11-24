import sqlite3
from dataclasses import replace

import pytest
from conftest import NOW, row, write_csv

from dataops_pipeline.pipeline import DataQualityError, run


def count(db, table):
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_happy_path_loads_valid_and_quarantines_invalid(settings, notifier):
    write_csv(settings.input_dir / "a.csv", [
        row(), row(metric="mem_pct", value=60), row(metric="temperature")])

    result = run(settings, notifier, now=NOW)

    assert result.status == "success"
    assert (result.rows_read, result.rows_loaded, result.rows_quarantined) == (3, 2, 1)
    assert count(settings.db_path, "device_metrics") == 2
    assert count(settings.db_path, "quarantine") == 1


def test_rerun_is_idempotent(settings, notifier):
    write_csv(settings.input_dir / "a.csv", [row(), row(metric="mem_pct")])
    run(settings, notifier, now=NOW)

    # Simulate a manual re-run / backfill over the same files: reset the watermark.
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute("DELETE FROM pipeline_state")
    run(settings, notifier, now=NOW)

    assert count(settings.db_path, "device_metrics") == 2  # no duplicates


def test_incremental_load_uses_watermark(settings, notifier):
    write_csv(settings.input_dir / "a.csv", [row(ts="2026-11-24T09:00:00")])
    first = run(settings, notifier, now=NOW)

    write_csv(settings.input_dir / "b.csv", [row(ts="2026-11-24T11:00:00")])
    second = run(settings, notifier, now=NOW)

    assert first.rows_read == 1
    assert second.rows_read == 1  # a.csv is skipped: already below the watermark
    assert count(settings.db_path, "device_metrics") == 2


def test_stale_data_fails_run_and_alerts(settings, notifier):
    write_csv(settings.input_dir / "a.csv", [row(ts="2026-11-20T10:00:00")])

    with pytest.raises(DataQualityError, match="freshness"):
        run(settings, notifier, now=NOW)

    assert any(sev == "error" for _, _, sev in notifier.alerts)
    with sqlite3.connect(settings.db_path) as conn:
        status = conn.execute("SELECT status FROM pipeline_runs").fetchone()[0]
    assert status == "failed"


def test_high_quarantine_ratio_only_warns(settings, notifier):
    write_csv(settings.input_dir / "a.csv", [row(), row(metric="bad"), row(metric="worse")])

    result = run(settings, notifier, now=NOW)

    assert result.status == "success"
    assert [sev for _, _, sev in notifier.alerts] == ["warn"]


def test_missing_input_dir_fails_cleanly(settings, notifier):
    with pytest.raises(FileNotFoundError):
        run(replace(settings, input_dir=settings.input_dir / "nope"), notifier, now=NOW)
    assert notifier.alerts and notifier.alerts[0][2] == "error"
