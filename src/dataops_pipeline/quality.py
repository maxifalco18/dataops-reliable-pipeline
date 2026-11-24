"""Post-load data quality checks.

`error` checks fail the run (and alert); `warn` checks only alert.
Every result is persisted in `dq_results` for trend dashboards (e.g. Grafana).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class CheckResult:
    name: str
    severity: str  # "error" | "warn"
    passed: bool
    detail: str


def run_checks(conn: sqlite3.Connection, now: datetime, freshness_hours: float,
               rows_loaded: int, rows_quarantined: int) -> list[CheckResult]:
    results: list[CheckResult] = []

    total = conn.execute("SELECT COUNT(*) FROM device_metrics").fetchone()[0]
    results.append(CheckResult(
        "table_not_empty", "error", total > 0, f"rows in table: {total}"))

    nulls = conn.execute(
        "SELECT COUNT(*) FROM device_metrics WHERE value IS NULL OR device_id IS NULL"
    ).fetchone()[0]
    results.append(CheckResult(
        "no_null_keys_or_values", "error", nulls == 0, f"null rows: {nulls}"))

    dupes = conn.execute(
        "SELECT COUNT(*) FROM (SELECT device_id, metric, event_time, COUNT(*) c "
        "FROM device_metrics GROUP BY 1, 2, 3 HAVING c > 1)"
    ).fetchone()[0]
    results.append(CheckResult(
        "primary_key_unique", "error", dupes == 0, f"duplicated keys: {dupes}"))

    newest = conn.execute("SELECT MAX(event_time) FROM device_metrics").fetchone()[0]
    if newest is None:
        results.append(CheckResult("freshness", "error", False, "no data"))
    else:
        age = now - datetime.fromisoformat(newest)
        results.append(CheckResult(
            "freshness", "error", age <= timedelta(hours=freshness_hours),
            f"newest event {newest}, age {age}"))

    processed = rows_loaded + rows_quarantined
    ratio = rows_quarantined / processed if processed else 0.0
    results.append(CheckResult(
        "quarantine_ratio_below_10pct", "warn", ratio < 0.10,
        f"quarantined {rows_quarantined} of {processed} ({ratio:.1%})"))

    return results
