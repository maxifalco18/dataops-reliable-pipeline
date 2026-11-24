"""Warehouse access (SQLite for a zero-dependency demo; same patterns apply to Postgres).

Idempotency comes from two things:
1. Upserts on the contract's primary key: re-running a load never duplicates rows.
2. Data, quarantine and watermark are written in ONE transaction: a failed run
   leaves no partial state, so it can simply be retried.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS device_metrics (
    device_id   TEXT NOT NULL,
    metric      TEXT NOT NULL,
    event_time  TEXT NOT NULL,
    value       REAL NOT NULL,
    site        TEXT,
    run_id      TEXT NOT NULL,
    loaded_at   TEXT NOT NULL,
    PRIMARY KEY (device_id, metric, event_time)
);
CREATE TABLE IF NOT EXISTS quarantine (
    run_id      TEXT NOT NULL,
    source_file TEXT,
    raw_record  TEXT NOT NULL,
    reason      TEXT NOT NULL,
    quarantined_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline    TEXT PRIMARY KEY,
    watermark   TEXT
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      TEXT PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    rows_read   INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    rows_quarantined INTEGER DEFAULT 0,
    error       TEXT
);
CREATE TABLE IF NOT EXISTS dq_results (
    run_id      TEXT NOT NULL,
    check_name  TEXT NOT NULL,
    severity    TEXT NOT NULL,
    passed      INTEGER NOT NULL,
    detail      TEXT,
    checked_at  TEXT NOT NULL
);
"""


class Store:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # --- state -----------------------------------------------------------
    def get_watermark(self, pipeline: str) -> datetime | None:
        row = self.conn.execute(
            "SELECT watermark FROM pipeline_state WHERE pipeline = ?", (pipeline,)
        ).fetchone()
        return datetime.fromisoformat(row["watermark"]) if row and row["watermark"] else None

    @staticmethod
    def set_watermark(conn: sqlite3.Connection, pipeline: str, value: datetime) -> None:
        conn.execute(
            "INSERT INTO pipeline_state (pipeline, watermark) VALUES (?, ?) "
            "ON CONFLICT(pipeline) DO UPDATE SET watermark = excluded.watermark",
            (pipeline, value.isoformat()),
        )

    # --- data ------------------------------------------------------------
    @staticmethod
    def upsert_metrics(conn: sqlite3.Connection, rows: list[dict[str, Any]],
                       run_id: str, loaded_at: str) -> int:
        conn.executemany(
            """
            INSERT INTO device_metrics
                (device_id, metric, event_time, value, site, run_id, loaded_at)
            VALUES (:device_id, :metric, :event_time, :value, :site, :run_id, :loaded_at)
            ON CONFLICT(device_id, metric, event_time) DO UPDATE SET
                value = excluded.value, site = excluded.site,
                run_id = excluded.run_id, loaded_at = excluded.loaded_at
            """,
            [
                {**r, "event_time": r["event_time"].isoformat(),
                 "run_id": run_id, "loaded_at": loaded_at}
                for r in rows
            ],
        )
        return len(rows)

    @staticmethod
    def quarantine(conn: sqlite3.Connection, items: list[tuple[str, str, str]],
                   run_id: str, at: str) -> int:
        conn.executemany(
            "INSERT INTO quarantine (run_id, source_file, raw_record, reason, quarantined_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [(run_id, src, raw, reason, at) for src, raw, reason in items],
        )
        return len(items)

    # --- run log ---------------------------------------------------------
    def start_run(self, run_id: str, pipeline: str, started_at: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (run_id, pipeline, status, started_at) "
                "VALUES (?, ?, 'running', ?)",
                (run_id, pipeline, started_at),
            )

    def finish_run(self, run_id: str, status: str, finished_at: str,
                   counts: dict[str, int], error: str | None = None) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE pipeline_runs SET status = ?, finished_at = ?, rows_read = ?, "
                "rows_loaded = ?, rows_quarantined = ?, error = ? WHERE run_id = ?",
                (status, finished_at, counts.get("read", 0), counts.get("loaded", 0),
                 counts.get("quarantined", 0), error, run_id),
            )

    def save_dq_results(self, run_id: str, results: list[Any], at: str) -> None:
        with self.transaction() as conn:
            conn.executemany(
                "INSERT INTO dq_results (run_id, check_name, severity, passed, detail, checked_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(run_id, r.name, r.severity, int(r.passed), r.detail, at) for r in results],
            )
