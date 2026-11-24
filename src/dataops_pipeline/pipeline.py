"""Pipeline orchestration: extract -> validate (contract) -> load (idempotent) -> DQ -> alert."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from .alerting import Notifier, build_notifier
from .config import Settings
from .contract import Contract
from .extract import extract
from .logging_utils import get_logger
from .quality import CheckResult, run_checks
from .store import Store

log = get_logger(__name__)


class DataQualityError(RuntimeError):
    """Raised when an `error`-severity data quality check fails."""


@dataclass
class RunResult:
    run_id: str
    status: str
    rows_read: int
    rows_loaded: int
    rows_quarantined: int
    dq_results: list[CheckResult]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def run(settings: Settings, notifier: Notifier | None = None,
        now: datetime | None = None) -> RunResult:
    notifier = notifier or build_notifier(settings.alert_webhook_url)
    now = now or _now()
    run_id = uuid.uuid4().hex[:12]
    ctx = {"run_id": run_id, "pipeline": settings.pipeline_name}
    counts = {"read": 0, "loaded": 0, "quarantined": 0}

    contract = Contract.load(settings.contract_path)
    store = Store(settings.db_path)
    store.start_run(run_id, settings.pipeline_name, now.isoformat())
    log.info("run started", extra={"ctx": {**ctx, "contract": f"{contract.name}@{contract.version}"}})

    try:
        watermark = store.get_watermark(settings.pipeline_name)
        valid, rejected = [], []
        for raw in extract(settings.input_dir, watermark):
            counts["read"] += 1
            source = raw.pop("_source_file", None)
            typed, reason = contract.validate(raw)
            if typed is None:
                rejected.append((source, json.dumps(raw), reason))
            else:
                valid.append(typed)

        # Data, quarantine and watermark move together or not at all.
        with store.transaction() as conn:
            counts["loaded"] = store.upsert_metrics(conn, valid, run_id, now.isoformat())
            counts["quarantined"] = store.quarantine(conn, rejected, run_id, now.isoformat())
            if valid:
                store.set_watermark(conn, settings.pipeline_name,
                                    max(r["event_time"] for r in valid))

        results = run_checks(store.conn, now, settings.freshness_hours,
                             counts["loaded"], counts["quarantined"])
        store.save_dq_results(run_id, results, now.isoformat())

        failed_errors = [r for r in results if not r.passed and r.severity == "error"]
        failed_warns = [r for r in results if not r.passed and r.severity == "warn"]
        for r in failed_warns:
            notifier.send(f"DQ warning: {r.name}", f"run {run_id}: {r.detail}", "warn")
        if failed_errors:
            detail = "; ".join(f"{r.name} ({r.detail})" for r in failed_errors)
            raise DataQualityError(detail)

        store.finish_run(run_id, "success", _now().isoformat(), counts)
        log.info("run finished", extra={"ctx": {**ctx, **counts, "status": "success"}})
        return RunResult(run_id, "success", counts["read"], counts["loaded"],
                         counts["quarantined"], results)

    except Exception as exc:
        store.finish_run(run_id, "failed", _now().isoformat(), counts, error=str(exc))
        log.error("run failed", exc_info=True, extra={"ctx": {**ctx, **counts}})
        notifier.send(f"Pipeline {settings.pipeline_name} failed",
                      f"run {run_id}: {type(exc).__name__}: {exc}", "error")
        raise
    finally:
        store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the DataOps reference pipeline.")
    parser.add_argument("--input-dir", type=Path)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--freshness-hours", type=float)
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    overrides = {k: v for k, v in {
        "input_dir": args.input_dir, "db_path": args.db,
        "contract_path": args.contract, "freshness_hours": args.freshness_hours,
    }.items() if v is not None}
    settings = replace(settings, **overrides)

    try:
        run(settings)
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
