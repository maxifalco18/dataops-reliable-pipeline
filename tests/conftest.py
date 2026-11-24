import csv
from datetime import datetime
from pathlib import Path

import pytest

from dataops_pipeline.config import Settings

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ["device_id", "metric", "value", "event_time", "site"]
NOW = datetime(2026, 11, 24, 12, 0, 0)


class FakeNotifier:
    def __init__(self):
        self.alerts: list[tuple[str, str, str]] = []

    def send(self, title, body, severity):
        self.alerts.append((title, body, severity))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def row(device="rtr-01", metric="cpu_pct", value=50, ts="2026-11-24T10:00:00", site="north"):
    return {"device_id": device, "metric": metric, "value": value, "event_time": ts, "site": site}


@pytest.fixture
def settings(tmp_path):
    return Settings(
        input_dir=tmp_path / "input",
        db_path=tmp_path / "warehouse.db",
        contract_path=ROOT / "contracts" / "device_metrics.json",
        freshness_hours=24,
        alert_webhook_url="",
    )


@pytest.fixture
def notifier():
    return FakeNotifier()
