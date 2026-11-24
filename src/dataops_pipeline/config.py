"""Runtime settings, read from environment variables with safe defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    input_dir: Path
    db_path: Path
    contract_path: Path
    # Max age (hours) of the newest record before the freshness check fails.
    freshness_hours: float
    # Optional Slack-compatible webhook. Empty means console-only alerts.
    alert_webhook_url: str
    pipeline_name: str = "device_metrics"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            input_dir=Path(os.getenv("PIPELINE_INPUT_DIR", "data/input")),
            db_path=Path(os.getenv("PIPELINE_DB_PATH", "data/warehouse.db")),
            contract_path=Path(
                os.getenv("PIPELINE_CONTRACT", "contracts/device_metrics.json")
            ),
            freshness_hours=float(os.getenv("PIPELINE_FRESHNESS_HOURS", "24")),
            alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL", ""),
        )
