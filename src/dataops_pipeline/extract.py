"""Incremental extraction from CSV drops, filtered by the last loaded watermark."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from .retry import retry


@retry(attempts=3, base_delay=0.5)
def _read_file(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def extract(input_dir: Path, watermark: datetime | None) -> Iterator[dict[str, str]]:
    """Yield raw rows newer than the watermark.

    Rows with an unparseable timestamp are still yielded: the contract
    validation step decides what to do with them (quarantine), so nothing
    disappears without a trace.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"input directory not found: {input_dir}")
    for path in sorted(input_dir.glob("*.csv")):
        for row in _read_file(path):
            row["_source_file"] = path.name
            if watermark is not None:
                try:
                    if datetime.fromisoformat(row.get("event_time", "")) <= watermark:
                        continue
                except ValueError:
                    pass
            yield row
