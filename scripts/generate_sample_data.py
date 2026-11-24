"""Generate synthetic telemetry CSVs relative to *now* (so freshness checks pass).

Includes a few deliberately broken rows to show contract enforcement / quarantine.

Usage: python scripts/generate_sample_data.py [--out data/input] [--hours 6] [--seed 42]
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEVICES = [f"rtr-{i:02d}" for i in range(1, 6)]
SITES = ["north", "south", "east"]
RANGES = {"cpu_pct": (5, 95), "mem_pct": (20, 90), "latency_ms": (1, 120),
          "packet_loss_pct": (0, 3)}


def generate(out_dir: Path, hours: int, seed: int) -> Path:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    rows = []
    for h in range(hours, 0, -1):
        ts = (now - timedelta(hours=h - 1)).isoformat()
        for device in DEVICES:
            for metric, (lo, hi) in RANGES.items():
                rows.append({"device_id": device, "metric": metric,
                             "value": round(rng.uniform(lo, hi), 2),
                             "event_time": ts, "site": rng.choice(SITES)})
    # Contract violations that must end up in quarantine, not in the table.
    bad = [
        ("", "cpu_pct", 10),             # missing device_id
        ("rtr-01", "temperature", 40),   # metric not in the contract
        ("rtr-02", "latency_ms", -5),    # negative value
        ("rtr-03", "mem_pct", "n/a"),    # wrong type
    ]
    rows += [{"device_id": d, "metric": m, "value": v, "event_time": now.isoformat(), "site": "north"}
             for d, m, v in bad]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"metrics_{now:%Y%m%dT%H}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["device_id", "metric", "value", "event_time", "site"])
        writer.writeheader()
        writer.writerows(rows)
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("data/input"))
    parser.add_argument("--hours", type=int, default=6)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    print(generate(args.out, args.hours, args.seed))
