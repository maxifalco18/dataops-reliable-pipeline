"""Data contract: the schema and rules agreed with the data producer.

Records that break the contract are not dropped silently: they go to a
quarantine table with the reason, so they can be reviewed and replayed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

_CASTERS = {
    "string": str,
    "float": float,
    "int": int,
    "timestamp": datetime.fromisoformat,
}


@dataclass
class FieldRule:
    name: str
    type: str
    required: bool = True
    allowed: list[str] | None = None
    min: float | None = None
    max: float | None = None


@dataclass
class Contract:
    name: str
    version: str
    primary_key: list[str]
    fields: list[FieldRule] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path) -> Contract:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        unknown_types = {f["type"] for f in raw["fields"]} - _CASTERS.keys()
        if unknown_types:
            raise ValueError(f"Unsupported field types in contract: {unknown_types}")
        return cls(
            name=raw["name"],
            version=raw["version"],
            primary_key=raw["primary_key"],
            fields=[FieldRule(**f) for f in raw["fields"]],
        )

    def validate(self, record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        """Return (typed_record, None) if valid, or (None, reason) if not."""
        typed: dict[str, Any] = {}
        for rule in self.fields:
            value = record.get(rule.name)
            if value in (None, ""):
                if rule.required:
                    return None, f"missing required field '{rule.name}'"
                typed[rule.name] = None
                continue
            try:
                cast_value = _CASTERS[rule.type](value)
            except (TypeError, ValueError):
                return None, f"field '{rule.name}' is not a valid {rule.type}: {value!r}"
            if rule.allowed is not None and cast_value not in rule.allowed:
                return None, f"field '{rule.name}' value {cast_value!r} not in {rule.allowed}"
            if rule.min is not None and cast_value < rule.min:
                return None, f"field '{rule.name}' below min {rule.min}: {cast_value}"
            if rule.max is not None and cast_value > rule.max:
                return None, f"field '{rule.name}' above max {rule.max}: {cast_value}"
            typed[rule.name] = cast_value
        return typed, None
