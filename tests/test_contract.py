from datetime import datetime

from conftest import ROOT, row

from dataops_pipeline.contract import Contract

CONTRACT = Contract.load(ROOT / "contracts" / "device_metrics.json")


def test_valid_record_is_typed():
    typed, reason = CONTRACT.validate(row(value="42.5"))
    assert reason is None
    assert typed["value"] == 42.5
    assert typed["event_time"] == datetime(2026, 11, 24, 10, 0, 0)


def test_optional_field_can_be_empty():
    typed, reason = CONTRACT.validate(row(site=""))
    assert reason is None and typed["site"] is None


def test_missing_required_field_is_rejected():
    _, reason = CONTRACT.validate(row(device=""))
    assert "missing required field 'device_id'" in reason


def test_value_outside_allowed_set_is_rejected():
    _, reason = CONTRACT.validate(row(metric="temperature"))
    assert "not in" in reason


def test_negative_value_is_rejected():
    _, reason = CONTRACT.validate(row(value=-1))
    assert "below min" in reason


def test_wrong_type_is_rejected():
    _, reason = CONTRACT.validate(row(value="n/a"))
    assert "not a valid float" in reason
