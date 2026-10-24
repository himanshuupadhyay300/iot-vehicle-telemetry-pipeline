"""Each data quality rule is exercised with a minimal bad record."""
import pytest

from src.transformations.silver import to_typed
from src.validation.quality_rules import apply_rules, build_rules


def evaluate(make_bronze, cfg, overrides: dict):
    df = apply_rules(to_typed(make_bronze([overrides])), build_rules(cfg))
    row = df.select("_dq_errors", "_dq_warnings").first()
    return set(row["_dq_errors"]), set(row["_dq_warnings"])


def test_clean_record_passes(make_bronze, cfg):
    assert evaluate(make_bronze, cfg, {}) == (set(), set())


@pytest.mark.parametrize(
    "override, expected_error",
    [
        ({"engine_temp_c": ""}, "missing_engine_temp_c"),
        ({"engine_temp_c": None}, "missing_engine_temp_c"),
        ({"vehicle_id": "  "}, "missing_vehicle_id"),
        ({"vehicle_id": "XX-1"}, "invalid_vehicle_id_format"),
        ({"timestamp": ""}, "missing_timestamp"),
        ({"timestamp": "not-a-timestamp"}, "invalid_timestamp"),
        ({"timestamp": "2999-01-01T00:00:00Z"}, "future_timestamp"),
        ({"speed_kmph": "400"}, "out_of_range_speed_kmph"),
        ({"speed_kmph": "-1"}, "out_of_range_speed_kmph"),
        ({"speed_kmph": "fast"}, "invalid_type_speed_kmph"),
        ({"speed_kmph": "NaN"}, "invalid_type_speed_kmph"),
        ({"battery_voltage_v": "abc"}, "invalid_type_battery_voltage_v"),
        ({"battery_voltage_v": "20"}, "out_of_range_battery_voltage_v"),
        ({"latitude": "91"}, "out_of_range_latitude"),
        ({"longitude": "-181"}, "out_of_range_longitude"),
        ({"_corrupt_record": "{bad json"}, "malformed_record"),
    ],
)
def test_error_rules(make_bronze, cfg, override, expected_error):
    errors, _ = evaluate(make_bronze, cfg, override)
    assert expected_error in errors


def test_missing_vs_invalid_are_distinguished(make_bronze, cfg):
    errors, _ = evaluate(make_bronze, cfg, {"speed_kmph": ""})
    assert "missing_speed_kmph" in errors and "invalid_type_speed_kmph" not in errors


@pytest.mark.parametrize("field", ["fuel_level_pct", "battery_voltage_v"])
def test_optional_missing_is_warning_not_error(make_bronze, cfg, field):
    errors, warnings = evaluate(make_bronze, cfg, {field: ""})
    assert errors == set()
    assert warnings == {f"missing_{field}"}


def test_timestamp_formats_both_parse(make_bronze, cfg):
    for ts in ("2026-01-05T08:00:00Z", "2026-01-05 08:00:00"):
        assert evaluate(make_bronze, cfg, {"timestamp": ts}) == (set(), set())


def test_vehicle_id_is_normalised(make_bronze, cfg):
    df = to_typed(make_bronze([{"vehicle_id": " vh-0001 "}]))
    assert df.first()["vehicle_id"] == "VH-0001"
