"""End-to-end: generate synthetic data, run batch 1 then batch 2 then a re-run, compare with the
expectations computed independently by the data generator."""
import json
import shutil
from pathlib import Path

import pytest

from src.ingestion.generate_synthetic_data import generate
from src.pipeline import run_pipeline
from src.utils import storage
from src.utils.config import table_path


@pytest.fixture(scope="module")
def world(spark, tmp_path_factory):
    from src.utils.config import load_config
    from tests.conftest import CONFIG_PATH

    root = tmp_path_factory.mktemp("e2e")
    landing = root / "landing"
    expected = generate(landing, seed=42)
    # keep batch 2 out of the landing folder until "it arrives"
    held_back = root / "held" / "telemetry_batch_002.json"
    held_back.parent.mkdir()
    shutil.move(str(landing / "telemetry_batch_002.json"), held_back)
    cfg = load_config(
        CONFIG_PATH,
        {
            "storage": {"base_path": str(root / "lake"), "input_path": str(landing), "reports_path": str(root / "reports")},
            "spark": {"shuffle_partitions": 2},
        },
    )
    fmt = cfg["storage"]["format"]

    def table(key):
        return storage.read_table(spark, table_path(cfg, key), fmt)

    run1 = run_pipeline(cfg, spark=spark)
    vh1_after_1 = table("gold_daily_summary").filter("vehicle_id='VH-0001' and event_date='2026-01-05'").first()
    shutil.move(str(held_back), landing / "telemetry_batch_002.json")
    run2 = run_pipeline(cfg, spark=spark)
    vh1_after_2 = table("gold_daily_summary").filter("vehicle_id='VH-0001' and event_date='2026-01-05'").first()
    counts_after_2 = {k: table(k).count() for k in ("bronze", "silver", "silver_rejects", "gold_daily_summary", "gold_temperature_alerts", "gold_vehicle_kpis")}
    run3 = run_pipeline(cfg, spark=spark)  # nothing new
    return dict(cfg=cfg, expected=expected, run1=run1, run2=run2, run3=run3, vh1=(vh1_after_1, vh1_after_2), counts=counts_after_2, table=table, root=root)


def test_batch1_matches_expected(world):
    e, run1 = world["expected"]["after_batch_1"], world["run1"]
    assert run1["status"] == "success"
    assert run1["stages"]["bronze"]["rows_ingested"] == e["bronze_rows"]
    assert run1["stages"]["silver"]["valid_rows"] == e["valid_rows"]
    assert run1["stages"]["silver"]["rejected_rows"] == e["rejected_rows"]
    assert run1["stages"]["silver"]["late_arrival_rows"] == 0
    assert run1["table_row_counts"]["silver"] == e["silver_rows"]
    assert run1["table_row_counts"]["gold_daily_summary"] == e["gold_daily_rows"]
    assert run1["table_row_counts"]["gold_temperature_alerts"] == e["temperature_alerts"]


def test_batch1_dq_report_details(world):
    # batch ids are timestamped, so sorted order == chronological order (glob order is arbitrary)
    first = sorted(Path(world["cfg"]["storage"]["reports_path"]).glob("dq_report_*.json"))[0]
    report = json.loads(first.read_text())
    e = world["expected"]["after_batch_1"]
    assert report["in_batch_duplicate_rows_removed"] == e["in_batch_duplicate_rows_removed"]
    assert report["conflicting_duplicate_keys"] == e["conflicting_duplicate_keys"]
    assert report["rows_with_warnings"] == e["rows_with_warnings"]
    assert report["rejected_by_rule"] == {
        "missing_engine_temp_c": 4,
        "out_of_range_speed_kmph": 3,
        "invalid_timestamp": 2,
        "missing_vehicle_id": 2,
        "invalid_type_battery_voltage_v": 1,
    }


def test_batch2_matches_expected(world):
    e, run2 = world["expected"]["after_batch_2"], world["run2"]
    assert run2["stages"]["bronze"]["files_skipped_already_ingested"] == 1  # batch 1 not re-read
    assert run2["stages"]["silver"]["late_arrival_rows"] == e["late_arrival_rows"]
    assert world["counts"]["bronze"] == e["bronze_rows"]
    assert world["counts"]["silver"] == e["silver_rows"]
    assert world["counts"]["gold_daily_summary"] == e["gold_daily_rows"]
    assert world["counts"]["gold_vehicle_kpis"] == e["gold_vehicle_kpis"]
    assert world["counts"]["gold_temperature_alerts"] == e["temperature_alerts"]
    assert world["counts"]["silver_rejects"] == world["expected"]["after_batch_1"]["rejected_rows"] + e["rejected_rows_batch_2"]
    dq2 = [json.loads(p.read_text()) for p in Path(world["cfg"]["storage"]["reports_path"]).glob("dq_report_*.json")]
    batch2 = next(r for r in dq2 if r["late_arrival_rows"] > 0)
    assert batch2["rows_matching_existing_silver"] == e["rows_matching_existing_silver"]


def test_late_data_corrects_gold(world):
    before, after = world["vh1"]
    assert before["data_gap_segments"] == 1 and after["data_gap_segments"] == 0
    # batch 2 adds 15 late readings (08:30-08:44) + 60 new readings (10:00-10:59) for VH-0001 on day 1
    assert after["record_count"] == before["record_count"] + 15 + 60
    assert after["distance_km"] > before["distance_km"]
    assert after["late_arrival_records"] == 15


def test_alert_severity_split(world):
    alerts = world["table"]("gold_temperature_alerts")
    e = world["expected"]["after_batch_2"]
    assert alerts.filter("severity='WARNING'").count() == e["warning_alerts"]
    assert alerts.filter("severity='CRITICAL'").count() == e["critical_alerts"]
    assert {r[0] for r in alerts.select("vehicle_id").distinct().collect()} == {"VH-0002"}


def test_rerun_is_a_noop(world):
    run3 = world["run3"]
    assert run3["stages"]["bronze"]["files_ingested"] == 0
    assert run3["stages"]["silver"]["batches_processed"] == 0
    assert run3["table_row_counts"] == world["run2"]["table_row_counts"]


def test_no_duplicate_keys_in_silver(world):
    silver = world["table"]("silver")
    assert silver.count() == silver.select("vehicle_id", "event_ts").distinct().count()


def test_summary_report_written(world):
    assert list(Path(world["cfg"]["storage"]["reports_path"]).glob("pipeline_summary_*.md"))
