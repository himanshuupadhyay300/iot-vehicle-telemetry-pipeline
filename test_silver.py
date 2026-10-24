"""Silver behaviour: de-duplication, conflict resolution, late arrivals, idempotent re-processing."""
from pyspark.sql import functions as F

from src.transformations.dedupe import deduplicate_latest
from src.transformations.silver import KEYS, ORDER_COLS, process_batch
from src.utils import storage
from src.utils.config import table_path


def silver(spark, cfg):
    return storage.read_table(spark, table_path(cfg, "silver"), cfg["storage"]["format"])


def test_dedupe_keeps_latest_ingested(spark):
    df = spark.createDataFrame(
        [("V", "t1", 1, "2026-01-01 00:00:00"), ("V", "t1", 2, "2026-01-02 00:00:00"), ("V", "t2", 3, "2026-01-01 00:00:00")],
        "vehicle_id string, event_ts string, val int, _ingested_at string",
    )
    out = deduplicate_latest(df, ["vehicle_id", "event_ts"], ["_ingested_at"]).collect()
    assert {(r.event_ts, r.val) for r in out} == {("t1", 2), ("t2", 3)}


def test_exact_duplicates_collapse(spark, cfg, make_bronze):
    bronze = make_bronze([{}, {}, {}])  # same key three times
    m = process_batch(spark, cfg, bronze, "b1")
    assert m["valid_rows"] == 3 and m["in_batch_duplicate_rows_removed"] == 2
    assert m["conflicting_duplicate_keys"] == 0
    assert silver(spark, cfg).count() == 1


def test_conflicting_duplicate_latest_row_wins(spark, cfg, make_bronze):
    bronze = make_bronze([{"speed_kmph": "50"}, {"speed_kmph": "77"}])  # _row_id 1 is later in file
    m = process_batch(spark, cfg, bronze, "b1")
    assert m["conflicting_duplicate_keys"] == 1
    assert silver(spark, cfg).first()["speed_kmph"] == 77.0


def test_invalid_rows_go_to_quarantine_not_silver(spark, cfg, make_bronze):
    bronze = make_bronze([{}, {"engine_temp_c": ""}, {"speed_kmph": "999"}])
    m = process_batch(spark, cfg, bronze, "b1")
    assert (m["valid_rows"], m["rejected_rows"]) == (1, 2)
    assert silver(spark, cfg).count() == 1
    rejects = storage.read_table(spark, table_path(cfg, "silver_rejects"), "parquet")
    assert rejects.count() == 2
    reasons = {r[0] for r in rejects.select(F.explode("_dq_errors")).distinct().collect()}
    assert {"missing_engine_temp_c", "out_of_range_speed_kmph"} <= reasons


def test_later_batch_updates_existing_key(spark, cfg, make_bronze):
    process_batch(spark, cfg, make_bronze([{"speed_kmph": "50"}], "b1", "2026-01-10 00:00:00"), "b1")
    m = process_batch(spark, cfg, make_bronze([{"speed_kmph": "60"}], "b2", "2026-01-11 00:00:00"), "b2")
    assert m["rows_matching_existing_silver"] == 1 and m["late_arrival_rows"] == 0
    s = silver(spark, cfg)
    assert s.count() == 1 and s.first()["speed_kmph"] == 60.0


def test_older_ingestion_does_not_overwrite_newer(spark, cfg, make_bronze):
    process_batch(spark, cfg, make_bronze([{"speed_kmph": "60"}], "b2", "2026-01-11 00:00:00"), "b2")
    process_batch(spark, cfg, make_bronze([{"speed_kmph": "50"}], "b1", "2026-01-10 00:00:00"), "b1")  # replayed old batch
    assert silver(spark, cfg).first()["speed_kmph"] == 60.0


def test_late_arrival_is_flagged(spark, cfg, make_bronze):
    process_batch(spark, cfg, make_bronze([{"timestamp": "2026-01-05T09:00:00Z"}], "b1", "2026-01-10 00:00:00"), "b1")
    m = process_batch(spark, cfg, make_bronze([{"timestamp": "2026-01-05T08:00:00Z"}], "b2", "2026-01-11 00:00:00"), "b2")
    assert m["late_arrival_rows"] == 1
    flags = {r["event_ts"].hour: r["is_late_arrival"] for r in silver(spark, cfg).collect()}
    assert flags == {9: False, 8: True}


def test_reprocessing_same_batch_is_idempotent(spark, cfg, make_bronze):
    bronze = make_bronze([{}, {"timestamp": "2026-01-05T08:01:00Z"}, {"engine_temp_c": ""}])
    process_batch(spark, cfg, bronze, "b1")
    first = sorted(r.asDict().items() for r in silver(spark, cfg).drop("_processed_at").collect())
    process_batch(spark, cfg, bronze, "b1")
    second = sorted(r.asDict().items() for r in silver(spark, cfg).drop("_processed_at").collect())
    assert first == second
    rejects = storage.read_table(spark, table_path(cfg, "silver_rejects"), "parquet")
    assert rejects.count() == 1  # quarantine rows replaced per batch, not doubled


def test_keys_constant():
    assert KEYS == ["vehicle_id", "event_ts"] and ORDER_COLS[0] == "_ingested_at"
