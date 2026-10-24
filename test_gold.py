"""Gold transformations on tiny, hand-computed inputs."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src.transformations.gold import (
    build_daily_summary,
    build_temperature_alerts,
    build_vehicle_kpis,
    haversine_km,
)

SCHEMA = (
    "vehicle_id string, event_ts timestamp, event_date date, speed_kmph double, engine_temp_c double, "
    "fuel_level_pct double, battery_voltage_v double, latitude double, longitude double, is_late_arrival boolean"
)


def silver_df(spark, rows):
    data = [
        (v, datetime.fromisoformat(ts), datetime.fromisoformat(ts).date(), *map(float, (spd, temp, fuel, batt, lat, lon)), False)
        for v, ts, spd, temp, fuel, batt, lat, lon in rows
    ]
    return spark.createDataFrame(data, SCHEMA)


def test_haversine_one_hundredth_degree_latitude(spark):
    df = spark.createDataFrame([(0.0, 0.0, 0.01, 0.0)], "a double, b double, c double, d double")
    km = df.select(haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("km")).first()["km"]
    assert km == pytest.approx(1.1119, abs=0.001)


def test_daily_distance_gap_and_jump(spark, cfg):
    rows = [
        ("V1", "2026-01-05 08:00:00", 60, 90, 50, 13.8, 0.00, 0.0),
        ("V1", "2026-01-05 08:01:00", 60, 90, 50, 13.8, 0.01, 0.0),   # +1.112 km valid
        ("V1", "2026-01-05 08:02:00", 60, 90, 50, 13.8, 0.02, 0.0),   # +1.112 km valid
        ("V1", "2026-01-05 08:10:00", 60, 90, 50, 13.8, 0.03, 0.0),   # 8 min gap -> excluded
        ("V1", "2026-01-05 08:11:00", 60, 90, 50, 13.8, 5.00, 0.0),   # ~550 km in 1 min -> GPS jump
    ]
    r = build_daily_summary(silver_df(spark, rows), cfg).first()
    assert r["record_count"] == 5
    assert r["distance_km"] == pytest.approx(2.224, abs=0.01)
    assert r["moving_minutes"] == pytest.approx(2.0)
    assert r["data_gap_segments"] == 1 and r["gps_jump_segments"] == 1


def test_daily_summary_thresholds(spark, cfg):
    rows = [
        ("V1", "2026-01-05 08:00:00", 0, 106, 5, 11.0, 0.0, 0.0),
        ("V1", "2026-01-05 08:01:00", 0, 90, 50, 13.8, 0.0, 0.0),
    ]
    r = build_daily_summary(silver_df(spark, rows), cfg).first()
    assert (r["high_temp_readings"], r["low_fuel_readings"], r["low_battery_readings"]) == (1, 1, 1)
    assert r["max_engine_temp_c"] == 106 and r["moving_minutes"] == 0.0


def test_alert_severity_boundaries(spark, cfg):
    temps = [104.9, 105.0, 114.9, 115.0]
    rows = [("V1", f"2026-01-05 08:0{i}:00", 10, t, 50, 13.8, 0.0, 0.0) for i, t in enumerate(temps)]
    alerts = {r["engine_temp_c"]: r["severity"] for r in build_temperature_alerts(silver_df(spark, rows), cfg).collect()}
    assert alerts == {105.0: "WARNING", 114.9: "WARNING", 115.0: "CRITICAL"}


def test_vehicle_kpis(spark, cfg):
    rows = [
        ("V1", "2026-01-05 08:00:00", 40, 90, 50, 13.8, 0.00, 0.0),
        ("V1", "2026-01-05 08:01:00", 60, 110, 50, 13.8, 0.01, 0.0),
        ("V1", "2026-01-06 08:00:00", 20, 120, 8, 13.8, 0.00, 0.0),
    ]
    silver = silver_df(spark, rows)
    daily = build_daily_summary(silver, cfg)
    k = build_vehicle_kpis(silver, daily, cfg).first()
    assert (k["total_records"], k["active_days"]) == (3, 2)
    assert (k["warning_temp_readings"], k["critical_temp_readings"]) == (1, 1)
    assert k["low_fuel_readings"] == 1
    assert k["avg_speed_kmph"] == 40.0
    assert k["total_distance_km"] == pytest.approx(1.112, abs=0.01)
    assert k["temp_alert_rate_per_1000"] == pytest.approx(666.67, abs=0.01)
