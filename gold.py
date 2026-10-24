"""Gold layer: analytics-ready datasets built from Silver.

Tables
  gold_vehicle_daily_summary  one row per vehicle per day (usage, distance, temperature, fuel, battery)
  gold_temperature_alerts     one row per reading at/above the warning temperature
  gold_vehicle_kpis           one row per vehicle (lifetime KPIs)

Incremental strategy: daily summary and alerts are recomputed ONLY for event dates touched by new data
(partition replace) -> re-running is idempotent and late events correct the affected day automatically.
KPIs are a small full recompute from Silver + the daily summary.
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.utils import storage
from src.utils.config import table_path
from src.utils.logging_utils import get_logger

log = get_logger(__name__)

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in kilometres between two GPS points (Spark column expression)."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def build_daily_summary(silver: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Per-vehicle per-day metrics.

    Distance and moving time are summed over consecutive readings (ordered by time) but a segment is only
    counted when: 0 < gap <= max_gap_minutes AND the implied speed is plausible (otherwise it is a data gap or
    a GPS jump, reported separately). Segments never cross midnight (UTC) - a documented simplification.
    """
    g = cfg["gold"]
    w = Window.partitionBy("vehicle_id", "event_date").orderBy("event_ts")
    seg = (
        silver.withColumn("_prev_ts", F.lag("event_ts").over(w))
        .withColumn("_prev_lat", F.lag("latitude").over(w))
        .withColumn("_prev_lon", F.lag("longitude").over(w))
        .withColumn("_dt_min", (F.col("event_ts").cast("long") - F.col("_prev_ts").cast("long")) / 60.0)
        .withColumn("_dist_km", haversine_km(F.col("_prev_lat"), F.col("_prev_lon"), F.col("latitude"), F.col("longitude")))
    )
    has_prev = F.col("_dt_min").isNotNull() & (F.col("_dt_min") > 0)
    within_gap = has_prev & (F.col("_dt_min") <= F.lit(float(g["max_gap_minutes"])))
    jump = within_gap & ((F.col("_dist_km") / (F.col("_dt_min") / 60.0)) > F.lit(float(g["max_plausible_speed_kmph"])))
    seg = (
        seg.withColumn("_gap", has_prev & ~within_gap)
        .withColumn("_jump", jump)
        .withColumn("_valid_seg", within_gap & ~jump)
    )
    moving = F.col("speed_kmph") > F.lit(float(g["moving_speed_threshold_kmph"]))

    def count_if(cond: Column) -> Column:
        return F.sum(F.when(F.coalesce(cond, F.lit(False)), 1).otherwise(0)).cast("long")

    return seg.groupBy("vehicle_id", "event_date").agg(
        F.count("*").alias("record_count"),
        F.round(F.avg("speed_kmph"), 2).alias("avg_speed_kmph"),
        F.max("speed_kmph").alias("max_speed_kmph"),
        F.round(F.avg("engine_temp_c"), 2).alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.min("fuel_level_pct").alias("min_fuel_level_pct"),
        F.round(F.avg("battery_voltage_v"), 3).alias("avg_battery_voltage_v"),
        F.min("battery_voltage_v").alias("min_battery_voltage_v"),
        F.round(F.coalesce(F.sum(F.when(F.col("_valid_seg"), F.col("_dist_km"))), F.lit(0.0)), 3).alias("distance_km"),
        F.round(
            F.coalesce(F.sum(F.when(F.col("_valid_seg") & moving, F.col("_dt_min"))), F.lit(0.0)), 2
        ).alias("moving_minutes"),
        count_if(F.col("_gap")).alias("data_gap_segments"),
        count_if(F.col("_jump")).alias("gps_jump_segments"),
        count_if(F.col("engine_temp_c") >= F.lit(float(g["temperature_warning_c"]))).alias("high_temp_readings"),
        count_if(F.col("fuel_level_pct") < F.lit(float(g["low_fuel_pct"]))).alias("low_fuel_readings"),
        count_if(F.col("battery_voltage_v") < F.lit(float(g["low_battery_v"]))).alias("low_battery_readings"),
        count_if(F.col("is_late_arrival")).alias("late_arrival_records"),
        F.min("event_ts").alias("first_event_ts"),
        F.max("event_ts").alias("last_event_ts"),
    )


def build_temperature_alerts(silver: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """One alert row per reading at/above the warning threshold; CRITICAL at/above the critical threshold."""
    g = cfg["gold"]
    warn, crit = float(g["temperature_warning_c"]), float(g["temperature_critical_c"])
    severity = F.when(F.col("engine_temp_c") >= crit, "CRITICAL").otherwise("WARNING")
    return (
        silver.filter(F.col("engine_temp_c") >= warn)
        .withColumn("severity", severity)
        .withColumn("threshold_c", F.when(F.col("severity") == "CRITICAL", F.lit(crit)).otherwise(F.lit(warn)))
        .withColumn(
            "alert_id",
            F.sha2(F.concat_ws("|", F.col("vehicle_id"), F.col("event_ts").cast("string"), F.col("severity")), 256),
        )
        .select(
            "alert_id",
            "vehicle_id",
            "event_ts",
            "event_date",
            "engine_temp_c",
            "severity",
            "threshold_c",
            "speed_kmph",
            "latitude",
            "longitude",
        )
    )


def build_vehicle_kpis(silver: DataFrame, daily: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Lifetime KPIs per vehicle."""
    g = cfg["gold"]
    warn, crit = float(g["temperature_warning_c"]), float(g["temperature_critical_c"])
    base = silver.groupBy("vehicle_id").agg(
        F.count("*").alias("total_records"),
        F.countDistinct("event_date").alias("active_days"),
        F.min("event_ts").alias("first_seen"),
        F.max("event_ts").alias("last_seen"),
        F.round(F.avg("speed_kmph"), 2).alias("avg_speed_kmph"),
        F.max("speed_kmph").alias("max_speed_kmph"),
        F.round(F.avg("engine_temp_c"), 2).alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.min("fuel_level_pct").alias("min_fuel_level_pct"),
        F.min("battery_voltage_v").alias("min_battery_voltage_v"),
        F.sum(F.when((F.col("engine_temp_c") >= warn) & (F.col("engine_temp_c") < crit), 1).otherwise(0)).cast("long").alias("warning_temp_readings"),
        F.sum(F.when(F.col("engine_temp_c") >= crit, 1).otherwise(0)).cast("long").alias("critical_temp_readings"),
        F.sum(F.when(F.col("fuel_level_pct") < float(g["low_fuel_pct"]), 1).otherwise(0)).cast("long").alias("low_fuel_readings"),
        F.sum(F.when(F.col("battery_voltage_v") < float(g["low_battery_v"]), 1).otherwise(0)).cast("long").alias("low_battery_readings"),
    )
    usage = daily.groupBy("vehicle_id").agg(
        F.round(F.sum("distance_km"), 3).alias("total_distance_km"),
        F.round(F.sum("moving_minutes"), 2).alias("total_moving_minutes"),
    )
    return (
        base.join(usage, "vehicle_id", "left")
        .withColumn(
            "temp_alert_rate_per_1000",
            F.round((F.col("warning_temp_readings") + F.col("critical_temp_readings")) * 1000.0 / F.col("total_records"), 2),
        )
        .withColumn("avg_distance_km_per_active_day", F.round(F.col("total_distance_km") / F.col("active_days"), 3))
    )


def run_gold(spark: SparkSession, cfg: dict[str, Any], affected_dates: list[str]) -> dict[str, Any]:
    """Recompute Gold for the affected event dates (+ KPIs). Safe to call repeatedly."""
    fmt = cfg["storage"]["format"]
    silver_path = table_path(cfg, "silver")
    if not affected_dates or not storage.table_exists(spark, silver_path, fmt):
        log.info("gold: nothing to do")
        return {"dates_recomputed": 0, "daily_rows": 0, "alerts": 0, "vehicles": 0}

    silver_all = storage.read_table(spark, silver_path, fmt)
    silver_dates = silver_all.filter(F.col("event_date").cast("string").isin(affected_dates)).cache()

    daily = build_daily_summary(silver_dates, cfg).cache()
    alerts = build_temperature_alerts(silver_dates, cfg).cache()
    daily_rows, alert_rows = daily.count(), alerts.count()

    storage.replace_partitions(spark, daily, table_path(cfg, "gold_daily_summary"), fmt, "event_date", affected_dates)
    storage.replace_partitions(spark, alerts, table_path(cfg, "gold_temperature_alerts"), fmt, "event_date", affected_dates)

    daily_all = storage.read_table(spark, table_path(cfg, "gold_daily_summary"), fmt)
    kpis = build_vehicle_kpis(silver_all, daily_all, cfg).cache()
    vehicles = kpis.count()
    storage.overwrite_table(kpis, table_path(cfg, "gold_vehicle_kpis"), fmt)

    for df in (silver_dates, daily, alerts, kpis):
        df.unpersist()
    result = {
        "dates_recomputed": len(affected_dates),
        "daily_rows": daily_rows,
        "alerts": alert_rows,
        "vehicles": vehicles,
    }
    log.info("gold complete", extra={"ctx": result})
    return result
