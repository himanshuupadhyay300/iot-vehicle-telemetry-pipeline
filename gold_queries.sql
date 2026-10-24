-- Analytical queries (Spark SQL / Databricks SQL). Run locally with:
--   python -m src.utils.sql_runner sql/gold_queries.sql

-- name: Q1 Fleet daily overview
SELECT event_date,
       COUNT(DISTINCT vehicle_id)          AS active_vehicles,
       SUM(record_count)                   AS readings,
       ROUND(SUM(distance_km), 2)          AS fleet_distance_km,
       ROUND(SUM(moving_minutes), 1)       AS fleet_moving_minutes,
       MAX(max_engine_temp_c)              AS hottest_reading_c
FROM gold_vehicle_daily_summary
GROUP BY event_date
ORDER BY event_date;

-- name: Q2 Top vehicles by distance
SELECT vehicle_id, total_distance_km, total_moving_minutes, avg_speed_kmph, active_days
FROM gold_vehicle_kpis
ORDER BY total_distance_km DESC;

-- name: Q3 Temperature alerts by vehicle and severity
SELECT vehicle_id, severity, COUNT(*) AS alerts, MAX(engine_temp_c) AS peak_temp_c,
       MIN(event_ts) AS first_alert, MAX(event_ts) AS last_alert
FROM gold_temperature_alerts
GROUP BY vehicle_id, severity
ORDER BY vehicle_id, severity;

-- name: Q4 Alert rate per 1000 readings (ranked)
SELECT vehicle_id, total_records, warning_temp_readings, critical_temp_readings, temp_alert_rate_per_1000,
       RANK() OVER (ORDER BY temp_alert_rate_per_1000 DESC) AS risk_rank
FROM gold_vehicle_kpis
ORDER BY risk_rank, vehicle_id;

-- name: Q5 Hourly average speed trend (time series, from Silver)
SELECT vehicle_id,
       date_trunc('hour', event_ts)        AS hour_utc,
       ROUND(AVG(speed_kmph), 2)           AS avg_speed_kmph,
       ROUND(AVG(engine_temp_c), 2)        AS avg_engine_temp_c
FROM silver_telemetry
WHERE vehicle_id = 'VH-0001'
GROUP BY vehicle_id, date_trunc('hour', event_ts)
ORDER BY hour_utc;

-- name: Q6 Rolling 5-reading average engine temperature (VH-0002 around the overheating episode)
SELECT event_ts, engine_temp_c,
       ROUND(AVG(engine_temp_c) OVER (PARTITION BY vehicle_id ORDER BY event_ts
             ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS rolling_5_avg_temp_c
FROM silver_telemetry
WHERE vehicle_id = 'VH-0002' AND event_ts BETWEEN '2026-01-05 08:35:00' AND '2026-01-05 08:55:00'
ORDER BY event_ts;

-- name: Q7 Data quality - rejections by rule (quarantine table)
SELECT rule, COUNT(*) AS rejected_rows
FROM (SELECT explode(_dq_errors) AS rule FROM silver_rejects)
GROUP BY rule
ORDER BY rejected_rows DESC;

-- name: Q8 Late-arrival impact per vehicle/day
SELECT vehicle_id, event_date, record_count, late_arrival_records, data_gap_segments
FROM gold_vehicle_daily_summary
WHERE late_arrival_records > 0 OR data_gap_segments > 0
ORDER BY vehicle_id, event_date;
