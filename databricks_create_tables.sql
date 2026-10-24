-- Register the Delta tables written by the pipeline so BI tools and SQL users can query them by name.
-- Replace <storage_account>. Run in a Databricks SQL warehouse or notebook. TEMPLATE - not executed here.

CREATE SCHEMA IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.silver_telemetry
  USING DELTA LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/telemetry/silver/telemetry';

CREATE TABLE IF NOT EXISTS telemetry.silver_rejects
  USING DELTA LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/telemetry/silver/telemetry_rejects';

CREATE TABLE IF NOT EXISTS telemetry.gold_vehicle_daily_summary
  USING DELTA LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/telemetry/gold/vehicle_daily_summary';

CREATE TABLE IF NOT EXISTS telemetry.gold_temperature_alerts
  USING DELTA LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/telemetry/gold/temperature_alerts';

CREATE TABLE IF NOT EXISTS telemetry.gold_vehicle_kpis
  USING DELTA LOCATION 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/telemetry/gold/vehicle_kpis';

-- Maintenance (schedule weekly): compact small files and co-locate by the most common filter columns.
OPTIMIZE telemetry.silver_telemetry ZORDER BY (vehicle_id, event_ts);
