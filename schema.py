"""Schemas and the input contract for raw telemetry files."""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

# Columns every source file must provide (CSV header names / JSON keys).
RAW_COLUMNS: list[str] = [
    "vehicle_id",
    "timestamp",
    "speed_kmph",
    "engine_temp_c",
    "fuel_level_pct",
    "battery_voltage_v",
    "latitude",
    "longitude",
]

# Bronze keeps everything as text so that bad values are preserved exactly as received.
RAW_STRING_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

# JSON reader schema = raw columns + Spark's corrupt-record capture column.
JSON_READ_SCHEMA = StructType(
    [*RAW_STRING_SCHEMA.fields, StructField("_corrupt_record", StringType(), True)]
)

# Lineage metadata added by the Bronze step.
BRONZE_META_COLUMNS: list[str] = [
    "_batch_id",
    "_ingested_at",
    "_source_file",
    "_source_format",
    "_row_id",
    "_ingest_date",
]

# Typed numeric measurement columns in Silver.
NUMERIC_FIELDS: list[str] = [
    "speed_kmph",
    "engine_temp_c",
    "fuel_level_pct",
    "battery_voltage_v",
    "latitude",
    "longitude",
]
