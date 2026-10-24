"""Shared pytest fixtures: one local SparkSession per test session and isolated temp lakehouses."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.utils.config import load_config
from src.utils.spark_session import get_spark
from src.validation.schema import RAW_COLUMNS

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "pipeline_config.yaml"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    cfg = load_config(CONFIG_PATH, {"spark": {"shuffle_partitions": 2}})
    session = get_spark(cfg)
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path: Path) -> dict:
    """Config whose lakehouse, landing zone and reports live in a per-test temp folder."""
    return load_config(
        CONFIG_PATH,
        {
            "storage": {
                "base_path": str(tmp_path / "lake"),
                "input_path": str(tmp_path / "landing"),
                "reports_path": str(tmp_path / "reports"),
            },
            "spark": {"shuffle_partitions": 2},
        },
    )


GOOD_ROW = {
    "vehicle_id": "VH-0001",
    "timestamp": "2026-01-05T08:00:00Z",
    "speed_kmph": "50",
    "engine_temp_c": "90",
    "fuel_level_pct": "60",
    "battery_voltage_v": "13.8",
    "latitude": "18.5",
    "longitude": "73.8",
}


@pytest.fixture
def make_bronze(spark: SparkSession):
    """Factory: list of dict overrides -> Bronze-shaped DataFrame (all strings + lineage columns)."""

    def _make(rows: list[dict], batch_id: str = "b1", ingested_at: str = "2026-01-10 00:00:00") -> DataFrame:
        records = []
        for i, override in enumerate(rows):
            row = {**GOOD_ROW, **override}
            corrupt = row.pop("_corrupt_record", None)
            records.append(
                tuple(row.get(c) for c in RAW_COLUMNS)
                + (corrupt, batch_id, datetime.fromisoformat(ingested_at), "file_a.csv", "csv", i, "2026-01-10")
            )
        cols = [*RAW_COLUMNS, "_corrupt_record", "_batch_id", "_ingested_at", "_source_file", "_source_format", "_row_id", "_ingest_date"]
        df = spark.createDataFrame(records, schema=", ".join(f"{c} string" for c in RAW_COLUMNS) + ", _corrupt_record string, _batch_id string, _ingested_at timestamp, _source_file string, _source_format string, _row_id long, _ingest_date string")
        return df.select(*cols)

    return _make
