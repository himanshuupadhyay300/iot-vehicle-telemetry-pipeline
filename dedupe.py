"""Deduplication helper shared by the Silver step and the storage layer."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_latest(df: DataFrame, keys: list[str], order_cols: list[str]) -> DataFrame:
    """Keep exactly one row per ``keys``: the one that sorts highest on ``order_cols`` (descending).

    Typical call: ``keys=["vehicle_id", "event_ts"]``, ``order_cols=["_ingested_at", "_source_file", "_row_id"]``
    meaning "the most recently ingested copy of an event wins; ties are broken deterministically".
    """
    if not keys or not order_cols:
        raise ValueError("keys and order_cols must be non-empty")
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
