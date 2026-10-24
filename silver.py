"""Silver layer: schema enforcement, validation, quarantine, de-duplication and idempotent upsert.

Flow for one Bronze batch:
    Bronze (strings) -> to_typed -> apply_rules -> split valid / rejected
        valid    -> in-batch dedupe -> late-arrival flag -> upsert into Silver (key = vehicle_id + event_ts)
        rejected -> Silver rejects table (raw values + failed rule names) for investigation / replay
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.transformations.dedupe import deduplicate_latest
from src.utils import storage
from src.utils.config import table_path
from src.utils.logging_utils import get_logger
from src.validation.quality_rules import apply_rules, build_rules
from src.validation.schema import NUMERIC_FIELDS, RAW_COLUMNS

log = get_logger(__name__)

KEYS = ["vehicle_id", "event_ts"]
ORDER_COLS = ["_ingested_at", "_source_file", "_row_id"]  # latest ingested copy wins; deterministic ties
META_COLS = ["_batch_id", "_ingested_at", "_source_file", "_source_format", "_row_id"]


def _clean(col_name: str):
    """Trim text and turn empty strings into NULL."""
    return F.when(F.trim(F.col(col_name)) == "", F.lit(None).cast("string")).otherwise(F.trim(F.col(col_name)))


def to_typed(bronze: DataFrame) -> DataFrame:
    """Keep raw text as ``raw_<col>`` and add typed columns (``event_ts`` + numeric measurements).

    Unparseable values become NULL here; the validation rules then distinguish "missing" from "invalid type"
    by comparing against the raw text.
    """
    raw_cols = [F.col(c).alias(f"raw_{c}") for c in RAW_COLUMNS]
    typed = [
        F.upper(_clean("vehicle_id")).alias("vehicle_id"),
        _clean("timestamp").cast("timestamp").alias("event_ts"),
    ]
    for field in NUMERIC_FIELDS:
        value = _clean(field).cast("double")
        typed.append(F.when(F.isnan(value), F.lit(None).cast("double")).otherwise(value).alias(field))
    return bronze.select(*[F.col(c) for c in META_COLS], F.col("_corrupt_record"), *raw_cols, *typed)


def _candidates(valid: DataFrame) -> DataFrame:
    """Project valid rows to the Silver column set (adds event_date and a payload hash)."""
    payload = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in NUMERIC_FIELDS]), 256)
    return valid.select(
        "vehicle_id",
        "event_ts",
        F.to_date("event_ts").alias("event_date"),
        *NUMERIC_FIELDS,
        "_dq_warnings",
        payload.alias("_payload_hash"),
        *META_COLS,
    )


def _rejects(rejected: DataFrame) -> DataFrame:
    """Quarantine projection: raw values as received + why they failed."""
    return rejected.select(
        *META_COLS,
        "_corrupt_record",
        *[f"raw_{c}" for c in RAW_COLUMNS],
        F.col("_dq_errors").alias("_dq_errors"),
        F.concat_ws(",", "_dq_errors").alias("_dq_error_list"),
    )


def _count_by_rule(df: DataFrame, col: str) -> dict[str, int]:
    rows = df.select(F.explode(col).alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: int(r["count"]) for r in rows}


def process_batch(
    spark: SparkSession, cfg: dict[str, Any], bronze_df: DataFrame, batch_id: str
) -> dict[str, Any]:
    """Validate, de-duplicate and merge one Bronze batch into Silver. Returns data quality metrics."""
    fmt = cfg["storage"]["format"]
    silver_path = table_path(cfg, "silver")
    rejects_path = table_path(cfg, "silver_rejects")

    validated = apply_rules(to_typed(bronze_df), build_rules(cfg)).cache()
    bronze_rows = validated.count()
    is_valid = F.size("_dq_errors") == 0
    valid = validated.filter(is_valid)
    rejected = validated.filter(~is_valid)

    rejected_by_rule = _count_by_rule(rejected, "_dq_errors")
    warnings_by_rule = _count_by_rule(valid, "_dq_warnings")
    rows_with_warnings = valid.filter(F.size("_dq_warnings") > 0).count()
    rejected_rows = rejected.count()

    candidates = _candidates(valid).cache()
    valid_rows = candidates.count()
    unique_keys = candidates.select(*KEYS).distinct().count()
    conflicting = (
        candidates.groupBy(*KEYS)
        .agg(F.countDistinct("_payload_hash").alias("n_payloads"))
        .filter(F.col("n_payloads") > 1)
        .count()
    )
    deduped = deduplicate_latest(candidates, KEYS, ORDER_COLS)

    # ---- late-arrival + already-seen detection against what Silver already holds ----------------
    if storage.table_exists(spark, silver_path, fmt):
        existing = storage.read_table(spark, silver_path, fmt)
        watermark = existing.groupBy("vehicle_id").agg(F.max("event_ts").alias("_watermark"))
        known = existing.select(*KEYS).distinct().withColumn("_exists", F.lit(True))
        enriched = deduped.join(watermark, "vehicle_id", "left").join(known, KEYS, "left")
        enriched = enriched.withColumn("_matched_existing", F.col("_exists").isNotNull()).withColumn(
            "is_late_arrival",
            (~F.col("_matched_existing")) & F.col("_watermark").isNotNull() & (F.col("event_ts") < F.col("_watermark")),
        )
    else:
        enriched = deduped.withColumn("_matched_existing", F.lit(False)).withColumn(
            "is_late_arrival", F.lit(False)
        )
    enriched = enriched.cache()
    late_rows = enriched.filter("is_late_arrival").count()
    matched_existing = enriched.filter("_matched_existing").count()

    final = (
        enriched.drop("_watermark", "_exists", "_matched_existing")
        .withColumn("_processed_at", F.current_timestamp())
        .cache()
    )
    silver_rows_written = final.count()
    affected_dates = sorted({str(r[0]) for r in final.select("event_date").distinct().collect()})

    storage.upsert(spark, final, silver_path, fmt, KEYS, ORDER_COLS, "event_date")
    if rejected_rows:
        storage.replace_partitions(spark, _rejects(rejected), rejects_path, fmt, "_batch_id", [batch_id])

    silver_rows_after = storage.read_table(spark, silver_path, fmt).count() if affected_dates else 0
    for df in (validated, candidates, enriched, final):
        df.unpersist()

    metrics = {
        "batch_id": batch_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "rows_with_warnings": rows_with_warnings,
        "rejected_by_rule": rejected_by_rule,
        "warnings_by_rule": warnings_by_rule,
        "in_batch_duplicate_rows_removed": valid_rows - unique_keys,
        "conflicting_duplicate_keys": conflicting,
        "rows_matching_existing_silver": matched_existing,
        "late_arrival_rows": late_rows,
        "silver_rows_written": silver_rows_written,
        "silver_rows_after": silver_rows_after,
        "affected_dates": affected_dates,
    }
    log.info("silver batch complete", extra={"ctx": {k: v for k, v in metrics.items() if not isinstance(v, (dict, list))}})
    return metrics


def run_silver_for_batch(spark: SparkSession, cfg: dict[str, Any], batch_id: str) -> dict[str, Any]:
    """Read one batch from Bronze by ``_batch_id`` and process it."""
    bronze = storage.read_table(spark, table_path(cfg, "bronze"), cfg["storage"]["format"]).filter(
        F.col("_batch_id") == batch_id
    )
    return process_batch(spark, cfg, bronze, batch_id)
