"""Bronze layer: land raw CSV / JSON-lines files exactly as received, plus lineage metadata.

Design choices
  * All source columns are stored as STRING so bad values are never lost or silently coerced.
  * Each file is identified by its SHA-256 checksum. A file that was already ingested (even if renamed or
    copied) is skipped -> re-running the pipeline is idempotent at the ingestion boundary.
  * The state file is updated only AFTER the Bronze write succeeds. If the process dies in between, the file
    is re-ingested on the next run; Silver de-duplicates, so the end result is still correct.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils import storage
from src.utils.config import table_path, to_spark_path
from src.utils.exceptions import IngestionError
from src.utils.logging_utils import get_logger
from src.utils.state import StateStore
from src.validation.schema import JSON_READ_SCHEMA, RAW_COLUMNS

log = get_logger(__name__)

CSV_SUFFIXES = {".csv"}
JSON_SUFFIXES = {".json", ".jsonl"}


@dataclass
class BronzeResult:
    """Outcome of one Bronze run."""

    batch_id: str | None
    files_ingested: int
    files_skipped: int
    rows_ingested: int


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    """Streaming SHA-256 of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_files(input_dir: str | Path, patterns: list[str]) -> list[Path]:
    """Sorted list of files in ``input_dir`` matching any glob pattern (non-recursive)."""
    folder = Path(input_dir)
    if not folder.is_dir():
        raise IngestionError(f"Input folder not found: {folder}")
    found: set[Path] = set()
    for pattern in patterns:
        found.update(p for p in folder.glob(pattern) if p.is_file())
    return sorted(found)


def _read_csv(spark: SparkSession, paths: list[Path]) -> DataFrame:
    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "false")
        .csv([to_spark_path(str(p)) for p in paths])
    )
    missing = [c for c in RAW_COLUMNS if c not in df.columns]
    if missing:
        raise IngestionError(f"CSV input is missing required columns {missing} (files: {[p.name for p in paths]})")
    return df.select(*[F.col(c).cast("string") for c in RAW_COLUMNS]).withColumn(
        "_corrupt_record", F.lit(None).cast("string")
    )


def _read_json(spark: SparkSession, paths: list[Path]) -> DataFrame:
    df = (
        spark.read.schema(JSON_READ_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json([to_spark_path(str(p)) for p in paths])
    )
    return df.select(*RAW_COLUMNS, "_corrupt_record")


def ingest_to_bronze(
    spark: SparkSession,
    cfg: dict[str, Any],
    state: StateStore,
    patterns: list[str] | None = None,
    batch_id: str | None = None,
) -> BronzeResult:
    """Ingest every not-yet-seen source file into the Bronze table as one batch."""
    patterns = patterns or cfg["ingestion"]["file_patterns"]
    files = discover_files(cfg["storage"]["input_path"], patterns)

    new_files: list[dict[str, str]] = []
    seen_in_run: set[str] = set()
    skipped = 0
    for path in files:
        digest = file_sha256(path)
        if state.has_file(digest) or digest in seen_in_run:
            skipped += 1
            log.info("skipping already-ingested file", extra={"ctx": {"file": path.name}})
            continue
        seen_in_run.add(digest)
        new_files.append({"path": str(path), "sha256": digest})

    if not new_files:
        log.info("bronze: nothing new to ingest", extra={"ctx": {"files_skipped": skipped}})
        return BronzeResult(None, 0, skipped, 0)

    batch_id = batch_id or f"batch_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    csv_paths = [Path(f["path"]) for f in new_files if Path(f["path"]).suffix.lower() in CSV_SUFFIXES]
    json_paths = [Path(f["path"]) for f in new_files if Path(f["path"]).suffix.lower() in JSON_SUFFIXES]

    frames: list[DataFrame] = []
    if csv_paths:
        frames.append(_read_csv(spark, csv_paths).withColumn("_source_format", F.lit("csv")))
    if json_paths:
        frames.append(_read_json(spark, json_paths).withColumn("_source_format", F.lit("json")))
    if not frames:
        raise IngestionError("No readable CSV/JSON files among new inputs")

    raw = frames[0]
    for extra in frames[1:]:
        raw = raw.unionByName(extra)

    bronze = (
        raw.withColumn("_source_file", F.input_file_name())
        .withColumn("_row_id", F.monotonically_increasing_id())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_ingest_date", F.to_date(F.col("_ingested_at")).cast("string"))
    ).cache()

    rows = bronze.count()
    path = table_path(cfg, "bronze")
    fmt = cfg["storage"]["format"]
    storage.append(bronze, path, fmt, partition_by=["_ingest_date"])
    bronze.unpersist()

    state.register_batch(batch_id, new_files, rows)
    log.info(
        "bronze complete",
        extra={"ctx": {"batch_id": batch_id, "files": len(new_files), "rows": rows, "skipped": skipped}},
    )
    return BronzeResult(batch_id, len(new_files), skipped, rows)
