"""Pipeline orchestrator (CLI): Bronze -> Silver -> Gold with restartable, idempotent state.

Examples
    python -m src.pipeline                                   # all stages, all new files
    python -m src.pipeline --pattern "telemetry_batch_001.csv"   # ingest only matching files
    python -m src.pipeline --stage gold                      # run one stage (resumes pending batches)
    python -m src.pipeline --format delta                    # use Delta Lake instead of Parquet
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from src.ingestion.bronze_ingest import ingest_to_bronze
from src.transformations.gold import run_gold
from src.transformations.silver import run_silver_for_batch
from src.utils import storage
from src.utils.config import load_config, state_file_path, table_path
from src.utils.exceptions import PipelineError
from src.utils.logging_utils import configure_logging, get_logger
from src.utils.spark_session import get_spark
from src.utils.state import StateStore
from src.validation.quality_report import (
    render_dq_markdown,
    render_summary_markdown,
    write_report,
)

log = get_logger(__name__)
STAGE_CHOICES = ("all", "bronze", "silver", "gold")
TABLE_KEYS = (
    "bronze",
    "silver",
    "silver_rejects",
    "gold_daily_summary",
    "gold_temperature_alerts",
    "gold_vehicle_kpis",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _table_counts(spark: SparkSession, cfg: dict[str, Any]) -> dict[str, int]:
    fmt = cfg["storage"]["format"]
    counts: dict[str, int] = {}
    for key in TABLE_KEYS:
        path = table_path(cfg, key)
        counts[key] = storage.read_table(spark, path, fmt).count() if storage.table_exists(spark, path, fmt) else 0
    return counts


def run_pipeline(
    cfg: dict[str, Any],
    stage: str = "all",
    patterns: list[str] | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Run the requested stage(s) and return (and persist) the execution summary.

    Raises:
        PipelineError: when any stage fails (the summary is still written with status ``failed``).
    """
    if stage not in STAGE_CHOICES:
        raise PipelineError(f"Unknown stage {stage!r}; choose from {STAGE_CHOICES}")

    run_id = f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    started = time.time()
    summary: dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "storage_format": cfg["storage"]["format"],
        "started_at": _now(),
        "stages": {},
        "dq_reports": [],
    }
    spark = spark or get_spark(cfg)
    state = StateStore(state_file_path(cfg))
    reports_dir = cfg["storage"]["reports_path"]
    do = lambda name: stage in ("all", name)  # noqa: E731

    try:
        if do("bronze"):
            t0 = time.time()
            res = ingest_to_bronze(spark, cfg, state, patterns=patterns)
            summary["stages"]["bronze"] = {
                "batch_id": res.batch_id,
                "files_ingested": res.files_ingested,
                "files_skipped_already_ingested": res.files_skipped,
                "rows_ingested": res.rows_ingested,
                "seconds": round(time.time() - t0, 2),
            }

        if do("silver"):
            t0 = time.time()
            processed = []
            for batch_id in state.pending_batches("silver"):
                metrics = run_silver_for_batch(spark, cfg, batch_id)
                paths = write_report(metrics, reports_dir, f"dq_report_{batch_id}", render_dq_markdown(metrics))
                summary["dq_reports"].append(paths["markdown"])
                state.mark_stage_done(batch_id, "silver", affected_dates=metrics["affected_dates"])
                processed.append(metrics)
            summary["stages"]["silver"] = {
                "batches_processed": len(processed),
                "valid_rows": sum(m["valid_rows"] for m in processed),
                "rejected_rows": sum(m["rejected_rows"] for m in processed),
                "late_arrival_rows": sum(m["late_arrival_rows"] for m in processed),
                "seconds": round(time.time() - t0, 2),
            }

        if do("gold"):
            t0 = time.time()
            pending = state.pending_batches("gold")
            dates = sorted({d for b in pending for d in state.get_batch(b)["affected_dates"]})
            result = run_gold(spark, cfg, dates)
            for batch_id in pending:
                state.mark_stage_done(batch_id, "gold")
            summary["stages"]["gold"] = {
                "batches_processed": len(pending),
                **result,
                "seconds": round(time.time() - t0, 2),
            }

        summary["table_row_counts"] = _table_counts(spark, cfg)
        summary["status"] = "success"
    except Exception as exc:  # noqa: BLE001 - we re-raise as PipelineError after recording the failure
        summary["status"] = "failed"
        summary["error"] = f"{type(exc).__name__}: {exc}"
        log.exception("pipeline failed")
        raise PipelineError(summary["error"]) from exc
    finally:
        summary["finished_at"] = _now()
        summary["duration_seconds"] = round(time.time() - started, 2)
        paths = write_report(summary, reports_dir, f"pipeline_summary_{run_id}", render_summary_markdown(summary))
        summary["summary_report"] = paths["markdown"]
        log.info("pipeline finished", extra={"ctx": {"status": summary["status"], "report": paths["markdown"]}})
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IoT vehicle telemetry medallion pipeline")
    p.add_argument("--config", default=None, help="YAML config path (default config/pipeline_config.yaml)")
    p.add_argument("--stage", choices=STAGE_CHOICES, default="all")
    p.add_argument("--pattern", action="append", help="Only ingest files matching this glob (repeatable)")
    p.add_argument("--format", choices=("parquet", "delta"), help="Override storage.format")
    p.add_argument("--input-path", help="Override storage.input_path")
    p.add_argument("--base-path", help="Override storage.base_path")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    overrides: dict[str, Any] = {"storage": {}}
    if args.format:
        overrides["storage"]["format"] = args.format
    if args.input_path:
        overrides["storage"]["input_path"] = args.input_path
    if args.base_path:
        overrides["storage"]["base_path"] = args.base_path
    try:
        cfg = load_config(args.config, overrides)
        configure_logging(cfg.get("logging", {}).get("level", "INFO"), cfg.get("logging", {}).get("json", True))
        summary = run_pipeline(cfg, stage=args.stage, patterns=args.pattern)
    except PipelineError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"\nPipeline {summary['status']}. Summary: {summary['summary_report']}")
    for path in summary["dq_reports"]:
        print(f"Data quality report: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
