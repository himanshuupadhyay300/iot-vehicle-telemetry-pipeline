"""SparkSession factory: local development, Delta-enabled local, or the Databricks-provided session."""
from __future__ import annotations

import os
from typing import Any

from pyspark.sql import SparkSession

from src.utils.exceptions import ConfigError
from src.utils.logging_utils import get_logger

log = get_logger(__name__)


def _on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: dict[str, Any]) -> SparkSession:
    """Create (or reuse) a SparkSession configured from ``cfg``.

    * On Databricks the cluster session is reused (Delta is built in).
    * Locally with ``storage.format: delta`` the ``delta-spark`` pip package wires up the jars. This needs
      internet access the first time (jars are downloaded from Maven Central).
    """
    spark_cfg = cfg["spark"]
    builder = (
        SparkSession.builder.appName(spark_cfg.get("app_name", "iot-vehicle-telemetry"))
        .config("spark.sql.session.timeZone", spark_cfg.get("timezone", "UTC"))
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 4)))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Make casts/parsing return NULL instead of raising, so bad records flow to quarantine, not crash.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if not _on_databricks():
        builder = builder.master(spark_cfg.get("master", "local[2]"))

    if cfg["storage"]["format"] == "delta" and not _on_databricks():
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:  # pragma: no cover - depends on optional dependency
            raise ConfigError("storage.format=delta requires the 'delta-spark' package") from exc
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    log.info("spark session ready", extra={"ctx": {"version": spark.version, "format": cfg["storage"]["format"]}})
    return spark
