"""OPTIONAL stage: stream telemetry from Azure Event Hubs into Bronze -> Silver -> Gold.

STATUS
  * ``parse_event_payload`` is pure DataFrame logic and IS unit-tested locally (tests/test_streaming_parse.py).
  * ``start_stream`` needs a real Event Hubs namespace (Kafka endpoint) and the spark-sql-kafka connector
    (built into Databricks). It has NOT been executed in this repository - treat it as a reviewed template.

How it connects
  Event Hubs exposes a Kafka-compatible endpoint on port 9093. Spark reads it with the Kafka source using SASL
  PLAIN where the username is the literal string ``$ConnectionString`` and the password is the Event Hubs
  connection string. The connection string comes from the environment (EVENTHUB_CONNECTION_STRING), which on
  Databricks should be populated from a secret scope backed by Azure Key Vault - never from source code.

Idempotency
  Each micro-batch is processed by ``foreachBatch`` with a deterministic batch id (``stream_<epoch>``). Silver
  upserts on (vehicle_id, event_ts), so a replayed micro-batch after a failure does not create duplicates.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.transformations.gold import run_gold
from src.transformations.silver import process_batch
from src.utils import storage
from src.utils.config import table_path
from src.utils.exceptions import ConfigError
from src.utils.logging_utils import get_logger
from src.validation.schema import JSON_READ_SCHEMA, RAW_COLUMNS

log = get_logger(__name__)


def parse_event_payload(df: DataFrame, value_col: str = "value") -> DataFrame:
    """Turn a DataFrame with a JSON ``value`` column into Bronze-shaped raw string columns.

    Malformed JSON never raises: the original text is kept in ``_corrupt_record`` and the record is quarantined
    by Silver's ``malformed_record`` rule.
    """
    text = F.col(value_col).cast("string")
    parsed = F.from_json(
        text, JSON_READ_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
    )
    return df.select(parsed.alias("p")).select(*[F.col(f"p.{c}") for c in [*RAW_COLUMNS, "_corrupt_record"]])


def add_bronze_metadata(df: DataFrame, batch_id: str, source: str = "eventhubs") -> DataFrame:
    """Add the same lineage columns the batch Bronze step adds."""
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return (
        df.withColumn("_source_format", F.lit("json"))
        .withColumn("_source_file", F.lit(source))
        .withColumn("_row_id", F.monotonically_increasing_id())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_ingest_date", F.to_date("_ingested_at").cast("string"))
    )


def kafka_options(cfg: dict[str, Any]) -> dict[str, str]:
    """Kafka-protocol options for Event Hubs. Reads secrets from the environment only."""
    namespace = os.environ.get("EVENTHUB_NAMESPACE")
    hub = os.environ.get("EVENTHUB_NAME")
    conn = os.environ.get("EVENTHUB_CONNECTION_STRING")
    if not (namespace and hub and conn):
        raise ConfigError("Set EVENTHUB_NAMESPACE, EVENTHUB_NAME and EVENTHUB_CONNECTION_STRING (see .env.example)")
    prefix = "kafkashaded." if cfg["streaming"].get("use_databricks_shaded_kafka", True) else ""
    jaas = (
        f"{prefix}org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{conn}";'
    )
    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": hub,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": jaas,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


def start_stream(spark: SparkSession, cfg: dict[str, Any]):  # pragma: no cover - needs Azure
    """Start the streaming query. Returns the StreamingQuery handle."""
    fmt = cfg["storage"]["format"]
    stream_cfg = cfg["streaming"]

    def handle_micro_batch(batch_df: DataFrame, epoch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        batch_id = f"stream_{epoch_id:08d}_{uuid.uuid4().hex[:4]}"
        bronze = add_bronze_metadata(parse_event_payload(batch_df), batch_id).cache()
        storage.append(bronze, table_path(cfg, "bronze"), fmt, partition_by=["_ingest_date"])
        metrics = process_batch(spark, cfg, bronze, batch_id)
        run_gold(spark, cfg, metrics["affected_dates"])
        bronze.unpersist()

    source = spark.readStream.format("kafka").options(**kafka_options(cfg)).load()
    writer = (
        source.writeStream.foreachBatch(handle_micro_batch)
        .option("checkpointLocation", stream_cfg["checkpoint_path"])
    )
    if stream_cfg.get("trigger_available_now", True):
        writer = writer.trigger(availableNow=True)
    return writer.start()
