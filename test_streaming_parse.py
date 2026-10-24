import pytest

from src.ingestion.eventhub_stream import add_bronze_metadata, kafka_options, parse_event_payload
from src.utils.exceptions import ConfigError


def test_parse_event_payload_good_and_malformed(spark):
    good = '{"vehicle_id":"VH-0001","timestamp":"2026-01-05T08:00:00Z","speed_kmph":50.5,"engine_temp_c":90,"fuel_level_pct":60,"battery_voltage_v":13.8,"latitude":18.5,"longitude":73.8}'
    df = spark.createDataFrame([(good,), ('{"vehicle_id": "VH-0002", ',)], "value string")
    rows = {r["_corrupt_record"] is not None: r for r in parse_event_payload(df).collect()}
    assert rows[False]["speed_kmph"] == "50.5" and rows[False]["vehicle_id"] == "VH-0001"
    assert rows[True]["_corrupt_record"].startswith('{"vehicle_id": "VH-0002"')


def test_bronze_metadata_columns(spark):
    df = spark.createDataFrame([("x",)], "vehicle_id string")
    out = add_bronze_metadata(df, "stream_1")
    assert {"_batch_id", "_ingested_at", "_row_id", "_source_file", "_ingest_date"} <= set(out.columns)


def test_kafka_options_require_env(monkeypatch, cfg):
    for var in ("EVENTHUB_NAMESPACE", "EVENTHUB_NAME", "EVENTHUB_CONNECTION_STRING"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ConfigError):
        kafka_options(cfg)
    monkeypatch.setenv("EVENTHUB_NAMESPACE", "ns")
    monkeypatch.setenv("EVENTHUB_NAME", "hub")
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://x")
    opts = kafka_options(cfg)
    assert opts["kafka.bootstrap.servers"] == "ns.servicebus.windows.net:9093"
    assert "$ConnectionString" in opts["kafka.sasl.jaas.config"]
