"""Delta Lake smoke test. Needs delta-spark + internet (jars from Maven). Opt in with RUN_DELTA_TESTS=1."""
import os

import pytest

pytestmark = [
    pytest.mark.delta,
    pytest.mark.skipif(os.environ.get("RUN_DELTA_TESTS") != "1", reason="set RUN_DELTA_TESTS=1 to run Delta tests"),
]


def test_pipeline_runs_on_delta(tmp_path):
    from pyspark.sql import SparkSession

    from src.ingestion.generate_synthetic_data import generate
    from src.pipeline import run_pipeline
    from src.utils.config import load_config
    from src.utils.spark_session import get_spark
    from tests.conftest import CONFIG_PATH

    SparkSession.getActiveSession() and SparkSession.getActiveSession().stop()
    landing = tmp_path / "landing"
    expected = generate(landing)
    cfg = load_config(
        CONFIG_PATH,
        {"storage": {"format": "delta", "base_path": str(tmp_path / "lake"), "input_path": str(landing), "reports_path": str(tmp_path / "rep")}},
    )
    spark = get_spark(cfg)
    run = run_pipeline(cfg, spark=spark)
    assert run["status"] == "success"
    assert run["table_row_counts"]["silver"] == expected["after_batch_2"]["silver_rows"]
    run2 = run_pipeline(cfg, spark=spark)  # idempotent
    assert run2["table_row_counts"] == run["table_row_counts"]
