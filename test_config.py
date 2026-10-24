import pytest

from src.utils.config import join_path, load_config, state_file_path, table_path, to_spark_path
from src.utils.exceptions import ConfigError
from tests.conftest import CONFIG_PATH


def test_default_config_loads():
    cfg = load_config(CONFIG_PATH)
    assert cfg["storage"]["format"] == "parquet"
    assert cfg["gold"]["temperature_warning_c"] < cfg["gold"]["temperature_critical_c"]


def test_env_override(monkeypatch):
    monkeypatch.setenv("TELEMETRY_BASE_PATH", "/tmp/xyz")
    assert load_config(CONFIG_PATH)["storage"]["base_path"] == "/tmp/xyz"


def test_explicit_override_wins_over_env(monkeypatch):
    monkeypatch.setenv("TELEMETRY_STORAGE_FORMAT", "parquet")
    cfg = load_config(CONFIG_PATH, {"storage": {"format": "delta"}})
    assert cfg["storage"]["format"] == "delta"


def test_missing_file():
    with pytest.raises(ConfigError):
        load_config("does/not/exist.yaml")


def test_invalid_format_rejected():
    with pytest.raises(ConfigError, match="storage.format"):
        load_config(CONFIG_PATH, {"storage": {"format": "csv"}})


def test_warning_must_be_below_critical():
    with pytest.raises(ConfigError, match="temperature_warning_c"):
        load_config(CONFIG_PATH, {"gold": {"temperature_warning_c": 120}})


def test_bad_range_rejected():
    with pytest.raises(ConfigError, match="quality.ranges.speed_kmph"):
        load_config(CONFIG_PATH, {"quality": {"ranges": {"speed_kmph": [10, 5]}}})


def test_table_path_and_uri_join():
    cfg = load_config(CONFIG_PATH, {"storage": {"base_path": "abfss://lake@acct.dfs.core.windows.net/tel"}})
    assert table_path(cfg, "silver") == "abfss://lake@acct.dfs.core.windows.net/tel/silver/telemetry"
    assert join_path("a/b", "c") == "a/b/c"


def test_state_path_rejects_object_store_uri():
    cfg = load_config(CONFIG_PATH, {"storage": {"base_path": "abfss://lake@acct.dfs.core.windows.net/tel"}})
    with pytest.raises(ConfigError, match="state_path"):
        state_file_path(cfg)
    cfg = load_config(CONFIG_PATH, {"storage": {"base_path": "abfss://x@y.dfs.core.windows.net/t", "state_path": "/dbfs/mnt/s"}})
    assert str(state_file_path(cfg)) == "/dbfs/mnt/s/state.json"


def test_to_spark_path():
    assert to_spark_path("/dbfs/mnt/a") == "dbfs:/mnt/a"
    assert to_spark_path("data/x.csv") == "data/x.csv"
