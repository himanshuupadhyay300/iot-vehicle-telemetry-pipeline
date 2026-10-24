"""Configuration loading, environment overrides and validation.

Configuration lives in YAML (config/pipeline_config.yaml). Secrets never live there:
they are read from environment variables (see .env.example) or Azure Key Vault.
"""
from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

from src.utils.exceptions import ConfigError

DEFAULT_CONFIG_PATH = Path("config/pipeline_config.yaml")

# environment variable -> path inside the config dict
ENV_OVERRIDES: dict[str, tuple[str, str]] = {
    "TELEMETRY_STORAGE_FORMAT": ("storage", "format"),
    "TELEMETRY_BASE_PATH": ("storage", "base_path"),
    "TELEMETRY_INPUT_PATH": ("storage", "input_path"),
    "TELEMETRY_REPORTS_PATH": ("storage", "reports_path"),
    "TELEMETRY_LOG_LEVEL": ("logging", "level"),
}

SUPPORTED_FORMATS = {"parquet", "delta"}
NUMERIC_FIELDS = (
    "speed_kmph",
    "engine_temp_c",
    "fuel_level_pct",
    "battery_voltage_v",
    "latitude",
    "longitude",
)


def load_config(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load YAML config, apply environment + explicit overrides, validate and return it.

    Args:
        path: YAML file. Defaults to ``config/pipeline_config.yaml``.
        overrides: nested dict deep-merged on top (highest priority), e.g.
            ``{"storage": {"base_path": "/tmp/x"}}``.

    Raises:
        ConfigError: file missing, invalid YAML, or failed validation.
    """
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.is_file():
        raise ConfigError(f"Config file not found: {cfg_path}")
    try:
        with cfg_path.open("r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {cfg_path}: {exc}") from exc
    if not isinstance(cfg, dict):
        raise ConfigError(f"Config root must be a mapping: {cfg_path}")

    for env_name, (section, key) in ENV_OVERRIDES.items():
        value = os.environ.get(env_name)
        if value:
            cfg.setdefault(section, {})[key] = value
    if overrides:
        cfg = _deep_merge(cfg, overrides)

    validate_config(cfg)
    return cfg


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def validate_config(cfg: dict[str, Any]) -> None:
    """Fail fast with a readable message when the configuration is unusable."""
    for section in ("storage", "quality", "gold", "spark"):
        if not isinstance(cfg.get(section), dict):
            raise ConfigError(f"Missing config section: '{section}'")

    storage = cfg["storage"]
    fmt = storage.get("format")
    if fmt not in SUPPORTED_FORMATS:
        raise ConfigError(f"storage.format must be one of {sorted(SUPPORTED_FORMATS)}, got {fmt!r}")
    for key in ("base_path", "input_path", "reports_path"):
        if not storage.get(key):
            raise ConfigError(f"storage.{key} is required")
    tables = storage.get("tables") or {}
    for name in (
        "bronze",
        "silver",
        "silver_rejects",
        "gold_daily_summary",
        "gold_temperature_alerts",
        "gold_vehicle_kpis",
    ):
        if not tables.get(name):
            raise ConfigError(f"storage.tables.{name} is required")

    quality = cfg["quality"]
    required = set(quality.get("required_fields", []))
    optional = set(quality.get("optional_fields", []))
    unknown = (required | optional) - set(NUMERIC_FIELDS)
    if unknown:
        raise ConfigError(f"Unknown fields in quality config: {sorted(unknown)}")
    if required & optional:
        raise ConfigError(f"Fields cannot be both required and optional: {sorted(required & optional)}")
    if (required | optional) != set(NUMERIC_FIELDS):
        missing = set(NUMERIC_FIELDS) - (required | optional)
        raise ConfigError(f"Every numeric field must be required or optional. Missing: {sorted(missing)}")
    ranges = quality.get("ranges", {})
    for field in NUMERIC_FIELDS:
        bounds = ranges.get(field)
        if not (isinstance(bounds, list) and len(bounds) == 2 and bounds[0] < bounds[1]):
            raise ConfigError(f"quality.ranges.{field} must be [min, max] with min < max")

    gold = cfg["gold"]
    if gold["temperature_warning_c"] >= gold["temperature_critical_c"]:
        raise ConfigError("gold.temperature_warning_c must be lower than gold.temperature_critical_c")
    if gold["max_gap_minutes"] <= 0:
        raise ConfigError("gold.max_gap_minutes must be > 0")


def join_path(base: str, *parts: str) -> str:
    """Join paths for both local folders and URI-style locations (abfss://, dbfs:/)."""
    if "://" in base or base.startswith("dbfs:/"):
        return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])
    return str(Path(base, *parts))


def table_path(cfg: dict[str, Any], table_key: str) -> str:
    """Absolute-ish location of a lakehouse table, e.g. table_path(cfg, 'silver')."""
    try:
        rel = cfg["storage"]["tables"][table_key]
    except KeyError as exc:
        raise ConfigError(f"Unknown table key: {table_key}") from exc
    return join_path(cfg["storage"]["base_path"], rel)


def state_file_path(cfg: dict[str, Any]) -> Path:
    """Where the incremental-load state file lives. Must be openable by plain Python."""
    explicit = cfg["storage"].get("state_path")
    base = explicit if explicit else join_path(cfg["storage"]["base_path"], "_state")
    if "://" in str(base) or str(base).startswith("dbfs:/"):
        raise ConfigError(
            "storage.state_path must be a path Python can open (local disk, /dbfs/... FUSE mount or "
            "/Volumes/...), not an object-store URI. Set storage.state_path explicitly."
        )
    return Path(base) / "state.json"


def to_spark_path(path: str) -> str:
    """Translate Databricks FUSE paths (/dbfs/x) to Spark URIs (dbfs:/x). Other paths pass through."""
    if path.startswith("/dbfs/"):
        return "dbfs:/" + path[len("/dbfs/") :]
    return path
