"""Run the analytical SQL file against the local Gold/Silver tables.

    python -m src.utils.sql_runner sql/gold_queries.sql

Each query in the file starts with a comment line ``-- name: <title>``.
Tables are exposed as temp views: silver_telemetry, silver_rejects, gold_vehicle_daily_summary,
gold_temperature_alerts, gold_vehicle_kpis.
"""
from __future__ import annotations

import re
import sys

from src.utils import storage
from src.utils.config import load_config, table_path
from src.utils.exceptions import PipelineError
from src.utils.spark_session import get_spark

VIEWS = {
    "silver_telemetry": "silver",
    "silver_rejects": "silver_rejects",
    "gold_vehicle_daily_summary": "gold_daily_summary",
    "gold_temperature_alerts": "gold_temperature_alerts",
    "gold_vehicle_kpis": "gold_vehicle_kpis",
}


def split_queries(sql_text: str) -> list[tuple[str, str]]:
    """Split a SQL file into (name, statement) pairs using ``-- name:`` markers."""
    parts = re.split(r"^--\s*name:\s*(.+)$", sql_text, flags=re.MULTILINE)
    queries = []
    for i in range(1, len(parts), 2):
        statement = parts[i + 1].strip().rstrip(";").strip()
        if statement:
            queries.append((parts[i].strip(), statement))
    return queries


def main(sql_file: str, config_path: str | None = None) -> int:
    cfg = load_config(config_path)
    spark = get_spark(cfg)
    fmt = cfg["storage"]["format"]
    for view, key in VIEWS.items():
        path = table_path(cfg, key)
        if storage.table_exists(spark, path, fmt):
            storage.read_table(spark, path, fmt).createOrReplaceTempView(view)
    with open(sql_file, encoding="utf-8") as fh:
        queries = split_queries(fh.read())
    if not queries:
        raise PipelineError(f"No '-- name:' queries found in {sql_file}")
    for name, statement in queries:
        print(f"\n=== {name} ===")
        spark.sql(statement).show(20, truncate=False)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "sql/gold_queries.sql"))
