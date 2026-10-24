"""Declarative data quality rules.

Every rule is a (name, severity, failed-expression) triple. ``apply_rules`` evaluates all rules in one pass and
adds two array columns:
    _dq_errors   - names of failed *error* rules  -> record is rejected (quarantined)
    _dq_warnings - names of failed *warning* rules -> record is kept, but flagged

Expected input columns (produced by ``silver.to_typed``):
    raw_<col> (string as received), typed columns (vehicle_id, event_ts, speed_kmph, ...), _corrupt_record.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.validation.schema import NUMERIC_FIELDS


@dataclass(frozen=True)
class Rule:
    """A single data quality rule. ``failed`` is True when the record VIOLATES the rule."""

    name: str
    severity: str  # "error" | "warning"
    failed: Column
    description: str


def _blank(raw_col: str) -> Column:
    return F.trim(F.coalesce(F.col(raw_col), F.lit(""))) == ""


def build_rules(cfg: dict[str, Any]) -> list[Rule]:
    """Create the rule list from configuration (ranges, required/optional fields, ID pattern...)."""
    quality = cfg["quality"]
    required = set(quality["required_fields"])
    ranges = quality["ranges"]
    max_future = int(quality["max_future_minutes"])

    rules: list[Rule] = [
        Rule("malformed_record", "error", F.col("_corrupt_record").isNotNull(), "Line could not be parsed"),
        Rule("missing_vehicle_id", "error", _blank("raw_vehicle_id"), "vehicle_id is null/blank"),
        Rule(
            "invalid_vehicle_id_format",
            "error",
            F.col("vehicle_id").isNotNull() & ~F.col("vehicle_id").rlike(quality["vehicle_id_pattern"]),
            "vehicle_id does not match the configured pattern",
        ),
        Rule("missing_timestamp", "error", _blank("raw_timestamp"), "timestamp is null/blank"),
        Rule(
            "invalid_timestamp",
            "error",
            ~_blank("raw_timestamp") & F.col("event_ts").isNull(),
            "timestamp present but not parseable",
        ),
        Rule(
            "future_timestamp",
            "error",
            F.col("event_ts") > (F.current_timestamp() + F.expr(f"INTERVAL {max_future} MINUTES")),
            "event time is in the future (clock fault)",
        ),
    ]

    for field in NUMERIC_FIELDS:
        lo, hi = ranges[field]
        raw = f"raw_{field}"
        rules.append(
            Rule(
                f"missing_{field}",
                "error" if field in required else "warning",
                _blank(raw),
                f"{field} is null/blank ({'required' if field in required else 'optional'})",
            )
        )
        rules.append(
            Rule(f"invalid_type_{field}", "error", ~_blank(raw) & F.col(field).isNull(), f"{field} is not numeric")
        )
        rules.append(
            Rule(
                f"out_of_range_{field}",
                "error",
                F.col(field).isNotNull() & ((F.col(field) < F.lit(lo)) | (F.col(field) > F.lit(hi))),
                f"{field} outside [{lo}, {hi}]",
            )
        )
    return rules


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add ``_dq_errors`` and ``_dq_warnings`` array columns evaluating every rule once."""

    def names(severity: str) -> Column:
        exprs = [
            F.when(F.coalesce(r.failed, F.lit(False)), F.lit(r.name))
            for r in rules
            if r.severity == severity
        ]
        arr = F.array(*exprs).cast("array<string>") if exprs else F.array().cast("array<string>")
        return F.filter(arr, lambda x: x.isNotNull())

    return df.withColumn("_dq_errors", names("error")).withColumn("_dq_warnings", names("warning"))
