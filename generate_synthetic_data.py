"""Generate 100% synthetic vehicle telemetry (no real company or customer data).

Creates, in --output (default data/sample):
  telemetry_batch_001.csv    day 1, 08:00-09:59 for 5 vehicles (clean + deliberately dirty rows)
  telemetry_batch_002.json   JSON-lines: day 1 10:00-10:59, day 2 08:00-08:59, LATE rows, cross-batch duplicates
  expected/expected_counts.json  independently computed expectations used by the integration tests
                             (kept in a sub-folder so the landing-zone glob never ingests it)

Injected data problems (all deterministic for a given --seed):
  batch 1: exact duplicates, conflicting duplicates, missing required values, missing optional values,
           out-of-range speed, unparseable timestamp, empty vehicle_id, non-numeric voltage,
           one overheating episode (5 WARNING + 5 CRITICAL readings), 15 readings withheld (arrive late in batch 2)
  batch 2: those 15 late readings, 10 duplicates of batch-1 events, one malformed JSON line
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.validation.schema import RAW_COLUMNS

VEHICLES = ["VH-0001", "VH-0002", "VH-0003", "VH-0004", "VH-0005"]
START_LAT, START_LON = 18.5204, 73.8567  # arbitrary synthetic start area
DAY1 = datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc)
DAY2 = datetime(2026, 1, 6, 8, 0, tzinfo=timezone.utc)


def _simulate(rng: random.Random, vehicle_idx: int, start: datetime, minutes: int, state: dict[str, float]) -> list[dict[str, Any]]:
    """One reading per minute using a simple random-walk driving model."""
    rows = []
    for i in range(minutes):
        state["speed"] = min(110.0, max(0.0, state["speed"] + rng.uniform(-12, 12)))
        state["heading"] += rng.uniform(-0.15, 0.15)
        km = state["speed"] / 60.0
        state["lat"] += km * math.cos(state["heading"]) / 111.0
        state["lon"] += km * math.sin(state["heading"]) / (111.0 * math.cos(math.radians(state["lat"])))
        state["fuel"] = max(15.0, state["fuel"] - 0.004 * state["speed"] - 0.003)
        temp = min(100.0, 82.0 + state["speed"] * 0.12 + rng.uniform(-2.0, 2.0))
        rows.append(
            {
                "vehicle_id": VEHICLES[vehicle_idx],
                "ts": start + timedelta(minutes=i),
                "speed_kmph": round(state["speed"], 2),
                "engine_temp_c": round(temp, 1),
                "fuel_level_pct": round(state["fuel"], 2),
                "battery_voltage_v": round(13.8 + rng.uniform(-0.4, 0.4), 2),
                "latitude": round(state["lat"], 6),
                "longitude": round(state["lon"], 6),
            }
        )
    return rows


def _fmt_iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fmt_space(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _to_raw(row: dict[str, Any], ts_fmt) -> dict[str, Any]:
    out = {c: row.get(c) for c in RAW_COLUMNS if c != "timestamp"}
    out["timestamp"] = ts_fmt(row["ts"])
    return {c: out[c] for c in RAW_COLUMNS}


def generate(output_dir: str | Path = "data/sample", seed: int = 42) -> dict[str, Any]:
    """Create the sample files and return the expected-counts dictionary."""
    rng = random.Random(seed)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    day1, day1_late_hour, day2 = [], [], []
    for idx in range(len(VEHICLES)):
        st = {
            "speed": rng.uniform(20, 60),
            "heading": rng.uniform(0, 6.28),
            "lat": START_LAT + idx * 0.02,
            "lon": START_LON + idx * 0.02,
            "fuel": rng.uniform(60, 90),
        }
        day1 += _simulate(rng, idx, DAY1, 120, st)
        day1_late_hour += _simulate(rng, idx, DAY1 + timedelta(hours=2), 60, st)
        day2 += _simulate(rng, idx, DAY2, 60, st)

    # --- withhold 15 readings of VH-0001 (08:30-08:44): they will arrive late in batch 2 ---------
    late_start = DAY1 + timedelta(minutes=30)
    late_end = DAY1 + timedelta(minutes=45)
    late_rows = [r for r in day1 if r["vehicle_id"] == "VH-0001" and late_start <= r["ts"] < late_end]
    batch1 = [r for r in day1 if r not in late_rows]

    # --- overheating episode: VH-0002 at 08:40-08:49 -------------------------------------------
    temps = [108, 109, 110, 111, 112, 116, 117, 118, 117, 116]
    block_start = DAY1 + timedelta(minutes=40)
    block_idx = [
        i for i, r in enumerate(batch1) if r["vehicle_id"] == "VH-0002" and block_start <= r["ts"] < block_start + timedelta(minutes=10)
    ]
    block_idx.sort(key=lambda i: batch1[i]["ts"])
    for i, t in zip(block_idx, temps, strict=True):
        batch1[i]["engine_temp_c"] = float(t)

    # --- inject dirty rows into distinct, non-overlapping positions ----------------------------
    pool = [i for i in range(len(batch1)) if i not in set(block_idx)]
    rng.shuffle(pool)

    def take(n: int) -> list[int]:
        picked, rest = pool[:n], pool[n:]
        pool[:] = rest
        return picked

    dup_idx, conflict_idx = take(6), take(2)
    null_temp, null_fuel, speed400 = take(4), take(5), take(3)
    bad_ts, empty_vehicle, bad_batt = take(2), take(2), take(1)
    cross_dup_idx = take(10)

    raw1 = [_to_raw(r, _fmt_iso) for r in batch1]
    for i in null_temp:
        raw1[i]["engine_temp_c"] = ""
    for i in null_fuel:
        raw1[i]["fuel_level_pct"] = ""
    for i in speed400:
        raw1[i]["speed_kmph"] = 400
    for i in bad_ts:
        raw1[i]["timestamp"] = "not-a-timestamp"
    for i in empty_vehicle:
        raw1[i]["vehicle_id"] = ""
    for i in bad_batt:
        raw1[i]["battery_voltage_v"] = "abc"

    cross_dup_rows = [_to_raw(batch1[i], _fmt_space) for i in cross_dup_idx]  # clean copies for batch 2
    extra = [dict(raw1[i]) for i in dup_idx]  # exact duplicates
    for i in conflict_idx:  # same key, different speed (appended later => should win)
        row = dict(raw1[i])
        row["speed_kmph"] = round(float(row["speed_kmph"]) + 5.0, 2)
        extra.append(row)
    batch1_file = raw1 + extra

    corrupted = len(null_temp) + len(speed400) + len(bad_ts) + len(empty_vehicle) + len(bad_batt)
    with (out / "telemetry_batch_001.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerows(batch1_file)

    # --- batch 2 (JSON lines) ------------------------------------------------------------------
    clean_b2 = [_to_raw(r, _fmt_space) for r in day1_late_hour + day2 + late_rows]
    batch2_rows = clean_b2 + cross_dup_rows
    rng.shuffle(batch2_rows)
    with (out / "telemetry_batch_002.json").open("w", encoding="utf-8") as fh:
        for row in batch2_rows:
            fh.write(json.dumps(row) + "\n")
        fh.write('{"vehicle_id": "VH-0003", "timestamp": "2026-01-06 09:30:00", "speed_kmph": \n')  # malformed

    b1_unique = len(batch1)
    silver_after_1 = b1_unique - corrupted
    silver_after_2 = silver_after_1 + len(clean_b2)
    n2_lines = len(batch2_rows) + 1
    expected = {
        "note": "Computed by the generator from the injected defects, independent of the pipeline code.",
        "seed": seed,
        "after_batch_1": {
            "bronze_rows": len(batch1_file),
            "rejected_rows": corrupted,
            "valid_rows": len(batch1_file) - corrupted,
            "rows_with_warnings": len(null_fuel),
            "in_batch_duplicate_rows_removed": len(dup_idx) + len(conflict_idx),
            "conflicting_duplicate_keys": len(conflict_idx),
            "late_arrival_rows": 0,
            "silver_rows": silver_after_1,
            "gold_daily_rows": len(VEHICLES),
            "temperature_alerts": 10,
        },
        "after_batch_2": {
            "bronze_rows": len(batch1_file) + n2_lines,
            "rejected_rows_batch_2": 1,
            "valid_rows_batch_2": n2_lines - 1,
            "rows_matching_existing_silver": len(cross_dup_rows),
            "late_arrival_rows": len(late_rows),
            "silver_rows": silver_after_2,
            "gold_daily_rows": len(VEHICLES) * 2,
            "gold_vehicle_kpis": len(VEHICLES),
            "temperature_alerts": 10,
            "warning_alerts": 5,
            "critical_alerts": 5,
        },
    }
    (out / "expected").mkdir(exist_ok=True)
    (out / "expected" / "expected_counts.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic vehicle telemetry sample data")
    parser.add_argument("--output", default="data/sample")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    expected = generate(args.output, args.seed)
    print(json.dumps(expected, indent=2))


if __name__ == "__main__":
    main()
