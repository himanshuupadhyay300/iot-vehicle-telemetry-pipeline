"""Tiny JSON state store that makes incremental loading restartable and idempotent.

It records:
  * which source files (by SHA-256 checksum) were already ingested -> a re-run never re-ingests them
  * for each ingestion batch, which stages (bronze/silver/gold) completed, so a crash between stages is
    resumed on the next run instead of silently losing data.

Production note: on Azure this could be a Delta table or a Cosmos/SQL control table. A JSON file keeps the
local workflow dependency-free.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

STAGES = ("bronze", "silver", "gold")


class StateStore:
    """File-backed state with atomic writes (write temp file, then rename)."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {"files": {}, "batches": {}}
        with self.path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        data.setdefault("files", {})
        data.setdefault("batches", {})
        return data

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    # ---- files -----------------------------------------------------------------------------------
    def has_file(self, sha256: str) -> bool:
        """True when a file with this checksum was already ingested (regardless of its name)."""
        return sha256 in self._data["files"]

    # ---- batches ---------------------------------------------------------------------------------
    def register_batch(self, batch_id: str, files: list[dict[str, str]], rows: int) -> None:
        """Record a freshly written bronze batch and the files it contained."""
        for item in files:
            self._data["files"][item["sha256"]] = {"path": item["path"], "batch_id": batch_id}
        self._data["batches"][batch_id] = {
            "files": [f["path"] for f in files],
            "completed_stages": ["bronze"],
            "bronze_rows": rows,
            "affected_dates": [],
        }
        self._save()

    def mark_stage_done(self, batch_id: str, stage: str, **info: Any) -> None:
        """Mark ``stage`` complete for the batch and store any extra info (e.g. affected_dates)."""
        if stage not in STAGES:
            raise ValueError(f"Unknown stage: {stage}")
        batch = self._data["batches"][batch_id]
        if stage not in batch["completed_stages"]:
            batch["completed_stages"].append(stage)
        batch.update(info)
        self._save()

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self._data["batches"][batch_id]

    def pending_batches(self, stage: str) -> list[str]:
        """Batch ids whose previous stage is done but ``stage`` is not (insertion order = arrival order)."""
        idx = STAGES.index(stage)
        previous = STAGES[idx - 1] if idx > 0 else None
        pending = []
        for batch_id, batch in self._data["batches"].items():
            done = batch["completed_stages"]
            if stage not in done and (previous is None or previous in done):
                pending.append(batch_id)
        return pending

    def snapshot(self) -> dict[str, Any]:
        return json.loads(json.dumps(self._data))
