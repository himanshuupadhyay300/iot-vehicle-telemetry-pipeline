"""Data quality report and pipeline execution summary writers (JSON + Markdown)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _pct(part: int, whole: int) -> str:
    return f"{(100.0 * part / whole):.2f}%" if whole else "n/a"


def render_dq_markdown(report: dict[str, Any]) -> str:
    """Human-readable data quality report for one batch."""
    total = report["bronze_rows"]
    lines = [
        f"# Data Quality Report - batch `{report['batch_id']}`",
        "",
        f"- Generated at (UTC): {report['generated_at']}",
        f"- Bronze rows evaluated: **{total}**",
        f"- Valid rows: **{report['valid_rows']}** ({_pct(report['valid_rows'], total)})",
        f"- Rejected rows (quarantined): **{report['rejected_rows']}** ({_pct(report['rejected_rows'], total)})",
        f"- Valid rows carrying warnings: **{report['rows_with_warnings']}**",
        "",
        "## Duplicates and late data",
        "",
        f"- Duplicate rows removed inside this batch: **{report['in_batch_duplicate_rows_removed']}**",
        f"- Keys with conflicting duplicate payloads (latest wins): **{report['conflicting_duplicate_keys']}**",
        f"- Events already present in Silver (updated, not double-counted): **{report['rows_matching_existing_silver']}**",
        f"- Late-arriving new events (older than the vehicle's Silver watermark): **{report['late_arrival_rows']}**",
        "",
        "## Rejections by rule",
        "",
    ]
    rejected = report["rejected_by_rule"]
    lines += [f"- `{k}`: {v}" for k, v in sorted(rejected.items())] or ["- none"]
    lines += ["", "## Warnings by rule", ""]
    warnings = report["warnings_by_rule"]
    lines += [f"- `{k}`: {v}" for k, v in sorted(warnings.items())] or ["- none"]
    lines += [
        "",
        "## Result",
        "",
        f"- Distinct events written to Silver from this batch: **{report['silver_rows_written']}**",
        f"- Silver total rows after merge: **{report['silver_rows_after']}**",
        f"- Affected event dates: {', '.join(report['affected_dates']) or 'none'}",
        "",
    ]
    return "\n".join(lines)


def render_summary_markdown(summary: dict[str, Any]) -> str:
    """Human-readable pipeline execution summary."""
    lines = [
        f"# Pipeline Execution Summary - run `{summary['run_id']}`",
        "",
        f"- Status: **{summary['status']}**",
        f"- Storage format: `{summary['storage_format']}`",
        f"- Started: {summary['started_at']}  |  Finished: {summary['finished_at']}",
        f"- Total duration: {summary['duration_seconds']} s",
        "",
        "## Stages",
        "",
    ]
    for name, info in summary["stages"].items():
        lines.append(f"- **{name}**: " + ", ".join(f"{k}={v}" for k, v in info.items()))
    lines += ["", "## Table row counts after run", ""]
    counts = summary.get("table_row_counts", {})
    lines += [f"- `{k}`: {v}" for k, v in counts.items()] or ["- n/a"]
    if summary.get("error"):
        lines += ["", "## Error", "", f"`{summary['error']}`"]
    lines.append("")
    return "\n".join(lines)


def write_report(payload: dict[str, Any], reports_dir: str | Path, stem: str, markdown: str) -> dict[str, str]:
    """Write ``<stem>.json`` and ``<stem>.md`` into ``reports_dir``; return both paths."""
    out = Path(reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / f"{stem}.json"
    md_path = out / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}
