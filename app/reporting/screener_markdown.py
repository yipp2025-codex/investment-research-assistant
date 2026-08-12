"""Deterministic Markdown rendering for the S6A Screener report."""

from __future__ import annotations

import json
import re


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def render_markdown(report: object, report_sha256: str) -> str:
    """Render a report model without current time or free-form interpretation."""

    if not _SHA256.fullmatch(report_sha256):
        raise ValueError("report_sha256 must be lowercase SHA-256")
    data = report.as_dict()
    lines: list[str] = [
        f"# Daily Screener Report — {data['market_date']}",
        "",
        "> 本報告為描述性研究，內容只重述已成功保存的 Screener 結果。",
        "",
        "## Run Summary",
        "",
        "| Field | Value |",
        "|---|---|",
    ]
    summary_rows = (
        ("Market date", data["market_date"]),
        ("Screener run ID", data["screener_run_id"]),
        ("Universe run ID", data["universe_run_id"]),
        ("Stage 1 methodology", data["methodology_versions"]["stage1"]),
        ("Stage 2 methodology", data["methodology_versions"]["stage2"]),
        ("Source policy", data["source_policy"]),
        ("Universe count", data["universe_count"]),
        ("Screened count", data["screened_count"]),
        ("Triggered count", data["triggered_count"]),
        ("Candidate count", data["candidate_count"]),
        ("Candidate limit", data["candidate_limit"]),
        ("Truncated", data["truncated"]),
        ("Screener canonical SHA-256", data["screener_canonical_sha256"]),
    )
    lines.extend(f"| {_cell(key)} | {_cell(value)} |" for key, value in summary_rows)

    lines.extend(("", "## Ranked Candidate Summary", "", _candidate_table(data)))
    for candidate in data["candidates"]:
        lines.extend(("", f"### Rank {candidate['rank']} — {candidate['symbol']}", ""))
        lines.extend(
            (
                f"- Name: {_cell(candidate['name'])}",
                f"- Market: {_cell(candidate['market'])}",
                f"- Candidate kind: {_cell(candidate['candidate_kind'])}",
                f"- Data quality: {_cell(candidate['data_quality_status'])}",
                f"- Validation: {_cell(candidate['validation_status'])}",
                f"- Analysis: {_cell(candidate['analysis_status'])}",
            )
        )
        lines.extend(("", "#### Stage 1 Reasons", "", _reason_table(candidate["stage1_reasons"])))
        lines.extend(("", "#### Stage 2 Reasons", "", _reason_table(candidate["stage2_reasons"])))
        lines.extend(("", "#### Metrics", "", _metric_table(candidate["metrics"])))
        lines.extend(("", "#### Provenance / Data Quality", ""))
        provenance = candidate["provenance"]
        lines.extend(
            (
                f"- Canonical sources: {_cell(', '.join(provenance['canonical_sources']) or None)}",
                f"- Validation sources: {_cell(', '.join(provenance['validation_sources']) or None)}",
                f"- Pipeline run ID: {_cell(provenance['pipeline_run_id'])}",
                f"- Historical run ID: {_cell(provenance['historical_run_id'])}",
                f"- Validation run ID: {_cell(provenance['validation_run_id'])}",
            )
        )
        discrepancies = provenance["discrepancies"]
        if discrepancies:
            lines.extend(("", "Discrepancies:", "", _discrepancy_table(discrepancies)))
        else:
            lines.append("- Discrepancies: none persisted")
        lines.extend(("", "Evidence references:", "", _evidence_table(provenance["evidence_refs"])))

    lines.extend(("", "## Scope / Disclaimer", ""))
    lines.extend(f"- {item}" for item in data["scope_disclaimers"])
    lines.extend(
        (
            "",
            "## Report Integrity",
            "",
            f"- Report contract: `{_cell(data['report_contract_version'])}`",
            f"- Content version: `{_cell(data['content_version'])}`",
            f"- Report ID: `{_cell(data['report_id'])}`",
            f"- Report SHA-256: `{report_sha256}`",
            "",
        )
    )
    return "\n".join(lines)


def _candidate_table(data: dict[str, object]) -> str:
    lines = [
        "| Rank | Symbol | Name | Kind | Data quality | Validation | Analysis |",
        "|---:|---|---|---|---|---|---|",
    ]
    for item in data["candidates"]:
        lines.append(
            "| "
            + " | ".join(
                _cell(item[field])
                for field in (
                    "rank",
                    "symbol",
                    "name",
                    "candidate_kind",
                    "data_quality_status",
                    "validation_status",
                    "analysis_status",
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _reason_table(reasons: list[dict[str, object]]) -> str:
    if not reasons:
        return "No persisted reasons."
    lines = [
        "| # | Code | Metric | Previous | Current | Delta | Unit | Rule |",
        "|---:|---|---|---:|---:|---:|---|---|",
    ]
    for item in reasons:
        lines.append(
            "| "
            + " | ".join(
                _cell(item[field])
                for field in (
                    "ordinal",
                    "code",
                    "metric",
                    "previous",
                    "current",
                    "delta",
                    "unit",
                    "rule_version",
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _metric_table(metrics: list[dict[str, object]]) -> str:
    lines = [
        "| Metric | Status | Value | Previous | Delta | Unit | As of | Previous as of |",
        "|---|---|---:|---:|---:|---|---|---|",
    ]
    for item in metrics:
        lines.append(
            "| "
            + " | ".join(
                _cell(item[field])
                for field in (
                    "name",
                    "status",
                    "value",
                    "previous_value",
                    "delta",
                    "unit",
                    "as_of_date",
                    "previous_as_of_date",
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _discrepancy_table(discrepancies: list[dict[str, object]]) -> str:
    lines = [
        "| Field | Left | Right | Reason |",
        "|---|---|---|---|",
    ]
    for item in discrepancies:
        lines.append(
            "| "
            + " | ".join(
                _cell(item[field]) for field in ("field", "left_value", "right_value", "reason")
            )
            + " |"
        )
    return "\n".join(lines)


def _evidence_table(evidence: list[dict[str, object]]) -> str:
    if not evidence:
        return "No persisted evidence references."
    lines = [
        "| Provider | Dataset | Owner | Source ref | Payload SHA-256 | Size | Hash basis |",
        "|---|---|---|---|---|---:|---|",
    ]
    for item in evidence:
        owner = f"{item['owner_kind']}:{item['owner_run_id']}"
        lines.append(
            "| "
            + " | ".join(
                _cell(value)
                for value in (
                    item["provider"],
                    item["dataset"],
                    owner,
                    item["source_ref"],
                    item["payload_sha256"],
                    item["payload_size_bytes"],
                    item["hash_basis"],
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _cell(value: object) -> str:
    if value is None:
        return "N/A (unavailable)"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", " ")


__all__ = ["render_markdown"]
