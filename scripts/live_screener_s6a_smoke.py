"""Manual S6A report smoke over an existing S5 successful v11 run.

This script only replays the explicit persisted Screener run and writes
derived JSON/Markdown files.  It does not run S5, acquire market data, call a
provider, migrate a database, or invoke any recurring automation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

from app.reporting.screener_report import (
    ScreenerReportArtifactWriter,
    ScreenerReportGenerator,
)


def _production_fingerprint(path: Path) -> str | None:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _schema_version(path: Path) -> int:
    uri = path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute("PRAGMA query_only = ON")
        row = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        connection.close()


def _iso_run_id(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise argparse.ArgumentTypeError("screener-run-id must be lowercase SHA-256")
    return normalized


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the S6A report smoke for one explicit S5 run."
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--screener-run-id", type=_iso_run_id, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument(
        "--production-database",
        type=Path,
        default=Path("data/research.db"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    production_before = _production_fingerprint(args.production_database)

    generator = ScreenerReportGenerator(args.database)
    writer = ScreenerReportArtifactWriter()
    first = generator.generate(args.screener_run_id)
    first_write = writer.write(first, output_directory=args.output_directory)

    second = generator.generate(args.screener_run_id)
    second_write = writer.write(second, output_directory=args.output_directory)
    production_after = _production_fingerprint(args.production_database)

    if first.canonical_json != second.canonical_json:
        raise RuntimeError("S6A canonical JSON changed on second generation")
    if first.markdown != second.markdown:
        raise RuntimeError("S6A Markdown changed on second generation")
    if first.report_sha256 != second.report_sha256:
        raise RuntimeError("S6A report SHA changed on second generation")
    if not second_write.no_op:
        raise RuntimeError("S6A second artifact generation was not a zero-write no-op")
    if production_before != production_after:
        raise RuntimeError("production database changed during S6A smoke")

    print(
        json.dumps(
            {
                "ok": True,
                "scope": "s6a_subset_report_from_persisted_s5_success",
                "subset": True,
                "database": str(args.database),
                "schema_version": _schema_version(args.database),
                "screener_run_id": args.screener_run_id,
                "market_date": first.report.market_date.isoformat(),
                "candidate_count": first.report.candidate_count,
                "candidate_order": [
                    item.symbol for item in first.report.candidates
                ],
                "report_sha256": first.report_sha256,
                "screener_canonical_sha256": first.report.screener_canonical_sha256,
                "json_size_bytes": len(first.json_bytes),
                "markdown_size_bytes": len(first.markdown_bytes),
                "first_generation": {
                    "total_seconds": round(first.total_seconds, 6),
                    "canonical_json_seconds": round(
                        first.canonical_json_seconds, 6
                    ),
                    "markdown_seconds": round(first.markdown_seconds, 6),
                    "json_written": first_write.json_artifact.written,
                    "markdown_written": first_write.markdown_artifact.written,
                },
                "second_generation": {
                    "total_seconds": round(second.total_seconds, 6),
                    "canonical_json_seconds": round(
                        second.canonical_json_seconds, 6
                    ),
                    "markdown_seconds": round(second.markdown_seconds, 6),
                    "json_written": second_write.json_artifact.written,
                    "markdown_written": second_write.markdown_artifact.written,
                    "no_op": second_write.no_op,
                },
                "research_execution": {
                    "universe": 0,
                    "stage1": 0,
                    "stage2": 0,
                    "m9": 0,
                    "provider": 0,
                    "ranking": 0,
                },
                "production_database_unchanged": production_before == production_after,
                "json_path": str(first_write.json_artifact.path),
                "markdown_path": str(first_write.markdown_artifact.path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
