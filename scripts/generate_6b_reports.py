"""Generate Phase 6B Markdown reports from existing canonical SQLite data.

This script does not fetch data or run a provider.  It reads existing
daily_prices/company_metrics and writes only daily_research_results/reports.

Example:
    .venv/Scripts/python.exe scripts/generate_6b_reports.py \
        --database data/research.db --symbol 2330 --days 10 \
        --output-dir data/daily-reports
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.reports.daily_research import DailyResearchReportService
from app.storage import SQLiteDailyReportRepository, SQLiteResearchRepository


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render Phase 6B reports from existing canonical SQLite data."
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--end-date", type=_iso_date)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be positive")
    symbol = args.symbol.strip().upper()
    repository = SQLiteResearchRepository(args.database)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()

    end_date = args.end_date
    prices = repository.list_daily_prices(symbol, end_date=end_date)
    dates = sorted({item.trade_date for item in prices})[-args.days :]
    if len(dates) < args.days:
        raise SystemExit(
            f"only {len(dates)} market dates available for {symbol}; "
            f"need {args.days}"
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
    )
    print(f"Generating {len(dates)} Phase 6B reports for {symbol}")
    print(f"Source database: {args.database}")
    for market_date in dates:
        generated = service.generate(symbol, market_date)
        markdown_path = args.output_dir / f"{symbol}_{market_date.isoformat()}.md"
        markdown_path.write_text(generated.report.markdown or "", encoding="utf-8")
        changes = generated.canonical.payload["comparison"]["changes"]
        print(
            f"{market_date}  "
            f"result={generated.canonical.result_id}  "
            f"report={generated.report.report_status}  "
            f"replay={generated.idempotent_replay}  "
            f"changes={len(changes)}  "
            f"file={markdown_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
