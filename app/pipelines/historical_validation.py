"""Cross-validated deterministic historical research from TWSE and E.SUN."""

from __future__ import annotations

import re
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone

from app.analysis import HistoricalResearchAnalysis
from app.models import (
    CrossValidationOutcome,
    HistoricalSourceObservation,
    HistoricalValidationDiscrepancy,
    PipelineRunStatus,
    ResearchNote,
)
from app.providers import MarketDataProvider
from app.storage import (
    HistoricalValidationConflictError,
    HistoricalValidationStateError,
    SQLiteHistoricalSyncRepository,
    SQLiteHistoricalValidationRepository,
    SQLiteResearchRepository,
)

from .historical_sync import HistoricalSyncPipeline, HistoricalSyncResult
from .retry import RetryPolicy


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


@dataclass(frozen=True, slots=True)
class CrossValidatedHistoricalResult:
    run_id: str
    run_status: PipelineRunStatus
    run_attempt_count: int
    idempotent_replay: bool
    symbol: str
    target_date: date
    target_observations: int
    outcome: CrossValidationOutcome
    common_date_count: int
    matched_date_count: int
    left_only_date_count: int
    right_only_date_count: int
    field_discrepancy_count: int
    left_latest_date: date
    right_latest_date: date
    left_sync: HistoricalSyncResult
    right_sync: HistoricalSyncResult
    discrepancies: tuple[HistoricalValidationDiscrepancy, ...]
    research_note_id: int
    analysis: HistoricalResearchAnalysis
    summary: str


class CrossValidatedHistoricalResearchPipeline:
    """Sync both official sources, compare them, and analyze TWSE deterministically."""

    def __init__(
        self,
        twse_provider: MarketDataProvider,
        esun_provider: MarketDataProvider,
        repository: SQLiteResearchRepository,
        *,
        retry_policy: RetryPolicy | None = None,
        provider_timeout_seconds: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if twse_provider.source != "twse-historical":
            raise ValueError("historical research baseline must be twse-historical")
        if esun_provider.source != "esun-historical":
            raise ValueError("historical validator must be esun-historical")
        self.repository = repository
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        policy = retry_policy or RetryPolicy()
        self.twse_sync = HistoricalSyncPipeline(
            twse_provider,
            repository,
            retry_policy=policy,
            provider_timeout_seconds=provider_timeout_seconds,
            sleep=sleep,
            clock=self.clock,
            write_canonical_prices=True,
        )
        self.esun_sync = HistoricalSyncPipeline(
            esun_provider,
            repository,
            retry_policy=policy,
            provider_timeout_seconds=provider_timeout_seconds,
            sleep=sleep,
            clock=self.clock,
            write_canonical_prices=False,
        )
        self.history_repository = SQLiteHistoricalSyncRepository(repository)
        self.validation_repository = SQLiteHistoricalValidationRepository(repository)

    def run(
        self,
        symbol: str,
        target_date: date,
        *,
        target_observations: int = 250,
        max_months: int = 18,
        resume_twse_run_id: str | None = None,
        resume_esun_run_id: str | None = None,
        resume_validation_run_id: str | None = None,
    ) -> CrossValidatedHistoricalResult:
        twse_result = self.twse_sync.run(
            symbol,
            target_date,
            target_observations=target_observations,
            max_months=max_months,
            resume_run_id=resume_twse_run_id,
        )
        esun_result = self.esun_sync.run(
            symbol,
            target_date,
            target_observations=target_observations,
            max_months=max_months,
            resume_run_id=resume_esun_run_id,
        )
        twse_run = self.history_repository.get(twse_result.run_id)
        esun_run = self.history_repository.get(esun_result.run_id)
        if twse_run is None or esun_run is None:  # pragma: no cover - pipeline contract.
            raise HistoricalValidationStateError(
                "completed historical source checkpoint disappeared"
            )

        validation = self.validation_repository.get_or_create(
            left_run=twse_run,
            right_run=esun_run,
            created_at=self._now(),
        )
        if (
            resume_validation_run_id is not None
            and resume_validation_run_id != validation.run_id
        ):
            raise HistoricalValidationConflictError(
                "resume_validation_run_id does not match the source pair"
            )
        if validation.status is PipelineRunStatus.SUCCESS:
            return self._load_successful(
                validation.run_id, twse_result=twse_result, esun_result=esun_result
            )

        running = self.validation_repository.start(
            validation.run_id,
            started_at=self._now(),
            resume_running=resume_validation_run_id == validation.run_id,
        )
        try:
            left = self.history_repository.list_source_observations(
                twse_result.run_id,
                end_date=target_date,
                limit=target_observations,
            )
            right = self.history_repository.list_source_observations(
                esun_result.run_id,
                end_date=target_date,
                limit=target_observations,
            )
            if len(left) != target_observations or len(right) != target_observations:
                raise HistoricalValidationStateError(
                    "cross-validation requires persisted observations from both runs"
                )
            comparison = self._compare(running.run_id, left, right)
            outcome = (
                CrossValidationOutcome.DISCREPANCY
                if comparison.discrepancies
                else CrossValidationOutcome.MATCH
            )
            summary = self._summary(
                twse_result.summary,
                outcome=outcome,
                comparison=comparison,
            )
            note = ResearchNote(
                symbol=twse_result.symbol,
                created_at=self._now(),
                analysis_type="cross-validated-historical-research-v1",
                title=(
                    f"{twse_result.symbol} 雙來源歷史研究摘要 "
                    f"{twse_result.analysis.period_end} ({target_observations} 日)"
                ),
                summary=summary,
                source_data_start=twse_result.analysis.period_start,
                source_data_end=twse_result.analysis.period_end,
                provider_source="twse-historical+esun-historical",
                historical_validation_run_id=running.run_id,
            )
            with self.validation_repository.successful_run(running.run_id) as unit:
                saved_note = unit.create_research_note(note)
                if saved_note.id is None:  # pragma: no cover - SQLite supplies id.
                    raise HistoricalValidationStateError(
                        "historical validation note was stored without id"
                    )
                stored_outcome = unit.complete(
                    saved_note.id,
                    comparison.discrepancies,
                    common_date_count=comparison.common_date_count,
                    matched_date_count=comparison.matched_date_count,
                    left_only_date_count=comparison.left_only_date_count,
                    right_only_date_count=comparison.right_only_date_count,
                    field_discrepancy_count=comparison.field_discrepancy_count,
                    left_latest_date=comparison.left_latest_date,
                    right_latest_date=comparison.right_latest_date,
                    finished_at=self._now(),
                )
        except Exception as error:
            self.validation_repository.mark_failed(
                running.run_id,
                finished_at=self._now(),
                error_message=self._safe_error_message(error),
            )
            raise

        return CrossValidatedHistoricalResult(
            run_id=running.run_id,
            run_status=PipelineRunStatus.SUCCESS,
            run_attempt_count=running.attempt_count,
            idempotent_replay=False,
            symbol=twse_result.symbol,
            target_date=target_date,
            target_observations=target_observations,
            outcome=stored_outcome,
            common_date_count=comparison.common_date_count,
            matched_date_count=comparison.matched_date_count,
            left_only_date_count=comparison.left_only_date_count,
            right_only_date_count=comparison.right_only_date_count,
            field_discrepancy_count=comparison.field_discrepancy_count,
            left_latest_date=comparison.left_latest_date,
            right_latest_date=comparison.right_latest_date,
            left_sync=twse_result,
            right_sync=esun_result,
            discrepancies=tuple(
                self.validation_repository.list_discrepancies(running.run_id)
            ),
            research_note_id=saved_note.id,
            analysis=twse_result.analysis,
            summary=summary,
        )

    def _load_successful(
        self,
        run_id: str,
        *,
        twse_result: HistoricalSyncResult,
        esun_result: HistoricalSyncResult,
    ) -> CrossValidatedHistoricalResult:
        run = self.validation_repository.get(run_id)
        if run is None or run.status is not PipelineRunStatus.SUCCESS:
            raise HistoricalValidationStateError(
                "successful historical validation checkpoint is missing"
            )
        if run.outcome is None or run.research_note_id is None:
            raise HistoricalValidationStateError(
                "successful historical validation is incomplete"
            )
        note = self.repository.get_research_note(run.research_note_id)
        if note is None or note.historical_validation_run_id != run.run_id:
            raise HistoricalValidationStateError(
                "historical validation research note linkage is invalid"
            )
        if run.left_latest_date is None or run.right_latest_date is None:
            raise HistoricalValidationStateError(
                "historical validation latest dates are missing"
            )
        return CrossValidatedHistoricalResult(
            run_id=run.run_id,
            run_status=run.status,
            run_attempt_count=run.attempt_count,
            idempotent_replay=True,
            symbol=run.symbol,
            target_date=run.target_date,
            target_observations=run.target_observations,
            outcome=run.outcome,
            common_date_count=run.common_date_count,
            matched_date_count=run.matched_date_count,
            left_only_date_count=run.left_only_date_count,
            right_only_date_count=run.right_only_date_count,
            field_discrepancy_count=run.field_discrepancy_count,
            left_latest_date=run.left_latest_date,
            right_latest_date=run.right_latest_date,
            left_sync=twse_result,
            right_sync=esun_result,
            discrepancies=tuple(self.validation_repository.list_discrepancies(run.run_id)),
            research_note_id=run.research_note_id,
            analysis=twse_result.analysis,
            summary=note.summary,
        )

    @staticmethod
    def _compare(
        run_id: str,
        left: Sequence[HistoricalSourceObservation],
        right: Sequence[HistoricalSourceObservation],
    ) -> "_HistoricalComparison":
        left_by_date = {item.trade_date: item for item in left}
        right_by_date = {item.trade_date: item for item in right}
        if len(left_by_date) != len(left) or len(right_by_date) != len(right):
            raise HistoricalValidationStateError(
                "historical source observations must have unique dates"
            )
        left_dates = set(left_by_date)
        right_dates = set(right_by_date)
        discrepancies: list[HistoricalValidationDiscrepancy] = []
        left_only = sorted(left_dates - right_dates)
        right_only = sorted(right_dates - left_dates)
        for trade_date in left_only:
            discrepancies.append(
                HistoricalValidationDiscrepancy(
                    run_id=run_id,
                    trade_date=trade_date,
                    field="market_date",
                    left_value=trade_date.isoformat(),
                    right_value=None,
                    reason="missing_in_esun",
                )
            )
        for trade_date in right_only:
            discrepancies.append(
                HistoricalValidationDiscrepancy(
                    run_id=run_id,
                    trade_date=trade_date,
                    field="market_date",
                    left_value=None,
                    right_value=trade_date.isoformat(),
                    reason="missing_in_twse",
                )
            )

        matched_dates = 0
        field_discrepancies = 0
        for trade_date in sorted(left_dates & right_dates):
            left_item = left_by_date[trade_date]
            right_item = right_by_date[trade_date]
            date_matched = True
            for field in ("open", "high", "low", "close", "volume"):
                left_value = getattr(left_item, field)
                right_value = getattr(right_item, field)
                if left_value == right_value:
                    continue
                date_matched = False
                field_discrepancies += 1
                absolute = abs(float(left_value) - float(right_value))
                relative = (
                    None
                    if float(left_value) == 0
                    else absolute / abs(float(left_value)) * 100
                )
                discrepancies.append(
                    HistoricalValidationDiscrepancy(
                        run_id=run_id,
                        trade_date=trade_date,
                        field=field,
                        left_value=_canonical_number(left_value),
                        right_value=_canonical_number(right_value),
                        reason=(
                            "volume_definition_or_source_revision_unresolved"
                            if field == "volume"
                            else "source_value_or_revision_unresolved"
                        ),
                        absolute_difference=absolute,
                        relative_difference_pct=relative,
                    )
                )
            if date_matched:
                matched_dates += 1
        return _HistoricalComparison(
            common_date_count=len(left_dates & right_dates),
            matched_date_count=matched_dates,
            left_only_dates=tuple(left_only),
            right_only_dates=tuple(right_only),
            field_discrepancy_count=field_discrepancies,
            left_latest_date=max(left_dates),
            right_latest_date=max(right_dates),
            discrepancies=tuple(discrepancies),
        )

    @staticmethod
    def _summary(
        baseline_summary: str,
        *,
        outcome: CrossValidationOutcome,
        comparison: "_HistoricalComparison",
    ) -> str:
        field_counts = Counter(
            item.field
            for item in comparison.discrepancies
            if item.field != "market_date"
        )
        lines = [
            baseline_summary,
            "",
            "### TWSE ↔ E.SUN 歷史交叉驗證",
            "",
            "- 指標基線：TWSE 原始歷史觀察；E.SUN 僅作驗證，不混合補值。",
            f"- 驗證結果：{outcome.value}",
            f"- TWSE／E.SUN 最新市場日：{comparison.left_latest_date}／{comparison.right_latest_date}",
            f"- 同時存在交易日：{comparison.common_date_count}",
            f"- OHLCV 完全一致交易日：{comparison.matched_date_count}",
            f"- 只存在 TWSE／只存在 E.SUN：{comparison.left_only_date_count}／{comparison.right_only_date_count}",
            f"- 同日欄位差異：{comparison.field_discrepancy_count}",
        ]
        if comparison.left_only_dates:
            lines.append(
                "- E.SUN 缺口日期："
                + ", ".join(day.isoformat() for day in comparison.left_only_dates)
            )
        if comparison.right_only_dates:
            lines.append(
                "- TWSE 缺口日期："
                + ", ".join(day.isoformat() for day in comparison.right_only_dates)
            )
        if field_counts:
            lines.append(
                "- 差異欄位統計："
                + "；".join(
                    f"{field}={count}" for field, count in sorted(field_counts.items())
                )
            )
        lines.extend(
            [
                "- 所有差異保留雙方值與 provenance，不自行判定哪一來源錯誤。",
                "- 本驗證與研究指標皆為 deterministic output，不構成投資建議或交易訊號。",
            ]
        )
        return "\n".join(lines)

    def _now(self) -> datetime:
        now = self.clock()
        if now.utcoffset() is None:
            raise ValueError("historical validation clock must be timezone-aware")
        return now

    @staticmethod
    def _safe_error_message(error: Exception) -> str:
        detail = str(error).replace("\r", " ").replace("\n", " ").strip()
        detail = _SECRET_ASSIGNMENT.sub(r"\1=[REDACTED]", detail)
        detail = _BEARER_TOKEN.sub("Bearer [REDACTED]", detail)
        error_type = type(error).__name__
        return error_type if not detail else f"{error_type}: {detail}"[:1000]


@dataclass(frozen=True, slots=True)
class _HistoricalComparison:
    common_date_count: int
    matched_date_count: int
    left_only_dates: tuple[date, ...]
    right_only_dates: tuple[date, ...]
    field_discrepancy_count: int
    left_latest_date: date
    right_latest_date: date
    discrepancies: tuple[HistoricalValidationDiscrepancy, ...]

    @property
    def left_only_date_count(self) -> int:
        return len(self.left_only_dates)

    @property
    def right_only_date_count(self) -> int:
        return len(self.right_only_dates)


def _canonical_number(value: float | int) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")
