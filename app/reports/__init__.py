"""Canonical daily research result and change-only report services."""

from .daily_research import (
    CANONICAL_SCHEMA_VERSION,
    METHODOLOGY_VERSION,
    CanonicalSchemaError,
    ChangeThresholds,
    DailyResearchReportError,
    DailyResearchReportService,
    DailyReportGenerationResult,
    MetricValue,
    render_daily_research_markdown,
    validate_canonical_payload,
    validate_rendered_markdown,
)
from .contracts import (
    BatchContextReader,
    DailyBatchContext,
    DailyBatchSymbolContext,
    DailyResultReader,
    DailyResultStore,
)
from .composition import compose_sqlite_daily_research_report_service

__all__ = [
    "CANONICAL_SCHEMA_VERSION",
    "METHODOLOGY_VERSION",
    "CanonicalSchemaError",
    "ChangeThresholds",
    "DailyResearchReportError",
    "DailyResearchReportService",
    "DailyReportGenerationResult",
    "MetricValue",
    "BatchContextReader",
    "DailyBatchContext",
    "DailyBatchSymbolContext",
    "DailyResultReader",
    "DailyResultStore",
    "compose_sqlite_daily_research_report_service",
    "render_daily_research_markdown",
    "validate_canonical_payload",
    "validate_rendered_markdown",
]
