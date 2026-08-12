"""Configurable retry policy for temporary provider failures."""

from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_backoff_seconds: float = 0.5
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 60.0
    max_total_delay_seconds: float = 120.0

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if (
            not _is_finite_number(self.initial_backoff_seconds)
            or self.initial_backoff_seconds < 0
        ):
            raise ValueError("initial_backoff_seconds must not be negative")
        if (
            not _is_finite_number(self.backoff_multiplier)
            or self.backoff_multiplier < 1
        ):
            raise ValueError("backoff_multiplier must be at least 1")
        if (
            not _is_finite_number(self.max_delay_seconds)
            or self.max_delay_seconds < 0
        ):
            raise ValueError("max_delay_seconds must be finite and non-negative")
        if (
            not _is_finite_number(self.max_total_delay_seconds)
            or self.max_total_delay_seconds < 0
        ):
            raise ValueError(
                "max_total_delay_seconds must be finite and non-negative"
            )

    def delay_after_failure(self, failed_attempt: int) -> float:
        if failed_attempt < 1:
            raise ValueError("failed_attempt must be at least 1")
        try:
            delay = self.initial_backoff_seconds * (
                self.backoff_multiplier ** (failed_attempt - 1)
            )
        except OverflowError:
            return float(self.max_delay_seconds)
        if not math.isfinite(delay):
            return float(self.max_delay_seconds)
        return float(min(delay, self.max_delay_seconds))

    def next_delay(
        self,
        failed_attempt: int,
        *,
        retry_after_seconds: float | None = None,
        total_delay_seconds: float = 0.0,
    ) -> float | None:
        """Return a bounded next delay, or ``None`` when the budget is exhausted."""

        if not _is_finite_number(total_delay_seconds) or total_delay_seconds < 0:
            raise ValueError("total_delay_seconds must be finite and non-negative")
        delay = self.delay_after_failure(failed_attempt)
        if retry_after_seconds is not None:
            if not _is_finite_number(retry_after_seconds) or retry_after_seconds < 0:
                raise ValueError(
                    "retry_after_seconds must be finite and non-negative"
                )
            delay = max(delay, min(retry_after_seconds, self.max_delay_seconds))
        if total_delay_seconds + delay > self.max_total_delay_seconds:
            return None
        return float(delay)


def _is_finite_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )
