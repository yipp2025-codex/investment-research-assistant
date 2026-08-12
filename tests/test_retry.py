import pytest

from app.pipelines import RetryPolicy


def test_retry_policy_exponential_backoff_is_deterministic() -> None:
    policy = RetryPolicy(
        max_attempts=4,
        initial_backoff_seconds=0.25,
        backoff_multiplier=2,
    )

    assert [policy.delay_after_failure(attempt) for attempt in (1, 2, 3)] == [
        0.25,
        0.5,
        1.0,
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_attempts": True},
        {"initial_backoff_seconds": -0.1},
        {"initial_backoff_seconds": float("nan")},
        {"backoff_multiplier": 0.5},
        {"backoff_multiplier": float("inf")},
        {"max_delay_seconds": -0.1},
        {"max_delay_seconds": float("inf")},
        {"max_total_delay_seconds": -0.1},
        {"max_total_delay_seconds": float("nan")},
    ],
)
def test_retry_policy_rejects_unsafe_configuration(kwargs) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_retry_policy_caps_remote_hint_and_enforces_total_budget() -> None:
    policy = RetryPolicy(
        max_attempts=3,
        initial_backoff_seconds=0.25,
        max_delay_seconds=60.0,
        max_total_delay_seconds=60.0,
    )

    assert policy.next_delay(1, retry_after_seconds=1_000_000_000) == 60.0
    assert (
        policy.next_delay(
            2,
            retry_after_seconds=1_000_000_000,
            total_delay_seconds=60.0,
        )
        is None
    )


def test_retry_policy_caps_exponential_overflow() -> None:
    policy = RetryPolicy(
        max_attempts=10_000,
        initial_backoff_seconds=1.0,
        backoff_multiplier=10.0,
        max_delay_seconds=17.0,
    )

    assert policy.delay_after_failure(10_000) == 17.0
