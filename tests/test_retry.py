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
        {"initial_backoff_seconds": -0.1},
        {"backoff_multiplier": 0.5},
    ],
)
def test_retry_policy_rejects_unsafe_configuration(kwargs) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)
