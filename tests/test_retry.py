import pytest

from dataops_pipeline.retry import retry


def test_retries_then_succeeds():
    calls, sleeps = [], []

    @retry(attempts=3, base_delay=1, sleep=sleeps.append)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise OSError("transient")
        return "ok"

    assert flaky() == "ok"
    assert sleeps == [1, 2]  # exponential backoff


def test_gives_up_after_max_attempts():
    @retry(attempts=2, base_delay=0, sleep=lambda _: None)
    def always_fails():
        raise OSError("down")

    with pytest.raises(OSError):
        always_fails()


def test_non_transient_errors_are_not_retried():
    calls = []

    @retry(attempts=3, sleep=lambda _: None)
    def bug():
        calls.append(1)
        raise ValueError("bug")

    with pytest.raises(ValueError):
        bug()
    assert len(calls) == 1
