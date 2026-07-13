import pytest
import requests

from runner.client import (
    BackendAPIError,
    BackendAuthError,
    RunnerConfig,
    RunnerIdentity,
)
from runner.registration import register_with_backoff


class FakeClient:
    """register() plays back a scripted list of outcomes."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.calls = []

    def register(self, registration_token, name):
        self.calls.append((registration_token, name))
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def make_identity():
    return RunnerIdentity('rn_ok', 'rk_tok', RunnerConfig(15, 3, 8))


def test_success_on_first_try_no_sleep():
    identity = make_identity()
    client = FakeClient([identity])
    sleeps = []

    result = register_with_backoff(
        client,
        'reg-token',
        'runner-1',
        sleep=sleeps.append,
    )

    assert result is identity
    assert sleeps == []
    assert client.calls == [('reg-token', 'runner-1')]


def test_backoff_sequence_across_many_failures_then_success():
    identity = make_identity()
    # 8 transient failures, then success. Sleep schedule must be
    # 1,2,4,8,16,30,30,30 (capped at the final value).
    outcomes = [BackendAPIError('fail', status_code=500) for _ in range(8)]
    outcomes.append(identity)
    client = FakeClient(outcomes)
    sleeps = []

    result = register_with_backoff(
        client,
        'reg-token',
        'runner-1',
        sleep=sleeps.append,
    )

    assert result is identity
    assert sleeps == [1, 2, 4, 8, 16, 30, 30, 30]


def test_network_error_is_retried():
    identity = make_identity()
    outcomes = [
        requests.ConnectionError('boom'),
        requests.Timeout('slow'),
        identity,
    ]
    client = FakeClient(outcomes)
    sleeps = []

    result = register_with_backoff(
        client,
        'reg-token',
        'runner-1',
        sleep=sleeps.append,
    )

    assert result is identity
    assert sleeps == [1, 2]


def test_401_raises_immediately_with_zero_sleeps():
    client = FakeClient([BackendAuthError('nope', status_code=401)])
    sleeps = []

    with pytest.raises(BackendAuthError):
        register_with_backoff(
            client,
            'bad-token',
            'runner-1',
            sleep=sleeps.append,
        )

    assert sleeps == []
    assert len(client.calls) == 1


def test_401_after_transient_failures_stops_retrying():
    outcomes = [
        BackendAPIError('fail', status_code=500),
        BackendAuthError('nope', status_code=401),
    ]
    client = FakeClient(outcomes)
    sleeps = []

    with pytest.raises(BackendAuthError):
        register_with_backoff(
            client,
            'reg-token',
            'runner-1',
            sleep=sleeps.append,
        )

    # Only the transient failure slept; the 401 aborts immediately.
    assert sleeps == [1]
