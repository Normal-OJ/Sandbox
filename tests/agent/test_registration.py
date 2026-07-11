from unittest.mock import MagicMock, call, patch

import pytest

from agent.registration import register_runner, register_runner_with_retry
from agent.client import BackendClient


def test_register_runner_returns_credentials_and_config():
    fake_client = MagicMock(spec=BackendClient)
    fake_client.register.return_value = {
        "runner_id": "rn_xyz",
        "token": "rk_xyz",
        "config": {
            "heartbeat_interval_sec": 10,
            "poll_interval_sec": 5,
            "max_concurrent_jobs": 4,
        },
    }

    result = register_runner(fake_client,
                             name="r1",
                             registration_token="dev-token")

    assert result.runner_id == "rn_xyz"
    assert result.token == "rk_xyz"
    assert result.heartbeat_interval_sec == 10
    assert result.poll_interval_sec == 5
    assert result.max_concurrent_jobs == 4

    fake_client.register.assert_called_once_with(
        name="r1", registration_token="dev-token")


def test_register_runner_uses_defaults_for_missing_config_fields():
    fake_client = MagicMock(spec=BackendClient)
    fake_client.register.return_value = {
        "runner_id": "rn_a",
        "token": "rk_a",
        "config": {},
    }

    result = register_runner(fake_client, name="r1", registration_token="t")

    # Backend should always send config, but defensively use sensible defaults
    assert result.heartbeat_interval_sec == 15
    assert result.poll_interval_sec == 3
    assert result.max_concurrent_jobs == 8


def test_register_runner_retries_transient_errors_with_capped_backoff():
    fake_client = MagicMock(spec=BackendClient)
    transient = BackendClient.TransientError("not ready")
    fake_client.register.side_effect = [transient] * 6 + [{
        "runner_id": "rn_a",
        "token": "rk_a",
        "config": {},
    }]

    with patch("agent.registration.time.sleep") as sleep:
        result = register_runner_with_retry(
            fake_client,
            name="r1",
            registration_token="t",
        )

    assert result.runner_id == "rn_a"
    assert sleep.call_args_list == [
        call(1.0),
        call(2.0),
        call(4.0),
        call(8.0),
        call(16.0),
        call(30.0),
    ]


def test_register_runner_does_not_retry_auth_error():
    fake_client = MagicMock(spec=BackendClient)
    fake_client.register.side_effect = BackendClient.AuthError("invalid token")

    with patch("agent.registration.time.sleep") as sleep:
        with pytest.raises(BackendClient.AuthError):
            register_runner_with_retry(
                fake_client,
                name="r1",
                registration_token="bad",
            )

    fake_client.register.assert_called_once()
    sleep.assert_not_called()
