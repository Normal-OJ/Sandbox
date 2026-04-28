from unittest.mock import MagicMock
from agent.registration import register_runner
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
