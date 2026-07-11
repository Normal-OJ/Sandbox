import threading
import time
from unittest.mock import MagicMock

import pytest

from agent.client import BackendClient
from agent.heartbeat import HeartbeatThread


def test_heartbeat_calls_client_at_interval():
    client = MagicMock(spec=BackendClient)
    shutdown = threading.Event()

    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=0.05,
        shutdown_event=shutdown,
    )
    hb.start()
    time.sleep(0.18)  # ~3 intervals
    shutdown.set()
    hb.join(timeout=1)

    # Should have been called 3-4 times
    assert 2 <= client.heartbeat.call_count <= 5
    client.heartbeat.assert_called_with(runner_id="rn_1")


def test_heartbeat_swallows_transient_errors_and_keeps_going():
    client = MagicMock(spec=BackendClient)
    client.heartbeat.side_effect = [
        BackendClient.TransientError("boom"),
        None,
        None,
    ]
    shutdown = threading.Event()

    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=0.05,
        shutdown_event=shutdown,
    )
    hb.start()
    time.sleep(0.2)
    shutdown.set()
    hb.join(timeout=1)

    # Despite first call raising, subsequent calls happened
    assert client.heartbeat.call_count >= 3


def test_heartbeat_stops_promptly_on_shutdown():
    client = MagicMock(spec=BackendClient)
    shutdown = threading.Event()
    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=10.0,  # long interval
        shutdown_event=shutdown,
    )
    hb.start()
    time.sleep(0.05)
    shutdown.set()
    hb.join(timeout=0.5)
    assert not hb.is_alive(), "heartbeat thread should exit promptly"


def test_heartbeat_auth_error_triggers_shutdown():
    client = MagicMock(spec=BackendClient)
    client.heartbeat.side_effect = BackendClient.AuthError("forgot runner")
    shutdown = threading.Event()
    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=10.0,
        shutdown_event=shutdown,
    )

    hb.start()
    hb.join(timeout=0.5)

    assert shutdown.is_set()
    assert not hb.is_alive()
    client.heartbeat.assert_called_once_with(runner_id="rn_1")
