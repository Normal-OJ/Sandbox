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


def test_heartbeat_two_consecutive_auth_errors_triggers_shutdown():
    client = MagicMock(spec=BackendClient)
    client.heartbeat.side_effect = BackendClient.AuthError("forgot runner")
    shutdown = threading.Event()
    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=0.01,
        shutdown_event=shutdown,
    )

    hb.start()
    hb.join(timeout=1)

    assert shutdown.is_set()
    assert not hb.is_alive()
    assert client.heartbeat.call_count == 2


def test_single_auth_error_does_not_shutdown():
    """A single AuthError shouldn't kill the agent — only two in a row."""
    client = MagicMock(spec=BackendClient)
    client.heartbeat.side_effect = BackendClient.AuthError("forgot runner")
    shutdown = threading.Event()
    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=10.0,  # long wait so only one call happens during test
        shutdown_event=shutdown,
    )

    hb.start()
    hb.join(timeout=0.5)  # thread keeps waiting; join times out, doesn't exit

    # First AuthError alone must not have set shutdown or killed the thread.
    assert not shutdown.is_set()
    assert hb.is_alive()
    assert hb._auth_failed_once
    client.heartbeat.assert_called_once_with(runner_id="rn_1")

    # Cleanup: unblock the thread's long wait so it doesn't linger.
    shutdown.set()
    hb.join(timeout=1)


def test_auth_error_flag_resets_after_success():
    """A success between two AuthErrors resets the confirm-once flag."""
    client = MagicMock(spec=BackendClient)
    client.heartbeat.side_effect = [
        BackendClient.AuthError("forgot runner"),
        None,
        BackendClient.AuthError("forgot runner again"),
    ]
    shutdown = threading.Event()
    hb = HeartbeatThread(
        client=client,
        runner_id="rn_1",
        interval_sec=0.05,
        shutdown_event=shutdown,
    )

    hb.start()
    # Calls happen at t=0, 0.05, 0.10. Sleep past that point — deflaked: the
    # side_effect sequence guarantees at least these three outcomes fired, so
    # assert call_count >= 3 instead of == 3 (a slow test machine could let a
    # 4th call land before we check, which would exhaust the side_effect list
    # and raise StopIteration into the thread's generic `except Exception`;
    # that's tolerated — it logs and continues without touching the flag).
    time.sleep(0.12)

    # The intervening success reset the flag, so the third call (another
    # single AuthError) must not trigger shutdown either.
    assert not shutdown.is_set()
    assert hb.is_alive()
    assert hb._auth_failed_once
    assert client.heartbeat.call_count >= 3

    shutdown.set()
    hb.join(timeout=1)
