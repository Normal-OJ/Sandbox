import threading
import time

import requests

from runner.client import (
    BackendAPIError,
    BackendAuthError,
    RunnerConfig,
    RunnerIdentity,
)
from runner.heartbeat import HeartbeatThread
from runner.active_jobs import ActiveJobTracker


class ScriptedClient:
    """heartbeat() plays back scripted outcomes and records snapshots."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.snapshots = []
        self.identities = []

    def heartbeat(self, identity, active_job_ids):
        self.identities.append(identity)
        self.snapshots.append(list(active_job_ids))
        if self._outcomes:
            outcome = self._outcomes.pop(0)
        else:
            outcome = None
        if isinstance(outcome, Exception):
            raise outcome


def make_identity(interval=15):
    return RunnerIdentity('rn_x', 'rk_tok', RunnerConfig(interval, 3, 8))


class FatalFlag:

    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1


def build_thread(outcomes, tracker=None, interval_sec=15):
    client = ScriptedClient(outcomes)
    identity = make_identity()
    tracker = tracker if tracker is not None else ActiveJobTracker()
    fatal = FatalFlag()
    hb = HeartbeatThread(
        client,
        identity,
        tracker,
        on_fatal=fatal,
        interval_sec=interval_sec,
    )
    return hb, client, fatal


def test_beat_sends_tracker_snapshot():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    hb, client, _ = build_thread([None], tracker=tracker)

    fatal_triggered = hb._beat()

    assert fatal_triggered is False
    assert client.snapshots == [['jb_1']]


def test_single_401_does_not_trigger_fatal():
    hb, client, fatal = build_thread(
        [BackendAuthError('nope', status_code=401)])

    fatal_triggered = hb._beat()

    assert fatal_triggered is False
    assert fatal.count == 0


def test_two_consecutive_401_triggers_fatal_once():
    hb, client, fatal = build_thread([
        BackendAuthError('nope', status_code=401),
        BackendAuthError('nope', status_code=401),
    ])

    assert hb._beat() is False
    assert hb._beat() is True

    assert fatal.count == 1


def test_401_then_success_then_401_does_not_trigger_fatal():
    hb, client, fatal = build_thread([
        BackendAuthError('nope', status_code=401),
        None,  # success resets the counter
        BackendAuthError('nope', status_code=401),
    ])

    assert hb._beat() is False  # first 401
    assert hb._beat() is False  # success resets
    assert hb._beat() is False  # isolated 401 again

    assert fatal.count == 0


def test_401_then_non_401_error_resets_counter():
    hb, client, fatal = build_thread([
        BackendAuthError('nope', status_code=401),
        BackendAPIError('boom', status_code=500),
        BackendAuthError('nope', status_code=401),
    ])

    assert hb._beat() is False
    assert hb._beat() is False  # non-401 error resets
    assert hb._beat() is False

    assert fatal.count == 0


def test_non_401_errors_keep_thread_alive():
    hb, client, fatal = build_thread([
        BackendAPIError('boom', status_code=500),
        requests.ConnectionError('down'),
        None,
    ])

    assert hb._beat() is False
    assert hb._beat() is False
    assert hb._beat() is False

    assert fatal.count == 0


def test_real_thread_beats_and_stops_promptly():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    hb, client, fatal = build_thread(
        [None] * 50,
        tracker=tracker,
        interval_sec=0.01,
    )

    hb.start()
    # Give it time to emit several beats.
    time.sleep(0.1)
    t0 = time.time()
    hb.stop()
    hb.join(timeout=1.0)
    elapsed = time.time() - t0

    assert not hb.is_alive()
    assert elapsed < 0.5  # stop() interrupts the wait promptly
    assert len(client.snapshots) >= 2
    assert client.snapshots[0] == ['jb_1']
    assert fatal.count == 0


def test_real_thread_fails_fast_on_two_401():
    fatal_event = threading.Event()

    class EventClient:

        def heartbeat(self, identity, active_job_ids):
            raise BackendAuthError('nope', status_code=401)

    identity = make_identity()
    hb = HeartbeatThread(
        EventClient(),
        identity,
        ActiveJobTracker(),
        on_fatal=fatal_event.set,
        interval_sec=0.01,
    )

    hb.start()
    assert fatal_event.wait(timeout=1.0)
    hb.join(timeout=1.0)
    assert not hb.is_alive()


class FakeClock:
    """Stands in for the time module; only monotonic() is used."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now


class RecordingStopEvent:
    """Duck-types threading.Event; stops the loop after the first wait."""

    def __init__(self):
        self.waits = []
        self._stopped = False

    def is_set(self):
        return self._stopped

    def wait(self, timeout=None):
        self.waits.append(timeout)
        self._stopped = True
        return False

    def set(self):
        self._stopped = True


class SlowClient:
    """heartbeat() advances the fake clock to simulate a slow request."""

    def __init__(self, clock, cost_sec):
        self._clock = clock
        self._cost = cost_sec

    def heartbeat(self, identity, active_job_ids):
        self._clock.now += self._cost


def run_one_cycle(monkeypatch, beat_cost_sec, interval_sec):
    clock = FakeClock()
    monkeypatch.setattr('runner.heartbeat.time', clock)
    hb = HeartbeatThread(
        SlowClient(clock, beat_cost_sec),
        make_identity(),
        ActiveJobTracker(),
        on_fatal=FatalFlag(),
        interval_sec=interval_sec,
    )
    stop_event = RecordingStopEvent()
    hb._stop_event = stop_event
    hb.run()
    return stop_event.waits


def test_wait_subtracts_beat_duration(monkeypatch):
    # A beat that takes 10s must shrink the following wait to 5s so the
    # next beat stays on the fixed 15s cadence (lease-renewal budget).
    waits = run_one_cycle(monkeypatch, beat_cost_sec=10, interval_sec=15)
    assert waits == [5]


def test_wait_clamps_to_zero_when_beat_exceeds_interval(monkeypatch):
    waits = run_one_cycle(monkeypatch, beat_cost_sec=40, interval_sec=15)
    assert waits == [0.0]


def test_default_interval_comes_from_identity_config():
    client = ScriptedClient([None])
    identity = RunnerIdentity('rn_x', 'rk_tok', RunnerConfig(42, 3, 8))
    hb = HeartbeatThread(
        client,
        identity,
        ActiveJobTracker(),
        on_fatal=FatalFlag(),
    )
    assert hb._interval_sec == 42
