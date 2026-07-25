import os
import queue
import signal
import threading

import pytest

import main
import runner.config
from runner.active_jobs import ActiveJobTracker
from runner.client import BackendAuthError, RunnerConfig, RunnerIdentity
from runner.result_sender import (
    AbortRequest,
    CompleteRequest,
    ResultSenderThread,
)


class FakeComponent:
    """Records stop()/join() calls into a shared ordered log."""

    def __init__(self, log, name, alive_after_join=False):
        self._log = log
        self._name = name
        self._alive_after_join = alive_after_join
        self.join_timeouts = []

    def stop(self):
        self._log.append(f'{self._name}.stop')

    def join(self, timeout=None):
        self._log.append(f'{self._name}.join')
        self.join_timeouts.append(timeout)

    def is_alive(self):
        return self._alive_after_join


class FakeDispatcher(FakeComponent):

    def __init__(self, log, victims, has_jobs_seq, always_has_jobs=False):
        super().__init__(log, 'dispatcher')
        self._victims = victims
        self._has_jobs_seq = list(has_jobs_seq)
        self._always_has_jobs = always_has_jobs

    def stop_accepting(self):
        self._log.append('dispatcher.stop_accepting')

    def drain_unstarted(self):
        self._log.append('dispatcher.drain_unstarted')
        return self._victims

    def has_jobs(self):
        # Logged like the other calls so tests can pin *when* the in-flight
        # wait happened relative to dispatcher.stop.
        self._log.append('dispatcher.has_jobs')
        if self._always_has_jobs:
            return True
        return self._has_jobs_seq.pop(0) if self._has_jobs_seq else False


def test_drain_orders_shutdown_and_aborts_unstarted():
    log = []
    victims = [('jb_1', 'sub_1'), ('jb_2', 'sub_2')]
    poller = FakeComponent(log, 'poller')
    dispatcher = FakeDispatcher(log, victims, [True, True, False])
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')
    result_queue = queue.Queue()

    rc = main.drain(
        poller,
        dispatcher,
        sender,
        heartbeat,
        result_queue,
        sleep=lambda s: None,
    )

    assert rc == 0
    # poller fully stopped before the dispatcher is scanned for victims.
    assert log.index('poller.stop') < log.index('poller.join')
    assert log.index('poller.join') < log.index('dispatcher.drain_unstarted')
    # The latch comes FIRST -- before the poller is even asked to stop. Any
    # later and there is a window where the poller finishes a prep, dispatches,
    # and the run loop starts that job while the dispatcher still accepts work:
    # judging that begins after shutdown, which the scan then leaves alone
    # because it looks in-flight.
    assert log[0] == 'dispatcher.stop_accepting', log
    assert log.index('dispatcher.stop_accepting') < log.index('poller.stop')
    # both unstarted jobs aborted with reason 'drain'.
    aborts = []
    while not result_queue.empty():
        aborts.append(result_queue.get_nowait())
    assert all(
        isinstance(a, AbortRequest) and a.reason == 'drain' for a in aborts)
    assert {(a.job_id, a.submission_id) for a in aborts} == set(victims)
    # dispatcher stops before sender, sender before heartbeat.
    assert log.index('dispatcher.stop') < log.index('sender.stop')
    assert log.index('sender.stop') < log.index('heartbeat.stop')
    # sender.stop precedes sender.join (drains the queue before joining).
    assert log.index('sender.stop') < log.index('sender.join')
    # every join is capped by the shared budget, never unbounded.
    for component in (poller, dispatcher, sender, heartbeat):
        assert component.join_timeouts and all(
            t is not None and t > 0 for t in component.join_timeouts)


def test_drain_stops_dispatcher_only_after_the_in_flight_wait():
    # The dispatcher has to outlive the whole has_jobs() wait. A job whose
    # first case is in flight still needs the run loop alive to dispatch its
    # remaining cases; stop the dispatcher before the wait and that job never
    # completes, has_jobs() never clears, the drain burns its entire budget,
    # and the results already computed are thrown away.
    log = []
    poller = FakeComponent(log, 'poller')
    # At least one True, so the wait loop genuinely iterates instead of
    # falling straight through and passing for the wrong reason.
    dispatcher = FakeDispatcher(log, [], [True, True, False])
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')

    rc = main.drain(
        poller,
        dispatcher,
        sender,
        heartbeat,
        queue.Queue(),
        sleep=lambda s: None,
    )

    assert rc == 0
    assert log.count('dispatcher.has_jobs') >= 2, log
    last_wait = len(log) - 1 - log[::-1].index('dispatcher.has_jobs')
    assert last_wait < log.index('dispatcher.stop'), log


def test_drain_budget_is_shared_not_per_component():
    # Each join must get what is LEFT of the budget, not a fresh copy of it.
    # With a fresh copy per component, four slow components could stretch the
    # drain to 4x the budget and blow past docker's stop grace period.
    log = []
    poller = FakeComponent(log, 'poller')
    dispatcher = FakeDispatcher(log, [], [False])
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')
    # Each monotonic() call advances one second, so the budget visibly drains.
    ticks = iter(range(1000))

    main.drain(
        poller,
        dispatcher,
        sender,
        heartbeat,
        queue.Queue(),
        sleep=lambda s: None,
        timeout_sec=100.0,
        monotonic=lambda: float(next(ticks)),
    )

    granted = [
        component.join_timeouts[0]
        for component in (dispatcher, sender, heartbeat)
    ]
    assert granted == sorted(granted, reverse=True), granted
    assert granted[0] > granted[-1]
    # The poller is outside that ordering on purpose: it is capped at its own
    # share of the budget, so it can be granted less than the components that
    # join after it.
    assert poller.join_timeouts[0] <= 100.0 * main.POLLER_JOIN_SHARE


def test_drain_keeps_a_reserve_for_reporting():
    # A long-running in-flight job must not be able to consume the budget the
    # reporting tail needs. If the wait ran to the last second, the sender
    # would be joined with nothing left and every complete already computed --
    # plus the drain aborts just queued -- would go unsent.
    log = []
    poller = FakeComponent(log, 'poller')
    dispatcher = FakeDispatcher(log, [], [], always_has_jobs=True)
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')
    ticks = iter(range(1000))

    main.drain(
        poller,
        dispatcher,
        sender,
        heartbeat,
        queue.Queue(),
        sleep=lambda s: None,
        timeout_sec=100.0,
        monotonic=lambda: float(next(ticks)),
    )

    assert sender.join_timeouts[0] > 0, 'sender left with no time to flush'
    assert heartbeat.join_timeouts[0] > 0


def test_drain_rescans_when_the_poller_outlived_its_share():
    # A poller that blew its share is still running, so it can dispatch after
    # the first scan. Those jobs cannot start (the latch), but without a second
    # scan they are never handed back either -- silently dropped rather than
    # requeued attempt-neutrally.
    log = []
    poller = FakeComponent(log, 'poller', alive_after_join=True)
    late = [('jb_late', 'sub_late')]
    dispatcher = FakeDispatcher(log, late, [False])
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')
    result_queue = queue.Queue()

    main.drain(
        poller,
        dispatcher,
        sender,
        heartbeat,
        result_queue,
        sleep=lambda s: None,
    )

    assert log.count('dispatcher.drain_unstarted') == 2, log
    # The rescan has to happen while the sender can still send it.
    assert log.index('sender.stop') > len(log) - 1 - log[::-1].index(
        'dispatcher.drain_unstarted')
    aborts = []
    while not result_queue.empty():
        aborts.append(result_queue.get_nowait())
    assert [(a.job_id, a.reason) for a in aborts] == [('jb_late', 'drain')] * 2


def test_drain_does_not_rescan_when_the_poller_finished():
    log = []
    poller = FakeComponent(log, 'poller')
    dispatcher = FakeDispatcher(log, [], [False])

    main.drain(
        poller,
        dispatcher,
        FakeComponent(log, 'sender'),
        FakeComponent(log, 'heartbeat'),
        queue.Queue(),
        sleep=lambda s: None,
    )

    assert log.count('dispatcher.drain_unstarted') == 1, log


def test_drain_gives_up_on_in_flight_jobs_when_budget_runs_out():
    # Waiting forever would hand the decision to docker's SIGKILL and lose the
    # results already queued; the budget makes the drain finish on its own and
    # leave the stragglers to lease expiry.
    log = []
    dispatcher = FakeDispatcher(log, [], [], always_has_jobs=True)
    poller = FakeComponent(log, 'poller')
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')

    rc = main.drain(
        poller,
        dispatcher,
        sender,
        heartbeat,
        queue.Queue(),
        sleep=lambda s: None,
        timeout_sec=0.05,
    )

    assert rc == 0
    # It still shut everything down in order rather than bailing out early.
    assert log.index('dispatcher.stop') < log.index('sender.stop')
    assert log.index('sender.stop') < log.index('heartbeat.stop')
    assert 'heartbeat.join' in log


def test_drain_logs_components_that_outlive_the_budget(caplog):
    log = []
    poller = FakeComponent(log, 'poller', alive_after_join=True)
    dispatcher = FakeDispatcher(log, [], [False])
    sender = FakeComponent(log, 'sender')
    heartbeat = FakeComponent(log, 'heartbeat')

    with caplog.at_level('ERROR'):
        rc = main.drain(
            poller,
            dispatcher,
            sender,
            heartbeat,
            queue.Queue(),
            sleep=lambda s: None,
            timeout_sec=0.05,
        )

    assert rc == 0
    assert 'poller outlived the budget' in caplog.text


class RecordingResultClient:
    """Records what the sender actually reported to the backend."""

    def __init__(self):
        self.completes = []
        self.aborts = []

    def complete(self, identity, job_id, tasks):
        self.completes.append(job_id)

    def abort(self, identity, job_id, reason):
        self.aborts.append((job_id, reason))


def test_result_sender_flushes_the_queue_after_stop():
    # Drain step 6 hands the sender everything it collected -- the completes
    # from the in-flight jobs and the drain aborts from the scan -- and then
    # stops it. If stop() discarded what was still queued, the drain would
    # silently lose results it had already computed. So this uses the REAL
    # sender: the fakes in the drain-ordering tests cannot see this.
    client = RecordingResultClient()
    result_queue = queue.Queue()
    tracker = ActiveJobTracker()
    identity = RunnerIdentity(
        'rn_x',
        'tok',
        RunnerConfig(
            heartbeat_interval_sec=60,
            poll_interval_sec=1,
            max_concurrent_jobs=8,
        ),
    )
    for i in range(3):
        tracker.add(f'jb_{i}')
        result_queue.put(CompleteRequest(f'jb_{i}', f'sub_{i}', []))
    tracker.add('jb_late')
    result_queue.put(AbortRequest('jb_late', 'sub_late', 'drain'))

    sender = ResultSenderThread(
        client,
        identity,
        tracker,
        result_queue,
        cleanup=lambda job_id: None,
        backup=lambda job_id: None,
        queue_poll_sec=0.01,
    )
    # Everything is queued before the thread ever runs, so "stop() was already
    # requested when the work arrived" is guaranteed rather than raced.
    sender.stop()
    sender.start()
    try:
        sender.join(timeout=10.0)
        assert not sender.is_alive(), 'sender never exited after stop()'
    finally:
        # A failed assertion above must not leave the thread behind.
        sender.stop()

    assert client.completes == ['jb_0', 'jb_1', 'jb_2']
    assert client.aborts == [('jb_late', 'drain')]
    assert tracker.snapshot() == []


def test_missing_token_returns_1_without_registering(monkeypatch):
    monkeypatch.setattr(runner.config, 'RUNNER_REGISTRATION_TOKEN', '')

    def _boom(*args, **kwargs):
        raise AssertionError('must not register without a token')

    monkeypatch.setattr(main, 'register_with_backoff', _boom)

    assert main.main() == 1


def test_registration_401_returns_1(monkeypatch):
    monkeypatch.setattr(runner.config, 'RUNNER_REGISTRATION_TOKEN', 'tok')

    def _reject(*args, **kwargs):
        raise BackendAuthError('nope', status_code=401)

    monkeypatch.setattr(main, 'register_with_backoff', _reject)

    assert main.main() == 1


class RejectingClient:

    def register(self, registration_token, name):
        import requests
        raise requests.ConnectionError('backend down')


def test_interruptible_registration_sleep_raises_on_shutdown(monkeypatch):
    # A docker stop during a registration retry must abort immediately rather
    # than sleep through the backoff.
    monkeypatch.setattr(runner.config, 'RUNNER_REGISTRATION_TOKEN', 'tok')
    shutdown = threading.Event()
    shutdown.set()

    with pytest.raises(SystemExit):
        main.register(RejectingClient(), shutdown)


def test_install_signal_handlers_sets_shutdown_on_sigterm():
    # docker stop sends a real SIGTERM; exercise that path rather than calling
    # the handler directly.
    shutdown = threading.Event()
    previous_term = signal.getsignal(signal.SIGTERM)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
        main.install_signal_handlers(shutdown)
        os.kill(os.getpid(), signal.SIGTERM)
        assert shutdown.wait(1.0)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
