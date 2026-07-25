import os
import queue
import signal
import threading

import pytest

import main
import runner.config
from runner.client import BackendAuthError
from runner.result_sender import AbortRequest


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

    def drain_unstarted(self):
        self._log.append('dispatcher.drain_unstarted')
        return self._victims

    def has_jobs(self):
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
