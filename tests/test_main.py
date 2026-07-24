import queue
import threading

import pytest

import main
import runner.config
from runner.client import BackendAuthError
from runner.result_sender import AbortRequest


class FakeComponent:
    """Records stop()/join() calls into a shared ordered log."""

    def __init__(self, log, name):
        self._log = log
        self._name = name

    def stop(self):
        self._log.append(f'{self._name}.stop')

    def join(self):
        self._log.append(f'{self._name}.join')


class FakeDispatcher(FakeComponent):

    def __init__(self, log, victims, has_jobs_seq):
        super().__init__(log, 'dispatcher')
        self._victims = victims
        self._has_jobs_seq = list(has_jobs_seq)

    def drain_unstarted(self):
        self._log.append('dispatcher.drain_unstarted')
        return self._victims

    def has_jobs(self):
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
