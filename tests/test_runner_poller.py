import io
import queue
import threading
import time

import pytest
import requests

from dispatcher.exception import DispatcherDrainingError
from runner.client import (
    BackendAPIError,
    BackendAuthError,
    JobPayload,
    RunnerConfig,
    RunnerIdentity,
)
from runner.active_jobs import ActiveJobTracker
from runner.result_sender import AbortRequest
from runner.config import PREP_MAX_ATTEMPTS
from runner.poller import PollerThread, prepare_job
import runner.poller as poller_mod


def make_identity(max_concurrent=8, poll_interval=3):
    return RunnerIdentity('rn_x', 'rk_tok',
                          RunnerConfig(15, poll_interval, max_concurrent))


def make_payload(job_id='jb_1', submission_id='sub_1', problem_id=42):
    return JobPayload(
        job_id=job_id,
        submission_id=submission_id,
        problem_id=problem_id,
        language=2,
        code_url='http://minio/code.zip',
        checker=None,
        tasks=[{
            'taskScore': 100,
            'memoryLimit': 65536,
            'timeLimit': 1000,
            'caseCount': 1,
        }],
    )


class ScriptedClient:
    """next_job() pops scripted outcomes (JobPayload / None / Exception)."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.call_count = 0

    def next_job(self, identity):
        self.call_count += 1
        outcome = self._outcomes.pop(0) if self._outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class DispatchRecorder:

    def __init__(self, fail_times=0, exc=None, superseded=False):
        self.calls = []
        self._fail_times = fail_times
        self._exc = exc or RuntimeError('dispatch failed')
        # Mirrors Dispatcher.handle()'s return value: True when this dispatch
        # replaced a previous generation of the same job_id.
        self._superseded = superseded

    def __call__(self, job_id, submission_id):
        self.calls.append((job_id, submission_id))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise self._exc
        return self._superseded


class PrepareRecorder:

    def __init__(self, tracker, fail_times=0, exc=None):
        self._tracker = tracker
        self.calls = []
        self.tracker_at_call = []
        self._fail_times = fail_times
        self._exc = exc or RuntimeError('prep failed')

    def __call__(self, payload):
        self.calls.append(payload)
        self.tracker_at_call.append(self._tracker.snapshot())
        if self._fail_times > 0:
            self._fail_times -= 1
            raise self._exc


class SleepRecorder:

    def __init__(self):
        self.slept = []

    def __call__(self, seconds):
        self.slept.append(seconds)


def build_poller(outcomes,
                 *,
                 tracker=None,
                 identity=None,
                 prepare=None,
                 dispatch=None):
    tracker = tracker if tracker is not None else ActiveJobTracker()
    identity = identity if identity is not None else make_identity()
    client = ScriptedClient(outcomes)
    dispatch = dispatch if dispatch is not None else DispatchRecorder()
    q = queue.Queue()
    sleep = SleepRecorder()
    poller = PollerThread(
        client,
        identity,
        tracker,
        q,
        dispatch,
        prepare=prepare if prepare is not None else PrepareRecorder(tracker),
        sleep=sleep,
    )
    return poller, client, tracker, q, sleep, dispatch


def test_capacity_gate_returns_true_no_next_job_call():
    tracker = ActiveJobTracker()
    tracker.add('a')
    tracker.add('b')
    identity = make_identity(max_concurrent=2)
    poller, client, _, q, _, _ = build_poller([],
                                              tracker=tracker,
                                              identity=identity)

    assert poller._poll_once() is True
    assert client.call_count == 0
    assert q.empty()


def test_204_returns_true_tracker_untouched():
    poller, client, tracker, q, _, _ = build_poller([None])

    assert poller._poll_once() is True
    assert client.call_count == 1
    assert len(tracker) == 0
    assert q.empty()


def test_api_error_returns_true_loop_survives():
    poller, _, tracker, q, _, _ = build_poller(
        [BackendAPIError('boom', status_code=500)])

    assert poller._poll_once() is True
    assert len(tracker) == 0
    assert q.empty()


def test_connection_error_returns_true():
    poller, _, _, q, _, _ = build_poller([requests.ConnectionError('down')])
    assert poller._poll_once() is True
    assert q.empty()


def test_auth_error_returns_true():
    poller, _, _, q, _, _ = build_poller(
        [BackendAuthError('nope', status_code=401)])
    assert poller._poll_once() is True
    assert q.empty()


def test_success_adds_to_tracker_before_prepare():
    tracker = ActiveJobTracker()
    prepare = PrepareRecorder(tracker)
    dispatch = DispatchRecorder()
    poller, client, tracker, q, _, _ = build_poller([make_payload()],
                                                    tracker=tracker,
                                                    prepare=prepare,
                                                    dispatch=dispatch)

    result = poller._poll_once()

    assert result is False
    # tracker.add happened BEFORE prepare was invoked.
    assert prepare.tracker_at_call == [['jb_1']]
    assert len(prepare.calls) == 1
    assert dispatch.calls == [('jb_1', 'sub_1')]
    assert q.empty()
    assert tracker.snapshot() == ['jb_1']


def test_prepare_fails_once_then_succeeds():
    tracker = ActiveJobTracker()
    prepare = PrepareRecorder(tracker, fail_times=1)
    dispatch = DispatchRecorder()
    poller, _, tracker, q, sleep, _ = build_poller([make_payload()],
                                                   tracker=tracker,
                                                   prepare=prepare,
                                                   dispatch=dispatch)

    result = poller._poll_once()

    assert result is False
    assert len(prepare.calls) == 2
    assert dispatch.calls == [('jb_1', 'sub_1')]
    assert q.empty()
    assert sleep.slept == [1]  # PREP_BACKOFF_SCHEDULE[0]


def test_prepare_fails_all_attempts_queues_abort():
    tracker = ActiveJobTracker()
    prepare = PrepareRecorder(tracker, fail_times=3)
    dispatch = DispatchRecorder()
    poller, _, tracker, q, sleep, _ = build_poller([make_payload()],
                                                   tracker=tracker,
                                                   prepare=prepare,
                                                   dispatch=dispatch)

    result = poller._poll_once()

    assert result is False
    assert len(prepare.calls) == 3
    assert dispatch.calls == []  # never reached a successful dispatch
    item = q.get_nowait()
    assert isinstance(item, AbortRequest)
    assert item.job_id == 'jb_1'
    assert item.submission_id == 'sub_1'
    assert item.reason == 'prep_failed'
    assert sleep.slept == [1, 2]  # PREP_BACKOFF_SCHEDULE
    # Still in tracker: the sender removes it after the abort resolves.
    assert tracker.snapshot() == ['jb_1']


def test_dispatch_failure_aborts_without_retry():
    # A failed handle() may have partially enqueued task entries; calling
    # it again for the same job_id would revive them (duplicate execution),
    # so dispatch gets exactly one shot.
    tracker = ActiveJobTracker()
    prepare = PrepareRecorder(tracker)
    dispatch = DispatchRecorder(fail_times=1, exc=queue.Full())
    poller, _, tracker, q, sleep, _ = build_poller([make_payload()],
                                                   tracker=tracker,
                                                   prepare=prepare,
                                                   dispatch=dispatch)

    result = poller._poll_once()

    assert result is False
    assert len(prepare.calls) == 1
    assert len(dispatch.calls) == 1
    assert sleep.slept == []
    item = q.get_nowait()
    assert isinstance(item, AbortRequest)
    assert item.reason == 'prep_failed'
    # Still in tracker: the sender removes it as it finalizes the abort.
    assert tracker.snapshot() == ['jb_1']


def test_real_thread_polls_and_stops_promptly():
    tracker = ActiveJobTracker()
    client = ScriptedClient([None] * 100)
    poller = PollerThread(
        client,
        make_identity(poll_interval=0.01),
        tracker,
        queue.Queue(),
        DispatchRecorder(),
        prepare=PrepareRecorder(tracker),
        poll_interval_sec=0.01,
    )

    poller.start()
    time.sleep(0.1)
    t0 = time.time()
    poller.stop()
    poller.join(timeout=1.0)
    elapsed = time.time() - t0

    assert not poller.is_alive()
    assert elapsed < 0.5
    assert client.call_count >= 2


def test_run_survives_unexpected_error_from_poll():
    # A malformed 200 body raises KeyError inside next_job; the run loop
    # must log it and keep polling, not die silently.
    tracker = ActiveJobTracker()
    dispatch = DispatchRecorder()
    client = ScriptedClient([KeyError('job_id'), make_payload()])
    poller = PollerThread(
        client,
        make_identity(),
        tracker,
        queue.Queue(),
        dispatch,
        prepare=PrepareRecorder(tracker),
        poll_interval_sec=0.01,
    )

    poller.start()
    deadline = time.time() + 1.0
    while not dispatch.calls and time.time() < deadline:
        time.sleep(0.01)
    poller.stop()
    poller.join(timeout=1.0)

    assert not poller.is_alive()
    assert dispatch.calls == [('jb_1', 'sub_1')]


# --- drain behaviour ---


def test_stop_during_prep_returns_claim_as_drain():
    # stop() can land while the claim HTTP call is in flight, so the prep loop
    # is the last place that can still return the claim. It must go back as
    # 'drain' (attempt-neutral), never 'prep_failed'.
    tracker = ActiveJobTracker()
    dispatch = DispatchRecorder()
    holder = {}

    class StopThenFail:

        def __init__(self):
            self.calls = []

        def __call__(self, payload):
            self.calls.append(payload)
            holder['poller']._stop_event.set()
            raise RuntimeError('prep failed')

    prepare = StopThenFail()
    poller, _, tracker, q, _, _ = build_poller([make_payload()],
                                               tracker=tracker,
                                               prepare=prepare,
                                               dispatch=dispatch)
    holder['poller'] = poller

    assert poller._poll_once() is False
    assert len(prepare.calls) == 1  # no further attempt after stop()
    assert dispatch.calls == []
    item = q.get_nowait()
    assert isinstance(item, AbortRequest)
    assert item.job_id == 'jb_1'
    assert item.submission_id == 'sub_1'
    assert item.reason == 'drain'
    assert q.empty()
    # Still in tracker: the sender removes it as it finalizes the abort.
    assert tracker.snapshot() == ['jb_1']


def test_stop_during_final_prep_attempt_returns_claim_as_drain():
    # stop() landing during the LAST attempt lands after the loop's own check
    # has run for the last time; the exhausted path has to re-check, or the
    # claim burns an attempt exactly when a rolling restart is under way.
    tracker = ActiveJobTracker()
    dispatch = DispatchRecorder()
    holder = {}

    class FailAndStopOnLastAttempt:

        def __init__(self):
            self.calls = []

        def __call__(self, payload):
            self.calls.append(payload)
            if len(self.calls) == PREP_MAX_ATTEMPTS:
                holder['poller']._stop_event.set()
            raise RuntimeError('prep failed')

    prepare = FailAndStopOnLastAttempt()
    poller, _, tracker, q, _, _ = build_poller([make_payload()],
                                               tracker=tracker,
                                               prepare=prepare,
                                               dispatch=dispatch)
    holder['poller'] = poller

    assert poller._poll_once() is False
    assert len(prepare.calls) == PREP_MAX_ATTEMPTS
    assert dispatch.calls == []
    item = q.get_nowait()
    assert isinstance(item, AbortRequest)
    assert item.reason == 'drain'
    assert q.empty()


def test_dispatch_refused_by_draining_dispatcher_aborts_as_drain():
    # A prep that finishes after the drain latched the dispatcher still tries
    # to dispatch; handle() refuses it without publishing anything. That claim
    # never started any work, so it must go back as 'drain' (attempt-neutral),
    # never 'prep_failed' -- otherwise every rolling restart would burn an
    # attempt for the claim that lost this race.
    tracker = ActiveJobTracker()
    prepare = PrepareRecorder(tracker)
    dispatch = DispatchRecorder(fail_times=1,
                                exc=DispatcherDrainingError('draining'))
    poller, _, tracker, q, sleep, _ = build_poller([make_payload()],
                                                   tracker=tracker,
                                                   prepare=prepare,
                                                   dispatch=dispatch)

    result = poller._poll_once()

    assert result is False
    assert len(dispatch.calls) == 1  # refused once, never retried
    assert sleep.slept == []
    item = q.get_nowait()
    assert isinstance(item, AbortRequest)
    assert item.job_id == 'jb_1'
    assert item.submission_id == 'sub_1'
    assert item.reason == 'drain'
    assert q.empty()
    # Still in tracker: the sender removes it as it finalizes the abort.
    assert tracker.snapshot() == ['jb_1']


def test_supersede_dispatch_releases_the_dead_claims_count():
    # A dispatch that superseded a previous generation (handle() returned
    # True) means the old claim of this job_id can never produce an outcome:
    # nothing else will ever remove its tracker count, so the poller must.
    # Without this, the dead count would hold a capacity slot forever and
    # keep the drain's reporting tail waiting for a report that cannot come.
    tracker = ActiveJobTracker()
    tracker.add('jb_1')  # the dead claim, left over from an expired lease
    prepare = PrepareRecorder(tracker)
    dispatch = DispatchRecorder(superseded=True)
    poller, _, tracker, q, _, _ = build_poller([make_payload()],
                                               tracker=tracker,
                                               prepare=prepare,
                                               dispatch=dispatch)

    assert poller._poll_once() is False

    assert dispatch.calls == [('jb_1', 'sub_1')]
    assert q.empty()
    # One count for the new claim; the dead claim's count was released.
    assert len(tracker) == 1
    assert tracker.snapshot() == ['jb_1']


def test_stop_before_claim_skips_next_job():
    poller, client, tracker, q, _, dispatch = build_poller([make_payload()])
    poller.stop()

    assert poller._poll_once() is True
    assert client.call_count == 0
    assert len(tracker) == 0
    assert q.empty()


def test_prep_backoff_is_interruptible():
    # The production default sleep is stop-aware, so a drain must not have to
    # wait out PREP_BACKOFF_SCHEDULE (1 + 2 seconds). No sleep= injection here
    # on purpose: the default is the thing under test.
    tracker = ActiveJobTracker()
    prepared = threading.Event()

    def always_failing_prepare(payload):
        prepared.set()
        raise RuntimeError('prep failed')

    q = queue.Queue()
    poller = PollerThread(
        ScriptedClient([make_payload()] * 10),
        make_identity(poll_interval=0.01),
        tracker,
        q,
        DispatchRecorder(),
        prepare=always_failing_prepare,
        poll_interval_sec=0.01,
    )

    poller.start()
    assert prepared.wait(1.0)
    t0 = time.time()
    poller.stop()
    poller.join(timeout=1.0)
    elapsed = time.time() - t0

    assert not poller.is_alive()
    assert elapsed < 1.0
    while not q.empty():
        item = q.get_nowait()
        assert isinstance(item, AbortRequest)
        assert item.reason == 'drain'


def test_default_sleep_is_stop_aware():
    poller = PollerThread(
        ScriptedClient([]),
        make_identity(),
        ActiveJobTracker(),
        queue.Queue(),
        DispatchRecorder(),
    )
    assert poller._sleep == poller._stop_event.wait


# --- prepare_job unit tests ---


class StubDownloadResponse:

    def __init__(self, content=b'zipbytes', raises=None):
        self.content = content
        self._raises = raises

    def raise_for_status(self):
        if self._raises is not None:
            raise self._raises


def test_prepare_job_happy_path(tmp_path, monkeypatch):
    ensure_calls = []
    monkeypatch.setattr(poller_mod.testdata, 'ensure_testdata',
                        lambda pid: ensure_calls.append(pid))
    monkeypatch.setattr(poller_mod.testdata, 'get_problem_root',
                        lambda pid: tmp_path / f'root_{pid}')

    get_calls = []

    def fake_get(url, timeout=None):
        get_calls.append((url, timeout))
        return StubDownloadResponse(content=b'the-zip')

    monkeypatch.setattr(poller_mod.requests, 'get', fake_get)
    monkeypatch.setattr(poller_mod, 'SUBMISSION_DIR', tmp_path)

    extract_kwargs = {}
    monkeypatch.setattr(poller_mod.file_manager, 'extract',
                        lambda **kw: extract_kwargs.update(kw))

    payload = make_payload(problem_id=42)
    prepare_job(payload)

    assert ensure_calls == [42]
    assert get_calls[0][0] == 'http://minio/code.zip'
    assert get_calls[0][1] == poller_mod.REQUEST_TIMEOUT
    assert extract_kwargs['root_dir'] == tmp_path
    assert extract_kwargs['job_id'] == 'jb_1'
    meta = extract_kwargs['meta']
    assert int(meta.language) == 2
    assert len(meta.tasks) == 1
    assert isinstance(extract_kwargs['source'], io.BytesIO)
    assert extract_kwargs['source'].getvalue() == b'the-zip'
    assert extract_kwargs['testdata'] == tmp_path / 'root_42'


def test_prepare_job_download_error_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(poller_mod.testdata, 'ensure_testdata',
                        lambda pid: None)
    monkeypatch.setattr(poller_mod.testdata, 'get_problem_root',
                        lambda pid: tmp_path)
    monkeypatch.setattr(poller_mod.requests,
                        'get',
                        lambda url, timeout=None: StubDownloadResponse(
                            raises=requests.HTTPError('404')))
    monkeypatch.setattr(poller_mod, 'SUBMISSION_DIR', tmp_path)
    monkeypatch.setattr(poller_mod.file_manager, 'extract', lambda **kw: None)

    with pytest.raises(requests.HTTPError):
        prepare_job(make_payload())


def test_prepare_job_removes_leftover_job_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(poller_mod.testdata, 'ensure_testdata',
                        lambda pid: None)
    monkeypatch.setattr(poller_mod.testdata, 'get_problem_root',
                        lambda pid: tmp_path / 'root')
    monkeypatch.setattr(
        poller_mod.requests,
        'get',
        lambda url, timeout=None: StubDownloadResponse(content=b'z'))
    monkeypatch.setattr(poller_mod, 'SUBMISSION_DIR', tmp_path)

    seen = {}

    def fake_extract(**kw):
        # By the time extract runs, the leftover dir must be gone.
        seen['job_dir_exists'] = (tmp_path / 'jb_1').exists()

    monkeypatch.setattr(poller_mod.file_manager, 'extract', fake_extract)

    leftover = tmp_path / 'jb_1'
    leftover.mkdir()
    (leftover / 'stale.txt').write_text('old')

    prepare_job(make_payload())

    assert seen['job_dir_exists'] is False
