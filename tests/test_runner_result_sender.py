import queue
import time

import requests

from runner.client import BackendAPIError, BackendAuthError
from runner.active_jobs import ActiveJobTracker
from runner.result_sender import (
    CompleteRequest,
    AbortRequest,
    ResultSenderThread,
    _default_cleanup,
    _default_backup,
)
import runner.result_sender as result_sender


class ScriptedClient:
    """complete()/abort() replay scripted outcomes and record calls."""

    def __init__(self, complete_outcomes=None, abort_outcomes=None):
        self._complete = list(complete_outcomes or [])
        self._abort = list(abort_outcomes or [])
        self.complete_calls = []
        self.abort_calls = []

    def complete(self, identity, job_id, tasks):
        self.complete_calls.append((job_id, tasks))
        if self._complete:
            outcome = self._complete.pop(0)
            if isinstance(outcome, Exception):
                raise outcome

    def abort(self, identity, job_id, reason):
        self.abort_calls.append((job_id, reason))
        if self._abort:
            outcome = self._abort.pop(0)
            if isinstance(outcome, Exception):
                raise outcome


class Recorder:

    def __init__(self, raise_once=None):
        self.calls = []
        self._raise_once = raise_once

    def __call__(self, job_id):
        self.calls.append(job_id)
        if self._raise_once is not None:
            exc = self._raise_once
            self._raise_once = None
            raise exc


class SleepRecorder:

    def __init__(self):
        self.slept = []

    def __call__(self, seconds):
        self.slept.append(seconds)


def build_sender(client, tracker=None, job_id='jb_1'):
    tracker = tracker if tracker is not None else ActiveJobTracker()
    tracker.add(job_id)
    cleanup = Recorder()
    backup = Recorder()
    sleep = SleepRecorder()
    sender = ResultSenderThread(
        client,
        identity=object(),
        tracker=tracker,
        result_queue=queue.Queue(),
        cleanup=cleanup,
        backup=backup,
        sleep=sleep,
    )
    return sender, tracker, cleanup, backup, sleep


def test_complete_204_cleanup_and_tracker_emptied():
    client = ScriptedClient(complete_outcomes=[None])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', [{'status': 0}]))

    assert client.complete_calls == [('jb_1', [{'status': 0}])]
    assert client.abort_calls == []
    assert cleanup.calls == ['jb_1']
    assert backup.calls == []
    assert sleep.slept == []
    assert len(tracker) == 0


def test_complete_409_terminal_no_retry_cleanup():
    client = ScriptedClient(
        complete_outcomes=[BackendAPIError('conflict', status_code=409)])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert client.abort_calls == []
    assert cleanup.calls == ['jb_1']
    assert backup.calls == []
    assert sleep.slept == []
    assert len(tracker) == 0


def test_complete_404_terminal_cleanup():
    client = ScriptedClient(
        complete_outcomes=[BackendAPIError('gone', status_code=404)])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert client.abort_calls == []
    assert cleanup.calls == ['jb_1']
    assert len(tracker) == 0


def test_complete_400_triggers_abort_rejected_with_backup():
    # rejected keeps the job dir as evidence of what the backend refused.
    client = ScriptedClient(
        complete_outcomes=[BackendAPIError('bad', status_code=400)],
        abort_outcomes=[None],
    )
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert client.abort_calls == [('jb_1', 'rejected')]
    assert backup.calls == ['jb_1']
    assert cleanup.calls == []
    assert len(tracker) == 0


def test_complete_400_then_abort_exhausts_backup():
    client = ScriptedClient(
        complete_outcomes=[BackendAPIError('bad', status_code=400)],
        abort_outcomes=[requests.ConnectionError('x')] * 6,
    )
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert client.abort_calls == [('jb_1', 'rejected')] * 6
    assert backup.calls == ['jb_1']
    assert cleanup.calls == []
    assert sleep.slept == [1, 2, 4, 8, 16]
    assert len(tracker) == 0


def test_complete_400_then_abort_409_still_backed_up():
    client = ScriptedClient(
        complete_outcomes=[BackendAPIError('bad', status_code=400)],
        abort_outcomes=[BackendAPIError('conflict', status_code=409)],
    )
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert client.abort_calls == [('jb_1', 'rejected')]
    assert backup.calls == ['jb_1']
    assert cleanup.calls == []
    assert sleep.slept == []
    assert len(tracker) == 0


def test_abort_finalizes_local_state_before_send():
    # The 202 response means the backend has already requeued the job and
    # this runner may re-claim it at once: by the time the abort request
    # goes out, the dir must be gone and the tracker entry removed.
    events = []
    tracker = ActiveJobTracker()
    tracker.add('jb_1')

    class OrderClient:

        def abort(self, identity, job_id, reason):
            events.append(('abort', reason, len(tracker)))

    sender = ResultSenderThread(
        OrderClient(),
        identity=object(),
        tracker=tracker,
        result_queue=queue.Queue(),
        cleanup=lambda job_id: events.append(('cleanup', job_id)),
        backup=lambda job_id: events.append(('backup', job_id)),
        sleep=SleepRecorder(),
    )

    sender._process(AbortRequest('jb_1', 'sub_1', 'prep_failed'))

    # cleanup first, then the send observes an already-empty tracker.
    assert events == [('cleanup', 'jb_1'), ('abort', 'prep_failed', 0)]


def test_abort_exhaustion_does_not_backup():
    # The dir was already finalized before the first send attempt, so
    # exhaustion has nothing left to back up.
    client = ScriptedClient(abort_outcomes=[requests.ConnectionError('x')] * 6)
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(AbortRequest('jb_1', 'sub_1', 'prep_failed'))

    assert cleanup.calls == ['jb_1']
    assert backup.calls == []
    assert sleep.slept == [1, 2, 4, 8, 16]
    assert len(tracker) == 0


def test_finalize_failure_still_sends_abort():
    # An unsent abort would leave the job leased until lease expiry, so a
    # failing cleanup must not block the send (or the tracker removal).
    client = ScriptedClient(abort_outcomes=[None])
    sender, tracker, cleanup, backup, sleep = build_sender(client)
    cleanup._raise_once = PermissionError('denied')

    sender._process(AbortRequest('jb_1', 'sub_1', 'prep_failed'))

    assert client.abort_calls == [('jb_1', 'prep_failed')]
    assert len(tracker) == 0


def test_complete_500_twice_then_success():
    client = ScriptedClient(complete_outcomes=[
        BackendAPIError('err', status_code=500),
        BackendAPIError('err', status_code=500),
        None,
    ])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert len(client.complete_calls) == 3
    assert sleep.slept == [1, 2]
    assert cleanup.calls == ['jb_1']
    assert backup.calls == []


def test_complete_connection_error_exhausts_backup():
    client = ScriptedClient(
        complete_outcomes=[requests.ConnectionError('down')] * 6)
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert len(client.complete_calls) == 6  # initial + 5 retries
    assert sleep.slept == [1, 2, 4, 8, 16]
    assert backup.calls == ['jb_1']
    assert cleanup.calls == []
    assert len(tracker) == 0


def test_complete_401_not_terminal_then_success():
    client = ScriptedClient(complete_outcomes=[
        BackendAuthError('nope', status_code=401),
        None,
    ])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(CompleteRequest('jb_1', 'sub_1', []))

    assert len(client.complete_calls) == 2  # 401 retried, not terminal
    assert sleep.slept == [1]
    assert cleanup.calls == ['jb_1']
    assert backup.calls == []


def test_abort_request_prep_failed_202_cleanup():
    client = ScriptedClient(abort_outcomes=[None])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(AbortRequest('jb_1', 'sub_1', 'prep_failed'))

    assert client.abort_calls == [('jb_1', 'prep_failed')]
    assert cleanup.calls == ['jb_1']
    assert backup.calls == []
    assert len(tracker) == 0


def test_abort_409_terminal_cleanup():
    client = ScriptedClient(
        abort_outcomes=[BackendAPIError('conflict', status_code=409)])
    sender, tracker, cleanup, backup, sleep = build_sender(client)

    sender._process(AbortRequest('jb_1', 'sub_1', 'drain'))

    assert cleanup.calls == ['jb_1']
    assert sleep.slept == []
    assert len(tracker) == 0


def test_raising_cleanup_still_removes_job_from_tracker():
    # A lingering tracker entry would keep the lease renewed and consume
    # capacity forever, so removal must survive a failing cleanup.
    client = ScriptedClient(complete_outcomes=[None])
    sender, tracker, cleanup, backup, sleep = build_sender(client)
    cleanup._raise_once = PermissionError('denied')

    try:
        sender._process(CompleteRequest('jb_1', 'sub_1', []))
    except PermissionError:
        pass

    assert len(tracker) == 0


def test_default_cleanup_tolerates_missing_dir(tmp_path, monkeypatch):
    monkeypatch.setattr('dispatcher.config.SUBMISSION_DIR', tmp_path)
    # Must not raise even though the job dir does not exist.
    _default_cleanup('does_not_exist')


def test_default_backup_tolerates_missing_dir(tmp_path, monkeypatch):
    monkeypatch.setattr('dispatcher.config.SUBMISSION_DIR', tmp_path)
    monkeypatch.setattr('dispatcher.config.SUBMISSION_BACKUP_DIR', tmp_path)
    _default_backup('does_not_exist')


def test_real_thread_drains_queued_items_on_stop():
    client = ScriptedClient(complete_outcomes=[None, None])
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    tracker.add('jb_2')
    cleanup = Recorder()
    q = queue.Queue()
    q.put(CompleteRequest('jb_1', 'sub_1', []))
    q.put(CompleteRequest('jb_2', 'sub_2', []))
    sender = ResultSenderThread(
        client,
        identity=object(),
        tracker=tracker,
        result_queue=q,
        cleanup=cleanup,
        backup=Recorder(),
        queue_poll_sec=0.01,
    )

    sender.start()
    sender.stop()
    sender.join(timeout=2.0)

    assert not sender.is_alive()
    assert sorted(cleanup.calls) == ['jb_1', 'jb_2']
    assert len(tracker) == 0


def test_real_thread_empty_queue_stops_promptly():
    sender = ResultSenderThread(
        ScriptedClient(),
        identity=object(),
        tracker=ActiveJobTracker(),
        result_queue=queue.Queue(),
        cleanup=Recorder(),
        backup=Recorder(),
        queue_poll_sec=0.01,
    )

    sender.start()
    time.sleep(0.05)
    t0 = time.time()
    sender.stop()
    sender.join(timeout=1.0)
    assert not sender.is_alive()
    assert time.time() - t0 < 0.5


def test_bad_item_does_not_kill_thread():
    # cleanup raises once; the second queued item must still be processed.
    client = ScriptedClient(complete_outcomes=[None, None])
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    tracker.add('jb_2')
    cleanup = Recorder(raise_once=RuntimeError('boom'))
    q = queue.Queue()
    q.put(CompleteRequest('jb_1', 'sub_1', []))
    q.put(CompleteRequest('jb_2', 'sub_2', []))
    sender = ResultSenderThread(
        client,
        identity=object(),
        tracker=tracker,
        result_queue=q,
        cleanup=cleanup,
        backup=Recorder(),
        queue_poll_sec=0.01,
    )

    sender.start()
    sender.stop()
    sender.join(timeout=2.0)

    assert not sender.is_alive()
    # jb_1's cleanup raised, jb_2's succeeded.
    assert 'jb_2' in cleanup.calls
