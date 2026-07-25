"""End-to-end drain tests against REAL runner threads.

tests/test_main.py pins the drain *ordering* with synchronous fakes, which
never exercises a live PollerThread, a poller blocked inside prep, or an
actual ResultSenderThread flush. These tests wire the real PollerThread /
ResultSenderThread / HeartbeatThread / Dispatcher together and only fake the
three things that would reach outside the process: the backend HTTP client,
the container executor, and the local job-dir cleanup.
"""

import queue
import threading
import time
from types import SimpleNamespace

import pytest

import main
import dispatcher.dispatcher as dispatcher_mod
from dispatcher.constant import Language
from dispatcher.dispatcher import Dispatcher
from runner.active_jobs import ActiveJobTracker
from runner.client import JobPayload, RunnerConfig, RunnerIdentity
from runner.heartbeat import HeartbeatThread
from runner.poller import PollerThread
from runner.result_sender import CompleteRequest, ResultSenderThread
from tests.test_dispatcher import _write_job_dir

# Same path conftest.py uses; it is absent from the repo, so the Dispatcher
# falls back to its defaults and the test sets what it needs explicitly.
TEST_CONFIG_PATH = '.config/dispatcher.test.json'
# Every blocking wait in this file is bounded, so a regression fails the test
# instead of hanging the suite.
WAIT_TIMEOUT = 10.0


def wait_until(predicate, message, timeout=WAIT_TIMEOUT):
    """Bounded poll-until-deadline; fails loudly instead of hanging."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f'timed out after {timeout}s waiting for {message}')


def release_when(predicate, event, message):
    """Set ``event`` once ``predicate`` holds, from a helper thread.

    Releases have to be event-driven rather than racing a fixed delay: drain
    runs on the test thread, and a slow CI box would otherwise let the release
    land before the drain step it is supposed to follow, silently testing a
    different interleaving. The event is set even on timeout so a wrong
    predicate fails an assertion instead of hanging the drain.
    """

    def _run():
        try:
            wait_until(predicate, message)
        finally:
            event.set()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


def make_payload(job_id, submission_id):
    # python3 (language 2) on purpose: the dispatcher enqueues no Compile job
    # for it, so one payload == exactly one Execute task and the capacity
    # bookkeeping below stays trivial to reason about.
    return JobPayload(
        job_id=job_id,
        submission_id=submission_id,
        problem_id=42,
        language=int(Language.PY),
        code_url='http://minio/code.zip',
        checker=None,
        tasks=[{
            'taskScore': 100,
            'memoryLimit': 65536,
            'timeLimit': 1000,
            'caseCount': 1,
        }],
    )


class RecordingClient:
    """The four backend calls the runner threads make, all recorded.

    ``next_job`` walks a script; a ``threading.Event`` entry in the script is a
    gate the test opens when it wants the *next* claim to happen, which is how
    these tests pin down claim ordering without sleeping. Nothing here raises,
    so the sender's retry path stays out of the picture.
    """

    def __init__(self, script):
        self._lock = threading.Lock()
        self._script = list(script)
        self.completes = []
        self.aborts = []
        self.heartbeats = []
        self.gate_timed_out = False

    def _pop(self):
        with self._lock:
            return self._script.pop(0) if self._script else None

    def next_job(self, identity):
        item = self._pop()
        if isinstance(item, threading.Event):
            # Generous: the dispatcher run loop sleeps a full second when its
            # queue is empty, so the test needs several of those to line up
            # its preconditions before opening the gate.
            if not item.wait(WAIT_TIMEOUT):
                self.gate_timed_out = True
                return None
            item = self._pop()
        return item

    def complete(self, identity, job_id, tasks):
        with self._lock:
            self.completes.append((job_id, tasks))

    def abort(self, identity, job_id, reason):
        with self._lock:
            self.aborts.append((job_id, reason))

    def heartbeat(self, identity, active_job_ids):
        with self._lock:
            self.heartbeats.append(active_job_ids)


class ExecutorRecorder:
    """Shared state for the StubExecutor installed over SubmissionExecutor."""

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.ran = []


def install_stub_executor(monkeypatch, recorder):
    """Replace the container executor with one the test drives by hand."""

    class StubExecutor:

        def __init__(self, job_id, *args, **kwargs):
            self.job_id = job_id

        def run(self):
            with recorder.lock:
                recorder.ran.append(self.job_id)
            recorder.started.set()
            # Blocks until the test releases it, so "in-flight work" is a
            # state the test fully controls. Bounded so a stuck test fails.
            recorder.release.wait(5.0)
            return {
                'Status': 'AC',
                'Stdout': '',
                'Stderr': '',
                'DockerExitCode': 0,
                'Duration': 1,
                'MemUsage': 1,
            }

        def compile(self):
            return {'Status': 'AC'}

    monkeypatch.setattr(dispatcher_mod, 'SubmissionExecutor', StubExecutor)


class Rig:
    """Owns every thread/event so teardown always unblocks and joins them."""

    def __init__(self):
        self.threads = []
        self.events = []

    def add_thread(self, thread):
        self.threads.append(thread)
        return thread

    def add_event(self, event):
        self.events.append(event)
        return event

    def shutdown(self):
        # Release blocked executors / preps first: a thread parked on one of
        # these would otherwise make the joins below time out.
        for event in self.events:
            event.set()
        for thread in reversed(self.threads):
            thread.stop()
        for thread in reversed(self.threads):
            if thread.is_alive():
                thread.join(timeout=WAIT_TIMEOUT)


@pytest.fixture
def rig():
    r = Rig()
    try:
        yield r
    finally:
        # Runs even when an assertion fails, so a broken drain can never hang
        # the rest of the suite.
        r.shutdown()


def build_stack(rig, tmp_path, client, prepare):
    """Real dispatcher + sender + poller + heartbeat, started and tracked."""
    identity = RunnerIdentity(
        'rn_x',
        'tok',
        RunnerConfig(
            heartbeat_interval_sec=60,
            poll_interval_sec=0.01,
            max_concurrent_jobs=8,
        ),
    )
    result_queue = queue.Queue()
    tracker = ActiveJobTracker()

    d = Dispatcher(
        TEST_CONFIG_PATH,
        on_complete=lambda job_id, submission_id, tasks: result_queue.put(
            CompleteRequest(job_id, submission_id, tasks)),
    )
    d.SUBMISSION_DIR = tmp_path
    # testing stays False: on_job_complete's real post-processing (the nested
    # task/case result shape handed to on_complete) is exactly what we assert.
    assert d.testing is False
    # One container at a time. This is what makes the "claimed but never
    # started" state reachable deterministically: while job A's executor is
    # blocked, the run loop sees container_count >= MAX_CONTAINER_SIZE, never
    # calls queue.get(), and job B stays in result/ but out of started/.
    d.MAX_CONTAINER_SIZE = 1

    sender = ResultSenderThread(
        client,
        identity,
        tracker,
        result_queue,
        # Fake both so nothing touches the real submissions/ tree: the default
        # cleanup resolves job dirs against the module-level config path, not
        # the tmp_path we gave the dispatcher.
        cleanup=lambda job_id: None,
        backup=lambda job_id: None,
    )
    poller = PollerThread(
        client,
        identity,
        tracker,
        result_queue,
        dispatch=d.handle,
        prepare=prepare,
    )
    heartbeat = HeartbeatThread(client,
                                identity,
                                tracker,
                                on_fatal=lambda: None)

    for thread in (d, sender, poller, heartbeat):
        rig.add_thread(thread)
        thread.start()
    return d, sender, poller, heartbeat, result_queue, tracker


def test_drain_completes_in_flight_and_returns_unstarted(
    tmp_path,
    monkeypatch,
    rig,
):
    # A started job must still report complete; a claimed-but-never-started
    # job must come back as abort('drain'). Both through the real sender.
    recorder = ExecutorRecorder()
    rig.add_event(recorder.release)
    install_stub_executor(monkeypatch, recorder)

    gate = rig.add_event(threading.Event())
    # The gate sits between A and B so B is only claimed once the test says so.
    client = RecordingClient(
        [make_payload('A', 'sub_a'), gate,
         make_payload('B', 'sub_b')])

    # The poller starts claiming inside build_stack, so prep cannot close over
    # the dispatcher object; it only needs the job-dir root, which is tmp_path.
    job_root = SimpleNamespace(SUBMISSION_DIR=tmp_path)

    def prepare(payload):
        _write_job_dir(job_root, payload.job_id, Language.PY, [1])

    d, sender, poller, heartbeat, result_queue, tracker = build_stack(
        rig, tmp_path, client, prepare)

    # Open the gate only once A's executor is genuinely running AND the
    # container slot is taken AND the dispatcher marked A started. That exact
    # ordering is what guarantees the run loop is saturated when B arrives.
    assert recorder.started.wait(WAIT_TIMEOUT), 'executor for A never ran'
    wait_until(lambda: d.container_count == 1, 'container slot to be taken')
    wait_until(lambda: 'A' in d.started, 'A to be marked started')
    gate.set()

    wait_until(lambda: d.contains('B'), 'B to be handled by the dispatcher')
    assert 'B' not in d.started, 'B must stay unstarted for this test to mean anything'

    # Let A finish only once drain_unstarted() has already returned B (B is
    # gone from the dispatcher). Releasing earlier would free the container
    # slot, let the run loop start B, and quietly turn this into a test of a
    # different interleaving.
    release_when(
        lambda: not d.contains('B'),
        recorder.release,
        'B to be released by drain_unstarted',
    )
    t0 = time.monotonic()
    rc = main.drain(
        poller,
        d,
        sender,
        heartbeat,
        result_queue,
        sleep=lambda s: time.sleep(0.02),
        # Bound the budget: drain runs on the test thread, so a regression must
        # fail the test rather than hang the whole suite.
        timeout_sec=10.0,
    )
    elapsed = time.monotonic() - t0

    assert rc == 0
    assert client.aborts == [('B', 'drain')]
    assert [job_id for job_id, _ in client.completes] == ['A']
    tasks = client.completes[0][1]
    assert len(tasks) == 1 and len(tasks[0]) == 1
    assert tasks[0][0]['status'] == 'AC'
    assert tracker.snapshot() == []
    assert not client.gate_timed_out
    # The dispatcher run loop sleeps up to 1s before noticing stop(), so the
    # bound is generous; the point is that drain returns on its own.
    assert elapsed < 8.0, f'drain took {elapsed:.2f}s'


def test_drain_returns_claim_whose_prep_succeeded_after_stop(
    tmp_path,
    monkeypatch,
    rig,
):
    # The nastier variant: stop() lands while prep is blocked, but prep then
    # SUCCEEDS, so the poller dispatches the job on its way out. Nothing may
    # start judging after shutdown -- the dispatcher latch has to return this
    # job as a drain abort instead, and the executor must never run.
    recorder = ExecutorRecorder()
    rig.add_event(recorder.release)
    install_stub_executor(monkeypatch, recorder)

    client = RecordingClient([make_payload('E', 'sub_e')])
    prep_entered = threading.Event()
    prep_release = rig.add_event(threading.Event())
    job_root = SimpleNamespace(SUBMISSION_DIR=tmp_path)

    def prepare(payload):
        prep_entered.set()
        prep_release.wait(WAIT_TIMEOUT)
        # Succeeds this time: the claim is fully prepped and will be
        # dispatched even though the poller is already stopping.
        _write_job_dir(job_root, payload.job_id, Language.PY, [1])

    d, sender, poller, heartbeat, result_queue, tracker = build_stack(
        rig, tmp_path, client, prepare)

    assert prep_entered.wait(WAIT_TIMEOUT), 'prep was never entered'

    release_when(
        poller._stop_event.is_set,
        prep_release,
        'poller stop to be requested',
    )
    rc = main.drain(
        poller,
        d,
        sender,
        heartbeat,
        result_queue,
        sleep=lambda s: time.sleep(0.02),
        timeout_sec=10.0,
    )

    assert rc == 0
    assert client.aborts == [('E', 'drain')]
    assert client.completes == []
    # Never judged: no work may begin once shutdown was requested.
    assert recorder.ran == []
    assert 'E' not in d.started
    assert tracker.snapshot() == []


def test_drain_returns_claim_blocked_in_prep(tmp_path, monkeypatch, rig):
    # The P1 fix end-to-end: a claim still stuck in prep when SIGTERM lands
    # comes back as 'drain' (attempt-neutral), never 'prep_failed'.
    recorder = ExecutorRecorder()
    rig.add_event(recorder.release)
    install_stub_executor(monkeypatch, recorder)

    client = RecordingClient([make_payload('C', 'sub_c')])
    prep_entered = threading.Event()
    prep_release = rig.add_event(threading.Event())

    def prepare(payload):
        prep_entered.set()
        # Models the slow backend I/O the reviewer described: blocked, then
        # failing, with stop() landing in between.
        prep_release.wait(WAIT_TIMEOUT)
        raise RuntimeError('backend gone')

    d, sender, poller, heartbeat, result_queue, tracker = build_stack(
        rig, tmp_path, client, prepare)

    assert prep_entered.wait(WAIT_TIMEOUT), 'prep was never entered'

    # Unblock prep only after stop() has landed: that is the exact state the
    # P1 fix is about (I/O that fails *while* draining must not be reported as
    # prep_failed). Releasing before stop() would just be an ordinary retry.
    release_when(
        poller._stop_event.is_set,
        prep_release,
        'poller stop to be requested',
    )
    t0 = time.monotonic()
    rc = main.drain(
        poller,
        d,
        sender,
        heartbeat,
        result_queue,
        sleep=lambda s: time.sleep(0.02),
        # Bound the budget: drain runs on the test thread, so a regression must
        # fail the test rather than hang the whole suite.
        timeout_sec=10.0,
    )
    elapsed = time.monotonic() - t0

    assert rc == 0
    assert client.aborts == [('C', 'drain')]
    assert client.completes == []
    assert tracker.snapshot() == []
    assert elapsed < 8.0, f'drain took {elapsed:.2f}s'
