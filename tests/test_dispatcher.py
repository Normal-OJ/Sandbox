import json
import threading
import time

import pytest

from dispatcher import job
from dispatcher.constant import Language
from dispatcher.dispatcher import Dispatcher
from tests.submission_generator import SubmissionGenerator


def _write_job_dir(dispatcher, job_id, language, case_counts):
    '''Lay out a minimal job dir (meta.json only) so handle() can read it.'''
    job_dir = dispatcher.SUBMISSION_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    n = len(case_counts)
    # taskScore must sum to 100 (Meta validator); spread it evenly.
    scores = [100 // n] * n
    scores[-1] += 100 - sum(scores)
    tasks = [{
        'taskScore': scores[i],
        'memoryLimit': 65536,
        'timeLimit': 1000,
        'caseCount': c,
    } for i, c in enumerate(case_counts)]
    (job_dir / 'meta.json').write_text(
        json.dumps({
            'language': int(language),
            'tasks': tasks,
        }))


def test_create_dispatcher():
    docker_dispatcher = Dispatcher()
    assert docker_dispatcher is not None
    # Daemon: fail-fast exit paths (heartbeat 401) return from main() without
    # stopping the dispatcher; a non-daemon thread would keep the interpreter
    # alive forever and the restart policy would never re-register.
    assert docker_dispatcher.daemon


def test_start_dispatcher(docker_dispatcher: Dispatcher):
    docker_dispatcher.start()


def test_normal_submission(
    docker_dispatcher: Dispatcher,
    submission_generator,
):
    docker_dispatcher.start()
    _ids = []
    for _id, prob in submission_generator.submission_ids.items():
        if prob == 'normal-submission':
            _ids.append((_id, prob))

    assert len(_ids) != 0

    for _id, prob in _ids:
        docker_dispatcher.handle(job_id=_id, submission_id=f'sub-{_id}')
        assert docker_dispatcher.contains(_id)


def test_same_submission_parallel_jobs(
    docker_dispatcher: Dispatcher,
    submission_generator,
):
    prob_name = next(iter(submission_generator.submission_ids.values()))

    job1 = submission_generator.gen_submission_id()
    submission_generator.gen_submission(prob_name, job1)
    job2 = submission_generator.gen_submission_id()
    submission_generator.gen_submission(prob_name, job2)

    docker_dispatcher.handle(job_id=job1, submission_id='same-sub')
    docker_dispatcher.handle(job_id=job2, submission_id='same-sub')

    assert docker_dispatcher.contains(job1)
    assert docker_dispatcher.contains(job2)
    # queued work must target each job's own directory
    queued_job_ids = {j.job_id for j in docker_dispatcher.queue.queue}
    assert queued_job_ids == {job1, job2}

    docker_dispatcher.release(job1)

    assert not docker_dispatcher.contains(job1)
    assert docker_dispatcher.contains(job2)


def test_handle_enqueues_all_cases_no_size_limit(
        docker_dispatcher: Dispatcher):
    # A C job with 20 cases (> the old QUEUE_SIZE bound of 16) must enqueue
    # every case plus the compile job with no queue.Full failure.
    _write_job_dir(docker_dispatcher, 'big', Language.C, [20])

    docker_dispatcher.handle(job_id='big', submission_id='sub-big')

    queued = list(docker_dispatcher.queue.queue)
    compiles = [j for j in queued if isinstance(j, job.Compile)]
    executes = [j for j in queued if isinstance(j, job.Execute)]
    assert len(compiles) == 1
    assert len(executes) == 20
    assert docker_dispatcher.contains('big')


def test_handle_missing_dir_is_atomic(docker_dispatcher: Dispatcher):
    # A missing job dir raises before any state is assigned: no leftover
    # entry in result / locks / submission_ids and nothing enqueued.
    with pytest.raises(FileNotFoundError):
        docker_dispatcher.handle(job_id='ghost', submission_id='sub-ghost')

    assert not docker_dispatcher.contains('ghost')
    assert docker_dispatcher.result == {}
    assert docker_dispatcher.locks == {}
    assert docker_dispatcher.compile_locks == {}
    assert docker_dispatcher.submission_ids == {}
    assert docker_dispatcher.queue.empty()


def test_drain_unstarted_releases_only_unstarted(
        docker_dispatcher: Dispatcher):
    _write_job_dir(docker_dispatcher, 'j1', Language.PY, [1])
    _write_job_dir(docker_dispatcher, 'j2', Language.PY, [1])
    docker_dispatcher.handle(job_id='j1', submission_id='sub-j1')
    docker_dispatcher.handle(job_id='j2', submission_id='sub-j2')
    # j1 has picked up work; j2 has not.
    docker_dispatcher.started.add('j1')

    victims = docker_dispatcher.drain_unstarted()

    assert victims == [('j2', 'sub-j2')]
    assert docker_dispatcher.contains('j1')
    assert not docker_dispatcher.contains('j2')


def test_drain_unstarted_latches_the_run_loop(docker_dispatcher: Dispatcher):
    # After the scan, the run loop must not start anything else. Otherwise a
    # job the poller dispatched in its final iteration could begin judging
    # after shutdown, only to be abandoned when the process exits -- and it
    # would be reclaimed with its attempts already spent, not requeued
    # attempt-neutrally like a drain abort.
    docker_dispatcher.start()
    docker_dispatcher.stop_accepting()
    assert docker_dispatcher.accepting is False

    _write_job_dir(docker_dispatcher, 'late', Language.PY, [1])
    docker_dispatcher.handle(job_id='late', submission_id='sub-late')

    # The run loop consuming the task is the signal that it has had its
    # chance at this job; waiting a fixed slice instead would pass vacuously
    # whenever the loop happened to still be in its idle sleep.
    deadline = time.time() + 5.0
    while not docker_dispatcher.queue.empty():
        assert time.time() < deadline, 'run loop never consumed the task'
        time.sleep(0.02)
    assert 'late' not in docker_dispatcher.started
    assert docker_dispatcher.container_count == 0
    # Still releasable as an unstarted job, which is what drain does with it.
    assert docker_dispatcher.drain_unstarted() == [('late', 'sub-late')]


def test_latched_dispatcher_still_finishes_started_jobs(
    docker_dispatcher: Dispatcher,
    monkeypatch,
):
    # The latch must let an in-flight job consume its REMAINING cases. If it
    # blocked those too, a multi-case job would never complete: drain would
    # wait out its whole budget and the results already computed would be lost.
    ran = []
    first_case_running = threading.Event()
    release = threading.Event()

    class StubExecutor:

        def __init__(self, job_id, *args, **kwargs):
            self.case_no = kwargs.get('case_no')

        def run(self):
            ran.append(self.case_no)
            if len(ran) == 1:
                first_case_running.set()
                release.wait(5.0)
            return {'Status': 'AC'}

    monkeypatch.setattr('dispatcher.dispatcher.SubmissionExecutor',
                        StubExecutor)
    # One container at a time, so the second case can only run after the first
    # finishes -- i.e. after the latch is already in place.
    docker_dispatcher.MAX_CONTAINER_SIZE = 1
    _write_job_dir(docker_dispatcher, 'multi', Language.PY, [2])
    docker_dispatcher.handle(job_id='multi', submission_id='sub-multi')
    docker_dispatcher.start()

    assert first_case_running.wait(5.0), 'first case never started'
    assert docker_dispatcher.drain_unstarted() == []  # in flight, so kept
    release.set()

    deadline = time.time() + 5.0
    while len(ran) < 2:
        assert time.time() < deadline, 'second case never ran after the latch'
        time.sleep(0.02)
    assert sorted(ran) == ['0000', '0001']


def test_drain_unstarted_leaves_in_flight_jobs_running(
        docker_dispatcher: Dispatcher):
    # The latch must not strand a job that is already in flight: its remaining
    # cases still need to run, or the job never completes and drain waits out
    # its whole budget.
    _write_job_dir(docker_dispatcher, 'inflight', Language.PY, [2])
    docker_dispatcher.handle(job_id='inflight', submission_id='sub-inflight')
    docker_dispatcher.started.add('inflight')

    assert docker_dispatcher.drain_unstarted() == []
    assert docker_dispatcher.contains('inflight')


def test_on_complete_wiring_receives_result_and_releases(
    docker_dispatcher: Dispatcher,
    submission_generator,
):
    recorded = []
    docker_dispatcher.testing = False
    docker_dispatcher.on_complete = lambda *a: recorded.append(a)

    job_id, prob = next(
        (i, p) for i, p in submission_generator.submission_ids.items()
        if p == 'normal-submission')
    docker_dispatcher.handle(job_id=job_id, submission_id=f'sub-{job_id}')

    _, task_content = docker_dispatcher.result[job_id]
    for case_no in list(task_content):
        docker_dispatcher.on_case_complete(
            job_id=job_id,
            case_no=case_no,
            stdout='',
            stderr='',
            exit_code=0,
            exec_time=1,
            mem_usage=1,
            prob_status='AC',
        )

    assert len(recorded) == 1
    got_job_id, got_submission_id, tasks = recorded[0]
    assert got_job_id == job_id
    assert got_submission_id == f'sub-{job_id}'
    # normal-submission has two single-case tasks -> [[case], [case]].
    assert [len(t) for t in tasks] == [1, 1]
    assert all(c['status'] == 'AC' for t in tasks for c in t)
    # release() ran as part of on_job_complete.
    assert not docker_dispatcher.contains(job_id)


def test_compile_je_fallback(docker_dispatcher: Dispatcher, monkeypatch):
    # A crash the executor does not wrap (e.g. docker APIError) must still
    # produce a JE compile result so the job can complete instead of hanging.
    class BoomExecutor:

        def __init__(self, *args, **kwargs):
            pass

        def compile(self):
            raise RuntimeError('docker exploded')

    monkeypatch.setattr('dispatcher.dispatcher.SubmissionExecutor',
                        BoomExecutor)
    _write_job_dir(docker_dispatcher, 'cj', Language.C, [1])
    docker_dispatcher.handle(job_id='cj', submission_id='sub-cj')

    docker_dispatcher.compile('cj', Language.C)

    assert docker_dispatcher.compile_results['cj']['Status'] == 'JE'
