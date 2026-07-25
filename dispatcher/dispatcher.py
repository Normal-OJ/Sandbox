import json
import os
import threading
import time
import pathlib
import queue

from executor.submission import SubmissionExecutor
from . import job, config
from .exception import *
from .meta import Meta
from .constant import Language
from .utils import logger


class Dispatcher(threading.Thread):

    def __init__(
        self,
        dispatcher_config='.config/dispatcher.json',
        submission_config='.config/submission.json',
        *,
        on_complete=None,
    ):
        super().__init__()
        # Daemon so fail-fast exit paths (heartbeat 401 -> exit without
        # drain) are not blocked by this thread: the interpreter waits for
        # non-daemon threads, and the run loop only ends via stop(). The
        # graceful drain path still stops and joins explicitly.
        self.daemon = True
        self.testing = False
        # read config
        d_config = {}
        if os.path.exists(dispatcher_config):
            with open(dispatcher_config) as f:
                d_config = json.load(f)
        # flag to decided whether the thread should run
        self.do_run = True
        # submission location
        self.SUBMISSION_DIR = config.SUBMISSION_DIR
        # Unbounded task queue: in pull mode the poller is the only producer
        # and total task volume is capped by the job-level capacity gate
        # (max_concurrent_jobs) × per-job case count, so a task-level bound
        # only creates spurious queue.Full failures (see handle()).
        # type Queue[Union[job.Compile, job.Execute]]
        self.queue = queue.Queue()
        # task result
        # type: Dict[job_id, Tuple[submission_info, List[result]]]
        self.result = {}
        # threading locks for each job
        self.locks = {}
        self.compile_locks = {}
        self.compile_results = {}
        # maps job_id -> submission_id, for the backend callback
        self.submission_ids = {}
        # invoked with (job_id, submission_id, tasks) when a job finishes;
        # main.py wires this to the result queue consumed by ResultSenderThread.
        self.on_complete = on_complete
        # job ids whose work has actually been picked up in run(). state_lock
        # guards the started-set transition against drain_unstarted() so a job
        # can never be both drain-aborted and started.
        self.started = set()
        # Cleared by drain_unstarted(): from then on the run loop starts no
        # further jobs, so nothing begins judging after shutdown was requested.
        self.accepting = True
        self.state_lock = threading.Lock()
        # manage containers
        self.MAX_CONTAINER_SIZE = d_config.get('MAX_CONTAINER_NUMBER', 8)
        self.container_count_lock = threading.Lock()
        self.container_count = 0
        # read cwd from submission executor config
        with open(submission_config) as f:
            s_config = json.load(f)
            self.submission_executor_cwd = pathlib.Path(
                s_config['working_dir'])

    def compile_need(self, lang: Language):
        return lang in {Language.C, Language.CPP}

    def contains(self, job_id: str):
        return job_id in self.result

    def inc_container(self):
        with self.container_count_lock:
            self.container_count += 1

    def dec_container(self):
        with self.container_count_lock:
            self.container_count -= 1

    def handle(self, job_id: str, submission_id: str):
        '''
        handle a job, save its config and push into task queue

        multiple jobs for the same submission are legal concurrency
        (e.g. rejudge racing an in-flight judge run); they are kept
        fully separate here, keyed by job_id.

        handle() is atomic: every validation that can fail (missing dir, not a
        directory, meta parse) runs BEFORE any state assignment or queue
        mutation, so on any raise no state and no queue entries exist. The
        unbounded queue is what makes this hold -- put_nowait can no longer
        raise queue.Full, so there is no partial-enqueue rollback to do.
        '''
        logger().info(f'receive job {job_id} (submission {submission_id}).')
        job_path = self.SUBMISSION_DIR / job_id
        # check whether the job directory exist
        if not job_path.exists():
            raise FileNotFoundError(f'job id: {job_id} file not found.')
        elif not job_path.is_dir():
            raise NotADirectoryError(f'{job_path} is not a directory')
        # read submission meta
        with (job_path / 'meta.json').open() as f:
            submission_config = Meta.parse_obj(json.load(f))

        # Publish the job state and its tasks under state_lock, so a drain
        # scan holding that lock sees either all of this job or none of it.
        # A late dispatch (the poller finishing a prep as drain begins) is
        # therefore never half-visible to drain_unstarted().
        task_content = {}
        with self.state_lock:
            self.result[job_id] = (submission_config, task_content)
            self.locks[job_id] = threading.Lock()
            self.compile_locks[job_id] = threading.Lock()
            self.submission_ids[job_id] = submission_id

            logger().debug(f'current jobs: {[*self.result.keys()]}')
            if self.compile_need(submission_config.language):
                self.queue.put_nowait(job.Compile(job_id=job_id))
            for i, task in enumerate(submission_config.tasks):
                for j in range(task.caseCount):
                    case_no = f'{i:02d}{j:02d}'
                    task_content[case_no] = None
                    _job = job.Execute(
                        job_id=job_id,
                        task_id=i,
                        case_id=j,
                    )
                    self.queue.put_nowait(_job)

    def release(self, job_id: str):
        with self.state_lock:
            self._release_locked(job_id)

    def _release_locked(self, job_id: str):
        '''
        Release variable about job. Caller must hold state_lock.
        '''
        for v in (
                self.result,
                self.compile_locks,
                self.compile_results,
                self.locks,
                self.submission_ids,
        ):
            if job_id in v:
                del v[job_id]
        self.started.discard(job_id)

    def stop_accepting(self):
        '''Stop starting new jobs; in-flight ones keep running.

        Call this BEFORE waiting for the poller to finish, not after: the
        poller can still complete a prep and dispatch a job while drain is
        joining it, and until this latch is set the run loop is free to pick
        that job up. It would then be judging work that began after shutdown,
        get abandoned when the process exits, and come back through reclaim
        with its claim attempt already spent, instead of being requeued
        attempt-neutrally by drain_unstarted().
        '''
        with self.state_lock:
            self.accepting = False

    def drain_unstarted(self):
        '''Release every job with no started work, and return them.

        Returns [(job_id, submission_id)] for the released jobs. Call after
        stop_accepting() and after the poller has finished, so the scan sees
        every job that was ever dispatched and none of them can still become
        started; in-flight jobs are left to finish and report normally.

        Latches on its own too, so a caller that skips stop_accepting() still
        gets a consistent scan -- just a wider window before it.
        '''
        with self.state_lock:
            self.accepting = False
            victims = [(j, self.submission_ids[j]) for j in list(self.result)
                       if j not in self.started]
            for job_id, _ in victims:
                self._release_locked(job_id)
        return victims

    def has_jobs(self):
        return bool(self.result)

    def run(self):
        self.do_run = True
        logger().debug('start dispatcher loop')
        while True:
            # end the loop
            if not self.do_run:
                logger().debug('exit dispatcher loop')
                break
            # no testcase need to be run
            if self.queue.empty():
                time.sleep(1)
                continue
            # no space for new cotainer now
            if self.container_count >= self.MAX_CONTAINER_SIZE:
                time.sleep(1)
                continue
            # get a case
            _job = self.queue.get()
            job_id = _job.job_id
            # Read job context and transition to started atomically w.r.t.
            # drain_unstarted(): once we mark a job started here, drain leaves
            # it to finish; before that, drain may abort it out from under us.
            with self.state_lock:
                # if a job was discarded, it will not appear in `self.result`
                if not self.contains(job_id):
                    logger().info(f'discarded job [id={job_id}]')
                    continue
                # Draining: only jobs already in flight may keep consuming
                # their remaining cases. Anything still unstarted belongs to
                # drain_unstarted(), which returns it to the backend.
                if not self.accepting and job_id not in self.started:
                    logger().info(f'draining, not starting [id={job_id}]')
                    continue
                # get task info
                submission_config, _ = self.result[job_id]
                if isinstance(_job, job.Compile):
                    self.started.add(job_id)
                    threading.Thread(
                        target=self.compile,
                        args=(
                            job_id,
                            submission_config.language,
                        ),
                    ).start()
                # if this job needs compile and it haven't finished
                elif self.compile_need(submission_config.language) \
                    and self.compile_results.get(job_id) is None:
                    # unbounded queue: never blocks, safe under the lock
                    self.queue.put(_job)
                else:
                    self.started.add(job_id)
                    task_info = submission_config.tasks[_job.task_id]
                    case_no = f'{_job.task_id:02d}{_job.case_id:02d}'
                    logger().info(
                        f'create container [task={job_id}/{case_no}]')
                    logger().debug(f'task info: {task_info}')
                    # output path should be the container path
                    base_path = self.SUBMISSION_DIR / job_id / 'testcase'
                    out_path = str((base_path / f'{case_no}.out').absolute())
                    # input path should be the host path
                    base_path = (self.submission_executor_cwd / job_id /
                                 'testcase')
                    in_path = str((base_path / f'{case_no}.in').absolute())
                    # debug log
                    logger().debug('in path: ' + in_path)
                    logger().debug('out path: ' + out_path)
                    # assign a new executor
                    threading.Thread(
                        target=self.create_container,
                        args=(
                            job_id,
                            case_no,
                            task_info.memoryLimit,
                            task_info.timeLimit,
                            in_path,
                            out_path,
                            submission_config.language,
                        ),
                    ).start()

    def stop(self):
        self.do_run = False

    def compile(
        self,
        job_id: str,
        lang: Language,
    ):
        # Look the lock up defensively: a concurrent drain/release may have
        # dropped the job between run() spawning us and this thread starting.
        lock = self.compile_locks.get(job_id)
        if lock is None:
            logger().warning(
                f'compile thread for released job {job_id}; nothing to do')
            return
        # another thread is compiling this job, bye
        if lock.locked():
            logger().error(f'start a compile thread on locked job {job_id}')
            return
        # this job should not be compiled!
        if not self.compile_need(lang):
            logger().warning(
                f'try to compile job {job_id}'
                f' with language {lang}', )
            return
        # compile this job. don't forget to acquire the lock
        with lock:
            logger().info(f'start compiling {job_id}')
            # A JE fallback keeps the drain invariant "in-flight jobs always
            # finish": an unhandled error here (e.g. docker APIError from
            # create_container) would otherwise never write a compile result,
            # so the job would stay leased forever. JudgeError is already
            # wrapped inside SubmissionExecutor; this catches what it does not.
            try:
                res = SubmissionExecutor(
                    job_id=job_id,
                    time_limit=-1,
                    mem_limit=-1,
                    testdata_input_path='',
                    testdata_output_path='',
                    lang=['c11', 'cpp17'][int(lang)],
                ).compile()
            except Exception:
                logger().exception(f'compile crashed for {job_id}')
                res = {'Status': 'JE'}
            self.compile_results[job_id] = res
            logger().debug(f'finish compiling, get status {res["Status"]}')

    def create_container(
        self,
        job_id: str,
        case_no: str,
        mem_limit: int,
        time_limit: int,
        case_in_path: str,
        case_out_path: str,
        lang: Language,
    ):
        # JE fallback: an unhandled error anywhere before the case result
        # exists (executor construction reading its config file, docker
        # APIError from run()) must still resolve the case, or the job would
        # never complete and stay leased forever.
        try:
            lang = ['c11', 'cpp17', 'python3'][int(lang)]
            executor = SubmissionExecutor(
                job_id,
                time_limit,
                mem_limit,
                case_in_path,
                case_out_path,
                lang=lang,
                case_no=case_no,
            )
            res = self.extract_compile_result(job_id, lang)
            # Execute if compile successfully
            if res['Status'] != 'CE':
                try:
                    self.inc_container()
                    res = executor.run()
                finally:
                    self.dec_container()
        except Exception:
            logger().exception(f'judge crashed for {job_id}/{case_no}')
            res = {'Status': 'JE'}
        logger().info(f'finish task {job_id}/{case_no}')
        with self.locks[job_id]:
            self.on_case_complete(
                job_id=job_id,
                case_no=case_no,
                stdout=res.get('Stdout', ''),
                stderr=res.get('Stderr', ''),
                exit_code=res.get('DockerExitCode', -1),
                exec_time=res.get('Duration', -1),
                mem_usage=res.get('MemUsage', -1),
                prob_status=res['Status'],
            )

    def extract_compile_result(self, job_id: str, lang: Language):
        '''
        Get compile result for specific job. If the language does
        not need to be compiled, return a AC result.
        '''
        try:
            return self.compile_results[job_id]
        except KeyError:
            status = 'CE' if self.compile_need(lang) else 'AC'
            return {'Status': status}

    def on_case_complete(
        self,
        job_id: str,
        case_no: str,
        stdout: str,
        stderr: str,
        exit_code: int,
        exec_time: int,
        mem_usage: int,
        prob_status: str,
    ):
        # if id not exists
        if job_id not in self.result:
            raise JobIdNotFoundError(
                f'received case result for unknown job {job_id}')
        # update case result
        _, results = self.result[job_id]
        if case_no not in results:
            raise ValueError(f'{job_id}/{case_no} not found.')
        results[case_no] = {
            'stdout': stdout,
            'stderr': stderr,
            'exitCode': exit_code,
            'execTime': exec_time,
            'memoryUsage': mem_usage,
            'status': prob_status
        }
        # check completion
        _results = [k for k, v in results.items() if not v]
        logger().debug(f'tasks wait for judge: {_results}')
        if all(results.values()):
            self.on_job_complete(job_id)

    def on_job_complete(self, job_id: str):
        if not self.contains(job_id):
            raise JobIdNotFoundError(f'{job_id} not found!')
        if self.testing:
            logger().info(
                f'skip job post processing in testing [job_id={job_id}]')
            return True
        _, results = self.result[job_id]
        # parse results
        submission_result = {}
        for no, r in results.items():
            task_no = int(no[:2])
            case_no = int(no[2:])
            if task_no not in submission_result:
                submission_result[task_no] = {}
            submission_result[task_no][case_no] = r
        # convert to list and check
        for task_no, cases in submission_result.items():
            assert [*cases.keys()] == [*range(len(cases))]
            submission_result[task_no] = [*cases.values()]
        assert [*submission_result.keys()] == [*range(len(submission_result))]
        submission_result = [*submission_result.values()]
        # Hand the result off to the result queue via on_complete. Local dir
        # cleanup/backup and tracker bookkeeping are now entirely the
        # ResultSenderThread's job; release() only drops dispatcher state.
        submission_id = self.submission_ids[job_id]
        # Enqueue BEFORE release: has_jobs() must stay True until the result
        # is in the queue, or a concurrent drain could observe an empty
        # dispatcher, stop the sender, and lose this result.
        if self.on_complete is None:
            logger().error(
                f'no on_complete wired; dropping result for job {job_id}')
        else:
            logger().info(f'enqueue result [job_id={job_id},'
                          f' submission_id={submission_id}]')
            self.on_complete(job_id, submission_id, submission_result)
        self.release(job_id)
