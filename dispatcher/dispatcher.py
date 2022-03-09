import json
import os
import threading
import time
import pathlib
import queue
import textwrap
from runner.submission import SubmissionRunner
from . import job
from .exception import *
from .meta import Meta
from .constant import Language
from .utils import (get_redis_client, logger)


class Dispatcher(threading.Thread):

    def __init__(
        self,
        dispatcher_config='.config/dispatcher.json',
        submission_config='.config/submission.json',
    ):
        super().__init__()
        self.testing = False
        # read config
        config = {}
        if os.path.exists(dispatcher_config):
            with open(dispatcher_config) as f:
                config = json.load(f)
        # flag to decided whether the thread should run
        self.do_run = True
        # submission location
        self.SUBMISSION_DIR = pathlib.Path(
            config.get(
                'SUBMISSION_DIR',
                'submissions',
            ))
        self.SUBMISSION_DIR.mkdir(exist_ok=True)
        # task queue
        # type Queue[Tuple[submission_id, task_no]]
        self.MAX_TASK_COUNT = config.get('QUEUE_SIZE', 16)
        self.queue = queue.Queue(self.MAX_TASK_COUNT)
        # task result
        # type: Dict[submission_id, Tuple[submission_info, List[result]]]
        self.result = {}
        # threading locks for each submission
        self.locks = {}
        self.compile_locks = {}
        self.compile_status = {}
        # manage containers
        self.MAX_CONTAINER_SIZE = config.get('MAX_CONTAINER_NUMBER', 8)
        self.container_count = 0
        # read cwd from submission runner config
        with open(submission_config) as f:
            s_config = json.load(f)
            self.submission_runner_cwd = pathlib.Path(s_config['working_dir'])

    def compile_need(self, lang: Language):
        return lang in {Language.C, Language.CPP}

    def handle(self, submission_id: str):
        '''
        handle a submission, save its config and push into task queue
        '''
        logger().info(f'receive submission {submission_id}.')
        submission_path = self.SUBMISSION_DIR / submission_id
        # check whether the submission directory exist
        if not submission_path.exists():
            raise FileNotFoundError(
                f'submission id: {submission_id} file not found.')
        elif not submission_path.is_dir():
            raise NotADirectoryError(f'{submission_path} is not a directory')
        # duplicated
        if submission_id in self.result:
            raise DuplicatedSubmissionIdError(
                f'duplicated submission id {submission_id}.')
        # read submission meta
        with (submission_path / 'meta.json').open() as f:
            submission_config = Meta.parse_obj(json.load(f))
        task_content = {}
        self.result[submission_id] = (submission_config, task_content)
        self.locks[submission_id] = threading.Lock()
        self.compile_locks[submission_id] = threading.Lock()
        logger().debug(f'current submissions: {[*self.result.keys()]}')
        try:
            if self.compile_need(submission_config.language):
                self.queue.put_nowait(job.Compile(submission_id=submission_id))
            for i, task in enumerate(submission_config.tasks):
                for j in range(task.caseCount):
                    case_no = f'{i:02d}{j:02d}'
                    task_content[case_no] = None
                    _job = job.Execute(
                        submission_id=submission_id,
                        task_id=i,
                        case_id=j,
                    )
                    self.queue.put_nowait(_job)
        except queue.Full as e:
            self.release(submission_id)
            raise e

    def release(self, submission_id: str):
        '''
        Release variable about submission
        '''
        for v in (
                self.result,
                self.compile_locks,
                self.compile_status,
                self.locks,
        ):
            if submission_id in v:
                del v[submission_id]

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
            submission_id = _job.submission_id
            # if a submission was discarded, it will not appear in the `self.result`
            if submission_id not in self.result:
                logger().info(f'discarded submission [id={submission_id}]')
                continue
            # get task info
            submission_config, _ = self.result[submission_id]
            if isinstance(_job, job.Compile):
                threading.Thread(
                    target=self.compile,
                    args=(
                        submission_id,
                        submission_config.language,
                    ),
                ).start()
            # if this submission needs compile and it haven't finished
            elif self.compile_need(submission_config.language) \
                and self.compile_status.get(submission_id) is None:
                self.queue.put(_job)
            else:
                task_info = submission_config.tasks[_job.task_id]
                case_no = f'{_job.task_id:02d}{_job.case_id:02d}'
                logger().info(
                    f'create container [task={submission_id}/{case_no}]')
                logger().debug(f'task info: {task_info}')
                # output path should be the container path
                base_path = self.SUBMISSION_DIR / submission_id / 'testcase'
                out_path = str((base_path / f'{case_no}.out').absolute())
                # input path should be the host path
                base_path = self.submission_runner_cwd / submission_id / 'testcase'
                in_path = str((base_path / f'{case_no}.in').absolute())
                # debug log
                logger().debug('in path: ' + in_path)
                logger().debug('out path: ' + out_path)
                # assign a new runner
                threading.Thread(
                    target=self.create_container,
                    args=(
                        submission_id,
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
        submission_id: str,
        lang: Language,
    ):
        # another thread is compileing this submission, bye
        if self.compile_locks[submission_id].locked():
            logger().error(
                f'start a compile thread on locked submission {submission_id}')
            return
        # this submission should not be compiled!
        if not self.compile_need(lang):
            logger().warning(
                f'try to compile submission {submission_id}'
                f' with language {lang}', )
            return
        # compile this submission don't forget to acquire the lock
        with self.compile_locks[submission_id]:
            logger().info(f'start compiling {submission_id}')
            res = SubmissionRunner(
                submission_id=submission_id,
                time_limit=-1,
                mem_limit=-1,
                testdata_input_path='',
                testdata_output_path='',
                lang=['c11', 'cpp17'][int(lang)],
            ).compile()
            self.compile_status[submission_id] = res['Status']
            logger().debug(f'finish compiling, get status {res["Status"]}')

    def create_container(
        self,
        submission_id: str,
        case_no: str,
        mem_limit: int,
        time_limit: int,
        case_in_path: str,
        case_out_path: str,
        lang: Language,
    ):
        self.container_count += 1
        lang = ['c11', 'cpp17', 'python3'][int(lang)]
        runner = SubmissionRunner(
            submission_id,
            time_limit,
            mem_limit,
            case_in_path,
            case_out_path,
            lang=lang,
        )
        # get compile status (if exist)
        res = {
            'Status': self.compile_status.get(submission_id, 'AC'),
        }
        # executing
        if res['Status'] != 'CE':
            res = runner.run()
        # logging
        logger().info(f'finish task {submission_id}/{case_no}')
        # truncate long stdout/stderr
        _res = res.copy()
        for k in ('Stdout', 'Stderr'):
            _res[k] = textwrap.shorten(_res.get(k, ''), 37, placeholder='...')
        logger().debug(f'runner result: {_res}')
        self.container_count -= 1
        with self.locks[submission_id]:
            self.on_case_complete(
                submission_id=submission_id,
                case_no=case_no,
                stdout=res.get('Stdout', ''),
                stderr=res.get('Stderr', ''),
                exit_code=res.get('DockerExitCode', -1),
                exec_time=res.get('Duration', -1),
                mem_usage=res.get('MemUsage', -1),
                prob_status=res['Status'],
            )

    def on_case_complete(
        self,
        submission_id: str,
        case_no: str,
        stdout: str,
        stderr: str,
        exit_code: int,
        exec_time: int,
        mem_usage: int,
        prob_status: str,
    ):
        # if id not exists
        if submission_id not in self.result:
            raise SubmissionIdNotFoundError(
                f'Unexisted id {submission_id} recieved')
        # update case result
        _, results = self.result[submission_id]
        if case_no not in results:
            raise ValueError(f'{submission_id}/{case_no} not found.')
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
            self.on_submission_complete(submission_id)

    def on_submission_complete(self, submission_id: str):
        if submission_id not in self.result:
            raise SubmissionIdNotFoundError(f'{submission_id} not found!')
        if self.testing:
            logger().info(
                'current in testing'
                f'skip send {submission_id} result to http handler', )
            return True
        _, results = self.result[submission_id]
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
        # post data
        submission_data = {'tasks': submission_result}
        json.dump(
            submission_data,
            (self.SUBMISSION_DIR / submission_id / 'result.json').open('w'),
        )
        get_redis_client().publish('submission-completed', submission_id)
