import json
import os
import threading
import time
import requests
import logging
import pathlib
import queue

from submission import SubmissionRunner
from .exception import *


class Dispatcher(threading.Thread):
    def __init__(self, config_path='.config/dispatcher.json'):
        super().__init__()
        self.testing = False

        # read config
        config = {}
        if os.path.exists(config_path):
            with open(config_path) as f:
                config = json.load(f)

        # flag to decided whether the thread should run
        self.do_run = True

        # http handler URL
        self.HTTP_HANDLER_URL = config.get('HTTP_HANDLER_URL',
                                           'localhost:1450')

        # submission location
        self.SUBMISSION_DIR = pathlib.Path(
            config.get('SUBMISSION_DIR', 'submissions'))
        self.SUBMISSION_DIR.mkdir(exist_ok=True)

        # task queue
        # type Queue[Tuple[submission_id, task_no]]
        self.MAX_TASK_COUNT = config.get('QUEUE_SIZE', 16)
        self.queue = queue.Queue(self.MAX_TASK_COUNT)

        # task result
        # type: Dict[submission_id, Tuple[submission_info, List[result]]]
        self.result = {}

        # manage containers
        self.MAX_CONTAINER_SIZE = config.get('MAX_CONTAINER_NUMBER', 8)
        self.container_count = 0

        # read cwd from submission runner config
        with open('.config/submission.json') as f:
            s_config = json.load(f)
            self.submission_runner_cwd = pathlib.Path(s_config['working_dir'])

    def handle(self, submission_id):
        '''
        handle a submission, save its config and push into task queue

        Args:
            submission_id -> str: the submission's unique id
        Returns:
            a bool denote whether the submission has successfully put into queue
        '''
        logging.info(f'receive submission {submission_id}.')

        submission_path = self.SUBMISSION_DIR / submission_id

        # check whether the submission directory exist
        if not os.path.exists(submission_path):
            raise FileNotFoundError(
                f'submission id: {submission_id} file not found.')
        elif not os.path.isdir(submission_path):
            raise NotADirectoryError(f'{submission_path} is not a directory')

        # duplicated
        if submission_id in self.result:
            raise DuplicatedSubmissionIdError(
                f'duplicated submission id {submission_id}.')

        # read submission meta
        with open(f'{submission_path}/meta.json') as f:
            submission_config = json.load(f)

        task_content = {}
        for i, task in enumerate(submission_config['tasks']):
            for j in range(task['caseCount']):
                case_no = f'{i:02d}{j:02d}'
                task_content[case_no] = None
                # put (submission_id, case_no)
                self.queue.put_nowait((submission_id, case_no))

        self.result[submission_id] = (submission_config, task_content)

        return True

    def run(self):
        self.do_run = True
        while True:
            if not self.do_run:
                break
            if self.queue.empty():
                continue
            if self.container_count >= self.MAX_CONTAINER_SIZE:
                continue

            # get a case
            submission_id, case_no = self.queue.get()

            if submission_id not in self.result:
                logging.info(f'discarded case {submission_id}/{case_no}')
                continue

            # get task info
            submission_config = self.result[submission_id][0]
            task_info = submission_config['tasks'][int(case_no[:2])]

            # read task's stdin and stdout
            logging.info(f'create container for {submission_id}/{case_no}')
            base_path = self.SUBMISSION_DIR / submission_id / 'testcase'
            out_path = str((base_path / f'{case_no}.out').absolute())
            base_path = self.submission_runner_cwd / submission_id / 'testcase'
            in_path = str((base_path / f'{case_no}.in').absolute())

            logging.debug('in path: ' + in_path)
            logging.debug('out path: ' + out_path)

            # assign a new runner
            threading.Thread(
                target=self.create_container,
                args=(
                    submission_id,
                    case_no,
                    task_info['memoryLimit'],
                    task_info['timeLimit'],
                    in_path,
                    out_path,
                    submission_config['language'],
                ),
            ).start()

    def stop(self):
        self.do_run = False

    def create_container(
        self,
        submission_id,
        case_no,
        mem_limit,
        time_limit,
        case_in_path,
        case_out_path,
        lang,
    ):
        self.container_count += 1
        lang = ['c11', 'cpp11', 'python3'][lang]
        runner = SubmissionRunner(
            submission_id,
            time_limit,
            mem_limit,
            case_in_path,
            case_out_path,
            lang=lang,
        )

        if lang in {'c11', 'cpp11'}:
            res = runner.compile()
        else:
            res = {'Status': 'AC'}

        if res['Status'] != 'CE':
            res = runner.run()

        logging.info(f'finish task {submission_id}/{case_no}')
        logging.debug(f'get submission runner res: {res}')

        self.container_count -= 1
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
        submission_id,
        case_no,
        stdout,
        stderr,
        exit_code,
        exec_time,
        mem_usage,
        prob_status,
    ):
        # if id not exists
        if submission_id not in self.result:
            raise SubmissionIdNotFoundError(
                f'Unexisted id {submission_id} recieved')

        # update case result
        info, results = self.result[submission_id]
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

        logging.debug(f'current sub task result: {results}')
        if all(results.values()):
            self.on_submission_complete(submission_id)

        return True

    def on_submission_complete(self, submission_id):
        if submission_id not in self.result:
            raise SubmissionIdNotFoundError(f'{submission_id} not found!')
        if self.testing:
            return True

        endpoint = f'{self.HTTP_HANDLER_URL}/result/{submission_id}'
        info, results = self.result[submission_id]
        # parse results
        submission_result = {}
        for no, r in results:
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

        submission_data = {'tasks': submission_result}

        logging.debug(f'{submission_id} send to http handler ({endpoint})')
        res = requests.post(endpoint, json=submission_data)

        logging.info(f'finish submission {submission_id}')

        # remove this submission
        del self.result[submission_id]

        if res.status_code != 200:
            logging.warning(
                'dispatcher receive err\n'
                f'status code: {res.status_code}\n'
                f'msg: {res.text}', )
            return False
        return True
