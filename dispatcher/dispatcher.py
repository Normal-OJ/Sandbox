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

    def handle(self, submission_id, lang):
        '''
        handle a submission, save its config and push into task queue

        Args:
            submission_id -> str: the submission's unique id
            lang -> str:
                the programming language this submission use.
                currently accept {'c11', 'cpp11', 'python3'}
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
        with open(f'{submission_path}/testcase/meta.json') as f:
            submission_config = json.load(f)
        submission_config['lang'] = lang

        task_count = len(submission_config['cases'])

        for i in range(task_count):
            self.queue.put_nowait((submission_id, i))

        self.result[submission_id] = (submission_config, [None] * task_count)

        return True

    def run(self):
        self.do_run = True
        while True:
            if not self.do_run:
                break
            if not self.queue.empty() and \
                self.container_count < self.MAX_CONTAINER_SIZE:
                # get a task
                submission_id, task_id = self.queue.get()

                if submission_id not in self.result:
                    logging.info(f'discarded task {submission_id}/{task_id}')
                    continue

                # get task info
                submission_config = self.result[submission_id][0]
                task_info = submission_config['cases'][task_id]

                # read task's stdin and stdout
                logging.info(f'create container for {submission_id}/{task_id}')
                base_path = self.SUBMISSION_DIR / submission_id / 'testcase' / str(
                    task_id)
                out_path = str((base_path / 'out').absolute())

                base_path = self.submission_runner_cwd / submission_id / 'testcase' / str(
                    task_id)

                in_path = str((base_path / 'in').absolute())

                logging.debug('in path: ' + in_path)
                logging.debug('out path: ' + out_path)

                # assign a new runner
                threading.Thread(
                    target=self.create_container,
                    args=(
                        submission_id,
                        task_id,
                        task_info['memoryLimit'],
                        task_info['timeLimit'],
                        in_path,
                        out_path,
                        submission_config['lang'],
                    ),
                ).start()

    def stop(self):
        self.do_run = False

    def create_container(self, submission_id, task_id, mem_limit, time_limit,
                         case_in_path, case_out_path, lang):
        self.container_count += 1
        runner = SubmissionRunner(submission_id,
                                  time_limit,
                                  mem_limit,
                                  case_in_path,
                                  case_out_path,
                                  lang=lang)

        if lang in {'c11', 'cpp11'}:
            res = runner.compile()
        else:
            res = {'Status': 'AC'}

        if res['Status'] != 'CE':
            res = runner.run()

        logging.debug(f'finish task {submission_id}/{task_id}')
        logging.debug(f'get submission runner res: {res}')

        self.container_count -= 1
        self.on_sub_task_complete(
            submission_id=submission_id,
            task_id=task_id,
            stdout=res.get('Stdout', ''),
            stderr=res.get('Stderr', ''),
            exit_code=res.get('DockerExitCode', -1),
            score=self.result[submission_id][0]['cases'][task_id]['caseScore'],
            exec_time=res.get('Duration', -1),
            mem_usage=res.get('MemUsage', -1),
            prob_status=res['Status'])

    def on_sub_task_complete(self, submission_id, task_id, stdout, stderr,
                             exit_code, score, exec_time, mem_usage,
                             prob_status):
        # if id not exists
        if submission_id not in self.result:
            raise SubmissionIdNotFoundError(
                f'Unexisted id {submission_id} recieved')

        info, results = self.result[submission_id]
        if task_id >= len(info['cases']):
            raise ValueError(
                f'task number {task_id} in {submission_id} more than excepted.'
            )

        results[task_id] = {
            'stdout': stdout,
            'stderr': stderr,
            'exitCode': exit_code,
            'execTime': exec_time,
            'memoryUsage': mem_usage,
            'status': prob_status,
            'score': score if prob_status == 'AC' else 0
        }

        logging.debug(f'current sub task result: {results}')
        if all(results):
            self.on_submission_complete(submission_id)

        return True

    def on_submission_complete(self, submission_id):
        if submission_id not in self.result:
            raise SubmissionIdNotFoundError(f'{submission_id} not found!')
        if self.testing:
            return True

        endpoint = f'{self.HTTP_HANDLER_URL}/result/{submission_id}'
        info, results = self.result[submission_id]
        submission_data = {
            'score': sum(x['score'] for x in results),
            'status': results[-1]['status'],
            'cases': results
        }
        for result in results:
            del result['score']

        logging.debug(f'{submission_id} send to http handler ({endpoint})')
        res = requests.post(endpoint, json=submission_data)

        logging.info(f'finish submission {submission_id}')

        # remove this submission
        del self.result[submission_id]

        if res.status_code != 200:
            logging.warning(
                'dispatcher receive err'
                f'status code: {res.status_code}\n'
                f'msg: {res.text}', )
            return False
        return True
