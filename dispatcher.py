import docker
import json
import os
import threading
import time
import requests

from queue import Queue
from submission import SubmissionRunner

class Dispatcher(threading.Thread):
    def __init__(self, config_path='.config/dispatcher.json'):
        super().__init__()

        # read config
        with open('.config/dispatcher.json') as f:
            config = json.load(f)

        # flag to decided whether the thread should run
        self.do_run = True

        # http handler URL
        self.HTTP_HANDLER_URL = config.get('HTTP_HANDLER_URL', 'localhost:8888')

        # submission location
        self.SUBMISSION_DIR = config.get('SUBMISSION_DIR', './submissions')

        # task queue
        # type Queue[Tuple[submission_id, task_no]]
        self.MAX_TASK_COUNT = config.get('QUEUE_SIZE', 1)
        self.queue = Queue(self.MAX_TASK_COUNT)
        self.task_count = 0

        # task result
        # type: Dict[submission_id, Tuple[submission_info, List[result]]]
        self.result = {}

        # manage containers
        self.MAX_CONTAINER_SIZE = config.get('MAX_CONTAINER_NUMBER', 1)
        self.container_count = 0

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
        submission_path = f'{self.SUBMISSION_DIR}/{submission_id}'

        # check whether the submission directory exist
        if not os.path.exists(submission_path):
            raise FileNotFoundError(f'submission id: {submission_id} file not found.')
        elif not os.path.isdir(submission_path):
            raise NotADirectoryError(f'{submission_path} is not a directory')

        # duplicated
        if submission_id in self.result:
            print('duplicated submission id.')
            return False

        # read submission meta
        with open(f'{submission_path}/testcase/meta.json') as f:
            submission_config = json.load(f)
        submission_config['lang'] = lang

        task_count = len(submission_config['cases'])

        if self.task_count + task_count >= self.MAX_TASK_COUNT:
            print('Queue is full now.')
            return False

        for i in range(task_count):
            self.queue.put((submission_id, i))

        self.result[submission_id] = (submission_config, [])

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
                # get task info
                submission_config = self.result[submission_id][0]
                task_info = submission_config['cases'][task_id]

                # read task's stdin and stdout
                base_path = f'{self.SUBMISSION_DIR}/{submission_id}/testcase/{task_id}'
                with open(f'{base_path}/in') as f:
                    task_in = f.read()
                with open(f'{base_path}/out') as f:
                    task_out = f.read()

                # assign a new runner
                threading.Thread(
                    target=self.create_container,
                    args=(submission_id, task_id, task_info['memoryLimit'],
                          task_info['timeLimit'], task_in, task_out,
                          submission_config['lang'])).start()

    def stop(self):
        self.do_run = False

    def create_container(self, submission_id, task_id, mem_limit, time_limit,
                         case_in, case_out, lang):
        self.container_count += 1

        runner = SubmissionRunner(submission_id,
                                  time_limit,
                                  mem_limit,
                                  case_in,
                                  case_out,
                                  lang=lang)
        if lang in { 'c11', 'cpp11' }:
            res = runner.compile()
        else:
            res = {'Status': 'AC'}

        if res['Status'] != 'CE':
            res = runner.run()

        self.container_count -= 1

        self.on_sub_task_complete(
            submission_id=submission_id,
            stdout=res['Stdout'],
            stderr=res['Stderr'],
            exit_code=res['ExitCode'],
            score=self.result[submission_id][0]['cases'][task_id]['caseScore'],
            exec_time='',
            mem_usage='',
            prob_status='')

    def on_sub_task_complete(self, submission_id, stdout, stderr, exit_code,
                             score, exec_time, mem_usage, prob_status):
        # if id not exists
        if submission_id not in self.result:
            print('Unexisted id recieved')
            return False

        info = self.result[submission_id][0]
        results = self.result[submission_id][1]
        results.append({
            'stdout': stdout,
            'stderr': stderr,
            'exit_code': exit_code,
            'score': score,
            'exec_time': exec_time,
            'mem_usage': mem_usage,
            'prob_status': prob_status
        })

        if len(results) > len(info['cases']):
            print('task number more than excepted.')
            return False
        elif len(results) == len(info['cases']):
            self.on_submission_complete(submission_id)
        return True

    def on_submission_complete(self, submission_id):
        endpoint = f'{self.HTTP_HANDLER_URL}/result'
        submission_data = {
            ## not implementated yet
            # 'stdout': stdout,
            # 'stderr': stderr,
            # 'exitCode': exit_code,
            # 'score': score,
            # 'execTime': exec_time,
            # 'memoryUsage': mem_usage,
            # 'problemStatusId': prob_status,
            # 'toekn': token
        }
        res = requests.post(endpoint, data=submission_data)
        # remove this submission
        del self.result[submission_id]
        # TODO: should i clean submission files here?

        if res.status_code != 200:
            # TODO: error log here, maybe
            return False
        return True


if __name__ == "__main__":
    dispatcher = Dispatcher()
