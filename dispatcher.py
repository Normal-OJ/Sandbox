import docker
import json
import os
import threading
import time
from queue import Queue

SUBMISSION_DIR = './submissions'

class Dispatcher:
    def __init__(self):
        # read config
        with open('.config/dispatcher.json') as f:
            config = json.load(f)
        self.client = docker.from_env()
        # submission queue
        self.queue = Queue(config.get('QUEUE_SIZE', 0))
        # manage containers 
        self.MAX_CONTAINER_SIZE = config.get('MAX_CONTAINER_NUMBER', 0)
        self.containers = {}

        self.thread = threading.Thread(target=self._run)

    def handle(self, submission):
        '''
        '''
        if self.queue.full():
            print('Queue is full now.')
            return False
        # if not os.path.isdir(f'{SUBMISSION_DIR}/{submission}'):
        #     print('No submission found.')
        #     return False
        
        self.queue.put(submission)

    def run(self):
        self.thread.start()

    def stop(self):
        self.thread.stop()

    def _run(self):
        while True:
            if not self.queue.empty() and \
                len(self.containers) <= self.MAX_CONTAINER_SIZE:
                submission = self.queue.get()
                container = self.create_container()
                self.containers[container.id] = container

    def create_container(self):
        container = self.client.containers.run('alpine', 'echo Hello, world!', detach=True)

        print(f'{container} created.')

        return container
        
if __name__ == "__main__":
    dispatcher = Dispatcher()
    dispatcher.run()

    while True:
        dispatcher.handle(input('>>> '))
        time.sleep(1)
