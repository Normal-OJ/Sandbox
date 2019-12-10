import json
import logging

import docker


class Sandbox():
    def __init__(self,
                 time_limit,
                 mem_limit,
                 image,
                 src_dir,
                 command,
                 volume_readonly=True):
        with open('.config/submission.json') as f:
            config = json.load(f)
        self.time_limit = time_limit  # int:ms
        self.mem_limit = mem_limit  # int:kb
        self.image = image  # str
        self.src_dir = src_dir  # str
        self.command = command  # str
        self.volume_readonly = volume_readonly  # bool
        self.client = docker.APIClient(base_url=config['docker_url'])

    def run(self):
        read_mode = 'ro' if self.volume_readonly else 'rw'
        volume = {self.src_dir: {'bind': '/submission', 'mode': read_mode}}
        container_working_dir = '/submission'
        host_config = self.client.create_host_config(
            binds={self.src_dir: {
                'bind': '/submission',
                'mode': read_mode
            }})
        container = self.client.create_container(
            image=self.image,
            command=self.command,
            volumes=volume,
            network_disabled=True,
            working_dir=container_working_dir,
            host_config=host_config)
        if container.get('Warning'):
            docker_msg = container.get('Warning')
            logging.warning(f'Warning: {docker_msg}')

        self.client.start(container)
        exit_status = self.client.wait(container, timeout=5 * self.time_limit / 1000)

        stdout = self.client.logs(container, stdout=True,
                                  stderr=False).decode('utf-8')
        stderr = self.client.logs(container, stdout=False,
                                  stderr=True).decode('utf-8')
        self.client.remove_container(container, v=True, force=True)
        return {
            'Error': exit_status['Error'],
            'ExitCode': exit_status['StatusCode'],
            'Stdout': stdout,
            'Stderr': stderr,
            'Duration': 10,
            'MemUsage': 200,
            'Timeout': False
        }  # Error:str Exit_code:int Stdout:str Stderr:str Duration:int(ms) MemUsage:int(kb) Timeout:bool
