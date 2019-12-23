import json
import logging
import os
import tarfile
from io import BytesIO
from uuid import uuid1

import docker


class Sandbox():
    def __init__(self,
                 time_limit,
                 mem_limit,
                 image,
                 src_dir,
                 command,
                 compile,
                 stdin_path=None,
                 volume_readonly=True):
        with open('.config/submission.json') as f:
            config = json.load(f)
        self.time_limit = time_limit  # int:ms
        self.mem_limit = mem_limit  # int:kb
        self.image = image  # str
        self.src_dir = src_dir  # str
        self.stdin_path = stdin_path  # str
        self.command = command  # str
        self.compile = compile  # bool
        self.client = docker.APIClient(base_url=config['docker_url'])

    def run(self):
        # docker container settings
        stdin_path = '/dev/null' if not self.stdin_path else '/testdata/in'
        command_sandbox = self.command if self.compile else f'sandbox "{self.command}" {stdin_path} /result/stdout /result/stderr {self.time_limit} {self.mem_limit} 1 1048576 10 /result/result'  # 10 process 1GB output limit
        read_mode = 'rw'
        volume = {
            self.src_dir: {
                'bind': '/src',
                'mode': read_mode
            },
            self.stdin_path: {
                'bind': '/testdata/in',
                'mode': read_mode
            }
        }
        container_working_dir = '/src'
        host_config = self.client.create_host_config(
            binds={
                self.src_dir: {
                    'bind': '/src',
                    'mode': read_mode
                },
                self.stdin_path: {
                    'bind': '/testdata/in',
                    'mode': read_mode
                }
            })

        container = self.client.create_container(
            image=self.image,
            command=command_sandbox,
            volumes=volume,
            network_disabled=True,
            working_dir=container_working_dir,
            host_config=host_config)
        if container.get('Warning'):
            docker_msg = container.get('Warning')
            logging.warning(f'Warning: {docker_msg}')

        # start and wait container
        self.client.start(container)

        try:
            exit_status = self.client.wait(container,
                                           timeout=5 * self.time_limit)
        except:
            self.client.remove_container(container, v=True, force=True)
            return {'Status': 'JE'}

        # result retrive
        try:
            result = ['', '', -1, -1] if self.compile else self.get(
                container=container, path='/result/',
                filename='result').split('\n')

            stdout = self.client.logs(
                container, stdout=True,
                stderr=False).decode('utf-8') if self.compile else self.get(
                    container=container, path='/result/', filename='stdout')
            stderr = self.client.logs(
                container, stdout=False,
                stderr=True).decode('utf-8') if self.compile else self.get(
                    container=container, path='/result/', filename='stderr')
        except:
            self.client.remove_container(container, v=True, force=True)
            return {'Status': 'JE'}

        self.client.remove_container(container, v=True, force=True)
        return {
            'Status': result[0],
            'Duration': int(result[2]),
            'MemUsage': int(result[3]),
            'Stdout': stdout,
            'Stderr': stderr,
            'ExitMsg': result[1],
            'DockerError': exit_status['Error'],
            'DockerExitCode': exit_status['StatusCode']
        }  # tdout:str Stderr:str Duration:int(ms) MemUsage:int(kb)

    def get(self, container, path, filename):
        bits, stat = self.client.get_archive(container, f'{path}{filename}')
        tarbits = b''.join(chunk for chunk in bits)
        tar = tarfile.open(fileobj=BytesIO(tarbits))
        extract_path = f'/tmp/{uuid1()}'
        tar.extract(filename, extract_path)
        with open(f'{extract_path}/{filename}', 'r') as f:
            contents = f.read()
        os.remove(f'{extract_path}/{filename}')
        os.rmdir(extract_path)
        return contents
