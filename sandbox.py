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
                 lang_id,
                 compile_need,
                 stdin_path=None):
        with open('.config/submission.json') as f:
            config = json.load(f)
        self.time_limit = time_limit  # int:ms
        self.mem_limit = mem_limit  # int:kb
        self.image = image  # str
        self.src_dir = src_dir  # str
        self.stdin_path = stdin_path  # str
        self.lang_id = lang_id  # str
        self.compile_need = compile_need  # bool
        self.client = docker.APIClient(base_url=config['docker_url'])

    def run(self):
        # docker container settings
        stdin_path = '/dev/null' if not self.stdin_path else '/testdata/in'
        command_sandbox = f'sandbox {self.lang_id} {self.compile_need} {stdin_path} /result/stdout /result/stderr {self.time_limit} {self.mem_limit} 1 1073741824 10 /result/result'  # 10 process 1GB output limit
        volume = {
            self.src_dir: {
                'bind': '/src',
                'mode': 'rw'
            },
            self.stdin_path: {
                'bind': '/testdata/in',
                'mode': 'ro'
            }
        }
        container_working_dir = '/src'
        host_config = self.client.create_host_config(
            binds={
                self.src_dir: {
                    'bind': '/src',
                    'mode': 'rw'
                },
                self.stdin_path: {
                    'bind': '/testdata/in',
                    'mode': 'ro'
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
            result = self.get(container=container,
                              path='/result/',
                              filename='result').split('\n')
            stdout = self.get(container=container,
                              path='/result/',
                              filename='stdout')
            stderr = self.get(container=container,
                              path='/result/',
                              filename='stderr')
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
