import json

import docker


class submission_runner():
    """docstring for submission_runner"""
    def __init__(self,
                 submission_id,
                 time_limit,
                 mem_limit,
                 special_judge=False,
                 image='ubuntu:18.04',
                 lang=None):
        # config file
        with open('.config/submission.json') as f:
            config = json.load(f)
        # docker
        self.client = docker.APIClient(base_url=config['docker_url'])
        self.image = image  # str
        # optional
        self.lang = lang  # str
        self.special_judge = special_judge  # bool
        # required
        self.submission_id = submission_id  # str
        self.time_limit = time_limit  # int ms
        self.mem_limit = mem_limit  # int kb
        # working_dir
        self.voulme = config['working_dir']

    def c_cpp_compile(self):
        compile_argument = {
            'c11':
            'gcc -DONLINE_JUDGE -O2 -Wall -fmax-errors=3 -std=c11 main.c -lm -o main',
            'cpp11':
            'g++ -DONLINE_JUDGE -O2 -Wall -fmax-errors=3 -std=c++11 main.cpp -lm -o main'
        }
        volume = {
            f'{self.voulme}/{self.submission_id}': {
                'bind': '/submission',
                'mode': 'rw'
            }
        }
        working_dir = '/submission'
        host_config = self.client.create_host_config(
            binds={
                f'{self.voulme}/{self.submission_id}': {
                    'bind': '/submission',
                    'mode': 'rw'
                }
            })
        container = self.client.create_container(
            image='c_cpp',
            command=compile_argument[self.lang],
            detach=True,
            volumes=volume,
            network_disabled=True,
            working_dir=working_dir,
            host_config=host_config)
        if container.get('Warning'):
            print('Warning: {}'.format(container.get('Warning')))
        self.client.start(container=container.get('Id'))
        self.client.wait(container=container,
                         timeout=5 * self.time_limit / 1000)
        stdout = self.client.logs(container=container,
                                  stdout=True,
                                  stderr=False).decode('utf-8')
        stderr = self.client.logs(container, stdout=False,
                                  stderr=True).decode('utf-8')
        self.client.remove_container(container, v=True, force=True)
        return stdout, stderr

    def c_cpp_run(self):
        execute = './main'
        volume = {
            f'{self.voulme}/{self.submission_id}': {
                'bind': '/submission',
                'mode': 'ro'
            }
        }
        working_dir = '/submission'
        host_config = self.client.create_host_config(
            binds={
                f'{self.voulme}/{self.submission_id}': {
                    'bind': '/submission',
                    'mode': 'ro'
                }
            })
        container = self.client.create_container(image='c_cpp',
                                                 command=execute,
                                                 detach=True,
                                                 volumes=volume,
                                                 network_disabled=True,
                                                 working_dir=working_dir,
                                                 host_config=host_config)
        if container.get('Warning'):
            print('Warning: {}'.format(container.get('Warning')))
        self.client.start(container=container.get('Id'))
        self.client.wait(container=container,
                         timeout=5 * self.time_limit / 1000)
        stdout = self.client.logs(container=container,
                                  stdout=True,
                                  stderr=False).decode('utf-8')
        stderr = self.client.logs(container, stdout=False,
                                  stderr=True).decode('utf-8')
        self.client.remove_container(container, v=True, force=True)
        return stdout, stderr

    def python_run(self):
        execute = 'python3 main.py'
        volume = {
            f'{self.voulme}/{self.submission_id}': {
                'bind': '/submission',
                'mode': 'ro'
            }
        }
        working_dir = '/submission'
        host_config = self.client.create_host_config(
            binds={
                f'{self.voulme}/{self.submission_id}': {
                    'bind': '/submission',
                    'mode': 'ro'
                }
            })
        container = self.client.create_container(
            image='python:3.7.5-alpine3.9',
            command=execute,
            detach=True,
            volumes=volume,
            network_disabled=True,
            working_dir=working_dir,
            host_config=host_config)
        if container.get('Warning'):
            print('Warning: {}'.format(container.get('Warning')))
        self.client.start(container=container.get('Id'))
        self.client.wait(container=container,
                         timeout=5 * self.time_limit / 1000)
        stdout = self.client.logs(container=container,
                                  stdout=True,
                                  stderr=False).decode('utf-8')
        stderr = self.client.logs(container, stdout=False,
                                  stderr=True).decode('utf-8')
        self.client.remove_container(container, v=True, force=True)
        return stdout, stderr

