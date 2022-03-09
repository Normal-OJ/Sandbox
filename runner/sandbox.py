import json
import logging
import tarfile
import tempfile
from dataclasses import dataclass
from io import BytesIO
from typing import Optional
import docker


class JudgeError(Exception):
    pass


@dataclass
class Result:
    Status: str
    Duration: int
    MemUsage: int
    Stdout: str
    Stderr: str
    ExitMsg: str
    DockerError: str
    DockerExitCode: int


class Sandbox:

    def __init__(
        self,
        time_limit: int,  # ms
        mem_limit: int,  # KB
        image: str,
        src_dir: str,
        lang_id: str,
        compile_need: bool,
        stdin_path: Optional[str] = None,
    ):
        with open('.config/submission.json') as f:
            config = json.load(f)
        self.time_limit = time_limit
        self.mem_limit = mem_limit
        self.image = image
        self.src_dir = src_dir
        self.stdin_path = stdin_path
        self.lang_id = lang_id
        self.compile_need = compile_need
        self.client = docker.APIClient(base_url=config['docker_url'])

    def run(self):
        # docker container settings
        stdin_path = '/dev/null' if not self.stdin_path else '/testdata/in'
        command_sandbox = ' '.join(
            map(
                str,
                (
                    'sandbox',
                    self.lang_id,
                    int(self.compile_need),
                    stdin_path,
                    '/result/stdout',
                    '/result/stderr',
                    self.time_limit,
                    self.mem_limit,
                    '1',
                    '1073741824',  # 1 GB output limit
                    '10',  # 10 process
                    '/result/result',
                ),
            ))
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
            host_config=host_config,
        )
        if container.get('Warning'):
            docker_msg = container.get('Warning')
            logging.warning(f'Warning: {docker_msg}')
        # start and wait container
        self.client.start(container)
        try:
            exit_status = self.client.wait(
                container,
                timeout=5 * self.time_limit // 1000,
            )
        except Exception as e:
            self.client.remove_container(container, v=True, force=True)
            logging.error(e)
            raise JudgeError
        # retrive result
        try:
            result = self.get(
                container=container,
                path='/result/',
                filename='result',
            ).split('\n')
            stdout = self.get(
                container=container,
                path='/result/',
                filename='stdout',
            )
            stderr = self.get(
                container=container,
                path='/result/',
                filename='stderr',
            )
        except Exception as e:
            self.client.remove_container(container, v=True, force=True)
            logging.error(e)
            raise JudgeError
        self.client.remove_container(container, v=True, force=True)

        return Result(
            Status=result[0],
            Duration=int(result[2]),  # ms
            MemUsage=int(result[3]),  # KB
            Stdout=stdout,
            Stderr=stderr,
            ExitMsg=result[1],
            DockerError=exit_status['Error'],
            DockerExitCode=exit_status['StatusCode'],
        )

    def get(self, container, path, filename):
        bits, _ = self.client.get_archive(container, f'{path}{filename}')
        tarbits = b''.join(bits)
        tar = tarfile.open(fileobj=BytesIO(tarbits))
        with tempfile.TemporaryDirectory() as extract_path:
            tar.extract(filename, extract_path)
            with open(
                    f'{extract_path}/{filename}',
                    'r',
                    errors='ignore',
            ) as f:
                contents = f.read()
        return contents
