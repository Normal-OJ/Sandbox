import json
import logging
import tarfile
import tempfile
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
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
        name: Optional[str] = None,
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
        self.name = name
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
            name=self.name,
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
            result, stdout, stderr = self.get_result(
                container,
                ['result', 'stdout', 'stderr'],
            )
            result = result.split('\n')
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
            DockerError=exit_status.get('Error', ''),
            DockerExitCode=exit_status['StatusCode'],
        )

    def get_result(self, container, filenames: list[str]) -> list[str]:
        result_dir = '/result'
        bits, _ = self.client.get_archive(container, result_dir)
        with (tempfile.NamedTemporaryFile() as
              tarball, tempfile.TemporaryDirectory() as extract_path):
            for chunk in bits:
                tarball.write(chunk)
            tarball.flush()
            tarball.seek(0)
            with tarfile.open(fileobj=tarball) as tar:
                tar.extractall(extract_path)
            return [
                open(
                    Path(extract_path) / result_dir.lstrip('/') / filename,
                    'r',
                    errors='ignore',
                ).read() for filename in filenames
            ]
