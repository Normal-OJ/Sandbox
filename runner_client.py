"""
Pull-based runner client for Normal OJ Sandbox.

Works like a GitHub Actions self-hosted runner:
1. Polls the backend for pending judge jobs
2. Claims a job
3. Downloads code and test data
4. Compiles and executes in Docker containers
5. Reports results back to the backend

Usage:
    python runner_client.py

Environment variables:
    BACKEND_API     - Backend server URL (default: http://web:8080)
    RUNNER_TOKEN    - Authentication token (default: KoNoSandboxDa)
    RUNNER_NAME     - Unique runner name (default: hostname)
    POLL_INTERVAL   - Seconds between polls (default: 5)
    MAX_CONCURRENT  - Max concurrent jobs (default: 4)
"""

import io
import json
import logging
import os
import pathlib
import shutil
import signal
import threading
import time
from dataclasses import dataclass
from zipfile import ZipFile

import requests

from dispatcher.constant import Language
from dispatcher.meta import Meta
from runner.submission import SubmissionRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('runner')

# Configuration from environment
BACKEND_API = os.getenv('BACKEND_API', 'http://web:8080')
RUNNER_TOKEN = os.getenv('RUNNER_TOKEN',
                         os.getenv('SANDBOX_TOKEN', 'KoNoSandboxDa'))
RUNNER_NAME = os.getenv('RUNNER_NAME', os.uname().nodename)
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))
MAX_CONCURRENT = int(os.getenv('MAX_CONCURRENT', '4'))
SUBMISSION_DIR = pathlib.Path(os.getenv('SUBMISSION_DIR', 'submissions'))
HEARTBEAT_INTERVAL = 60  # seconds

SUBMISSION_DIR.mkdir(exist_ok=True)


def _safe_extractall(zf: ZipFile, dest: pathlib.Path):
    """Extract zip while rejecting paths that escape dest (Zip Slip)."""
    dest = dest.resolve()
    for member in zf.infolist():
        target = (dest / member.filename).resolve()
        if not str(target).startswith(str(dest) + os.sep) and target != dest:
            raise ValueError(f'Zip Slip detected: {member.filename}')
    zf.extractall(dest)


@dataclass
class JobInfo:
    submission_id: str
    problem_id: int
    language: int
    token: str
    meta: dict


class Runner:
    """
    A pull-based runner that polls the backend for jobs,
    similar to a GitHub Actions self-hosted runner.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'X-Runner-Token': RUNNER_TOKEN,
            'X-Runner-Name': RUNNER_NAME,
        })
        self.running_jobs = 0
        self.running_lock = threading.Lock()
        self.shutdown = False

    def poll_for_jobs(self):
        """Poll the backend for available jobs."""
        try:
            resp = self.session.get(
                f'{BACKEND_API}/runner/jobs',
                timeout=30,
            )
            if not resp.ok:
                logger.warning(
                    f'Failed to poll for jobs: {resp.status_code} {resp.text}')
                return []
            data = resp.json().get('data', {})
            return data.get('jobs', [])
        except requests.RequestException as e:
            logger.error(f'Error polling for jobs: {e}')
            return []

    def claim_job(self, submission_id: str) -> JobInfo | None:
        """Claim a job from the backend."""
        try:
            resp = self.session.post(
                f'{BACKEND_API}/runner/jobs/{submission_id}/claim',
                timeout=30,
            )
            if resp.status_code == 409:
                logger.info(f'Job {submission_id} already claimed')
                return None
            if not resp.ok:
                logger.warning(
                    f'Failed to claim job {submission_id}: {resp.status_code}')
                return None
            data = resp.json()['data']
            return JobInfo(
                submission_id=data['submissionId'],
                problem_id=data['problemId'],
                language=data['language'],
                token=data['token'],
                meta=data['meta'],
            )
        except requests.RequestException as e:
            logger.error(f'Error claiming job {submission_id}: {e}')
            return None

    def download_code(self, submission_id: str, dest_dir: pathlib.Path):
        """Download source code zip and extract to dest_dir/src/."""
        resp = self.session.get(
            f'{BACKEND_API}/runner/jobs/{submission_id}/code',
            timeout=60,
        )
        resp.raise_for_status()
        src_dir = dest_dir / 'src'
        src_dir.mkdir(exist_ok=True)
        with ZipFile(io.BytesIO(resp.content)) as zf:
            _safe_extractall(zf, src_dir)

    def download_testdata(self, submission_id: str, dest_dir: pathlib.Path):
        """Download testdata zip and extract to dest_dir/testcase/."""
        resp = self.session.get(
            f'{BACKEND_API}/runner/jobs/{submission_id}/testdata',
            timeout=120,
        )
        resp.raise_for_status()
        testcase_dir = dest_dir / 'testcase'
        testcase_dir.mkdir(exist_ok=True)
        with ZipFile(io.BytesIO(resp.content)) as zf:
            _safe_extractall(zf, testcase_dir)

    def send_heartbeat(self, submission_id: str):
        """Send heartbeat to extend claim timeout."""
        try:
            self.session.post(
                f'{BACKEND_API}/runner/heartbeat',
                json={'submissionId': submission_id},
                timeout=10,
            )
        except requests.RequestException:
            pass

    def report_result(self, submission_id: str, token: str, tasks: list):
        """Report job completion to the backend."""
        resp = self.session.put(
            f'{BACKEND_API}/runner/jobs/{submission_id}/complete',
            json={
                'tasks': tasks,
                'token': token,
            },
            timeout=60,
        )
        if not resp.ok:
            logger.error(f'Failed to report result for {submission_id}: '
                         f'{resp.status_code} {resp.text}')
            return False
        return True

    def process_job(self, job: JobInfo):
        """
        Process a single job: download data, compile, execute, report.
        Runs in a separate thread.
        """
        submission_id = job.submission_id
        submission_dir = SUBMISSION_DIR / submission_id
        heartbeat_stop = threading.Event()

        try:
            logger.info(f'Processing job {submission_id} '
                        f'[problem={job.problem_id}, lang={job.language}]')

            # Prepare submission directory
            if submission_dir.exists():
                shutil.rmtree(submission_dir)
            submission_dir.mkdir(parents=True)

            # Write meta.json
            meta = Meta.parse_obj(job.meta)
            (submission_dir / 'meta.json').write_text(meta.json())

            # Download code and testdata
            self.download_code(submission_id, submission_dir)
            self.download_testdata(submission_id, submission_dir)

            # Move chaos files to src directory (same as file_manager.extract)
            testcase_dir = submission_dir / 'testcase'
            src_dir = submission_dir / 'src'
            chaos_dir = testcase_dir / 'chaos'
            if chaos_dir.exists() and chaos_dir.is_dir():
                for chaos_file in chaos_dir.iterdir():
                    shutil.move(str(chaos_file), str(src_dir))
                os.rmdir(chaos_dir)

            # Start heartbeat thread
            def heartbeat_loop():
                while not heartbeat_stop.is_set():
                    heartbeat_stop.wait(HEARTBEAT_INTERVAL)
                    if not heartbeat_stop.is_set():
                        self.send_heartbeat(submission_id)

            hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
            hb_thread.start()

            # Compile if needed
            lang = Language(job.language)
            lang_str = ['c11', 'cpp17', 'python3'][int(lang)]
            compile_result = None

            if lang in {Language.C, Language.CPP}:
                logger.info(f'Compiling {submission_id}')
                compile_result = SubmissionRunner(
                    submission_id=submission_id,
                    time_limit=-1,
                    mem_limit=-1,
                    testdata_input_path='',
                    testdata_output_path='',
                    lang=lang_str,
                ).compile()
                logger.info(f'Compile result: {compile_result["Status"]}')

            # Read config once for host path resolution
            with open('.config/submission.json') as f:
                s_config = json.load(f)
            host_base = pathlib.Path(
                s_config['working_dir']) / submission_id / 'testcase'
            container_base = submission_dir / 'testcase'

            # Execute each test case
            results = {}
            for i, task in enumerate(meta.tasks):
                for j in range(task.caseCount):
                    case_no = f'{i:02d}{j:02d}'

                    # Check compile result
                    if compile_result and compile_result['Status'] == 'CE':
                        results[case_no] = {
                            'stdout': compile_result.get('Stdout', ''),
                            'stderr': compile_result.get('Stderr', ''),
                            'exitCode': -1,
                            'execTime': -1,
                            'memoryUsage': -1,
                            'status': 'CE',
                        }
                        continue

                    in_path = str((host_base / f'{case_no}.in').absolute())
                    out_path = str(
                        (container_base / f'{case_no}.out').absolute())

                    logger.info(f'Executing {submission_id}/{case_no}')
                    runner = SubmissionRunner(
                        submission_id=submission_id,
                        time_limit=task.timeLimit,
                        mem_limit=task.memoryLimit,
                        testdata_input_path=in_path,
                        testdata_output_path=out_path,
                        lang=lang_str,
                    )
                    res = runner.run()
                    results[case_no] = {
                        'stdout': res.get('Stdout', ''),
                        'stderr': res.get('Stderr', ''),
                        'exitCode': res.get('DockerExitCode', -1),
                        'execTime': res.get('Duration', -1),
                        'memoryUsage': res.get('MemUsage', -1),
                        'status': res['Status'],
                    }

            # Convert results to task-based format
            submission_result = {}
            for no, r in results.items():
                task_no = int(no[:2])
                case_no = int(no[2:])
                if task_no not in submission_result:
                    submission_result[task_no] = {}
                submission_result[task_no][case_no] = r

            task_list = []
            for task_no in sorted(submission_result.keys()):
                cases = submission_result[task_no]
                task_list.append([cases[c] for c in sorted(cases.keys())])

            # Report results
            logger.info(f'Reporting results for {submission_id}')
            success = self.report_result(submission_id, job.token, task_list)
            if success:
                logger.info(f'Job {submission_id} completed successfully')
                # Clean up
                shutil.rmtree(submission_dir, ignore_errors=True)
            else:
                logger.error(f'Failed to report results for {submission_id}')

        except Exception as e:
            logger.exception(f'Error processing job {submission_id}: {e}')
        finally:
            heartbeat_stop.set()
            with self.running_lock:
                self.running_jobs -= 1

    def run(self):
        """Main runner loop — poll, claim, process."""
        logger.info(f'Runner "{RUNNER_NAME}" starting')
        logger.info(f'Backend: {BACKEND_API}')
        logger.info(f'Max concurrent jobs: {MAX_CONCURRENT}')
        logger.info(f'Poll interval: {POLL_INTERVAL}s')

        # Handle graceful shutdown
        def on_signal(signum, frame):
            logger.info('Shutting down...')
            self.shutdown = True

        signal.signal(signal.SIGINT, on_signal)
        signal.signal(signal.SIGTERM, on_signal)

        while not self.shutdown:
            # Check if we can take more jobs
            with self.running_lock:
                available_slots = MAX_CONCURRENT - self.running_jobs

            if available_slots <= 0:
                time.sleep(POLL_INTERVAL)
                continue

            # Poll for jobs
            jobs = self.poll_for_jobs()
            if not jobs:
                time.sleep(POLL_INTERVAL)
                continue

            # Try to claim and process jobs
            for job_info in jobs[:available_slots]:
                job = self.claim_job(job_info['submissionId'])
                if job is None:
                    continue

                with self.running_lock:
                    self.running_jobs += 1

                thread = threading.Thread(
                    target=self.process_job,
                    args=(job, ),
                    daemon=True,
                )
                thread.start()

            time.sleep(POLL_INTERVAL)

        # Wait for running jobs to finish
        logger.info('Waiting for running jobs to complete...')
        while True:
            with self.running_lock:
                if self.running_jobs == 0:
                    break
            time.sleep(1)
        logger.info('Runner stopped')


if __name__ == '__main__':
    Runner().run()
