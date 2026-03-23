import io
import json
import pathlib
import threading
import time
from unittest.mock import MagicMock, patch, PropertyMock
from zipfile import ZipFile

import pytest
import requests

import runner_client
from runner_client import JobInfo, Runner


@pytest.fixture(autouse=True)
def reset_module_globals(tmp_path, monkeypatch):
    """Reset module-level globals for each test."""
    monkeypatch.setattr(runner_client, 'BACKEND_API', 'http://test:8080')
    monkeypatch.setattr(runner_client, 'RUNNER_TOKEN', 'test-token')
    monkeypatch.setattr(runner_client, 'RUNNER_NAME', 'test-runner')
    monkeypatch.setattr(runner_client, 'POLL_INTERVAL', 1)
    monkeypatch.setattr(runner_client, 'MAX_CONCURRENT', 4)
    monkeypatch.setattr(runner_client, 'SUBMISSION_DIR', tmp_path / 'submissions')
    (tmp_path / 'submissions').mkdir()


@pytest.fixture
def runner():
    return Runner()


def _make_zip(files: dict[str, str]) -> bytes:
    """Create an in-memory zip with the given {filename: content} mapping."""
    buf = io.BytesIO()
    with ZipFile(buf, 'w') as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


# ── poll_for_jobs ────────────────────────────────────────────────

class TestPollForJobs:
    def test_returns_jobs_on_success(self, runner):
        jobs = [{'submissionId': 'abc123'}]
        runner.session.get = MagicMock(return_value=MagicMock(
            ok=True,
            json=MagicMock(return_value={'data': {'jobs': jobs}}),
        ))
        assert runner.poll_for_jobs() == jobs

    def test_returns_empty_on_http_error(self, runner):
        runner.session.get = MagicMock(return_value=MagicMock(
            ok=False, status_code=500, text='Internal Server Error',
        ))
        assert runner.poll_for_jobs() == []

    def test_returns_empty_on_network_error(self, runner):
        runner.session.get = MagicMock(side_effect=requests.ConnectionError)
        assert runner.poll_for_jobs() == []


# ── claim_job ────────────────────────────────────────────────────

class TestClaimJob:
    def test_returns_job_info_on_success(self, runner):
        data = {
            'submissionId': 'abc123',
            'problemId': 1,
            'language': 0,
            'token': 'tok',
            'meta': {'tasks': []},
        }
        runner.session.post = MagicMock(return_value=MagicMock(
            ok=True,
            status_code=200,
            json=MagicMock(return_value={'data': data}),
        ))
        job = runner.claim_job('abc123')
        assert job == JobInfo(
            submission_id='abc123',
            problem_id=1,
            language=0,
            token='tok',
            meta={'tasks': []},
        )

    def test_returns_none_on_conflict(self, runner):
        runner.session.post = MagicMock(return_value=MagicMock(
            ok=False, status_code=409,
        ))
        assert runner.claim_job('abc123') is None

    def test_returns_none_on_other_error(self, runner):
        runner.session.post = MagicMock(return_value=MagicMock(
            ok=False, status_code=500,
        ))
        assert runner.claim_job('abc123') is None

    def test_returns_none_on_network_error(self, runner):
        runner.session.post = MagicMock(side_effect=requests.ConnectionError)
        assert runner.claim_job('abc123') is None


# ── download_code / download_testdata ────────────────────────────

class TestDownloads:
    def test_download_code_extracts_to_src(self, runner, tmp_path):
        zip_bytes = _make_zip({'main.c': '#include <stdio.h>'})
        runner.session.get = MagicMock(return_value=MagicMock(
            content=zip_bytes,
            raise_for_status=MagicMock(),
        ))
        dest = tmp_path / 'job1'
        dest.mkdir()
        runner.download_code('job1', dest)
        assert (dest / 'src' / 'main.c').read_text() == '#include <stdio.h>'

    def test_download_testdata_extracts_to_testcase(self, runner, tmp_path):
        zip_bytes = _make_zip({'0000.in': '1 2\n', '0000.out': '3\n'})
        runner.session.get = MagicMock(return_value=MagicMock(
            content=zip_bytes,
            raise_for_status=MagicMock(),
        ))
        dest = tmp_path / 'job1'
        dest.mkdir()
        runner.download_testdata('job1', dest)
        assert (dest / 'testcase' / '0000.in').read_text() == '1 2\n'
        assert (dest / 'testcase' / '0000.out').read_text() == '3\n'

    def test_download_code_raises_on_http_error(self, runner, tmp_path):
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError('404')
        runner.session.get = MagicMock(return_value=resp)
        dest = tmp_path / 'job1'
        dest.mkdir()
        with pytest.raises(requests.HTTPError):
            runner.download_code('job1', dest)


# ── send_heartbeat ───────────────────────────────────────────────

class TestHeartbeat:
    def test_send_heartbeat_posts_correctly(self, runner):
        runner.session.post = MagicMock()
        runner.send_heartbeat('abc123')
        runner.session.post.assert_called_once()
        args, kwargs = runner.session.post.call_args
        assert 'heartbeat' in args[0]
        assert kwargs['json']['submissionId'] == 'abc123'

    def test_send_heartbeat_swallows_errors(self, runner):
        runner.session.post = MagicMock(side_effect=requests.ConnectionError)
        # Should not raise
        runner.send_heartbeat('abc123')


# ── report_result ────────────────────────────────────────────────

class TestReportResult:
    def test_report_result_success(self, runner):
        runner.session.put = MagicMock(return_value=MagicMock(ok=True))
        tasks = [[{'status': 'AC', 'stdout': '', 'stderr': '',
                    'exitCode': 0, 'execTime': 100, 'memoryUsage': 1024}]]
        assert runner.report_result('abc123', 'tok', tasks) is True

    def test_report_result_failure(self, runner):
        runner.session.put = MagicMock(return_value=MagicMock(
            ok=False, status_code=500, text='error',
        ))
        assert runner.report_result('abc123', 'tok', []) is False

    def test_report_result_sends_correct_payload(self, runner):
        runner.session.put = MagicMock(return_value=MagicMock(ok=True))
        tasks = [['task0_case0']]
        runner.report_result('sub1', 'my-token', tasks)
        _, kwargs = runner.session.put.call_args
        assert kwargs['json'] == {'tasks': tasks, 'token': 'my-token'}


# ── run (main loop) ─────────────────────────────────────────────

class TestRunLoop:
    def test_shutdown_stops_loop(self, runner):
        """Runner should exit when shutdown is set."""
        runner.poll_for_jobs = MagicMock(return_value=[])

        def set_shutdown(*_args):
            runner.shutdown = True

        runner.poll_for_jobs.side_effect = set_shutdown
        runner.run()
        # If we get here, the loop exited correctly

    def test_skips_poll_when_no_slots(self, runner):
        """When all slots are occupied, runner should not poll."""
        with runner.running_lock:
            runner.running_jobs = 4  # MAX_CONCURRENT

        poll_called = False

        def counting_poll():
            nonlocal poll_called
            poll_called = True
            return []

        runner.poll_for_jobs = counting_poll

        # On first sleep (slots full), set shutdown and reset running_jobs
        # so the drain loop also exits immediately.
        def mock_sleep(_):
            runner.shutdown = True
            with runner.running_lock:
                runner.running_jobs = 0

        with patch.object(runner_client.time, 'sleep', side_effect=mock_sleep):
            runner.run()

        # poll should not have been called since slots were full
        assert not poll_called

    def test_claims_and_processes_jobs(self, runner):
        """Runner should claim available jobs and spawn processing threads."""
        jobs = [{'submissionId': 'sub1'}]
        job_info = JobInfo('sub1', 1, 0, 'tok', {})

        call_count = 0

        def poll_then_shutdown():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return jobs
            runner.shutdown = True
            return []

        runner.poll_for_jobs = poll_then_shutdown
        runner.claim_job = MagicMock(return_value=job_info)
        # Mock process_job to also decrement running_jobs (like the real finally block)
        def mock_process(job):
            with runner.running_lock:
                runner.running_jobs -= 1

        runner.process_job = MagicMock(side_effect=mock_process)

        with patch.object(runner_client.time, 'sleep', return_value=None):
            runner.run()

        runner.claim_job.assert_called_with('sub1')
