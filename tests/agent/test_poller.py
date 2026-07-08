import threading
import time
from unittest.mock import MagicMock, patch

from agent.client import BackendClient
from agent.poller import PollerThread


def _make_dispatcher(can_accept=True):
    d = MagicMock()
    d.has_capacity.return_value = can_accept
    return d


def test_poller_does_nothing_when_no_jobs():
    client = MagicMock(spec=BackendClient)
    client.next_job.return_value = None
    dispatcher = _make_dispatcher()
    shutdown = threading.Event()

    poller = PollerThread(
        client=client,
        runner_id="rn_1",
        dispatcher=dispatcher,
        poll_interval_sec=0.05,
        shutdown_event=shutdown,
    )
    poller.start()
    time.sleep(0.15)
    shutdown.set()
    poller.join(timeout=1)

    assert client.next_job.call_count >= 2
    dispatcher.handle.assert_not_called()


def test_poller_dispatches_job_when_received(tmp_path):
    """When a job comes back, poller downloads code and calls dispatcher."""
    client = MagicMock(spec=BackendClient)
    job_payload = {
        "job_id":
        "jb_1",
        "submission_id":
        "sub_1",
        "problem_id":
        42,
        "language":
        0,
        "code_url":
        "http://minio/code.zip",
        "checker":
        "",
        "tasks": [{
            "task_id": 0,
            "case_count": 1,
            "memory_limit": 1024,
            "time_limit": 1000
        }],
    }
    client.next_job.side_effect = [job_payload, None, None]
    dispatcher = _make_dispatcher()
    shutdown = threading.Event()

    with patch("agent.poller.prepare_submission_dir_for_job") as prepare:
        poller = PollerThread(
            client=client,
            runner_id="rn_1",
            dispatcher=dispatcher,
            poll_interval_sec=0.05,
            shutdown_event=shutdown,
        )
        poller.start()
        time.sleep(0.15)
        shutdown.set()
        poller.join(timeout=1)

    prepare.assert_called_once()
    dispatcher.handle.assert_called_once_with(
        submission_id="sub_1",
        job_id="jb_1",
    )


def test_poller_aborts_job_when_prepare_fails():
    client = MagicMock(spec=BackendClient)
    job_payload = {
        "job_id": "jb_1",
        "submission_id": "sub_1",
        "problem_id": 42,
        "language": 0,
        "code_url": "http://minio/code.zip",
        "checker": "",
        "tasks": [],
    }
    client.next_job.side_effect = [job_payload, None, None]
    dispatcher = _make_dispatcher()
    shutdown = threading.Event()

    with patch("agent.poller.prepare_submission_dir_for_job",
               side_effect=RuntimeError("download failed")):
        poller = PollerThread(
            client=client,
            runner_id="rn_1",
            dispatcher=dispatcher,
            poll_interval_sec=0.05,
            shutdown_event=shutdown,
        )
        poller.start()
        time.sleep(0.15)
        shutdown.set()
        poller.join(timeout=1)

    client.abort_job.assert_called_once_with(
        runner_id="rn_1",
        job_id="jb_1",
        reason="download failed",
    )
    dispatcher.handle.assert_not_called()


def test_poller_skips_when_dispatcher_full():
    client = MagicMock(spec=BackendClient)
    dispatcher = _make_dispatcher(can_accept=False)
    shutdown = threading.Event()

    poller = PollerThread(
        client=client,
        runner_id="rn_1",
        dispatcher=dispatcher,
        poll_interval_sec=0.05,
        shutdown_event=shutdown,
    )
    poller.start()
    time.sleep(0.15)
    shutdown.set()
    poller.join(timeout=1)

    # When at capacity, poller should NOT call next_job
    client.next_job.assert_not_called()


def test_poller_swallows_transient_errors():
    client = MagicMock(spec=BackendClient)
    client.next_job.side_effect = [
        BackendClient.TransientError("boom"),
        None,
        None,
    ]
    dispatcher = _make_dispatcher()
    shutdown = threading.Event()

    poller = PollerThread(
        client=client,
        runner_id="rn_1",
        dispatcher=dispatcher,
        poll_interval_sec=0.05,
        shutdown_event=shutdown,
    )
    poller.start()
    time.sleep(0.2)
    shutdown.set()
    poller.join(timeout=1)

    # Despite first call failing, subsequent polls happen
    assert client.next_job.call_count >= 2
