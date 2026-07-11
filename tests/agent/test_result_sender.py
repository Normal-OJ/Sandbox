import queue
import threading
import time
from unittest.mock import MagicMock

import pytest

from agent.client import BackendClient
from agent.result_sender import ResultSenderThread, JobResult


def test_result_sender_delivers_one_result():
    client = MagicMock(spec=BackendClient)
    client.complete_job.return_value = "ok"
    result_queue: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()

    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=result_queue,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    result_queue.put(
        JobResult(job_id="jb_1",
                  submission_id="sub_1",
                  tasks=[{
                      "status": "AC"
                  }]))
    time.sleep(0.05)
    shutdown.set()
    sender.join(timeout=1)

    client.complete_job.assert_called_once_with(runner_id="rn_1",
                                                job_id="jb_1",
                                                tasks=[{
                                                    "status": "AC"
                                                }])


def test_result_sender_retries_on_transient_error():
    client = MagicMock(spec=BackendClient)
    client.complete_job.side_effect = [
        BackendClient.TransientError("first"),
        BackendClient.TransientError("second"),
        "ok",
    ]
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()

    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=5,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    time.sleep(0.5)
    shutdown.set()
    sender.join(timeout=1)

    assert client.complete_job.call_count == 3


def test_result_sender_drops_on_reclaimed():
    """If backend says reclaimed (409), drop the result silently — no retry."""
    client = MagicMock(spec=BackendClient)
    client.complete_job.return_value = "reclaimed"
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()

    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    time.sleep(0.05)
    shutdown.set()
    sender.join(timeout=1)

    assert client.complete_job.call_count == 1  # no retry


def test_result_sender_drops_on_not_found():
    """404 also drops — submission may have been deleted."""
    client = MagicMock(spec=BackendClient)
    client.complete_job.return_value = "not_found"
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()
    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    time.sleep(0.05)
    shutdown.set()
    sender.join(timeout=1)
    assert client.complete_job.call_count == 1


def test_result_sender_aborts_after_max_attempts():
    """After exhausting retries, abort the job so backend can release the lease."""
    client = MagicMock(spec=BackendClient)
    client.complete_job.side_effect = BackendClient.TransientError("always")
    client.abort_job.return_value = "requeued"
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()
    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    time.sleep(0.5)

    assert client.complete_job.call_count == 3
    client.abort_job.assert_called_once_with(
        runner_id="rn_1",
        job_id="jb_1",
        reason="result delivery failed after 3 attempts",
    )

    # Thread still alive, ready for next job
    assert sender.is_alive()
    shutdown.set()
    sender.join(timeout=1)


def test_result_sender_retries_abort_on_transient_errors():
    client = MagicMock(spec=BackendClient)
    client.complete_job.side_effect = BackendClient.TransientError("always")
    client.abort_job.side_effect = [
        BackendClient.TransientError("first"),
        BackendClient.TransientError("second"),
        "requeued",
    ]
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()
    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    rq.join()

    assert client.abort_job.call_count == 3
    shutdown.set()
    sender.join(timeout=1)


def test_finalize_called_after_successful_delivery():
    """On a clean 'ok' delivery, finalize(submission_id) fires exactly once."""
    client = MagicMock(spec=BackendClient)
    client.complete_job.return_value = "ok"
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()

    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    rq.join()
    shutdown.set()
    sender.join(timeout=1)

    finalize.assert_called_once_with("sub_1")


def test_finalize_called_after_giving_up():
    """Even after exhausting retries and aborting, finalize still runs —
    local state/files must not leak just because delivery failed."""
    client = MagicMock(spec=BackendClient)
    client.complete_job.side_effect = BackendClient.TransientError("always")
    client.abort_job.return_value = "requeued"
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()

    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    rq.join()
    shutdown.set()
    sender.join(timeout=1)

    finalize.assert_called_once_with("sub_1")


def test_rejected_result_aborts_and_finalizes():
    """400 from complete_job means backend permanently rejected the payload
    (e.g. schema mismatch) — abort immediately, no retry, then finalize."""
    client = MagicMock(spec=BackendClient)
    client.complete_job.return_value = "rejected"
    client.abort_job.return_value = "requeued"
    rq: queue.Queue = queue.Queue()
    shutdown = threading.Event()
    finalize = MagicMock()

    sender = ResultSenderThread(
        client=client,
        runner_id="rn_1",
        result_queue=rq,
        shutdown_event=shutdown,
        finalize=finalize,
        retry_max_attempts=3,
        retry_initial_backoff_sec=0.01,
        retry_max_backoff_sec=0.1,
    )
    sender.start()
    rq.put(JobResult(job_id="jb_1", submission_id="sub_1", tasks=[]))
    rq.join()
    shutdown.set()
    sender.join(timeout=1)

    assert client.complete_job.call_count == 1  # no retry
    client.abort_job.assert_called_once_with(
        runner_id="rn_1",
        job_id="jb_1",
        reason="result delivery failed after 1 attempts",
    )
    finalize.assert_called_once_with("sub_1")
