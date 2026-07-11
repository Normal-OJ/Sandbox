"""Result delivery daemon thread with exponential backoff retry."""
import logging
import queue
import threading
from dataclasses import dataclass
from typing import Callable, List

from .client import BackendClient

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobResult:
    """Pending result waiting to be sent to backend."""
    job_id: str
    submission_id: str
    tasks: List[dict]


class ResultSenderThread(threading.Thread):
    """Drains result_queue, POSTs to backend, retries on transient errors."""

    def __init__(
        self,
        client: BackendClient,
        runner_id: str,
        result_queue: queue.Queue,
        shutdown_event: threading.Event,
        finalize: Callable[[str], None],
        retry_max_attempts: int,
        retry_initial_backoff_sec: float,
        retry_max_backoff_sec: float,
    ):
        super().__init__(daemon=True, name="result_sender")
        self.client = client
        self.runner_id = runner_id
        self.result_queue = result_queue
        self.shutdown_event = shutdown_event
        self.finalize = finalize
        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_backoff_sec = retry_initial_backoff_sec
        self.retry_max_backoff_sec = retry_max_backoff_sec

    def run(self) -> None:
        while not (self.shutdown_event.is_set() and self.result_queue.empty()):
            try:
                job_result = self.result_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._deliver_with_retry(job_result)
            except Exception as e:  # defensive
                log.exception(
                    f"result delivery for {job_result.job_id} crashed: {e}")
            finally:
                try:
                    self.finalize(job_result.submission_id)
                except Exception as e:
                    log.exception(f"finalize failed: {e}")
                self.result_queue.task_done()

    def _deliver_with_retry(self, jr: JobResult) -> None:
        backoff = self.retry_initial_backoff_sec
        for attempt in range(1, self.retry_max_attempts + 1):
            try:
                outcome = self.client.complete_job(
                    runner_id=self.runner_id,
                    job_id=jr.job_id,
                    tasks=jr.tasks,
                )
            except BackendClient.TransientError as e:
                log.warning(
                    f"complete_job {jr.job_id} attempt {attempt} failed: {e}")
                if attempt == self.retry_max_attempts:
                    log.error(
                        f"giving up on {jr.job_id} after {attempt} attempts")
                    self._abort_failed_delivery(jr, attempt)
                    return
                self.shutdown_event.wait(timeout=backoff)
                backoff = min(backoff * 2, self.retry_max_backoff_sec)
                continue
            except BackendClient.AuthError as e:
                log.error(f"complete_job {jr.job_id} auth failed: {e}")
                return  # cannot retry without re-register
            # Outcome handling
            if outcome == "ok":
                log.info(f"delivered {jr.job_id}")
                return
            if outcome == "reclaimed":
                log.warning(f"{jr.job_id} was reclaimed; dropping result")
                return
            if outcome == "not_found":
                log.warning(
                    f"{jr.job_id} not found on backend; dropping result")
                return
            if outcome == "rejected":
                log.error(f"{jr.job_id} rejected by backend; aborting")
                self._abort_failed_delivery(jr, attempt)
                return

    def _abort_failed_delivery(self, jr: JobResult, attempts: int) -> None:
        reason = f"result delivery failed after {attempts} attempts"
        backoff = self.retry_initial_backoff_sec
        for attempt in range(1, 4):
            try:
                outcome = self.client.abort_job(
                    runner_id=self.runner_id,
                    job_id=jr.job_id,
                    reason=reason,
                )
            except BackendClient.AuthError as e:
                log.error(f"abort_job {jr.job_id} auth failed: {e}")
                return
            except BackendClient.TransientError as e:
                if attempt == 3:
                    log.error(
                        f"abort_job {jr.job_id} failed after {attempt} attempts: {e}"
                    )
                    return
                log.warning(
                    f"abort_job {jr.job_id} attempt {attempt} failed: {e}")
                self.shutdown_event.wait(timeout=backoff)
                backoff = min(backoff * 2, self.retry_max_backoff_sec)
                continue
            log.warning(
                f"aborted {jr.job_id} after failed delivery: {outcome}")
            return
