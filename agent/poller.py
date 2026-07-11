"""Poller daemon thread: pulls jobs from backend, hands to dispatcher."""
import logging
import tempfile
import threading

from dispatcher.constant import Language
from dispatcher.testdata import (
    ensure_testdata,
    get_problem_meta,
    get_problem_root,
)
from .client import BackendClient

log = logging.getLogger(__name__)


def prepare_submission_dir_for_job(dispatcher, job: dict,
                                   client: BackendClient):
    """Download code + ensure testdata + extract into dispatcher's submission dir.

    Reuses the existing dispatcher.prepare_submission_dir() — same testdata
    fetching path as the old POST /submit handler.
    """
    submission_id = job["submission_id"]
    problem_id = job["problem_id"]
    language = Language(job["language"])

    ensure_testdata(problem_id)
    meta = get_problem_meta(problem_id, language)

    with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
        client.download_code(job["code_url"], tmp.name)
        with open(tmp.name, "rb") as src:
            dispatcher.prepare_submission_dir(
                root_dir=dispatcher.SUBMISSION_DIR,
                submission_id=submission_id,
                meta=meta,
                source=src,
                testdata=get_problem_root(problem_id),
            )


class PollerThread(threading.Thread):
    """Polls backend for jobs and dispatches them to the internal dispatcher."""

    def __init__(
        self,
        client: BackendClient,
        runner_id: str,
        dispatcher,  # existing Dispatcher instance
        poll_interval_sec: float,
        shutdown_event: threading.Event,
    ):
        super().__init__(daemon=True, name="poller")
        self.client = client
        self.runner_id = runner_id
        self.dispatcher = dispatcher
        self.poll_interval_sec = poll_interval_sec
        self.shutdown_event = shutdown_event

    def run(self) -> None:
        while not self.shutdown_event.is_set():
            if not self.dispatcher.has_capacity():
                self.shutdown_event.wait(timeout=0.5)
                continue
            try:
                job = self.client.next_job(runner_id=self.runner_id)
            except BackendClient.TransientError as e:
                log.warning(f"next_job failed: {e}")
                self.shutdown_event.wait(timeout=self.poll_interval_sec)
                continue
            except BackendClient.AuthError as e:
                log.error(f"next_job auth failed: {e}")
                self.shutdown_event.set()
                break
            except Exception as e:
                log.warning(f"next_job unexpected error: {e}")
                self.shutdown_event.wait(timeout=self.poll_interval_sec)
                continue

            if job is None:
                self.shutdown_event.wait(timeout=self.poll_interval_sec)
                continue

            try:
                self._prepare_with_retry(job)
                self.dispatcher.handle(
                    submission_id=job["submission_id"],
                    job_id=job["job_id"],
                )
                log.info(f"dispatched submission={job['submission_id']} "
                         f"job={job['job_id']}")
            except Exception as e:
                log.exception(
                    f"failed to dispatch job {job.get('job_id')}: {e}")
                self._abort_with_retry(job["job_id"], str(e))
                self.shutdown_event.wait(timeout=self.poll_interval_sec)

    def _prepare_with_retry(self, job: dict) -> None:
        backoff = self.poll_interval_sec
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                prepare_submission_dir_for_job(self.dispatcher, job,
                                               self.client)
                return
            except Exception as e:
                last_exc = e
                if attempt < 3:
                    log.warning(f"prepare attempt {attempt} for "
                                f"{job['job_id']} failed: {e}")
                    self.shutdown_event.wait(timeout=backoff)
                    backoff = min(backoff * 2, 30.0)
        raise last_exc

    def _abort_with_retry(self, job_id: str, reason: str) -> bool:
        backoff = self.poll_interval_sec
        for attempt in range(1, 4):
            try:
                outcome = self.client.abort_job(
                    runner_id=self.runner_id,
                    job_id=job_id,
                    reason=reason,
                )
            except BackendClient.AuthError as e:
                log.error(f"abort_job {job_id} auth failed: {e}")
                return False
            except BackendClient.TransientError as e:
                if attempt == 3:
                    log.error(
                        f"abort_job {job_id} failed after {attempt} attempts: {e}"
                    )
                    return False
                log.warning(
                    f"abort_job {job_id} attempt {attempt} failed: {e}")
                self.shutdown_event.wait(timeout=backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            log.warning(f"aborted {job_id} after prepare failure: {outcome}")
            return True
