import logging
import queue
import threading
import time
from dataclasses import dataclass

import requests

from dispatcher import file_manager
from .client import BackendAPIError
from .config import SEND_RETRY_BACKOFF_SCHEDULE

logger = logging.getLogger(__name__)


@dataclass
class CompleteRequest:
    job_id: str
    submission_id: str
    tasks: list


@dataclass
class AbortRequest:
    job_id: str
    submission_id: str
    reason: str  # 'drain' | 'prep_failed' | 'rejected'


def _default_cleanup(job_id):
    """Remove the local job dir, tolerating a job that was never prepped."""
    try:
        file_manager.clean_data(job_id)
    except FileNotFoundError:
        logger.warning('cleanup: job dir for %s already absent', job_id)


def _default_backup(job_id):
    """Move the local job dir aside, tolerating a missing dir."""
    try:
        file_manager.backup_data(job_id)
    except FileNotFoundError:
        logger.warning('backup: job dir for %s already absent', job_id)


class ResultSenderThread(threading.Thread):
    """Reports job outcomes (complete/abort) back to the backend (spec §7).

    Consumes CompleteRequest / AbortRequest off ``result_queue`` and retries
    each report with exponential backoff. After ``stop()`` the loop keeps
    draining whatever is already queued, then exits (the slice-4 drain
    semantic).
    """

    def __init__(
        self,
        client,
        identity,
        tracker,
        result_queue,
        *,
        cleanup=None,
        backup=None,
        sleep=time.sleep,
        queue_poll_sec=0.5,
    ):
        super().__init__(daemon=True)
        self._client = client
        self._identity = identity
        self._tracker = tracker
        self._result_queue = result_queue
        self._cleanup = cleanup if cleanup is not None else _default_cleanup
        self._backup = backup if backup is not None else _default_backup
        self._sleep = sleep
        self._queue_poll_sec = queue_poll_sec
        self._stop_event = threading.Event()

    def run(self):
        while True:
            try:
                item = self._result_queue.get(timeout=self._queue_poll_sec)
            except queue.Empty:
                # Drain semantic: only exit once stop() was requested AND the
                # queue is empty; otherwise keep waiting for more work.
                if self._stop_event.is_set():
                    return
                continue
            try:
                self._process(item)
            except Exception:
                # One bad item must not kill the sender.
                logger.exception('result sender failed to process %r', item)

    def _process(self, item):
        if isinstance(item, CompleteRequest):
            self._process_complete(item)
        elif isinstance(item, AbortRequest):
            self._process_abort(item.job_id, item.reason)
        else:
            logger.error('result sender got unknown item %r', item)

    def _process_complete(self, item):
        job_id = item.job_id
        outcome, status = self._send_with_retry(
            'complete',
            lambda: self._client.complete(self._identity, job_id, item.tasks),
            terminal_statuses=(409, 404, 400),
        )
        if outcome == 'terminal' and status == 400:
            # Backend rejected the payload: requeue via abort so the job can
            # converge (spec §7.5, INV5). Keep the job dir as evidence of
            # what the backend refused.
            logger.warning(
                'complete for %s rejected with 400; aborting as rejected',
                job_id,
            )
            self._process_abort(job_id, 'rejected', preserve_evidence=True)
            return
        try:
            if outcome == 'exhausted':
                # Keep local evidence; lease-expiry reclaim is the safety net.
                self._backup(job_id)
            else:
                self._cleanup(job_id)
        finally:
            # Even a failing cleanup must not leave the job in the tracker:
            # a lingering entry would keep the lease renewed and consume
            # capacity forever.
            self._tracker.remove(job_id)

    def _process_abort(self, job_id, reason, preserve_evidence=False):
        # Finalize local state BEFORE the send: the moment the backend
        # accepts an abort it requeues the job, and this same runner may
        # claim it again immediately -- no stale dir or tracker entry may
        # survive to that point (ABA race).
        try:
            if preserve_evidence:
                self._backup(job_id)
            else:
                self._cleanup(job_id)
        except Exception:
            # A failed finalize must not block the abort: an unsent abort
            # leaves the job leased until its lease expires.
            logger.exception('finalize before abort of %s failed', job_id)
        self._tracker.remove(job_id)
        self._send_abort(job_id, reason)

    def _send_abort(self, job_id, reason):
        outcome, _ = self._send_with_retry(
            'abort',
            lambda: self._client.abort(self._identity, job_id, reason),
            terminal_statuses=(409, 404),
        )
        return outcome

    def _send_with_retry(self, action, send, terminal_statuses):
        """Retry ``send`` until success, a terminal status, or exhaustion.

        Retries everything except 2xx (success) and ``terminal_statuses``,
        with exponential backoff, at most len(SEND_RETRY_BACKOFF_SCHEDULE)
        retries (spec §7 "Runner retry 規則"). Note 401 is deliberately NOT
        terminal here; heartbeat owns 401 fail-fast.
        """
        retries = 0
        while True:
            try:
                send()
                return ('sent', None)
            except BackendAPIError as err:
                if err.status_code in terminal_statuses:
                    logger.info('%s terminal with status %d', action,
                                err.status_code)
                    return ('terminal', err.status_code)
            except requests.RequestException:
                pass  # network error -> retry
            if retries == len(SEND_RETRY_BACKOFF_SCHEDULE):
                logger.error('%s exhausted all retries', action)
                return ('exhausted', None)
            self._sleep(SEND_RETRY_BACKOFF_SCHEDULE[retries])
            retries += 1

    def stop(self):
        self._stop_event.set()
