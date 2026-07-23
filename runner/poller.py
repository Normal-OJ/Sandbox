import io
import logging
import shutil
import threading
import time

import requests

from dispatcher import file_manager, testdata
from dispatcher.config import SUBMISSION_DIR
from dispatcher.meta import Meta
from .client import BackendAPIError
from .config import REQUEST_TIMEOUT, PREP_MAX_ATTEMPTS, PREP_BACKOFF_SCHEDULE
from .result_sender import AbortRequest

logger = logging.getLogger(__name__)


def prepare_job(payload):
    """Fetch testdata + source and lay out the local job dir (spec §10).

    Talks to the legacy testdata channel (backend + redis) for now; that
    coexists until the keystone slice.
    """
    testdata.ensure_testdata(payload.problem_id)
    resp = requests.get(payload.code_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    job_dir = SUBMISSION_DIR / payload.job_id
    if job_dir.exists():
        shutil.rmtree(job_dir)  # leftover from a failed prior attempt
    file_manager.extract(
        root_dir=SUBMISSION_DIR,
        job_id=payload.job_id,
        meta=Meta.parse_obj({
            'language': payload.language,
            'tasks': payload.tasks,
        }),
        source=io.BytesIO(resp.content),
        testdata=testdata.get_problem_root(payload.problem_id),
    )


class PollerThread(threading.Thread):
    """Claims jobs from the backend and preps them for dispatch (spec §10).

    Only polls when there is spare capacity, adds the claimed job to the
    tracker before prep so the heartbeat renews the lease while downloading,
    and retries prep locally before giving up with a prep_failed abort.
    """

    def __init__(
        self,
        client,
        identity,
        tracker,
        result_queue,
        dispatch,
        *,
        prepare=None,
        poll_interval_sec=None,
        sleep=time.sleep,
    ):
        super().__init__(daemon=True)
        self._client = client
        self._identity = identity
        self._tracker = tracker
        self._result_queue = result_queue
        self._dispatch = dispatch
        self._prepare = prepare if prepare is not None else prepare_job
        self._poll_interval_sec = (poll_interval_sec
                                   if poll_interval_sec is not None else
                                   identity.config.poll_interval_sec)
        self._sleep = sleep
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            try:
                idle = self._poll_once()
            except Exception:
                # A malformed payload (or any bug) must not kill the poller:
                # the heartbeat would keep the runner looking alive while it
                # never claims work again.
                logger.exception('poller iteration failed')
                idle = True
            if idle:
                # Interruptible wait so stop() takes effect immediately.
                self._stop_event.wait(self._poll_interval_sec)

    def _poll_once(self):
        """Claim and prep one job. Returns True when the loop should idle."""
        # Capacity gate: only GET next-job when there is room (spec §10).
        if len(self._tracker) >= self._identity.config.max_concurrent_jobs:
            return True
        try:
            payload = self._client.next_job(self._identity)
        except (BackendAPIError, requests.RequestException) as err:
            # 401 also just logs; heartbeat owns fail-fast.
            logger.warning('next-job failed: %s', err)
            return True
        if payload is None:
            return True

        # Add BEFORE prep so the heartbeat renews the lease while we download;
        # removal is the sender's job once the outcome resolves.
        self._tracker.add(payload.job_id)

        for i in range(PREP_MAX_ATTEMPTS):
            try:
                self._prepare(payload)
                self._dispatch(payload.job_id, payload.submission_id)
                return False
            except Exception as err:
                # Heterogeneous causes: network, extract ValueError, queue.Full.
                logger.warning(
                    'prep/dispatch for %s failed (attempt %d/%d): %s',
                    payload.job_id, i + 1, PREP_MAX_ATTEMPTS, err)
                if i < len(PREP_BACKOFF_SCHEDULE):
                    self._sleep(PREP_BACKOFF_SCHEDULE[i])

        logger.error('prep for %s exhausted all attempts; aborting',
                     payload.job_id)
        self._result_queue.put(
            AbortRequest(payload.job_id, payload.submission_id, 'prep_failed'))
        # Do NOT remove from tracker; the sender does that after the abort.
        return False

    def stop(self):
        self._stop_event.set()
