import io
import logging
import shutil
import threading

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

    Drain safety: a claim that is still in prep when stop() lands is returned
    to the backend as a 'drain' abort (see _poll_once). A claim whose prep
    happens to finish after stop() still dispatches, and is returned by the
    dispatcher instead: main.drain() joins the poller before calling
    dispatcher.drain_unstarted(), which latches the dispatcher out of starting
    new work and scans under the same lock the run loop uses. Checking the stop
    event here before dispatching could not give that guarantee on its own --
    the run loop could still mark the job started in the gap between the check
    and the scan.
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
        sleep=None,
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
        self._stop_event = threading.Event()
        # The prep backoff must be interruptible by stop(), or a drain waits
        # out the full schedule before the poller can return its claim.
        self._sleep = sleep if sleep is not None else self._stop_event.wait

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
        # Capacity gate: only GET next-job when there is room (spec §10). This
        # also bounds the task dimension: the dispatcher task queue is unbounded
        # and its volume is bounded by this job-level gate × per-job case count,
        # so dispatch can never fail on capacity.
        if len(self._tracker) >= self._identity.config.max_concurrent_jobs:
            return True
        # Do not take on new work once draining. This narrows but cannot close
        # the window -- stop() can still land while the claim HTTP call is in
        # flight -- so the prep-loop check below is what guarantees correctness.
        if self._stop_event.is_set():
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
            if self._stop_event.is_set():
                # This claim never started any work, so it has to go back as a
                # 'drain' abort: drain is attempt-neutral and requeued
                # immediately, whereas 'prep_failed' counts against the
                # submission's attempts and would let a rolling restart push
                # otherwise healthy submissions to JE (spec §7.5, §12).
                logger.info('draining: returning unstarted claim %s',
                            payload.job_id)
                self._abort(payload, 'drain')
                return False
            try:
                self._prepare(payload)
                break
            except Exception as err:
                # Heterogeneous causes: network errors, extract ValueError.
                logger.warning('prep for %s failed (attempt %d/%d): %s',
                               payload.job_id, i + 1, PREP_MAX_ATTEMPTS, err)
                if i < len(PREP_BACKOFF_SCHEDULE):
                    self._sleep(PREP_BACKOFF_SCHEDULE[i])
        else:
            # stop() can also land during the final attempt, i.e. after the
            # loop's own check has run for the last time. The claim still
            # never started any work, so drain semantics win here too.
            if self._stop_event.is_set():
                logger.info(
                    'draining: returning unstarted claim %s after a '
                    'failed prep', payload.job_id)
                self._abort(payload, 'drain')
            else:
                logger.error('prep for %s exhausted all attempts; aborting',
                             payload.job_id)
                self._abort(payload, 'prep_failed')
            return False

        try:
            self._dispatch(payload.job_id, payload.submission_id)
        except Exception as err:
            # handle() is atomic (it validates before any state/queue
            # mutation), so an exception here means the prepped job dir/meta
            # is genuinely bad -- abort(prep_failed) is the right response even
            # mid-drain, because the job must converge rather than be requeued
            # onto another runner that would hit the same bad payload.
            logger.warning('dispatch for %s failed: %s', payload.job_id, err)
            self._abort(payload, 'prep_failed')
        return False

    def _abort(self, payload, reason):
        self._result_queue.put(
            AbortRequest(payload.job_id, payload.submission_id, reason))
        # Do NOT remove from tracker; the sender does that as part of
        # finalizing the abort.

    def stop(self):
        self._stop_event.set()
