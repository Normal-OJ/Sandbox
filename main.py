"""Pull-based runner entrypoint (spec §10).

Wires the coordination threads together: the dispatcher runs judge work, the
poller claims jobs, the heartbeat renews leases, and the result sender reports
outcomes. There is no Flask and no inbound HTTP server anymore -- the runner
only talks *out* to the backend runner API.

Import-safe: nothing here starts a thread, talks to the network, or configures
logging at import time, so tests can import main and exercise the helpers
directly. (Importing dispatcher.config on the way in does create the working
directories it owns -- that predates this module.)
"""

import logging
import os
import queue
import signal
import sys
import threading
import time

from runner.client import BackendClient, BackendAuthError
from runner.active_jobs import ActiveJobTracker
from runner.config import DRAIN_TIMEOUT_SEC
from runner.heartbeat import HeartbeatThread
from runner.poller import PollerThread
from runner.registration import register_with_backoff
from runner.result_sender import (
    ResultSenderThread,
    CompleteRequest,
    AbortRequest,
)

logger = logging.getLogger('sandbox')

# Fractions of the drain budget (see drain()). Waiting for the poller is the
# least valuable thing the drain does -- an unstarted claim costs nothing to
# hand back -- and the likeliest to get stuck, since a slow-drip response can
# keep one prep attempt alive past any per-request timeout. Reserving a slice
# for the reporting tail keeps a long-running job from leaving finished
# results with no time left to send them.
POLLER_JOIN_SHARE = 0.25
REPORTING_RESERVE_SHARE = 0.25


def setup_logging():
    """Configure root logging for the runner process.

    Logs go to both a file (persisted via the bind-mounted logs/ dir) and
    stdout so ``docker logs`` shows them. Level comes from LOG_LEVEL.
    """
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO'),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        handlers=[
            logging.FileHandler('logs/sandbox.log'),
            logging.StreamHandler(),
        ],
    )


def register(client, shutdown):
    """Register with the backend, honouring shutdown during retry backoff.

    A docker stop mid-registration must not hang, so the retry sleep waits on
    ``shutdown`` and raises SystemExit the moment it is set. A rejected token
    (401) is fatal per ADR-0004: exit 1 so the restart policy re-registers
    with a fresh identity. Returns a RunnerIdentity, or None on 401.
    """

    def interruptible_sleep(seconds):
        if shutdown.wait(seconds):
            raise SystemExit(0)

    from runner.config import RUNNER_NAME
    try:
        return register_with_backoff(
            client,
            _registration_token(),
            RUNNER_NAME,
            sleep=interruptible_sleep,
        )
    except BackendAuthError:
        logger.error(
            'registration token rejected (401); exiting for restart policy '
            'to re-register')
        return None


def _registration_token():
    # Read at call time (not import) so tests can monkeypatch runner.config.
    from runner import config
    return config.RUNNER_REGISTRATION_TOKEN


def install_signal_handlers(shutdown):
    """Route SIGTERM (docker stop) and SIGINT into the graceful drain.

    The handler only sets ``shutdown``; the drain itself runs on the main
    thread once ``shutdown.wait()`` returns, so no drain work happens in
    signal context.
    """

    def _on_signal(signum, _frame):
        logger.info('received signal %s; shutting down', signum)
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)


def drain(poller,
          dispatcher,
          sender,
          heartbeat,
          result_queue,
          tracker,
          sleep=time.sleep,
          timeout_sec=DRAIN_TIMEOUT_SEC,
          monotonic=time.monotonic):
    """SIGTERM graceful drain (spec §10). Returns 0.

    The ordering here is the whole point of the slice:
    1. Latch the dispatcher FIRST, before touching anything else. The
       invariant is "once a drain starts, no job begins judging", so the latch
       has to precede every other step -- including stopping the poller. The
       latch is also what makes the single scan below COMPLETE: handle()
       checks it inside the same critical section that publishes a job, so
       every job that ever published is visible to the scan, and any dispatch
       after the latch is refused and returned by the poller itself as a
       'drain' abort. Quiescence comes from the latch, not from guessing
       whether the poller might still be running -- there is deliberately no
       rescan and no is_alive() heuristic here.
    2. Stop and JOIN the poller, capped at its share of the budget. The join
       is an optimization, not a correctness step: a poller that exits now
       makes the reporting tail below trivial, but a stuck one can no longer
       publish anything the scan would miss.
    3. Scan, and abort everything unstarted with reason 'drain' (the backend
       does not count those against a submission's attempts).
    4. Wait for in-flight (already-started) jobs to finish; they push
       CompleteRequests. The dispatcher has to stay running for this: a job
       whose first case is in flight still needs its remaining cases dispatched
       or it never completes at all. This wait keeps the reporting reserve.
    5. Stop the dispatcher. Its join is bounded away from the reserve too:
       the reserve exists for the sender, so no earlier step may consume it.
    6. Reporting tail: keep the sender alive until the poller thread has
       exited AND every claimed job's outcome has been finalized (the tracker
       is empty), or the budget runs out. A poller stuck in prep I/O can
       still wake up and queue a late 'drain' abort; stopping the sender any
       earlier would throw that report away, so a stuck poller holds the
       sender open to the deadline. In-flight jobs the wait above gave up on
       also stay in the tracker -- if one finishes during this tail, its
       result still goes out instead of being lost to lease expiry.
    7. Stop the sender -- it exits only once the queue is empty, so every
       queued complete and drain abort is actually reported first. (The
       tracker can go empty a moment before the last abort's HTTP send
       finishes -- the sender finalizes local state first -- and the join
       here covers that last send within what is left of the budget.)
    8. Stop the heartbeat LAST: complete/abort need a live lease until sent,
       so the heartbeat must outlive the sender.

    Every wait shares one budget (``timeout_sec``). Draining is best-effort:
    prep and result I/O can be bounded per request but never in total, so
    waiting forever would just hand the decision to docker's SIGKILL and throw
    away the results already collected. Anything that misses the budget is left
    to lease expiry and backend reclaim.

    The budget is not first-come-first-served, because the steps are not
    equally worth waiting for. Reporting a finished judge run matters more than
    waiting out a poller stuck in prep, so the poller gets a capped share and
    the reporting tail keeps a reserve.
    """
    deadline = monotonic() + timeout_sec
    reserve = timeout_sec * REPORTING_RESERVE_SHARE

    def remaining(reserve=0.0):
        return max(0.0, deadline - monotonic() - reserve)

    def join_within(component, name, limit=None, reserve=0.0):
        budget = remaining(reserve)
        if limit is not None:
            budget = min(budget, limit)
        component.join(timeout=budget)
        if component.is_alive():
            logger.error(
                'drain: %s outlived the budget; its work is left to lease '
                'expiry and backend reclaim', name)

    # Before anything else, including stopping the poller: see step 1.
    dispatcher.stop_accepting()
    poller.stop()
    join_within(poller, 'poller', limit=timeout_sec * POLLER_JOIN_SHARE)

    # The single scan (step 3): complete by construction, see the docstring.
    for job_id, submission_id in dispatcher.drain_unstarted():
        result_queue.put(AbortRequest(job_id, submission_id, 'drain'))

    waited = 0
    while dispatcher.has_jobs():
        if not remaining(reserve=reserve):
            logger.error('drain: budget exhausted with jobs still in flight')
            break
        sleep(0.5)
        waited += 1
        if waited % 20 == 0:
            logger.info('drain: waiting for in-flight jobs to finish')

    dispatcher.stop()
    join_within(dispatcher, 'dispatcher', reserve=reserve)

    # The reporting tail (step 6).
    waited = 0
    while remaining() and (poller.is_alive() or len(tracker)):
        sleep(0.5)
        waited += 1
        if waited % 20 == 0:
            logger.info(
                'drain: reporting tail waiting (poller alive: %s, '
                'unreported jobs: %d)', poller.is_alive(), len(tracker))

    sender.stop()
    join_within(sender, 'result sender')
    heartbeat.stop()
    join_within(heartbeat, 'heartbeat')
    return 0


def main():
    from dispatcher.dispatcher import Dispatcher
    from runner.config import BACKEND_URL

    shutdown = threading.Event()
    fatal = threading.Event()

    install_signal_handlers(shutdown)

    if not _registration_token():
        logger.error(
            'RUNNER_REGISTRATION_TOKEN is empty; cannot register. Set it in '
            'the runner environment (ADR-0005).')
        return 1

    client = BackendClient(BACKEND_URL)
    identity = register(client, shutdown)
    if identity is None:
        return 1

    result_queue = queue.Queue()
    tracker = ActiveJobTracker()

    dispatcher = Dispatcher(
        os.getenv('DISPATCHER_CONFIG', '.config/dispatcher.json.example'),
        on_complete=lambda job_id, submission_id, tasks: result_queue.put(
            CompleteRequest(job_id, submission_id, tasks)),
    )
    sender = ResultSenderThread(client, identity, tracker, result_queue)
    poller = PollerThread(
        client,
        identity,
        tracker,
        result_queue,
        dispatch=dispatcher.handle,
    )
    heartbeat = HeartbeatThread(
        client,
        identity,
        tracker,
        on_fatal=lambda: (fatal.set(), shutdown.set()),
    )

    dispatcher.start()
    sender.start()
    poller.start()
    heartbeat.start()
    logger.info('runner %s up', identity.runner_id)

    shutdown.wait()

    if fatal.is_set():
        # 401 fail-fast: no drain. Our identity has evaporated, so complete /
        # abort would 401 anyway; compose restart re-registers a fresh one.
        logger.error('heartbeat fail-fast; exiting without drain')
        return 1

    return drain(poller, dispatcher, sender, heartbeat, result_queue, tracker)


if __name__ == '__main__':
    setup_logging()
    sys.exit(main())
