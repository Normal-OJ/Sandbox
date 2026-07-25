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
          sleep=time.sleep,
          timeout_sec=DRAIN_TIMEOUT_SEC,
          monotonic=time.monotonic):
    """SIGTERM graceful drain (spec §10). Returns 0.

    The ordering here is the whole point of the slice:
    1. Latch the dispatcher FIRST, before touching anything else. The
       invariant is "once a drain starts, no job begins judging", so the latch
       has to precede every other step -- including stopping the poller. Doing
       it even one statement later leaves a window where the poller finishes a
       prep, dispatches, and the run loop starts that job while the dispatcher
       is still accepting. A job claimed in that same window is not lost: it
       cannot start, and the scan below returns it.
    2. Stop and JOIN the poller BEFORE scanning: a claim in flight mid
       iteration must land in the dispatcher before we scan, or it would be
       dispatched afterwards and never aborted.
    3. Scan, and abort everything unstarted with reason 'drain' (the backend
       does not count those against a submission's attempts).
    4. Wait for in-flight (already-started) jobs to finish; they push
       CompleteRequests.
    5. Stop the dispatcher.
    6. Stop the sender -- it exits only once the queue is empty, so every
       queued complete and drain abort is actually reported first.
    7. Stop the heartbeat LAST: complete/abort need a live lease until sent,
       so the heartbeat must outlive the sender.

    Every wait shares one budget (``timeout_sec``). Draining is best-effort:
    prep and result I/O can be bounded per request but never in total, so
    waiting forever would just hand the decision to docker's SIGKILL and throw
    away the results already collected. Anything that misses the budget is left
    to lease expiry and backend reclaim.
    """
    deadline = monotonic() + timeout_sec

    def remaining():
        return max(0.0, deadline - monotonic())

    def join_within(component, name):
        component.join(timeout=remaining())
        if component.is_alive():
            logger.error(
                'drain: %s outlived the budget; its work is left to lease '
                'expiry and backend reclaim', name)

    # Before anything else, including stopping the poller: see step 1.
    dispatcher.stop_accepting()
    poller.stop()
    join_within(poller, 'poller')

    for job_id, submission_id in dispatcher.drain_unstarted():
        result_queue.put(AbortRequest(job_id, submission_id, 'drain'))

    waited = 0
    while dispatcher.has_jobs():
        if not remaining():
            logger.error('drain: budget exhausted with jobs still in flight')
            break
        sleep(0.5)
        waited += 1
        if waited % 20 == 0:
            logger.info('drain: waiting for in-flight jobs to finish')

    dispatcher.stop()
    join_within(dispatcher, 'dispatcher')
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

    return drain(poller, dispatcher, sender, heartbeat, result_queue)


if __name__ == '__main__':
    setup_logging()
    sys.exit(main())
