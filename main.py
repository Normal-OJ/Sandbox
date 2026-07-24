"""Pull-based runner entrypoint (spec §10).

Wires the coordination threads together: the dispatcher runs judge work, the
poller claims jobs, the heartbeat renews leases, and the result sender reports
outcomes. There is no Flask and no inbound HTTP server anymore -- the runner
only talks *out* to the backend runner API.

Import-safe: nothing here touches the filesystem, network, or logging config
at import time, so tests can import main and exercise the helpers directly.
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


def drain(poller,
          dispatcher,
          sender,
          heartbeat,
          result_queue,
          sleep=time.sleep):
    """SIGTERM graceful drain (spec §10). Returns 0.

    The ordering here is the whole point of the slice:
    1. Stop and JOIN the poller BEFORE scanning the dispatcher: a claim in
       flight mid-iteration must land in the dispatcher before we scan, or it
       would be dispatched after the scan and never aborted.
    2. Abort every not-yet-started job with reason 'drain' (the backend does
       not count drain aborts against a submission's attempts).
    3. Wait for in-flight (already-started) jobs to finish; they push
       CompleteRequests. Per-case executor timeouts bound this wait; the
       docker stop grace period is the backstop.
    4. Stop the dispatcher.
    5. Stop the sender -- it exits only once the queue is empty, so every
       queued complete and drain abort is actually reported first.
    6. Stop the heartbeat LAST: complete/abort need a live lease until sent,
       so the heartbeat must outlive the sender.
    """
    poller.stop()
    poller.join()

    for job_id, submission_id in dispatcher.drain_unstarted():
        result_queue.put(AbortRequest(job_id, submission_id, 'drain'))

    waited = 0
    while dispatcher.has_jobs():
        sleep(0.5)
        waited += 1
        if waited % 20 == 0:
            logger.info('drain: waiting for in-flight jobs to finish')

    dispatcher.stop()
    dispatcher.join()
    sender.stop()
    sender.join()
    heartbeat.stop()
    heartbeat.join()
    return 0


def main():
    from dispatcher.dispatcher import Dispatcher
    from runner.config import BACKEND_URL

    shutdown = threading.Event()
    fatal = threading.Event()

    def _on_signal(signum, _frame):
        logger.info('received signal %s; shutting down', signum)
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

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
