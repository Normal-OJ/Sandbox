import logging
import threading
import time

import requests

from .client import BackendAPIError, BackendAuthError

logger = logging.getLogger(__name__)

# Consecutive 401s that trigger fail-fast (spec §10).
_FATAL_AUTH_FAILURES = 2


class HeartbeatThread(threading.Thread):
    """Periodically POST /runners/<rn>/heartbeat to renew leases.

    Sends the first beat immediately, then every ``interval_sec``. Two
    consecutive 401s trigger ``on_fatal`` (the future main.py wires this to
    process exit) and stop the loop. Any non-401 outcome resets the counter.
    """

    def __init__(self, client, identity, tracker, on_fatal, interval_sec=None):
        super().__init__(daemon=True)
        self._client = client
        self._identity = identity
        self._tracker = tracker
        self._on_fatal = on_fatal
        self._interval_sec = (interval_sec if interval_sec is not None else
                              identity.config.heartbeat_interval_sec)
        self._stop_event = threading.Event()
        self._consecutive_auth_failures = 0

    def run(self):
        while not self._stop_event.is_set():
            started = time.monotonic()
            fatal = self._beat()
            if fatal:
                break
            # Fixed cadence: subtract the time the beat took so a slow or
            # timed-out request cannot push the next beat past the lease
            # budget (15s interval vs 30s TTL allows exactly one miss).
            # Interruptible wait so stop() takes effect immediately.
            elapsed = time.monotonic() - started
            self._stop_event.wait(max(0.0, self._interval_sec - elapsed))

    def _beat(self):
        """Send one heartbeat. Returns True iff fail-fast was triggered."""
        try:
            self._client.heartbeat(self._identity, self._tracker.snapshot())
        except BackendAuthError:
            self._consecutive_auth_failures += 1
            if self._consecutive_auth_failures >= _FATAL_AUTH_FAILURES:
                logger.error(
                    'heartbeat got %d consecutive 401s; failing fast',
                    self._consecutive_auth_failures,
                )
                self._on_fatal()
                return True
            logger.warning('heartbeat got 401 (%d consecutive)',
                           self._consecutive_auth_failures)
            return False
        except (BackendAPIError, requests.RequestException) as err:
            self._consecutive_auth_failures = 0
            logger.warning('heartbeat failed: %s', err)
            return False
        self._consecutive_auth_failures = 0
        return False

    def stop(self):
        self._stop_event.set()
