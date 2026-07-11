"""Heartbeat daemon thread: refreshes runner alive TTL on backend."""
import logging
import threading

from .client import BackendClient

log = logging.getLogger(__name__)


class HeartbeatThread(threading.Thread):
    """Periodically POSTs heartbeat. Tolerates transient errors silently."""

    def __init__(
        self,
        client: BackendClient,
        runner_id: str,
        interval_sec: float,
        shutdown_event: threading.Event,
    ):
        super().__init__(daemon=True, name="heartbeat")
        self.client = client
        self.runner_id = runner_id
        self.interval_sec = interval_sec
        self.shutdown_event = shutdown_event
        self._auth_failed_once = False

    def run(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                self.client.heartbeat(runner_id=self.runner_id)
            except BackendClient.TransientError as e:
                log.warning(f"heartbeat failed (transient): {e}")
            except BackendClient.AuthError as e:
                log.error(f"heartbeat auth failed: {e}")
                if self._auth_failed_once:
                    log.error("second consecutive auth failure; shutting down")
                    self.shutdown_event.set()
                    break
                self._auth_failed_once = True
            except Exception as e:  # defensive — never let thread die
                log.exception(f"heartbeat unexpected error: {e}")
            else:
                self._auth_failed_once = False
            # Wait, but break early on shutdown
            self.shutdown_event.wait(timeout=self.interval_sec)
