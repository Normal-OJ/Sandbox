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

    def run(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                self.client.heartbeat(runner_id=self.runner_id)
            except BackendClient.TransientError as e:
                log.warning(f"heartbeat failed (transient): {e}")
            except BackendClient.AuthError as e:
                # Auth fail means the backend forgot us (e.g., Redis loss).
                # Caller will need to re-register; for now just log.
                log.error(f"heartbeat auth failed: {e}")
            except Exception as e:  # defensive — never let thread die
                log.exception(f"heartbeat unexpected error: {e}")
            # Wait, but break early on shutdown
            self.shutdown_event.wait(timeout=self.interval_sec)
