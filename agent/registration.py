"""Self-registration on startup."""
import logging
import time
from dataclasses import dataclass

from .client import BackendClient

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunnerCredentials:
    runner_id: str
    token: str
    heartbeat_interval_sec: int
    poll_interval_sec: int
    max_concurrent_jobs: int


def register_runner(
    client: BackendClient,
    name: str,
    registration_token: str,
) -> RunnerCredentials:
    """Call backend's register endpoint and return RunnerCredentials.

    Raises BackendClient.AuthError or TransientError on failure.
    """
    rv = client.register(name=name, registration_token=registration_token)
    cfg = rv.get("config", {})
    return RunnerCredentials(
        runner_id=rv["runner_id"],
        token=rv["token"],
        heartbeat_interval_sec=cfg.get("heartbeat_interval_sec", 15),
        poll_interval_sec=cfg.get("poll_interval_sec", 3),
        max_concurrent_jobs=cfg.get("max_concurrent_jobs", 8),
    )


def register_runner_with_retry(
    client: BackendClient,
    name: str,
    registration_token: str,
    initial_backoff_sec: float = 1.0,
    max_backoff_sec: float = 30.0,
) -> RunnerCredentials:
    """Retry transient startup failures until registration succeeds.

    Auth errors are intentionally propagated because retrying the same invalid
    registration token cannot recover.
    """
    backoff = initial_backoff_sec
    while True:
        try:
            return register_runner(
                client=client,
                name=name,
                registration_token=registration_token,
            )
        except BackendClient.TransientError as e:
            log.warning(f"registration failed; retrying in {backoff}s: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff_sec)
