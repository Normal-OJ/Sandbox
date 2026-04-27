"""Self-registration on startup."""
from dataclasses import dataclass

from .client import BackendClient


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
