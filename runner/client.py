import logging
from dataclasses import dataclass, field

import requests

from .config import (
    REQUEST_TIMEOUT,
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    DEFAULT_POLL_INTERVAL_SEC,
    DEFAULT_MAX_CONCURRENT_JOBS,
)

logger = logging.getLogger(__name__)


class BackendAPIError(Exception):
    """A backend runner API call returned an unexpected non-2xx status."""

    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


class BackendAuthError(BackendAPIError):
    """The backend rejected our credentials (HTTP 401)."""


@dataclass
class RunnerConfig:
    heartbeat_interval_sec: int
    poll_interval_sec: int
    max_concurrent_jobs: int

    @classmethod
    def from_dict(cls, data):
        data = data or {}
        return cls(
            heartbeat_interval_sec=data.get(
                'heartbeat_interval_sec',
                DEFAULT_HEARTBEAT_INTERVAL_SEC,
            ),
            poll_interval_sec=data.get(
                'poll_interval_sec',
                DEFAULT_POLL_INTERVAL_SEC,
            ),
            max_concurrent_jobs=data.get(
                'max_concurrent_jobs',
                DEFAULT_MAX_CONCURRENT_JOBS,
            ),
        )


@dataclass
class RunnerIdentity:
    runner_id: str
    # Memory-only credential (ADR-0004); never leak it via repr.
    token: str = field(repr=False)
    config: RunnerConfig


class BackendClient:
    """HTTP client for the backend runner API (spec §7)."""

    def __init__(self, base_url, session=None, timeout=REQUEST_TIMEOUT):
        self.base_url = base_url.rstrip('/')
        self.session = session if session is not None else requests.Session()
        self.timeout = timeout

    def register(self, registration_token, name):
        """POST /runners/register -> RunnerIdentity.

        201 -> parsed identity; 401 -> BackendAuthError; other non-2xx ->
        BackendAPIError. Network errors propagate as requests.RequestException.
        """
        resp = self.session.post(
            f'{self.base_url}/runners/register',
            json={
                'registration_token': registration_token,
                'name': name,
            },
            timeout=self.timeout,
        )
        if resp.status_code == 201:
            body = resp.json()
            return RunnerIdentity(
                runner_id=body['runner_id'],
                token=body['token'],
                config=RunnerConfig.from_dict(body.get('config')),
            )
        self._raise_for_status(resp, 'register')

    def heartbeat(self, identity, active_job_ids):
        """POST /runners/<rn>/heartbeat. Expect 204."""
        resp = self.session.post(
            f'{self.base_url}/runners/{identity.runner_id}/heartbeat',
            json={'active_job_ids': list(active_job_ids)},
            headers={'Authorization': f'Bearer {identity.token}'},
            timeout=self.timeout,
        )
        if resp.status_code == 204:
            return
        self._raise_for_status(resp, 'heartbeat')

    @staticmethod
    def _raise_for_status(resp, action):
        # Never include tokens in the message: only the status code is logged.
        status = resp.status_code
        if status == 401:
            raise BackendAuthError(
                f'runner {action} rejected with 401',
                status_code=status,
            )
        raise BackendAPIError(
            f'runner {action} failed with status {status}',
            status_code=status,
        )
