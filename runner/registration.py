import logging
import time

import requests

from .client import BackendAPIError, BackendAuthError
from .config import REGISTRATION_BACKOFF_SCHEDULE

logger = logging.getLogger(__name__)


def register_with_backoff(
    client,
    registration_token,
    name,
    *,
    schedule=REGISTRATION_BACKOFF_SCHEDULE,
    sleep=time.sleep,
):
    """Register with the backend, retrying transient failures indefinitely.

    Backoff follows ``schedule`` (1->2->4->8->16->30s) and then stays at the
    final value forever. A 401 (BackendAuthError) is fatal per ADR-0004: it is
    re-raised immediately so the caller can fail-fast. ``sleep`` is injectable
    for tests.
    """
    attempt = 0
    while True:
        try:
            return client.register(registration_token, name)
        except BackendAuthError:
            # Bad registration token: no amount of retrying will help.
            raise
        except (BackendAPIError, requests.RequestException) as err:
            delay = schedule[min(attempt, len(schedule) - 1)]
            logger.warning(
                'runner registration failed (attempt %d): %s; retrying in %ds',
                attempt + 1,
                err,
                delay,
            )
            attempt += 1
            sleep(delay)
