import os
import socket

# Single source of truth for the backend base URL, shared with the legacy
# testdata channel. runner/ already imports from dispatcher/ so this reuse
# introduces no layering issue or import cycle.
from dispatcher.config import BACKEND_URL
# Shared secret presented to POST /runners/register.
RUNNER_REGISTRATION_TOKEN = os.getenv(
    'RUNNER_REGISTRATION_TOKEN',
    '',
)
# Stable, human-readable name for logs / admin display.
RUNNER_NAME = os.getenv('RUNNER_NAME') or socket.gethostname()

# Registration retry backoff (seconds). After the schedule is exhausted the
# caller stays at the final value (30s) forever.
REGISTRATION_BACKOFF_SCHEDULE = (1, 2, 4, 8, 16, 30)

# HTTP timeout (seconds) applied to every backend client call.
REQUEST_TIMEOUT = 10

# Fallback runner config values used when the register response omits a key.
DEFAULT_HEARTBEAT_INTERVAL_SEC = 15
DEFAULT_POLL_INTERVAL_SEC = 3
DEFAULT_MAX_CONCURRENT_JOBS = 8

# Local prep attempts before giving up and aborting with prep_failed (spec §10).
PREP_MAX_ATTEMPTS = 3
# Backoff (seconds) between prep attempts; len == PREP_MAX_ATTEMPTS - 1.
PREP_BACKOFF_SCHEDULE = (1, 2)
# Backoff (seconds) between complete/abort resends; len == max retries (spec §7).
SEND_RETRY_BACKOFF_SCHEDULE = (1, 2, 4, 8, 16)
# Total budget (seconds) for the SIGTERM drain. Prep and result I/O can only
# be bounded per request, never in total, so the drain caps itself instead of
# letting docker's SIGKILL decide -- leaving headroom under the
# `docker stop --time=600` the deployment uses (spec §10).
DRAIN_TIMEOUT_SEC = 540
