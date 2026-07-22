import os
import socket

# Backend base URL for the pull-based runner API. Coexists with the legacy
# BACKEND_API (dispatcher/config.py) until the keystone slice removes the
# old push path.
BACKEND_URL = os.getenv(
    'BACKEND_URL',
    'http://web:8080',
)
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
