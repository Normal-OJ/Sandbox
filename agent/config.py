"""Environment-driven configuration for the runner agent."""
import os
from pathlib import Path

# Backend URL — where to register, poll, send results
BACKEND_URL: str = os.getenv("BACKEND_URL", "http://web:8080")

# Shared registration secret (must match backend's RUNNER_REGISTRATION_TOKEN).
DEV_REGISTRATION_TOKEN = "dev-only-registration-token-change-me"


def _load_registration_token() -> str:
    token = os.getenv("RUNNER_REGISTRATION_TOKEN", DEV_REGISTRATION_TOKEN)
    require_secure = os.getenv("RUNNER_REQUIRE_SECURE_TOKEN",
                               "").lower() in {"1", "true", "yes", "on"}
    if require_secure and (not token.strip()
                           or token.strip() == DEV_REGISTRATION_TOKEN):
        raise RuntimeError(
            "RUNNER_REGISTRATION_TOKEN must be set to a non-default value")
    return token


RUNNER_REGISTRATION_TOKEN: str = _load_registration_token()

# Display name shown in admin/listing. Defaults to hostname.
RUNNER_NAME: str = os.getenv("RUNNER_NAME", os.uname().nodename)

# Tunings (defaults match what backend returns from register; override here is rarely needed)
HEARTBEAT_INTERVAL_SEC: int = int(os.getenv("HEARTBEAT_INTERVAL_SEC", "15"))
POLL_INTERVAL_SEC: int = int(os.getenv("POLL_INTERVAL_SEC", "3"))

# Result delivery retry policy
RESULT_RETRY_MAX_ATTEMPTS: int = 5
RESULT_RETRY_INITIAL_BACKOFF_SEC: float = 1.0
RESULT_RETRY_MAX_BACKOFF_SEC: float = 16.0

# HTTP timeouts
HTTP_REQUEST_TIMEOUT_SEC: int = 10

# Where to download code zip to (per-job temp dir)
CODE_DOWNLOAD_DIR: Path = Path(
    os.getenv("CODE_DOWNLOAD_DIR", "/tmp/runner-code"))
CODE_DOWNLOAD_DIR.mkdir(exist_ok=True)
