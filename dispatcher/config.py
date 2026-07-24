import os
from pathlib import Path

# backend config
BACKEND_URL = os.getenv(
    'BACKEND_URL',
    'http://web:8080',
)
# Shared secret that now ONLY authenticates the legacy testdata channel
# (backend /problem/<id>/meta|testdata|checksum ?token= query). Its retirement
# is coordinated with the backend keystone re-authing those endpoints.
SANDBOX_TOKEN = os.getenv(
    'SANDBOX_TOKEN',
    'KoNoSandboxDa',
)
TESTDATA_ROOT = Path(os.getenv(
    'TESTDATA_ROOT',
    'sandbox-testdata',
))
TESTDATA_ROOT.mkdir(exist_ok=True)
SUBMISSION_DIR = Path(os.getenv(
    'SUBMISSION_DIR',
    'submissions',
))
SUBMISSION_BACKUP_DIR = Path(
    os.getenv(
        'SUBMISSION_BACKUP_DIR',
        'submissions.bk',
    ))
# create directory
SUBMISSION_DIR.mkdir(exist_ok=True)
SUBMISSION_BACKUP_DIR.mkdir(exist_ok=True)