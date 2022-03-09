import os
from pathlib import Path
from .utils import logger

# backend config
BACKEND_API = os.getenv(
    'BACKEND_API',
    'http://web:8080',
)
# sandbox token
SANDBOX_TOKEN = os.getenv(
    'SANDBOX_TOKEN',
    'KoNoSandboxDa',
)
TESTDATA_ROOT = Path(os.getenv(
    'TESTDATA_ROOT',
    '/sandbox-testdata',
))
TESTDATA_ROOT.mkdir(exist_ok=True)

# data storage
SUBMISSION_DIR = Path(os.getenv(
    'SUBMISSION_DIR',
    'submissions',
))
SUBMISSION_BACKUP_DIR = Path(
    os.getenv(
        'SUBMISSION_BACKUP_DIR',
        'submissions.bk',
    ))
# check
if SUBMISSION_DIR == SUBMISSION_BACKUP_DIR:
    logger().error('use the same dir for submission and backup!')
# create directory
SUBMISSION_DIR.mkdir(exist_ok=True)
SUBMISSION_BACKUP_DIR.mkdir(exist_ok=True)
