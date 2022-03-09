import os
from pathlib import Path

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
