import io
import json
import secrets
import shutil
import hashlib
from pathlib import Path
from zipfile import ZipFile
import requests as rq

from .constant import Language
from .meta import Meta
from .utils import (
    get_redis_client,
    logger,
)
from .config import (
    BACKEND_API,
    SANDBOX_TOKEN,
    TESTDATA_ROOT,
)

META_DIR = TESTDATA_ROOT / 'meta'
META_DIR.mkdir(exist_ok=True)


def calc_checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def handle_problem_response(resp: rq.Response):
    if resp.status_code == 404:
        raise ValueError('Problem not found')
    if resp.status_code == 401:
        raise PermissionError()
    if not resp.ok:
        logger().error(f'Error during get problem data [resp={resp.text}]')
        raise RuntimeError()


# TODO: Schema validation
def fetch_problem_meta(problem_id: int) -> str:
    resp = rq.get(
        f'{BACKEND_API}/problem/{problem_id}/meta',
        params={
            'token': SANDBOX_TOKEN,
        },
    )
    handle_problem_response(resp)
    content = json.dumps(resp.json()['data'])
    (META_DIR / f'{problem_id}.json').write_text(content)
    return content


def get_problem_meta(problem_id: int, language: Language) -> Meta:
    meta_path = META_DIR / f'{problem_id}.json'
    if not meta_path.exists():
        fetch_problem_meta(problem_id)
    obj = json.load(meta_path.open())
    obj['language'] = int(language)
    return Meta.parse_obj(obj)


def get_problem_root(problem_id: int) -> Path:
    return TESTDATA_ROOT / str(problem_id)


# TODO: Delete unused testdata
def fetch_testdata(problem_id: int):
    '''
    Fetch testdata from backend server
    '''
    resp = rq.get(
        f'{BACKEND_API}/problem/{problem_id}/testdata',
        params={
            'token': SANDBOX_TOKEN,
        },
    )
    handle_problem_response(resp)
    return resp.content


def get_checksum(problem_id: int) -> str:
    resp = rq.get(
        f'{BACKEND_API}/problem/{problem_id}/checksum',
        params={
            'token': SANDBOX_TOKEN,
        },
    )
    handle_problem_response(resp)
    return resp.json()['data']


def ensure_testdata(problem_id: int):
    '''
    Ensure the testdata of problem is up to date
    '''
    client = get_redis_client()
    key = f'problem-{problem_id}-checksum'
    curr_checksum = client.get(key)
    if curr_checksum is not None:
        curr_checksum = curr_checksum.decode()
        checksum = get_checksum(problem_id)
        # No need to update
        if secrets.compare_digest(curr_checksum, checksum):
            return
    testdata = fetch_testdata(problem_id)
    problem_root = TESTDATA_ROOT / str(problem_id)
    if problem_root.exists():
        shutil.rmtree(problem_root)
    with ZipFile(io.BytesIO(testdata)) as zf:
        zf.extractall(problem_root)
    meta = fetch_problem_meta(problem_id)
    checksum = calc_checksum(testdata + meta.encode())
    client.setex(key, 600, checksum)
