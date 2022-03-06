import os
import json
import zipfile
import pathlib
import logging
import shutil
import requests
import queue
import secrets
from datetime import datetime
from flask import Flask, request, jsonify
from dispatcher.dispatcher import Dispatcher
from dispatcher.meta import Meta
from pydantic import ValidationError

logging.basicConfig(filename='logs/sandbox.log')
app = Flask(__name__)
if __name__ != '__main__':
    # let flask app use gunicorn's logger
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    logging.getLogger().setLevel(gunicorn_logger.level)
logger = app.logger

# data storage
SUBMISSION_DIR = pathlib.Path(os.getenv(
    'SUBMISSION_DIR',
    'submissions',
))
SUBMISSION_BACKUP_DIR = pathlib.Path(
    os.getenv(
        'SUBMISSION_BACKUP_DIR',
        'submissions.bk',
    ))
TMP_DIR = pathlib.Path(os.getenv(
    'TMP_DIR',
    '/tmp' / SUBMISSION_DIR,
))
# check
if SUBMISSION_DIR == SUBMISSION_BACKUP_DIR:
    logger.error('use the same dir for submission and backup!')
# create directory
SUBMISSION_DIR.mkdir(exist_ok=True)
SUBMISSION_BACKUP_DIR.mkdir(exist_ok=True)
TMP_DIR.mkdir(exist_ok=True)
# setup dispatcher
DISPATCHER_CONFIG = os.getenv(
    'DISPATCHER_CONFIG',
    '.config/dispatcher.json.example',
)
DISPATCHER = Dispatcher(DISPATCHER_CONFIG)
DISPATCHER.start()
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


@app.route('/submit/<submission_id>', methods=['POST'])
def submit(submission_id):
    token = request.values['token']
    if not secrets.compare_digest(token, SANDBOX_TOKEN):
        app.logger.debug(f'get invalid token: {token}')
        return 'invalid token', 403
    # make submission directory
    submission_dir = SUBMISSION_DIR / submission_id
    submission_dir.mkdir()
    # process meta
    meta = request.files['meta.json']
    meta.save(submission_dir / 'meta.json')
    try:
        meta = Meta.parse_obj(json.load(open(submission_dir / 'meta.json')))
    except ValidationError as e:
        app.logger.debug(f'Invalid meta [err={e.json()}]')
        return 'Invalid meta value', 400
    app.logger.debug(f'{submission_id}\'s meta: {meta}')
    # check format
    for i, task in enumerate(meta.tasks):
        if task.caseCount == 0:
            logger.warning(f'empty task. [id={submission_id}/{i:02d}]')
    # 0: C, 1: C++, 2: python3
    language_id = int(meta.language)
    language_type = ['.c', '.cpp', '.py'][language_id]
    # extract source code
    code = request.files['src']
    code_dir = submission_dir / 'src'
    code_dir.mkdir()
    with zipfile.ZipFile(code) as zf:
        zf.extractall(code_dir)
    # extract testcase zip
    testcase = request.files['testcase']
    testcase_dir = submission_dir / 'testcase'
    testcase_dir.mkdir()
    with zipfile.ZipFile(testcase) as f:
        f.extractall(testcase_dir)
    # check source code
    if len([*code_dir.iterdir()]) == 0:
        return 'no file in \'src\' directory', 400
    else:
        for _file in code_dir.iterdir():
            if _file.stem != 'main':
                return 'none main', 400
            if _file.suffix != language_type:
                return 'data type is not match', 400
    # move chaos files to src directory
    chaos_dir = testcase_dir / 'chaos'
    if chaos_dir.exists():
        if chaos_dir.is_file():
            return '\'chaos\' can not be a file', 400
        for chaos_file in chaos_dir.iterdir():
            shutil.move(str(chaos_file), str(code_dir))
    logger.debug(f'send submission {submission_id} to dispatcher')
    try:
        DISPATCHER.handle(submission_id)
    except queue.Full:
        return jsonify({
            'status': 'err',
            'msg': 'task queue is full now.\n'
            'please wait a moment and re-send the submission.',
            'data': None,
        }), 500
    return jsonify({
        'status': 'ok',
        'msg': 'ok',
        'data': 'ok',
    })


@app.route('/status', methods=['GET'])
def status():
    ret = {
        'load': DISPATCHER.queue.qsize() / DISPATCHER.MAX_TASK_COUNT,
    }
    # if token is provided
    if secrets.compare_digest(SANDBOX_TOKEN, request.args.get('token', '')):
        ret.update({
            'queueSize': DISPATCHER.queue.qsize(),
            'maxTaskCount': DISPATCHER.MAX_TASK_COUNT,
            'containerCount': DISPATCHER.container_count,
            'maxContainerCount': DISPATCHER.MAX_TASK_COUNT,
            'submissions': [*DISPATCHER.result.keys()],
            'running': DISPATCHER.do_run,
        })
    return jsonify(ret), 200


def clean_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    shutil.rmtree(submission_dir)


def backup_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    dest = SUBMISSION_BACKUP_DIR / f'{submission_id}_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}'
    shutil.move(submission_dir, dest)


@app.route('/result/<submission_id>', methods=['POST'])
def recieve_result(submission_id):
    post_data = request.get_json()
    post_data['token'] = SANDBOX_TOKEN
    logger.info(f'send {submission_id} to BE server')
    resp = requests.put(
        f'{BACKEND_API}/submission/{submission_id}/complete',
        json=post_data,
    )
    logger.debug(f'get BE response: [{resp.status_code}] {resp.text}', )
    # clear
    if resp.status_code == 200:
        clean_data(submission_id)
    # copy to another place
    else:
        backup_data(submission_id)
    return 'data sent to BE server', 200
