import os
import json
import zipfile
import glob
import pathlib
import logging
import shutil
import requests
import queue
import secrets

from flask import Flask, request, jsonify
from os import walk
from dispatcher.dispatcher import Dispatcher

logging.basicConfig(
    filename='sandbox.log',
    level=logging.DEBUG,
)

app = Flask(__name__)
if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
logger = app.logger

# setup constant

# data storage
SUBMISSION_DIR = pathlib.Path(os.environ.get(
    'SUBMISSION_DIR',
    'submissions',
))
TMP_DIR = pathlib.Path(os.environ.get(
    'TMP_DIR',
    '/tmp/submissions',
))

# create directory
SUBMISSION_DIR.mkdir(exist_ok=True)
TMP_DIR.mkdir(exist_ok=True)

# setup dispatcher
DISPATCHER_CONFIG = os.environ.get(
    'DISPATCHER_CONFIG',
    '.config/dispatcher.json.example',
)
DISPATCHER = Dispatcher(DISPATCHER_CONFIG)
DISPATCHER.start()

# backend config
BACKEND_PORT = os.environ.get(
    'BACKEND_PORT',
    8080,
)
BACKEND_API = os.environ.get(
    'BACKEND_API',
    f'http://web:{BACKEND_PORT}',
)

SANDBOX_TOKEN = os.getenv(
    'SANDBOX_TOKEN',
    'KoNoSandboxDa',
)


@app.route('/submit/<submission_id>', methods=['POST'])
def submit(submission_id):
    token = request.values['token']
    if not secrets.compare_digest(token, SANDBOX_TOKEN):
        return 'invalid token', 403

    checker = request.values['checker']

    submission_dir = SUBMISSION_DIR / submission_id
    submission_dir.mkdir()

    # meta
    meta = request.files['meta.json']
    meta.save(submission_dir / 'meta.json')
    meta = json.load(open(submission_dir / 'meta.json'))
    # check format
    if 'tasks' not in meta:
        return 'no task in meta', 400
    tasks = meta['tasks']
    if len(tasks) == 0:
        return 'empty tasks meta', 400
    for i, task in enumerate(tasks):
        ks = [
            'taskScore',
            'memoryLimit',
            'timeLimit',
            'caseCount',
        ]
        for k in ks:
            if k not in task or type(task[k]) != int:
                return 'wrong meta.json schema', 400
        if task['caseCount'] == 0:
            logger.warning(f'no case in task: {submission_id}/{i:02d}')

    # 0:C, 1:C++, 2:python3
    languages = ['.c', '.cpp', '.py']
    try:
        language_id = meta['language']
        language_type = languages[language_id]
    except (ValueError, IndexError):
        return 'invalid language id', 400
    except KeyError:
        return 'no language specified', 400

    zip_dir = TMP_DIR / submission_id
    zip_dir.mkdir(exist_ok=True)

    code = request.files['src']  # get file
    code_dir = submission_dir / 'src'
    code_dir.mkdir()
    with zipfile.ZipFile(code, 'r') as zf:
        zf.extractall(str(code_dir))

    # extract testcase zip
    testcase = request.files['testcase']
    testcase_dir = submission_dir / 'testcase'
    testcase_dir.mkdir()
    with zipfile.ZipFile(testcase, 'r') as f:
        f.extractall(str(testcase_dir))

    # check source code
    if len([*code_dir.iterdir()]) == 0:
        return 'under src does not have any file', 400
    else:
        for _file in code_dir.iterdir():
            if _file.stem != 'main':
                return 'none main', 400
            if _file.suffix != language_type:
                return 'data type is not match', 400

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


def clean_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    shutil.rmtree(submission_dir)


@app.route('/status', methods=['GET'])
def status():
    ret = {
        'load': DISPATCHER.queue.qsize() / DISPATCHER.MAX_TASK_COUNT,
    }
    return jsonify(ret), 200


@app.route('/result/<submission_id>', methods=['POST'])
def recieve_result(submission_id):
    post_data = request.get_json()
    post_data['token'] = SANDBOX_TOKEN

    logger.info(f'send {submission_id} to BE server')
    logger.debug(f'send json: f{post_data}')

    resp = requests.put(
        f'{BACKEND_API}/submission/{submission_id}/complete',
        json=post_data,
    )
    logger.debug(f'resp: {resp.text}')

    # clear
    if resp.status_code == 200:
        clean_data(submission_id)

    return 'data sent to BE server', 200
