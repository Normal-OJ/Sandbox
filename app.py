import os
import pathlib
import logging
import shutil
import requests
import queue
import secrets
from datetime import datetime
from flask import Flask, request, jsonify
from dispatcher import file_manager
from dispatcher.dispatcher import Dispatcher

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
# check
if SUBMISSION_DIR == SUBMISSION_BACKUP_DIR:
    logger.error('use the same dir for submission and backup!')
# create directory
SUBMISSION_DIR.mkdir(exist_ok=True)
SUBMISSION_BACKUP_DIR.mkdir(exist_ok=True)
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


@app.post('/submit/<submission_id>')
def submit(submission_id):
    token = request.values['token']
    if not secrets.compare_digest(token, SANDBOX_TOKEN):
        logger.debug(f'get invalid token: {token}')
        return 'invalid token', 403
    try:
        file_manager.extract(
            root_dir=SUBMISSION_DIR,
            submission_id=submission_id,
            meta=request.files['meta.json'],
            source=request.files['src'],
            testdata=request.files['testcase'],
        )
    except ValueError as e:
        return str(e), 400
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


@app.get('/status')
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


@app.post('/result/<submission_id>')
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
