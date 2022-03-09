import os
import logging
import queue
import secrets
import threading
from flask import Flask, request, jsonify
from dispatcher import file_manager
from dispatcher.constant import Language
from dispatcher.dispatcher import Dispatcher
from dispatcher.result_sender import send_submission_result
from dispatcher.testdata import (
    ensure_testdata,
    get_problem_meta,
    get_problem_root,
)
from dispatcher.config import (
    SANDBOX_TOKEN,
    SUBMISSION_DIR,
)

DISPATCHER_CONFIG = os.getenv(
    'DISPATCHER_CONFIG',
    '.config/dispatcher.json.example',
)
DISPATCHER = Dispatcher(DISPATCHER_CONFIG)
DISPATCHER.start()
threading.Thread(target=send_submission_result).start()


def create_app():
    logging.basicConfig(filename='logs/sandbox.log')
    app = Flask(__name__)
    if __name__ != '__main__':
        # let flask app use gunicorn's logger
        gunicorn_logger = logging.getLogger('gunicorn.error')
        app.logger.handlers = gunicorn_logger.handlers
        app.logger.setLevel(gunicorn_logger.level)
        logging.getLogger().setLevel(gunicorn_logger.level)
    logger = app.logger

    @app.post('/submit/<submission_id>')
    def submit(submission_id: str):
        token = request.values['token']
        if not secrets.compare_digest(token, SANDBOX_TOKEN):
            logger.debug(f'get invalid token: {token}')
            return 'invalid token', 403
        # Ensure the testdata is up to data
        problem_id = request.form.get('problem_id', type=int)
        ensure_testdata(problem_id)
        language = Language(request.form.get('language', type=int))
        try:
            file_manager.extract(
                root_dir=SUBMISSION_DIR,
                submission_id=submission_id,
                meta=get_problem_meta(problem_id, language),
                source=request.files['src'],
                testdata=get_problem_root(problem_id),
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
        if secrets.compare_digest(
                SANDBOX_TOKEN,
                request.args.get('token', ''),
        ):
            ret.update({
                'queueSize': DISPATCHER.queue.qsize(),
                'maxTaskCount': DISPATCHER.MAX_TASK_COUNT,
                'containerCount': DISPATCHER.container_count,
                'maxContainerCount': DISPATCHER.MAX_TASK_COUNT,
                'submissions': [*DISPATCHER.result.keys()],
                'running': DISPATCHER.do_run,
            })
        return jsonify(ret), 200

    return app
