import json
import logging
import shutil
from datetime import datetime
import requests as rq
from .config import (
    BACKEND_API,
    SANDBOX_TOKEN,
    SUBMISSION_DIR,
    SUBMISSION_BACKUP_DIR,
)

# FIXME: confused with dispatcher.utils.logger
def logger() -> logging.Logger:
    return logging.getLogger('rq.worker')


def clean_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    shutil.rmtree(submission_dir)


def backup_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    dest = SUBMISSION_BACKUP_DIR / f'{submission_id}_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}'
    shutil.move(submission_dir, dest)


def send_submission_result(submission_id: str):
    '''
    Send submission result to backend server
    '''
    logger().info(f'submission completed [id={submission_id}]')
    result = (SUBMISSION_DIR / submission_id / 'result.json')
    if not result.exists():
        logger().error(f'cannot find submission result [id={submission_id}]')
        return
    post_data = json.load(result.open())
    post_data['token'] = SANDBOX_TOKEN
    logger().info(f'send submission to backend [id={submission_id}]')
    resp = rq.put(
        f'{BACKEND_API}/submission/{submission_id}/complete',
        json=post_data,
    )
    logger().debug(
        f'backend response [{json.dumps({"code": resp.status_code, "text": resp.text})}',
    )
    if resp.status_code == 200:
        clean_data(submission_id)
    else:
        backup_data(submission_id)
