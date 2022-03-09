import json
import shutil
from datetime import datetime
from typing import Any, Dict
import requests as rq
from .utils import (
    get_redis_client,
    logger,
)
from .config import (
    BACKEND_API,
    SANDBOX_TOKEN,
    SUBMISSION_DIR,
    SUBMISSION_BACKUP_DIR,
)


def clean_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    shutil.rmtree(submission_dir)


def backup_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    dest = SUBMISSION_BACKUP_DIR / f'{submission_id}_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}'
    shutil.move(submission_dir, dest)


def on_submission_complete(message: Dict[str, Any]):
    submission_id: str = message['data'].decode()
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


def send_submission_result():
    client = get_redis_client()
    p = client.pubsub(ignore_subscribe_messages=True)
    p.subscribe(
        'stop',
        **{'submission-completed': on_submission_complete},
    )

    logger().info(f'start waiting to send submission result')
    for msg in p.listen():
        if msg['channel'] == b'stop':
            break
