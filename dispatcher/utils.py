import logging
import os
import redis
from flask import current_app


def logger() -> logging.Logger:
    try:
        return current_app.logger
    except RuntimeError:
        return logging.getLogger('gunicorn.error')


# Fake redis server
server = None
# Redis connection pool
redis_pool = None
REDIS_URL = os.getenv(
    'REDIS_URL',
    'redis://redis:6379/14',
)


def get_redis_client():
    # Only import fakeredis in testing environment
    # if config['TESTING'] == True:
    #     import fakeredis
    #     global server
    #     if server is None:
    #         server = fakeredis.FakeServer()
    #     return fakeredis.FakeStrictRedis(server=server)
    # else:
    # Create connection pool
    global redis_pool
    if redis_pool is None:
        logger().debug(f'Try connecting redis [url={REDIS_URL}]')
        redis_pool = redis.ConnectionPool.from_url(REDIS_URL)
    return redis.Redis(connection_pool=redis_pool)
