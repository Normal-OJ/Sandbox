import logging
import os
import redis


def logger() -> logging.Logger:
    return logging.getLogger('sandbox')


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
        # Bounded socket I/O for the same reason as the testdata HTTP
        # timeouts: a wedged redis must not be able to hang a SIGTERM drain
        # until docker's stop grace period kills us.
        redis_pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            socket_timeout=10,
            socket_connect_timeout=5,
        )
    return redis.Redis(connection_pool=redis_pool)
