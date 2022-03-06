import logging
from flask import current_app


def logger() -> logging.Logger:
    try:
        return current_app.logger
    except RuntimeError:
        return logging.getLogger('gunicorn.error')
