port = 1450
bind = f'0.0.0.0:{port}'
timeout = 60

# loglevel = 'debug'
accesslog = 'access.log'
errorlog = 'error.log'

worker_class = 'gthread'
threads = 5
