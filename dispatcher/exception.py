class JobIdNotFoundError(Exception):
    '''
    raise this error when job id not found
    '''


class DispatcherDrainingError(Exception):
    '''
    raised by handle() when the dispatcher has been latched by a drain
    (stop_accepting) and refuses to take on new jobs
    '''
