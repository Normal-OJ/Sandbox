class SubmissionIdNotFoundError(BaseException):
    '''
    raise this error when submission id not found
    '''


class DuplicatedSubmissionIdError(BaseException):
    '''
    raise this when receive a duplicated submission id
    '''