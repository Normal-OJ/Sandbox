import pytest
from dispatcher.dispatcher import Dispatcher
from dispatcher.exception import *
from tests.submission_generator import SubmissionGenerator


def test_create_dispatcher():
    docker_dispatcher = Dispatcher()
    assert docker_dispatcher is not None


def test_start_dispatcher(docker_dispatcher: Dispatcher):
    docker_dispatcher.start()


def test_normal_submission(
    docker_dispatcher: Dispatcher,
    submission_generator: SubmissionGenerator,
):
    docker_dispatcher.start()
    _id = submission_generator.gen_submission('normal-submission')
    docker_dispatcher.handle(_id)


def test_duplicated_submission(
    docker_dispatcher: Dispatcher,
    submission_generator: SubmissionGenerator,
):
    docker_dispatcher.start()
    _id = submission_generator.gen_submission('normal-submission')
    docker_dispatcher.handle(_id)
    with pytest.raises(DuplicatedSubmissionIdError):
        docker_dispatcher.handle(_id)
