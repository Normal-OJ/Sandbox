import pytest
import pathlib
from dispatcher.dispatcher import Dispatcher
from submission import SubmissionRunner
from tests.submission_generator import SubmissionGenerator

TEST_CONFIG_PATH = '.config/dispatcher.test.json'


@pytest.fixture
def docker_dispatcher(tmp_path):
    # create a dispatcer in test config
    d = Dispatcher(TEST_CONFIG_PATH)
    d.SUBMISSION_DIR = tmp_path / d.SUBMISSION_DIR
    d.testing = True
    yield d
    # ensure we stop the dispatcher after every function call
    d.stop()


@pytest.fixture
def submission_generator(tmp_path):
    generator = SubmissionGenerator(submission_path=tmp_path / 'submissions')
    generator.gen_all()

    yield generator

    generator.clear()


@pytest.fixture
def TestSubmissionRunner(tmp_path):

    class TestSubmissionRunner(SubmissionRunner):

        def __init__(
            self,
            submission_id,
            time_limit,
            mem_limit,
            testdata_input_path,
            testdata_output_path,
            special_judge=False,
            lang=None,
        ):
            super().__init__(
                submission_id,
                time_limit,
                mem_limit,
                testdata_input_path,
                testdata_output_path,
                special_judge=special_judge,
                lang=lang,
            )
            self.working_dir = str(
                tmp_path /
                pathlib.Path(self.working_dir or 'submissions').name)

    return TestSubmissionRunner
