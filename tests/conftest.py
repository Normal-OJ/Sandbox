import pytest
from dispatcher import Dispatcher

TEST_CONFIG_PATH = '.config/dispatcher.test.json'

@pytest.fixture
def docker_dispatcher():
    # create a dispatcer in test config
    d = Dispatcher(TEST_CONFIG_PATH)
    yield d
    # ensure we stop the dispatcher after every function call
    d.stop()