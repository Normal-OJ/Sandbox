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
    submission_generator,
):
    docker_dispatcher.start()
    _ids = []
    for _id, prob in submission_generator.submission_ids.items():
        if prob == 'normal-submission':
            _ids.append((_id, prob))

    assert len(_ids) != 0

    for _id, prob in _ids:
        docker_dispatcher.handle(_id)


def test_duplicated_submission(
    docker_dispatcher: Dispatcher,
    submission_generator,
):
    import random
    docker_dispatcher.start()

    _id, prob = random.choice(list(
        submission_generator.submission_ids.items()))

    assert _id is not None
    assert prob is not None

    docker_dispatcher.handle(_id)

    try:
        docker_dispatcher.handle(_id)
    except DuplicatedSubmissionIdError:
        return
    assert False


import queue


def test_dispatcher_has_capacity_returns_true_when_queue_empty(
        docker_dispatcher):
    assert docker_dispatcher.has_capacity() is True


def test_dispatcher_has_capacity_respects_active_submission_limit(
        docker_dispatcher):
    docker_dispatcher.max_concurrent_jobs = 1
    docker_dispatcher.result["sub_1"] = (None, {})

    assert docker_dispatcher.has_capacity() is False


def test_dispatcher_exposes_result_queue(docker_dispatcher):
    """Dispatcher should expose a result_queue that result_sender drains."""
    assert isinstance(docker_dispatcher.result_queue, queue.Queue)


def test_dispatcher_exposes_job_ids_mapping(docker_dispatcher):
    """Dispatcher should track submission_id -> job_id mapping."""
    assert isinstance(docker_dispatcher.job_ids, dict)
    assert docker_dispatcher.job_ids == {}


def test_handle_records_job_id(docker_dispatcher, submission_generator):
    """When poller calls handle() with job_id, dispatcher records the mapping."""
    docker_dispatcher.start()
    submission_ids = list(submission_generator.submission_ids.keys())
    assert submission_ids, "submission_generator should have created at least one submission"
    sub_id = submission_ids[0]

    docker_dispatcher.handle(submission_id=sub_id, job_id="jb_xyz")

    assert docker_dispatcher.job_ids.get(sub_id) == "jb_xyz"


def test_handle_works_without_job_id_for_backwards_compat(
        docker_dispatcher, submission_generator):
    """handle() should work without job_id — for backwards compat with existing tests."""
    docker_dispatcher.start()
    submission_ids = list(submission_generator.submission_ids.keys())
    assert submission_ids
    sub_id = submission_ids[0]

    docker_dispatcher.handle(submission_id=sub_id)  # no job_id

    # Should not raise, and submission should be tracked
    assert docker_dispatcher.contains(sub_id)


import threading
from unittest.mock import MagicMock


def _setup_fake_submission(docker_dispatcher, submission_id, job_id):
    """Helper: set up fake in-memory state + create the submission dir on disk."""
    from dispatcher import file_manager
    docker_dispatcher.testing = False
    docker_dispatcher.job_ids[submission_id] = job_id
    docker_dispatcher.result[submission_id] = (
        MagicMock(language=2),  # python doesn't compile
        {
            "0000": {
                "stdout": "",
                "stderr": "",
                "exitCode": 0,
                "execTime": 1,
                "memoryUsage": 1,
                "status": "AC"
            },
        },
    )
    docker_dispatcher.locks[submission_id] = threading.Lock()
    docker_dispatcher.compile_locks[submission_id] = threading.Lock()
    docker_dispatcher.created_at[submission_id] = __import__(
        "datetime").datetime.now()
    # create submission dir so file_manager.clean_data doesn't fail
    sub_dir = file_manager.config.SUBMISSION_DIR / submission_id
    sub_dir.mkdir(parents=True, exist_ok=True)


def test_on_submission_complete_pushes_to_result_queue(docker_dispatcher):
    """When a submission is done, result lands in result_queue (no HTTP)."""
    from agent.result_sender import JobResult

    _setup_fake_submission(docker_dispatcher, "sub_1", "jb_1")
    docker_dispatcher.on_submission_complete("sub_1")

    pushed = docker_dispatcher.result_queue.get_nowait()
    assert isinstance(pushed, JobResult)
    assert pushed.job_id == "jb_1"
    assert isinstance(pushed.tasks, list)
    assert len(pushed.tasks) == 1


def test_on_submission_complete_releases_state_after_push(docker_dispatcher):
    """After completion, submission state should be cleared."""
    from agent.result_sender import JobResult

    _setup_fake_submission(docker_dispatcher, "sub_2", "jb_2")
    docker_dispatcher.on_submission_complete("sub_2")

    # State cleaned up
    assert "sub_2" not in docker_dispatcher.result
    assert "sub_2" not in docker_dispatcher.job_ids
