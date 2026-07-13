from dispatcher.dispatcher import Dispatcher
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
        docker_dispatcher.handle(job_id=_id, submission_id=f'sub-{_id}')
        assert docker_dispatcher.contains(_id)


def test_same_submission_parallel_jobs(
    docker_dispatcher: Dispatcher,
    submission_generator,
):
    prob_name = next(iter(submission_generator.submission_ids.values()))

    job1 = submission_generator.gen_submission_id()
    submission_generator.gen_submission(prob_name, job1)
    job2 = submission_generator.gen_submission_id()
    submission_generator.gen_submission(prob_name, job2)

    docker_dispatcher.handle(job_id=job1, submission_id='same-sub')
    docker_dispatcher.handle(job_id=job2, submission_id='same-sub')

    assert docker_dispatcher.contains(job1)
    assert docker_dispatcher.contains(job2)
    # queued work must target each job's own directory
    queued_job_ids = {j.job_id for j in docker_dispatcher.queue.queue}
    assert queued_job_ids == {job1, job2}

    docker_dispatcher.release(job1)

    assert not docker_dispatcher.contains(job1)
    assert docker_dispatcher.contains(job2)
