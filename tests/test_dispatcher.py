from dispatcher import Dispatcher
from tests.submission_generator import SubmissionGenerator


class TestDispatcher:
    @classmethod
    def setup_class(cls):
        '''
        prepare submissions
        '''
        cls.generator = SubmissionGenerator()
        cls.generator.gen_all()

    @classmethod
    def teardown_class(cls):
        '''
        clean up submission data
        '''
        cls.generator.clear()

    def test_create_dispatcher(self):
        docker_dispatcher = Dispatcher()
        assert docker_dispatcher is not None

    def test_start_dispatcher(self, docker_dispatcher: Dispatcher):
        docker_dispatcher.start()

    def test_normal_submission(self, docker_dispatcher: Dispatcher):
        docker_dispatcher.start()
        _ids = []
        for _id, prob in TestDispatcher.generator.submission_ids.items():
            if prob == 'normal-submission':
                _ids.append((_id, prob))

        assert len(_ids) != 0

        for _id, prob in _ids:
            assert docker_dispatcher.handle(
                _id,
                TestDispatcher.generator.problem[prob]['meta']['lang']) is True


    def test_duplicated_submission(self, docker_dispatcher: Dispatcher):
        import random
        docker_dispatcher.start()

        _id, prob = random.choice(
            list(TestDispatcher.generator.submission_ids.items()))

        assert _id is not None
        assert prob is not None

        assert docker_dispatcher.handle(
            _id,
            TestDispatcher.generator.problem[prob]['meta']['lang']) is True
        assert docker_dispatcher.handle(
            _id,
            TestDispatcher.generator.problem[prob]['meta']['lang']) is False
