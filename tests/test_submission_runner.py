import json
import re
import pytest


@pytest.mark.parametrize(
    'stdout, answer, excepted',
    [
        # exactly the same
        ('aaa\nbbb\n', 'aaa\nbbb\n', True),
        # trailing space before new line
        ('aaa  \nbbb\n', 'aaa\nbbb\n', True),
        # redundant new line at the end
        ('aaa\nbbb\n\n', 'aaa\nbbb\n', True),
        # redundant new line in the middle
        ('aaa\n\nbbb\n', 'aaa\nbbb\n', False),
        # trailing space at the start
        ('aaa\n bbb\b', 'aaa\nbbb\n', False),
        # empty string
        ('', '', True),
        # only new line
        ('\n\n\n\n', '', True),
        # empty character
        ('\t\r\n', '', True),
        # crlf
        ('crlf\r\n', 'crlf\n', True),
    ],
)
def test_strip_func(TestSubmissionRunner, stdout, answer, excepted):
    assert (TestSubmissionRunner.strip(stdout) == TestSubmissionRunner.strip(
        answer)) is excepted


def test_container_name_keyed_by_job_id(TestSubmissionRunner):
    runner = TestSubmissionRunner(
        job_id='abc123',
        time_limit=1000,
        mem_limit=32768,
        testdata_input_path='',
        testdata_output_path='',
        lang='python3',
    )
    name = runner.container_name('0001')
    assert name.startswith('abc123-0001-')
    # valid docker container name
    assert re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]+', name)
    # suffix keeps duplicated runs of the same job/case from colliding
    assert name != runner.container_name('0001')


def test_c_tle(submission_generator, TestSubmissionRunner):
    job_id = [
        _id for _id, pn in submission_generator.submission_ids.items()
        if pn == 'c-TLE'
    ][0]
    submission_path = submission_generator.get_submission_path(job_id)

    runner = TestSubmissionRunner(
        job_id=job_id,
        time_limit=1000,
        mem_limit=32768,
        testdata_input_path=submission_path + '/testcase/0000.in',
        testdata_output_path=submission_path + '/testcase/0000.out',
        lang='c11',
    )

    res = runner.compile()
    assert res['Status'] == 'AC', json.dumps(res)
    res = runner.run()
    assert res['Status'] == 'TLE', json.dumps(res)


def test_non_strict_diff(submission_generator, TestSubmissionRunner):
    job_id = [
        _id for _id, pn in submission_generator.submission_ids.items()
        if pn == 'space-before-lf'
    ][0]
    submission_path = submission_generator.get_submission_path(job_id)

    runner = TestSubmissionRunner(
        job_id=job_id,
        time_limit=1000,
        mem_limit=32768,
        testdata_input_path=submission_path + '/testcase/0000.in',
        testdata_output_path=submission_path + '/testcase/0000.out',
        lang='python3',
    )

    res = runner.run()
    assert res['Status'] == 'AC', res
