import json
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
    assert (TestSubmissionRunner.strip(stdout)
            == TestSubmissionRunner.strip(answer)) is excepted


def test_c_tle(submission_generator, TestSubmissionRunner):
    submission_id = [
        _id for _id, pn in submission_generator.submission_ids.items()
        if pn == 'c-TLE'
    ][0]
    submission_path = submission_generator.get_submission_path(submission_id)

    runner = TestSubmissionRunner(
        submission_id=submission_id,
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
    submission_id = [
        _id for _id, pn in submission_generator.submission_ids.items()
        if pn == 'space-before-lf'
    ][0]
    submission_path = submission_generator.get_submission_path(submission_id)

    runner = TestSubmissionRunner(
        submission_id=submission_id,
        time_limit=1000,
        mem_limit=32768,
        testdata_input_path=submission_path + '/testcase/0000.in',
        testdata_output_path=submission_path + '/testcase/0000.out',
        lang='python3',
    )

    res = runner.run()
    assert res['Status'] == 'AC', res
