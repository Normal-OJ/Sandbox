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
        testdata_input_path=submission_path + '/testcase/0/in',
        testdata_output_path=submission_path + '/testcase/0/out',
        lang='c11',
    )

    res = runner.compile()
    assert res['Status'] == 'AC'

    res = runner.run()
    assert res['Status'] == 'TLE'
