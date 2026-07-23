import pytest
import requests

from runner.client import (
    BackendClient,
    BackendAPIError,
    BackendAuthError,
    JobPayload,
    RunnerIdentity,
    RunnerConfig,
)


class StubResponse:

    def __init__(self, status_code, json_body=None):
        self.status_code = status_code
        self._json_body = json_body

    def json(self):
        return self._json_body


class RecordingSession:
    """Duck-typed replacement for requests.Session that records calls."""

    def __init__(self, response=None, exc=None):
        self._response = response
        self._exc = exc
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({
            'method': 'post',
            'url': url,
            'json': json,
            'headers': headers,
            'timeout': timeout,
        })
        if self._exc is not None:
            raise self._exc
        return self._response

    def get(self, url, json=None, headers=None, timeout=None):
        self.calls.append({
            'method': 'get',
            'url': url,
            'json': json,
            'headers': headers,
            'timeout': timeout,
        })
        if self._exc is not None:
            raise self._exc
        return self._response

    def put(self, url, json=None, headers=None, timeout=None):
        self.calls.append({
            'method': 'put',
            'url': url,
            'json': json,
            'headers': headers,
            'timeout': timeout,
        })
        if self._exc is not None:
            raise self._exc
        return self._response


def test_register_success_parses_identity_and_config():
    session = RecordingSession(
        StubResponse(
            201,
            {
                'runner_id': 'rn_abc',
                'token': 'rk_secret',
                'config': {
                    'heartbeat_interval_sec': 20,
                    'poll_interval_sec': 5,
                    'max_concurrent_jobs': 4,
                },
            },
        ))
    client = BackendClient('http://web:8080', session=session, timeout=10)

    identity = client.register('reg-token', 'runner-1')

    assert isinstance(identity, RunnerIdentity)
    assert identity.runner_id == 'rn_abc'
    assert identity.token == 'rk_secret'
    assert identity.config == RunnerConfig(20, 5, 4)

    call = session.calls[0]
    assert call['url'] == 'http://web:8080/runners/register'
    assert call['json'] == {
        'registration_token': 'reg-token',
        'name': 'runner-1',
    }
    assert call['timeout'] == 10


def test_register_fills_config_fallback_defaults():
    session = RecordingSession(
        StubResponse(
            201,
            {
                'runner_id': 'rn_abc',
                'token': 'rk_secret',
                'config': {
                    'poll_interval_sec': 7
                },
            },
        ))
    client = BackendClient('http://web:8080', session=session)

    identity = client.register('reg-token', 'runner-1')

    # Missing keys use the spec fallback defaults (15 / 3 / 8).
    assert identity.config == RunnerConfig(15, 7, 8)


def test_register_missing_config_object_uses_all_defaults():
    session = RecordingSession(
        StubResponse(201, {
            'runner_id': 'rn_abc',
            'token': 'rk_secret',
        }))
    client = BackendClient('http://web:8080', session=session)

    identity = client.register('reg-token', 'runner-1')

    assert identity.config == RunnerConfig(15, 3, 8)


def test_register_401_raises_auth_error():
    session = RecordingSession(StubResponse(401))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(BackendAuthError) as excinfo:
        client.register('bad-token', 'runner-1')
    assert excinfo.value.status_code == 401
    assert isinstance(excinfo.value, BackendAPIError)


def test_register_other_error_raises_api_error():
    session = RecordingSession(StubResponse(500))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(BackendAPIError) as excinfo:
        client.register('reg-token', 'runner-1')
    assert not isinstance(excinfo.value, BackendAuthError)
    assert excinfo.value.status_code == 500


def test_register_network_error_propagates():
    session = RecordingSession(exc=requests.ConnectionError('boom'))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(requests.RequestException):
        client.register('reg-token', 'runner-1')


def test_heartbeat_sends_url_auth_header_and_body():
    session = RecordingSession(StubResponse(204))
    client = BackendClient('http://web:8080', session=session, timeout=10)
    identity = RunnerIdentity('rn_abc', 'rk_secret', RunnerConfig(15, 3, 8))

    result = client.heartbeat(identity, ['jb_1', 'jb_2'])

    assert result is None
    call = session.calls[0]
    assert call['url'] == 'http://web:8080/runners/rn_abc/heartbeat'
    assert call['headers'] == {'Authorization': 'Bearer rk_secret'}
    assert call['json'] == {'active_job_ids': ['jb_1', 'jb_2']}
    assert call['timeout'] == 10


def test_heartbeat_401_raises_auth_error():
    session = RecordingSession(StubResponse(401))
    client = BackendClient('http://web:8080', session=session)
    identity = RunnerIdentity('rn_abc', 'rk_secret', RunnerConfig(15, 3, 8))

    with pytest.raises(BackendAuthError):
        client.heartbeat(identity, [])


def test_heartbeat_other_error_raises_api_error():
    session = RecordingSession(StubResponse(503))
    client = BackendClient('http://web:8080', session=session)
    identity = RunnerIdentity('rn_abc', 'rk_secret', RunnerConfig(15, 3, 8))

    with pytest.raises(BackendAPIError) as excinfo:
        client.heartbeat(identity, [])
    assert not isinstance(excinfo.value, BackendAuthError)
    assert excinfo.value.status_code == 503


def test_base_url_trailing_slash_is_normalized():
    session = RecordingSession(StubResponse(204))
    client = BackendClient('http://web:8080/', session=session)
    identity = RunnerIdentity('rn_abc', 'rk_secret', RunnerConfig(15, 3, 8))

    client.heartbeat(identity, [])

    assert session.calls[0][
        'url'] == 'http://web:8080/runners/rn_abc/heartbeat'


def test_token_absent_from_identity_repr():
    identity = RunnerIdentity('rn_abc', 'rk_super_secret',
                              RunnerConfig(15, 3, 8))
    assert 'rk_super_secret' not in repr(identity)
    assert 'rn_abc' in repr(identity)


def make_identity():
    return RunnerIdentity('rn_abc', 'rk_secret', RunnerConfig(15, 3, 8))


NEXT_JOB_BODY = {
    'job_id':
    'jb_1',
    'submission_id':
    'sub_1',
    'problem_id':
    42,
    'language':
    2,
    'code_url':
    'http://minio/code.zip',
    'checker':
    'diff',
    'tasks': [{
        'taskScore': 100,
        'memoryLimit': 65536,
        'timeLimit': 1000,
        'caseCount': 3,
    }],
}


def test_next_job_200_parses_payload():
    session = RecordingSession(StubResponse(200, NEXT_JOB_BODY))
    client = BackendClient('http://web:8080', session=session, timeout=10)
    identity = make_identity()

    payload = client.next_job(identity)

    assert isinstance(payload, JobPayload)
    assert payload.job_id == 'jb_1'
    assert payload.submission_id == 'sub_1'
    assert payload.problem_id == 42
    assert payload.language == 2
    assert payload.code_url == 'http://minio/code.zip'
    assert payload.checker == 'diff'
    assert payload.tasks == NEXT_JOB_BODY['tasks']

    call = session.calls[0]
    assert call['method'] == 'get'
    assert call['url'] == 'http://web:8080/runners/rn_abc/next-job'
    assert call['headers'] == {'Authorization': 'Bearer rk_secret'}
    assert call['timeout'] == 10


def test_next_job_normalizes_string_problem_id():
    # The backend job hash stores fields as strings.
    body = dict(NEXT_JOB_BODY)
    body['problem_id'] = '42'
    session = RecordingSession(StubResponse(200, body))
    client = BackendClient('http://web:8080', session=session)

    payload = client.next_job(make_identity())

    assert payload.problem_id == 42


def test_next_job_defaults_checker_to_none_when_absent():
    body = dict(NEXT_JOB_BODY)
    del body['checker']
    session = RecordingSession(StubResponse(200, body))
    client = BackendClient('http://web:8080', session=session)

    payload = client.next_job(make_identity())

    assert payload.checker is None


def test_next_job_204_returns_none():
    session = RecordingSession(StubResponse(204))
    client = BackendClient('http://web:8080', session=session)

    assert client.next_job(make_identity()) is None


def test_next_job_401_raises_auth_error():
    session = RecordingSession(StubResponse(401))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(BackendAuthError) as excinfo:
        client.next_job(make_identity())
    assert excinfo.value.status_code == 401


def test_next_job_500_raises_api_error():
    session = RecordingSession(StubResponse(500))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(BackendAPIError) as excinfo:
        client.next_job(make_identity())
    assert not isinstance(excinfo.value, BackendAuthError)
    assert excinfo.value.status_code == 500


def test_complete_204_sends_url_body_and_auth():
    session = RecordingSession(StubResponse(204))
    client = BackendClient('http://web:8080', session=session, timeout=10)
    tasks = [{'status': 0}]

    result = client.complete(make_identity(), 'jb_1', tasks)

    assert result is None
    call = session.calls[0]
    assert call['method'] == 'put'
    assert call['url'] == 'http://web:8080/runners/rn_abc/jobs/jb_1/complete'
    assert call['json'] == {'tasks': tasks}
    assert call['headers'] == {'Authorization': 'Bearer rk_secret'}
    assert call['timeout'] == 10


def test_complete_409_raises_api_error():
    session = RecordingSession(StubResponse(409))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(BackendAPIError) as excinfo:
        client.complete(make_identity(), 'jb_1', [])
    assert excinfo.value.status_code == 409


def test_abort_202_sends_url_and_reason():
    session = RecordingSession(StubResponse(202))
    client = BackendClient('http://web:8080', session=session)

    result = client.abort(make_identity(), 'jb_1', 'prep_failed')

    assert result is None
    call = session.calls[0]
    assert call['method'] == 'put'
    assert call['url'] == 'http://web:8080/runners/rn_abc/jobs/jb_1/abort'
    assert call['json'] == {'reason': 'prep_failed'}
    assert call['headers'] == {'Authorization': 'Bearer rk_secret'}


def test_abort_404_raises_api_error():
    session = RecordingSession(StubResponse(404))
    client = BackendClient('http://web:8080', session=session)

    with pytest.raises(BackendAPIError) as excinfo:
        client.abort(make_identity(), 'jb_1', 'drain')
    assert excinfo.value.status_code == 404
