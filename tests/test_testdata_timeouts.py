"""Every prep-path network call must be bounded (drain safety).

Unbounded HTTP or redis-lock waits inside ensure_testdata() would keep a
poller iteration alive past stop(), so a SIGTERM drain would hang until
docker SIGKILLs the runner and in-flight results are lost.
"""

import pytest

from dispatcher import testdata


class StubResponse:

    def __init__(self, payload=None, content=b''):
        self.status_code = 200
        self.ok = True
        self.content = content
        self._payload = payload

    def json(self):
        return self._payload


class GetRecorder:

    def __init__(self, response):
        self.calls = []
        self._response = response

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self._response


def test_fetch_problem_meta_is_bounded(tmp_path, monkeypatch):
    recorder = GetRecorder(StubResponse(payload={'data': {'tasks': []}}))
    monkeypatch.setattr(testdata.rq, 'get', recorder)
    monkeypatch.setattr(testdata, 'META_DIR', tmp_path)

    testdata.fetch_problem_meta(42)

    assert recorder.calls[0][1]['timeout'] == testdata.HTTP_TIMEOUT


def test_fetch_testdata_is_bounded(monkeypatch):
    recorder = GetRecorder(StubResponse(content=b'zip'))
    monkeypatch.setattr(testdata.rq, 'get', recorder)

    assert testdata.fetch_testdata(42) == b'zip'
    assert recorder.calls[0][1]['timeout'] == testdata.HTTP_TIMEOUT


def test_get_checksum_is_bounded(monkeypatch):
    recorder = GetRecorder(StubResponse(payload={'data': 'abc'}))
    monkeypatch.setattr(testdata.rq, 'get', recorder)

    assert testdata.get_checksum(42) == 'abc'
    assert recorder.calls[0][1]['timeout'] == testdata.HTTP_TIMEOUT


class LockSentinel(Exception):
    pass


class FakeRedis:
    """Records lock() kwargs, then short-circuits the rest of ensure_testdata."""

    def __init__(self):
        self.lock_kwargs = None

    def lock(self, key, **kwargs):
        self.lock_kwargs = kwargs
        raise LockSentinel()


def test_ensure_testdata_lock_has_blocking_timeout(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(testdata, 'get_redis_client', lambda: fake)

    with pytest.raises(LockSentinel):
        testdata.ensure_testdata(42)

    assert fake.lock_kwargs['timeout'] == 60
    assert fake.lock_kwargs[
        'blocking_timeout'] == testdata.LOCK_BLOCKING_TIMEOUT
