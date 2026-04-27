"""Tests for the BackendClient HTTP wrapper."""
import pytest
import responses

from agent.client import BackendClient


@pytest.fixture
def client():
    return BackendClient(base_url="http://test-backend", rk_token="rk_test")


@responses.activate
def test_register_posts_to_correct_url(client):
    responses.add(
        responses.POST,
        "http://test-backend/runners/register",
        json={"data": {"runner_id": "rn_1", "token": "rk_xyz", "config": {}}},
        status=201,
    )
    rv = client.register(name="r1", registration_token="dev-token")
    assert rv["runner_id"] == "rn_1"
    assert rv["token"] == "rk_xyz"
    req = responses.calls[0].request
    assert req.headers["Content-Type"] == "application/json"


@responses.activate
def test_register_raises_on_401():
    c = BackendClient(base_url="http://test-backend", rk_token=None)
    responses.add(
        responses.POST,
        "http://test-backend/runners/register",
        json={"message": "invalid"},
        status=401,
    )
    with pytest.raises(BackendClient.AuthError):
        c.register(name="r1", registration_token="wrong")


@responses.activate
def test_heartbeat_sends_bearer_token(client):
    responses.add(
        responses.POST,
        "http://test-backend/runners/rn_1/heartbeat",
        status=204,
    )
    client.heartbeat(runner_id="rn_1")
    req = responses.calls[0].request
    assert req.headers["Authorization"] == "Bearer rk_test"


@responses.activate
def test_next_job_returns_payload_when_200(client):
    responses.add(
        responses.GET,
        "http://test-backend/runners/rn_1/next-job",
        json={"data": {"job_id": "jb_1", "submission_id": "sub_1",
                       "problem_id": 42, "language": 0, "code_url": "http://...",
                       "checker": "", "tasks": []}},
        status=200,
    )
    job = client.next_job(runner_id="rn_1")
    assert job["job_id"] == "jb_1"


@responses.activate
def test_next_job_returns_none_when_204(client):
    responses.add(
        responses.GET,
        "http://test-backend/runners/rn_1/next-job",
        status=204,
    )
    assert client.next_job(runner_id="rn_1") is None


@responses.activate
def test_complete_job_returns_status_string(client):
    responses.add(
        responses.PUT,
        "http://test-backend/runners/rn_1/jobs/jb_1/complete",
        status=204,
    )
    assert client.complete_job("rn_1", "jb_1", tasks=[]) == "ok"


@responses.activate
def test_complete_job_returns_reclaimed_on_409(client):
    responses.add(
        responses.PUT,
        "http://test-backend/runners/rn_1/jobs/jb_1/complete",
        status=409,
    )
    assert client.complete_job("rn_1", "jb_1", tasks=[]) == "reclaimed"


@responses.activate
def test_complete_job_returns_not_found_on_404(client):
    responses.add(
        responses.PUT,
        "http://test-backend/runners/rn_1/jobs/jb_1/complete",
        status=404,
    )
    assert client.complete_job("rn_1", "jb_1", tasks=[]) == "not_found"


@responses.activate
def test_complete_job_raises_on_5xx(client):
    responses.add(
        responses.PUT,
        "http://test-backend/runners/rn_1/jobs/jb_1/complete",
        status=503,
    )
    with pytest.raises(BackendClient.TransientError):
        client.complete_job("rn_1", "jb_1", tasks=[])
