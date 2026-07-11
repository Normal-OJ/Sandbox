import pytest

from agent import config


@pytest.mark.parametrize("token",
                         [None, "", "   ", config.DEV_REGISTRATION_TOKEN])
def test_secure_registration_token_rejects_missing_or_default(
        monkeypatch, token):
    monkeypatch.setenv("RUNNER_REQUIRE_SECURE_TOKEN", "true")
    if token is None:
        monkeypatch.delenv("RUNNER_REGISTRATION_TOKEN", raising=False)
    else:
        monkeypatch.setenv("RUNNER_REGISTRATION_TOKEN", token)

    with pytest.raises(RuntimeError):
        config._load_registration_token()


def test_secure_registration_token_accepts_non_default(monkeypatch):
    monkeypatch.setenv("RUNNER_REQUIRE_SECURE_TOKEN", "true")
    monkeypatch.setenv("RUNNER_REGISTRATION_TOKEN", "production-secret")

    assert config._load_registration_token() == "production-secret"
