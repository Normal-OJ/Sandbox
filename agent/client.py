"""HTTP client for the Backend runner API."""
import requests

from . import config


class BackendClient:
    """Thin wrapper around requests, adding auth, base URL, error mapping."""

    class AuthError(Exception):
        """Raised when backend rejects auth (401)."""

    class TransientError(Exception):
        """Raised on 5xx or network errors — caller should retry."""

    def __init__(self, base_url: str = None, rk_token: str = None):
        self.base_url = base_url or config.BACKEND_URL
        self.rk_token = rk_token

    # ------- Public API -------

    def register(self, name: str, registration_token: str) -> dict:
        """Register this runner. Returns the `data` payload from backend."""
        rv = self._request(
            "POST",
            "/runners/register",
            json_body={
                "registration_token": registration_token,
                "name": name
            },
            need_auth=False,
            expected_statuses=(201, ),
        )
        return rv.json()["data"]

    def heartbeat(self, runner_id: str) -> None:
        """Send a heartbeat. Raises AuthError on 401."""
        self._request(
            "POST",
            f"/runners/{runner_id}/heartbeat",
            expected_statuses=(204, ),
        )

    def next_job(self, runner_id: str) -> dict | None:
        """Poll for next job. Returns None if no job available (204)."""
        rv = self._request(
            "GET",
            f"/runners/{runner_id}/next-job",
            expected_statuses=(200, 204),
        )
        if rv.status_code == 204:
            return None
        return rv.json()["data"]

    def complete_job(self, runner_id: str, job_id: str, tasks: list) -> str:
        """Send result. Returns 'ok' / 'reclaimed' / 'not_found'.

        Raises TransientError on 5xx or network — caller should retry.
        """
        rv = self._request(
            "PUT",
            f"/runners/{runner_id}/jobs/{job_id}/complete",
            json_body={"tasks": tasks},
            expected_statuses=(204, 409, 404),
        )
        return {204: "ok", 409: "reclaimed", 404: "not_found"}[rv.status_code]

    def abort_job(self, runner_id: str, job_id: str, reason: str) -> str:
        """Tell backend this runner cannot process the leased job."""
        rv = self._request(
            "PUT",
            f"/runners/{runner_id}/jobs/{job_id}/abort",
            json_body={"reason": reason},
            expected_statuses=(202, 409, 404),
        )
        return {
            202: "requeued",
            409: "reclaimed",
            404: "not_found"
        }[rv.status_code]

    def download_code(self, code_url: str, dest_path: str) -> None:
        """Download code zip from a presigned URL."""
        try:
            with requests.get(code_url,
                              stream=True,
                              timeout=config.HTTP_REQUEST_TIMEOUT_SEC) as r:
                r.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        f.write(chunk)
        except requests.RequestException as e:
            raise self.TransientError(f"code download failed: {e}") from e

    # ------- Internals -------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body=None,
        need_auth=True,
        expected_statuses=(200, )) -> requests.Response:
        headers = {}
        if need_auth:
            if not self.rk_token:
                raise self.AuthError("rk_token not set")
            headers["Authorization"] = f"Bearer {self.rk_token}"
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        try:
            rv = requests.request(
                method=method,
                url=f"{self.base_url}{path}",
                json=json_body,
                headers=headers,
                timeout=config.HTTP_REQUEST_TIMEOUT_SEC,
            )
        except requests.RequestException as e:
            raise self.TransientError(f"network error: {e}") from e

        if rv.status_code == 401:
            raise self.AuthError(rv.text)
        if rv.status_code >= 500:
            raise self.TransientError(f"backend {rv.status_code}: {rv.text}")
        if rv.status_code not in expected_statuses:
            raise self.TransientError(
                f"unexpected status {rv.status_code}: {rv.text}")
        return rv
