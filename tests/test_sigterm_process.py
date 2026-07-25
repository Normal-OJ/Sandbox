"""SIGTERM drain proven against a REAL process (issue #70 done-criterion).

Every other drain test runs in-process: tests/test_main.py fakes the threads,
tests/test_drain_integration.py wires the real threads but still calls
``main.drain()`` by hand, and the signal test only checks that the handler sets
an event. None of them prove that ``docker stop`` on a live runner actually
completes the drain and exits. This one does: it launches main.py as a
subprocess against a stub backend, sends it a real SIGTERM at a point where the
child is demonstrably inside prep (it has opened a connection to our stub
redis), and asserts the backend saw abort(reason='drain') and the process
exited 0.

NOTE: this is the slow file in the suite (a few seconds -- it spawns a process
and waits for real HTTP round trips). It needs no docker daemon and no extra
dependencies.
"""

import json
import os
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER_ID = 'rn_test'
JOB_ID = 'job_sigterm'
SUBMISSION_ID = 'sub_sigterm'

# Bounded everywhere: a regression must fail this test, never hang the suite.
WAIT_TIMEOUT = 20.0
# Generous headroom over the ~1s the drain actually needs, but far below any
# hang. The drain's own budget (DRAIN_TIMEOUT_SEC) is 540s, so a real deadlock
# would blow past this instead of quietly passing.
EXIT_TIMEOUT = 30.0

TASKS = [{
    'taskScore': 100,
    'memoryLimit': 65536,
    'timeLimit': 1000,
    'caseCount': 1,
}]
# language 2 is python3: the dispatcher enqueues no Compile job for it, so this
# would be exactly one Execute task if it ever got dispatched (it must not).
JOB_PAYLOAD = {
    'job_id': JOB_ID,
    'submission_id': SUBMISSION_ID,
    'problem_id': 42,
    'language': 2,
    'code_url': 'http://127.0.0.1:1/code.zip',
    'checker': None,
    'tasks': TASKS,
}


class StubBackend:
    """The five runner-API endpoints main.py calls, recorded thread-safely.

    ``next_job_served`` is the test's trigger: SIGTERM has to land while the
    claim is still in prep, and the only observable edge for that is the
    backend handing out the job.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.requests = []
        self.job_handed_out = False
        self.next_job_served = threading.Event()

    def record(self, path, body):
        with self.lock:
            self.requests.append((path, body))

    def paths(self):
        with self.lock:
            return [path for path, _ in self.requests]

    def find(self, suffix):
        with self.lock:
            return [(p, b) for p, b in self.requests if p.endswith(suffix)]

    def take_job(self):
        with self.lock:
            if self.job_handed_out:
                return None
            self.job_handed_out = True
            return JOB_PAYLOAD


class QuietHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer without HTTPServer's reverse-DNS bind.

    ``HTTPServer.server_bind`` resolves the bound address with
    ``socket.getfqdn``, which blocks for ~35s on a macOS box with mDNS in the
    resolver chain -- all of it before the test body even starts. The FQDN is
    only used to fill in ``server_name``, which nothing here reads.
    """
    daemon_threads = True

    def server_bind(self):
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


def make_handler(state):

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass  # keep pytest output readable

        def _body(self):
            length = int(self.headers.get('Content-Length') or 0)
            if not length:
                return None
            raw = self.rfile.read(length)
            try:
                return json.loads(raw)
            except ValueError:
                return raw.decode('utf-8', 'replace')

        def _respond(self, status, payload=None):
            self.send_response(status)
            if payload is None:
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            encoded = json.dumps(payload).encode()
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self):
            state.record(self.path, self._body())
            if self.path == '/runners/register':
                self._respond(
                    201, {
                        'runner_id': RUNNER_ID,
                        'token': 'tok',
                        'config': {
                            'heartbeat_interval_sec': 60,
                            'poll_interval_sec': 0.05,
                            'max_concurrent_jobs': 8,
                        },
                    })
            elif self.path == f'/runners/{RUNNER_ID}/heartbeat':
                self._respond(204)
            else:
                self._respond(404)

        def do_GET(self):
            state.record(self.path, None)
            if self.path == f'/runners/{RUNNER_ID}/next-job':
                payload = state.take_job()
                if payload is None:
                    self._respond(204)
                else:
                    self._respond(200, payload)
                    state.next_job_served.set()
            else:
                self._respond(404)

        def do_PUT(self):
            state.record(self.path, self._body())
            if self.path.endswith('/complete'):
                self._respond(204)
            elif self.path.endswith('/abort'):
                self._respond(202)
            else:
                self._respond(404)

    return Handler


class StubRedis:
    """A socket that accepts connections and then says nothing.

    This is the test's synchronisation point, and it has to be this rather
    than "the backend served next-job": that only proves the response was
    written, not that the child received it, let alone that it entered
    ``_prepare``. SIGTERM arriving a moment too early would be handled by the
    prep loop's very first stop check -- still an abort(drain), so the test
    would pass while proving something weaker than it claims.

    ``prepare_job`` starts with ``ensure_testdata()``, which talks to redis, so
    a connection here means the child is demonstrably inside prep. Holding the
    connection open (never answering) keeps it parked there until the test has
    sent the signal and drops it.
    """

    def __init__(self):
        self._sock = socket.socket()
        self._sock.bind(('127.0.0.1', 0))
        self._sock.listen(8)
        self.port = self._sock.getsockname()[1]
        self.connected = threading.Event()
        self._lock = threading.Lock()
        self._conns = []
        self._closed = False

    def serve(self):
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return  # listening socket closed during teardown
            with self._lock:
                if self._closed:
                    conn.close()
                    return
                self._conns.append(conn)
            self.connected.set()

    def drop_connections(self):
        """Fail the child's in-flight redis call now.

        Without this it would sit there until its socket timeout, turning a
        1.6s test into an 11s one.
        """
        with self._lock:
            conns, self._conns = self._conns, []
        for conn in conns:
            conn.close()

    def close(self):
        with self._lock:
            self._closed = True
        self.drop_connections()
        self._sock.close()


@pytest.fixture
def stub_redis():
    stub = StubRedis()
    threading.Thread(target=stub.serve, daemon=True).start()
    try:
        yield stub
    finally:
        stub.close()


@pytest.fixture
def stub_backend():
    state = StubBackend()
    server = QuietHTTPServer(('127.0.0.1', 0), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f'http://127.0.0.1:{server.server_address[1]}'
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=WAIT_TIMEOUT)


def test_real_sigterm_drains_and_exits(tmp_path, stub_backend, stub_redis):
    # docker stop on a runner whose claim is still in prep: the claim must come
    # back as an attempt-neutral 'drain' abort (never 'prep_failed', which
    # would push otherwise healthy submissions towards JE on a rolling
    # restart), nothing may be reported complete, and the process must exit 0
    # on its own rather than wait for SIGKILL.
    env = {
        **os.environ,
        'BACKEND_URL': stub_backend.url,
        'RUNNER_REGISTRATION_TOKEN': 'reg-token',
        'RUNNER_NAME': 'sigterm-test-runner',
        # Answers the TCP handshake and nothing else, so prep parks here --
        # see StubRedis.
        'REDIS_URL': f'redis://127.0.0.1:{stub_redis.port}/0',
        # Keep every directory the runner creates or writes inside tmp_path so
        # the test never touches the working tree.
        'SUBMISSION_DIR': str(tmp_path / 'submissions'),
        'SUBMISSION_BACKUP_DIR': str(tmp_path / 'submissions.bk'),
        'TESTDATA_ROOT': str(tmp_path / 'testdata'),
    }
    out_path = tmp_path / 'runner.out'
    err_path = tmp_path / 'runner.err'

    def output():
        return (f'\n--- runner stdout ---\n{out_path.read_text()}'
                f'\n--- runner stderr ---\n{err_path.read_text()}')

    with out_path.open('wb') as out, err_path.open('wb') as err:
        proc = subprocess.Popen(
            [sys.executable, 'main.py'],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=out,
            stderr=err,
        )
        try:
            wait_for(
                proc,
                lambda: '/runners/register' in stub_backend.paths(),
                'the runner to register',
                output,
            )
            wait_for(
                proc,
                stub_backend.next_job_served.is_set,
                'the runner to claim a job',
                output,
            )
            # The claim is not enough: wait until the child is provably inside
            # prep (it has opened a redis connection, which only ensure_testdata
            # does) before signalling.
            wait_for(
                proc,
                stub_redis.connected.is_set,
                'the runner to reach redis from inside prep',
                output,
            )

            # The job is now claimed, prep is running, and nothing has been
            # dispatched -- the exact state whose drain behaviour is at stake.
            proc.send_signal(signal.SIGTERM)
            # Let the parked redis call fail immediately so prep unwinds now
            # rather than after its socket timeout.
            stub_redis.drop_connections()

            try:
                returncode = proc.wait(timeout=EXIT_TIMEOUT)
            except subprocess.TimeoutExpired:
                pytest.fail(f'runner did not exit within {EXIT_TIMEOUT}s of '
                            f'SIGTERM{output()}')
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=WAIT_TIMEOUT)

    assert returncode == 0, f'runner exited {returncode}{output()}'

    aborts = stub_backend.find('/abort')
    assert len(aborts) == 1, f'expected exactly one abort, got {aborts}'
    path, body = aborts[0]
    assert path == f'/runners/{RUNNER_ID}/jobs/{JOB_ID}/abort'
    # The substantive claim: interrupted-by-shutdown, not failed.
    assert body == {'reason': 'drain'}
    assert stub_backend.find('/complete') == []


def wait_for(proc, predicate, message, output, timeout=WAIT_TIMEOUT):
    """Bounded poll that also fails fast (with output) if the runner died.

    Without the proc.poll() check, a registration failure or an import error
    would show up as an opaque timeout instead of the actual traceback.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        if proc.poll() is not None:
            pytest.fail(f'runner exited early ({proc.returncode}) while '
                        f'waiting for {message}{output()}')
        time.sleep(0.02)
    pytest.fail(f'timed out after {timeout}s waiting for {message}{output()}')
