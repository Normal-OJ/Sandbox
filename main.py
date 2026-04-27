"""Runner agent entrypoint.

Replaces the old Flask app — this process actively polls Backend instead of
listening for incoming HTTP. Spawns 4 daemon threads:
  - dispatcher (existing)
  - heartbeat
  - poller
  - result_sender

Graceful shutdown on SIGTERM/SIGINT: stop polling, drain result queue, exit.
"""
import logging
import os
import signal
import threading
import time

from agent import config as agent_config
from agent.client import BackendClient
from agent.heartbeat import HeartbeatThread
from agent.poller import PollerThread
from agent.registration import register_runner
from agent.result_sender import ResultSenderThread
from dispatcher.dispatcher import Dispatcher

# Ensure logs/ directory exists before configuring file handler
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s/%(threadName)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/runner.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("main")


def main():
    log.info("runner agent starting")
    shutdown_event = threading.Event()

    # 1. Register
    bootstrap_client = BackendClient()  # no token yet
    creds = register_runner(
        client=bootstrap_client,
        name=agent_config.RUNNER_NAME,
        registration_token=agent_config.RUNNER_REGISTRATION_TOKEN,
    )
    log.info(f"registered as {creds.runner_id}")

    # 2. Authenticated client used by all daemon threads
    client = BackendClient(rk_token=creds.token)

    # 3. Start dispatcher (existing)
    dispatcher_config_path = os.getenv("DISPATCHER_CONFIG",
                                       ".config/dispatcher.json.example")
    dispatcher = Dispatcher(dispatcher_config_path)
    dispatcher.start()
    log.info("dispatcher started")

    # 4. Start daemon threads
    heartbeat = HeartbeatThread(
        client=client,
        runner_id=creds.runner_id,
        interval_sec=creds.heartbeat_interval_sec,
        shutdown_event=shutdown_event,
    )
    poller = PollerThread(
        client=client,
        runner_id=creds.runner_id,
        dispatcher=dispatcher,
        poll_interval_sec=creds.poll_interval_sec,
        shutdown_event=shutdown_event,
    )
    sender = ResultSenderThread(
        client=client,
        runner_id=creds.runner_id,
        result_queue=dispatcher.result_queue,
        shutdown_event=shutdown_event,
        retry_max_attempts=agent_config.RESULT_RETRY_MAX_ATTEMPTS,
        retry_initial_backoff_sec=agent_config.
        RESULT_RETRY_INITIAL_BACKOFF_SEC,
        retry_max_backoff_sec=agent_config.RESULT_RETRY_MAX_BACKOFF_SEC,
    )
    heartbeat.start()
    poller.start()
    sender.start()
    log.info("all threads started")

    # 5. Wait for shutdown signal
    def handle_sig(signum, frame):
        log.info(f"received signal {signum}, shutting down")
        shutdown_event.set()
        dispatcher.stop()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    while not shutdown_event.is_set():
        time.sleep(1)

    # 6. Graceful drain — give result_sender up to 60s to flush
    log.info("waiting for in-flight work to complete (max 60s)")
    sender.join(timeout=60)
    log.info("runner agent exiting")


if __name__ == "__main__":
    main()
