import threading


class ActiveJobTracker:
    """Thread-safe set of the job ids this runner currently holds.

    The snapshot is sent with every heartbeat to renew the backend leases
    (spec §7.2); jobs absent from the snapshot stop being renewed and become
    orphans once their lease expires.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._jobs = set()

    def add(self, job_id):
        with self._lock:
            self._jobs.add(job_id)

    def remove(self, job_id):
        # Idempotent: removing an absent id is a no-op.
        with self._lock:
            self._jobs.discard(job_id)

    def snapshot(self):
        with self._lock:
            return list(self._jobs)

    def __len__(self):
        with self._lock:
            return len(self._jobs)
