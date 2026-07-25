import threading


class ActiveJobTracker:
    """Thread-safe multiset of the claims this runner currently holds.

    Keyed by job_id but counted per CLAIM: the backend can reclaim a job
    whose lease expired and requeue it, and this same runner can claim it
    again while the first claim's outcome is still being finalized. A plain
    set would collapse the two -- the first outcome's remove would also drop
    the still-active claim, so the heartbeat would stop renewing its lease,
    the capacity gate would free a slot that is still busy, and the drain's
    reporting tail would believe everything was already reported.

    snapshot() deduplicates to job_ids (the heartbeat renews one lease per
    job); len() counts claims (capacity and the drain's reporting tail need
    the un-collapsed number).
    """

    def __init__(self):
        self._lock = threading.Lock()
        # job_id -> number of outstanding claims
        self._claims = {}

    def add(self, job_id):
        with self._lock:
            self._claims[job_id] = self._claims.get(job_id, 0) + 1

    def remove(self, job_id):
        # Releases ONE claim; idempotent once none are left. Every claim is
        # released exactly once: by the sender when its outcome is finalized,
        # or by the poller when a re-claim supersedes it (see _poll_once).
        with self._lock:
            count = self._claims.get(job_id, 0)
            if count <= 1:
                self._claims.pop(job_id, None)
            else:
                self._claims[job_id] = count - 1

    def snapshot(self):
        with self._lock:
            return list(self._claims)

    def __len__(self):
        with self._lock:
            return sum(self._claims.values())
