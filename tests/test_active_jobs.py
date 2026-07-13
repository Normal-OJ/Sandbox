import threading

from runner.active_jobs import ActiveJobTracker


def test_add_and_snapshot():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    tracker.add('jb_2')

    assert sorted(tracker.snapshot()) == ['jb_1', 'jb_2']
    assert len(tracker) == 2


def test_add_is_idempotent():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    tracker.add('jb_1')

    assert tracker.snapshot() == ['jb_1']
    assert len(tracker) == 1


def test_remove():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    tracker.add('jb_2')
    tracker.remove('jb_1')

    assert tracker.snapshot() == ['jb_2']
    assert len(tracker) == 1


def test_remove_absent_is_noop():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    tracker.remove('jb_does_not_exist')

    assert tracker.snapshot() == ['jb_1']
    assert len(tracker) == 1


def test_snapshot_is_a_copy():
    tracker = ActiveJobTracker()
    tracker.add('jb_1')
    snap = tracker.snapshot()
    snap.append('jb_mutated')

    assert tracker.snapshot() == ['jb_1']


def test_thread_safety_smoke():
    tracker = ActiveJobTracker()
    n = 200

    def worker(base):
        for i in range(n):
            job_id = f'{base}_{i}'
            tracker.add(job_id)
            tracker.remove(job_id)

    threads = [threading.Thread(target=worker, args=[b]) for b in 'abcd']
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every add was paired with a remove, so the tracker ends empty and did
    # not raise (e.g. "set changed size during iteration") under contention.
    assert len(tracker) == 0
    assert tracker.snapshot() == []
