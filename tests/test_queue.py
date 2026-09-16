"""Unit tests for the jobqueue package.

Run with:
    python -m pytest tests/ -v
or:
    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import tempfile
import threading
import unittest
import uuid

from jobqueue import Queue, Worker, compute_backoff
from jobqueue.queue import DONE, FAILED, PENDING, RUNNING


def temp_db_path() -> str:
    return os.path.join(tempfile.gettempdir(), f"jobqueue_test_{uuid.uuid4().hex}.sqlite3")


class QueueBasicsTest(unittest.TestCase):
    def setUp(self):
        self.db_path = temp_db_path()
        self.queue = Queue(self.db_path)

    def tearDown(self):
        self.queue.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_enqueue_creates_pending_job(self):
        job_id = self.queue.enqueue("greet", {"name": "Ada"})
        job = self.queue.get_job(job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job.status, PENDING)
        self.assertEqual(job.job_type, "greet")
        self.assertEqual(job.payload, {"name": "Ada"})
        self.assertEqual(job.retry_count, 0)

    def test_claim_job_dequeues_oldest_pending_job(self):
        first = self.queue.enqueue("a", {"n": 1})
        self.queue.enqueue("b", {"n": 2})

        claimed = self.queue.claim_job()

        self.assertEqual(claimed.id, first)
        self.assertEqual(claimed.status, RUNNING)
        # And it's no longer pending in the DB.
        self.assertEqual(self.queue.get_job(first).status, RUNNING)

    def test_claim_job_returns_none_when_empty(self):
        self.assertIsNone(self.queue.claim_job())

    def test_claim_job_skips_jobs_not_yet_available(self):
        import time

        self.queue.enqueue("later", {}, available_at=time.time() + 100)
        self.assertIsNone(self.queue.claim_job())

    def test_job_result_is_persisted_and_retrievable(self):
        job_id = self.queue.enqueue("compute", {"x": 21})
        claimed = self.queue.claim_job()
        self.queue.mark_done(claimed.id, {"doubled": 42})

        job = self.queue.get_job(job_id)
        self.assertEqual(job.status, DONE)
        self.assertEqual(job.result, {"doubled": 42})
        self.assertIsNone(job.error)

    def test_failed_job_retried_up_to_max_then_permanently_failed(self):
        max_retries = 3
        handlers = {"boom": lambda job: (_ for _ in ()).throw(RuntimeError("nope"))}
        clock = {"t": 1000.0}
        worker = Worker(
            self.queue,
            handlers,
            max_retries=max_retries,
            backoff_base=0.01,
            clock=lambda: clock["t"],
            sleep=lambda s: None,
        )

        job_id = self.queue.enqueue("boom", {})

        # Attempts 1..max_retries -> retried (back to pending).
        for attempt in range(1, max_retries + 1):
            processed = worker.run_once()
            self.assertTrue(processed)
            job = self.queue.get_job(job_id)
            self.assertEqual(job.retry_count, attempt)
            self.assertEqual(job.status, PENDING)
            # Make the retry immediately available for the next run_once().
            clock["t"] += 1000

        # One more attempt exceeds max_retries -> permanently failed.
        processed = worker.run_once()
        self.assertTrue(processed)
        job = self.queue.get_job(job_id)
        self.assertEqual(job.status, FAILED)
        self.assertEqual(job.retry_count, max_retries + 1)
        self.assertIn("nope", job.error)

        # No more jobs left to claim.
        self.assertIsNone(self.queue.claim_job())

    def test_successful_handler_marks_job_done_via_worker(self):
        handlers = {"echo": lambda job: job["payload"]["msg"]}
        worker = Worker(self.queue, handlers, sleep=lambda s: None)
        job_id = self.queue.enqueue("echo", {"msg": "hello"})

        processed = worker.run_once()

        self.assertTrue(processed)
        job = self.queue.get_job(job_id)
        self.assertEqual(job.status, DONE)
        self.assertEqual(job.result, "hello")

    def test_unknown_job_type_is_treated_as_a_failure_and_retried(self):
        worker = Worker(self.queue, handlers={}, max_retries=1, backoff_base=0.01, sleep=lambda s: None)
        job_id = self.queue.enqueue("mystery", {})

        worker.run_once()
        job = self.queue.get_job(job_id)
        self.assertEqual(job.status, PENDING)
        self.assertEqual(job.retry_count, 1)
        self.assertIn("no handler", job.error)

    def test_counts_reflects_job_statuses(self):
        self.queue.enqueue("a", {})
        self.queue.enqueue("a", {})
        self.queue.claim_job()  # claims the oldest job -> running; the other stays pending

        counts = self.queue.counts()
        self.assertEqual(counts.get("running", 0), 1)
        self.assertEqual(counts.get("pending", 0), 1)


class ConcurrencyTest(unittest.TestCase):
    """Verifies that many worker threads sharing one SQLite-backed queue
    never process the same job twice, even under real concurrency."""

    def setUp(self):
        self.db_path = temp_db_path()

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        for ext in ("-wal", "-shm"):
            p = self.db_path + ext
            if os.path.exists(p):
                os.remove(p)

    def test_each_job_processed_exactly_once_by_concurrent_workers(self):
        num_jobs = 200
        num_workers = 8

        queue = Queue(self.db_path)
        job_ids = [queue.enqueue("count", {"i": i}) for i in range(num_jobs)]

        processed_lock = threading.Lock()
        processed_job_ids: list[str] = []

        def handler(job):
            # Record (under a lock) that this job was processed by this
            # call. If claim_job() ever let two threads claim the same
            # job, this list would contain a duplicate id.
            with processed_lock:
                processed_job_ids.append(job["id"])
            return job["payload"]["i"]

        def worker_main():
            worker_queue = Queue(self.db_path)  # separate connection per thread
            worker = Worker(worker_queue, {"count": handler}, poll_interval=0.005, sleep=lambda s: __import__("time").sleep(s))
            # Run until no more pending/running jobs remain.
            worker.run_until_empty(timeout=30)
            worker_queue.close()

        threads = [threading.Thread(target=worker_main) for _ in range(num_workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
            self.assertFalse(t.is_alive(), "worker thread did not finish in time")

        # Every job processed, none processed more than once.
        self.assertEqual(len(processed_job_ids), num_jobs)
        self.assertEqual(len(set(processed_job_ids)), num_jobs)
        self.assertEqual(set(processed_job_ids), set(job_ids))

        # And the DB agrees: every job is 'done'.
        counts = queue.counts()
        self.assertEqual(counts.get("done", 0), num_jobs)
        self.assertEqual(counts.get("pending", 0), 0)
        self.assertEqual(counts.get("running", 0), 0)
        queue.close()


class BackoffTest(unittest.TestCase):
    """Pure-function tests for the exponential backoff schedule -- no
    real sleeping involved."""

    def test_first_retry_uses_base_delay(self):
        self.assertEqual(compute_backoff(1, base=1.0, factor=2.0, max_delay=1000), 1.0)

    def test_delay_doubles_each_retry(self):
        self.assertEqual(compute_backoff(1, base=2.0, factor=2.0, max_delay=1000), 2.0)
        self.assertEqual(compute_backoff(2, base=2.0, factor=2.0, max_delay=1000), 4.0)
        self.assertEqual(compute_backoff(3, base=2.0, factor=2.0, max_delay=1000), 8.0)
        self.assertEqual(compute_backoff(4, base=2.0, factor=2.0, max_delay=1000), 16.0)

    def test_delay_is_capped_at_max_delay(self):
        self.assertEqual(compute_backoff(10, base=1.0, factor=2.0, max_delay=5.0), 5.0)

    def test_retry_count_below_one_is_treated_as_one(self):
        self.assertEqual(compute_backoff(0, base=1.0, factor=2.0, max_delay=1000), 1.0)


if __name__ == "__main__":
    unittest.main()
