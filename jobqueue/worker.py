"""Worker(s) that poll a Queue and dispatch jobs to registered handlers."""

from __future__ import annotations

import threading
import time
import traceback
from typing import Callable, Optional

from .queue import Queue

# A handler receives a dict describing the job:
#   {"id": str, "job_type": str, "payload": Any, "retry_count": int}
# and returns a JSON-serializable result, or raises an exception on failure.
Handler = Callable[[dict], object]


def compute_backoff(
    retry_count: int,
    base: float = 1.0,
    factor: float = 2.0,
    max_delay: float = 60.0,
) -> float:
    """Compute the exponential backoff delay (in seconds) before the
    ``retry_count``-th retry of a job.

    Pure function -- no sleeping, no I/O -- so it can be unit tested
    directly. ``retry_count`` is expected to be >= 1 (the number of
    the attempt that just failed).

    delay = base * factor ** (retry_count - 1), capped at max_delay.
    """
    if retry_count < 1:
        retry_count = 1
    delay = base * (factor ** (retry_count - 1))
    return min(delay, max_delay)


class Worker:
    """A single worker that repeatedly claims and processes jobs from a Queue."""

    def __init__(
        self,
        queue: Queue,
        handlers: dict[str, Handler],
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_max: float = 60.0,
        poll_interval: float = 0.05,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        name: Optional[str] = None,
        on_event: Optional[Callable[[str, dict], None]] = None,
    ):
        self.queue = queue
        self.handlers = handlers
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_factor = backoff_factor
        self.backoff_max = backoff_max
        self.poll_interval = poll_interval
        self.clock = clock
        self.sleep = sleep
        self.name = name or f"worker-{id(self):x}"
        self.on_event = on_event or (lambda event, data: None)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> bool:
        """Claim and process a single job, if one is available.

        Returns True if a job was claimed (and processed), False if
        the queue had nothing available right now.
        """
        job = self.queue.claim_job()
        if job is None:
            return False
        self._process(job)
        return True

    def run(self) -> None:
        """Loop claiming and processing jobs until stop() is called."""
        while not self._stop.is_set():
            processed = self.run_once()
            if not processed:
                self.sleep(self.poll_interval)

    def run_until_empty(self, timeout: Optional[float] = None) -> None:
        """Process jobs until there is nothing pending/running left, or
        until timeout (seconds) elapses. Useful for demos/tests that
        want a deterministic end without a background thread."""
        start = self.clock()
        while True:
            processed = self.run_once()
            if not processed:
                counts = self.queue.counts()
                remaining = counts.get("pending", 0) + counts.get("running", 0)
                if remaining == 0:
                    return
                self.sleep(self.poll_interval)
            if timeout is not None and (self.clock() - start) > timeout:
                return

    def _process(self, job) -> None:
        handler = self.handlers.get(job.job_type)
        context = {
            "id": job.id,
            "job_type": job.job_type,
            "payload": job.payload,
            "retry_count": job.retry_count,
        }
        self.on_event("started", context)
        try:
            if handler is None:
                raise LookupError(f"no handler registered for job_type {job.job_type!r}")
            result = handler(context)
        except Exception as exc:  # noqa: BLE001 - we want to catch anything a handler raises
            self._handle_failure(job, exc)
        else:
            self.queue.mark_done(job.id, result)
            self.on_event("done", {**context, "result": result})

    def _handle_failure(self, job, exc: Exception) -> None:
        error_text = f"{exc.__class__.__name__}: {exc}"
        retry_count = job.retry_count + 1
        if retry_count > self.max_retries:
            self.queue.mark_failed(job.id, error_text, retry_count)
            self.on_event(
                "failed",
                {
                    "id": job.id,
                    "job_type": job.job_type,
                    "payload": job.payload,
                    "retry_count": retry_count,
                    "error": error_text,
                },
            )
        else:
            delay = compute_backoff(
                retry_count, self.backoff_base, self.backoff_factor, self.backoff_max
            )
            available_at = self.clock() + delay
            self.queue.mark_retry(job.id, error_text, retry_count, available_at)
            self.on_event(
                "retry",
                {
                    "id": job.id,
                    "job_type": job.job_type,
                    "payload": job.payload,
                    "retry_count": retry_count,
                    "error": error_text,
                    "delay": delay,
                },
            )


class WorkerPool:
    """Runs several Worker instances, each on its own thread, against the
    same Queue -- this is how you get concurrent processing without
    double-processing any single job (the Queue's claim_job is what
    guarantees that)."""

    def __init__(self, queue: Queue, handlers: dict[str, Handler], num_workers: int = 4, **worker_kwargs):
        self.queue = queue
        self.workers = [
            Worker(queue, handlers, name=f"worker-{i}", **worker_kwargs) for i in range(num_workers)
        ]
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for worker in self.workers:
            t = threading.Thread(target=worker.run, name=worker.name, daemon=True)
            self._threads.append(t)
            t.start()

    def stop(self, join: bool = True, timeout: Optional[float] = 5.0) -> None:
        for worker in self.workers:
            worker.stop()
        if join:
            for t in self._threads:
                t.join(timeout=timeout)
        self._threads = []

    def __enter__(self) -> "WorkerPool":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()
