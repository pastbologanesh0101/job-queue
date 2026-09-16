#!/usr/bin/env python3
"""Demo: enqueue a handful of jobs (one of which deliberately fails a
couple of times before succeeding) and process them with a pool of
concurrent worker threads pulling from the same SQLite-backed queue.

Run with:
    python demo.py
"""

from __future__ import annotations

import os
import tempfile
import threading
import time

from jobqueue import Queue, WorkerPool

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def on_event(event: str, data: dict) -> None:
    job_id = data["id"][:8]
    job_type = data["job_type"]
    if event == "started":
        log(f"  [started] {job_type} ({job_id}) payload={data['payload']}")
    elif event == "done":
        log(f"  [done]    {job_type} ({job_id}) -> {data['result']}")
    elif event == "retry":
        log(
            f"  [retry]   {job_type} ({job_id}) attempt {data['retry_count']} failed "
            f"({data['error']}); retrying in {data['delay']:.2f}s"
        )
    elif event == "failed":
        log(
            f"  [FAILED]  {job_type} ({job_id}) permanently failed after "
            f"{data['retry_count']} attempts: {data['error']}"
        )


def handle_add(job: dict) -> int:
    a, b = job["payload"]["a"], job["payload"]["b"]
    time.sleep(0.05)
    return a + b


def handle_flaky(job: dict) -> str:
    """Fails on the first two attempts, then succeeds -- demonstrates
    retry-with-backoff."""
    if job["retry_count"] < 2:
        raise RuntimeError("simulated transient failure")
    return "recovered after retries"


def handle_always_fails(job: dict) -> None:
    raise ValueError(f"cannot process {job['payload']!r}: this job type is broken")


HANDLERS = {
    "add": handle_add,
    "flaky": handle_flaky,
    "always_fails": handle_always_fails,
}


def main() -> None:
    db_path = os.path.join(tempfile.gettempdir(), "job_queue_demo.sqlite3")
    if os.path.exists(db_path):
        os.remove(db_path)

    queue = Queue(db_path)

    log(f"Using database: {db_path}")
    log("Enqueuing jobs...")
    job_ids = []
    job_ids.append(queue.enqueue("add", {"a": 2, "b": 3}))
    job_ids.append(queue.enqueue("add", {"a": 10, "b": 32}))
    job_ids.append(queue.enqueue("flaky", {"note": "will fail twice then succeed"}))
    job_ids.append(queue.enqueue("always_fails", {"note": "will exhaust retries"}))
    job_ids.append(queue.enqueue("add", {"a": -5, "b": 5}))

    log(f"Starting a pool of 3 workers (max_retries=3, backoff base=0.2s, factor=2)...")
    pool = WorkerPool(
        queue,
        HANDLERS,
        num_workers=3,
        max_retries=3,
        backoff_base=0.2,
        backoff_factor=2.0,
        backoff_max=2.0,
        poll_interval=0.02,
        on_event=on_event,
    )
    pool.start()

    # Wait until every job has settled into a terminal state (done/failed).
    deadline = time.time() + 15
    while time.time() < deadline:
        counts = queue.counts()
        if counts.get("pending", 0) == 0 and counts.get("running", 0) == 0:
            break
        time.sleep(0.05)

    pool.stop()

    log("\nFinal job states:")
    for job_id in job_ids:
        job = queue.get_job(job_id)
        log(
            f"  {job.job_type:<14} {job.id[:8]}  status={job.status:<7} "
            f"retries={job.retry_count}  result={job.result}  error={job.error}"
        )

    counts = queue.counts()
    log(f"\nSummary: {counts}")
    queue.close()


if __name__ == "__main__":
    main()
