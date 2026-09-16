# Job Queue System

A small, dependency-free background job queue for Python. No Redis, no
RabbitMQ, no broker to run — just a SQLite file. Jobs survive a process
restart, failed jobs are retried with exponential backoff, and multiple
worker threads can safely pull from the same queue without ever
processing the same job twice.

## Why SQLite

SQLite gives us durable, transactional storage for free, with zero
external services to install or configure. The trick that makes this
safe for concurrent workers is `claim_job()`, which runs inside a
`BEGIN IMMEDIATE` transaction:

```sql
BEGIN IMMEDIATE;
SELECT * FROM jobs WHERE status='pending' AND available_at<=now ORDER BY created_at LIMIT 1;
UPDATE jobs SET status='running' WHERE id=? AND status='pending';
COMMIT;
```

`BEGIN IMMEDIATE` grabs a write lock on the database file before the
transaction does anything else. If two threads (or two separate
processes, each with their own connection) race to claim a job, SQLite
simply makes one of them wait until the other's transaction commits —
so the `SELECT ... UPDATE` pair is atomic across the whole queue, not
just within one connection. Whichever transaction commits first
"wins" the job; every later claimer either sees it as no-longer
`pending`, or picks a different row. That's what guarantees **exactly
one worker processes a given job.**

## Architecture

```
jobqueue/
  queue.py    Queue  -- SQLite-backed storage: enqueue, claim, mark done/retry/failed
  worker.py   Worker -- polls the Queue, dispatches to a handler by job_type,
              WorkerPool -- runs several Workers on their own threads
              compute_backoff -- pure function: retry_count -> delay (seconds)
demo.py       End-to-end example: enqueue a few jobs, run a worker pool, print progress
tests/        Unit tests (stdlib unittest)
```

**`Queue`** stores each job as a row: `id`, `job_type`, `payload` (JSON),
`status` (`pending` / `running` / `done` / `failed`), `retry_count`,
`created_at`, `available_at` (when it becomes eligible to claim again —
used for backoff), `result` (JSON), `error`.

**`Worker`** calls `queue.claim_job()` in a loop. On success, it looks
up the handler registered for the job's `job_type` and calls it with a
dict `{"id", "job_type", "payload", "retry_count"}`. The handler's
return value (must be JSON-serializable) is stored as the job's
`result`; if the handler raises, the worker either reschedules the job
for a later retry (with an exponential backoff delay) or, once
`max_retries` is exceeded, marks it permanently `failed` with the
error message.

**`WorkerPool`** just spins up N `Worker`s, each on its own daemon
thread, all pointed at the same `Queue` (and hence the same SQLite
file). Because `claim_job()` is what enforces "exactly once," adding
more workers is purely a concurrency knob — it never risks
double-processing.

**Backoff** is computed by a pure function so it's trivially unit
testable without any real sleeping:

```python
def compute_backoff(retry_count, base=1.0, factor=2.0, max_delay=60.0):
    delay = base * (factor ** (retry_count - 1))
    return min(delay, max_delay)
```

`Worker` also accepts injectable `clock` and `sleep` callables
(defaulting to `time.time` / `time.sleep`), so tests can fully control
time without waiting on real backoff delays.

## Running the demo

```bash
python demo.py
```

This enqueues 5 jobs — three simple `add` jobs, one `flaky` job that
fails on its first two attempts and succeeds on the third, and one
`always_fails` job that exhausts its retries — then processes them
with a pool of 3 worker threads. Example output:

```
Using database: /tmp/job_queue_demo.sqlite3
Enqueuing jobs...
Starting a pool of 3 workers (max_retries=3, backoff base=0.2s, factor=2)...
  [started] add (b84a0256) payload={'a': 2, 'b': 3}
  [started] add (4c895c3c) payload={'a': 10, 'b': 32}
  [started] flaky (3d24ee80) payload={'note': 'will fail twice then succeed'}
  [retry]   flaky (3d24ee80) attempt 1 failed (RuntimeError: simulated transient failure); retrying in 0.20s
  [started] always_fails (547c18a5) payload={'note': 'will exhaust retries'}
  [retry]   always_fails (547c18a5) attempt 1 failed (ValueError: cannot process {'note': 'will exhaust retries'}: this job type is broken); retrying in 0.20s
  [started] add (2010feb3) payload={'a': -5, 'b': 5}
  [done]    add (b84a0256) -> 5
  [done]    add (4c895c3c) -> 42
  [done]    add (2010feb3) -> 0
  [started] flaky (3d24ee80) payload={'note': 'will fail twice then succeed'}
  [retry]   flaky (3d24ee80) attempt 2 failed (RuntimeError: simulated transient failure); retrying in 0.40s
  [retry]   always_fails (547c18a5) attempt 2 failed (...); retrying in 0.40s
  [started] flaky (3d24ee80) payload={'note': 'will fail twice then succeed'}
  [done]    flaky (3d24ee80) -> recovered after retries
  [retry]   always_fails (547c18a5) attempt 3 failed (...); retrying in 0.80s
  [FAILED]  always_fails (547c18a5) permanently failed after 4 attempts: ValueError: ...

Final job states:
  add            b84a0256  status=done    retries=0  result=5  error=None
  add            4c895c3c  status=done    retries=0  result=42  error=None
  flaky          3d24ee80  status=done    retries=2  result=recovered after retries  error=None
  always_fails   547c18a5  status=failed  retries=4  result=None  error=ValueError: ...
  add            2010feb3  status=done    retries=0  result=0  error=None

Summary: {'done': 4, 'failed': 1}
```

## Using it in your own code

```python
from jobqueue import Queue, Worker, WorkerPool

queue = Queue("jobs.sqlite3")

def send_email(job):
    to, subject = job["payload"]["to"], job["payload"]["subject"]
    ...  # do the work; raise on failure
    return {"sent_to": to}

queue.enqueue("send_email", {"to": "a@example.com", "subject": "Hi"})

pool = WorkerPool(queue, {"send_email": send_email}, num_workers=4, max_retries=5)
pool.start()
...
pool.stop()
```

## Running the tests

```bash
python -m unittest discover -s tests -v
```

Pure standard library — no `pip install` required. Tests cover:

- enqueue / claim (dequeue) behavior, including `available_at` gating
- exactly-once processing under real concurrent worker threads (a temp
  SQLite file + real `threading.Thread`s)
- retry-then-permanently-fail behavior up to `max_retries`
- the exponential backoff schedule (pure function, no real sleeping)
- job results and errors being persisted and retrievable after the fact
- unknown job types and queue-count bookkeeping

CI (`.github/workflows/tests.yml`) runs the suite on every push and PR
against Python 3.11 and 3.12.

## License

MIT — see [LICENSE](LICENSE).
