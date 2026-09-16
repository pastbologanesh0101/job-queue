"""SQLite-backed job queue.

Jobs are persisted to a SQLite database so they survive a process
restart. Claiming a job for processing is done inside an immediate
(write-locking) SQLite transaction, so that when multiple worker
threads (or processes) share the same database file, exactly one of
them succeeds in transitioning a given job from ``pending`` to
``running`` -- no job is ever processed twice.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

PENDING = "pending"
RUNNING = "running"
DONE = "done"
FAILED = "failed"


@dataclass
class Job:
    id: str
    job_type: str
    payload: Any
    status: str
    retry_count: int
    created_at: float
    available_at: float
    result: Any = None
    error: Optional[str] = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        return cls(
            id=row["id"],
            job_type=row["job_type"],
            payload=json.loads(row["payload"]),
            status=row["status"],
            retry_count=row["retry_count"],
            created_at=row["created_at"],
            available_at=row["available_at"],
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error=row["error"],
        )


class Queue:
    """A SQLite-backed job queue.

    Safe to share across threads: each thread gets its own SQLite
    connection (SQLite connections are not thread-safe by default),
    but they all point at the same on-disk database file, and the
    ``claim_job`` transaction uses ``BEGIN IMMEDIATE`` to serialize
    claims across connections/threads/processes.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._init_db()

    # -- connection management -------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        with self._init_lock:
            conn = self._connect()
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    available_at REAL NOT NULL,
                    result TEXT,
                    error TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_claim "
                "ON jobs (status, available_at, created_at)"
            )

    # -- public API --------------------------------------------------------

    def enqueue(self, job_type: str, payload: Any, available_at: Optional[float] = None) -> str:
        """Add a new job to the queue and return its id."""
        job_id = str(uuid.uuid4())
        now = time.time()
        conn = self._connect()
        conn.execute(
            "INSERT INTO jobs "
            "(id, job_type, payload, status, retry_count, created_at, available_at) "
            "VALUES (?, ?, ?, ?, 0, ?, ?)",
            (
                job_id,
                job_type,
                json.dumps(payload),
                PENDING,
                now,
                available_at if available_at is not None else now,
            ),
        )
        return job_id

    def claim_job(self) -> Optional[Job]:
        """Atomically claim the oldest available pending job.

        Returns the claimed job (now with status ``running``) or
        ``None`` if there is nothing to claim right now.
        """
        conn = self._connect()
        now = time.time()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM jobs "
                "WHERE status = ? AND available_at <= ? "
                "ORDER BY created_at LIMIT 1",
                (PENDING, now),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute(
                "UPDATE jobs SET status = ? WHERE id = ? AND status = ?",
                (RUNNING, row["id"], PENDING),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return self.get_job(row["id"])

    def mark_done(self, job_id: str, result: Any) -> None:
        conn = self._connect()
        conn.execute(
            "UPDATE jobs SET status = ?, result = ?, error = NULL WHERE id = ?",
            (DONE, json.dumps(result), job_id),
        )

    def mark_retry(self, job_id: str, error: str, retry_count: int, available_at: float) -> None:
        """Mark a job as failed-but-retryable: back to pending, with a
        bumped retry_count and a future available_at."""
        conn = self._connect()
        conn.execute(
            "UPDATE jobs SET status = ?, error = ?, retry_count = ?, available_at = ? "
            "WHERE id = ?",
            (PENDING, error, retry_count, available_at, job_id),
        )

    def mark_failed(self, job_id: str, error: str, retry_count: int) -> None:
        """Mark a job as permanently failed (retries exhausted)."""
        conn = self._connect()
        conn.execute(
            "UPDATE jobs SET status = ?, error = ?, retry_count = ? WHERE id = ?",
            (FAILED, error, retry_count, job_id),
        )

    def get_job(self, job_id: str) -> Optional[Job]:
        conn = self._connect()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return Job.from_row(row)

    def list_jobs(self, status: Optional[str] = None) -> list[Job]:
        conn = self._connect()
        if status is None:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY created_at", (status,)
            ).fetchall()
        return [Job.from_row(r) for r in rows]

    def counts(self) -> dict[str, int]:
        conn = self._connect()
        rows = conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
        return {row["status"]: row["n"] for row in rows}

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
