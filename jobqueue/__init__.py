"""Job Queue System

A small, dependency-free, SQLite-backed background job queue for Python.
"""

from .queue import Queue
from .worker import Worker, WorkerPool, compute_backoff

__all__ = ["Queue", "Worker", "WorkerPool", "compute_backoff"]

__version__ = "1.0.0"
