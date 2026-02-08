"""Shared types for cluster_api."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable


class JobStatus(enum.Enum):
    """Status of a cluster job."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    KILLED = "killed"
    UNKNOWN = "unknown"


class JobExitCondition(enum.Enum):
    """Conditions for callback dispatch."""

    SUCCESS = "success"
    FAILURE = "failure"
    KILLED = "killed"
    ANY = "any"


_TERMINAL_STATUSES = frozenset({JobStatus.DONE, JobStatus.FAILED, JobStatus.KILLED})


@dataclass
class ResourceSpec:
    """Resource requirements for a job."""

    cpus: int | None = None
    memory: str | None = None
    walltime: str | None = None
    queue: str | None = None
    account: str | None = None
    work_dir: str | None = None
    cluster_options: list[str] | None = None


@dataclass
class JobRecord:
    """Tracks a submitted job and its metadata."""

    job_id: str
    name: str
    command: str
    status: JobStatus = JobStatus.PENDING
    exit_code: int | None = None
    resources: ResourceSpec | None = None
    script_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    exec_host: str | None = None
    max_mem: str | None = None
    submit_time: datetime | None = None
    start_time: datetime | None = None
    finish_time: datetime | None = None
    _callbacks: list[tuple[JobExitCondition, Callable]] = field(
        default_factory=list, repr=False
    )
    _last_seen: datetime | None = field(default=None, repr=False)

    def on_exit(self, callback: Callable, condition: JobExitCondition = JobExitCondition.ANY) -> JobRecord:
        """Register a callback for the given exit condition. Returns self for chaining."""
        self._callbacks.append((condition, callback))
        return self

    def on_success(self, callback: Callable) -> JobRecord:
        """Register a callback for successful completion."""
        return self.on_exit(callback, JobExitCondition.SUCCESS)

    def on_failure(self, callback: Callable) -> JobRecord:
        """Register a callback for failure."""
        return self.on_exit(callback, JobExitCondition.FAILURE)

    @property
    def is_terminal(self) -> bool:
        """Whether the job has reached a terminal state."""
        return self.status in _TERMINAL_STATUSES
