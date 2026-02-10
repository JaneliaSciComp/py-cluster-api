"""Shared types for cluster_api."""

from __future__ import annotations

import enum
import os
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
    gpus: int | None = None
    memory: str | None = None
    walltime: str | None = None
    queue: str | None = None
    account: str | None = None
    work_dir: str = field(default_factory=os.getcwd)
    stdout_path: str | None = None
    stderr_path: str | None = None
    cluster_options: list[str] | None = None


@dataclass
class ArrayElement:
    """Tracks a single element of a job array."""

    index: int
    status: JobStatus = JobStatus.PENDING
    exit_code: int | None = None
    exec_host: str | None = None
    max_mem: str | None = None
    submit_time: datetime | None = None
    start_time: datetime | None = None
    finish_time: datetime | None = None


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
    array_elements: dict[int, ArrayElement] = field(default_factory=dict)
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

    @property
    def is_array(self) -> bool:
        """Whether this is an array job."""
        return "array_range" in self.metadata

    @property
    def element_count(self) -> int:
        """Total number of expected array elements (0 if not an array)."""
        if not self.is_array:
            return 0
        start, end = self.metadata["array_range"]
        return end - start + 1

    @property
    def completed_elements(self) -> int:
        """Number of elements in a terminal state."""
        return sum(1 for e in self.array_elements.values() if e.status in _TERMINAL_STATUSES)

    @property
    def failed_element_indices(self) -> list[int]:
        """Indices of failed or killed elements."""
        return [
            e.index for e in self.array_elements.values()
            if e.status in {JobStatus.FAILED, JobStatus.KILLED}
        ]

    def compute_array_status(self) -> JobStatus:
        """Derive overall status from element statuses.

        Conservative: only returns a terminal status when ALL expected
        elements have been seen and are themselves terminal.
        """
        if not self.array_elements:
            return self.status

        statuses = {e.status for e in self.array_elements.values()}

        # Any non-terminal element → still in progress
        if statuses & {JobStatus.RUNNING, JobStatus.PENDING, JobStatus.UNKNOWN}:
            return JobStatus.RUNNING

        # All seen elements are terminal — but have we seen them all?
        if len(self.array_elements) < self.element_count:
            return JobStatus.RUNNING

        # All expected elements accounted for and terminal
        if JobStatus.KILLED in statuses:
            return JobStatus.KILLED
        if JobStatus.FAILED in statuses:
            return JobStatus.FAILED
        return JobStatus.DONE
