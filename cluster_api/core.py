"""Abstract Executor base class."""

from __future__ import annotations

import abc
import asyncio
import logging
import os
import re
import secrets
import string
from datetime import datetime, timezone
from typing import Any

from .config import ClusterConfig
from .exceptions import ClusterAPIError, CommandFailedError, CommandTimeoutError, SubmitError
from ._types import ArrayElement, JobRecord, JobStatus, ResourceSpec

logger = logging.getLogger(__name__)

_ARRAY_ELEMENT_RE = re.compile(r"^(.+)\[(\d+)\]$")


class Executor(abc.ABC):
    """Abstract base for cluster job executors."""

    submit_command: str
    cancel_command: str
    status_command: str
    job_id_regexp: str = r"(?P<job_id>\d+)"

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self._jobs: dict[str, JobRecord] = {}
        if config.job_name_prefix:
            self._prefix = config.job_name_prefix
        else:
            # Generate a random prefix so concurrent users/sessions don't
            # see each other's jobs when polling by name.
            alphabet = string.ascii_lowercase + string.digits
            self._prefix = "".join(secrets.choice(alphabet) for _ in range(5))

    # --- Submission ---

    async def submit(
        self,
        command: str,
        name: str,
        resources: ResourceSpec | None = None,
        prologue: list[str] | None = None,
        epilogue: list[str] | None = None,
        env: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> JobRecord:
        """Submit a job to the scheduler."""
        resources = resources or ResourceSpec()
        full_name = f"{self._prefix}-{name}"

        job_id, script_path = await self._submit_job(
            command, full_name, resources, prologue, epilogue, env,
            cwd=resources.work_dir,
        )

        record = JobRecord(
            job_id=job_id,
            name=full_name,
            command=command,
            status=JobStatus.PENDING,
            resources=resources,
            script_path=script_path,
            metadata=metadata or {},
            _last_seen=datetime.now(timezone.utc),
        )
        self._jobs[job_id] = record
        logger.info("Submitted job %s (%s)", job_id, full_name)
        return record

    async def submit_array(
        self,
        command: str,
        name: str,
        array_range: tuple[int, int],
        resources: ResourceSpec | None = None,
        prologue: list[str] | None = None,
        epilogue: list[str] | None = None,
        env: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        max_concurrent: int | None = None,
    ) -> JobRecord:
        """Submit a job array to the scheduler."""
        resources = resources or ResourceSpec()
        full_name = f"{self._prefix}-{name}"

        job_id, script_path = await self._submit_array_job(
            command, full_name, array_range, resources, prologue, epilogue,
            env, max_concurrent, cwd=resources.work_dir,
        )

        meta = {**(metadata or {}), "array_range": array_range}
        if max_concurrent is not None:
            meta["max_concurrent"] = max_concurrent

        record = JobRecord(
            job_id=job_id,
            name=full_name,
            command=command,
            status=JobStatus.PENDING,
            resources=resources,
            script_path=script_path,
            metadata=meta,
            _last_seen=datetime.now(timezone.utc),
        )
        self._jobs[job_id] = record
        logger.info(
            "Submitted array job %s (%s[%d-%d])",
            job_id, full_name, array_range[0], array_range[1],
        )
        return record

    @abc.abstractmethod
    async def _submit_job(
        self,
        command: str,
        name: str,
        resources: ResourceSpec,
        prologue: list[str] | None = None,
        epilogue: list[str] | None = None,
        env: dict[str, str] | None = None,
        *,
        cwd: str | None = None,
    ) -> tuple[str, str | None]:
        """Submit a single job.

        Returns ``(job_id, script_path)`` where *script_path* may be
        ``None`` for executors that don't write scripts to disk.
        """
        ...

    async def _submit_array_job(
        self,
        command: str,
        name: str,
        array_range: tuple[int, int],
        resources: ResourceSpec,
        prologue: list[str] | None = None,
        epilogue: list[str] | None = None,
        env: dict[str, str] | None = None,
        max_concurrent: int | None = None,
        *,
        cwd: str | None = None,
    ) -> tuple[str, str | None]:
        """Submit an array job. Override in subclasses."""
        return await self._submit_job(
            command, name, resources, prologue, epilogue, env, cwd=cwd,
        )

    def _job_id_from_submit_output(self, out: str) -> str:
        """Extract job ID from submission output using regex."""
        match = re.search(self.job_id_regexp, out)
        if not match:
            raise SubmitError(
                f"Could not parse job ID from output: {out!r}"
            )
        return match.group("job_id")

    # --- Cancellation ---

    async def cancel(self, job_id: str) -> None:
        """Cancel a job by ID."""
        await self._call(
            [self.cancel_command, job_id],
            timeout=self.config.command_timeout,
        )
        if job_id in self._jobs:
            self._jobs[job_id].status = JobStatus.KILLED
        logger.info("Cancelled job %s", job_id)

    async def cancel_by_name(self, name_pattern: str) -> None:
        """Cancel jobs by name pattern. Override in subclasses for native support."""
        raise NotImplementedError("cancel_by_name not supported by this executor")

    async def reconnect(self) -> list[JobRecord]:
        """Reconnect to running jobs and resume tracking them.

        Queries the scheduler, discovers existing jobs by name prefix, and
        reconstructs ``JobRecord`` instances so monitoring can resume after
        a process restart.

        Returns:
            List of newly created ``JobRecord`` instances.
        """
        raise NotImplementedError("reconnect not supported by this executor")

    async def cancel_all(self) -> None:
        """Cancel all tracked jobs."""
        to_cancel = [jid for jid, r in self._jobs.items() if not r.is_terminal]
        await asyncio.gather(*(self.cancel(jid) for jid in to_cancel))

    # --- Status polling ---

    @abc.abstractmethod
    def _build_status_args(self) -> list[str]:
        """Build args for the status query command."""
        ...

    @abc.abstractmethod
    def _parse_job_statuses(
        self, output: str
    ) -> dict[str, tuple[JobStatus, dict[str, Any]]]:
        """Parse status command output into {job_id: (status, metadata_dict)}."""
        ...

    async def poll(self) -> dict[str, JobStatus]:
        """Query scheduler, update job records, detect zombies. Returns current statuses."""
        active = [r for r in self._jobs.values() if not r.is_terminal]
        if not active:
            return {jid: r.status for jid, r in self._jobs.items()}

        args = self._build_status_args()
        try:
            out = await self._call(args, timeout=self.config.command_timeout)
        except (ClusterAPIError, OSError) as e:
            logger.warning("Status query failed, skipping poll cycle: %s", e, exc_info=True)
            return {jid: r.status for jid, r in self._jobs.items()}

        statuses = self._parse_job_statuses(out)
        now = datetime.now(timezone.utc)
        array_jobs_updated: set[str] = set()

        for raw_id, (new_status, meta) in statuses.items():
            # Check if this is an array element ID like "12345[1]"
            m = _ARRAY_ELEMENT_RE.match(raw_id)
            if m:
                parent_id, element_index = m.group(1), int(m.group(2))
                record = self._jobs.get(parent_id)
                if record and not record.is_terminal and record.is_array:
                    if element_index not in record.array_elements:
                        record.array_elements[element_index] = ArrayElement(index=element_index)
                    elem = record.array_elements[element_index]
                    elem.status = new_status
                    for key in ("exec_host", "max_mem", "exit_code",
                                "submit_time", "start_time", "finish_time"):
                        if key in meta and meta[key] is not None:
                            setattr(elem, key, meta[key])
                    record._last_seen = now
                    array_jobs_updated.add(parent_id)
            else:
                record = self._jobs.get(raw_id)
                if record and not record.is_terminal:
                    record.status = new_status
                    record._last_seen = now
                    for key in ("exec_host", "max_mem", "exit_code",
                                "submit_time", "start_time", "finish_time"):
                        if key in meta and meta[key] is not None:
                            setattr(record, key, meta[key])

        # Aggregate parent status for array jobs that got element updates
        for parent_id in array_jobs_updated:
            record = self._jobs[parent_id]
            record.status = record.compute_array_status()

        return {jid: r.status for jid, r in self._jobs.items()}

    # --- Subprocess helper ---

    @staticmethod
    async def _call(
        cmd: list[str],
        shell: bool = False,
        timeout: float = 100.0,
        env: dict[str, str] | None = None,
        stdin_data: str | None = None,
    ) -> str:
        """Run a subprocess and return stdout.

        Inspired by dask-jobqueue's _call(), with added timeout support.
        """
        full_env = None
        if env:
            full_env = {**os.environ, **env}

        if shell:
            proc = await asyncio.create_subprocess_shell(
                cmd if isinstance(cmd, str) else " ".join(cmd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=full_env,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=full_env,
                stdin=asyncio.subprocess.PIPE if stdin_data else None,
            )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(stdin_data.encode() if stdin_data else None),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            raise CommandTimeoutError(
                f"Command timed out after {timeout}s: {cmd}"
            )

        out = stdout.decode().strip()
        err = stderr.decode().strip()

        if proc.returncode != 0:
            raise CommandFailedError(
                f"Command failed (exit {proc.returncode}): {cmd}\nstderr: {err}"
            )

        return out

    # --- Properties ---

    def remove_job(self, job_id: str) -> None:
        """Remove a job from tracking."""
        self._jobs.pop(job_id, None)

    @property
    def jobs(self) -> dict[str, JobRecord]:
        """All tracked jobs."""
        return dict(self._jobs)

    @property
    def active_jobs(self) -> dict[str, JobRecord]:
        """Non-terminal jobs."""
        return {jid: r for jid, r in self._jobs.items() if not r.is_terminal}
