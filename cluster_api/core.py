"""Abstract Executor base class."""

from __future__ import annotations

import abc
import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from .config import ClusterConfig
from .exceptions import ClusterAPIError, CommandFailedError, CommandTimeoutError, SubmitError
from ._types import ArrayElement, JobRecord, JobStatus, ResourceSpec

logger = logging.getLogger(__name__)

# Check for array element IDs like "12345[1]"
_ARRAY_ELEMENT_RE = re.compile(r"^(.+)\[(\d+)\]$")

# Check for job names that are unsafe in scheduler job names
_UNSAFE_NAME_RE = re.compile(r"[^\w\-.]")


def _sanitize_job_name(name: str) -> str:
    """Replace characters that are unsafe in scheduler job names."""
    return _UNSAFE_NAME_RE.sub("-", name)


class Executor(abc.ABC):
    """Abstract base for cluster job executors.

    Lifecycle:
        1. **Construct** — instantiate with a ``ClusterConfig``.
        2. **Submit** — call :meth:`submit` or :meth:`submit_array` to enqueue
           jobs.  Each returns a :class:`JobRecord` tracked in-process.
        3. **Poll** — call :meth:`poll` (usually via :class:`~cluster_api.monitor.Monitor`)
           to query the scheduler and update every tracked ``JobRecord``.
        4. **Cancel** — call :meth:`cancel`, :meth:`cancel_all`, or
           :meth:`cancel_by_name` to kill running jobs.

    Subclass requirements:
        Must implement:
            - :meth:`_submit_job` — run the scheduler submit command.
            - :meth:`_build_status_args` — build the CLI args for a status query.
            - :meth:`_parse_job_statuses` — parse status output into per-job dicts.

        May override:
            - :meth:`_submit_array_job` — array submission (default delegates
              to ``_submit_job``).
            - :meth:`_cancel_job` — cancel a single job.
            - :meth:`cancel_by_name` — cancel by name pattern.
            - :meth:`reconnect` — rediscover running jobs after restart.

    Class attributes:
        submit_command: CLI executable used for submission (e.g. ``"bsub"``).
        cancel_command: CLI executable used for cancellation (e.g. ``"bkill"``).
        status_command: CLI executable used for status queries (e.g. ``"bjobs"``).
        job_id_regexp: Regex with a ``job_id`` named group, applied to submit
            output to extract the job ID.
    """

    submit_command: str
    cancel_command: str
    status_command: str
    job_id_regexp: str = r"(?P<job_id>\d+)"

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self._jobs: dict[str, JobRecord] = {}
        self._prefix = config.job_name_prefix  # None if not configured

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
        full_name = _sanitize_job_name(f"{self._prefix}-{name}" if self._prefix else name)

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
        full_name = _sanitize_job_name(f"{self._prefix}-{name}" if self._prefix else name)

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

    async def cancel(self, job_id: str, *, done: bool = False) -> None:
        """Cancel a job by ID.

        Args:
            job_id: The job ID to cancel.
            done: If True, mark the job as DONE instead of KILLED.
                  Subclasses may translate this into scheduler-specific flags.
        """
        await self._cancel_job(job_id, done=done)
        if job_id in self._jobs:
            self._jobs[job_id].status = JobStatus.DONE if done else JobStatus.KILLED
        logger.info("Cancelled job %s (done=%s)", job_id, done)

    async def _cancel_job(self, job_id: str, *, done: bool = False) -> None:
        """Run the scheduler cancel command. Must be implemented by subclasses."""
        raise NotImplementedError("cancel is not supported by this executor")

    async def cancel_by_name(self, name_pattern: str) -> None:
        """Cancel jobs by name pattern. Override in subclasses for native support."""
        raise NotImplementedError("cancel_by_name is not supported by this executor")

    async def reconnect(self) -> list[JobRecord]:
        """Reconnect to running jobs and resume tracking them.

        Queries the scheduler, discovers existing jobs by name prefix, and
        reconstructs ``JobRecord`` instances so monitoring can resume after
        a process restart.

        Returns:
            List of newly created ``JobRecord`` instances.
        """
        raise NotImplementedError("reconnect is not supported by this executor")

    async def cancel_all(self, *, done: bool = False) -> None:
        """Cancel all tracked jobs."""
        to_cancel = [jid for jid, r in self._jobs.items() if not r.is_terminal]
        results = await asyncio.gather(
            *(self.cancel(jid, done=done) for jid in to_cancel),
            return_exceptions=True,
        )
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            logger.warning("cancel_all: %d/%d cancellations failed", len(errors), len(to_cancel))
            for err in errors:
                logger.debug("cancel_all error: %s", err)

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
        except CommandFailedError as e:
            # bjobs exits non-zero when some job IDs are gone, but still
            # writes valid JSON for the found jobs to stdout.  Try to
            # parse whatever we got before giving up.
            if e.stdout:
                logger.debug("Status query returned non-zero but produced output, parsing partial results")
                out = e.stdout
            else:
                logger.warning("Status query failed, skipping poll cycle: %s", e)
                return {jid: r.status for jid, r in self._jobs.items()}
        except (ClusterAPIError, OSError) as e:
            logger.warning("Status query failed, skipping poll cycle: %s", e)
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
        timeout: float = 100.0,
        env: dict[str, str] | None = None,
        stdin_file: str | None = None,
    ) -> str:
        """Run a subprocess and return stdout.

        Inspired by dask-jobqueue's _call(), with added timeout support.
        """
        full_env = None
        if env:
            full_env = {**os.environ, **env}

        cmd_str = " ".join(cmd)
        if stdin_file:
            logger.debug("Running: %s < %s", cmd_str, stdin_file)
        else:
            logger.debug("Running: %s", cmd_str)

        stdin_fh = None
        try:
            if stdin_file:
                stdin_fh = open(stdin_file)  # noqa: SIM115
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=stdin_fh,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=full_env,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise CommandTimeoutError(
                    f"Command timed out after {timeout}s: {cmd}"
                )
        finally:
            if stdin_fh:
                stdin_fh.close()

        out = stdout.decode().strip()
        err = stderr.decode().strip()

        if proc.returncode != 0:
            raise CommandFailedError(
                f"Command failed (exit {proc.returncode}): {cmd}\nstderr: {err}",
                stdout=out,
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
