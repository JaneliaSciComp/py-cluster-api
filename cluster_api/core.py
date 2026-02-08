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
from pathlib import Path
from typing import Any

from .config import ClusterConfig
from .exceptions import ClusterAPIError, CommandFailedError, CommandTimeoutError, SubmitError
from ._types import JobRecord, JobStatus, ResourceSpec

logger = logging.getLogger(__name__)

_SCRIPT_TEMPLATE = """\
%(shebang)s
%(job_header)s
%(prologue)s
%(command)s
%(epilogue)s
"""


class Executor(abc.ABC):
    """Abstract base for cluster job executors."""

    submit_command: str
    cancel_command: str
    status_command: str
    job_id_regexp: str = r"(?P<job_id>\d+)"
    directive_prefix: str = ""

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self._jobs: dict[str, JobRecord] = {}
        self._script_counter = 0
        self._log_dir = Path(config.log_directory).expanduser()
        self._log_dir.mkdir(parents=True, exist_ok=True)
        if config.job_name_prefix:
            self._prefix = config.job_name_prefix
        else:
            # Generate a random prefix so concurrent users/sessions don't
            # see each other's jobs when polling by name.
            alphabet = string.ascii_lowercase + string.digits
            self._prefix = "".join(secrets.choice(alphabet) for _ in range(5))

    # --- Script rendering ---

    def render_script(
        self,
        command: str,
        name: str,
        resources: ResourceSpec | None = None,
        prologue: list[str] | None = None,
        epilogue: list[str] | None = None,
    ) -> str:
        """Render a job script from the template."""
        header_lines = self.build_header(name, resources)
        # Filter via directives_skip
        skip = set(self.config.directives_skip)
        if skip:
            header_lines = [
                line
                for line in header_lines
                if not any(s in line for s in skip)
            ]
        # Extend with extra_directives
        header_lines.extend(self.config.extra_directives)

        all_prologue = list(self.config.script_prologue)
        if prologue:
            all_prologue.extend(prologue)

        all_epilogue = list(self.config.script_epilogue)
        if epilogue:
            all_epilogue.extend(epilogue)

        return _SCRIPT_TEMPLATE % {
            "shebang": self.config.shebang,
            "job_header": "\n".join(header_lines),
            "prologue": "\n".join(all_prologue),
            "command": command,
            "epilogue": "\n".join(all_epilogue),
        }

    @abc.abstractmethod
    def build_header(
        self, name: str, resources: ResourceSpec | None = None
    ) -> list[str]:
        """Build scheduler-specific directive lines."""
        ...

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
        full_name = f"{self._prefix}-{name}"
        script = self.render_script(command, full_name, resources, prologue, epilogue)
        script_path = self._write_script(script, full_name)

        job_id = await self._submit_job(script_path, full_name, env)

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
        full_name = f"{self._prefix}-{name}"
        script = self.render_script(command, full_name, resources, prologue, epilogue)
        script_path = self._write_script(script, full_name)

        job_id = await self._submit_array_job(
            script_path, full_name, array_range, env, max_concurrent
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

    async def _submit_job(
        self,
        script_path: str,
        name: str,
        env: dict[str, str] | None = None,
    ) -> str:
        """Submit a script and return the job ID. Override for stdin submission."""
        out = await self._call(
            [self.submit_command, script_path],
            env=env,
            timeout=self.config.command_timeout,
        )
        return self._job_id_from_submit_output(out)

    async def _submit_array_job(
        self,
        script_path: str,
        name: str,
        array_range: tuple[int, int],
        env: dict[str, str] | None = None,
        max_concurrent: int | None = None,
    ) -> str:
        """Submit an array job. Override in subclasses."""
        return await self._submit_job(script_path, name, env)

    def _write_script(self, script_content: str, name: str) -> str:
        """Write job script to log directory and return its path."""
        safe_name = re.sub(r"[^\w\-.]", "_", name)
        self._script_counter += 1
        script_path = self._log_dir / f"{safe_name}.{self._script_counter}.sh"
        script_path.write_text(script_content)
        script_path.chmod(0o755)
        return str(script_path)

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
        except (ClusterAPIError, OSError):
            logger.warning("Status query failed, skipping poll cycle")
            return {jid: r.status for jid, r in self._jobs.items()}

        statuses = self._parse_job_statuses(out)
        now = datetime.now(timezone.utc)

        for job_id, record in self._jobs.items():
            if record.is_terminal:
                continue
            if job_id in statuses:
                new_status, meta = statuses[job_id]
                record.status = new_status
                record._last_seen = now
                # Update rich metadata
                for key in ("exec_host", "max_mem", "exit_code",
                            "submit_time", "start_time", "finish_time"):
                    if key in meta and meta[key] is not None:
                        setattr(record, key, meta[key])

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
