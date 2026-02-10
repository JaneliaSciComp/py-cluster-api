"""Local subprocess executor for testing without a real scheduler."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .._types import JobStatus, ResourceSpec
from ..config import ClusterConfig
from ..core import Executor
from ..script import render_script, write_script

logger = logging.getLogger(__name__)


class LocalExecutor(Executor):
    """Runs jobs as local bash subprocesses. Useful for testing."""

    submit_command = "bash"
    cancel_command = "kill"
    status_command = "ps"
    directive_prefix = "# LOCAL"

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__(config)
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._next_id = 1
        self._script_counter = 0

    def build_header(
        self, name: str, resources: ResourceSpec | None = None
    ) -> list[str]:
        """Local executor doesn't need scheduler directives."""
        return [f"# LOCAL Job: {name}"]

    async def _submit_job(
        self,
        command: str,
        name: str,
        resources: ResourceSpec | None = None,
        prologue: list[str] | None = None,
        epilogue: list[str] | None = None,
        env: dict[str, str] | None = None,
        *,
        cwd: str | None = None,
    ) -> tuple[str, str | None]:
        """Render script, write to disk, run as a background subprocess."""
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        self._script_counter += 1
        script_path = write_script(self._log_dir, script, name, self._script_counter)

        full_env = {**os.environ, **(env or {})}

        proc = await asyncio.create_subprocess_exec(
            "bash", script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=full_env,
            cwd=cwd,
        )

        job_id = str(self._next_id)
        self._next_id += 1
        self._processes[job_id] = proc
        return job_id, script_path

    def _build_status_args(self) -> list[str]:
        # Not used for local executor; poll() is overridden
        return []

    def _parse_job_statuses(
        self, output: str
    ) -> dict[str, tuple[JobStatus, dict[str, Any]]]:
        # Not used for local executor; poll() is overridden
        return {}

    async def poll(self) -> dict[str, JobStatus]:
        """Check subprocess return codes."""
        for job_id, record in self._jobs.items():
            if record.is_terminal:
                continue

            proc = self._processes.get(job_id)
            if proc is None:
                continue

            if proc.returncode is not None:
                # Process finished — capture output to log files
                await self._write_output_files(record.name, proc, record.resources)

                now = datetime.now(timezone.utc)
                record.finish_time = now
                record._last_seen = now
                if proc.returncode == 0:
                    record.status = JobStatus.DONE
                    record.exit_code = 0
                else:
                    record.status = JobStatus.FAILED
                    record.exit_code = proc.returncode
            else:
                record.status = JobStatus.RUNNING
                record._last_seen = datetime.now(timezone.utc)

        return {jid: r.status for jid, r in self._jobs.items()}

    async def cancel(self, job_id: str) -> None:
        """Terminate a local subprocess."""
        proc = self._processes.get(job_id)
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()

        if job_id in self._jobs:
            self._jobs[job_id].status = JobStatus.KILLED
        logger.info("Cancelled local job %s", job_id)

    async def _write_output_files(
        self, job_name: str, proc: asyncio.subprocess.Process,
        resources: ResourceSpec | None = None,
    ) -> None:
        """Write captured stdout/stderr to log files.

        Uses per-job paths from ResourceSpec if set, otherwise falls back
        to the global log directory.
        """
        stdout_data, stderr_data = await proc.communicate()
        out_path = Path(resources.stdout_path) if resources and resources.stdout_path else self._log_dir / f"{job_name}.out"
        err_path = Path(resources.stderr_path) if resources and resources.stderr_path else self._log_dir / f"{job_name}.err"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(stdout_data or b"")
        err_path.write_bytes(stderr_data or b"")
