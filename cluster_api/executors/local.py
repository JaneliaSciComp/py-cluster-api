"""Local subprocess executor for testing without a real scheduler."""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .._types import ArrayElement, JobStatus, ResourceSpec
from ..config import ClusterConfig
from ..core import Executor
from ..script import render_script, write_script

_ARRAY_ELEMENT_RE = re.compile(r"^(\d+)\[(\d+)\]$")

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
        self._script_counter = itertools.count(1)

    def build_header(
        self, name: str, resources: ResourceSpec | None = None
    ) -> list[str]:
        """Local executor doesn't need scheduler directives."""
        return [f"# LOCAL Job: {name}"]

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
        """Render script, write to disk, run as a background subprocess."""
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))

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
        """Spawn one subprocess per array element with ARRAY_INDEX env var."""
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))

        job_id = str(self._next_id)
        self._next_id += 1

        full_env = {**os.environ, **(env or {})}

        for index in range(array_range[0], array_range[1] + 1):
            element_env = {**full_env, "ARRAY_INDEX": str(index)}
            proc = await asyncio.create_subprocess_exec(
                "bash", script_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=element_env,
                cwd=cwd,
            )
            self._processes[f"{job_id}[{index}]"] = proc

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
        # --- Single jobs ---
        for job_id, record in self._jobs.items():
            if record.is_terminal or record.is_array:
                continue

            proc = self._processes.get(job_id)
            if proc is None:
                continue

            if proc.returncode is not None:
                # Process finished — capture output to log files
                await self._write_output_files(record.name, proc, record.resources or ResourceSpec(), job_id)

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

        # --- Array element processes ---
        array_jobs_updated: set[str] = set()
        for key, proc in self._processes.items():
            m = _ARRAY_ELEMENT_RE.match(key)
            if not m:
                continue
            parent_id, element_index = m.group(1), int(m.group(2))
            record = self._jobs.get(parent_id)
            if record is None or record.is_terminal:
                continue

            if element_index not in record.array_elements:
                record.array_elements[element_index] = ArrayElement(index=element_index)
            elem = record.array_elements[element_index]

            if elem.status in {JobStatus.DONE, JobStatus.FAILED, JobStatus.KILLED}:
                continue

            if proc.returncode is not None:
                resources = record.resources or ResourceSpec()
                await self._write_output_files(record.name, proc, resources, parent_id, element_index=element_index)

                now = datetime.now(timezone.utc)
                elem.finish_time = now
                if proc.returncode == 0:
                    elem.status = JobStatus.DONE
                    elem.exit_code = 0
                else:
                    elem.status = JobStatus.FAILED
                    elem.exit_code = proc.returncode
                array_jobs_updated.add(parent_id)
            else:
                elem.status = JobStatus.RUNNING
                record._last_seen = datetime.now(timezone.utc)
                array_jobs_updated.add(parent_id)

        for parent_id in array_jobs_updated:
            record = self._jobs[parent_id]
            record.status = record.compute_array_status()
            if record.is_terminal:
                record.finish_time = datetime.now(timezone.utc)

        return {jid: r.status for jid, r in self._jobs.items()}

    async def cancel(self, job_id: str) -> None:
        """Terminate a local subprocess (or all element processes for an array job)."""
        # Kill single-job process if present
        proc = self._processes.get(job_id)
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()

        # Kill array element processes matching "{job_id}[*]"
        prefix = f"{job_id}["
        for key, proc in self._processes.items():
            if key.startswith(prefix) and proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    proc.kill()

        if job_id in self._jobs:
            record = self._jobs[job_id]
            record.status = JobStatus.KILLED
            for elem in record.array_elements.values():
                if elem.status not in {JobStatus.DONE, JobStatus.FAILED, JobStatus.KILLED}:
                    elem.status = JobStatus.KILLED
        logger.info("Cancelled local job %s", job_id)

    async def _write_output_files(
        self, job_name: str, proc: asyncio.subprocess.Process,
        resources: ResourceSpec, job_id: str,
        element_index: int | None = None,
    ) -> None:
        """Write captured stdout/stderr to output files.

        Uses per-job paths from ResourceSpec if set, otherwise writes
        ``stdout.{job_id}.log`` / ``stderr.{job_id}.log`` into the effective
        work directory.  When *element_index* is provided (array jobs), the
        filename becomes ``stdout.{job_id}.{element_index}.log``.
        """
        stdout_data, stderr_data = await proc.communicate()
        base = Path(resources.work_dir)
        if element_index is not None:
            out_path = base / f"stdout.{job_id}.{element_index}.log"
            err_path = base / f"stderr.{job_id}.{element_index}.log"
        elif resources.stdout_path:
            out_path = Path(resources.stdout_path)
            err_path = Path(resources.stderr_path) if resources.stderr_path else base / f"stderr.{job_id}.log"
        else:
            out_path = base / f"stdout.{job_id}.log"
            err_path = base / f"stderr.{job_id}.log"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(stdout_data or b"")
        err_path.write_bytes(stderr_data or b"")
