"""Local subprocess executor for testing without a real scheduler."""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .._types import ArrayElement, JobStatus, ResourceSpec
from ..config import ClusterConfig
from ..core import Executor, _ARRAY_ELEMENT_RE
from ..exceptions import ClusterAPIError
from ..script import render_script, write_script

logger = logging.getLogger(__name__)

# PATH for jobs that don't inherit the submitting process's environment;
# mirrors what cron/sshd hand a fresh process. A login shell's profile will
# prepend the user's own entries on top of this.
_BASELINE_PATH = "/usr/local/bin:/usr/bin:/bin"

# Identity and locale variables that describe the *user*, not the submitting
# process, so they are safe to carry into a non-inheriting job.
_BASELINE_KEYS = ("HOME", "USER", "LOGNAME", "SHELL", "LANG", "TERM")


def _baseline_env(env: dict[str, str] | None) -> dict[str, str]:
    """Minimal job environment for inherit_env=False submissions."""
    base = {k: os.environ[k] for k in _BASELINE_KEYS if k in os.environ}
    base["PATH"] = _BASELINE_PATH
    return {**base, **(env or {})}


class LocalExecutor(Executor):
    """Runs jobs as local bash subprocesses. Useful for testing."""

    submit_command = "bash"
    cancel_command = "kill"
    status_command = "ps"
    directive_prefix = "# LOCAL"

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__(config)
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._open_fds: dict[str, tuple[int, int]] = {}
        self._next_id = 1
        self._script_counter = itertools.count(1)
        self._waiting: dict[str, dict] = {}

    def build_header(
        self, name: str, resources: ResourceSpec | None = None
    ) -> list[str]:
        """Local executor doesn't need scheduler directives."""
        return [f"# LOCAL Job: {name}"]

    @staticmethod
    def _job_env(env: dict[str, str] | None, inherit_env: bool) -> dict[str, str]:
        return {**os.environ, **(env or {})} if inherit_env else _baseline_env(env)

    @staticmethod
    def _bash_cmd(script_path: str, login_shell: bool) -> list[str]:
        return ["bash", "-l", script_path] if login_shell else ["bash", script_path]

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
        inherit_env: bool = True,
        login_shell: bool = False,
        depends_on: list[str] | None = None,
    ) -> tuple[str, str | None]:
        """Render script, write to disk, run as a background subprocess.

        The job_id is the subprocess PID, which doubles as its process-group
        id (the child is spawned with ``start_new_session=True``). This makes
        the id a durable OS handle: a fresh executor in a later process can
        cancel the job's whole tree by PID alone, without the in-memory
        ``_processes`` entry (see :meth:`cancel`).
        """
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        token = next(self._script_counter)
        script_path = write_script(resources.work_dir, script, name, token)

        if depends_on:
            job_id = f"waiting-{self._next_id}"
            self._next_id += 1
            self._waiting[job_id] = {
                "script_path": script_path,
                "resources": resources,
                "env": env,
                "cwd": cwd,
                "inherit_env": inherit_env,
                "login_shell": login_shell,
                "array_range": None,
            }
            return job_id, script_path

        full_env = self._job_env(env, inherit_env)
        bash_cmd = self._bash_cmd(script_path, login_shell)

        # Open logs before spawn (they become the child's stdout/stderr). The
        # PID isn't known yet, so key them by the script token, then re-key to
        # the PID once we have it.
        stdout_dest, stderr_dest = self._open_output_files(resources, job_id=str(token))

        proc = await asyncio.create_subprocess_exec(
            *bash_cmd,
            stdout=stdout_dest,
            stderr=stderr_dest,
            env=full_env,
            cwd=cwd,
            start_new_session=True,
        )

        job_id = str(proc.pid)
        self._open_fds[job_id] = self._open_fds.pop(str(token))
        # Default log files were named by the pre-spawn token; re-point them at
        # the PID so the documented stdout.{job_id}.log resolves. The open fds
        # keep writing to the same inode across the rename.
        if not resources.stdout_path:
            base = Path(resources.work_dir)
            for stream in ("stdout", "stderr"):
                (base / f"{stream}.{token}.log").rename(base / f"{stream}.{job_id}.log")
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
        inherit_env: bool = True,
        login_shell: bool = False,
        depends_on: list[str] | None = None,
    ) -> tuple[str, str | None]:
        """Spawn one subprocess per array element with ARRAY_INDEX env var."""
        if max_concurrent is not None:
            logger.warning(
                "LocalExecutor does not support max_concurrent; "
                "all %d elements will run simultaneously",
                array_range[1] - array_range[0] + 1,
            )
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))

        if depends_on:
            job_id = f"waiting-{self._next_id}"
            self._next_id += 1
            self._waiting[job_id] = {
                "script_path": script_path,
                "resources": resources,
                "env": env,
                "cwd": cwd,
                "inherit_env": inherit_env,
                "login_shell": login_shell,
                "array_range": array_range,
            }
            return job_id, script_path

        # Array jobs spawn N processes, so no single PID identifies the job.
        # Use a synthetic, deliberately non-numeric id: it can't be mistaken
        # for a process-group id by the stateless cancel path (which would
        # otherwise int() it), so an array is only ever cancelled in-process
        # via its element handles.
        job_id = f"array-{self._next_id}"
        self._next_id += 1

        full_env = self._job_env(env, inherit_env)
        bash_cmd = self._bash_cmd(script_path, login_shell)

        for index in range(array_range[0], array_range[1] + 1):
            element_env = {**full_env, "ARRAY_INDEX": str(index)}
            stdout_dest, stderr_dest = self._open_output_files(
                resources, job_id=job_id, element_index=index,
            )
            proc = await asyncio.create_subprocess_exec(
                *bash_cmd,
                stdout=stdout_dest,
                stderr=stderr_dest,
                env=element_env,
                cwd=cwd,
            )
            self._processes[f"{job_id}[{index}]"] = proc

        return job_id, script_path

    async def _spawn_deferred(self, job_id: str, entry: dict) -> None:
        """Spawn a parked job whose dependencies are now satisfied.

        The job keeps its synthetic ``waiting-N`` id (ids never change once
        returned), so the PID-based stateless cancel path does not apply —
        same trade-off as local array jobs.
        """
        resources = entry["resources"]
        full_env = self._job_env(entry["env"], entry["inherit_env"])
        bash_cmd = self._bash_cmd(entry["script_path"], entry["login_shell"])

        if entry["array_range"] is None:
            stdout_dest, stderr_dest = self._open_output_files(resources, job_id=job_id)
            proc = await asyncio.create_subprocess_exec(
                *bash_cmd,
                stdout=stdout_dest,
                stderr=stderr_dest,
                env=full_env,
                cwd=entry["cwd"],
                start_new_session=True,
            )
            self._processes[job_id] = proc
        else:
            start, end = entry["array_range"]
            for index in range(start, end + 1):
                element_env = {**full_env, "ARRAY_INDEX": str(index)}
                stdout_dest, stderr_dest = self._open_output_files(
                    resources, job_id=job_id, element_index=index,
                )
                proc = await asyncio.create_subprocess_exec(
                    *bash_cmd,
                    stdout=stdout_dest,
                    stderr=stderr_dest,
                    env=element_env,
                    cwd=entry["cwd"],
                )
                self._processes[f"{job_id}[{index}]"] = proc

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
                self._close_output_files(job_id)
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
                self._close_output_files(key)
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

        # --- Waiting (dependency-deferred) jobs ---
        for job_id in list(self._waiting):
            record = self._jobs.get(job_id)
            if record is None or record.is_terminal:
                self._waiting.pop(job_id)
                continue
            dep_records = [self._jobs.get(d) for d in record.depends_on]
            failed = [
                d for d, r in zip(record.depends_on, dep_records)
                if r is None or r.status in {JobStatus.FAILED, JobStatus.KILLED}
            ]
            now = datetime.now(timezone.utc)
            if failed:
                # An untracked dep (None) can never be satisfied — treat as failed.
                self._waiting.pop(job_id)
                record.status = JobStatus.KILLED
                record.metadata["dependency_failed"] = failed
                record.finish_time = now
            elif all(r.status == JobStatus.DONE for r in dep_records):
                entry = self._waiting.pop(job_id)
                await self._spawn_deferred(job_id, entry)
                record._last_seen = now
            else:
                record._last_seen = now  # keep zombie detection at bay while parked

        return {jid: r.status for jid, r in self._jobs.items()}

    async def cancel(self, job_id: str, *, done: bool = False) -> None:
        """Terminate a local job and its whole process tree.

        Fast path: if this executor submitted the job (its ``asyncio``
        subprocess handle is still in ``_processes``), signal that handle
        directly. Stateless path: otherwise ``job_id`` is treated as the
        job's process-group id (== leader PID from submit) and the entire
        group is killed by PID — so a fresh executor in a later process can
        cancel a job it never submitted.
        """
        # A parked (dependency-deferred) job has no process yet; discard its
        # entry so a later poll can't spawn it after cancellation.
        deferred = self._waiting.pop(job_id, None)

        # Fast path: live in-memory handles for this job (single + array elements).
        live: list[tuple[str, asyncio.subprocess.Process]] = []
        proc = self._processes.get(job_id)
        if proc and proc.returncode is None:
            live.append((job_id, proc))
        prefix = f"{job_id}["
        for key, proc in self._processes.items():
            if key.startswith(prefix) and proc.returncode is None:
                live.append((key, proc))

        if live:
            # Send SIGTERM to all, then wait concurrently
            for _key, p in live:
                p.terminate()
            tasks = [asyncio.ensure_future(p.wait()) for _key, p in live]
            _, pending = await asyncio.wait(tasks, timeout=5.0)
            # SIGKILL any that didn't exit in time
            for _key, p in live:
                if p.returncode is None:
                    p.kill()
            # Reap the killed processes
            if pending:
                await asyncio.wait(pending, timeout=5.0)
            for key, _p in live:
                self._close_output_files(key)
        elif deferred is None and job_id not in self._processes:
            # Stateless path: no handle for this job — kill by process group.
            await self._terminate_group(job_id)

        target_status = JobStatus.DONE if done else JobStatus.KILLED
        if job_id in self._jobs:
            record = self._jobs[job_id]
            record.status = target_status
            for elem in record.array_elements.values():
                if elem.status not in {JobStatus.DONE, JobStatus.FAILED, JobStatus.KILLED}:
                    elem.status = target_status
        logger.info("Cancelled local job %s (done=%s)", job_id, done)

    async def _terminate_group(
        self, job_id: str, grace_seconds: float = 3.0
    ) -> None:
        """Kill a job's process group (SIGTERM, then SIGKILL) by PID.

        ``job_id`` is the process-group id (the leader PID from submit, which
        used ``start_new_session=True``). Killing the group reaches the launcher
        bash *and* its workload — bash does not forward SIGTERM to its child, so
        signalling the leader alone would orphan the real job.

        Raises ClusterAPIError if anything in the group outlives SIGKILL.
        """
        try:
            pgid = int(job_id)
        except ValueError:
            # Non-PID id (e.g. an array counter) — nothing to kill statelessly.
            logger.warning("Cannot cancel local job %s: no live handle", job_id)
            return

        def present() -> bool:
            # True while the group has any member — including zombies not yet
            # reaped. After SIGKILL a dead process lingers as a zombie until its
            # parent (init, once the submitter exited) reaps it, so we poll
            # briefly rather than trusting the first check.
            try:
                os.killpg(pgid, 0)
                return True
            except ProcessLookupError:
                return False
            except PermissionError:
                # Group exists but is owned by someone else — treat as alive.
                return True

        # ponytail: no PID-reuse guard — the group id could have been recycled
        # onto an unrelated process. This matches the prior job.pid behavior;
        # add a start-time check here if reuse ever bites in practice.
        if not present():
            return
        os.killpg(pgid, signal.SIGTERM)
        deadline = time.monotonic() + grace_seconds
        while present() and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        if present():
            os.killpg(pgid, signal.SIGKILL)
            # SIGKILL is immediate, but reaping the resulting zombies isn't.
            deadline = time.monotonic() + 2.0
            while present() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
        if present():
            raise ClusterAPIError(
                f"Local job {job_id} survived SIGKILL; may still be running"
            )

    def _open_output_files(
        self,
        resources: ResourceSpec,
        job_id: str | None = None,
        element_index: int | None = None,
    ) -> tuple[int, int]:
        """Open stdout/stderr log files for direct subprocess output.

        Returns a pair of file descriptors suitable for passing to
        ``asyncio.create_subprocess_exec`` as *stdout* and *stderr*.

        Uses per-job paths from ResourceSpec if set, otherwise writes
        ``stdout.{job_id}.log`` / ``stderr.{job_id}.log`` into the work
        directory.  For array elements the filename becomes
        ``stdout.{job_id}.{element_index}.log``.
        """
        base = Path(resources.work_dir)
        if element_index is not None:
            out_path = base / f"stdout.{job_id}.{element_index}.log"
            err_path = base / f"stderr.{job_id}.{element_index}.log"
        elif resources.stdout_path:
            out_path = Path(resources.stdout_path)
            err_path = Path(resources.stderr_path) if resources.stderr_path else base / "stderr.log"
        else:
            out_path = base / f"stdout.{job_id}.log" if job_id else base / "stdout.log"
            err_path = base / f"stderr.{job_id}.log" if job_id else base / "stderr.log"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.parent.mkdir(parents=True, exist_ok=True)
        out_fd = os.open(str(out_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        err_fd = os.open(str(err_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        # Track open file descriptors by process key for cleanup
        key = f"{job_id}[{element_index}]" if element_index is not None else (job_id or "")
        self._open_fds[key] = (out_fd, err_fd)
        return out_fd, err_fd

    def _close_output_files(self, key: str) -> None:
        """Close file descriptors for a finished process."""
        fds = self._open_fds.pop(key, None)
        if fds:
            for fd in fds:
                try:
                    os.close(fd)
                except OSError:
                    pass
