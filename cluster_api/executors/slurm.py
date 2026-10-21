"""SLURM executor using sbatch/sacct/scancel."""

from __future__ import annotations

import dataclasses
import itertools
import logging
import math
import re
import shlex
from datetime import datetime, timezone
from typing import Any

from .._types import ArrayElement, JobRecord, JobStatus, ResourceSpec, _TERMINAL_STATUSES
from ..config import ClusterConfig, parse_memory_bytes
from ..core import Executor, _ARRAY_ELEMENT_RE
from ..exceptions import ClusterAPIError, CommandFailedError, SubmitError
from ..script import render_script, write_script

logger = logging.getLogger(__name__)

# Injected into every SLURM CLI call: pin the timestamp format sacct emits
# and print full array task expressions instead of truncating at 64 bytes.
_SLURM_ENV = {"SLURM_TIME_FORMAT": "standard", "SLURM_BITSTR_LEN": "0"}

# https://slurm.schedmd.com/job_state_codes.html; Nextflow's STATUS_MAP agrees
# on every state it covers. PREEMPTED is terminal because every job is
# submitted with --no-requeue.
_SLURM_STATUS_MAP: dict[str, JobStatus] = {
    "PENDING": JobStatus.PENDING,
    "CONFIGURING": JobStatus.PENDING,
    "REQUEUED": JobStatus.PENDING,
    "REQUEUE_HOLD": JobStatus.PENDING,
    "REQUEUE_FED": JobStatus.PENDING,
    "RESV_DEL_HOLD": JobStatus.PENDING,
    "SPECIAL_EXIT": JobStatus.PENDING,
    "RUNNING": JobStatus.RUNNING,
    "COMPLETING": JobStatus.RUNNING,
    "SUSPENDED": JobStatus.RUNNING,
    "STOPPED": JobStatus.RUNNING,
    "SIGNALING": JobStatus.RUNNING,
    "STAGE_OUT": JobStatus.RUNNING,
    "RESIZING": JobStatus.RUNNING,
    "COMPLETED": JobStatus.DONE,
    "FAILED": JobStatus.FAILED,
    "BOOT_FAIL": JobStatus.FAILED,
    "NODE_FAIL": JobStatus.FAILED,
    "OUT_OF_MEMORY": JobStatus.FAILED,
    "TIMEOUT": JobStatus.FAILED,
    "DEADLINE": JobStatus.FAILED,
    "REVOKED": JobStatus.FAILED,
    "CANCELLED": JobStatus.KILLED,
    "PREEMPTED": JobStatus.KILLED,
}

_SACCT_FIELDS = "JobID,JobName,State,ExitCode,NodeList,Submit,Start,End"
_SACCT_FIELD_COUNT = 8

# sacct JobID forms: "123", "123_4", and for not-yet-split pending array
# tasks "123_[0-5,8%4]" (throttle suffix optional).
_ARRAY_TASK_RE = re.compile(r"^(?P<parent>\d+)_(?P<index>\d+)$")
_ARRAY_RANGE_RE = re.compile(r"^(?P<parent>\d+)_\[(?P<expr>[\d,\-:]+)(?:%\d+)?\]$")

_SHELL_SHEBANG_RE = re.compile(r"^#!\s*(/\S*/)?(ba|z|k)?sh$")


def slurm_format_mem(n_bytes: int) -> str:
    """Format bytes as an sbatch ``--mem`` value in MiB, rounding up.

    dask-jobqueue's ``slurm_format_bytes_ceil`` picks the largest exact
    unit; SLURM's default unit is MiB so we always emit ``<n>M``.
    """
    return f"{max(1, math.ceil(n_bytes / 1024**2))}M"


def _login_shebang(shebang: str) -> str:
    """Make the batch script run under a login shell (LSF ``-L`` equivalent).

    sbatch has no login-shell flag; the kernel honours the script's shebang,
    so ``#!/bin/bash`` becomes ``#!/bin/bash -l``. ``#!/usr/bin/env bash``
    cannot take a second argument on Linux and non-shell interpreters have
    no login mode, so those are left unchanged with a warning.
    """
    tokens = shebang.split()
    if "-l" in tokens or "--login" in tokens:
        return shebang
    if len(tokens) == 1 and _SHELL_SHEBANG_RE.match(tokens[0]):
        return f"{shebang} -l"
    logger.warning(
        "login_shell requested but shebang %r is not a plain shell path; "
        "leaving it unchanged", shebang,
    )
    return shebang


def expand_array_expr(expr: str) -> list[int]:
    """Expand a SLURM task expression like ``"0-5,8,10-20:5"`` into indices."""
    indices: list[int] = []
    for part in expr.split(","):
        if not part:
            continue
        step = 1
        if ":" in part:
            part, step_str = part.split(":", 1)
            step = int(step_str)
        if "-" in part:
            lo, hi = part.split("-", 1)
            indices.extend(range(int(lo), int(hi) + 1, step))
        else:
            indices.append(int(part))
    return indices


def _expand_job_id(job_id: str) -> list[str]:
    """Translate a sacct JobID into base-class keys (``"123"``, ``"123[4]"``)."""
    if job_id.isdigit():
        return [job_id]
    m = _ARRAY_TASK_RE.match(job_id)
    if m:
        return [f"{m.group('parent')}[{m.group('index')}]"]
    m = _ARRAY_RANGE_RE.match(job_id)
    if m:
        parent = m.group("parent")
        try:
            return [f"{parent}[{i}]" for i in expand_array_expr(m.group("expr"))]
        except ValueError:
            pass  # malformed expression such as "0-" or "0:"; fall through
    logger.warning("Unrecognised sacct JobID %r; skipping", job_id)
    return []


def _clean_field(value: str | None) -> str | None:
    """Return None for sacct's empty / placeholder values."""
    if value is None:
        return None
    s = value.strip()
    if s in ("", "None", "None assigned", "Unknown"):
        return None
    return s


def _parse_exit_code(raw: str) -> int | None:
    """Parse sacct ``ExitCode`` (``rc:sig``).

    Returns ``rc`` normally, or ``-sig`` when a signal terminated the job,
    matching the negative return codes ``subprocess`` reports for the local
    executor.
    """
    s = raw.strip()
    if ":" not in s:
        return None
    rc_str, sig_str = s.split(":", 1)
    try:
        rc, sig = int(rc_str), int(sig_str)
    except ValueError:
        return None
    return -sig if sig else rc


def _parse_slurm_time(raw: str) -> datetime | None:
    """Parse ``YYYY-MM-DDTHH:MM:SS`` (SLURM_TIME_FORMAT=standard, local time) to UTC."""
    s = _clean_field(raw)
    if s is None:
        return None
    try:
        naive_local = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    return naive_local.astimezone(timezone.utc)


class SlurmExecutor(Executor):
    """SLURM executor using sbatch, sacct, scancel."""

    submit_command = "sbatch"
    cancel_command = "scancel"
    status_command = "sacct"
    directive_prefix = "#SBATCH"
    # sbatch --parsable prints "123" or "123;cluster"; matched against the
    # last non-empty stdout line (see _job_id_from_submit_output).
    job_id_regexp = r"^(?P<job_id>\d+)"

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__(config)
        self._script_counter = itertools.count(1)

    def build_header(
        self,
        name: str,
        resources: ResourceSpec | None = None,
        array_spec: str | None = None,
    ) -> list[str]:
        """Build #SBATCH directive lines.

        *array_spec* (e.g. ``"1-10%3"``) switches log names to ``%A_%a``
        and adds ``--array``; arrays get a correct header on first render
        rather than a post-hoc rewrite.
        """
        resources = resources or ResourceSpec()
        p = self.directive_prefix
        lines: list[str] = [f"{p} --job-name={name}"]

        tag = "%A_%a" if array_spec else "%j"
        out = resources.stdout_path or f"{resources.work_dir}/stdout.{tag}.log"
        err = resources.stderr_path or f"{resources.work_dir}/stderr.{tag}.log"
        lines.append(f'{p} --output="{out}"')
        lines.append(f'{p} --error="{err}"')

        # dask-jobqueue emits both unconditionally; Nextflow emits --no-requeue
        # unconditionally so preempted/node-failed jobs stay terminal for us.
        lines.append(f"{p} --nodes=1")
        lines.append(f"{p} --ntasks=1")
        lines.append(f"{p} --no-requeue")

        if array_spec:
            lines.append(f"{p} --array={array_spec}")

        queue = resources.queue or self.config.queue
        if queue:
            lines.append(f"{p} --partition={queue}")

        cpus = resources.cpus or self.config.cpus
        if cpus:
            lines.append(f"{p} --cpus-per-task={cpus}")

        gpus = resources.gpus or self.config.gpus
        if gpus:
            lines.append(f"{p} --gres=gpu:{gpus}")

        memory_str = resources.memory or self.config.memory
        if memory_str:
            lines.append(f"{p} --mem={slurm_format_mem(parse_memory_bytes(memory_str))}")

        walltime = resources.walltime or self.config.walltime
        if walltime:
            lines.append(f"{p} --time={walltime}")

        lines.append(f'{p} --chdir="{resources.work_dir}"')

        for opt in resources.extra_directives or []:
            lines.append(f"{p} {opt}")

        return lines

    # --- Submission ---

    @staticmethod
    async def _call(
        cmd: list[str],
        timeout: float = 100.0,
        env: dict[str, str] | None = None,
        stdin_file: str | None = None,
    ) -> str:
        """Run a SLURM CLI command with the pinned SLURM_* environment."""
        return await Executor._call(
            cmd, timeout=timeout, env={**_SLURM_ENV, **(env or {})},
            stdin_file=stdin_file,
        )

    def _job_id_from_submit_output(self, out: str) -> str:
        """Take the job ID from the last non-empty line of ``sbatch --parsable``.

        Warnings may precede it on stdout; a ``;cluster`` suffix is ignored
        (multi-cluster submission is unsupported).
        """
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        last = lines[-1] if lines else ""
        match = re.match(self.job_id_regexp, last)
        if not match:
            raise SubmitError(f"Could not parse job ID from output: {out!r}")
        return match.group("job_id")

    def _submit_args(
        self,
        resources: ResourceSpec,
        inherit_env: bool,
        depends_on: list[str] | None,
    ) -> list[str]:
        """sbatch command-line arguments after the executable."""
        args = ["--parsable", *self.config.extra_args, *(resources.extra_args or [])]
        if not inherit_env:
            # Only SLURM_* variables reach the job (plus, per the sbatch man
            # page, an implicitly loaded login environment on the node).
            args.append("--export=NONE")
        if depends_on:
            # afterok = every listed job exited zero; kill-on-invalid-dep
            # cancels this job if one of them fails (LSF -w done() -ti).
            args.append("--dependency=afterok:" + ":".join(depends_on))
            args.append("--kill-on-invalid-dep=yes")
        return args

    def _render(
        self,
        command: str,
        header: list[str],
        prologue: list[str] | None,
        epilogue: list[str] | None,
        env: dict[str, str] | None,
        inherit_env: bool,
        login_shell: bool,
    ) -> str:
        """Render the job script, applying login-shell and clean-env rules."""
        config = self.config
        if login_shell:
            config = dataclasses.replace(config, shebang=_login_shebang(config.shebang))
        if not inherit_env:
            # --export=NONE would otherwise also restrict srun steps the
            # user's command launches; the sbatch man page recommends this.
            exports = ["export SLURM_EXPORT_ENV=ALL"]
            exports += [f"export {k}={shlex.quote(v)}" for k, v in (env or {}).items()]
            prologue = [*exports, *(prologue or [])]
        return render_script(config, command, header, prologue, epilogue)

    async def _sbatch(
        self, script_path: str, env: dict[str, str] | None, args: list[str]
    ) -> str:
        return await self._call(
            [self.submit_command, *args],
            stdin_file=script_path,
            env=env or None,
            timeout=self.config.command_timeout,
        )

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
        """Render script, write to disk, submit via sbatch."""
        header = self.build_header(name, resources)
        script = self._render(command, header, prologue, epilogue, env, inherit_env, login_shell)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))
        out = await self._sbatch(
            script_path, env, self._submit_args(resources, inherit_env, depends_on)
        )
        return self._job_id_from_submit_output(out), script_path

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
        """Render an array script (``--array=A-B[%N]``) and submit via sbatch."""
        array_spec = f"{array_range[0]}-{array_range[1]}"
        if max_concurrent is not None:
            array_spec += f"%{max_concurrent}"
        header = self.build_header(name, resources, array_spec=array_spec)
        script = self._render(command, header, prologue, epilogue, env, inherit_env, login_shell)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))
        out = await self._sbatch(
            script_path, env, self._submit_args(resources, inherit_env, depends_on)
        )
        return self._job_id_from_submit_output(out), script_path

    # --- Status polling ---

    def _sacct_args(self, job_ids: list[str]) -> list[str]:
        """sacct query for explicit job IDs; shared by poll() and reconnect().

        ``-n`` no header, ``-P`` pipe-delimited, ``-X`` allocation rows only.
        With ``-j`` the time window defaults to epoch 0 (sacct man page).
        """
        args = [self.status_command, "-n", "-P", "-X"]
        if self.config.poll_all_users:
            args.append("--allusers")
        args.extend(["-j", ",".join(job_ids), "-o", _SACCT_FIELDS])
        return args

    def _build_status_args(self) -> list[str]:
        active_ids = [jid for jid, r in self._jobs.items() if not r.is_terminal]
        return self._sacct_args(active_ids)

    def _parse_job_statuses(
        self, output: str
    ) -> dict[str, tuple[JobStatus, dict[str, Any]]]:
        """Parse ``sacct -n -P -X`` rows into status + metadata dicts."""
        result: dict[str, tuple[JobStatus, dict[str, Any]]] = {}
        # Expansions of compressed pending ranges ("17_[2-3]") are applied
        # last and never override an explicit task row ("17_2"): sacct can
        # list a task in the array's stale pending meta-row after it started.
        from_ranges: dict[str, tuple[JobStatus, dict[str, Any]]] = {}
        for line in output.splitlines():
            if not line.strip():
                continue
            fields = line.split("|")
            if len(fields) < _SACCT_FIELD_COUNT:
                logger.warning("Unexpected sacct line (expected %d fields): %r",
                               _SACCT_FIELD_COUNT, line)
                continue
            job_id, job_name, state_raw, exit_raw, nodelist, submit, start, end = fields[:8]
            job_id = job_id.strip()
            if "." in job_id:
                continue  # step row (123.batch, 123.0)

            # "CANCELLED by 1234" -> "CANCELLED"
            state_tok = state_raw.split()[0] if state_raw.split() else ""
            status = _SLURM_STATUS_MAP.get(state_tok)
            if status is None:
                logger.warning("Unmapped SLURM state %r for job %s", state_raw, job_id)
                status = JobStatus.UNKNOWN

            # sacct prints "0:0" for jobs that have not finished; only a
            # terminal state carries a meaningful exit code.
            exit_code = None
            if status in _TERMINAL_STATUSES:
                exit_code = _parse_exit_code(exit_raw)
                if exit_code is None and status == JobStatus.DONE:
                    exit_code = 0

            meta: dict[str, Any] = {
                "exec_host": _clean_field(nodelist),
                "exit_code": exit_code,
                "submit_time": _parse_slurm_time(submit),
                "start_time": _parse_slurm_time(start),
                "finish_time": _parse_slurm_time(end),
                "job_name": _clean_field(job_name),
            }
            target = from_ranges if "[" in job_id else result
            for key in _expand_job_id(job_id):
                target[key] = (status, dict(meta))
        for key, value in from_ranges.items():
            result.setdefault(key, value)
        return result

    # --- Cancellation ---

    async def _cancel_job(self, job_id: str, *, done: bool = False) -> None:
        """Run scancel. *done* has no SLURM equivalent (no ``bkill -d``);
        the base class still marks the record DONE in-process."""
        await self._call([self.cancel_command, job_id], timeout=self.config.command_timeout)

    async def cancel_by_name(self, name_pattern: str) -> None:
        """Cancel jobs with exactly this name via ``scancel --name``.

        scancel documents no wildcard matching, so unlike LSF's ``bkill -J``
        the argument is an exact job name.
        """
        if any(ch in name_pattern for ch in "*?["):
            logger.warning(
                "cancel_by_name(%r): SLURM matches exact job names only; "
                "glob characters are passed through literally", name_pattern,
            )
        cmd = [self.cancel_command, f"--name={name_pattern}"]
        try:
            await self._call(cmd, timeout=self.config.command_timeout)
        except CommandFailedError as e:
            # scancel: "No active jobs match ALL job filters"
            if "match" in str(e).lower():
                logger.debug("No jobs matched name %s", name_pattern)
                return
            raise
        for record in self._jobs.values():
            if not record.is_terminal and record.name == name_pattern:
                record.status = JobStatus.KILLED
        logger.info("Cancelled jobs named %s", name_pattern)

    # --- Reconnection ---

    _RECORD_FIELDS = ("exec_host", "exit_code", "submit_time", "start_time", "finish_time")

    def _build_discovery_args(self) -> list[str]:
        """squeue query listing live jobs as ``parent_id|name`` lines.

        squeue shows every pending and running job regardless of
        eligibility (held, dependency-waiting), which ``sacct --state``
        does not. ``-r`` gives one line per array element with ``%F`` the
        parent ID; squeue lists all users by default, ``--me`` restricts it.
        """
        if not self._prefix:
            raise ClusterAPIError(
                "Cannot reconnect: no job_name_prefix was configured. "
                "Set job_name_prefix in config to enable reconnection."
            )
        args = ["squeue", "-h", "-r", "-o", "%F|%j"]
        if not self.config.poll_all_users:
            args.append("--me")
        return args

    async def reconnect(self) -> list[JobRecord]:
        """Rediscover live jobs by name prefix and resume tracking them.

        Step 1 discovers IDs with squeue; step 2 fetches full rows for them
        with the ordinary sacct query, which (being unfiltered by state)
        also returns finished array elements so partially complete arrays
        are rebuilt with their full extent.
        """
        args = self._build_discovery_args()
        output = await self._call(args, timeout=self.config.command_timeout)

        wanted = f"{self._prefix}-"
        ids: list[str] = []
        for line in output.splitlines():
            if "|" not in line:
                continue
            job_id, name = line.split("|", 1)
            job_id = job_id.strip()
            if name.strip().startswith(wanted) and job_id not in ids and job_id not in self._jobs:
                ids.append(job_id)
        if not ids:
            return []

        rows = await self._call(self._sacct_args(ids), timeout=self.config.command_timeout)
        statuses = self._parse_job_statuses(rows)

        singles: dict[str, tuple[JobStatus, dict[str, Any]]] = {}
        arrays: dict[str, list[tuple[int, JobStatus, dict[str, Any]]]] = {}
        for raw_id, (status, meta) in statuses.items():
            m = _ARRAY_ELEMENT_RE.match(raw_id)
            if m:
                arrays.setdefault(m.group(1), []).append((int(m.group(2)), status, meta))
            else:
                singles[raw_id] = (status, meta)

        now = datetime.now(timezone.utc)
        new_records: list[JobRecord] = []

        for job_id, (status, meta) in singles.items():
            if job_id in self._jobs or job_id in arrays or status in _TERMINAL_STATUSES:
                continue
            record = JobRecord(
                job_id=job_id, name=meta.get("job_name") or "", command="",
                status=status, metadata={"reconnected": True}, _last_seen=now,
            )
            for key in self._RECORD_FIELDS:
                if meta.get(key) is not None:
                    setattr(record, key, meta[key])
            self._jobs[job_id] = record
            new_records.append(record)

        for parent_id, elements in arrays.items():
            if parent_id in self._jobs:
                continue
            if all(s in _TERMINAL_STATUSES for _, s, _ in elements):
                continue
            indices = sorted(idx for idx, _, _ in elements)
            record = JobRecord(
                job_id=parent_id, name=elements[0][2].get("job_name") or "", command="",
                status=JobStatus.PENDING,
                metadata={"reconnected": True, "array_range": (indices[0], indices[-1])},
                _last_seen=now,
            )
            for idx, elem_status, elem_meta in elements:
                ae = ArrayElement(index=idx, status=elem_status)
                for key in self._RECORD_FIELDS:
                    if elem_meta.get(key) is not None:
                        setattr(ae, key, elem_meta[key])
                record.array_elements[idx] = ae
            record.status = record.compute_array_status()
            self._jobs[parent_id] = record
            new_records.append(record)

        logger.info("Reconnected to %d job(s)", len(new_records))
        return new_records
