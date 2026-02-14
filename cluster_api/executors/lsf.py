"""LSF executor using bsub/bjobs/bkill."""

from __future__ import annotations

import fnmatch
import itertools
import json
import logging
import math
import re
from datetime import datetime, timezone
from typing import Any

from .._types import ArrayElement, JobRecord, JobStatus, ResourceSpec
from ..config import ClusterConfig, parse_memory_bytes
from ..core import Executor, _ARRAY_ELEMENT_RE
from ..exceptions import ClusterAPIError, CommandFailedError
from ..script import render_script, write_script

logger = logging.getLogger(__name__)


_LSF_STATUS_MAP: dict[str, JobStatus] = {
    "PEND": JobStatus.PENDING,
    "RUN": JobStatus.RUNNING,
    "DONE": JobStatus.DONE,
    "EXIT": JobStatus.FAILED,
    "ZOMBI": JobStatus.FAILED,
    "UNKWN": JobStatus.UNKNOWN,
    "WAIT": JobStatus.PENDING,
    "PROV": JobStatus.PENDING,
    "USUSP": JobStatus.PENDING,
    "PSUSP": JobStatus.PENDING,
    "SSUSP": JobStatus.PENDING,
}

_BJOBS_FIELDS = (
    "jobid stat exit_code exec_host max_mem "
    "submit_time start_time finish_time"
)

_BJOBS_RECONNECT_FIELDS = (
    "jobid job_name stat exit_code exec_host max_mem "
    "submit_time start_time finish_time"
)


def lsf_format_bytes_ceil(n_bytes: int, lsf_units: str = "MB") -> str:
    """Format bytes into LSF memory units, rounding up.

    Inspired by dask-jobqueue's lsf_format_bytes_ceil.
    """
    units = {"KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    if lsf_units not in units:
        raise ValueError(f"Unknown LSF units: {lsf_units}")
    return str(math.ceil(n_bytes / units[lsf_units]))


async def lsf_detect_units(
    timeout: float = 100.0,
) -> str:
    """Detect LSF memory units from lsadmin output.

    Inspired by dask-jobqueue's approach.
    """
    try:
        out = await Executor._call(
            ["lsadmin", "showconf", "lim"],
            timeout=timeout,
        )
        for line in out.splitlines():
            if "LSF_UNIT_FOR_LIMITS" in line:
                return line.split("=")[-1].strip().upper()
    except (ClusterAPIError, OSError):
        pass
    return "KB"  # LSF default


class LSFExecutor(Executor):
    """LSF executor using bsub, bjobs, bkill."""

    submit_command = "bsub"
    cancel_command = "bkill"
    status_command = "bjobs"
    directive_prefix = "#BSUB"
    job_id_regexp = r"Job <(?P<job_id>\d+)>"

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__(config)
        self._lsf_units = config.lsf_units
        self._script_counter = itertools.count(1)

    def build_header(
        self, name: str, resources: ResourceSpec | None = None
    ) -> list[str]:
        """Build #BSUB directive lines."""
        resources = resources or ResourceSpec()
        lines: list[str] = []
        p = self.directive_prefix

        lines.append(f"{p} -J {name}")

        out = resources.stdout_path or f"{resources.work_dir}/stdout.%J.log"
        err = resources.stderr_path or f"{resources.work_dir}/stderr.%J.log"
        lines.append(f"{p} -o {out}")
        lines.append(f"{p} -e {err}")

        # Queue
        queue = resources.queue or self.config.queue
        if queue:
            lines.append(f"{p} -q {queue}")

        # CPUs
        cpus = resources.cpus or self.config.cpus
        if cpus:
            lines.append(f"{p} -n {cpus}")
            if cpus > 1:
                lines.append(f'{p} -R "span[hosts=1]"')

        # GPUs
        gpus = resources.gpus or self.config.gpus
        if gpus:
            lines.append(f'{p} -gpu "num={gpus}"')

        # Memory
        memory_str = resources.memory or self.config.memory
        if memory_str:
            mem_bytes = parse_memory_bytes(memory_str)
            mem_val = lsf_format_bytes_ceil(mem_bytes, self._lsf_units)
            lines.append(f"{p} -M {mem_val}")
            lines.append(f'{p} -R "rusage[mem={mem_val}]"')

        # Walltime
        walltime = resources.walltime or self.config.walltime
        if walltime:
            lines.append(f"{p} -W {walltime}")

        # Working directory
        lines.append(f"{p} -cwd {resources.work_dir}")

        # Custom cluster options
        if resources.extra_directives:
            for opt in resources.extra_directives:
                lines.append(f"{p} {opt}")

        return lines

    def _build_submit_env(self, env: dict[str, str] | None) -> dict[str, str] | None:
        """Build environment dict for bsub, applying email suppression."""
        submit_env = dict(env) if env else {}
        if self.config.suppress_job_email:
            submit_env["LSB_JOB_REPORT_MAIL"] = "N"
        return submit_env or None

    def _collect_extra_args(self, resources: ResourceSpec | None = None) -> list[str]:
        """Merge config-level and per-job extra CLI args."""
        args = list(self.config.extra_args)
        if resources and resources.extra_args:
            args.extend(resources.extra_args)
        return args

    async def _bsub(
        self, script_path: str, env: dict[str, str] | None,
        extra_args: list[str] | None = None,
    ) -> str:
        """Run bsub with a script file and return raw output."""
        submit_env = self._build_submit_env(env)
        extra = " ".join(extra_args) + " " if extra_args else ""
        cmd = f"{self.submit_command} {extra}< {script_path}"
        logger.debug("Running: %s", cmd)
        return await self._call(
            cmd,
            shell=True,
            env=submit_env,
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
    ) -> tuple[str, str | None]:
        """Render script, write to disk, submit via bsub."""
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))

        extra_args = self._collect_extra_args(resources)
        out = await self._bsub(script_path, env, extra_args)
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
    ) -> tuple[str, str | None]:
        """Render script, rewrite for array syntax, submit via bsub."""
        header = self.build_header(name, resources)
        script = render_script(self.config, command, header, prologue, epilogue)
        script_path = write_script(resources.work_dir, script, name, next(self._script_counter))

        array_spec = f"{array_range[0]}-{array_range[1]}"
        if max_concurrent is not None:
            array_spec += f"%{max_concurrent}"
        array_name = f"{name}[{array_spec}]"

        # Rewrite only #BSUB directive lines for array syntax
        with open(script_path) as f:
            lines = f.readlines()
        new_lines = []
        for line in lines:
            if line.startswith(self.directive_prefix):
                line = line.replace(f"-J {name}", f"-J {array_name}")
                line = line.replace(f"{name}.out", f"{name}.%I.out")
                line = line.replace(f"{name}.err", f"{name}.%I.err")
                line = line.replace("stdout.%J.log", "stdout.%J.%I.log")
                line = line.replace("stderr.%J.log", "stderr.%J.%I.log")
            new_lines.append(line)
        content = "".join(new_lines)
        with open(script_path, "w") as f:
            f.write(content)

        extra_args = self._collect_extra_args(resources)
        out = await self._bsub(script_path, env, extra_args)
        return self._job_id_from_submit_output(out), script_path

    def _build_status_args(self) -> list[str]:
        """Build bjobs command with JSON output."""
        prefix = self._prefix
        args = [
            self.status_command,
            "-J", f"{prefix}-*",
            "-a",
            "-o", _BJOBS_FIELDS,
            "-json",
        ]
        return args

    def _parse_job_statuses(
        self, output: str
    ) -> dict[str, tuple[JobStatus, dict[str, Any]]]:
        """Parse bjobs JSON output into status + metadata dicts."""
        result: dict[str, tuple[JobStatus, dict[str, Any]]] = {}

        if not output.strip():
            return result

        try:
            data = json.loads(output)
        except json.JSONDecodeError:
            logger.warning("Failed to parse bjobs JSON output")
            return result

        records = data.get("RECORDS", [])
        for rec in records:
            job_id = str(rec.get("JOBID", "")).strip()
            if not job_id:
                continue

            stat = rec.get("STAT", "").strip()
            status = _LSF_STATUS_MAP.get(stat)
            if status is None:
                logger.warning("Unmapped LSF status: %r for job %s", stat, job_id)
                status = JobStatus.UNKNOWN

            exit_code_str = str(rec.get("EXIT_CODE", "")).strip()
            exit_code = None
            if exit_code_str and exit_code_str != "-":
                try:
                    exit_code = int(exit_code_str)
                except ValueError:
                    pass
            # LSF returns "" for exit_code on DONE jobs — infer 0
            if exit_code is None and status == JobStatus.DONE:
                exit_code = 0

            meta: dict[str, Any] = {
                "exec_host": _clean_field(rec.get("EXEC_HOST")),
                "max_mem": _clean_field(rec.get("MAX_MEM")),
                "exit_code": exit_code,
                "submit_time": _parse_lsf_time(rec.get("SUBMIT_TIME")),
                "start_time": _parse_lsf_time(rec.get("START_TIME")),
                "finish_time": _parse_lsf_time(rec.get("FINISH_TIME")),
                "job_name": _clean_field(rec.get("JOB_NAME")),
            }

            result[job_id] = (status, meta)

        return result

    async def cancel_by_name(self, name_pattern: str) -> None:
        """Cancel jobs matching name pattern via bkill -J."""
        cmd = [self.cancel_command, "-J", name_pattern]
        logger.debug("Running: %s", " ".join(cmd))
        await self._call(cmd, timeout=self.config.command_timeout)
        # Update in-memory state for matching jobs
        for record in self._jobs.values():
            if not record.is_terminal and fnmatch.fnmatch(record.name, name_pattern):
                record.status = JobStatus.KILLED
        logger.info("Cancelled jobs matching %s", name_pattern)

    # --- Reconnection ---

    def _build_reconnect_args(self) -> list[str]:
        """Build bjobs command for reconnection queries."""
        if not self.config.job_name_prefix:
            raise ClusterAPIError(
                "Cannot reconnect: no job_name_prefix was configured. "
                "Set job_name_prefix in config to enable reconnection."
            )
        return [
            self.status_command,
            "-J", f"{self._prefix}-*",
            "-a",
            "-o", _BJOBS_RECONNECT_FIELDS,
            "-json",
        ]

    async def reconnect(self) -> list[JobRecord]:
        """Reconnect to running jobs and resume tracking them.

        Queries bjobs, discovers existing jobs by name prefix, and reconstructs
        ``JobRecord`` instances so monitoring can resume after a process restart.

        Returns:
            List of newly created ``JobRecord`` instances.
        """
        args = self._build_reconnect_args()
        try:
            output = await self._call(args, timeout=self.config.command_timeout)
        except CommandFailedError as e:
            # bjobs returns non-zero when no jobs match
            if "No matching job" in str(e) or "No unfinished job" in str(e):
                return []
            raise

        statuses = self._parse_job_statuses(output)
        if not statuses:
            return []

        # Group into single jobs and array elements
        singles: dict[str, list[tuple[str, JobStatus, dict[str, Any]]]] = {}
        arrays: dict[str, list[tuple[int, JobStatus, dict[str, Any]]]] = {}

        for raw_id, (status, meta) in statuses.items():
            m = _ARRAY_ELEMENT_RE.match(raw_id)
            if m:
                parent_id, idx = m.group(1), int(m.group(2))
                arrays.setdefault(parent_id, []).append((idx, status, meta))
            else:
                singles.setdefault(raw_id, []).append((raw_id, status, meta))

        new_records: list[JobRecord] = []
        now = datetime.now(timezone.utc)

        # Process single (non-array) jobs
        for job_id, entries in singles.items():
            if job_id in self._jobs:
                continue
            _, status, meta = entries[0]
            record = JobRecord(
                job_id=job_id,
                name=meta.get("job_name") or "",
                command="",
                status=status,
                resources=None,
                metadata={"reconnected": True},
                _last_seen=now,
            )
            for key in ("exec_host", "max_mem", "exit_code",
                        "submit_time", "start_time", "finish_time"):
                if key in meta and meta[key] is not None:
                    setattr(record, key, meta[key])
            # Only add as single if it also has no array elements
            if job_id not in arrays:
                self._jobs[job_id] = record
                new_records.append(record)

        # Process array elements, grouping under parent
        for parent_id, elements in arrays.items():
            if parent_id in self._jobs:
                continue
            indices = sorted(idx for idx, _, _ in elements)
            array_range = (min(indices), max(indices))

            # Use job_name from the first element's meta
            first_meta = elements[0][2]
            record = JobRecord(
                job_id=parent_id,
                name=first_meta.get("job_name") or "",
                command="",
                status=JobStatus.PENDING,
                resources=None,
                metadata={"reconnected": True, "array_range": array_range},
                _last_seen=now,
            )

            for idx, elem_status, elem_meta in elements:
                ae = ArrayElement(index=idx, status=elem_status)
                for key in ("exec_host", "max_mem", "exit_code",
                            "submit_time", "start_time", "finish_time"):
                    if key in elem_meta and elem_meta[key] is not None:
                        setattr(ae, key, elem_meta[key])
                record.array_elements[idx] = ae

            record.status = record.compute_array_status()
            self._jobs[parent_id] = record
            new_records.append(record)

        logger.info("Reconnected to %d job(s)", len(new_records))
        return new_records


def _clean_field(value: Any) -> str | None:
    """Clean a bjobs field value, returning None for empty/dash values."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "-"):
        return None
    return s


def _parse_lsf_time(value: Any) -> datetime | None:
    """Parse an LSF timestamp string."""
    s = _clean_field(value)
    if s is None:
        return None
    # Strip trailing timezone indicator (e.g. " L" in "Feb  8 10:31:17 2026 L")
    s = re.sub(r"\s+[A-Z]$", "", s)
    # LSF timestamps are typically like "Jan  1 12:00:00 2024"
    for fmt in ("%b %d %H:%M:%S %Y", "%b  %d %H:%M:%S %Y", "%Y/%m/%d-%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None
