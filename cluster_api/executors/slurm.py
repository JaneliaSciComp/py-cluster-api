"""SLURM executor using sbatch/sacct/scancel."""

from __future__ import annotations

import dataclasses
import itertools
import logging
import math
import re
import shlex

from .._types import ResourceSpec
from ..config import ClusterConfig, parse_memory_bytes
from ..core import Executor
from ..exceptions import SubmitError
from ..script import render_script, write_script

logger = logging.getLogger(__name__)

# Injected into every SLURM CLI call: pin the timestamp format sacct emits
# and print full array task expressions instead of truncating at 64 bytes.
_SLURM_ENV = {"SLURM_TIME_FORMAT": "standard", "SLURM_BITSTR_LEN": "0"}

_SHELL_SHEBANG_RE = re.compile(r"^#!\s*(/\S*/)?(ba|z|k)?sh$")


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


def slurm_format_mem(n_bytes: int) -> str:
    """Format bytes as an sbatch ``--mem`` value in MiB, rounding up.

    dask-jobqueue's ``slurm_format_bytes_ceil`` picks the largest exact
    unit; SLURM's default unit is MiB so we always emit ``<n>M``.
    """
    return f"{max(1, math.ceil(n_bytes / 1024**2))}M"


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

    # Abstract-method stubs so the class is instantiable; replaced by the
    # real implementations in the status-polling task.

    def _build_status_args(self):  # pragma: no cover
        raise NotImplementedError

    def _parse_job_statuses(self, output):  # pragma: no cover
        raise NotImplementedError
