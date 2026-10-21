"""SLURM executor using sbatch/sacct/scancel."""

from __future__ import annotations

import itertools
import logging
import math

from .._types import ResourceSpec
from ..config import ClusterConfig, parse_memory_bytes
from ..core import Executor

logger = logging.getLogger(__name__)


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

    # Abstract-method stubs so the class is instantiable; replaced by the
    # real implementations in the submission and status-polling tasks.

    async def _submit_job(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    def _build_status_args(self):  # pragma: no cover
        raise NotImplementedError

    def _parse_job_statuses(self, output):  # pragma: no cover
        raise NotImplementedError
