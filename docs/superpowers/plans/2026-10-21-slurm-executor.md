# SLURM Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `SlurmExecutor` that submits with `sbatch`, polls with `sacct`, cancels with `scancel`, and reconnects via `squeue`, behind the existing `Executor` interface.

**Architecture:** One new module `cluster_api/executors/slurm.py` subclasses `Executor` and implements the same hooks `LSFExecutor` does (`build_header`, `_submit_job`, `_submit_array_job`, `_build_status_args`, `_parse_job_statuses`, `_cancel_job`, `cancel_by_name`, `reconnect`), sharing only `render_script`, `write_script` and `parse_memory_bytes`. The base class `poll()` is unchanged: array elements are re-keyed from SLURM's `123_4` to the base class's `123[4]` form so aggregation works as for LSF. A `_call` override injects `SLURM_TIME_FORMAT=standard` and `SLURM_BITSTR_LEN=0` into every SLURM CLI invocation.

**Tech Stack:** Python 3.10+, asyncio, pytest + pytest-asyncio (`asyncio_mode = "auto"`), ruff, pixi. All commands run via `pixi run ...`.

**Spec:** `docs/superpowers/specs/2026-10-18-slurm-executor-design.md`

## Global Constraints

- Always run tools through pixi: `pixi run test`, `pixi run lint`, `pixi run fmt`, `pixi run test-integration`. Never call `python`, `pytest` or `ruff` directly.
- Python 3.10+ syntax (`X | Y` unions, `from __future__ import annotations`).
- Async-only API; no sync wrappers.
- Do not modify `cluster_api/executors/lsf.py`, `cluster_api/core.py`, `cluster_api/_types.py` or `cluster_api/script.py`. The SLURM executor is "completely separate from LSF" (spec Decision 4).
- No new `ClusterConfig` fields.
- Every `#SBATCH` directive and CLI flag must be one the spec lists; the spec cites a source for each. Do not add directives the spec does not name.
- Log file names: single jobs `stdout.%j.log` / `stderr.%j.log`; arrays `stdout.%A_%a.log` / `stderr.%A_%a.log`. Never `%J`.
- Walltime examples and integration-test values use `HH:MM:SS` (on SLURM `01:00` is one minute).
- Commit after every task with the message shown; end each commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Target `_SLURM_STATUS_MAP` exactly as the spec's table; unknown states → `JobStatus.UNKNOWN` with a warning.
- Version bump to `0.9.0` happens only in the final task.

## Review Focus

1. A sacct line with fewer than 8 `|`-fields (truncated output, customised site wrapper) must be logged and skipped, never raise. Test added to Task 3.
2. sbatch stdout with a warning line before the ID (`sbatch: warning: ...\n12345`) must still yield `12345`; the ID is parsed from the last non-empty line. Test added to Task 2.
3. `--export=NONE` with `env=None` must still add `export SLURM_EXPORT_ENV=ALL` to the prologue, because user-invoked `srun` steps otherwise inherit the restriction. Test added to Task 2.
4. `cancel_by_name` called with glob characters (`test-*`) must log a warning that SLURM matches exact names only, and still pass the string through unchanged. Test added to Task 4.
5. `reconnect()` when squeue lists a job that is already tracked must not create a duplicate record or re-query it. Test added to Task 5.

---

### Task 1: Module skeleton, registry, and `build_header`

**Files:**
- Create: `cluster_api/executors/slurm.py`
- Modify: `cluster_api/executors/__init__.py:13-22`
- Modify: `cluster_api/__init__.py:17-36`
- Modify: `tests/conftest.py` (append fixture)
- Create: `tests/test_slurm.py`

**Interfaces:**
- Consumes: `Executor` (`cluster_api/core.py`), `ResourceSpec`, `ClusterConfig`, `parse_memory_bytes`.
- Produces:
  - `class SlurmExecutor(Executor)` with class attrs `submit_command = "sbatch"`, `cancel_command = "scancel"`, `status_command = "sacct"`, `directive_prefix = "#SBATCH"`, `job_id_regexp = r"^(?P<job_id>\d+)"`.
  - `SlurmExecutor.build_header(self, name: str, resources: ResourceSpec | None = None, array_spec: str | None = None) -> list[str]`.
  - `slurm_format_mem(n_bytes: int) -> str` returning e.g. `"8192M"`.
  - Fixture `slurm_config` in `tests/conftest.py`.
  - Registry key `"slurm"`; `cluster_api.SlurmExecutor` export.

- [ ] **Step 1: Add the `slurm_config` fixture**

Append to `tests/conftest.py`:

```python
@pytest.fixture
def slurm_config():
    """A SLURM ClusterConfig for testing."""
    return ClusterConfig(
        executor="slurm",
        job_name_prefix="test",
        queue="normal",
        memory="8 GB",
        walltime="04:00:00",
        poll_interval=0.5,
        command_timeout=10.0,
    )
```

- [ ] **Step 2: Write the failing header tests**

Create `tests/test_slurm.py`:

```python
"""Tests for the SLURM executor."""

from __future__ import annotations

from cluster_api._types import ResourceSpec
from cluster_api.config import ClusterConfig
from cluster_api.executors import get_executor_class
from cluster_api.executors.slurm import SlurmExecutor, slurm_format_mem


class TestRegistry:
    def test_slurm_registered(self):
        assert get_executor_class("slurm") is SlurmExecutor

    def test_top_level_export(self):
        import cluster_api
        assert cluster_api.SlurmExecutor is SlurmExecutor


class TestSlurmFormatMem:
    def test_gigabytes(self):
        assert slurm_format_mem(8 * 1024**3) == "8192M"

    def test_rounds_up(self):
        assert slurm_format_mem(512 * 1024**2 + 1) == "513M"

    def test_sub_mebibyte_rounds_to_one(self):
        assert slurm_format_mem(10) == "1M"


class TestBuildHeader:
    def test_basic_header(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        lines = executor.build_header("test-job", ResourceSpec(work_dir="/w"))
        assert "#SBATCH --job-name=test-job" in lines
        assert '#SBATCH --output="/w/stdout.%j.log"' in lines
        assert '#SBATCH --error="/w/stderr.%j.log"' in lines
        assert "#SBATCH --nodes=1" in lines
        assert "#SBATCH --ntasks=1" in lines
        assert "#SBATCH --no-requeue" in lines
        assert "#SBATCH --partition=normal" in lines
        assert "#SBATCH --mem=8192M" in lines
        assert "#SBATCH --time=04:00:00" in lines
        assert '#SBATCH --chdir="/w"' in lines
        assert not any("--array" in line for line in lines)
        assert not any("%J" in line for line in lines)

    def test_with_resources(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        res = ResourceSpec(
            cpus=4, gpus=2, memory="16 GB", walltime="08:00:00",
            queue="long", work_dir="/scratch",
            extra_directives=["--account=proj1"],
        )
        lines = executor.build_header("test-job", res)
        assert "#SBATCH --partition=long" in lines
        assert "#SBATCH --cpus-per-task=4" in lines
        assert "#SBATCH --gres=gpu:2" in lines
        assert "#SBATCH --mem=16384M" in lines
        assert "#SBATCH --time=08:00:00" in lines
        assert '#SBATCH --chdir="/scratch"' in lines
        assert "#SBATCH --account=proj1" in lines

    def test_no_optional_resources(self):
        config = ClusterConfig(executor="slurm")
        executor = SlurmExecutor(config)
        lines = executor.build_header("j", ResourceSpec(work_dir="/w"))
        joined = "\n".join(lines)
        for flag in ("--partition", "--cpus-per-task", "--gres", "--mem", "--time"):
            assert flag not in joined

    def test_config_fallbacks(self):
        config = ClusterConfig(executor="slurm", cpus=2, gpus=1, memory="1 GB",
                               walltime="00:30:00", queue="gpu")
        executor = SlurmExecutor(config)
        lines = executor.build_header("j", ResourceSpec(work_dir="/w"))
        assert "#SBATCH --cpus-per-task=2" in lines
        assert "#SBATCH --gres=gpu:1" in lines
        assert "#SBATCH --mem=1024M" in lines
        assert "#SBATCH --time=00:30:00" in lines
        assert "#SBATCH --partition=gpu" in lines

    def test_explicit_log_paths(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        res = ResourceSpec(work_dir="/w", stdout_path="/logs/o.txt", stderr_path="/logs/e.txt")
        lines = executor.build_header("j", res)
        assert '#SBATCH --output="/logs/o.txt"' in lines
        assert '#SBATCH --error="/logs/e.txt"' in lines

    def test_array_header(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        lines = executor.build_header("arr", ResourceSpec(work_dir="/w"), array_spec="1-10%3")
        assert "#SBATCH --array=1-10%3" in lines
        assert '#SBATCH --output="/w/stdout.%A_%a.log"' in lines
        assert '#SBATCH --error="/w/stderr.%A_%a.log"' in lines
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_slurm.py -v`
Expected: FAIL at import with `ModuleNotFoundError: No module named 'cluster_api.executors.slurm'`.

- [ ] **Step 4: Create the module with class attributes and `build_header`**

Create `cluster_api/executors/slurm.py`:

```python
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
```

- [ ] **Step 5: Register and export**

In `cluster_api/executors/__init__.py` replace `_ensure_builtins`:

```python
def _ensure_builtins() -> None:
    """Lazily register built-in executors."""
    if {"lsf", "local", "slurm"} <= _REGISTRY.keys():
        return
    from .lsf import LSFExecutor
    from .local import LocalExecutor
    from .slurm import SlurmExecutor

    _REGISTRY.setdefault("lsf", LSFExecutor)
    _REGISTRY.setdefault("local", LocalExecutor)
    _REGISTRY.setdefault("slurm", SlurmExecutor)
```

In `cluster_api/__init__.py` add after the `LSFExecutor` import:

```python
from .executors.slurm import SlurmExecutor
```

and add `"SlurmExecutor",` to `__all__` after `"LSFExecutor",`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pixi run pytest tests/test_slurm.py -v`
Expected: all `TestRegistry`, `TestSlurmFormatMem`, `TestBuildHeader` PASS.

Run: `pixi run check`
Expected: lint clean, full suite passes.

- [ ] **Step 7: Commit**

```bash
git add cluster_api/executors/slurm.py cluster_api/executors/__init__.py cluster_api/__init__.py tests/conftest.py tests/test_slurm.py
git commit -m "SLURM: executor skeleton, registry entry, #SBATCH header builder

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Submission via `sbatch --parsable`

**Files:**
- Modify: `cluster_api/executors/slurm.py`
- Modify: `tests/test_slurm.py` (append)

**Interfaces:**
- Consumes: `build_header` from Task 1; `render_script(config, command, header_lines, prologue, epilogue)` and `write_script(directory, content, name, counter)` from `cluster_api/script.py`; `Executor._call(cmd, timeout, env, stdin_file)`.
- Produces:
  - `SlurmExecutor._submit_job(...)` and `_submit_array_job(...)` with the base-class signatures, returning `(job_id, script_path)`.
  - `SlurmExecutor._submit_args(resources, inherit_env, depends_on) -> list[str]`.
  - `SlurmExecutor._render(command, header, prologue, epilogue, env, inherit_env, login_shell) -> str`.
  - `_login_shebang(shebang: str) -> str` (module-level).
  - `SlurmExecutor._job_id_from_submit_output(out) -> str` override.
  - `SlurmExecutor._call` staticmethod override injecting `_SLURM_ENV`.

- [ ] **Step 1: Write the failing submission tests**

Replace the import block at the top of `tests/test_slurm.py` with:

```python
from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.config import ClusterConfig
from cluster_api.exceptions import SubmitError
from cluster_api.executors import get_executor_class
from cluster_api.executors.slurm import (
    _SLURM_ENV,
    SlurmExecutor,
    _login_shebang,
    slurm_format_mem,
)
```

Then append to the file:

```python
class TestLoginShebang:
    def test_bash(self):
        assert _login_shebang("#!/bin/bash") == "#!/bin/bash -l"

    def test_sh(self):
        assert _login_shebang("#!/bin/sh") == "#!/bin/sh -l"

    def test_already_login(self):
        assert _login_shebang("#!/bin/bash -l") == "#!/bin/bash -l"

    def test_env_style_left_alone(self, caplog):
        # Linux passes a single argument after the interpreter, so
        # "#!/usr/bin/env bash -l" would break; leave it and warn.
        with caplog.at_level("WARNING"):
            assert _login_shebang("#!/usr/bin/env bash") == "#!/usr/bin/env bash"
        assert "login_shell" in caplog.text

    def test_non_shell_left_alone(self, caplog):
        with caplog.at_level("WARNING"):
            assert _login_shebang("#!/usr/bin/python3") == "#!/usr/bin/python3"
        assert "login_shell" in caplog.text


class TestSubmission:

    async def test_submit(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(
            executor, "_call", new_callable=AsyncMock, return_value="12345",
        ) as mock_call:
            job = await executor.submit(
                command="echo hello", name="my-job",
                resources=ResourceSpec(work_dir=work_dir),
            )
            assert job.job_id == "12345"
            assert job.name == "test-my-job"
            assert job.status == JobStatus.PENDING
            cmd = mock_call.call_args[0][0]
            assert cmd[:2] == ["sbatch", "--parsable"]
            assert mock_call.call_args[1]["stdin_file"].endswith(".sh")
            with open(job.script_path) as f:
                script = f.read()
            assert script.startswith("#!/bin/bash\n")
            assert "#SBATCH --job-name=test-my-job" in script
            assert "#SBATCH --no-requeue" in script
            assert "echo hello" in script

    async def test_submit_parses_cluster_suffix_and_warning_lines(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        out = "sbatch: warning: something about the partition\n777;mycluster\n"
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=out):
            job = await executor.submit(
                command="true", name="j", resources=ResourceSpec(work_dir=work_dir),
            )
        assert job.job_id == "777"

    async def test_submit_unparseable_output_raises(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="error: nope"):
            with pytest.raises(SubmitError):
                await executor.submit(
                    command="true", name="j", resources=ResourceSpec(work_dir=work_dir),
                )

    async def test_submit_extra_args_order(self, work_dir):
        config = ClusterConfig(executor="slurm", extra_args=["--qos=high"])
        executor = SlurmExecutor(config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="1") as mock_call:
            await executor.submit(
                command="true", name="j",
                resources=ResourceSpec(work_dir=work_dir, extra_args=["--reservation=r1"]),
            )
        cmd = mock_call.call_args[0][0]
        assert cmd == ["sbatch", "--parsable", "--qos=high", "--reservation=r1"]

    async def test_submit_default_env_args_absent(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="1") as mock_call:
            job = await executor.submit(
                command="echo hello", name="j",
                resources=ResourceSpec(work_dir=work_dir), env={"FOO": "bar"},
            )
        cmd = mock_call.call_args[0][0]
        assert not any(a.startswith("--export") for a in cmd)
        # env reaches sbatch's own environment so --export=ALL carries it
        assert mock_call.call_args[1]["env"]["FOO"] == "bar"
        with open(job.script_path) as f:
            script = f.read()
        assert "SLURM_EXPORT_ENV" not in script
        assert script.startswith("#!/bin/bash\n")

    async def test_submit_clean_env_login_shell(self, slurm_config, work_dir):
        """inherit_env=False adds --export=NONE and moves env into the script;
        login_shell rewrites the shebang to bash -l."""
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="1") as mock_call:
            job = await executor.submit(
                command="echo hello", name="j",
                resources=ResourceSpec(work_dir=work_dir),
                env={"FOO": "bar baz"}, inherit_env=False, login_shell=True,
            )
        cmd = mock_call.call_args[0][0]
        assert "--export=NONE" in cmd
        assert "--get-user-env" not in cmd
        with open(job.script_path) as f:
            script = f.read()
        assert script.startswith("#!/bin/bash -l\n")
        assert "export SLURM_EXPORT_ENV=ALL" in script
        assert "export FOO='bar baz'" in script
        assert script.index("export SLURM_EXPORT_ENV") < script.index("export FOO")
        assert script.index("export FOO") < script.index("echo hello")

    async def test_clean_env_without_env_still_sets_export_env(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="1"):
            job = await executor.submit(
                command="echo hello", name="j",
                resources=ResourceSpec(work_dir=work_dir), inherit_env=False,
            )
        with open(job.script_path) as f:
            script = f.read()
        assert "export SLURM_EXPORT_ENV=ALL" in script

    async def test_submit_with_dependencies(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="3") as mock_call:
            job = await executor.submit(
                command="true", name="j", resources=ResourceSpec(work_dir=work_dir),
                depends_on=["1", "2"],
            )
        cmd = mock_call.call_args[0][0]
        assert "--dependency=afterok:1:2" in cmd
        assert "--kill-on-invalid-dep=yes" in cmd
        assert job.depends_on == ["1", "2"]

    async def test_submit_array(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="12345") as mock_call:
            job = await executor.submit_array(
                command="python process.py --index $SLURM_ARRAY_TASK_ID",
                name="batch", array_range=(1, 50), max_concurrent=5,
                resources=ResourceSpec(work_dir=work_dir),
                depends_on=["9"],
            )
        assert job.job_id == "12345"
        assert job.metadata["array_range"] == (1, 50)
        assert job.metadata["max_concurrent"] == 5
        cmd = mock_call.call_args[0][0]
        assert "--dependency=afterok:9" in cmd
        with open(job.script_path) as f:
            script = f.read()
        assert "#SBATCH --array=1-50%5" in script
        assert "stdout.%A_%a.log" in script
        assert "stderr.%A_%a.log" in script
        assert "%j" not in script

    async def test_submit_array_no_throttle(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="1"):
            job = await executor.submit_array(
                command="true", name="batch", array_range=(0, 3),
                resources=ResourceSpec(work_dir=work_dir),
            )
        with open(job.script_path) as f:
            script = f.read()
        assert "#SBATCH --array=0-3\n" in script


class TestCallEnv:
    async def test_call_injects_slurm_env(self):
        """Every SLURM CLI call pins timestamp format and disables bitstring truncation."""
        captured = {}

        async def fake_base_call(cmd, timeout=100.0, env=None, stdin_file=None):
            captured["env"] = env
            return ""

        with patch("cluster_api.executors.slurm.Executor._call", side_effect=fake_base_call):
            await SlurmExecutor._call(["sacct"], env={"EXTRA": "1"})
        assert captured["env"]["SLURM_TIME_FORMAT"] == "standard"
        assert captured["env"]["SLURM_BITSTR_LEN"] == "0"
        assert captured["env"]["EXTRA"] == "1"
        assert _SLURM_ENV == {"SLURM_TIME_FORMAT": "standard", "SLURM_BITSTR_LEN": "0"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_slurm.py -v -k "Submission or LoginShebang or CallEnv"`
Expected: FAIL with `ImportError: cannot import name '_login_shebang'`.

- [ ] **Step 3: Implement submission**

In `cluster_api/executors/slurm.py`, extend the imports:

```python
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
```

Add after `logger = ...`:

```python
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
```

Add these methods to `SlurmExecutor` after `build_header`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run pytest tests/test_slurm.py -v`
Expected: all PASS.

Run: `pixi run check`
Expected: lint clean, full suite passes.

- [ ] **Step 5: Commit**

```bash
git add cluster_api/executors/slurm.py tests/test_slurm.py
git commit -m "SLURM: sbatch --parsable submission, env/login-shell/dependency flags, array headers

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Status polling via `sacct`

**Files:**
- Modify: `cluster_api/executors/slurm.py`
- Modify: `tests/test_slurm.py` (append)

**Interfaces:**
- Consumes: base `Executor.poll()` which calls `_build_status_args()` then `_parse_job_statuses(out)` and routes `"123[4]"` keys to `array_elements`.
- Produces:
  - `_SLURM_STATUS_MAP: dict[str, JobStatus]`.
  - `_SACCT_FIELDS = "JobID,JobName,State,ExitCode,NodeList,Submit,Start,End"`.
  - `expand_array_expr(expr: str) -> list[int]`.
  - `_expand_job_id(job_id: str) -> list[str]` returning base-class keys (`"123"`, `"123[4]"`, ...) or `[]` when unparseable.
  - `_parse_exit_code(raw: str) -> int | None`, `_parse_slurm_time(raw: str) -> datetime | None`, `_clean_field(raw) -> str | None`.
  - `SlurmExecutor._sacct_args(job_ids: list[str]) -> list[str]` (also used by Task 5).
  - `SlurmExecutor._build_status_args()` and `_parse_job_statuses(output)`; the meta dict carries keys `exec_host, exit_code, submit_time, start_time, finish_time, job_name`.

- [ ] **Step 1: Write the failing parsing tests**

Replace the import block at the top of `tests/test_slurm.py` with:

```python
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.config import ClusterConfig
from cluster_api.exceptions import SubmitError
from cluster_api.executors import get_executor_class
from cluster_api.executors.slurm import (
    _SACCT_FIELDS,
    _SLURM_ENV,
    _SLURM_STATUS_MAP,
    SlurmExecutor,
    _expand_job_id,
    _login_shebang,
    _parse_exit_code,
    _parse_slurm_time,
    expand_array_expr,
    slurm_format_mem,
)
```

Then append to the file:

```python
def _row(job_id, name="test-j", state="RUNNING", exit_code="0:0",
         nodelist="node01", submit="2026-10-21T10:00:00",
         start="2026-10-21T10:01:00", end="Unknown"):
    return "|".join([job_id, name, state, exit_code, nodelist, submit, start, end])


class TestStatusMap:
    @pytest.mark.parametrize("state", [
        "PENDING", "CONFIGURING", "REQUEUED", "REQUEUE_HOLD", "REQUEUE_FED",
        "RESV_DEL_HOLD", "SPECIAL_EXIT",
    ])
    def test_pending(self, state):
        assert _SLURM_STATUS_MAP[state] == JobStatus.PENDING

    @pytest.mark.parametrize("state", [
        "RUNNING", "COMPLETING", "SUSPENDED", "STOPPED", "SIGNALING",
        "STAGE_OUT", "RESIZING",
    ])
    def test_running(self, state):
        assert _SLURM_STATUS_MAP[state] == JobStatus.RUNNING

    def test_done(self):
        assert _SLURM_STATUS_MAP["COMPLETED"] == JobStatus.DONE

    @pytest.mark.parametrize("state", [
        "FAILED", "BOOT_FAIL", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT",
        "DEADLINE", "REVOKED",
    ])
    def test_failed(self, state):
        assert _SLURM_STATUS_MAP[state] == JobStatus.FAILED

    @pytest.mark.parametrize("state", ["CANCELLED", "PREEMPTED"])
    def test_killed(self, state):
        assert _SLURM_STATUS_MAP[state] == JobStatus.KILLED


class TestHelpers:
    def test_expand_array_expr(self):
        assert expand_array_expr("0-2") == [0, 1, 2]
        assert expand_array_expr("3,5,7-8") == [3, 5, 7, 8]
        assert expand_array_expr("0-10:5") == [0, 5, 10]
        assert expand_array_expr("4") == [4]

    def test_expand_job_id(self, caplog):
        assert _expand_job_id("123") == ["123"]
        assert _expand_job_id("123_4") == ["123[4]"]
        assert _expand_job_id("123_[3-5,8%2]") == ["123[3]", "123[4]", "123[5]", "123[8]"]
        assert _expand_job_id("123_[0-10:2]") == ["123[0]", "123[2]", "123[4]", "123[6]", "123[8]", "123[10]"]
        with caplog.at_level("WARNING"):
            assert _expand_job_id("123+1") == []
        assert "123+1" in caplog.text

    def test_parse_exit_code(self):
        assert _parse_exit_code("0:0") == 0
        assert _parse_exit_code("1:0") == 1
        assert _parse_exit_code("42:0") == 42
        assert _parse_exit_code("0:9") == -9
        assert _parse_exit_code("") is None
        assert _parse_exit_code("garbage") is None

    def test_parse_slurm_time(self):
        dt = _parse_slurm_time("2026-10-21T10:00:00")
        assert dt is not None and dt.tzinfo == timezone.utc
        expected = datetime(2026, 10, 21, 10, 0, 0).astimezone(timezone.utc)
        assert dt == expected
        assert _parse_slurm_time("Unknown") is None
        assert _parse_slurm_time("None") is None
        assert _parse_slurm_time("") is None


class TestStatusArgs:
    def test_queries_only_active_ids(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        executor.track("100", status=JobStatus.RUNNING)
        executor.track("101", status=JobStatus.DONE)
        executor.track("102", status=JobStatus.PENDING)
        args = executor._build_status_args()
        assert args == ["sacct", "-n", "-P", "-X", "-j", "100,102", "-o", _SACCT_FIELDS]

    def test_all_users(self):
        config = ClusterConfig(executor="slurm", poll_all_users=True)
        executor = SlurmExecutor(config)
        executor.track("5")
        args = executor._build_status_args()
        assert "--allusers" in args
        assert args[-4:] == ["-j", "5", "-o", _SACCT_FIELDS]


class TestParseJobStatuses:
    def test_running_job(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        result = executor._parse_job_statuses(_row("123"))
        status, meta = result["123"]
        assert status == JobStatus.RUNNING
        assert meta["exec_host"] == "node01"
        assert meta["exit_code"] == 0
        assert meta["job_name"] == "test-j"
        assert meta["start_time"] is not None
        assert meta["finish_time"] is None

    def test_completed_job_empty_exit_code_is_zero(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        result = executor._parse_job_statuses(
            _row("123", state="COMPLETED", exit_code="", end="2026-10-21T10:05:00"))
        status, meta = result["123"]
        assert status == JobStatus.DONE
        assert meta["exit_code"] == 0
        assert meta["finish_time"] is not None

    def test_failed_job(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        result = executor._parse_job_statuses(_row("123", state="FAILED", exit_code="42:0"))
        assert result["123"][0] == JobStatus.FAILED
        assert result["123"][1]["exit_code"] == 42

    def test_cancelled_by_uid(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        result = executor._parse_job_statuses(
            _row("123", state="CANCELLED by 1234", exit_code="0:15"))
        assert result["123"][0] == JobStatus.KILLED
        assert result["123"][1]["exit_code"] == -15

    @pytest.mark.parametrize("state,expected", [
        ("TIMEOUT", JobStatus.FAILED),
        ("OUT_OF_MEMORY", JobStatus.FAILED),
        ("PREEMPTED", JobStatus.KILLED),
        ("PENDING", JobStatus.PENDING),
    ])
    def test_other_states(self, slurm_config, state, expected):
        executor = SlurmExecutor(slurm_config)
        result = executor._parse_job_statuses(_row("9", state=state))
        assert result["9"][0] == expected

    def test_unknown_state_logged(self, slurm_config, caplog):
        executor = SlurmExecutor(slurm_config)
        with caplog.at_level("WARNING"):
            result = executor._parse_job_statuses(_row("9", state="WEIRD"))
        assert result["9"][0] == JobStatus.UNKNOWN
        assert "WEIRD" in caplog.text

    def test_pending_nodelist_none_assigned(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        result = executor._parse_job_statuses(
            _row("9", state="PENDING", nodelist="None assigned", start="Unknown"))
        assert result["9"][1]["exec_host"] is None
        assert result["9"][1]["start_time"] is None

    def test_array_rows(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        output = "\n".join([
            _row("200_0", state="COMPLETED", exit_code="0:0", end="2026-10-21T10:02:00"),
            _row("200_1", state="FAILED", exit_code="1:0"),
            _row("200_2", state="RUNNING"),
            _row("200_[3-5%2]", state="PENDING", nodelist="None assigned", start="Unknown"),
        ])
        result = executor._parse_job_statuses(output)
        assert set(result) == {"200[0]", "200[1]", "200[2]", "200[3]", "200[4]", "200[5]"}
        assert result["200[0]"][0] == JobStatus.DONE
        assert result["200[1]"][0] == JobStatus.FAILED
        assert result["200[1]"][1]["exit_code"] == 1
        assert result["200[4]"][0] == JobStatus.PENDING

    def test_step_rows_skipped(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        output = "\n".join([_row("123"), _row("123.batch"), _row("123.0")])
        result = executor._parse_job_statuses(output)
        assert list(result) == ["123"]

    def test_short_line_skipped(self, slurm_config, caplog):
        executor = SlurmExecutor(slurm_config)
        with caplog.at_level("WARNING"):
            result = executor._parse_job_statuses("123|name|RUNNING\n" + _row("124"))
        assert list(result) == ["124"]
        assert "sacct" in caplog.text

    def test_empty_output(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        assert executor._parse_job_statuses("") == {}
        assert executor._parse_job_statuses("\n\n") == {}


class TestPollEndToEnd:
    async def test_poll_updates_single_and_array_records(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="300"):
            single = await executor.submit(command="true", name="s",
                                           resources=ResourceSpec(work_dir=work_dir))
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="400"):
            arr = await executor.submit_array(command="true", name="a", array_range=(0, 1),
                                              resources=ResourceSpec(work_dir=work_dir))
        output = "\n".join([
            _row("300", state="FAILED", exit_code="0:9", end="2026-10-21T10:02:00"),
            _row("400_0", state="COMPLETED", exit_code="0:0", end="2026-10-21T10:02:00"),
            _row("400_1", state="COMPLETED", exit_code="0:0", end="2026-10-21T10:02:00"),
        ])
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            await executor.poll()
        assert single.status == JobStatus.FAILED
        assert single.exit_code == -9
        assert single.exec_host == "node01"
        assert single.finish_time is not None
        assert arr.status == JobStatus.DONE
        assert arr.array_elements[0].exit_code == 0
        assert arr.array_elements[1].status == JobStatus.DONE
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_slurm.py -v -k "StatusMap or Helpers or StatusArgs or ParseJobStatuses or PollEndToEnd"`
Expected: FAIL with `ImportError: cannot import name '_SACCT_FIELDS'`.

- [ ] **Step 3: Implement status parsing**

In `cluster_api/executors/slurm.py`, extend imports:

```python
from datetime import datetime, timezone
from typing import Any

from .._types import JobStatus, ResourceSpec
```

Add after `_SLURM_ENV`:

```python
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
        return [f"{parent}[{i}]" for i in expand_array_expr(m.group("expr"))]
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
```

Add these methods to `SlurmExecutor` after `_submit_array_job`:

```python
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
            for key in _expand_job_id(job_id):
                result[key] = (status, dict(meta))
        return result
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run pytest tests/test_slurm.py -v`
Expected: all PASS.

Run: `pixi run check`
Expected: lint clean, full suite passes.

- [ ] **Step 5: Commit**

```bash
git add cluster_api/executors/slurm.py tests/test_slurm.py
git commit -m "SLURM: sacct status polling with array expansion and signal-aware exit codes

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Cancellation

**Files:**
- Modify: `cluster_api/executors/slurm.py`
- Modify: `tests/test_slurm.py` (append)

**Interfaces:**
- Consumes: base `Executor.cancel(job_id, done=False)` which calls `_cancel_job` then marks the record; `CommandFailedError` from `cluster_api/exceptions.py`.
- Produces: `SlurmExecutor._cancel_job(job_id, *, done=False)`, `SlurmExecutor.cancel_by_name(name)`.

- [ ] **Step 1: Write the failing cancel tests**

Add `CommandFailedError` to the exceptions import at the top of `tests/test_slurm.py`:

```python
from cluster_api.exceptions import CommandFailedError, SubmitError
```

Then append to the file:

```python
class TestCancel:

    async def test_cancel(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="123"):
            job = await executor.submit(command="sleep 60", name="c",
                                        resources=ResourceSpec(work_dir=work_dir))
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="") as mock_call:
            await executor.cancel(job.job_id)
        assert mock_call.call_args[0][0] == ["scancel", "123"]
        assert job.status == JobStatus.KILLED

    async def test_cancel_done_has_no_extra_flag(self, slurm_config, work_dir):
        """SLURM has no bkill -d equivalent; the record is still marked DONE."""
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="123"):
            job = await executor.submit(command="sleep 60", name="c",
                                        resources=ResourceSpec(work_dir=work_dir))
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="") as mock_call:
            await executor.cancel(job.job_id, done=True)
        assert mock_call.call_args[0][0] == ["scancel", "123"]
        assert job.status == JobStatus.DONE

    async def test_cancel_by_name(self, slurm_config, work_dir):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="1"):
            a = await executor.submit(command="sleep 60", name="alpha",
                                      resources=ResourceSpec(work_dir=work_dir))
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="2"):
            b = await executor.submit(command="sleep 60", name="beta",
                                      resources=ResourceSpec(work_dir=work_dir))
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="") as mock_call:
            await executor.cancel_by_name("test-alpha")
        assert mock_call.call_args[0][0] == ["scancel", "--name=test-alpha"]
        assert a.status == JobStatus.KILLED
        assert b.status == JobStatus.PENDING

    async def test_cancel_by_name_glob_warns(self, slurm_config, caplog):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="") as mock_call:
            with caplog.at_level("WARNING"):
                await executor.cancel_by_name("test-*")
        assert mock_call.call_args[0][0] == ["scancel", "--name=test-*"]
        assert "exact" in caplog.text

    async def test_cancel_by_name_no_match_swallowed(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        err = CommandFailedError(
            "Command failed (exit 1): scancel\nstderr: scancel: error: No active jobs match ALL job filters",
            stdout="",
        )
        with patch.object(executor, "_call", new_callable=AsyncMock, side_effect=err):
            await executor.cancel_by_name("nothing")  # must not raise

    async def test_cancel_by_name_other_error_raises(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        err = CommandFailedError("Command failed (exit 1): scancel\nstderr: Access denied", stdout="")
        with patch.object(executor, "_call", new_callable=AsyncMock, side_effect=err):
            with pytest.raises(CommandFailedError):
                await executor.cancel_by_name("x")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_slurm.py -v -k Cancel`
Expected: `test_cancel` FAILS with `NotImplementedError: cancel is not supported by this executor`; `cancel_by_name` tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement cancellation**

Extend imports in `cluster_api/executors/slurm.py`:

```python
from ..exceptions import CommandFailedError, SubmitError
```

Add to `SlurmExecutor` after `_parse_job_statuses`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run pytest tests/test_slurm.py -v`
Expected: all PASS.

Run: `pixi run check`
Expected: lint clean, full suite passes.

- [ ] **Step 5: Commit**

```bash
git add cluster_api/executors/slurm.py tests/test_slurm.py
git commit -m "SLURM: scancel by id and by exact name

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Reconnect via `squeue` discovery + `sacct` fetch

**Files:**
- Modify: `cluster_api/executors/slurm.py`
- Create: `tests/test_slurm_reconnect.py`

**Interfaces:**
- Consumes: `_sacct_args(job_ids)` and `_parse_job_statuses` from Task 3; `_ARRAY_ELEMENT_RE` from `cluster_api/core.py`; `ArrayElement`, `JobRecord`, `_TERMINAL_STATUSES` from `cluster_api/_types.py`; `ClusterAPIError`.
- Produces: `SlurmExecutor._build_discovery_args() -> list[str]`, `SlurmExecutor.reconnect() -> list[JobRecord]`.

- [ ] **Step 1: Write the failing reconnect tests**

Create `tests/test_slurm_reconnect.py`:

```python
"""Tests for SlurmExecutor.reconnect(): squeue discovery + sacct fetch."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus
from cluster_api.config import ClusterConfig
from cluster_api.exceptions import ClusterAPIError
from cluster_api.executors.slurm import _SACCT_FIELDS, SlurmExecutor


def _row(job_id, name="test-j", state="RUNNING", exit_code="0:0",
         nodelist="node01", submit="2026-10-21T10:00:00",
         start="2026-10-21T10:01:00", end="Unknown"):
    return "|".join([job_id, name, state, exit_code, nodelist, submit, start, end])


def _fake_calls(squeue_out, sacct_out):
    """Async stand-in for _call: squeue output first, sacct output second.

    Records the sacct command on ``fake.sacct_cmd`` so tests can assert on
    it after the patch context has exited.
    """
    async def fake_call(cmd, **kwargs):
        if cmd[0] == "squeue":
            return squeue_out
        assert cmd[0] == "sacct"
        fake_call.sacct_cmd = cmd
        return sacct_out
    fake_call.sacct_cmd = None
    return fake_call


class TestDiscoveryArgs:
    def test_me_by_default(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        assert executor._build_discovery_args() == ["squeue", "-h", "-r", "-o", "%F|%j", "--me"]

    def test_all_users(self):
        executor = SlurmExecutor(ClusterConfig(executor="slurm", job_name_prefix="test",
                                               poll_all_users=True))
        assert executor._build_discovery_args() == ["squeue", "-h", "-r", "-o", "%F|%j"]

    def test_requires_prefix(self):
        executor = SlurmExecutor(ClusterConfig(executor="slurm"))
        with pytest.raises(ClusterAPIError):
            executor._build_discovery_args()


class TestReconnect:

    async def test_single_running_job(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        fake = _fake_calls("12345|test-myjob\n", _row("12345", name="test-myjob"))
        with patch.object(executor, "_call", side_effect=fake):
            jobs = await executor.reconnect()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_id == "12345"
        assert job.name == "test-myjob"
        assert job.status == JobStatus.RUNNING
        assert job.exec_host == "node01"
        assert job.metadata["reconnected"] is True
        assert "12345" in executor.jobs
        assert fake.sacct_cmd == executor._sacct_args(["12345"])

    async def test_filters_by_prefix(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        squeue = "1|test-mine\n2|other-notmine\n3|testing-close-but-no\n"
        fake = _fake_calls(squeue, _row("1", name="test-mine"))
        with patch.object(executor, "_call", side_effect=fake):
            jobs = await executor.reconnect()
        assert [j.job_id for j in jobs] == ["1"]
        assert fake.sacct_cmd[-3] == "1"

    async def test_held_and_dependency_pending_jobs_found(self, slurm_config):
        """squeue lists pending jobs regardless of EligibleTime; sacct -s would not."""
        executor = SlurmExecutor(slurm_config)
        squeue = "10|test-held\n11|test-dep\n"
        sacct = "\n".join([
            _row("10", name="test-held", state="PENDING", nodelist="None assigned", start="Unknown"),
            _row("11", name="test-dep", state="PENDING", nodelist="None assigned", start="Unknown"),
        ])
        with patch.object(executor, "_call", side_effect=_fake_calls(squeue, sacct)):
            jobs = await executor.reconnect()
        assert {j.job_id for j in jobs} == {"10", "11"}
        assert all(j.status == JobStatus.PENDING for j in jobs)

    async def test_array_dedup_and_full_extent(self, slurm_config):
        """squeue -r lists one line per element with %F = parent; the
        unfiltered sacct fetch returns finished elements too, so the
        reconstructed array has its full extent and history."""
        executor = SlurmExecutor(slurm_config)
        squeue = "500|test-arr\n500|test-arr\n500|test-arr\n"
        sacct = "\n".join([
            _row("500_0", name="test-arr", state="COMPLETED", end="2026-10-21T10:02:00"),
            _row("500_1", name="test-arr", state="FAILED", exit_code="1:0", end="2026-10-21T10:02:00"),
            _row("500_2", name="test-arr", state="RUNNING"),
            _row("500_[3-4]", name="test-arr", state="PENDING", nodelist="None assigned", start="Unknown"),
        ])
        fake = _fake_calls(squeue, sacct)
        with patch.object(executor, "_call", side_effect=fake):
            jobs = await executor.reconnect()
        assert len(jobs) == 1
        arr = jobs[0]
        assert arr.job_id == "500"
        assert arr.is_array
        assert arr.metadata["array_range"] == (0, 4)
        assert arr.element_count == 5
        assert arr.completed_elements == 2
        assert arr.failed_element_indices == [1]
        assert arr.status == JobStatus.RUNNING
        assert fake.sacct_cmd[-3] == "500"

    async def test_all_terminal_array_skipped(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        sacct = "\n".join([
            _row("600_0", name="test-arr", state="COMPLETED", end="2026-10-21T10:02:00"),
            _row("600_1", name="test-arr", state="COMPLETED", end="2026-10-21T10:02:00"),
        ])
        with patch.object(executor, "_call", side_effect=_fake_calls("600|test-arr\n", sacct)):
            assert await executor.reconnect() == []

    async def test_terminal_single_skipped(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        fake = _fake_calls("7|test-done\n", _row("7", name="test-done", state="COMPLETED"))
        with patch.object(executor, "_call", side_effect=fake):
            assert await executor.reconnect() == []

    async def test_already_tracked_not_duplicated(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        existing = executor.track("12345", status=JobStatus.RUNNING)
        fake = _fake_calls("12345|test-myjob\n99|test-new\n", _row("99", name="test-new"))
        with patch.object(executor, "_call", side_effect=fake):
            jobs = await executor.reconnect()
        assert [j.job_id for j in jobs] == ["99"]
        assert executor.jobs["12345"] is existing
        assert fake.sacct_cmd[-3] == "99"

    async def test_empty_squeue(self, slurm_config):
        executor = SlurmExecutor(slurm_config)
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value="") as mock_call:
            assert await executor.reconnect() == []
        mock_call.assert_called_once()
        assert mock_call.call_args[0][0][0] == "squeue"

    async def test_no_prefix_raises(self):
        executor = SlurmExecutor(ClusterConfig(executor="slurm"))
        with pytest.raises(ClusterAPIError):
            await executor.reconnect()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_slurm_reconnect.py -v`
Expected: `TestDiscoveryArgs` FAIL with `AttributeError: 'SlurmExecutor' object has no attribute '_build_discovery_args'`; `TestReconnect` FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement reconnect**

Extend imports in `cluster_api/executors/slurm.py`:

```python
from .._types import ArrayElement, JobRecord, JobStatus, ResourceSpec, _TERMINAL_STATUSES
from ..core import Executor, _ARRAY_ELEMENT_RE
from ..exceptions import ClusterAPIError, CommandFailedError, SubmitError
```

Add to `SlurmExecutor` after `cancel_by_name`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run pytest tests/test_slurm_reconnect.py tests/test_slurm.py -v`
Expected: all PASS.

Run: `pixi run check`
Expected: lint clean, full suite passes.

- [ ] **Step 5: Commit**

```bash
git add cluster_api/executors/slurm.py tests/test_slurm_reconnect.py
git commit -m "SLURM: reconnect via squeue discovery and sacct fetch

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Integration tests and CI job

**Files:**
- Create: `tests/test_slurm_integration.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the finished `SlurmExecutor`; `create_executor`, `JobMonitor`, `JobStatus`, `ResourceSpec` from `cluster_api`.
- Produces: integration module honouring `CLUSTER_API_REQUIRE_SLURM=1`; CI job `slurm-integration`.

- [ ] **Step 1: Write the integration test module**

Create `tests/test_slurm_integration.py`:

```python
"""Integration tests that submit real jobs to a SLURM cluster.

Run with: pixi run test-integration

Skipped by default in `pixi run test`, and skipped at module level when
`sbatch` is not on PATH — unless CLUSTER_API_REQUIRE_SLURM=1 is set (CI),
in which case a missing scheduler fails the module instead of skipping.
Requires slurmdbd accounting (sacct).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from cluster_api import JobMonitor, JobStatus, ResourceSpec, create_executor

pytestmark = pytest.mark.integration

_REQUIRE = os.environ.get("CLUSTER_API_REQUIRE_SLURM") == "1"

if not shutil.which("sbatch"):
    if _REQUIRE:
        pytest.fail("CLUSTER_API_REQUIRE_SLURM=1 but sbatch is not on PATH")
    pytest.skip("SLURM not available (sbatch not on PATH)", allow_module_level=True)

_OUTPUT_DIR = Path(__file__).resolve().parent.parent / ".test_output"
_PREFIX = f"capitest{os.getpid()}"


@pytest.fixture
def work_dir():
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return str(_OUTPUT_DIR)


def _make_executor(**overrides):
    return create_executor(
        config_path=None,
        executor="slurm",
        job_name_prefix=_PREFIX,
        poll_interval=2.0,
        walltime="00:05:00",   # HH:MM:SS; on SLURM "05:00" would mean five minutes
        command_timeout=30.0,
        **overrides,
    )


@pytest.fixture
def slurm_executor():
    return _make_executor()


@pytest.fixture
def monitor(slurm_executor):
    return JobMonitor(slurm_executor, poll_interval=2.0)


def _read_log(work_dir: str, job_id: str) -> str:
    return (Path(work_dir) / f"stdout.{job_id}.log").read_text()


class TestAccountingAvailable:
    def test_sacct_works(self):
        """Fails loudly when slurmdbd accounting is not configured."""
        proc = subprocess.run(["sacct", "-n", "-X", "-j", "1", "-o", "JobID"],
                              capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr


class TestSubmitAndMonitor:

    async def test_short_sleep_succeeds(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            results = []
            job = await slurm_executor.submit(
                command="sleep 3 && echo 'job finished'", name="sleep-ok",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            job.on_success(lambda j: results.append("success"))
            job.on_failure(lambda j: results.append("failure"))
            await monitor.wait_for(job, timeout=180.0)
            assert job.status == JobStatus.DONE
            assert job.exit_code == 0
            assert job.exec_host
            assert job.start_time is not None and job.finish_time is not None
            assert results == ["success"]
            assert "job finished" in _read_log(work_dir, job.job_id)
        finally:
            await monitor.stop()

    async def test_failing_job(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command="echo 'about to fail' && exit 42", name="fail",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=180.0)
            assert job.status == JobStatus.FAILED
            assert job.exit_code == 42
        finally:
            await monitor.stop()

    async def test_signal_killed_job_reports_negative_exit_code(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command="kill -9 $$", name="sigkill",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=180.0)
            assert job.status == JobStatus.FAILED
            assert job.exit_code == -9
        finally:
            await monitor.stop()


class TestCancel:

    async def test_cancel_running_job(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command="sleep 120", name="cancel-me",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            # Let it start so sacct has a row
            await asyncio.sleep(6)
            await slurm_executor.cancel(job.job_id)
            assert job.status == JobStatus.KILLED
            await asyncio.sleep(6)
            await slurm_executor.poll()
            assert job.status == JobStatus.KILLED
        finally:
            await monitor.stop()


class TestJobArray:

    async def test_submit_array(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit_array(
                command='echo "element $SLURM_ARRAY_TASK_ID"', name="arr",
                array_range=(1, 3),
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=240.0)
            assert job.status == JobStatus.DONE
            assert sorted(job.array_elements) == [1, 2, 3]
            assert all(e.status == JobStatus.DONE for e in job.array_elements.values())
            assert all(e.exit_code == 0 for e in job.array_elements.values())
            log = (Path(work_dir) / f"stdout.{job.job_id}_2.log").read_text()
            assert "element 2" in log
        finally:
            await monitor.stop()


class TestDependencies:

    async def test_chain_success(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            a = await slurm_executor.submit(command="sleep 2 && echo A", name="dep-a",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir))
            b = await slurm_executor.submit(command="echo B", name="dep-b",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir),
                                            depends_on=[a])
            await monitor.wait_for(a, b, timeout=300.0)
            assert a.status == JobStatus.DONE
            assert b.status == JobStatus.DONE
            assert b.start_time >= a.finish_time
        finally:
            await monitor.stop()

    async def test_chain_failure_cancels_dependent(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            a = await slurm_executor.submit(command="exit 1", name="dep-fail-a",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir))
            b = await slurm_executor.submit(command="echo never", name="dep-fail-b",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir),
                                            depends_on=[a])
            await monitor.wait_for(a, b, timeout=300.0)
            assert a.status == JobStatus.FAILED
            assert b.status == JobStatus.KILLED
            assert b.metadata["dependency_failed"] == [a.job_id]
        finally:
            await monitor.stop()


class TestEnvironment:

    async def test_clean_env(self, slurm_executor, monitor, work_dir):
        os.environ["CAPI_MARKER"] = "present"
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command='echo "FOO=$FOO"; echo "MARKER=${CAPI_MARKER:-absent}"',
                name="clean-env",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
                env={"FOO": "bar"}, inherit_env=False,
            )
            await monitor.wait_for(job, timeout=180.0)
            log = _read_log(work_dir, job.job_id)
            assert "FOO=bar" in log
            assert "MARKER=absent" in log
        finally:
            await monitor.stop()
            os.environ.pop("CAPI_MARKER", None)

    async def test_inherit_env_default(self, slurm_executor, monitor, work_dir):
        os.environ["CAPI_MARKER"] = "present"
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command='echo "MARKER=${CAPI_MARKER:-absent}"', name="inherit-env",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=180.0)
            assert "MARKER=present" in _read_log(work_dir, job.job_id)
        finally:
            await monitor.stop()
            os.environ.pop("CAPI_MARKER", None)

    async def test_login_shell(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command='shopt -q login_shell && echo LOGIN=yes || echo LOGIN=no',
                name="login-shell",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
                login_shell=True,
            )
            await monitor.wait_for(job, timeout=180.0)
            assert "LOGIN=yes" in _read_log(work_dir, job.job_id)
        finally:
            await monitor.stop()


class TestReconnect:

    async def test_reconnect_finds_running_held_and_partial_array(self, slurm_executor, work_dir):
        running = await slurm_executor.submit(command="sleep 90", name="rc-run",
                                              resources=ResourceSpec(cpus=1, work_dir=work_dir))
        held = await slurm_executor.submit(command="echo held", name="rc-held",
                                           resources=ResourceSpec(cpus=1, work_dir=work_dir,
                                                                  extra_args=["--hold"]))
        arr = await slurm_executor.submit_array(
            command='[ "$SLURM_ARRAY_TASK_ID" = 1 ] && exit 0; sleep 90', name="rc-arr",
            array_range=(1, 3), max_concurrent=1,
            resources=ResourceSpec(cpus=1, work_dir=work_dir),
        )
        try:
            # let element 1 finish and element 2 start
            await asyncio.sleep(15)
            fresh = _make_executor()
            recovered = await fresh.reconnect()
            by_id = {r.job_id: r for r in recovered}
            assert running.job_id in by_id
            assert held.job_id in by_id
            assert by_id[held.job_id].status == JobStatus.PENDING
            assert arr.job_id in by_id
            rec_arr = by_id[arr.job_id]
            assert rec_arr.is_array
            assert rec_arr.metadata["array_range"] == (1, 3)
            assert rec_arr.array_elements[1].status == JobStatus.DONE
            assert rec_arr.status == JobStatus.RUNNING
        finally:
            await slurm_executor.cancel_all()
```

- [ ] **Step 2: Run the module locally to confirm it skips cleanly without SLURM**

Run: `pixi run pytest tests/test_slurm_integration.py -m integration -v`
Expected: `SKIPPED ... SLURM not available (sbatch not on PATH)`, exit code 0.

Run: `CLUSTER_API_REQUIRE_SLURM=1 pixi run pytest tests/test_slurm_integration.py -m integration -v`
Expected: collection error `Failed: CLUSTER_API_REQUIRE_SLURM=1 but sbatch is not on PATH`, non-zero exit code.

Run: `pixi run lint`
Expected: clean.

- [ ] **Step 3: Add the CI job**

Append to `.github/workflows/ci.yml` under `jobs:` (after the `check` job, same indentation):

```yaml
  slurm-integration:
    runs-on: ubuntu-latest

    services:
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: root
        ports:
          - "8888:3306"
        options: --health-cmd="mysqladmin ping" --health-interval=10s --health-timeout=5s --health-retries=3

    steps:
    - uses: actions/checkout@v5

    - name: Set up SLURM (single node with slurmdbd accounting)
      uses: koesterlab/setup-slurm-action@v1

    - name: Set up Pixi
      uses: prefix-dev/setup-pixi@v0.9.0
      with:
        pixi-version: v0.70.2
        cache: true

    - name: Smoke-test scheduler and accounting
      run: |
        sinfo
        sacct -n -X -j 1 -o JobID

    - name: SLURM integration tests
      env:
        CLUSTER_API_REQUIRE_SLURM: "1"
      run: pixi run test-integration
```

(The mysql service block is the one `koesterlab/setup-slurm-action`'s README requires verbatim.)

- [ ] **Step 4: Validate the workflow YAML parses**

Run: `pixi run python -c "import yaml,sys; d=yaml.safe_load(open('.github/workflows/ci.yml')); print(sorted(d['jobs']))"`
Expected: `['check', 'slurm-integration']`.

- [ ] **Step 5: Commit, push the branch, and watch the CI job**

```bash
git add tests/test_slurm_integration.py .github/workflows/ci.yml
git commit -m "SLURM: integration tests and GitHub Actions job on a real single-node SLURM

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

Open a pull request (or push the feature branch) so `slurm-integration` runs. Then:

Run: `gh run watch` (or `gh pr checks --watch`)
Expected: both `check` and `slurm-integration` green. If `slurm-integration` fails, read the failing test's output with `gh run view --log-failed`; it is the first real-SLURM feedback this executor gets, so fix the executor (not the test) unless the test's assumption about the CI node is wrong (e.g. the partition has no `--hold` support). Commit each fix separately with a message starting `SLURM: fix ...`. Do not mark this task complete until the job is green.

---

### Task 7: Documentation and version bump

**Files:**
- Modify: `README.md`
- Modify: `docs/API.md`
- Modify: `docs/Development.md`
- Modify: `CLAUDE.md`
- Modify: `pyproject.toml:3`

**Interfaces:**
- Consumes: the finished executor and tests.
- Produces: user-facing docs; version `0.9.0`.

- [ ] **Step 1: README**

Under `## Executors` (line 10-11), replace

```
* IBM Platform LSF
* We will accept PRs that implement and test additional executors (SLURM, etc.)
```

with

```
* IBM Platform LSF
* SLURM (requires slurmdbd accounting; see [SLURM notes](#slurm-notes))
* We will accept PRs that implement and test additional executors (SGE, PBS, etc.)
```

Replace the sentence after the Job Array example (line 88):

```
The array index environment variable depends on the executor: LSF uses `$LSB_JOBINDEX`, SLURM uses `$SLURM_ARRAY_TASK_ID`, and the local executor uses `$ARRAY_INDEX`.
```

In `### Job Dependencies`, after the LSF sentence ending `lists the failed upstream job ids.`, add:

```
On SLURM the same chain uses `sbatch --dependency=afterok:... --kill-on-invalid-dep=yes`.
```

In the example `cluster_api.yaml` add a profile after `janelia_lsf`:

```yaml
  slurm_cluster:
    executor: slurm
    queue: compute          # SLURM partition
    cpus: 2
    memory: "8 GB"
    walltime: "04:00:00"    # HH:MM:SS — "04:00" would mean four minutes on SLURM
```

In the config table change the `executor` row to `` Backend: `lsf`, `slurm` or `local` `` and the `walltime` row description to `` Default wall time (LSF `"04:00"` = 4 h; SLURM `"04:00:00"` = 4 h) ``.

Add a new section before `## API Reference`:

```markdown
### SLURM notes

- **Accounting required.** Status comes from `sacct`, so the cluster must run
  slurmdbd. History is kept subject to the site's `PurgeJobAfter` setting.
- **No requeue.** Every job is submitted with `--no-requeue` so preempted or
  node-failed jobs are reported as terminal. Sites that set
  `PrologFlags=ForceRequeueOnFail` or `requeue_on_resume_failure` can still
  requeue a job behind our back; the rerun is not tracked.
- **Exit codes.** `exit_code` is the script's return code, or `-signal` when
  a signal terminated the job (e.g. `-9`), as for the local executor.
- **`max_mem`** is not reported (sacct only has it on step rows).
- **`cancel_by_name`** matches an exact job name; `scancel` has no globbing.
- **`inherit_env=False`** uses `--export=NONE`, which SLURM documents as
  still loading the user's login environment on the node; `env` values are
  exported inside the script and `SLURM_EXPORT_ENV=ALL` is set so `srun`
  steps inside your command see them.
- **`login_shell=True`** runs the script with `#!/bin/bash -l`; shebangs of
  the form `#!/usr/bin/env bash` are left unchanged with a warning.
- **GPUs** are requested with `--gres=gpu:N`; use `extra_directives` for
  `--gpus-per-node`, `--mem-per-cpu`, `--account` and similar.
- **Multi-cluster** (`-M`, `SLURM_CLUSTERS`) is unsupported.
```

- [ ] **Step 2: API.md**

Line 12-13, replace the `inherit_env` and `login_shell` bullets with:

```
  - `inherit_env`: when `True` (default), the job inherits the submitting process's environment; when `False`, only `env` plus scheduler-provided variables reach the job. SLURM exception: `--export=NONE` also loads the user's login environment on the node.
  - `login_shell`: when `True`, the job runs under a login shell so the target user's own profile builds the environment (PATH, modules, conda). LSF uses `bsub -L`; SLURM rewrites the script shebang to `bash -l`.
```

Line 17: `` - `cancel_by_name(name_pattern)` — cancel jobs by name: glob pattern on LSF, exact name on SLURM ``

Line 27 (JobRecord fields), append the sentence: `` `exit_code` is the script's return code, or `-signal` when a signal terminated the job (SLURM and local executors). ``

- [ ] **Step 3: Development.md**

In the test layout table add rows after `test_reconnect.py`:

```
| `test_slurm.py` | `SlurmExecutor` header building, sbatch submission, sacct parsing, cancel | No — mocks `_call()` |
| `test_slurm_reconnect.py` | `SlurmExecutor.reconnect()` — squeue discovery + sacct fetch | No — mocks `_call()` |
| `test_slurm_integration.py` | Full SLURM round-trips (submit, monitor, signals, arrays, dependencies, env, reconnect) | **Yes** — requires a live SLURM cluster with accounting; runs in CI |
```

Add `slurm_config` to the shared-fixtures sentence. In `### Integration tests`, after the LSF prerequisites, add:

```markdown
SLURM integration tests live in `test_slurm_integration.py` and skip unless
`sbatch` is on `PATH`. In CI the `slurm-integration` job runs them on a real
single-node SLURM (via `koesterlab/setup-slurm-action`) with
`CLUSTER_API_REQUIRE_SLURM=1`, which turns that skip into a failure so the
job cannot pass by skipping.
```

- [ ] **Step 4: CLAUDE.md**

In Project Structure add after the `executors/lsf.py` line:

```
  - `executors/slurm.py` — SLURM executor (sbatch/sacct/scancel; squeue for reconnect discovery)
```

In Log File Naming add a bullet:

```
- **SLURM**: `stdout.%j.log` / `stderr.%j.log`; array jobs use `stdout.%A_%a.log` (`$SLURM_ARRAY_TASK_ID` per element)
```

In Testing, change the parenthetical to `(except `test_local.py` which runs real subprocesses, and `test_integration.py` / `test_slurm_integration.py` which require a live cluster and are skipped by default)`.

- [ ] **Step 5: Version bump**

In `pyproject.toml` line 3: `version = "0.9.0"`.

Run: `grep -rn "0\.8\.1" --include=*.py --include=*.toml --include=*.md . | grep -v .pixi`
Expected: no remaining references (if `cluster_api/__init__.py` or docs carry a version string, update them too).

- [ ] **Step 6: Verify and commit**

Run: `pixi run check`
Expected: lint clean, all tests pass.

```bash
git add README.md docs/API.md docs/Development.md CLAUDE.md pyproject.toml
git commit -m "Document SLURM executor; bump version to 0.9.0

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
