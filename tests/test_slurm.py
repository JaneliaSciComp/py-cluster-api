"""Tests for the SLURM executor."""

from __future__ import annotations

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
