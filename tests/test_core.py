"""Tests for the abstract Executor base class."""

from __future__ import annotations

import pytest

from cluster_api._types import JobStatus
from cluster_api.executors.local import LocalExecutor


class TestRenderScript:
    def test_basic_script(self, default_config):
        executor = LocalExecutor(default_config)
        script = executor.render_script(
            command="echo hello",
            name="test-job",
        )
        assert "#!/bin/bash" in script
        assert "echo hello" in script
        assert "# LOCAL Job: test-job" in script

    def test_prologue_epilogue(self, default_config):
        default_config.script_prologue = ["module load python"]
        executor = LocalExecutor(default_config)
        script = executor.render_script(
            command="python run.py",
            name="test-job",
            prologue=["export FOO=bar"],
            epilogue=["echo done"],
        )
        assert "module load python" in script
        assert "export FOO=bar" in script
        assert "python run.py" in script
        assert "echo done" in script

    def test_directives_skip(self, default_config):
        default_config.directives_skip = ["LOCAL"]
        executor = LocalExecutor(default_config)
        script = executor.render_script(
            command="echo hello",
            name="test-job",
        )
        assert "# LOCAL" not in script

    def test_extra_directives(self, default_config):
        default_config.extra_directives = ["# EXTRA directive1"]
        executor = LocalExecutor(default_config)
        script = executor.render_script(
            command="echo hello",
            name="test-job",
        )
        assert "# EXTRA directive1" in script

    def test_custom_shebang(self, default_config):
        default_config.shebang = "#!/usr/bin/env bash"
        executor = LocalExecutor(default_config)
        script = executor.render_script(command="echo hi", name="test")
        assert "#!/usr/bin/env bash" in script


class TestJobIdFromSubmitOutput:
    def test_default_regex(self, default_config):
        executor = LocalExecutor(default_config)
        # LocalExecutor uses the default regex r"(?P<job_id>\d+)"
        assert executor._job_id_from_submit_output("12345") == "12345"
        assert executor._job_id_from_submit_output("job 42 submitted") == "42"

    def test_no_match_raises(self, default_config):
        executor = LocalExecutor(default_config)
        with pytest.raises(RuntimeError, match="Could not parse job ID"):
            executor._job_id_from_submit_output("no numbers here")


class TestCallTimeout:
    @pytest.mark.asyncio
    async def test_timeout(self):
        with pytest.raises(RuntimeError, match="timed out"):
            await LocalExecutor._call(["sleep", "10"], timeout=0.5)

    @pytest.mark.asyncio
    async def test_successful_call(self):
        result = await LocalExecutor._call(["echo", "hello"])
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_failed_command(self):
        with pytest.raises(RuntimeError, match="Command failed"):
            await LocalExecutor._call(["false"])

    @pytest.mark.asyncio
    async def test_env_passing(self):
        result = await LocalExecutor._call(
            ["bash", "-c", "echo $MY_TEST_VAR"],
            env={"MY_TEST_VAR": "test_value"},
        )
        assert result == "test_value"


class TestWriteScript:
    def test_write_script(self, default_config):
        executor = LocalExecutor(default_config)
        path = executor._write_script("#!/bin/bash\necho hello", "my-job")
        assert path.endswith(".sh")
        with open(path) as f:
            assert "echo hello" in f.read()


class TestPrefix:
    def test_explicit_prefix(self, default_config):
        executor = LocalExecutor(default_config)
        assert executor._prefix == "test"

    def test_random_prefix_when_none(self, tmp_path):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(log_directory=str(tmp_path / "logs"))
        executor = LocalExecutor(config)
        assert len(executor._prefix) == 5
        assert executor._prefix.isalnum()

    def test_random_prefix_is_unique(self, tmp_path):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(log_directory=str(tmp_path / "logs"))
        a = LocalExecutor(config)
        b = LocalExecutor(config)
        assert a._prefix != b._prefix


class TestCancelAll:
    @pytest.mark.asyncio
    async def test_cancel_all(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="sleep 60", name="sleeper")
        assert not job.is_terminal

        await executor.cancel_all()
        assert job.status == JobStatus.KILLED
