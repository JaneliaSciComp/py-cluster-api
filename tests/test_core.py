"""Tests for the abstract Executor base class."""

from __future__ import annotations

import pytest

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.core import _sanitize_job_name
from cluster_api.exceptions import CommandFailedError, CommandTimeoutError, SubmitError
from cluster_api.executors.local import LocalExecutor
from cluster_api.script import render_script, write_script


class TestRenderScript:
    def test_basic_script(self, default_config):
        script = render_script(
            default_config,
            command="echo hello",
            header_lines=["# LOCAL Job: test-job"],
        )
        assert "#!/bin/bash" in script
        assert "echo hello" in script
        assert "# LOCAL Job: test-job" in script

    def test_prologue_epilogue(self, default_config):
        default_config.script_prologue = ["module load python"]
        script = render_script(
            default_config,
            command="python run.py",
            header_lines=["# LOCAL Job: test-job"],
            prologue=["export FOO=bar"],
            epilogue=["echo done"],
        )
        assert "module load python" in script
        assert "export FOO=bar" in script
        assert "python run.py" in script
        assert "echo done" in script

    def test_directives_skip(self, default_config):
        default_config.directives_skip = ["LOCAL"]
        script = render_script(
            default_config,
            command="echo hello",
            header_lines=["# LOCAL Job: test-job"],
        )
        assert "# LOCAL" not in script

    def test_extra_directives(self, default_config):
        default_config.extra_directives = ["# EXTRA directive1"]
        script = render_script(
            default_config,
            command="echo hello",
            header_lines=["# LOCAL Job: test-job"],
        )
        assert "# EXTRA directive1" in script

    def test_custom_shebang(self, default_config):
        default_config.shebang = "#!/usr/bin/env bash"
        script = render_script(
            default_config,
            command="echo hi",
            header_lines=["# LOCAL Job: test"],
        )
        assert "#!/usr/bin/env bash" in script


class TestJobIdFromSubmitOutput:
    def test_default_regex(self, default_config):
        executor = LocalExecutor(default_config)
        # LocalExecutor uses the default regex r"(?P<job_id>\d+)"
        assert executor._job_id_from_submit_output("12345") == "12345"
        assert executor._job_id_from_submit_output("job 42 submitted") == "42"

    def test_no_match_raises(self, default_config):
        executor = LocalExecutor(default_config)
        with pytest.raises(SubmitError, match="Could not parse job ID"):
            executor._job_id_from_submit_output("no numbers here")


class TestCallTimeout:

    async def test_timeout(self):
        with pytest.raises(CommandTimeoutError, match="timed out"):
            await LocalExecutor._call(["sleep", "10"], timeout=0.5)


    async def test_successful_call(self):
        result = await LocalExecutor._call(["echo", "hello"])
        assert result == "hello"


    async def test_failed_command(self):
        with pytest.raises(CommandFailedError, match="Command failed"):
            await LocalExecutor._call(["false"])


    async def test_env_passing(self):
        result = await LocalExecutor._call(
            ["bash", "-c", "echo $MY_TEST_VAR"],
            env={"MY_TEST_VAR": "test_value"},
        )
        assert result == "test_value"


class TestWriteScript:
    def test_write_script(self, tmp_path):
        path = write_script(str(tmp_path), "#!/bin/bash\necho hello", "my-job", 1)
        assert path.endswith(".sh")
        with open(path) as f:
            assert "echo hello" in f.read()


class TestPrefix:
    def test_explicit_prefix(self, default_config):
        executor = LocalExecutor(default_config)
        assert executor._prefix == "test"

    def test_random_prefix_when_none(self):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig()
        executor = LocalExecutor(config)
        assert len(executor._prefix) == 5
        assert executor._prefix.isalnum()

    def test_random_prefix_is_unique(self):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig()
        a = LocalExecutor(config)
        b = LocalExecutor(config)
        assert a._prefix != b._prefix


class TestSanitizeJobName:
    def test_spaces_replaced(self):
        assert _sanitize_job_name("my-Demo App-run") == "my-Demo-App-run"

    def test_multiple_spaces(self):
        assert _sanitize_job_name("a b c") == "a-b-c"

    def test_special_characters(self):
        assert _sanitize_job_name("job@host#1") == "job-host-1"

    def test_safe_characters_preserved(self):
        assert _sanitize_job_name("prefix-my_job.v2") == "prefix-my_job.v2"

    def test_already_clean(self):
        assert _sanitize_job_name("test-myjob") == "test-myjob"


class TestSanitizeJobNameIntegration:
    async def test_submit_sanitizes_name(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo hello",
            name="Demo App-run",
            resources=ResourceSpec(work_dir=work_dir),
        )
        assert " " not in job.name
        assert job.name == "test-Demo-App-run"
        await executor.cancel(job.job_id)

    async def test_submit_array_sanitizes_name(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit_array(
            command="echo hello",
            name="Demo App-run",
            array_range=(1, 2),
            resources=ResourceSpec(work_dir=work_dir),
        )
        assert " " not in job.name
        assert job.name == "test-Demo-App-run"
        await executor.cancel(job.job_id)


class TestCancelAll:

    async def test_cancel_all(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="sleep 60", name="sleeper",
            resources=ResourceSpec(work_dir=work_dir),
        )
        assert not job.is_terminal

        await executor.cancel_all()
        assert job.status == JobStatus.KILLED
