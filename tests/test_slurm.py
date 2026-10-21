"""Tests for the SLURM executor."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.config import ClusterConfig
from cluster_api.exceptions import CommandFailedError, SubmitError
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
        # sacct prints "0:0" for jobs that have not finished; that is not an exit code
        assert meta["exit_code"] is None
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
        assert result["9"][1]["exit_code"] is None

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

    @pytest.mark.parametrize("order", ["task_first", "range_first"])
    def test_explicit_task_row_beats_compressed_range(self, slurm_config, order):
        """Real sacct output: once task 2 starts it gets its own row, but the
        array's pending meta-row can still list it as 17_[2-3]. The explicit
        row must win regardless of which comes first."""
        executor = SlurmExecutor(slurm_config)
        rows = [
            _row("17_2", state="RUNNING"),
            _row("17_[2-3]", state="PENDING", nodelist="None assigned", start="Unknown"),
        ]
        if order == "range_first":
            rows.reverse()
        result = executor._parse_job_statuses("\n".join(rows))
        assert result["17[2]"][0] == JobStatus.RUNNING
        assert result["17[2]"][1]["exec_host"] == "node01"
        assert result["17[3]"][0] == JobStatus.PENDING

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
