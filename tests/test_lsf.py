"""Tests for the LSF executor."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.executors.lsf import (
    LSFExecutor,
    _LSF_STATUS_MAP,
    _parse_lsf_time,
    lsf_format_bytes_ceil,
)


class TestLsfFormatBytesCeil:
    def test_megabytes(self):
        assert lsf_format_bytes_ceil(512 * 1024**2, "MB") == "512"

    def test_rounds_up(self):
        assert lsf_format_bytes_ceil(512 * 1024**2 + 1, "MB") == "513"

    def test_gigabytes(self):
        assert lsf_format_bytes_ceil(8 * 1024**3, "GB") == "8"

    def test_kilobytes(self):
        assert lsf_format_bytes_ceil(1024 * 1024, "KB") == "1024"

    def test_terabytes(self):
        assert lsf_format_bytes_ceil(2 * 1024**4, "TB") == "2"

    def test_invalid_units(self):
        with pytest.raises(ValueError):
            lsf_format_bytes_ceil(1024, "PB")


class TestBuildHeader:
    def test_basic_header(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        lines = executor.build_header("test-job")
        assert "#BSUB -J test-job" in lines
        assert any("-q normal" in line for line in lines)
        assert any("-M" in line for line in lines)
        assert any("-W 04:00" in line for line in lines)
        assert any("-o" in line for line in lines)
        assert any("-e" in line for line in lines)

    def test_with_resources(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        res = ResourceSpec(
            cpus=4, memory="16 GB", walltime="08:00",
            queue="long", account="proj1", work_dir="/scratch",
        )
        lines = executor.build_header("test-job", res)
        assert any("-q long" in line for line in lines)
        assert any("-P proj1" in line for line in lines)
        assert any("-n 4" in line for line in lines)
        assert any("span[hosts=1]" in line for line in lines)
        assert any("-W 08:00" in line for line in lines)
        assert any("-cwd /scratch" in line for line in lines)

    def test_single_cpu_no_span(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        res = ResourceSpec(cpus=1)
        lines = executor.build_header("test-job", res)
        assert any("-n 1" in line for line in lines)
        assert not any("span[hosts=1]" in line for line in lines)

    def test_memory_formatting(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        res = ResourceSpec(memory="8 GB")
        lines = executor.build_header("test-job", res)
        # 8 GB = 8192 MB
        assert any("-M 8192" in line for line in lines)
        assert any("rusage[mem=8192]" in line for line in lines)

    def test_no_memory(self, tmp_path):
        """No -M or -R rusage when neither config nor ResourceSpec sets memory."""
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(
            executor="lsf",
            log_directory=str(tmp_path / "logs"),
            job_name_prefix="test",
            lsf_units="MB",
        )
        executor = LSFExecutor(config)
        lines = executor.build_header("test-job")
        assert not any("-M" in line for line in lines)
        assert not any("rusage" in line for line in lines)

    def test_with_gpus(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        res = ResourceSpec(gpus=2)
        lines = executor.build_header("test-job", res)
        assert any('-gpu "num=2"' in line for line in lines)

    def test_no_gpus(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        lines = executor.build_header("test-job")
        assert not any("-gpu" in line for line in lines)

    def test_gpu_from_config(self, tmp_path):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(
            executor="lsf",
            log_directory=str(tmp_path / "logs"),
            job_name_prefix="test",
            lsf_units="MB",
            gpus=1,
        )
        executor = LSFExecutor(config)
        lines = executor.build_header("test-job")
        assert any('-gpu "num=1"' in line for line in lines)

    def test_cluster_options(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        res = ResourceSpec(
            cluster_options=['-R "select[gpus>0]"', "-G mygroup"],
        )
        lines = executor.build_header("test-job", res)
        assert any('select[gpus>0]' in line for line in lines)
        assert any("-G mygroup" in line for line in lines)


class TestParseJobStatuses:
    def _make_bjobs_json(self, records):
        return json.dumps({"RECORDS": records})

    def test_running_job(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {
                "JOBID": "12345",
                "STAT": "RUN",
                "EXIT_CODE": "-",
                "EXEC_HOST": "node01",
                "MAX_MEM": "512 MB",
                "SUBMIT_TIME": "Jan  5 10:00:00 2024",
                "START_TIME": "Jan  5 10:01:00 2024",
                "FINISH_TIME": "-",
            }
        ])
        result = executor._parse_job_statuses(output)
        assert "12345" in result
        status, meta = result["12345"]
        assert status == JobStatus.RUNNING
        assert meta["exec_host"] == "node01"
        assert meta["max_mem"] == "512 MB"
        assert meta["exit_code"] is None

    def test_done_job(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {
                "JOBID": "12346",
                "STAT": "DONE",
                "EXIT_CODE": "0",
                "EXEC_HOST": "node02",
                "MAX_MEM": "1 GB",
                "SUBMIT_TIME": "Jan  5 10:00:00 2024",
                "START_TIME": "Jan  5 10:01:00 2024",
                "FINISH_TIME": "Jan  5 11:00:00 2024",
            }
        ])
        result = executor._parse_job_statuses(output)
        status, meta = result["12346"]
        assert status == JobStatus.DONE
        assert meta["exit_code"] == 0
        assert meta["finish_time"] is not None

    def test_done_job_empty_exit_code(self, lsf_config):
        """LSF returns empty EXIT_CODE for DONE jobs — should infer 0."""
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {
                "JOBID": "12348",
                "STAT": "DONE",
                "EXIT_CODE": "",
                "EXEC_HOST": "node02",
                "MAX_MEM": "1 GB",
                "SUBMIT_TIME": "Jan  5 10:00:00 2024",
                "START_TIME": "Jan  5 10:01:00 2024",
                "FINISH_TIME": "Jan  5 11:00:00 2024",
            }
        ])
        result = executor._parse_job_statuses(output)
        status, meta = result["12348"]
        assert status == JobStatus.DONE
        assert meta["exit_code"] == 0

    def test_failed_job(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {
                "JOBID": "12347",
                "STAT": "EXIT",
                "EXIT_CODE": "1",
                "EXEC_HOST": "node03",
                "MAX_MEM": "256 MB",
                "SUBMIT_TIME": "-",
                "START_TIME": "-",
                "FINISH_TIME": "-",
            }
        ])
        result = executor._parse_job_statuses(output)
        status, meta = result["12347"]
        assert status == JobStatus.FAILED
        assert meta["exit_code"] == 1

    def test_zombie_job(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {"JOBID": "99999", "STAT": "ZOMBI", "EXIT_CODE": "-",
             "EXEC_HOST": "-", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"}
        ])
        result = executor._parse_job_statuses(output)
        status, _ = result["99999"]
        assert status == JobStatus.FAILED

    def test_empty_output(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        assert executor._parse_job_statuses("") == {}

    def test_invalid_json(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        assert executor._parse_job_statuses("not json") == {}

    def test_multiple_jobs(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {"JOBID": "100", "STAT": "PEND", "EXIT_CODE": "-",
             "EXEC_HOST": "-", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "101", "STAT": "RUN", "EXIT_CODE": "-",
             "EXEC_HOST": "node01", "MAX_MEM": "128 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ])
        result = executor._parse_job_statuses(output)
        assert len(result) == 2
        assert result["100"][0] == JobStatus.PENDING
        assert result["101"][0] == JobStatus.RUNNING

    def test_suspended_states(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        for lsf_stat in ("USUSP", "PSUSP", "SSUSP"):
            output = self._make_bjobs_json([
                {"JOBID": "200", "STAT": lsf_stat, "EXIT_CODE": "-",
                 "EXEC_HOST": "-", "MAX_MEM": "-",
                 "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            ])
            result = executor._parse_job_statuses(output)
            assert result["200"][0] == JobStatus.PENDING


class TestBuildStatusArgs:
    def test_status_args(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        args = executor._build_status_args()
        assert "bjobs" in args
        assert "-J" in args
        assert "-a" in args
        assert "-json" in args
        assert "test-*" in args


class TestSubmission:

    async def test_submit_stdin(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            job = await executor.submit(
                command="echo hello",
                name="my-job",
            )
            assert job.job_id == "12345"
            assert job.name == "test-my-job"
            assert job.status == JobStatus.PENDING
            # Verify stdin submission was used
            call_args = mock_call.call_args
            assert call_args.kwargs.get("stdin_data") is not None


    async def test_submit_email_suppression(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            await executor.submit(command="echo hello", name="my-job")
            call_args = mock_call.call_args
            env = call_args.kwargs.get("env")
            assert env is not None
            assert env.get("LSB_JOB_REPORT_MAIL") == "N"


    async def test_submit_array(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            job = await executor.submit_array(
                command="python process.py --index $LSB_JOBINDEX",
                name="batch",
                array_range=(1, 50),
            )
            assert job.job_id == "12345"
            assert job.metadata["array_range"] == (1, 50)
            # Verify stdin submission included array name
            call_args = mock_call.call_args
            stdin = call_args.kwargs.get("stdin_data", "")
            assert "[1-50]" in stdin


class TestArrayScriptRewriting:

    async def test_percent_i_substitution(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            await executor.submit_array(
                command="echo hello",
                name="arr",
                array_range=(1, 10),
            )
            stdin = mock_call.call_args.kwargs.get("stdin_data", "")
            assert "%I.out" in stdin
            assert "%I.err" in stdin


class TestCancelByName:

    async def test_cancel_by_name(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="",
        ) as mock_call:
            await executor.cancel_by_name("test-*")
            mock_call.assert_called_once()
            args = mock_call.call_args[0][0]
            assert "bkill" in args
            assert "-J" in args
            assert "test-*" in args


class TestParseLsfTime:
    def test_standard_format(self):
        dt = _parse_lsf_time("Jan  5 10:00:00 2024")
        assert dt is not None
        assert dt.month == 1
        assert dt.day == 5
        assert dt.year == 2024

    def test_trailing_timezone(self):
        """LSF appends a timezone letter like ' L' to timestamps."""
        dt = _parse_lsf_time("Feb  8 10:31:17 2026 L")
        assert dt is not None
        assert dt.month == 2
        assert dt.day == 8
        assert dt.year == 2026

    def test_dash(self):
        assert _parse_lsf_time("-") is None

    def test_none(self):
        assert _parse_lsf_time(None) is None


class TestLsfStatusMap:
    def test_all_statuses_mapped(self):
        expected = {"PEND", "RUN", "DONE", "EXIT", "ZOMBI", "USUSP", "PSUSP", "SSUSP"}
        assert expected == set(_LSF_STATUS_MAP.keys())


class TestArrayConcurrency:

    async def test_with_max_concurrent(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            job = await executor.submit_array(
                command="echo hello",
                name="batch",
                array_range=(1, 100),
                max_concurrent=15,
            )
            assert job.job_id == "12345"
            assert job.metadata["max_concurrent"] == 15
            stdin = mock_call.call_args.kwargs.get("stdin_data", "")
            assert "[1-100%15]" in stdin


    async def test_without_max_concurrent(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            job = await executor.submit_array(
                command="echo hello",
                name="batch",
                array_range=(1, 100),
            )
            stdin = mock_call.call_args.kwargs.get("stdin_data", "")
            assert "[1-100]" in stdin
            j_line = [line for line in stdin.splitlines() if "-J " in line][0]
            assert "%" not in j_line
            assert "max_concurrent" not in job.metadata
