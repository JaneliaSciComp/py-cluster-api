"""Tests for the LSF executor."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import ArrayElement, JobRecord, JobStatus, ResourceSpec
from cluster_api.exceptions import CommandFailedError
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
            queue="long", work_dir="/scratch",
            extra_directives=["-P proj1"],
        )
        lines = executor.build_header("test-job", res)
        assert any("-q long" in line for line in lines)
        assert any("-P proj1" in line for line in lines)
        assert any("-n 4" in line for line in lines)
        assert any("span[hosts=1]" in line for line in lines)
        assert any("-W 08:00" in line for line in lines)
        assert any('-cwd "/scratch"' in line for line in lines)

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
            job_name_prefix="test",
            lsf_units="MB",
            gpus=1,
        )
        executor = LSFExecutor(config)
        lines = executor.build_header("test-job")
        assert any('-gpu "num=1"' in line for line in lines)

    def test_extra_directives(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        res = ResourceSpec(
            extra_directives=['-R "select[gpus>0]"', "-G mygroup"],
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
        # USUSP/SSUSP map to RUNNING (job had already started)
        for lsf_stat in ("USUSP", "SSUSP"):
            output = self._make_bjobs_json([
                {"JOBID": "200", "STAT": lsf_stat, "EXIT_CODE": "-",
                 "EXEC_HOST": "-", "MAX_MEM": "-",
                 "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            ])
            result = executor._parse_job_statuses(output)
            assert result["200"][0] == JobStatus.RUNNING
        # PSUSP maps to PENDING (job was suspended before running)
        output = self._make_bjobs_json([
            {"JOBID": "200", "STAT": "PSUSP", "EXIT_CODE": "-",
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

    def test_status_args_no_prefix(self):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(executor="lsf", lsf_units="MB")
        executor = LSFExecutor(config)
        args = executor._build_status_args()
        assert "bjobs" in args
        assert "-a" in args
        assert "-json" in args
        assert "-J" not in args


class TestSubmission:

    async def test_submit(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            job = await executor.submit(
                command="echo hello",
                name="my-job",
                resources=ResourceSpec(work_dir=work_dir),
            )
            assert job.job_id == "12345"
            assert job.name == "test-my-job"
            assert job.status == JobStatus.PENDING
            # Verify bsub invocation with stdin_file
            cmd = mock_call.call_args[0][0]
            assert cmd[0] == "bsub"
            kwargs = mock_call.call_args[1]
            assert kwargs["stdin_file"].endswith(".sh")


    async def test_submit_email_suppression(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ) as mock_call:
            await executor.submit(
                command="echo hello", name="my-job",
                resources=ResourceSpec(work_dir=work_dir),
            )
            call_args = mock_call.call_args
            env = call_args.kwargs.get("env")
            assert env is not None
            assert env.get("LSB_JOB_REPORT_MAIL") == "N"


    async def test_submit_array(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="python process.py --index $LSB_JOBINDEX",
                name="batch",
                array_range=(1, 50),
                resources=ResourceSpec(work_dir=work_dir),
            )
            assert job.job_id == "12345"
            assert job.metadata["array_range"] == (1, 50)
            with open(job.script_path) as f:
                script = f.read()
            assert "[1-50]" in script


class TestArrayScriptRewriting:

    async def test_percent_i_substitution(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="echo hello",
                name="arr",
                array_range=(1, 10),
                resources=ResourceSpec(work_dir=work_dir),
            )
            with open(job.script_path) as f:
                script = f.read()
            assert "stdout.%J.%I.log" in script
            assert "stderr.%J.%I.log" in script


class TestCancel:

    async def test_cancel_passes_d_flag_when_done(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <123> is being submitted",
        ):
            job = await executor.submit(
                command="echo hi", name="cancel-done",
                resources=ResourceSpec(work_dir="/tmp"),
            )

        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="",
        ) as mock_call:
            await executor.cancel(job.job_id, done=True)
            args = mock_call.call_args[0][0]
            assert args == ["bkill", "-d", job.job_id]
            assert job.status == JobStatus.DONE

    async def test_cancel_without_done_flag(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <456> is being submitted",
        ):
            job = await executor.submit(
                command="echo hi", name="cancel-kill",
                resources=ResourceSpec(work_dir="/tmp"),
            )

        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="",
        ) as mock_call:
            await executor.cancel(job.job_id)
            args = mock_call.call_args[0][0]
            assert args == ["bkill", job.job_id]
            assert job.status == JobStatus.KILLED


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

    async def test_cancel_by_name_no_match(self, lsf_config):
        """bkill -J returns non-zero when no jobs match; should not raise."""
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            side_effect=CommandFailedError("No matching job found"),
        ):
            await executor.cancel_by_name("nonexistent-*")


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
        expected = {"PEND", "RUN", "DONE", "EXIT", "ZOMBI", "UNKWN", "WAIT", "PROV", "USUSP", "PSUSP", "SSUSP"}
        assert expected == set(_LSF_STATUS_MAP.keys())


class TestArrayConcurrency:

    async def test_with_max_concurrent(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="echo hello",
                name="batch",
                array_range=(1, 100),
                max_concurrent=15,
                resources=ResourceSpec(work_dir=work_dir),
            )
            assert job.job_id == "12345"
            assert job.metadata["max_concurrent"] == 15
            with open(job.script_path) as f:
                script = f.read()
            assert "[1-100%15]" in script


    async def test_without_max_concurrent(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="echo hello",
                name="batch",
                array_range=(1, 100),
                resources=ResourceSpec(work_dir=work_dir),
            )
            with open(job.script_path) as f:
                script = f.read()
            assert "[1-100]" in script
            j_line = [line for line in script.splitlines() if "-J " in line][0]
            assert "%" not in j_line
            assert "max_concurrent" not in job.metadata


class TestParseArrayElements:
    def _make_bjobs_json(self, records):
        return json.dumps({"RECORDS": records})

    def test_element_ids_as_keys(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {"JOBID": "12345[1]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node01", "MAX_MEM": "128 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[2]", "STAT": "RUN", "EXIT_CODE": "-",
             "EXEC_HOST": "node02", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ])
        result = executor._parse_job_statuses(output)
        assert "12345[1]" in result
        assert "12345[2]" in result
        assert result["12345[1]"][0] == JobStatus.DONE
        assert result["12345[2]"][0] == JobStatus.RUNNING

    def test_mixed_parent_and_element(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = self._make_bjobs_json([
            {"JOBID": "12345", "STAT": "RUN", "EXIT_CODE": "-",
             "EXEC_HOST": "-", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[1]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node01", "MAX_MEM": "256 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ])
        result = executor._parse_job_statuses(output)
        assert "12345" in result
        assert "12345[1]" in result


class TestArrayStatusComputation:
    def test_no_elements(self):
        record = JobRecord(
            job_id="1", name="t", command="echo",
            status=JobStatus.PENDING,
            metadata={"array_range": (1, 3)},
        )
        assert record.compute_array_status() == JobStatus.PENDING

    def test_all_done(self):
        record = JobRecord(
            job_id="1", name="t", command="echo",
            metadata={"array_range": (1, 3)},
            array_elements={
                1: ArrayElement(index=1, status=JobStatus.DONE),
                2: ArrayElement(index=2, status=JobStatus.DONE),
                3: ArrayElement(index=3, status=JobStatus.DONE),
            },
        )
        assert record.compute_array_status() == JobStatus.DONE

    def test_any_running(self):
        record = JobRecord(
            job_id="1", name="t", command="echo",
            metadata={"array_range": (1, 3)},
            array_elements={
                1: ArrayElement(index=1, status=JobStatus.DONE),
                2: ArrayElement(index=2, status=JobStatus.RUNNING),
                3: ArrayElement(index=3, status=JobStatus.PENDING),
            },
        )
        assert record.compute_array_status() == JobStatus.RUNNING

    def test_all_terminal_with_failure(self):
        record = JobRecord(
            job_id="1", name="t", command="echo",
            metadata={"array_range": (1, 3)},
            array_elements={
                1: ArrayElement(index=1, status=JobStatus.DONE),
                2: ArrayElement(index=2, status=JobStatus.FAILED),
                3: ArrayElement(index=3, status=JobStatus.DONE),
            },
        )
        assert record.compute_array_status() == JobStatus.FAILED

    def test_partial_visibility_stays_running(self):
        """If not all elements have been seen yet, status stays RUNNING."""
        record = JobRecord(
            job_id="1", name="t", command="echo",
            metadata={"array_range": (1, 5)},
            array_elements={
                1: ArrayElement(index=1, status=JobStatus.DONE),
                2: ArrayElement(index=2, status=JobStatus.DONE),
            },
        )
        assert record.compute_array_status() == JobStatus.RUNNING

    def test_convenience_properties(self):
        record = JobRecord(
            job_id="1", name="t", command="echo",
            metadata={"array_range": (1, 5)},
            array_elements={
                1: ArrayElement(index=1, status=JobStatus.DONE),
                2: ArrayElement(index=2, status=JobStatus.FAILED),
                3: ArrayElement(index=3, status=JobStatus.DONE),
                4: ArrayElement(index=4, status=JobStatus.KILLED),
                5: ArrayElement(index=5, status=JobStatus.DONE),
            },
        )
        assert record.is_array is True
        assert record.element_count == 5
        assert record.completed_elements == 5
        assert sorted(record.failed_element_indices) == [2, 4]

    def test_non_array_job(self):
        record = JobRecord(job_id="1", name="t", command="echo")
        assert record.is_array is False
        assert record.element_count == 0
        assert record.completed_elements == 0
        assert record.failed_element_indices == []


class TestArrayElementPolling:

    async def test_poll_populates_elements(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="echo hello",
                name="arr",
                array_range=(1, 3),
                resources=ResourceSpec(work_dir=work_dir),
            )

        assert job.is_array
        assert job.element_count == 3
        assert len(job.array_elements) == 0

        bjobs_output = json.dumps({"RECORDS": [
            {"JOBID": "12345[1]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node01", "MAX_MEM": "128 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[2]", "STAT": "RUN", "EXIT_CODE": "-",
             "EXEC_HOST": "node02", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[3]", "STAT": "PEND", "EXIT_CODE": "-",
             "EXEC_HOST": "-", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ]})

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=bjobs_output):
            statuses = await executor.poll()

        assert len(job.array_elements) == 3
        assert job.array_elements[1].status == JobStatus.DONE
        assert job.array_elements[1].exec_host == "node01"
        assert job.array_elements[2].status == JobStatus.RUNNING
        assert job.array_elements[3].status == JobStatus.PENDING
        assert job.status == JobStatus.RUNNING
        assert statuses["12345"] == JobStatus.RUNNING

    async def test_poll_all_done(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="echo hello",
                name="arr",
                array_range=(1, 2),
                resources=ResourceSpec(work_dir=work_dir),
            )

        bjobs_output = json.dumps({"RECORDS": [
            {"JOBID": "12345[1]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node01", "MAX_MEM": "128 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[2]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node02", "MAX_MEM": "256 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ]})

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=bjobs_output):
            await executor.poll()

        assert job.status == JobStatus.DONE
        assert job.is_terminal

    async def test_poll_partial_failure(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            job = await executor.submit_array(
                command="echo hello",
                name="arr",
                array_range=(1, 3),
                resources=ResourceSpec(work_dir=work_dir),
            )

        bjobs_output = json.dumps({"RECORDS": [
            {"JOBID": "12345[1]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node01", "MAX_MEM": "128 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[2]", "STAT": "EXIT", "EXIT_CODE": "1",
             "EXEC_HOST": "node02", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
            {"JOBID": "12345[3]", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node03", "MAX_MEM": "-",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ]})

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=bjobs_output):
            await executor.poll()

        assert job.status == JobStatus.FAILED
        assert job.failed_element_indices == [2]
        assert job.array_elements[2].exit_code == 1


class TestPollPartialBjobsFailure:
    """bjobs exits non-zero when some job IDs are gone, but still returns
    valid JSON for the remaining jobs.  poll() should parse those results
    instead of skipping the entire cycle."""

    async def test_poll_uses_stdout_from_failed_bjobs(self, work_dir):
        """Two tracked jobs; bjobs fails because one ID is gone, but returns
        valid JSON for the other.  The surviving job should still be updated."""
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(
            executor="lsf", lsf_units="MB",
            command_timeout=10.0, poll_interval=0.5,
        )
        executor = LSFExecutor(config)

        # Submit two jobs
        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            return_value="Job <100> is submitted to queue <normal>.",
        ):
            job1 = await executor.submit(
                command="echo a", name="a",
                resources=ResourceSpec(work_dir=work_dir),
            )
        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            return_value="Job <200> is submitted to queue <normal>.",
        ):
            job2 = await executor.submit(
                command="echo b", name="b",
                resources=ResourceSpec(work_dir=work_dir),
            )

        # bjobs returns DONE for job 100 but exits non-zero because job 200
        # has been cleaned from LSF's history.
        bjobs_stdout = json.dumps({"RECORDS": [
            {"JOBID": "100", "STAT": "DONE", "EXIT_CODE": "0",
             "EXEC_HOST": "node01", "MAX_MEM": "128 MB",
             "SUBMIT_TIME": "-", "START_TIME": "-", "FINISH_TIME": "-"},
        ]})

        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            side_effect=CommandFailedError(
                "Command failed (exit 255): ['bjobs', ...]\nstderr: Job <200> is not found",
                stdout=bjobs_stdout,
            ),
        ):
            statuses = await executor.poll()

        assert job1.status == JobStatus.DONE
        assert statuses["100"] == JobStatus.DONE
        # job2 stays PENDING (not seen, but not incorrectly skipped either)
        assert job2.status == JobStatus.PENDING

    async def test_poll_skips_when_no_stdout(self, work_dir):
        """If bjobs fails and produces no stdout, poll skips as before."""
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(
            executor="lsf", lsf_units="MB",
            command_timeout=10.0, poll_interval=0.5,
        )
        executor = LSFExecutor(config)

        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            return_value="Job <100> is submitted to queue <normal>.",
        ):
            job = await executor.submit(
                command="echo a", name="a",
                resources=ResourceSpec(work_dir=work_dir),
            )

        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            side_effect=CommandFailedError("Command failed", stdout=""),
        ):
            statuses = await executor.poll()

        # Should skip gracefully, job unchanged
        assert job.status == JobStatus.PENDING
        assert statuses["100"] == JobStatus.PENDING
