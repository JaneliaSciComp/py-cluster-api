"""Tests for executor reconnection support."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.exceptions import ClusterAPIError, CommandFailedError
from cluster_api.executors.lsf import LSFExecutor


def _make_bjobs_json(records):
    return json.dumps({"RECORDS": records})


def _make_record(
    job_id="12345",
    job_name="test-myjob",
    stat="RUN",
    exit_code="-",
    exec_host="node01",
    max_mem="512 MB",
    submit_time="Jan  5 10:00:00 2024",
    start_time="Jan  5 10:01:00 2024",
    finish_time="-",
):
    return {
        "JOBID": job_id,
        "JOB_NAME": job_name,
        "STAT": stat,
        "EXIT_CODE": exit_code,
        "EXEC_HOST": exec_host,
        "MAX_MEM": max_mem,
        "SUBMIT_TIME": submit_time,
        "START_TIME": start_time,
        "FINISH_TIME": finish_time,
    }


class TestReconnectByPrefix:

    async def test_single_running_job(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(job_id="12345", job_name="test-myjob", stat="RUN"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_id == "12345"
        assert job.name == "test-myjob"
        assert job.status == JobStatus.RUNNING
        assert job.metadata["reconnected"] is True
        assert job.command == ""
        assert job.resources is None
        assert job.exec_host == "node01"

    async def test_completed_job(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(
                job_id="12346", job_name="test-done", stat="DONE",
                exit_code="0", finish_time="Jan  5 12:00:00 2024",
            ),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert len(jobs) == 1
        assert jobs[0].status == JobStatus.DONE
        assert jobs[0].exit_code == 0

    async def test_multiple_jobs(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(job_id="100", job_name="test-a", stat="RUN"),
            _make_record(job_id="101", job_name="test-b", stat="PEND", exec_host="-"),
            _make_record(
                job_id="102", job_name="test-c", stat="DONE",
                exit_code="0", finish_time="Jan  5 12:00:00 2024",
            ),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert len(jobs) == 3
        ids = {j.job_id for j in jobs}
        assert ids == {"100", "101", "102"}
        by_id = {j.job_id: j for j in jobs}
        assert by_id["100"].status == JobStatus.RUNNING
        assert by_id["101"].status == JobStatus.PENDING
        assert by_id["102"].status == JobStatus.DONE

    async def test_skips_already_tracked(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)

        # Submit a job first to populate _jobs
        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            return_value="Job <12345> is submitted to queue <normal>.",
        ):
            await executor.submit(
                command="echo hello", name="existing",
                resources=ResourceSpec(work_dir=work_dir),
            )

        # Reconnect sees the same job ID — should skip it
        output = _make_bjobs_json([
            _make_record(job_id="12345", job_name="test-existing", stat="RUN"),
            _make_record(job_id="99999", job_name="test-new", stat="RUN"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert len(jobs) == 1
        assert jobs[0].job_id == "99999"

    async def test_no_prefix_raises_error(self):
        from cluster_api.config import ClusterConfig

        config = ClusterConfig(executor="lsf", lsf_units="MB")
        executor = LSFExecutor(config)

        with pytest.raises(ClusterAPIError, match="job_name_prefix"):
            await executor.reconnect()

    async def test_empty_result(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert jobs == []

    async def test_no_matching_job_error(self, lsf_config):
        executor = LSFExecutor(lsf_config)

        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            side_effect=CommandFailedError("No matching job found"),
        ):
            jobs = await executor.reconnect()

        assert jobs == []

    async def test_no_unfinished_job_error(self, lsf_config):
        executor = LSFExecutor(lsf_config)

        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            side_effect=CommandFailedError("No unfinished job found"),
        ):
            jobs = await executor.reconnect()

        assert jobs == []

    async def test_other_command_error_propagates(self, lsf_config):
        executor = LSFExecutor(lsf_config)

        with patch.object(
            executor, "_call", new_callable=AsyncMock,
            side_effect=CommandFailedError("Permission denied"),
        ):
            with pytest.raises(CommandFailedError, match="Permission denied"):
                await executor.reconnect()

    async def test_build_reconnect_args_with_prefix(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        args = executor._build_reconnect_args()
        assert "bjobs" in args
        assert "-J" in args
        assert "test-*" in args
        assert "-a" in args
        assert "-json" in args

    async def test_jobs_added_to_tracking(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(job_id="500", job_name="test-tracked", stat="RUN"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            await executor.reconnect()

        assert "500" in executor.jobs
        assert "500" in executor.active_jobs


class TestReconnectArrayJobs:

    async def test_elements_grouped_under_parent(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(job_id="12345[1]", job_name="test-arr", stat="DONE", exit_code="0"),
            _make_record(job_id="12345[2]", job_name="test-arr", stat="RUN"),
            _make_record(job_id="12345[3]", job_name="test-arr", stat="PEND", exec_host="-"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_id == "12345"
        assert job.is_array
        assert job.metadata["array_range"] == (1, 3)
        assert len(job.array_elements) == 3
        assert job.array_elements[1].status == JobStatus.DONE
        assert job.array_elements[2].status == JobStatus.RUNNING
        assert job.array_elements[3].status == JobStatus.PENDING

    async def test_range_inferred_from_visible_indices(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        # Only elements 5 and 10 visible — range should be (5, 10)
        output = _make_bjobs_json([
            _make_record(job_id="500[5]", job_name="test-sparse", stat="DONE", exit_code="0"),
            _make_record(job_id="500[10]", job_name="test-sparse", stat="RUN"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert jobs[0].metadata["array_range"] == (5, 10)

    async def test_status_computed(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        # All elements done → parent status should be DONE
        output = _make_bjobs_json([
            _make_record(job_id="600[1]", job_name="test-alldone", stat="DONE", exit_code="0"),
            _make_record(job_id="600[2]", job_name="test-alldone", stat="DONE", exit_code="0"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert jobs[0].status == JobStatus.DONE

    async def test_status_computed_with_failure(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(job_id="700[1]", job_name="test-mixed", stat="DONE", exit_code="0"),
            _make_record(job_id="700[2]", job_name="test-mixed", stat="EXIT", exit_code="1"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert jobs[0].status == JobStatus.FAILED
        assert jobs[0].failed_element_indices == [2]

    async def test_mixed_single_and_array(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(job_id="800", job_name="test-single", stat="RUN"),
            _make_record(job_id="900[1]", job_name="test-arr", stat="DONE", exit_code="0"),
            _make_record(job_id="900[2]", job_name="test-arr", stat="RUN"),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        assert len(jobs) == 2
        by_id = {j.job_id: j for j in jobs}
        assert "800" in by_id
        assert "900" in by_id
        assert not by_id["800"].is_array
        assert by_id["900"].is_array

    async def test_array_element_metadata(self, lsf_config):
        executor = LSFExecutor(lsf_config)
        output = _make_bjobs_json([
            _make_record(
                job_id="1000[1]", job_name="test-meta", stat="DONE",
                exit_code="0", exec_host="node01", max_mem="256 MB",
            ),
        ])

        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=output):
            jobs = await executor.reconnect()

        elem = jobs[0].array_elements[1]
        assert elem.exec_host == "node01"
        assert elem.max_mem == "256 MB"
        assert elem.exit_code == 0


class TestReconnectThenPoll:

    async def test_poll_updates_reconnected_jobs(self, lsf_config):
        executor = LSFExecutor(lsf_config)

        # Reconnect finds a running job
        reconnect_output = _make_bjobs_json([
            _make_record(job_id="12345", job_name="test-myjob", stat="RUN"),
        ])
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=reconnect_output):
            jobs = await executor.reconnect()

        assert len(jobs) == 1
        assert jobs[0].status == JobStatus.RUNNING

        # Poll sees the job completed
        poll_output = _make_bjobs_json([
            _make_record(
                job_id="12345", stat="DONE", exit_code="0",
                finish_time="Jan  5 12:00:00 2024",
            ),
        ])
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=poll_output):
            statuses = await executor.poll()

        assert statuses["12345"] == JobStatus.DONE
        assert jobs[0].status == JobStatus.DONE
        assert jobs[0].exit_code == 0

    async def test_poll_updates_reconnected_array(self, lsf_config):
        executor = LSFExecutor(lsf_config)

        # Reconnect finds array with one running element
        reconnect_output = _make_bjobs_json([
            _make_record(job_id="12345[1]", job_name="test-arr", stat="DONE", exit_code="0"),
            _make_record(job_id="12345[2]", job_name="test-arr", stat="RUN"),
        ])
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=reconnect_output):
            jobs = await executor.reconnect()

        assert len(jobs) == 1
        assert jobs[0].status == JobStatus.RUNNING

        # Poll sees element 2 completed
        poll_output = _make_bjobs_json([
            _make_record(job_id="12345[1]", stat="DONE", exit_code="0"),
            _make_record(job_id="12345[2]", stat="DONE", exit_code="0",
                         finish_time="Jan  5 12:00:00 2024"),
        ])
        with patch.object(executor, "_call", new_callable=AsyncMock, return_value=poll_output):
            statuses = await executor.poll()

        assert statuses["12345"] == JobStatus.DONE
        assert jobs[0].status == JobStatus.DONE
