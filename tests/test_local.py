"""Tests for the Local executor — end-to-end with real subprocesses."""

from __future__ import annotations

import asyncio

import pytest

from cluster_api._types import JobStatus
from cluster_api.executors.local import LocalExecutor


class TestLocalSubmitAndPoll:
    @pytest.mark.asyncio
    async def test_submit_and_poll_success(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="echo hello", name="echo-test")
        assert job.status == JobStatus.PENDING

        # Wait for subprocess to finish
        proc = executor._processes[job.job_id]
        await proc.wait()

        await executor.poll()
        assert job.status == JobStatus.DONE
        assert job.exit_code == 0

    @pytest.mark.asyncio
    async def test_submit_and_poll_failure(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="exit 1", name="fail-test")

        proc = executor._processes[job.job_id]
        await proc.wait()

        await executor.poll()
        assert job.status == JobStatus.FAILED
        assert job.exit_code == 1

    @pytest.mark.asyncio
    async def test_running_job(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="sleep 10", name="sleep-test")

        # Poll while still running
        await asyncio.sleep(0.1)
        await executor.poll()
        assert job.status == JobStatus.RUNNING

        # Cleanup
        await executor.cancel(job.job_id)

    @pytest.mark.asyncio
    async def test_cancel(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="sleep 60", name="cancel-test")

        await asyncio.sleep(0.1)
        await executor.cancel(job.job_id)
        assert job.status == JobStatus.KILLED

    @pytest.mark.asyncio
    async def test_multiple_jobs(self, default_config):
        executor = LocalExecutor(default_config)
        job1 = await executor.submit(command="echo one", name="job1")
        job2 = await executor.submit(command="echo two", name="job2")

        assert job1.job_id != job2.job_id

        proc1 = executor._processes[job1.job_id]
        proc2 = executor._processes[job2.job_id]
        await proc1.wait()
        await proc2.wait()

        await executor.poll()
        assert job1.status == JobStatus.DONE
        assert job2.status == JobStatus.DONE

    @pytest.mark.asyncio
    async def test_jobs_property(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="echo hello", name="prop-test")
        assert job.job_id in executor.jobs
        assert job.job_id in executor.active_jobs

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        assert job.job_id in executor.jobs
        assert job.job_id not in executor.active_jobs


class TestLocalCallback:
    @pytest.mark.asyncio
    async def test_callback_fires_on_success(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="echo hello", name="cb-test")

        results = []
        job.on_success(lambda j: results.append(("success", j.job_id)))
        job.on_failure(lambda j: results.append(("failure", j.job_id)))

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        # Manually dispatch callbacks (normally monitor does this)
        from cluster_api.monitor import JobMonitor
        monitor = JobMonitor(executor, poll_interval=0.1)
        await monitor._dispatch_callbacks(job)

        assert len(results) == 1
        assert results[0] == ("success", job.job_id)

    @pytest.mark.asyncio
    async def test_callback_fires_on_failure(self, default_config):
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="exit 42", name="fail-cb-test")

        results = []
        job.on_success(lambda j: results.append("success"))
        job.on_failure(lambda j: results.append("failure"))

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        from cluster_api.monitor import JobMonitor
        monitor = JobMonitor(executor, poll_interval=0.1)
        await monitor._dispatch_callbacks(job)

        assert results == ["failure"]
