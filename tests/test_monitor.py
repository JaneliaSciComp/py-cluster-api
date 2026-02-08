"""Tests for the JobMonitor."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from cluster_api._types import JobExitCondition, JobRecord, JobStatus
from cluster_api.executors.local import LocalExecutor
from cluster_api.monitor import JobMonitor


class TestMonitorPollLoop:

    async def test_monitor_fires_callback(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.2)

        job = await executor.submit(command="echo hello", name="mon-test")

        results = []
        job.on_success(lambda j: results.append(j.job_id))

        await monitor.start()
        try:
            await monitor.wait_for(job, timeout=5.0)
        finally:
            await monitor.stop()

        assert job.status == JobStatus.DONE
        assert len(results) == 1
        assert results[0] == job.job_id


    async def test_wait_for_timeout(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.2)

        job = await executor.submit(command="sleep 60", name="timeout-test")

        await monitor.start()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await monitor.wait_for(job, timeout=0.5)
        finally:
            await monitor.stop()
            await executor.cancel(job.job_id)


    async def test_async_callback(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.2)

        job = await executor.submit(command="echo hello", name="async-cb")

        results = []

        async def async_handler(j):
            await asyncio.sleep(0.01)
            results.append(j.job_id)

        job.on_success(async_handler)

        await monitor.start()
        try:
            await monitor.wait_for(job, timeout=5.0)
        finally:
            await monitor.stop()

        assert len(results) == 1


    async def test_multiple_jobs(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.2)

        job1 = await executor.submit(command="echo one", name="multi1")
        job2 = await executor.submit(command="echo two", name="multi2")

        results = []
        job1.on_success(lambda j: results.append("job1"))
        job2.on_success(lambda j: results.append("job2"))

        await monitor.start()
        try:
            await monitor.wait_for(job1, job2, timeout=5.0)
        finally:
            await monitor.stop()

        assert set(results) == {"job1", "job2"}


class TestZombieDetection:

    async def test_zombie_detected(self, default_config):
        # Set very short zombie timeout
        default_config.zombie_timeout_minutes = 0.001  # ~0.06 seconds
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        # Manually create a job record that looks like it hasn't been seen
        record = JobRecord(
            job_id="ghost-1",
            name="test-ghost",
            command="echo ghost",
            status=JobStatus.RUNNING,
            _last_seen=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        executor._jobs["ghost-1"] = record

        await monitor._check_zombies()

        assert record.status == JobStatus.FAILED
        assert record.metadata.get("zombie") is True


class TestPurgeCompleted:

    async def test_purge_old_completed(self, default_config):
        default_config.completed_retention_minutes = 0.001
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        record = JobRecord(
            job_id="old-1",
            name="test-old",
            command="echo old",
            status=JobStatus.DONE,
            finish_time=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        executor._jobs["old-1"] = record

        await monitor._purge_completed()

        assert "old-1" not in executor._jobs


    async def test_no_purge_with_callbacks(self, default_config):
        default_config.completed_retention_minutes = 0.001
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        record = JobRecord(
            job_id="cb-1",
            name="test-cb",
            command="echo cb",
            status=JobStatus.DONE,
            finish_time=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        record.on_success(lambda j: None)
        executor._jobs["cb-1"] = record

        await monitor._purge_completed()

        # Should NOT be purged because it still has callbacks
        assert "cb-1" in executor._jobs


    async def test_no_purge_recent(self, default_config):
        default_config.completed_retention_minutes = 60.0
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        record = JobRecord(
            job_id="recent-1",
            name="test-recent",
            command="echo recent",
            status=JobStatus.DONE,
            finish_time=datetime.now(timezone.utc),
        )
        executor._jobs["recent-1"] = record

        await monitor._purge_completed()

        assert "recent-1" in executor._jobs


class TestExitConditionCallback:

    async def test_any_condition(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        record = JobRecord(
            job_id="any-1", name="test-any", command="echo any",
            status=JobStatus.DONE,
        )
        executor._jobs["any-1"] = record

        results = []
        record.on_exit(lambda j: results.append("any"), JobExitCondition.ANY)

        await monitor._dispatch_callbacks(record)
        assert results == ["any"]


    async def test_killed_condition(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        record = JobRecord(
            job_id="kill-1", name="test-kill", command="echo kill",
            status=JobStatus.KILLED,
        )
        executor._jobs["kill-1"] = record

        results = []
        record.on_exit(lambda j: results.append("killed"), JobExitCondition.KILLED)
        record.on_exit(lambda j: results.append("success"), JobExitCondition.SUCCESS)

        await monitor._dispatch_callbacks(record)
        assert results == ["killed"]


    async def test_callbacks_cleared_after_dispatch(self, default_config):
        executor = LocalExecutor(default_config)
        monitor = JobMonitor(executor, poll_interval=0.1)

        record = JobRecord(
            job_id="clear-1", name="test-clear", command="echo clear",
            status=JobStatus.DONE,
        )
        executor._jobs["clear-1"] = record

        results = []
        record.on_success(lambda j: results.append("fired"))

        await monitor._dispatch_callbacks(record)
        await monitor._dispatch_callbacks(record)

        # Should only fire once
        assert results == ["fired"]
