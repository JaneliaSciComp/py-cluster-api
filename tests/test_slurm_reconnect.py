"""Tests for SlurmExecutor.reconnect(): squeue discovery + sacct fetch."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from cluster_api._types import JobStatus
from cluster_api.config import ClusterConfig
from cluster_api.exceptions import ClusterAPIError
from cluster_api.executors.slurm import SlurmExecutor


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
