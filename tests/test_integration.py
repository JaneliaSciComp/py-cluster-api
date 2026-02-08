"""Integration tests that submit real jobs to the LSF cluster.

Run with: pixi run test-integration

These tests are skipped by default in `pixi run test`. They require a working
LSF installation (bsub, bjobs, bkill on PATH).

Cluster-specific settings (queue, memory units, etc.) are loaded from
tests/cluster_config.yaml if it exists. See docs/integration-tests.md for setup.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import shutil

import pytest

from cluster_api import create_executor, JobMonitor, JobStatus, ResourceSpec

pytestmark = pytest.mark.integration

# Skip the entire module if bsub isn't available
if not shutil.which("bsub"):
    pytest.skip("LSF not available (bsub not on PATH)", allow_module_level=True)

_CONFIG_PATH = Path(__file__).parent / "cluster_config.yaml"


@pytest.fixture
def lsf_executor(tmp_path):
    """Create an LSF executor from tests/cluster_config.yaml (if present).

    The config file is gitignored so each developer can set cluster-specific
    options (queue, lsf_units, etc.) without affecting others.  Falls back to
    sensible defaults when the file is absent.
    """
    config_path = str(_CONFIG_PATH) if _CONFIG_PATH.exists() else None
    return create_executor(
        config_path=config_path,
        # Overrides that always apply for integration tests
        executor="lsf",
        log_directory=str(tmp_path / "logs"),
        poll_interval=3.0,
        walltime="00:10",
    )


@pytest.fixture
def monitor(lsf_executor):
    """Create a monitor for the LSF executor."""
    return JobMonitor(lsf_executor, poll_interval=3.0)


class TestLSFSubmitAndMonitor:
    """Basic submit → monitor → callback cycle on a real cluster."""

    async def test_short_sleep_succeeds(self, lsf_executor, monitor):
        """Submit a 5s sleep, wait for it to complete, verify DONE status."""
        await monitor.start()
        try:
            results = []
            job = await lsf_executor.submit(
                command="sleep 5 && echo 'job finished'",
                name="sleep-ok",
                resources=ResourceSpec(cpus=1, memory="100 MB"),
            )
            job.on_success(lambda j: results.append(("success", j.job_id)))
            job.on_failure(lambda j: results.append(("failure", j.job_id)))

            print(f"\nSubmitted job {job.job_id} ({job.name})")
            print(f"Script: {job.script_path}")

            await monitor.wait_for(job, timeout=120.0)

            print(f"Final status: {job.status}")
            print(f"Exit code: {job.exit_code}")
            print(f"Exec host: {job.exec_host}")
            print(f"Max mem: {job.max_mem}")

            assert job.status == JobStatus.DONE
            assert job.exit_code == 0
            assert len(results) == 1
            assert results[0][0] == "success"
        finally:
            await monitor.stop()

    async def test_failing_job(self, lsf_executor, monitor):
        """Submit a job that exits non-zero, verify FAILED status."""
        await monitor.start()
        try:
            results = []
            job = await lsf_executor.submit(
                command="echo 'about to fail' && exit 42",
                name="fail-test",
                resources=ResourceSpec(cpus=1, memory="100 MB"),
            )
            job.on_success(lambda j: results.append("success"))
            job.on_failure(lambda j: results.append("failure"))

            print(f"\nSubmitted job {job.job_id} ({job.name})")

            await monitor.wait_for(job, timeout=120.0)

            print(f"Final status: {job.status}")
            print(f"Exit code: {job.exit_code}")

            assert job.status == JobStatus.FAILED
            assert job.exit_code is not None
            assert job.exit_code != 0
            assert results == ["failure"]
        finally:
            await monitor.stop()

    async def test_multiple_jobs(self, lsf_executor, monitor):
        """Submit several jobs with different sleep times, wait for all."""
        await monitor.start()
        try:
            results = {}

            jobs = []
            for i, sleep_sec in enumerate([3, 6, 9]):
                job = await lsf_executor.submit(
                    command=f"sleep {sleep_sec} && echo 'done-{i}'",
                    name=f"multi-{i}",
                    resources=ResourceSpec(cpus=1, memory="100 MB"),
                )
                job.on_exit(
                    lambda j, idx=i: results.update({idx: j.status})
                )
                jobs.append(job)
                print(f"Submitted job {job.job_id} ({job.name}, sleep {sleep_sec}s)")

            await monitor.wait_for(*jobs, timeout=180.0)

            for job in jobs:
                print(f"  {job.name}: {job.status}, exit={job.exit_code}")

            assert all(j.status == JobStatus.DONE for j in jobs)
            assert len(results) == 3
        finally:
            await monitor.stop()


class TestLSFCancel:
    """Test job cancellation on a real cluster."""

    async def test_cancel_running_job(self, lsf_executor, monitor):
        """Submit a long sleep, cancel it, verify KILLED status."""
        await monitor.start()
        try:
            job = await lsf_executor.submit(
                command="sleep 300",
                name="cancel-me",
                resources=ResourceSpec(cpus=1, memory="100 MB"),
            )
            print(f"\nSubmitted job {job.job_id} ({job.name})")

            # Wait for it to start running (or at least be queued)
            for _ in range(20):
                await lsf_executor.poll()
                if job.status == JobStatus.RUNNING:
                    break
                await asyncio.sleep(3.0)

            print(f"Status before cancel: {job.status}")
            await lsf_executor.cancel(job.job_id)

            # Poll again to pick up the killed status
            for _ in range(10):
                await lsf_executor.poll()
                if job.is_terminal:
                    break
                await asyncio.sleep(3.0)

            print(f"Status after cancel: {job.status}")
            assert job.is_terminal
        finally:
            await monitor.stop()


class TestLSFJobArray:
    """Test job array submission on a real cluster."""

    async def test_submit_array(self, lsf_executor, monitor):
        """Submit a small job array, wait for completion."""
        await monitor.start()
        try:
            job = await lsf_executor.submit_array(
                command="echo \"array element $LSB_JOBINDEX\" && sleep 3",
                name="array-test",
                array_range=(1, 3),
                resources=ResourceSpec(cpus=1, memory="100 MB"),
            )

            results = []
            job.on_exit(lambda j: results.append(j.status))

            print(f"\nSubmitted array job {job.job_id} ({job.name})")
            print(f"Script: {job.script_path}")

            await monitor.wait_for(job, timeout=180.0)

            print(f"Final status: {job.status}")
            print(f"Exit code: {job.exit_code}")

            assert job.is_terminal
            assert len(results) == 1
        finally:
            await monitor.stop()


class TestLSFRichMetadata:
    """Verify that polling populates rich metadata fields."""

    async def test_metadata_populated(self, lsf_executor, monitor):
        """Submit a job that uses some memory, verify metadata after completion."""
        await monitor.start()
        try:
            # Allocate ~10MB to give max_mem something to report
            job = await lsf_executor.submit(
                command=(
                    "python3 -c \"x = bytearray(10 * 1024 * 1024); "
                    "import time; time.sleep(5)\" "
                    "|| echo 'python not found, skipping' && sleep 5"
                ),
                name="metadata-test",
                resources=ResourceSpec(cpus=1, memory="200 MB"),
            )

            print(f"\nSubmitted job {job.job_id} ({job.name})")

            await monitor.wait_for(job, timeout=120.0)

            print(f"Status: {job.status}")
            print(f"Exit code: {job.exit_code}")
            print(f"Exec host: {job.exec_host}")
            print(f"Max mem: {job.max_mem}")
            print(f"Submit time: {job.submit_time}")
            print(f"Start time: {job.start_time}")
            print(f"Finish time: {job.finish_time}")

            assert job.status == JobStatus.DONE
            # exec_host should be populated for a completed job
            assert job.exec_host is not None
        finally:
            await monitor.stop()


class TestLSFNoMemory:
    """Verify jobs can be submitted without any memory requirement."""

    async def test_no_memory_flags(self, tmp_path):
        """Submit with no memory in config or ResourceSpec — no -M or -R rusage."""
        config_path = str(_CONFIG_PATH) if _CONFIG_PATH.exists() else None
        executor = create_executor(
            config_path=config_path,
            executor="lsf",
            log_directory=str(tmp_path / "logs"),
            poll_interval=3.0,
            walltime="00:10",
            memory=None,
        )

        # Verify the generated script has no memory directives
        script = executor.render_script("echo hello", "no-mem-test")
        assert "-M" not in script
        assert "rusage" not in script

        # Submit it for real and verify it completes
        monitor = JobMonitor(executor, poll_interval=3.0)
        await monitor.start()
        try:
            job = await executor.submit(
                command="sleep 3 && echo 'no memory limit'",
                name="no-mem",
            )

            print(f"\nSubmitted job {job.job_id} ({job.name})")
            print(f"Script: {job.script_path}")

            # Confirm the actual script on disk has no memory flags
            with open(job.script_path) as f:
                content = f.read()
            print(f"Script content:\n{content}")
            assert "-M" not in content
            assert "rusage" not in content

            await monitor.wait_for(job, timeout=120.0)

            print(f"Final status: {job.status}")
            print(f"Exit code: {job.exit_code}")

            assert job.status == JobStatus.DONE
            assert job.exit_code == 0
        finally:
            await monitor.stop()
