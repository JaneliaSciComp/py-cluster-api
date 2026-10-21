"""Integration tests that submit real jobs to a SLURM cluster.

Run with: pixi run test-integration

Skipped by default in `pixi run test`, and skipped at module level when
`sbatch` is not on PATH — unless CLUSTER_API_REQUIRE_SLURM=1 is set (CI),
in which case a missing scheduler fails the module instead of skipping.
Requires slurmdbd accounting (sacct).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from cluster_api import JobMonitor, JobStatus, ResourceSpec, create_executor

pytestmark = pytest.mark.integration

_REQUIRE = os.environ.get("CLUSTER_API_REQUIRE_SLURM") == "1"

if not shutil.which("sbatch"):
    if _REQUIRE:
        pytest.fail("CLUSTER_API_REQUIRE_SLURM=1 but sbatch is not on PATH")
    pytest.skip("SLURM not available (sbatch not on PATH)", allow_module_level=True)

_OUTPUT_DIR = Path(__file__).resolve().parent.parent / ".test_output"
_PREFIX = f"capitest{os.getpid()}"


@pytest.fixture
def work_dir():
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return str(_OUTPUT_DIR)


def _make_executor(**overrides):
    return create_executor(
        config_path=None,
        executor="slurm",
        job_name_prefix=_PREFIX,
        poll_interval=2.0,
        walltime="00:05:00",   # HH:MM:SS; on SLURM "05:00" would mean five minutes
        command_timeout=30.0,
        **overrides,
    )


@pytest.fixture
def slurm_executor():
    return _make_executor()


@pytest.fixture
def monitor(slurm_executor):
    return JobMonitor(slurm_executor, poll_interval=2.0)


def _read_log(work_dir: str, job_id: str) -> str:
    return (Path(work_dir) / f"stdout.{job_id}.log").read_text()


class TestAccountingAvailable:
    def test_sacct_works(self):
        """Fails loudly when slurmdbd accounting is not configured."""
        proc = subprocess.run(["sacct", "-n", "-X", "-j", "1", "-o", "JobID"],
                              capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr


class TestSubmitAndMonitor:

    async def test_short_sleep_succeeds(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            results = []
            job = await slurm_executor.submit(
                command="sleep 3 && echo 'job finished'", name="sleep-ok",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            job.on_success(lambda j: results.append("success"))
            job.on_failure(lambda j: results.append("failure"))
            await monitor.wait_for(job, timeout=180.0)
            assert job.status == JobStatus.DONE
            assert job.exit_code == 0
            assert job.exec_host
            assert job.start_time is not None and job.finish_time is not None
            assert results == ["success"]
            assert "job finished" in _read_log(work_dir, job.job_id)
        finally:
            await monitor.stop()

    async def test_failing_job(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command="echo 'about to fail' && exit 42", name="fail",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=180.0)
            assert job.status == JobStatus.FAILED
            assert job.exit_code == 42
        finally:
            await monitor.stop()

    async def test_signal_killed_job_reports_negative_exit_code(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command="kill -9 $$", name="sigkill",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=180.0)
            assert job.status == JobStatus.FAILED
            assert job.exit_code == -9
        finally:
            await monitor.stop()


class TestCancel:

    async def test_cancel_running_job(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command="sleep 120", name="cancel-me",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            # Let it start so sacct has a row
            await asyncio.sleep(6)
            await slurm_executor.cancel(job.job_id)
            assert job.status == JobStatus.KILLED
            await asyncio.sleep(6)
            await slurm_executor.poll()
            assert job.status == JobStatus.KILLED
        finally:
            await monitor.stop()


class TestJobArray:

    async def test_submit_array(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit_array(
                command='echo "element $SLURM_ARRAY_TASK_ID"', name="arr",
                array_range=(1, 3),
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=240.0)
            assert job.status == JobStatus.DONE
            assert sorted(job.array_elements) == [1, 2, 3]
            assert all(e.status == JobStatus.DONE for e in job.array_elements.values())
            assert all(e.exit_code == 0 for e in job.array_elements.values())
            log = (Path(work_dir) / f"stdout.{job.job_id}_2.log").read_text()
            assert "element 2" in log
        finally:
            await monitor.stop()


class TestDependencies:

    async def test_chain_success(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            a = await slurm_executor.submit(command="sleep 2 && echo A", name="dep-a",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir))
            b = await slurm_executor.submit(command="echo B", name="dep-b",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir),
                                            depends_on=[a])
            await monitor.wait_for(a, b, timeout=300.0)
            assert a.status == JobStatus.DONE
            assert b.status == JobStatus.DONE
            assert b.start_time >= a.finish_time
        finally:
            await monitor.stop()

    async def test_chain_failure_cancels_dependent(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            a = await slurm_executor.submit(command="exit 1", name="dep-fail-a",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir))
            b = await slurm_executor.submit(command="echo never", name="dep-fail-b",
                                            resources=ResourceSpec(cpus=1, work_dir=work_dir),
                                            depends_on=[a])
            await monitor.wait_for(a, b, timeout=300.0)
            assert a.status == JobStatus.FAILED
            assert b.status == JobStatus.KILLED
            assert b.metadata["dependency_failed"] == [a.job_id]
        finally:
            await monitor.stop()


class TestEnvironment:

    async def test_clean_env(self, slurm_executor, monitor, work_dir):
        os.environ["CAPI_MARKER"] = "present"
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command='echo "FOO=$FOO"; echo "MARKER=${CAPI_MARKER:-absent}"',
                name="clean-env",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
                env={"FOO": "bar"}, inherit_env=False,
            )
            await monitor.wait_for(job, timeout=180.0)
            log = _read_log(work_dir, job.job_id)
            assert "FOO=bar" in log
            assert "MARKER=absent" in log
        finally:
            await monitor.stop()
            os.environ.pop("CAPI_MARKER", None)

    async def test_inherit_env_default(self, slurm_executor, monitor, work_dir):
        os.environ["CAPI_MARKER"] = "present"
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command='echo "MARKER=${CAPI_MARKER:-absent}"', name="inherit-env",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
            )
            await monitor.wait_for(job, timeout=180.0)
            assert "MARKER=present" in _read_log(work_dir, job.job_id)
        finally:
            await monitor.stop()
            os.environ.pop("CAPI_MARKER", None)

    async def test_login_shell(self, slurm_executor, monitor, work_dir):
        await monitor.start()
        try:
            job = await slurm_executor.submit(
                command='shopt -q login_shell && echo LOGIN=yes || echo LOGIN=no',
                name="login-shell",
                resources=ResourceSpec(cpus=1, work_dir=work_dir),
                login_shell=True,
            )
            await monitor.wait_for(job, timeout=180.0)
            assert "LOGIN=yes" in _read_log(work_dir, job.job_id)
        finally:
            await monitor.stop()


class TestReconnect:

    async def test_reconnect_finds_running_held_and_partial_array(self, slurm_executor, work_dir):
        # The array goes first: on a one-CPU node (as in CI) a long-running
        # single job submitted earlier would starve it, and the point of the
        # test is a partially completed array. Element 1 exits immediately,
        # element 2 then holds the CPU; the single job may stay PENDING.
        arr = await slurm_executor.submit_array(
            command='[ "$SLURM_ARRAY_TASK_ID" = 1 ] && exit 0; sleep 90', name="rc-arr",
            array_range=(1, 3), max_concurrent=1,
            resources=ResourceSpec(cpus=1, work_dir=work_dir),
        )
        queued = await slurm_executor.submit(command="sleep 90", name="rc-run",
                                             resources=ResourceSpec(cpus=1, work_dir=work_dir))
        held = await slurm_executor.submit(command="echo held", name="rc-held",
                                           resources=ResourceSpec(cpus=1, work_dir=work_dir,
                                                                  extra_args=["--hold"]))
        try:
            # wait until element 1 has finished and element 2 is running
            for _ in range(60):
                await slurm_executor.poll()
                elems = arr.array_elements
                if (elems.get(1) and elems[1].status == JobStatus.DONE
                        and elems.get(2) and elems[2].status == JobStatus.RUNNING):
                    break
                await asyncio.sleep(2)
            else:
                pytest.fail(f"array never reached the partial state: {arr.array_elements}")

            fresh = _make_executor()
            recovered = await fresh.reconnect()
            by_id = {r.job_id: r for r in recovered}
            assert queued.job_id in by_id
            assert by_id[queued.job_id].status in (JobStatus.PENDING, JobStatus.RUNNING)
            assert held.job_id in by_id
            assert by_id[held.job_id].status == JobStatus.PENDING
            assert arr.job_id in by_id
            rec_arr = by_id[arr.job_id]
            assert rec_arr.is_array
            assert rec_arr.metadata["array_range"] == (1, 3)
            assert rec_arr.array_elements[1].status == JobStatus.DONE
            assert rec_arr.array_elements[1].exit_code == 0
            assert rec_arr.array_elements[2].status == JobStatus.RUNNING
            assert rec_arr.status == JobStatus.RUNNING
        finally:
            await slurm_executor.cancel_all()
