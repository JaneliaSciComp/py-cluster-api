"""Tests for the Local executor — end-to-end with real subprocesses."""

from __future__ import annotations

import asyncio
from pathlib import Path

from cluster_api._types import JobStatus, ResourceSpec
from cluster_api.executors.local import LocalExecutor


class TestLocalSubmitAndPoll:

    async def test_submit_and_poll_success(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo hello", name="echo-test",
            resources=ResourceSpec(work_dir=work_dir),
        )
        assert job.status == JobStatus.PENDING

        # Wait for subprocess to finish
        proc = executor._processes[job.job_id]
        await proc.wait()

        await executor.poll()
        assert job.status == JobStatus.DONE
        assert job.exit_code == 0


    async def test_submit_and_poll_failure(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="exit 1", name="fail-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

        proc = executor._processes[job.job_id]
        await proc.wait()

        await executor.poll()
        assert job.status == JobStatus.FAILED
        assert job.exit_code == 1


    async def test_running_job(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="sleep 10", name="sleep-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

        # Poll while still running
        await asyncio.sleep(0.1)
        await executor.poll()
        assert job.status == JobStatus.RUNNING

        # Cleanup
        await executor.cancel(job.job_id)


    async def test_cancel(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="sleep 60", name="cancel-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

        await asyncio.sleep(0.1)
        await executor.cancel(job.job_id)
        assert job.status == JobStatus.KILLED


    async def test_multiple_jobs(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job1 = await executor.submit(
            command="echo one", name="job1",
            resources=ResourceSpec(work_dir=work_dir),
        )
        job2 = await executor.submit(
            command="echo two", name="job2",
            resources=ResourceSpec(work_dir=work_dir),
        )

        assert job1.job_id != job2.job_id

        proc1 = executor._processes[job1.job_id]
        proc2 = executor._processes[job2.job_id]
        await proc1.wait()
        await proc2.wait()

        await executor.poll()
        assert job1.status == JobStatus.DONE
        assert job2.status == JobStatus.DONE


    async def test_jobs_property(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo hello", name="prop-test",
            resources=ResourceSpec(work_dir=work_dir),
        )
        assert job.job_id in executor.jobs
        assert job.job_id in executor.active_jobs

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        assert job.job_id in executor.jobs
        assert job.job_id not in executor.active_jobs


class TestLocalOutputFiles:

    async def test_stdout_written_to_out_file(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo hello", name="out-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        out_file = Path(work_dir) / f"stdout.{job.job_id}.log"
        err_file = Path(work_dir) / f"stderr.{job.job_id}.log"
        assert out_file.exists()
        assert err_file.exists()
        assert out_file.read_text().strip() == "hello"
        assert err_file.read_text() == ""

    async def test_stderr_written_to_err_file(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo oops >&2", name="err-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        out_file = Path(work_dir) / f"stdout.{job.job_id}.log"
        err_file = Path(work_dir) / f"stderr.{job.job_id}.log"
        assert out_file.read_text() == ""
        assert err_file.read_text().strip() == "oops"

    async def test_failed_job_writes_output_files(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo failing >&2; exit 1", name="fail-out-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        err_file = Path(work_dir) / f"stderr.{job.job_id}.log"
        assert err_file.read_text().strip() == "failing"
        assert job.status == JobStatus.FAILED


class TestLocalWorkDir:

    async def test_work_dir_sets_cwd(self, default_config, tmp_path):
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="pwd",
            name="cwd-test",
            resources=ResourceSpec(work_dir=str(work_dir)),
        )

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        out_file = work_dir / f"stdout.{job.job_id}.log"
        assert out_file.read_text().strip() == str(work_dir)

    async def test_default_cwd_without_work_dir(self, default_config, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        executor = LocalExecutor(default_config)
        job = await executor.submit(command="pwd", name="no-cwd-test")

        proc = executor._processes[job.job_id]
        await proc.wait()
        await executor.poll()

        out_file = tmp_path / f"stdout.{job.job_id}.log"
        # Without work_dir, inherits the current process cwd
        assert out_file.read_text().strip() == str(tmp_path)


class TestLocalCallback:

    async def test_callback_fires_on_success(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="echo hello", name="cb-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

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


    async def test_callback_fires_on_failure(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit(
            command="exit 42", name="fail-cb-test",
            resources=ResourceSpec(work_dir=work_dir),
        )

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


class TestLocalArrayJobs:

    async def test_array_submit_and_poll(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit_array(
            command="echo $ARRAY_INDEX", name="array-test",
            array_range=(1, 3),
            resources=ResourceSpec(work_dir=work_dir),
        )
        assert job.status == JobStatus.PENDING
        assert job.is_array

        # Wait for all element processes to finish
        for key, proc in executor._processes.items():
            if key.startswith(f"{job.job_id}["):
                await proc.wait()

        await executor.poll()
        assert job.status == JobStatus.DONE
        assert len(job.array_elements) == 3
        for idx in (1, 2, 3):
            assert job.array_elements[idx].status == JobStatus.DONE
            assert job.array_elements[idx].exit_code == 0

    async def test_array_partial_failure(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit_array(
            command='if [ "$ARRAY_INDEX" -eq 2 ]; then exit 1; fi; echo ok',
            name="partial-fail",
            array_range=(1, 3),
            resources=ResourceSpec(work_dir=work_dir),
        )

        for key, proc in executor._processes.items():
            if key.startswith(f"{job.job_id}["):
                await proc.wait()

        await executor.poll()
        assert job.status == JobStatus.FAILED
        assert job.array_elements[1].status == JobStatus.DONE
        assert job.array_elements[2].status == JobStatus.FAILED
        assert job.array_elements[3].status == JobStatus.DONE
        assert job.failed_element_indices == [2]

    async def test_array_output_files(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit_array(
            command="echo hello", name="array-out",
            array_range=(1, 2),
            resources=ResourceSpec(work_dir=work_dir),
        )

        for key, proc in executor._processes.items():
            if key.startswith(f"{job.job_id}["):
                await proc.wait()

        await executor.poll()

        for idx in (1, 2):
            out_file = Path(work_dir) / f"stdout.{job.job_id}.{idx}.log"
            err_file = Path(work_dir) / f"stderr.{job.job_id}.{idx}.log"
            assert out_file.exists(), f"Missing stdout for element {idx}"
            assert err_file.exists(), f"Missing stderr for element {idx}"
            assert out_file.read_text().strip() == "hello"

    async def test_array_env_variable(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit_array(
            command="echo $ARRAY_INDEX", name="array-env",
            array_range=(5, 7),
            resources=ResourceSpec(work_dir=work_dir),
        )

        for key, proc in executor._processes.items():
            if key.startswith(f"{job.job_id}["):
                await proc.wait()

        await executor.poll()

        for idx in (5, 6, 7):
            out_file = Path(work_dir) / f"stdout.{job.job_id}.{idx}.log"
            assert out_file.read_text().strip() == str(idx)

    async def test_array_cancel(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        job = await executor.submit_array(
            command="sleep 60", name="array-cancel",
            array_range=(1, 3),
            resources=ResourceSpec(work_dir=work_dir),
        )

        await asyncio.sleep(0.1)
        await executor.cancel(job.job_id)
        assert job.status == JobStatus.KILLED
        for elem in job.array_elements.values():
            assert elem.status == JobStatus.KILLED
