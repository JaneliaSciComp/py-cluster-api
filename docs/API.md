# API Reference

## `create_executor(profile=None, config_path=None, **overrides)`

Factory function that loads config and returns an `Executor` instance.

## `Executor`

Abstract base class. Key methods:

- `submit(command, name, resources=None, prologue=None, epilogue=None, env=None, metadata=None, inherit_env=True, login_shell=False, depends_on=None)` — submit a job, returns `JobRecord`
  - `inherit_env`: when `True` (default), the job inherits the submitting process's environment; when `False`, only `env` plus scheduler-provided variables reach the job
  - `login_shell`: when `True`, the job runs under a login shell so the target user's own profile builds the environment (PATH, modules, conda)
  - `depends_on`: `JobRecord`s or raw job-id strings that must finish successfully before this job starts (fan-in supported); a failed/killed dependency cancels this job instead of leaving it pending forever
- `submit_array(command, name, array_range, ...)` — submit a job array
- `cancel(job_id, *, done=False)` — cancel a job by ID. By default marks the job as `KILLED`; pass `done=True` to mark it as `DONE` instead (useful for graceful pipeline termination where you don't want downstream logic to treat the cancellation as a failure). The local executor raises `ClusterAPIError` if the job's process group survives `SIGKILL`
- `cancel_by_name(name_pattern)` — cancel jobs matching a name pattern (LSF only)
- `cancel_all(*, done=False)` — cancel all tracked non-terminal jobs
- `reconnect()` — rediscover running jobs after a process restart (requires `job_name_prefix`)
- `track(job_id, status=JobStatus.PENDING)` — begin tracking a job by ID without re-submitting it, e.g. to seed the executor from a persistent store; the next `poll()` fills in the rest from the scheduler
- `remove_job(job_id)` — stop tracking a job
- `poll()` — query scheduler and update job statuses
- `jobs` / `active_jobs` — properties returning tracked job dicts

## `JobRecord`

Tracks a submitted job. Fields include `job_id`, `name`, `status`, `exit_code`, `exec_host`, `max_mem`, `submit_time`, `start_time`, `finish_time`, `depends_on`, and `metadata`. If a dependency fails, `metadata["dependency_failed"]` lists the failed upstream job ids.

- `on_success(callback)` — register callback for exit code 0
- `on_failure(callback)` — register callback for non-zero exit
- `on_exit(callback, condition=ANY)` — register callback for any exit condition
- `is_terminal` — whether the job has finished

## `JobMonitor`

Async polling loop that drives status updates and callback dispatch.

- `start()` / `stop()` — control the polling loop
- `wait_for(*records, timeout=None)` — block until jobs reach a terminal state

The monitor does not support `async with`, so use `try/finally` to ensure cleanup:

```python
monitor = JobMonitor(executor)
await monitor.start()
try:
    job = await executor.submit(command="echo hi", name="test")
    await monitor.wait_for(job)
finally:
    await monitor.stop()
```

## `ResourceSpec`

Resource requirements: `cpus`, `gpus`, `memory`, `walltime`, `queue`, `work_dir`, `stdout_path`, `stderr_path`, `extra_directives`, `extra_args`.

## Error Handling

All exceptions inherit from `ClusterAPIError`, so you can catch broadly or narrowly:

```python
from cluster_api import ClusterAPIError, SubmitError, CommandTimeoutError, CommandFailedError

try:
    job = await executor.submit(command="echo hi", name="test")
except SubmitError as e:
    # Could not parse job ID from scheduler output
    print(f"Submission failed: {e}")
except CommandTimeoutError as e:
    # Scheduler command (bsub, bjobs, bkill) exceeded command_timeout
    print(f"Scheduler timed out: {e}")
except CommandFailedError as e:
    # Scheduler command returned a non-zero exit code
    print(f"Scheduler error: {e}")
```

| Exception | Raised when |
|---|---|
| `ClusterAPIError` | Base class for all library errors |
| `SubmitError` | Job ID could not be parsed from submit output |
| `CommandTimeoutError` | A scheduler CLI command exceeded `command_timeout` |
| `CommandFailedError` | A scheduler CLI command exited with non-zero status |
