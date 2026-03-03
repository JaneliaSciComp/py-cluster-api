# API Reference

## `create_executor(profile=None, config_path=None, **overrides)`

Factory function that loads config and returns an `Executor` instance.

## `Executor`

Abstract base class. Key methods:

- `submit(command, name, resources=None, prologue=None, epilogue=None, env=None, metadata=None)` — submit a job, returns `JobRecord`
- `submit_array(command, name, array_range, ...)` — submit a job array
- `cancel(job_id, *, done=False)` — cancel a job by ID. By default marks the job as `KILLED`; pass `done=True` to mark it as `DONE` instead (useful for graceful pipeline termination where you don't want downstream logic to treat the cancellation as a failure)
- `cancel_by_name(name_pattern)` — cancel jobs matching a name pattern (LSF only)
- `cancel_all(*, done=False)` — cancel all tracked non-terminal jobs
- `reconnect()` — rediscover running jobs after a process restart (requires `job_name_prefix`)
- `poll()` — query scheduler and update job statuses
- `jobs` / `active_jobs` — properties returning tracked job dicts

## `JobRecord`

Tracks a submitted job. Fields include `job_id`, `name`, `status`, `exit_code`, `exec_host`, `max_mem`, `submit_time`, `start_time`, `finish_time`, and `metadata`.

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
