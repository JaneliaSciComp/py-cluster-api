# CLAUDE.md

Generic Python library for submitting and monitoring jobs on HPC clusters. Wraps scheduler CLIs (bsub/bjobs/bkill) behind an async executor abstraction with an active polling monitor that fires callbacks on job completion. Inspired by dask-jobqueue's script templating and Nextflow's portable config profiles, but unlike dask-jobqueue, this library actively polls the scheduler rather than relying on workers phoning home.

Founding principles: async-only API, executors are thin wrappers around scheduler CLIs, all state lives in `JobRecord` dataclasses tracked in-process, monitoring is poll-based via `bjobs -json`, and configuration uses Nextflow-style YAML profiles.

Always use `pixi run` to run commands — never invoke python, pytest, ruff, or other tools directly.

## Build & Run

```bash
pixi install          # set up environment
pixi run test         # run tests
pixi run test-v       # run tests (verbose)
pixi run test-cov     # run tests with coverage
pixi run lint         # lint with ruff
pixi run fmt          # format with ruff
pixi run check        # lint + test
```

## Project Structure

- `cluster_api/` — library source
  - `core.py` — abstract `Executor` base class
  - `_types.py` — `JobStatus`, `JobRecord`, `ResourceSpec`, `JobExitCondition`, `ArrayElement`
  - `config.py` — YAML config loader with profiles
  - `script.py` — script rendering (`render_script`) and writing (`write_script`)
  - `monitor.py` — async polling loop + callback dispatch
  - `exceptions.py` — `ClusterAPIError`, `CommandTimeoutError`, `CommandFailedError`, `SubmitError`
  - `executors/lsf.py` — LSF executor (bsub/bjobs/bkill)
  - `executors/local.py` — local subprocess executor (for testing), including array job simulation
- `tests/` — pytest tests; all async tests use pytest-asyncio with `asyncio_mode = "auto"`

## Log File Naming

Default log files include the job ID for uniqueness:
- **LSF**: `stdout.%J.log` / `stderr.%J.log` (`%J` = job ID); array jobs use `stdout.%J.%I.log` (`%I` = array index)
- **Local**: `stdout.{job_id}.log` / `stderr.{job_id}.log`; array jobs use `stdout.{job_id}.{index}.log` (`$ARRAY_INDEX` env var per element)

Explicit `stdout_path` / `stderr_path` in `ResourceSpec` override these defaults.

## Testing

All tests mock `Executor._call()` to avoid needing a real scheduler (except `test_local.py` which runs real subprocesses). Use `unittest.mock.patch` with `AsyncMock` for async method mocking.

## Style

- Async-only API, no sync wrappers
- Python 3.10+ (uses `X | Y` union syntax)
- Ruff for linting and formatting
