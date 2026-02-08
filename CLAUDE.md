# CLAUDE.md

Generic Python library for submitting and monitoring jobs on HPC clusters. Wraps scheduler CLIs (bsub/bjobs/bkill) behind an async executor abstraction with an active polling monitor that fires callbacks on job completion. Inspired by dask-jobqueue's script templating and Nextflow's portable config profiles, but unlike dask-jobqueue, this library actively polls the scheduler rather than relying on workers phoning home.

Founding principles: async-only API, executors are thin wrappers around scheduler CLIs, all state lives in `JobRecord` dataclasses tracked in-process, monitoring is poll-based via `bjobs -json`, and configuration uses Nextflow-style YAML profiles.

Always use `pixi run` to run commands — never invoke pytest, ruff, or other tools directly.

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
  - `_types.py` — `JobStatus`, `JobRecord`, `ResourceSpec`, `JobExitCondition`
  - `config.py` — YAML config loader with profiles
  - `monitor.py` — async polling loop + callback dispatch
  - `executors/lsf.py` — LSF executor (bsub/bjobs/bkill)
  - `executors/local.py` — local subprocess executor (for testing)
- `tests/` — pytest tests; all async tests use pytest-asyncio with `asyncio_mode = "auto"`

## Testing

All tests mock `Executor._call()` to avoid needing a real scheduler (except `test_local.py` which runs real subprocesses). Use `unittest.mock.patch` with `AsyncMock` for async method mocking.

## Style

- Async-only API, no sync wrappers
- Python 3.10+ (uses `X | Y` union syntax)
- Ruff for linting and formatting
