# CLAUDE.md

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
