# Development Notes

## Getting Started

You will need [Pixi](https://pixi.sh/latest/) to build this project.

```bash
pixi install
```

This creates two environments:

- **default** — development + testing (pytest, pytest-asyncio, pytest-cov, ruff)
- **release** — packaging + publishing (build, twine)

The library itself has only one runtime dependency: **PyYAML**.

## Testing

Always use `pixi run` — never invoke pytest or ruff directly.

```bash
pixi run test          # run unit tests
pixi run test-v        # verbose output
pixi run test-cov      # with coverage report
pixi run lint          # ruff check
pixi run fmt           # ruff format
pixi run check         # lint + test together
```

### Test layout

| File | What it tests | Real subprocesses? |
|---|---|---|
| `test_config.py` | YAML loading, profiles, memory parsing | No |
| `test_core.py` | Base `Executor` logic (submit, poll, cancel, zombies, arrays) | No — mocks `_call()` |
| `test_lsf.py` | `LSFExecutor` header building, bsub submission, bjobs parsing, array rewriting | No — mocks `_call()` |
| `test_local.py` | `LocalExecutor` end-to-end (submit, poll, output files, callbacks, array jobs) | **Yes** — runs real bash subprocesses |
| `test_monitor.py` | `JobMonitor` polling loop, callback dispatch, zombie detection, purging | No — mocks `poll()` |
| `test_integration.py` | Full LSF round-trips (submit, monitor, cancel, arrays, metadata) | **Yes** — requires a live LSF cluster |

### Writing tests

- All async tests use **pytest-asyncio** with `asyncio_mode = "auto"` (set in `pyproject.toml`), so no `@pytest.mark.asyncio` decorator is needed.
- Mock `Executor._call()` with `unittest.mock.patch` + `AsyncMock` to avoid hitting a real scheduler. Example:

```python
with patch.object(executor, "_call", new_callable=AsyncMock, return_value="Job <123> ...") as mock:
    job = await executor.submit(command="echo hi", name="test", resources=ResourceSpec(work_dir=work_dir))
    mock.assert_called_once()
```

- Shared fixtures live in `tests/conftest.py`: `work_dir` (temp directory), `default_config` (local), and `lsf_config` (LSF with stdin mode).
- `test_local.py` runs real subprocesses — keep commands fast (`echo`, `pwd`, short `sleep`).

### Integration tests

Integration tests submit real jobs to an LSF cluster and are **skipped by default** (via the `integration` pytest marker in `pyproject.toml`).

```bash
pixi run test-integration
```

Prerequisites:
- `bsub`, `bjobs`, `bkill` must be on `PATH`
- Optionally create `tests/cluster_config.yaml` (gitignored) with cluster-specific settings like `queue`, `lsf_units`, etc.

The tests cover: single job success/failure, multiple concurrent jobs, cancellation, job arrays, rich metadata population, and submission without memory limits.

## Architecture

### Key abstractions

```
create_executor()          # factory: loads config → picks executor class
    ↓
Executor (core.py)         # abstract base: submit, poll, cancel
    ├── LSFExecutor         # bsub/bjobs/bkill via _call()
    └── LocalExecutor       # asyncio subprocesses
    ↓
JobMonitor (monitor.py)    # async polling loop → callbacks + zombie detection
```

### Execution flow

1. **Configuration** — `load_config()` reads YAML, merges base → profile → overrides into a `ClusterConfig` dataclass.
2. **Submission** — `Executor.submit()` prefixes the job name, calls the subclass `_submit_job()` which renders a script (via `script.py`), writes it to disk, and invokes the scheduler CLI.
3. **Polling** — `Executor.poll()` runs the status command (e.g. `bjobs -json`), parses output, and updates `JobRecord` fields. For array jobs, element-level statuses are tracked in `ArrayElement` instances and aggregated via `compute_array_status()`.
4. **Monitoring** — `JobMonitor` drives `poll()` in a loop, dispatches callbacks on terminal jobs, detects zombies (jobs missing from scheduler output beyond `zombie_timeout_minutes`), and purges old records.
5. **Callbacks** — registered on `JobRecord` via `on_success()`, `on_failure()`, or `on_exit()`. Both sync and async callables are supported. Fired once, then removed.

### Script generation

`script.py` renders job scripts using a simple template:

```
{shebang}
{scheduler directives}
{prologue lines}
{command}
{epilogue lines}
```

- `build_header()` (per executor) produces directive lines from `ResourceSpec` + config defaults.
- `directives_skip` filters out unwanted lines; `extra_directives` appends custom ones.
- Scripts are written to `{work_dir}/{safe_name}.{counter}.sh` and made executable.

### Log file naming

Default log files include the job ID so each job gets unique output:

| Executor | stdout | stderr |
|---|---|---|
| LSF | `stdout.%J.log` | `stderr.%J.log` |
| LSF array | `stdout.%J.%I.log` | `stderr.%J.%I.log` |
| Local | `stdout.{job_id}.log` | `stderr.{job_id}.log` |
| Local array | `stdout.{job_id}.{index}.log` | `stderr.{job_id}.{index}.log` |

Setting `stdout_path` / `stderr_path` on `ResourceSpec` overrides these defaults.

### Job lifecycle

```
PENDING → RUNNING → DONE (exit 0)
                  → FAILED (non-zero exit, or zombie timeout)
                  → KILLED (cancel)
```

Terminal jobs are purged from memory after `completed_retention_minutes` (once all callbacks have fired).

### Key design decisions

- **Poll-based monitoring** — unlike dask-jobqueue (which relies on workers phoning home), this library actively polls the scheduler. This means it works with any executable, not just Python workers.
- **Stdin submission** — LSF's `bsub < script.sh` mode avoids filesystem race conditions on shared storage. Controlled by `use_stdin` config.
- **Job name prefixing** — all jobs get a `{prefix}-{name}` name. The prefix is either configured (`job_name_prefix`) or randomly generated, so concurrent sessions don't collide when polling by name.
- **Array status aggregation** — parent array job status is computed from element statuses. Only transitions to terminal when ALL expected elements are terminal.

## Module reference

| Module | Purpose |
|---|---|
| `__init__.py` | Public API, `create_executor()` factory |
| `_types.py` | `JobStatus`, `JobRecord`, `ResourceSpec`, `JobExitCondition`, `ArrayElement` |
| `config.py` | `ClusterConfig` dataclass, `load_config()`, YAML search paths |
| `core.py` | Abstract `Executor` base class, `_call()` subprocess helper |
| `script.py` | `render_script()`, `write_script()` |
| `monitor.py` | `JobMonitor` — polling loop, callback dispatch, zombie/purge logic |
| `exceptions.py` | `ClusterAPIError`, `CommandTimeoutError`, `CommandFailedError`, `SubmitError` |
| `executors/lsf.py` | `LSFExecutor` — bsub/bjobs/bkill, header building, bjobs JSON parsing |
| `executors/local.py` | `LocalExecutor` — asyncio subprocesses, stdout/stderr capture, array job simulation |

## Release

First, increment the version in `pyproject.toml` and push it to GitHub. Create a *Release* there and then publish it to PyPI as follows.

To create a Python source package (`.tar.gz`) and the binary package (`.whl`) in the `dist/` directory, do:

```bash
pixi run -e release pypi-build
```

To upload the package to PyPI, you'll need one of the project owners to add you as a collaborator. After setting up your access token, do:

```bash
pixi run -e release pypi-upload
```
