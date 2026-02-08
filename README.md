# py-cluster-api

A Python library for submitting and monitoring jobs on HPC clusters. Supports running arbitrary executables (Nextflow pipelines, Python scripts, Java tools, etc.) on LSF clusters and taking action when jobs complete via async callbacks.

## Features

- **Async-first** — built on `asyncio` for non-blocking job submission and monitoring
- **LSF executor** — submit via `bsub`, monitor via `bjobs -json`, cancel via `bkill`
- **Local executor** — run jobs as local subprocesses for development and testing
- **Job monitoring** — polls the scheduler and fires callbacks on job completion, failure, or cancellation
- **Job arrays** — submit array jobs with per-element log files
- **Zombie detection** — jobs that disappear from the scheduler are marked as failed
- **YAML config with profiles** — Nextflow-style config with per-environment profiles
- **Callback chaining** — register `on_success`, `on_failure`, or `on_exit` handlers on any job

## Installation

Requires Python 3.10+.

```bash
pixi install
```

Or install the package directly:

```bash
pip install -e .
```

## Quick Start

### Single Job

```python
import asyncio
from cluster_api import create_executor, ResourceSpec, JobMonitor

async def main():
    executor = create_executor(profile="janelia_lsf")
    monitor = JobMonitor(executor)
    await monitor.start()

    job = await executor.submit(
        command="nextflow run nf-core/rnaseq --input samples.csv",
        name="rnaseq-run",
        resources=ResourceSpec(cpus=4, memory="32 GB", walltime="24:00", queue="long"),
        env={"NXF_WORK": "/scratch/work"},
    )
    job.on_success(lambda j: print(f"Done! Job {j.job_id}, peak mem: {j.max_mem}"))
    job.on_failure(lambda j: print(f"FAILED! Job {j.job_id}, exit={j.exit_code}"))

    await monitor.wait_for(job)
    await monitor.stop()

asyncio.run(main())
```

### Job Array

```python
async def run_array():
    executor = create_executor(profile="janelia_lsf")
    monitor = JobMonitor(executor)
    await monitor.start()

    job = await executor.submit_array(
        command="python process.py --index $LSB_JOBINDEX",
        name="batch-process",
        array_range=(1, 50),
        resources=ResourceSpec(cpus=1, memory="4 GB", walltime="01:00"),
    )
    job.on_exit(lambda j: print(f"Array finished: {j.job_id}"))

    await monitor.wait_for(job)
    await monitor.stop()
```

### Local Testing

```python
async def local_test():
    executor = create_executor(executor="local")
    monitor = JobMonitor(executor, poll_interval=1.0)
    await monitor.start()

    job = await executor.submit(command="echo hello world", name="test")
    job.on_success(lambda j: print("It worked!"))

    await monitor.wait_for(job, timeout=10.0)
    await monitor.stop()
```

## Configuration

Configuration is loaded from YAML with optional profiles. The search order is:

1. Explicit `config_path` argument
2. `$CLUSTER_API_CONFIG` environment variable
3. `./cluster_api.yaml`
4. `~/.config/cluster_api/config.yaml`

### Example `cluster_api.yaml`

```yaml
executor: local
poll_interval: 10
log_directory: ./logs
job_name_prefix: "capi"

profiles:
  janelia_lsf:
    executor: lsf
    queue: normal
    memory: "8 GB"
    walltime: "04:00"
    use_stdin: true
    script_prologue:
      - "module load java/11"

  local_dev:
    executor: local
    poll_interval: 2
```

### Config Options

| Option | Default | Description |
|---|---|---|
| `executor` | `"local"` | Backend: `lsf` or `local` |
| `cpus` | `None` | Default CPU count |
| `memory` | `None` | Default memory (e.g. `"8 GB"`) |
| `walltime` | `None` | Default wall time (e.g. `"04:00"`) |
| `queue` | `None` | Default queue/partition |
| `account` | `None` | Account/project for billing |
| `poll_interval` | `10.0` | Seconds between status polls |
| `log_directory` | `"./logs"` | Where job scripts and logs are written |
| `job_name_prefix` | `"capi"` | Prefix for all job names |
| `shebang` | `"#!/bin/bash"` | Script shebang line |
| `script_prologue` | `[]` | Lines inserted before the command |
| `script_epilogue` | `[]` | Lines inserted after the command |
| `extra_directives` | `[]` | Additional scheduler directives |
| `directives_skip` | `[]` | Substrings to filter out of directives |
| `use_stdin` | `false` | Submit via stdin (`bsub < script.sh`) |
| `lsf_units` | `"MB"` | LSF memory units (`KB`, `MB`, `GB`) |
| `suppress_job_email` | `true` | Set `LSB_JOB_REPORT_MAIL=N` |
| `command_timeout` | `100.0` | Timeout in seconds for scheduler commands |
| `zombie_timeout_minutes` | `30.0` | Mark jobs as failed if unseen for this long |
| `completed_retention_minutes` | `10.0` | Keep finished jobs in memory for this long |

## API Reference

### `create_executor(profile=None, config_path=None, **overrides)`

Factory function that loads config and returns an `Executor` instance.

### `Executor`

Abstract base class. Key methods:

- `submit(command, name, resources=None, prologue=None, epilogue=None, env=None, metadata=None)` — submit a job, returns `JobRecord`
- `submit_array(command, name, array_range, ...)` — submit a job array
- `cancel(job_id)` — cancel a job by ID
- `cancel_by_name(name_pattern)` — cancel by name pattern (LSF only)
- `cancel_all()` — cancel all tracked jobs
- `poll()` — query scheduler and update job statuses
- `jobs` / `active_jobs` — properties returning tracked job dicts

### `JobRecord`

Tracks a submitted job. Fields include `job_id`, `name`, `status`, `exit_code`, `exec_host`, `max_mem`, `submit_time`, `start_time`, `finish_time`, and `metadata`.

- `on_success(callback)` — register callback for exit code 0
- `on_failure(callback)` — register callback for non-zero exit
- `on_exit(callback, condition=ANY)` — register callback for any exit condition
- `is_terminal` — whether the job has finished

### `JobMonitor`

Async polling loop that drives status updates and callback dispatch.

- `start()` / `stop()` — control the polling loop
- `wait_for(*records, timeout=None)` — block until jobs reach a terminal state

### `ResourceSpec`

Resource requirements: `cpus`, `memory`, `walltime`, `queue`, `account`, `work_dir`, `cluster_options`.

## Development

```bash
pixi install

# Run tests
pixi run test

# Run tests with verbose output
pixi run test-v

# Run tests with coverage
pixi run test-cov

# Lint
pixi run lint

# Format
pixi run fmt

# Lint + test
pixi run check
```

## Architecture

```
cluster_api/
├── __init__.py          # Public API + create_executor() factory
├── _types.py            # JobStatus, JobExitCondition, ResourceSpec, JobRecord
├── config.py            # YAML config loader with profiles
├── core.py              # Abstract Executor base class
├── monitor.py           # Async polling loop + callback dispatch
└── executors/
    ├── __init__.py      # Executor registry
    ├── lsf.py           # LSFExecutor (bsub/bjobs/bkill)
    └── local.py         # LocalExecutor (subprocess, for testing)
```
