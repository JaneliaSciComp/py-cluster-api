# py-cluster-api

[![CI](https://github.com/JaneliaSciComp/py-cluster-api/actions/workflows/ci.yml/badge.svg)](https://github.com/JaneliaSciComp/py-cluster-api/actions/workflows/ci.yml)

A Python library for submitting and monitoring jobs on HPC clusters. Supports running arbitrary executables (Nextflow pipelines, Python scripts, Java tools, etc.) on LSF clusters and taking action when jobs complete via async callbacks.

## Features

- **Async-first** — built on `asyncio` for non-blocking job submission and monitoring
- **LSF executor** — submit via `bsub`, monitor via `bjobs -json`, cancel via `bkill`
- **Local executor** — run jobs as local subprocesses for development and testing, including array jobs
- **Job monitoring** — polls the scheduler and fires callbacks on job completion, failure, or cancellation
- **Job arrays** — submit array jobs with per-element log files
- **Zombie detection** — jobs that disappear from the scheduler are marked as failed
- **YAML config with profiles** — Nextflow-style config with per-environment profiles
- **Callback chaining** — register `on_success`, `on_failure`, or `on_exit` handlers on any job

## Installation

Requires Python 3.10+.

```bash
pip install py-cluster-api
```

Or with [Pixi](https://pixi.sh/):

```bash
pixi add --pypi py-cluster-api
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
        resources=ResourceSpec(cpus=4, gpus=1, memory="32 GB", walltime="24:00", queue="long"),
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

The array index environment variable depends on the executor: LSF uses `$LSB_JOBINDEX`, while the local executor uses `$ARRAY_INDEX`.

### Reconnecting After Restart

If your process crashes or restarts, `reconnect()` rediscovers running jobs from the scheduler and resumes tracking them. Requires `job_name_prefix` to be set in config.

```python
async def resume():
    executor = create_executor(profile="janelia_lsf")
    monitor = JobMonitor(executor)
    await monitor.start()

    recovered = await executor.reconnect()
    for job in recovered:
        print(f"Reconnected to {job.job_id} ({job.name}), status={job.status}")
        job.on_exit(lambda j: print(f"Job {j.job_id} finished: {j.status}"))

    if recovered:
        await monitor.wait_for(*recovered)
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
job_name_prefix: "capi"

profiles:
  janelia_lsf:
    executor: lsf
    queue: normal
    gpus: 1
    memory: "8 GB"
    walltime: "04:00"
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
| `gpus` | `None` | Default GPU count |
| `memory` | `None` | Default memory (e.g. `"8 GB"`) |
| `walltime` | `None` | Default wall time (e.g. `"04:00"`) |
| `queue` | `None` | Default queue/partition |
| `poll_interval` | `10.0` | Seconds between status polls |
| `job_name_prefix` | `None` | Optional prefix prepended to job names. When set, polling filters by `{prefix}-*` and `reconnect()` is available; when unset, the user controls the full job name and polling queries all jobs |
| `shebang` | `"#!/bin/bash"` | Script shebang line |
| `script_prologue` | `[]` | Lines inserted before the command |
| `script_epilogue` | `[]` | Lines inserted after the command |
| `extra_directives` | `[]` | Additional scheduler directive lines appended verbatim to the script header (e.g. `"#BSUB -P myproject"`) |
| `directives_skip` | `[]` | Substrings to filter out of directives |
| `extra_args` | `[]` | Extra CLI args appended to the submit command (e.g. `bsub`) |
| `lsf_units` | `"MB"` | LSF memory units (`KB`, `MB`, `GB`) |
| `suppress_job_email` | `true` | Set `LSB_JOB_REPORT_MAIL=N` |
| `command_timeout` | `100.0` | Timeout in seconds for scheduler commands |
| `zombie_timeout_minutes` | `30.0` | Mark jobs as failed if unseen for this long |
| `completed_retention_minutes` | `10.0` | Keep finished jobs in memory for this long |

## API Reference

See [docs/API.md](docs/API.md) for the full API reference and error handling guide.

## Development

See [docs/Development.md](docs/Development.md) for build instructions, testing, and release process.
