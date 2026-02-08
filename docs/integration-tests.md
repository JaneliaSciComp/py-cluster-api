# Integration Tests

Integration tests submit real jobs to the cluster. They are skipped by default
and require a working LSF installation (`bsub` on PATH).

## Setup

Copy the example config and edit it for your cluster:

```bash
cp tests/cluster_config.example.yaml tests/cluster_config.yaml
```

Edit `tests/cluster_config.yaml` with your cluster-specific settings:

```yaml
queue: normal
memory: "1 GB"
use_stdin: true
lsf_units: MB
suppress_job_email: true

# Shell commands to run before each job's command
script_prologue:
  - "module load java/11"

# Extra #BSUB directives added to every job script.
# Each entry must include the full "#BSUB" prefix.
extra_directives:
  - "#BSUB -env 'all'"
```

This file is gitignored so each developer can configure their own environment
without affecting others.

Any option from `ClusterConfig` is valid here (see `cluster_api/config.py`).
The integration test fixture always overrides `executor`, `log_directory`,
`poll_interval`, and `walltime` regardless of what the config file says, so
you don't need to set those.

You can also filter out directives that the library generates automatically
using `directives_skip` — any directive line containing a listed substring
will be removed:

```yaml
directives_skip:
  - "-M"       # drop memory limit directive
  - "rusage"   # drop rusage resource request
```

If no config file is present, the tests will still run using built-in defaults.

## Running

```bash
pixi run test-integration
```

Tests print job IDs, statuses, and metadata as they go (`-v -s` flags), so you
can follow along with `bjobs` in another terminal.

Typical run time is 2-3 minutes depending on cluster queue wait times.
