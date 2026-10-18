# SLURM Executor — Design

**Date:** 2026-10-18
**Status:** Approved

## Motivation

py-cluster-api exists to put one async interface in front of several
schedulers, but only LSF is implemented. This adds a SLURM executor behind the
same `Executor` interface. Every mapping below is taken from an existing
library (submitit, clusterfutures, dask-jobqueue) or the SLURM man pages;
nothing is invented. Sources are cited inline.

## Decisions

1. **Status source: `sacct` only.** submitit's approach. One command keeps
   history forever and reports State, ExitCode, NodeList and timestamps, so
   FAILED vs DONE and `exit_code` behave as on LSF. Requires the cluster to run
   slurmdbd accounting, which is standard on real clusters and which the CI
   action provides. `squeue` was rejected because finished jobs vanish after
   `MinJobAge` (default 300 s) and it reports no exit code; a squeue→sacct
   fallback was deferred until a cluster without accounting shows up.
2. **Testing: mocked unit tests plus a real SLURM in CI.** No public SLURM
   cluster exists. The conda-forge `slurm` package's daemons are broken
   (plugins reference symbols the binaries don't export; verified). The
   proven route is `koesterlab/setup-slurm-action`, which stands up a
   single-node SLURM with slurmdbd on GitHub Actions.
3. **Completely separate from LSF.** `SlurmExecutor` shares only the
   `Executor` base class, `render_script`, `write_script` and
   `parse_memory_bytes`. No LSF code is touched.

## Module and wiring

- New file `cluster_api/executors/slurm.py` with `SlurmExecutor(Executor)`.
- Registered as `"slurm"` in `cluster_api/executors/__init__.py`.
- Exported as `SlurmExecutor` from `cluster_api/__init__.py`.
- No new `ClusterConfig` fields:
  - `queue` → `--partition`
  - `poll_all_users` → `sacct --allusers`
  - `job_name_prefix` → client-side filter in `reconnect()`
  - `extra_args` → appended to the `sbatch` command line
  - `lsf_units`, `suppress_job_email` → ignored (sbatch sends no mail by default)

Class attributes:

```python
submit_command = "sbatch"
cancel_command = "scancel"
status_command = "sacct"
directive_prefix = "#SBATCH"
job_id_regexp = r"(?P<job_id>\d+)"   # --parsable prints "123" or "123;cluster"
```

## Submission

The script is rendered with `render_script` using `#SBATCH` header lines,
written with `write_script`, and piped to sbatch on stdin, mirroring
`bsub < script`:

```
sbatch --parsable [config.extra_args] [resources.extra_args] [env flags] [dependency flags] < script
```

`--parsable` outputs only the job ID (and `;cluster` if present); the base
`_job_id_from_submit_output` regex extracts it (clusterfutures: `sbatch
--parsable`, split on `;`). sbatch reads the script from stdin when no
filename is given (sbatch man page); the default job name would then be
"sbatch", so `--job-name` is always emitted.

### Header mapping (`build_header(name, resources, array_spec=None)`)

| Ours | `#SBATCH` directive | Source |
|---|---|---|
| name | `--job-name=NAME` | dask-jobqueue `-J`, submitit |
| stdout | `--output=WORKDIR/stdout.%j.log` (array: `stdout.%A_%a.log`) | sbatch filename patterns |
| stderr | `--error=WORKDIR/stderr.%j.log` (array: `stderr.%A_%a.log`) | sbatch filename patterns |
| queue | `--partition=Q` | dask-jobqueue `-p` |
| cpus | `--cpus-per-task=N` | dask-jobqueue |
| gpus | `--gpus-per-node=N` | submitit |
| memory | `--mem=<ceil MiB>M` | dask-jobqueue `slurm_format_bytes_ceil` (binary units, no "B") |
| walltime | `--time=STR` passed through unchanged | dask-jobqueue `-t` |
| work_dir | `--chdir=DIR` | sbatch man page |
| array | `--array=A-B` or `--array=A-B%MAX` | submitit `array_parallelism` |
| extra_directives | `#SBATCH <raw>` | dask-jobqueue `job_extra_directives` |

Notes:
- `--ntasks`/`--nodes` are not emitted; sbatch defaults to one task on one
  node.
- `%J` is deliberately avoided for log names: in SLURM it means
  `jobid.stepid`, not job ID.
- `cpus`, `gpus`, `memory`, `walltime`, `queue` fall back to the config value
  when the `ResourceSpec` field is `None`, as in LSF.
- Arrays get a correct header on the first render (`array_spec` argument),
  not a post-hoc rewrite of the script file as in LSF.
- Per-element index inside the job: `$SLURM_ARRAY_TASK_ID`.

### Environment and dependencies (CLI flags on sbatch)

- `inherit_env=False` → `--export=NONE`, and `env` is folded into the script
  prologue as `export K=V` lines (same `_env_prologue` logic as LSF, written
  separately). Note from the man page: `NONE` still implicitly loads the
  user's login environment on the node, so "minimal" is less strict than
  LSF's `-env none`; `NIL` would be strict but is newer and the LSF semantics
  ("only env plus scheduler variables") are approximated, not matched.
- `inherit_env=True` (default) → nothing added; sbatch's default is
  `--export=ALL`, and `env` is passed to the sbatch subprocess environment.
- `login_shell=True` → `--get-user-env`. sbatch has no login-shell flag
  (`-L` is `--licenses`); `--get-user-env` is the documented mechanism.
- `depends_on=[A, B]` → `--dependency=afterok:A:B --kill-on-invalid-dep=yes`.
  `afterok` = all listed jobs exited zero; `--kill-on-invalid-dep=yes` cancels
  the dependent when a dependency fails (state CANCELLED), the exact
  equivalent of LSF's `-w 'done()' -ti`. (sbatch man page.)

## Status polling

Command built by `_build_status_args()`:

```
sacct -n -P -X [--allusers] -j id1,id2,... -o JobID,JobName,State,ExitCode,NodeList,Submit,Start,End
```

- `-n` no header, `-P` `|`-delimited without trailing `|`, `-X` allocation
  rows only (no steps). With `-j`, the time window defaults to epoch 0, so no
  `-S` is needed (sacct man page).
- Only non-terminal tracked job IDs are queried. When nothing is tracked, the
  base `poll()` returns early; unlike LSF there is no prefix-filtered query,
  because sacct's `--name` is exact-match only.
- Right after submission sacct may not know the job yet; it then simply
  returns no row and the record stays PENDING until the next poll.

`_parse_job_statuses(output)`, per submitit's `SlurmInfoWatcher.read_info`:

1. Split lines on `|`; skip rows whose JobID contains `.` (steps).
2. JobID forms (sacct man page): `123` → `"123"`; `123_4` →
   `"123[4]"` so the base `poll()` routes it to the parent's
   `array_elements`; `123_[0-5%4]` (pending, unexpanded) → expanded to
   `123[0]`…`123[5]` with submitit's regex
   `(?P<main_id>\d+)_\[(?P<arrays>(\d+(-\d+)?(,)?)+)(\%\d+)?\]`.
3. State: first whitespace token (`CANCELLED by 1234` → `CANCELLED`).
4. ExitCode `rc:sig` → `exit_code = int(rc)`. DONE with empty code → 0.
5. NodeList → `exec_host`; `None assigned` → `None`.
6. Submit/Start/End: `YYYY-MM-DDTHH:MM:SS` local time → UTC; `Unknown`,
   `None`, empty → `None`.
7. `job_name` kept in meta for `reconnect()`.
8. `max_mem` is never populated: sacct reports MaxRSS only on step rows,
   which `-X` drops.

State map (SLURM job_state_codes page, squeue/sacct man pages):

| SLURM state | JobStatus |
|---|---|
| PENDING, CONFIGURING, REQUEUED, REQUEUE_HOLD, REQUEUE_FED, RESV_DEL_HOLD, SPECIAL_EXIT | PENDING |
| RUNNING, COMPLETING, SUSPENDED, STOPPED, SIGNALING, STAGE_OUT, RESIZING | RUNNING |
| COMPLETED | DONE |
| FAILED, BOOT_FAIL, NODE_FAIL, OUT_OF_MEMORY, TIMEOUT, DEADLINE, REVOKED | FAILED |
| CANCELLED, PREEMPTED | KILLED |
| anything else | UNKNOWN (logged) |

PREEMPTED is treated as terminal KILLED. submitit treats it as still running
(it may be requeued), clusterfutures as finished; a never-requeued preempted
job would otherwise pend forever in our records, so terminal was chosen.

## Cancellation

- `cancel(job_id)` → `scancel JOB_ID`. For an array parent this cancels every
  element (scancel man page). `done=True` has no SLURM equivalent (no
  `bkill -d`); the record is still marked DONE in-process by the base class.
- `cancel_by_name(name)` → `scancel --name=NAME`. Exact match only; scancel
  documents no wildcard. Tracked records whose `name` equals `NAME` are
  marked KILLED. A `CommandFailedError` for no matching jobs is swallowed.

## Reconnect

Same query and parser as polling, with a state filter instead of job IDs:

```
sacct -n -P -X [--allusers] -s PD,R,CG,S,RQ -o <same fields>
```

Per the sacct man page, with `--state` and without `--jobs` the window
defaults to "now", returning jobs currently in those states. Rows are kept
when `job_name` starts with `"<prefix>-"`, then grouped into singles and
arrays and turned into `JobRecord`s exactly as LSF's `reconnect()` does
(terminal entries skipped, arrays get `array_range` from min/max index and
`compute_array_status()`). Raises `ClusterAPIError` when no
`job_name_prefix` is configured.

## Tests

`tests/test_slurm.py`, mirroring `test_lsf.py`, all with `_call` mocked:

- Header rendering: every row of the mapping table, config fallbacks,
  array header (`--array`, `%A_%a` log names, throttle), memory ceil to MiB,
  extra directives.
- Submit: `sbatch --parsable` with stdin script, `extra_args` ordering,
  `--export=NONE` + prologue exports, `--get-user-env`,
  `--dependency=afterok:…` + `--kill-on-invalid-dep=yes`, job ID parsing of
  `123` and `123;cluster`.
- sacct parsing on literal sample output: single job RUNNING/COMPLETED/
  FAILED/CANCELLED by uid/TIMEOUT/OUT_OF_MEMORY, exit code `1:0` and `0:9`,
  array rows `123_0` … plus `123_[3-5%2]` expansion, step rows skipped,
  `None assigned`, `Unknown` times, empty output.
- State map: every listed SLURM state maps as in the table; unknown state →
  UNKNOWN with a warning.
- Status args: `-j` list of only non-terminal IDs, `--allusers` when
  `poll_all_users`.
- Cancel: `scancel ID`; `cancel_by_name` exact name, no-match swallowed.
- Reconnect: `-s` filter, prefix filtering, singles and arrays, error when no
  prefix.
- Registry: `get_executor_class("slurm")`.

`tests/test_slurm_integration.py`, mirroring `test_integration.py`, marked
`integration`, skipped at module level unless `sbatch` is on PATH: submit and
wait, failing job → FAILED with exit code, array job with element statuses,
cancel, `depends_on` chain with upstream failure cancelling the dependent,
reconnect by prefix.

## CI

`.github/workflows/ci.yml` gains a second job, `slurm-integration`, on
`ubuntu-latest`:

1. `actions/checkout`, `prefix-dev/setup-pixi` (same as the existing job).
2. `koesterlab/setup-slurm-action@v1` with its required `mysql:8.0` service
   on port 8888 (per that action's README), giving a single-node SLURM with
   slurmdbd accounting so `sacct` works.
3. `pixi run test-integration`. The LSF integration module self-skips (no
   `bsub`), the SLURM one runs.

## Docs and version

- README: list SLURM as supported, add `executor: slurm` profile example,
  note `$SLURM_ARRAY_TASK_ID`, and a short "SLURM notes" list: accounting
  (slurmdbd) required, `max_mem` not reported, `cancel_by_name` exact match,
  `--export=NONE` semantics, `login_shell` → `--get-user-env`.
- `docs/API.md`: `cancel_by_name` note updated from "LSF only" to "LSF
  (glob) and SLURM (exact name)".
- `docs/Development.md`: tests table gains the two SLURM modules and the CI
  job.
- `CLAUDE.md`: add `executors/slurm.py` to project structure and SLURM log
  naming (`stdout.%j.log`, arrays `stdout.%A_%a.log`).
- Version bump to 0.9.0.

## Out of scope

- squeue polling or squeue→sacct fallback for clusters without accounting.
- Glob matching in `cancel_by_name` (would need a squeue listing plus
  client-side filtering).
- `max_mem` from step-level MaxRSS.
- `--signal`/preemption handling, `--open-mode`, srun wrapping (submitit
  features not present in the LSF executor either).
