# SLURM Executor — Design

**Date:** 2026-10-18 (revised 2026-10-20)
**Status:** Approved

## Motivation

py-cluster-api exists to put one async interface in front of several
schedulers, but only LSF is implemented. This adds a SLURM executor behind the
same `Executor` interface. Every mapping below is taken from an existing
library (submitit, clusterfutures, dask-jobqueue, Nextflow) or the SLURM man
pages; nothing is invented. Sources are cited inline.

Revision history: the 2026-10-20 revision folds in the Nextflow comparison and
the findings of `2026-10-20-slurm-executor-review.md`.

## Decisions

1. **Status source: `sacct`, with `squeue` only for reconnect discovery.**
   sacct (submitit's approach) reports State, ExitCode, NodeList and
   timestamps and keeps finished jobs, so FAILED vs DONE and `exit_code`
   behave as on LSF. It requires slurmdbd accounting, which is standard on
   real clusters and which the CI action provides. History is retained
   subject to the site's `PurgeJobAfter` setting, not forever. `squeue` alone
   was rejected because finished jobs vanish after `MinJobAge` (default
   300 s) and it reports no exit code. `squeue` is still used in
   `reconnect()` because sacct's `--state=PENDING` filter excludes held and
   dependency-waiting jobs (EligibleTime Unknown; sacct man page).
2. **No requeue by default.** Every job is submitted with `--no-requeue`
   (Nextflow emits this unconditionally, arrays included). This makes
   PREEMPTED, NODE_FAIL and BOOT_FAIL terminal from our side, so the
   one-shot lifecycle in `Executor.poll()` is sound. Caveat: site
   configuration can override it. `PrologFlags=ForceRequeueOnFail` and
   `SchedulerParameters=requeue_on_resume_failure` requeue jobs regardless
   of `--no-requeue` (slurm.conf man page). On such a cluster a job we have
   already marked terminal may run again under the same ID without our
   knowledge. This is documented as a known limitation; tracking requeues
   is out of scope.
3. **Testing: mocked unit tests plus a real SLURM in CI.** No public SLURM
   cluster exists. The conda-forge `slurm` package's daemons are broken
   (plugins reference symbols the binaries don't export; verified). The
   proven route is `koesterlab/setup-slurm-action`, which stands up a
   single-node SLURM with slurmdbd on GitHub Actions. The CI job must fail,
   not skip, if SLURM is unavailable.
4. **Completely separate from LSF.** `SlurmExecutor` shares only the
   `Executor` base class, `render_script`, `write_script` and
   `parse_memory_bytes`. No LSF code is touched.

## Module and wiring

- New file `cluster_api/executors/slurm.py` with `SlurmExecutor(Executor)`.
- Registered as `"slurm"` in `cluster_api/executors/__init__.py`.
- Exported as `SlurmExecutor` from `cluster_api/__init__.py`.
- No new `ClusterConfig` fields:
  - `queue` → `--partition`
  - `poll_all_users` → `sacct --allusers`, `squeue -u all`
  - `job_name_prefix` → client-side filter in `reconnect()`
  - `extra_args` → appended to the `sbatch` command line
  - `lsf_units`, `suppress_job_email` → ignored (sbatch sends no mail by default)

Class attributes:

```python
submit_command = "sbatch"
cancel_command = "scancel"
status_command = "sacct"
directive_prefix = "#SBATCH"
job_id_regexp = r"^(?P<job_id>\d+)"   # applied to the last non-empty line
```

## Submission

The script is rendered with `render_script` using `#SBATCH` header lines,
written with `write_script`, and piped to sbatch on stdin, mirroring
`bsub < script` (Nextflow also pipes the script when Fusion is enabled):

```
sbatch --parsable [config.extra_args] [resources.extra_args] [env flags] [dependency flags] < script
```

`--parsable` prints only `JOBID` or `JOBID;CLUSTER`. The job ID is taken from
the last non-empty line of stdout, anchored at line start (Nextflow scans
lines and accepts a bare integer; clusterfutures splits on `;`). A
`;cluster` suffix is ignored. Multi-cluster submission (`-M/--clusters`,
`SLURM_CLUSTERS`) is unsupported and documented as such; the numeric ID is
assumed to be local.

sbatch reads the script from stdin when no filename is given (sbatch man
page); the default job name would then be "sbatch", so `--job-name` is
always emitted.

### Header mapping (`build_header(name, resources, array_spec=None)`)

| Ours | `#SBATCH` directive | Source |
|---|---|---|
| name | `--job-name=NAME` | dask-jobqueue `-J`, submitit, Nextflow |
| stdout | `--output="WORKDIR/stdout.%j.log"` (array: `stdout.%A_%a.log`) | sbatch filename patterns; quoting as Nextflow and our LSF header |
| stderr | `--error="WORKDIR/stderr.%j.log"` (array: `stderr.%A_%a.log`) | same |
| always | `--nodes=1`, `--ntasks=1` | dask-jobqueue emits both unconditionally |
| always | `--no-requeue` | Nextflow; see Decision 2 |
| queue | `--partition=Q` | dask-jobqueue `-p`, Nextflow |
| cpus | `--cpus-per-task=N` | dask-jobqueue, Nextflow `-c` |
| gpus | `--gres=gpu:N` | SLURM GRES docs; works with and without `select/cons_tres`, unlike `--gpus*` |
| memory | `--mem=<ceil MiB>M` | dask-jobqueue `slurm_format_bytes_ceil`, Nextflow `toMega()+"M"` |
| walltime | `--time=STR` passed through unchanged | dask-jobqueue `-t`; docs recommend `HH:MM:SS` (Nextflow's format) |
| work_dir | `--chdir=DIR` | sbatch man page |
| array | `--array=A-B` or `--array=A-B%MAX` | submitit `array_parallelism`, Nextflow `--array` |
| extra_directives | `#SBATCH <raw>` | dask-jobqueue `job_extra_directives` |

Notes:
- `%J` is deliberately avoided for log names: in SLURM it means
  `jobid.stepid`, not job ID.
- `cpus`, `gpus`, `memory`, `walltime`, `queue` fall back to the config value
  when the `ResourceSpec` field is `None`, as in LSF.
- Arrays get a correct header on the first render (`array_spec` argument),
  not a post-hoc rewrite of the script file as in LSF.
- Per-element index inside the job: `$SLURM_ARRAY_TASK_ID`.
- Walltime: on SLURM `01:00` means one minute (`MM:SS`), not one hour.
  README examples and integration tests use `HH:MM:SS`. Users wanting
  `--mem-per-cpu`, `--account`, or multi-node allocations use
  `extra_directives`, optionally with `directives_skip`.

### Environment and login shell

- `inherit_env=True` (default) → nothing added; sbatch's default is
  `--export=ALL`, and `env` is passed to the sbatch subprocess environment.
- `inherit_env=False` → `--export=NONE` on the command line, `env` folded
  into the script prologue as `export K=V` lines (same logic as LSF's
  `_env_prologue`, written separately), and `export SLURM_EXPORT_ENV=ALL`
  added to the prologue so job steps launched by a user's own `srun` inherit
  the environment the script built (sbatch man page recommends exactly
  this). Known approximation: `NONE` still implicitly loads the user's login
  environment on the compute node (sbatch man page). The strict form
  `--export=NIL` is not in the SLURM 22.05 man page and so is not used;
  switching to it is a one-line change once 22.05 clusters are gone. This
  exception is documented in `docs/API.md` under `inherit_env`.
- `login_shell=True` → the script's shebang gets ` -l` appended
  (`#!/bin/bash` → `#!/bin/bash -l`), so the batch script itself runs under
  a login shell, which is what LSF's `-L /bin/bash` does. If the configured
  shebang already contains `-l`, or is not a bash/sh shebang, it is left
  alone and a warning is logged. `--get-user-env` is not used: it only
  fetches variables, it does not run the script under a login shell.

### Dependencies

`depends_on=[A, B]` → `--dependency=afterok:A:B --kill-on-invalid-dep=yes`
on the command line. `afterok` = all listed jobs exited zero;
`--kill-on-invalid-dep=yes` cancels the dependent (state CANCELLED) when a
dependency fails, the exact equivalent of LSF's `-w 'done()' -ti` (sbatch
man page).

## Status polling

Command built by `_build_status_args()`:

```
sacct -n -P -X --array [--allusers] -j id1,id2,... -o JobID,JobName,State,ExitCode,NodeList,Submit,Start,End
```

- `-n` no header, `-P` `|`-delimited without trailing `|`, `-X` allocation
  rows only (no steps), `--array` one row per array task (sacct man page:
  "Display all array tasks on separate lines instead of displaying groups of
  array tasks on single lines"). This removes any need to parse compressed
  expressions like `123_[0-5%4]`, which are also truncated at 64 bytes by
  default (`SLURM_BITSTR_LEN`).
- With `-j`, the time window defaults to epoch 0, so no `-S` is needed.
- The subprocess environment sets `SLURM_TIME_FORMAT=standard` so a user's
  shell setting cannot change the timestamp format (sacct man page).
- Only non-terminal tracked job IDs are queried. When nothing is tracked, the
  base `poll()` returns early.
- Right after submission sacct may not know the job yet; it then returns no
  row and the record stays PENDING until the next poll.

`_parse_job_statuses(output)`:

1. Split lines on `|`; skip rows whose JobID contains `.` (steps).
2. JobID `123` → `"123"`; `123_4` → `"123[4]"` so the base `poll()` routes
   it to the parent's `array_elements`. Any row whose JobID still contains
   `[` (compressed form, which `--array` should prevent) is logged and
   skipped rather than guessed at.
3. State: first whitespace token (`CANCELLED by 1234` → `CANCELLED`).
4. ExitCode `rc:sig` → `exit_code = int(rc)` if `sig == 0`, else
   `-int(sig)`, matching the negative return codes the local executor gets
   from `subprocess` for signal-terminated processes. DONE with empty code
   → 0.
5. NodeList → `exec_host`; `None assigned` → `None`.
6. Submit/Start/End: `YYYY-MM-DDTHH:MM:SS` local time → UTC; `Unknown`,
   `None`, empty → `None`.
7. `job_name` kept in meta for `reconnect()`.
8. `max_mem` is never populated: sacct reports MaxRSS only on step rows,
   which `-X` drops.

State map (SLURM job_state_codes page, squeue/sacct man pages; Nextflow's
`STATUS_MAP` agrees on every state it covers):

| SLURM state | JobStatus |
|---|---|
| PENDING, CONFIGURING, REQUEUED, REQUEUE_HOLD, REQUEUE_FED, RESV_DEL_HOLD, SPECIAL_EXIT | PENDING |
| RUNNING, COMPLETING, SUSPENDED, STOPPED, SIGNALING, STAGE_OUT, RESIZING | RUNNING |
| COMPLETED | DONE |
| FAILED, BOOT_FAIL, NODE_FAIL, OUT_OF_MEMORY, TIMEOUT, DEADLINE, REVOKED | FAILED |
| CANCELLED, PREEMPTED | KILLED |
| anything else | UNKNOWN (logged) |

PREEMPTED is terminal because of `--no-requeue` (Decision 2). Nextflow maps
it to ERROR, also terminal.

## Cancellation

- `cancel(job_id)` → `scancel JOB_ID`. For an array parent this cancels every
  element (scancel man page). `done=True` has no SLURM equivalent (no
  `bkill -d`); the record is still marked DONE in-process by the base class.
- `cancel_by_name(name)` → `scancel --name=NAME`. Exact match only; scancel
  documents no wildcard. Tracked records whose `name` equals `NAME` are
  marked KILLED. A `CommandFailedError` for no matching jobs is swallowed.

## Reconnect

Two steps, reusing the sacct parser for everything that becomes a record:

1. **Discover live jobs** with squeue, which lists every pending and
   running job regardless of eligibility (held, dependency-waiting,
   configuring):

   ```
   squeue -h -r -o '%F|%j' [-u all]
   ```

   `-r` prints one line per array element so no compressed expressions
   appear; `%F` is the array parent ID (or the job ID for non-arrays).
   Lines whose name starts with `"<prefix>-"` are kept and their IDs
   de-duplicated. Without `-u all`, squeue shows the invoking user's jobs,
   matching `poll_all_users=False`.
2. **Fetch full rows** for those IDs with the ordinary polling query
   (`sacct ... --array -j <ids>`). Because this query is unfiltered by
   state, finished elements of a partially complete array come back too, so
   `array_range` is reconstructed from the full extent and
   `completed_elements` / `failed_element_indices` are correct from the
   first poll.

Rows are then grouped into singles and arrays and turned into `JobRecord`s
exactly as LSF's `reconnect()` does (terminal singles skipped, arrays whose
elements are all terminal skipped, `compute_array_status()` applied).
Raises `ClusterAPIError` when no `job_name_prefix` is configured. Returns
`[]` when squeue finds nothing.

## Tests

`tests/test_slurm.py`, mirroring `test_lsf.py`, all with `_call` mocked:

- Header rendering: every row of the mapping table including `--nodes=1`,
  `--ntasks=1`, `--no-requeue`, `--gres=gpu:N`, quoted log paths, config
  fallbacks, array header (`--array`, `%A_%a` log names, throttle), memory
  ceil to MiB, extra directives.
- Submit: `sbatch --parsable` with stdin script, `extra_args` ordering,
  `--export=NONE` + prologue exports + `SLURM_EXPORT_ENV=ALL`, login-shell
  shebang rewrite (bash, already `-l`, non-bash warning),
  `--dependency=afterok:…` + `--kill-on-invalid-dep=yes`, job ID parsing of
  `123`, `123;cluster`, and a leading warning line.
- sacct parsing on literal sample output: single job RUNNING/COMPLETED/
  FAILED/`CANCELLED by 1234`/TIMEOUT/OUT_OF_MEMORY/PREEMPTED; exit codes
  `1:0` → 1, `0:9` → -9, `0:0` → 0, asserted on the resulting `JobRecord`
  and `ArrayElement`; array rows `123_0`…`123_4`; a compressed `123_[3-5]`
  row logged and skipped; step rows skipped; `None assigned`; `Unknown`
  times; empty output.
- State map: every listed SLURM state maps as in the table; unknown state →
  UNKNOWN with a warning.
- Status args: `-j` list of only non-terminal IDs, `--array` present,
  `--allusers` when `poll_all_users`, `SLURM_TIME_FORMAT=standard` in the
  call environment.
- Cancel: `scancel ID`; `cancel_by_name` exact name, no-match swallowed.
- Reconnect: squeue discovery args, prefix filtering, de-duplication of
  array parents, sacct fetch of discovered IDs, partially completed array
  reconstructed with full extent and completed/failed counts, held and
  dependency-waiting pending jobs discovered, error when no prefix, empty
  squeue → `[]`.
- Registry: `get_executor_class("slurm")`.

`tests/test_slurm_integration.py`, mirroring `test_integration.py`, marked
`integration`. Skipped at module level unless `sbatch` is on PATH, except
when `CLUSTER_API_REQUIRE_SLURM=1` is set, in which case a missing `sbatch`
or a failing `sacct` smoke query fails the module instead. Cases: submit
and wait, failing job → FAILED with exit code, job killed by signal →
negative exit code, array job with element statuses, cancel, `depends_on`
chain with upstream failure cancelling the dependent, reconnect by prefix
including a held job (`scontrol hold`) and a partially completed array,
`inherit_env=False` and `login_shell=True` round trips checking the job's
environment. Walltimes use `HH:MM:SS`.

## CI

`.github/workflows/ci.yml` gains a second job, `slurm-integration`, on
`ubuntu-latest`:

1. `actions/checkout`, `prefix-dev/setup-pixi` (same as the existing job).
2. `koesterlab/setup-slurm-action@v1` with its required `mysql:8.0` service
   on port 8888 (per that action's README), giving a single-node SLURM with
   slurmdbd accounting so `sacct` works.
3. `CLUSTER_API_REQUIRE_SLURM=1 pixi run test-integration`. The LSF
   integration module self-skips (no `bsub`); the SLURM one must run, and
   the job fails if it cannot.

## Docs and version

- README: list SLURM as supported, add `executor: slurm` profile example
  with `walltime: "01:00:00"`, note `$SLURM_ARRAY_TASK_ID`, and a "SLURM
  notes" list: accounting (slurmdbd) required and history subject to
  `PurgeJobAfter`; `--no-requeue` default and the site-config override
  caveat; `max_mem` not reported; `cancel_by_name` exact match;
  `--export=NONE` approximation; `login_shell` via `bash -l` shebang;
  multi-cluster unsupported; negative `exit_code` means signal.
- `docs/API.md`: `cancel_by_name` note updated from "LSF only" to "LSF
  (glob) and SLURM (exact name)"; `inherit_env` and `login_shell` gain the
  SLURM exceptions above; `exit_code` documents the signal convention.
- `docs/Development.md`: tests table gains the two SLURM modules, the
  `CLUSTER_API_REQUIRE_SLURM` switch and the CI job.
- `CLAUDE.md`: add `executors/slurm.py` to project structure and SLURM log
  naming (`stdout.%j.log`, arrays `stdout.%A_%a.log`).
- Version bump to 0.9.0.

## Out of scope

- squeue-based polling or a squeue→sacct fallback for clusters without
  accounting.
- Tracking requeues forced by site configuration despite `--no-requeue`.
- Multi-cluster (`-M`, `SLURM_CLUSTERS`) routing of poll and cancel.
- Glob matching in `cancel_by_name`.
- `max_mem` from step-level MaxRSS.
- `--mem-per-cpu`, `--account`, `--signal`, `--open-mode`, srun wrapping:
  reachable through `extra_directives`, not modelled.
