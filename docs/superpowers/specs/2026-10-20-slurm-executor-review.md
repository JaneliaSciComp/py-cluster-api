# SLURM Executor Design Review

**Date:** 2026-10-20  
**Spec:** [SLURM Executor Design](2026-10-18-slurm-executor-design.md)  
**Recommendation:** Revise before implementation.

The basic submission mappings are sound, but the proposed lifecycle handling,
reconnection, parsing, and environment behavior need changes to provide correct
Slurm support through this repository's existing executor interface.

P1 findings can lose active jobs or report completion prematurely. P2 findings
affect correctness under particular inputs or cluster configurations. Line
references below refer to the reviewed design spec.

## Required revisions

### Preserve monitoring across preemption

**Priority:** P1  
**Spec:** Lines 156–161, state mapping and preemption policy.

Slurm can requeue preempted jobs rather than ending their lifecycle permanently.
[Slurm preemption documentation](https://slurm.schedmd.com/preempt.html)

Mapping `PREEMPTED` immediately to terminal `KILLED` makes
[`Executor.poll()`](../../../cluster_api/core.py#L305) stop querying the job and
allows completion callbacks to fire prematurely. A targeted reproduction with
the base executor confirmed that a second poll does not query the scheduler
after this terminal transition.

Before finalizing a preempted job, determine whether it remains active or has
been requeued. Alternatively, define and enforce a no-requeue policy for the
initial implementation. Deferring checkpoint support does not make an
unconditional terminal mapping safe. Simply treating every preemption as
nonterminal is also insufficient because some preempted jobs never restart.

### Discover all active jobs and restore complete arrays

**Priority:** P1  
**Spec:** Lines 174–185, reconnect query and reconstruction.

`sacct --state=PENDING` excludes jobs with unknown eligibility, including held
jobs. The proposed state list also omits several states the spec itself maps to
nonterminal statuses. [sacct selection rules](https://slurm.schedmd.com/sacct.html)

Deriving an array's range from only its active elements loses its original
extent and completed-element history. Unlike the proposed state-filtered
query, LSF reconnect requests historical rows too. A later poll can add missing
elements, but the reconstructed `array_range` remains incorrect unless repaired.

Use live-job discovery, preferably `squeue`, then query discovered array parents
through unfiltered `sacct -j` before constructing records. Recover completed and
failed elements as well as active ones. This requires revisiting the sacct-only
decision; accounting can still supply historical results and exit metadata.

### Avoid incomplete compressed array parsing

**Priority:** P2  
**Spec:** Lines 133–138, array JobID parsing.

The copied regex cannot parse stepped ranges such as `123_[0-10:2]`. Slurm also
limits compressed array task expressions to 64 bytes by default, so long
expressions can be incomplete before the parser sees them.
[Array syntax](https://slurm.schedmd.com/job_array.html),
[sacct array output](https://slurm.schedmd.com/sacct.html)

Prefer `sacct --array` to obtain individual task rows. If compressed output is
retained, require complete range parsing and `SLURM_BITSTR_LEN=0`; reject or
diagnose incomplete expressions rather than silently omitting tasks. Test
fragmented arrays, stepped ranges, throttling, and expressions longer than the
default limit.

### Preserve signal termination information

**Priority:** P2  
**Spec:** Line 140, exit code conversion.

In Slurm's `ExitCode` field, the value after the colon identifies a terminating
signal. Parsing `0:9` as `exit_code=0` discards SIGKILL as the termination cause.
[Slurm exit codes](https://slurm.schedmd.com/job_exit_code.html)

The status would still indicate failure or cancellation, but consumers would
receive a misleading zero exit code. Define a signal-aware representation, such
as `-sig` when a signal is present and `rc` otherwise, consistent with the local
executor's subprocess return codes. Alternatively, preserve the signal in an
explicit field and document how callers should interpret `exit_code`.

Test the resulting `JobRecord` and `ArrayElement` values for both `1:0` and
`0:9`, not just whether those strings parse.

### Align environment controls with the API contract

**Priority:** P2  
**Spec:** Lines 99–108, environment and login-shell options.

`--get-user-env` retrieves environment variables; it does not run the batch
script under a login shell. `--export=NONE` implicitly retrieves the user's
environment, unlike `NIL`. When commands invoke `srun`, export restrictions can
also propagate to job steps.
[sbatch environment semantics](https://slurm.schedmd.com/sbatch.html)

These choices do not satisfy the existing
[`inherit_env` and `login_shell` contracts](../../API.md#executor).

Implement actual login-shell execution when requested and choose a supported
Slurm version policy for strict environment isolation with `NIL`. If these
behaviors remain approximations, explicitly document the exceptions in the API
reference as well as the Slurm notes. Define how custom shebangs interact with
login-shell execution.

After constructing the intended job environment, address step inheritance with
`SLURM_EXPORT_ENV=ALL` where appropriate. Test all four combinations of
`inherit_env` and `login_shell`, explicit environment overrides, and environment
visibility inside a user-invoked `srun` command.

### Retain cluster identity or reject remote submission

**Priority:** P2  
**Spec:** Lines 64–66, parsing `sbatch --parsable` output.

If `extra_args` selects another cluster, discarding `;cluster` leaves subsequent
polling and cancellation without the routing information. Independent clusters
can reuse numeric job IDs, so this can query or cancel an unrelated local job.
Slurm provides cluster selection for both operations.
[sacct cluster selection](https://slurm.schedmd.com/sacct.html),
[scancel cluster selection](https://slurm.schedmd.com/scancel.html)

Preserve cluster identity throughout submission, tracking, polling,
cancellation, and reconnection. If cross-cluster support is out of scope, reject
remote-cluster requests before submission, including applicable environment
settings. Accepting the syntax `123;cluster` alone does not establish functional
cross-cluster support.

## Additional implementation requirements

- **Explicit allocation size:** Emit `--nodes=1 --ntasks=1` for the default
  single-host execution model. Allocation defaults depend on partition settings
  and other options. Define how deliberate resource overrides interact with
  these defaults. [sbatch allocation options](https://slurm.schedmd.com/sbatch.html)
- **GPU compatibility:** Document that `--gpus-per-node` requires
  `select/cons_tres`, or consider `--gres=gpu:N` for broader compatibility.
  [Slurm GRES](https://slurm.schedmd.com/gres.html)
- **Deterministic timestamps:** Override `SLURM_TIME_FORMAT` for status commands
  so user preferences cannot invalidate the parser. Define timezone handling
  consistently for polling and reconnection.
  [sacct environment variables](https://slurm.schedmd.com/sacct.html)
- **Accounting retention:** Replace the claim that accounting keeps history
  forever with retention-dependent history. `PurgeJobAfter` can remove records.
  [Slurm accounting retention](https://slurm.schedmd.com/slurmdbd.conf.html)
- **Unambiguous walltimes:** Use `HH:MM:SS` in Slurm examples and integration
  tests. `01:00` means one minute, not one hour; do not copy LSF examples without
  checking their meaning. Scheduler-native pass-through can remain the API
  policy. [sbatch time formats](https://slurm.schedmd.com/sbatch.html)
- **Mandatory CI execution:** Keep optional local integration-test skips, but
  make the Slurm CI job fail if its scheduler or accounting setup is unavailable
  or if the Slurm integration tests do not execute.

## Acceptance tests before approval

- [ ] Preemption followed by requeue and successful completion remains tracked;
      callbacks fire only after final completion.
- [ ] Preemption without requeue reaches a terminal state rather than waiting
      indefinitely.
- [ ] Reconnect discovers held jobs, dependency-waiting jobs, and jobs in every
      supported nonterminal state.
- [ ] Reconnect restores partially completed arrays, including original extent,
      prior failures, and completed-element counts.
- [ ] Array parsing handles fragmented and stepped task sets without truncation
      or silent loss.
- [ ] Ordinary failures and signal terminations preserve meaningful exit
      information for single jobs and array elements.
- [ ] Environment and login-shell behavior is exercised on real Slurm, including
      explicit overrides and a user-invoked `srun` command.
- [ ] Cluster-qualified submission either routes subsequent operations correctly
      or is rejected before a job is submitted.
- [ ] Timestamp parsing remains correct with conflicting user format settings.
- [ ] CI verifies working submission and accounting and cannot pass by skipping
      all Slurm integration tests.

## Validation scope

The review compared the spec with the existing executor, job record, script,
monitor, and test code and with official Slurm documentation. Targeted local
reproductions checked the preemption terminal-state behavior and the proposed
array regex. Live Slurm integration remains required; this review is not a
runtime certification. The original design spec is unchanged.
