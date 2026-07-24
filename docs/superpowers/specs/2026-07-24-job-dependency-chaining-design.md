# Job Dependency Chaining — Design

**Date:** 2026-07-24
**Status:** Approved

## Motivation

A survey of LSF submission code across JaneliaSciComp and janelia-cellmap
(see `docs/lsf-implementations-comparison.html`) found job dependency
chaining to be one of the few features other implementations have that
py-cluster-api lacks. Tensorswitch chains pipeline steps (convert image s0
→ build pyramid → convert labels s0 → build pyramid) with a hand-rolled
coordinator script of sequential `bsub -K`/`bwait` calls; twocof used raw
`-w 'ended(...)'` conditions. This feature lets tensorswitch (and similar
pipelines) express those chains through py-cluster-api instead.

## Decisions

Settled during design discussion:

1. **Failure mode: auto-cancel dependents.** When an upstream job fails or
   is killed, downstream jobs are cancelled rather than pending forever
   (LSF's native `-w 'done(X)'` footgun).
2. **Granularity: whole-job only.** A job starts when its dependency jobs
   have fully succeeded (all array elements terminal). Element-wise array
   chaining (`done(A[*])`) is out of scope and addable later.
3. **Mechanism: scheduler-native `bsub -w`.** The whole chain is submitted
   to LSF upfront and survives client restarts; `reconnect()` picks it back
   up. Client-side chaining (monitor submits the next stage) was rejected:
   the chain would die with the client and pending stages would be
   invisible to `bjobs`.

## API

New optional keyword on `Executor.submit()` and `Executor.submit_array()`:

```python
depends_on: Sequence[JobRecord | str] | None = None
```

- Accepts `JobRecord` instances or raw job-ID strings (for jobs adopted
  via `track()` / `reconnect()`).
- Semantics: the job starts only after **all** listed jobs finish
  successfully. If any dependency fails or is killed, the job is
  cancelled instead of left pending.
- Fan-in falls out of the list form. Linear chains are:

```python
a = await ex.submit(convert_cmd, "convert-s0")
b = await ex.submit(pyramid_cmd, "pyramid", depends_on=[a])
```

`JobRecord` gains a field:

```python
depends_on: list[str] = field(default_factory=list)
```

populated with the resolved dependency job IDs at submit time, so chains
are introspectable and the local executor can act on them.

### Internal plumbing

`Executor._submit_job()` and `Executor._submit_array_job()` gain a
keyword-only `depends_on: list[str] | None = None` parameter. `submit()` /
`submit_array()` resolve `JobRecord`s to their IDs before passing down.

## LSF executor

When `depends_on` is set, append CLI args at submit time (same pattern as
`_env_control_args`, so the array `#BSUB` script-rewrite logic is
untouched):

```
-w "done(123) && done(456)" -ti
```

- `done(id)` = dependency completed successfully — matches after-success
  semantics. Waiting on an array-job ID waits for all elements.
- `-ti` (LSF 10.1+ per-job orphan termination): LSF itself kills the
  dependent the moment its dependency becomes unsatisfiable (dependency
  failed, was killed, or was cancelled). Auto-cancel therefore works
  scheduler-side even when no Python process is alive. The terminated job
  reports `EXIT` in `bjobs` → `FAILED` in `poll()`, with no special
  parsing needed.
- If a dependency job has already aged out of LSF's history, `bsub` fails
  and the error surfaces as `SubmitError`. Documented, not swallowed.

## Local executor

`LocalExecutor._submit_job()` currently spawns the subprocess immediately,
so dependent jobs need deferred start:

- With `depends_on`, do not spawn. Create the record with a synthetic
  non-numeric ID (`waiting-N`, same convention as `array-N`) and leave it
  `PENDING`.
- `poll()` (already fully overridden in `LocalExecutor`) gains a step: for
  each waiting job —
  - all deps `DONE` → spawn now, reusing the existing spawn path;
  - any dep `FAILED`/`KILLED` → mark the job `KILLED` and set
    `metadata["dependency_failed"] = [failed dep ids]`.
- `_last_seen` is refreshed on each poll while waiting, so zombie
  detection does not reap parked jobs.
- Documented trade-offs: dependent local jobs keep their synthetic ID, so
  the PID-based stateless-cancel path does not apply to them (same as
  local array jobs today). Dependencies must be tracked by the same
  executor instance.

Note: the spawned job keeps the `waiting-N` ID even after its process
starts (job IDs are dict keys and are returned to the caller; they never
change). The PID is tracked internally for process management.

## Shared failure annotation

Base `Executor.poll()` addition: when a record with `depends_on` becomes
terminal-failed while one of its tracked dependencies is
`FAILED`/`KILLED`, set `metadata["dependency_failed"]` to the culprit IDs.
`on_failure` callbacks can then distinguish "my job broke" from "upstream
broke."

## Backward compatibility

- New kwarg with `None` default on `submit()`/`submit_array()` — additive.
- New `JobRecord` field with a default — additive.
- `_submit_job`/`_submit_array_job` signature change is underscore-internal;
  both in-repo executors are updated in the same change.
- No existing behavior changes when `depends_on` is not passed.

## Testing

- **`test_lsf.py`** (mocked `_call`): exact `-w`/`-ti` args for a single
  dependency, fan-in, and mixed `JobRecord`+str deps; no dependency args
  when `depends_on` is absent; `SubmitError` on bsub failure.
- **`test_local.py`** (real subprocesses): chain where B reads a file A
  writes — B stays `PENDING` until A is `DONE`, then runs to `DONE`;
  failure case — A exits 1, B ends `KILLED` with `dependency_failed`
  metadata; `Monitor.wait_for(chain_tail)` resolves the whole chain.
- **`test_integration.py`** (live LSF, skipped by default): one chain test,
  which also verifies `-ti` behavior on the Janelia cluster — the one
  assumption in this design worth checking against real LSF.

## Out of scope (addable later without breaking changes)

- Element-wise array dependencies (`done(A[*])`).
- `ended()` / after-failure / run-always dependency conditions.
- A pipeline/DAG builder abstraction.
- Recovering `depends_on` in `reconnect()` — LSF owns the chain;
  reconnected dependents carry on without client-side dependency info.
