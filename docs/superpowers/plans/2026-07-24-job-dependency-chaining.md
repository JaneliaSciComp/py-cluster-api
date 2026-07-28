# Job Dependency Chaining Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `depends_on` option to `submit()`/`submit_array()` so a job starts only after all its dependency jobs succeed, and is auto-cancelled if any dependency fails.

**Architecture:** Scheduler-native on LSF (`bsub -w "done(a) && done(b)" -ti`, so chains survive client restarts and LSF itself terminates orphaned dependents); poll-driven deferred start on LocalExecutor (dependent jobs park as PENDING and spawn when deps finish). Base `Executor.poll()` annotates `metadata["dependency_failed"]` so callbacks can distinguish upstream failure from a job's own failure.

**Tech Stack:** Python 3.10+, asyncio, pytest + pytest-asyncio (`asyncio_mode = "auto"` — async tests need no decorator), ruff.

**Spec:** `docs/superpowers/specs/2026-07-24-job-dependency-chaining-design.md`

## Global Constraints

- Always run tools via `pixi run` — never invoke `python`, `pytest`, or `ruff` directly. Single-test runs: `pixi run pytest tests/test_x.py::TestClass::test_name -v`.
- Async-only API; no sync wrappers.
- Python 3.10+ union syntax (`X | Y`), `from __future__ import annotations` at top of every module (already present in all touched files).
- Backward compatibility: all new parameters/fields are optional with defaults; no existing behavior changes when `depends_on` is not passed.
- All non-integration tests mock `Executor._call()` (except `test_local.py`, which runs real subprocesses).
- Work on the current branch. Commit after every task.

---

### Task 1: Core plumbing — `depends_on` through types, base class, and executor signatures

**Files:**
- Modify: `cluster_api/_types.py` (JobRecord, ~line 100)
- Modify: `cluster_api/core.py` (submit, submit_array, `_submit_job`, `_submit_array_job`)
- Modify: `cluster_api/executors/lsf.py` (`_submit_job`, `_submit_array_job` signatures only)
- Modify: `cluster_api/executors/local.py` (`_submit_job`, `_submit_array_job` signatures only)
- Test: `tests/test_lsf.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `JobRecord.depends_on: list[str]` (default `[]`); `Executor.submit(..., depends_on: Sequence[JobRecord | str] | None = None)` and same on `submit_array`; keyword-only `depends_on: list[str] | None = None` on `_submit_job`/`_submit_array_job`; `Executor._resolve_dependency_ids(depends_on) -> list[str]`. Tasks 2–4 rely on these exact names.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lsf.py`:

```python
class TestDependencies:

    async def test_depends_on_resolved_to_ids(self, lsf_config, work_dir):
        """depends_on accepts JobRecords and raw id strings; record stores ids."""
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            side_effect=[
                "Job <111> is submitted to queue <normal>.",
                "Job <222> is submitted to queue <normal>.",
            ],
        ):
            a = await executor.submit(
                command="echo a", name="job-a",
                resources=ResourceSpec(work_dir=work_dir),
            )
            b = await executor.submit(
                command="echo b", name="job-b",
                resources=ResourceSpec(work_dir=work_dir),
                depends_on=[a, "999"],
            )
        assert a.depends_on == []
        assert b.depends_on == ["111", "999"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_lsf.py::TestDependencies -v`
Expected: FAIL with `TypeError: submit() got an unexpected keyword argument 'depends_on'`

- [ ] **Step 3: Add the JobRecord field**

In `cluster_api/_types.py`, inside `JobRecord`, add after the `array_elements` field:

```python
    depends_on: list[str] = field(default_factory=list)
```

- [ ] **Step 4: Plumb depends_on through core.py**

In `cluster_api/core.py`:

Add to imports (top of file):

```python
from collections.abc import Sequence
```

Add a static helper on `Executor` (place after `_sanitize_job_name`-adjacent helpers, e.g. right before `submit`):

```python
    @staticmethod
    def _resolve_dependency_ids(
        depends_on: Sequence[JobRecord | str] | None,
    ) -> list[str]:
        """Resolve JobRecords or raw id strings to a list of job ids."""
        if not depends_on:
            return []
        return [d.job_id if isinstance(d, JobRecord) else str(d) for d in depends_on]
```

In `submit()`: add parameter `depends_on: Sequence[JobRecord | str] | None = None` (after `login_shell`), extend the docstring:

```python
            depends_on: Jobs that must finish successfully before this job
                starts. Accepts JobRecords or raw job-id strings. If any
                dependency fails or is killed, this job is cancelled instead
                of pending forever.
```

then before the `_submit_job` call:

```python
        dep_ids = self._resolve_dependency_ids(depends_on)
```

pass `depends_on=dep_ids or None` to `self._submit_job(...)`, and add `depends_on=dep_ids,` to the `JobRecord(...)` construction.

In `submit_array()`: identical changes (parameter, `dep_ids` resolution, pass-through to `_submit_array_job`, `depends_on=dep_ids` on the record).

In the abstract `_submit_job()` signature and the default `_submit_array_job()` implementation: add keyword-only parameter `depends_on: list[str] | None = None` (after `login_shell`); `_submit_array_job`'s default body forwards it to `_submit_job`.

- [ ] **Step 5: Accept the new kwarg in both executors**

In `cluster_api/executors/lsf.py` and `cluster_api/executors/local.py`, add `depends_on: list[str] | None = None` as the last keyword-only parameter of both `_submit_job()` and `_submit_array_job()`. No behavior yet (Tasks 2 and 3 add it); this keeps the suite green in between.

- [ ] **Step 6: Run test to verify it passes**

Run: `pixi run pytest tests/test_lsf.py::TestDependencies -v`
Expected: PASS

- [ ] **Step 7: Run the full check**

Run: `pixi run check`
Expected: lint clean, all tests pass.

- [ ] **Step 8: Commit**

```bash
git add cluster_api/_types.py cluster_api/core.py cluster_api/executors/lsf.py cluster_api/executors/local.py tests/test_lsf.py
git commit -m "Add depends_on plumbing through JobRecord and Executor"
```

---

### Task 2: LSF executor — native `-w`/`-ti` dependency args

**Files:**
- Modify: `cluster_api/executors/lsf.py` (`_submit_job` ~line 179, `_submit_array_job` ~line 203)
- Test: `tests/test_lsf.py`

**Interfaces:**
- Consumes: `depends_on: list[str] | None` kwarg on `_submit_job`/`_submit_array_job` (Task 1).
- Produces: `LSFExecutor._dependency_args(depends_on) -> list[str]`; submitted bsub command lines contain `-w "done(id) && done(id)" -ti` when dependencies are set.

- [ ] **Step 1: Write the failing tests**

Append to the `TestDependencies` class in `tests/test_lsf.py` (created in Task 1):

```python
    async def test_single_dependency_args(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            side_effect=[
                "Job <111> is submitted to queue <normal>.",
                "Job <222> is submitted to queue <normal>.",
            ],
        ) as mock_call:
            a = await executor.submit(
                command="echo a", name="job-a",
                resources=ResourceSpec(work_dir=work_dir),
            )
            await executor.submit(
                command="echo b", name="job-b",
                resources=ResourceSpec(work_dir=work_dir),
                depends_on=[a],
            )
        cmd = mock_call.call_args[0][0]
        assert cmd[cmd.index("-w") + 1] == "done(111)"
        assert "-ti" in cmd

    async def test_fan_in_dependency_expression(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <333> is submitted to queue <normal>.",
        ) as mock_call:
            await executor.submit(
                command="echo c", name="job-c",
                resources=ResourceSpec(work_dir=work_dir),
                depends_on=["111", "222"],
            )
        cmd = mock_call.call_args[0][0]
        assert cmd[cmd.index("-w") + 1] == "done(111) && done(222)"
        assert "-ti" in cmd

    async def test_no_dependency_args_by_default(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <111> is submitted to queue <normal>.",
        ) as mock_call:
            await executor.submit(
                command="echo a", name="job-a",
                resources=ResourceSpec(work_dir=work_dir),
            )
        cmd = mock_call.call_args[0][0]
        assert "-w" not in cmd
        assert "-ti" not in cmd

    async def test_array_with_dependency(self, lsf_config, work_dir):
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            return_value="Job <444> is submitted to queue <normal>.",
        ) as mock_call:
            await executor.submit_array(
                command="echo x", name="arr",
                array_range=(1, 5),
                resources=ResourceSpec(work_dir=work_dir),
                depends_on=["111"],
            )
        cmd = mock_call.call_args[0][0]
        assert cmd[cmd.index("-w") + 1] == "done(111)"
        assert "-ti" in cmd
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_lsf.py::TestDependencies -v`
Expected: the four new tests FAIL (`ValueError: '-w' is not in list` / assertion errors); `test_depends_on_resolved_to_ids` still PASSES.

- [ ] **Step 3: Implement `_dependency_args` and wire it in**

In `cluster_api/executors/lsf.py`, add next to `_env_control_args`:

```python
    @staticmethod
    def _dependency_args(depends_on: list[str] | None) -> list[str]:
        """bsub args to start only after all *depends_on* jobs succeed.

        ``-ti`` makes LSF terminate this job as soon as the dependency can
        never be satisfied (a dependency failed or was killed), instead of
        leaving it pending forever.
        """
        if not depends_on:
            return []
        expr = " && ".join(f"done({d})" for d in depends_on)
        return ["-w", expr, "-ti"]
```

In both `_submit_job()` and `_submit_array_job()`, directly after the existing
`extra_args.extend(self._env_control_args(inherit_env, login_shell))` line, add:

```python
        extra_args.extend(self._dependency_args(depends_on))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_lsf.py::TestDependencies -v`
Expected: all PASS

- [ ] **Step 5: Run the full check**

Run: `pixi run check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add cluster_api/executors/lsf.py tests/test_lsf.py
git commit -m "LSF: submit dependencies as bsub -w done(...) with -ti orphan termination"
```

---

### Task 3: Local executor — deferred start for dependent jobs

**Files:**
- Modify: `cluster_api/executors/local.py`
- Test: `tests/test_local.py`

**Interfaces:**
- Consumes: `depends_on` kwarg (Task 1); `JobRecord.depends_on` (Task 1).
- Produces: dependent local jobs get synthetic ids `waiting-N`, stay `PENDING` until deps are `DONE`, then spawn inside `poll()`; on dep failure they become `KILLED` with `metadata["dependency_failed"] = [culprit ids]`. Internal: `self._waiting: dict[str, dict]`, `_job_env()`, `_bash_cmd()`, `_spawn_deferred()`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_local.py`:

```python
class TestLocalDependencies:

    async def test_dependent_waits_then_runs(self, default_config, work_dir):
        """B parks as PENDING, spawns after A succeeds, then completes."""
        executor = LocalExecutor(default_config)
        marker = Path(work_dir) / "a.txt"
        a = await executor.submit(
            command=f"echo done > {marker}", name="dep-a",
            resources=ResourceSpec(work_dir=work_dir),
        )
        b = await executor.submit(
            command=f"cat {marker}", name="dep-b",
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a],
        )
        assert b.job_id.startswith("waiting-")
        assert b.status == JobStatus.PENDING
        assert b.job_id not in executor._processes  # not spawned yet

        await executor._processes[a.job_id].wait()
        await executor.poll()  # sees A DONE -> spawns B in the same cycle
        assert a.status == JobStatus.DONE
        assert b.job_id in executor._processes

        await executor._processes[b.job_id].wait()
        await executor.poll()
        assert b.status == JobStatus.DONE
        assert b.exit_code == 0

    async def test_dependent_killed_when_dep_fails(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        a = await executor.submit(
            command="exit 1", name="fail-a",
            resources=ResourceSpec(work_dir=work_dir),
        )
        b = await executor.submit(
            command="echo never", name="dep-b",
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a],
        )
        await executor._processes[a.job_id].wait()
        await executor.poll()
        assert a.status == JobStatus.FAILED
        assert b.status == JobStatus.KILLED
        assert b.metadata["dependency_failed"] == [a.job_id]
        assert b.job_id not in executor._processes  # never spawned

    async def test_fan_in_waits_for_all(self, default_config, work_dir):
        executor = LocalExecutor(default_config)
        a = await executor.submit(
            command="true", name="fan-a",
            resources=ResourceSpec(work_dir=work_dir),
        )
        b = await executor.submit(
            command="sleep 0.4", name="fan-b",
            resources=ResourceSpec(work_dir=work_dir),
        )
        c = await executor.submit(
            command="echo c", name="fan-c",
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a, b],
        )
        await executor._processes[a.job_id].wait()
        await executor.poll()
        # A done, B still running -> C still parked
        assert c.job_id not in executor._processes
        assert c.status == JobStatus.PENDING

        await executor._processes[b.job_id].wait()
        await executor.poll()
        assert c.job_id in executor._processes
        await executor._processes[c.job_id].wait()
        await executor.poll()
        assert c.status == JobStatus.DONE

    async def test_dependent_array_job(self, default_config, work_dir):
        """An array job with depends_on defers all element spawns."""
        executor = LocalExecutor(default_config)
        a = await executor.submit(
            command="true", name="arr-dep",
            resources=ResourceSpec(work_dir=work_dir),
        )
        arr = await executor.submit_array(
            command='echo "element $ARRAY_INDEX"', name="arr",
            array_range=(1, 3),
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a],
        )
        assert arr.job_id.startswith("waiting-")
        assert not any(
            k.startswith(f"{arr.job_id}[") for k in executor._processes
        )
        await executor._processes[a.job_id].wait()
        await executor.poll()  # spawns the elements
        keys = [k for k in executor._processes if k.startswith(f"{arr.job_id}[")]
        assert len(keys) == 3
        for k in keys:
            await executor._processes[k].wait()
        await executor.poll()
        assert arr.status == JobStatus.DONE

    async def test_cancel_waiting_job(self, default_config, work_dir):
        """Cancelling a parked job removes it without spawning; dep unaffected."""
        executor = LocalExecutor(default_config)
        a = await executor.submit(
            command="sleep 5", name="slow-a",
            resources=ResourceSpec(work_dir=work_dir),
        )
        b = await executor.submit(
            command="echo never", name="dep-b",
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a],
        )
        await executor.cancel(b.job_id)
        assert b.status == JobStatus.KILLED
        assert b.job_id not in executor._waiting
        await executor.poll()
        assert b.job_id not in executor._processes
        assert a.status in {JobStatus.PENDING, JobStatus.RUNNING}
        await executor.cancel(a.job_id)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run pytest tests/test_local.py::TestLocalDependencies -v`
Expected: FAIL — jobs spawn immediately (`b.job_id` is a PID, not `waiting-N`), `executor._waiting` doesn't exist.

- [ ] **Step 3: Implement deferred start in LocalExecutor**

All changes in `cluster_api/executors/local.py`.

**3a.** In `__init__`, add:

```python
        self._waiting: dict[str, dict] = {}
```

**3b.** Add two small helpers (before `_submit_job`), then use them to replace the duplicated env/bash-building in `_submit_job` and `_submit_array_job` (the existing `if inherit_env: ... full_env = ...` / `bash_cmd = ...` blocks):

```python
    @staticmethod
    def _job_env(env: dict[str, str] | None, inherit_env: bool) -> dict[str, str]:
        return {**os.environ, **(env or {})} if inherit_env else _baseline_env(env)

    @staticmethod
    def _bash_cmd(script_path: str, login_shell: bool) -> list[str]:
        return ["bash", "-l", script_path] if login_shell else ["bash", script_path]
```

**3c.** In `_submit_job()`, right after `script_path = write_script(...)`, park the job when it has dependencies:

```python
        if depends_on:
            job_id = f"waiting-{self._next_id}"
            self._next_id += 1
            self._waiting[job_id] = {
                "script_path": script_path,
                "resources": resources,
                "env": env,
                "cwd": cwd,
                "inherit_env": inherit_env,
                "login_shell": login_shell,
                "array_range": None,
            }
            return job_id, script_path
```

**3d.** In `_submit_array_job()`, right after `script_path = write_script(...)` (and after the existing `max_concurrent` warning), the same with `array_range` set:

```python
        if depends_on:
            job_id = f"waiting-{self._next_id}"
            self._next_id += 1
            self._waiting[job_id] = {
                "script_path": script_path,
                "resources": resources,
                "env": env,
                "cwd": cwd,
                "inherit_env": inherit_env,
                "login_shell": login_shell,
                "array_range": array_range,
            }
            return job_id, script_path
```

**3e.** Add the deferred spawn helper (after `_submit_array_job`):

```python
    async def _spawn_deferred(self, job_id: str, entry: dict) -> None:
        """Spawn a parked job whose dependencies are now satisfied.

        The job keeps its synthetic ``waiting-N`` id (ids never change once
        returned), so the PID-based stateless cancel path does not apply —
        same trade-off as local array jobs.
        """
        resources = entry["resources"]
        full_env = self._job_env(entry["env"], entry["inherit_env"])
        bash_cmd = self._bash_cmd(entry["script_path"], entry["login_shell"])

        if entry["array_range"] is None:
            stdout_dest, stderr_dest = self._open_output_files(resources, job_id=job_id)
            proc = await asyncio.create_subprocess_exec(
                *bash_cmd,
                stdout=stdout_dest,
                stderr=stderr_dest,
                env=full_env,
                cwd=entry["cwd"],
                start_new_session=True,
            )
            self._processes[job_id] = proc
        else:
            start, end = entry["array_range"]
            for index in range(start, end + 1):
                element_env = {**full_env, "ARRAY_INDEX": str(index)}
                stdout_dest, stderr_dest = self._open_output_files(
                    resources, job_id=job_id, element_index=index,
                )
                proc = await asyncio.create_subprocess_exec(
                    *bash_cmd,
                    stdout=stdout_dest,
                    stderr=stderr_dest,
                    env=element_env,
                    cwd=entry["cwd"],
                )
                self._processes[f"{job_id}[{index}]"] = proc
```

**3f.** In `poll()`, insert this block immediately before the final `return` statement (after array aggregation, so same-cycle dep completion is visible):

```python
        # --- Waiting (dependency-deferred) jobs ---
        for job_id in list(self._waiting):
            record = self._jobs.get(job_id)
            if record is None or record.is_terminal:
                self._waiting.pop(job_id)
                continue
            dep_records = [self._jobs.get(d) for d in record.depends_on]
            failed = [
                d for d, r in zip(record.depends_on, dep_records)
                if r is None or r.status in {JobStatus.FAILED, JobStatus.KILLED}
            ]
            now = datetime.now(timezone.utc)
            if failed:
                # An untracked dep (None) can never be satisfied — treat as failed.
                self._waiting.pop(job_id)
                record.status = JobStatus.KILLED
                record.metadata["dependency_failed"] = failed
                record.finish_time = now
            elif all(r.status == JobStatus.DONE for r in dep_records):
                entry = self._waiting.pop(job_id)
                await self._spawn_deferred(job_id, entry)
                record._last_seen = now
            else:
                record._last_seen = now  # keep zombie detection at bay while parked
```

**3g.** In `cancel()`, discard any parked entry so a cancelled waiting job can't spawn later. At the top of the method add:

```python
        deferred = self._waiting.pop(job_id, None)
```

and change the stateless-path condition from
`elif job_id not in self._processes:` to
`elif deferred is None and job_id not in self._processes:`
(a parked job has no process to kill; the shared status-update code below it already marks the record KILLED).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run pytest tests/test_local.py::TestLocalDependencies -v`
Expected: all 5 PASS

- [ ] **Step 5: Run the full check**

Run: `pixi run check`
Expected: clean — especially the existing `test_local.py` suite (the env/bash helper refactor in 3b touches the immediate-spawn paths).

- [ ] **Step 6: Commit**

```bash
git add cluster_api/executors/local.py tests/test_local.py
git commit -m "Local: poll-driven deferred start for jobs with dependencies"
```

---

### Task 4: Base poll annotates dependency-driven failures

**Files:**
- Modify: `cluster_api/core.py` (`poll()`, before its `return`)
- Test: `tests/test_lsf.py`

**Interfaces:**
- Consumes: `JobRecord.depends_on` (Task 1); LSF `-ti` kills dependents → they surface as `EXIT`/`FAILED` via normal polling (Task 2).
- Produces: `record.metadata["dependency_failed"] = [culprit ids]` on any record whose failure coincides with a failed tracked dependency. Consumers: `on_failure` callbacks, fileglancer-style persistence layers.

- [ ] **Step 1: Write the failing test**

Append to `TestDependencies` in `tests/test_lsf.py`:

```python
    async def test_poll_annotates_dependency_failure(self, lsf_config, work_dir):
        """When -ti kills a dependent, poll marks why it died."""
        executor = LSFExecutor(lsf_config)
        with patch.object(
            executor, "_call",
            new_callable=AsyncMock,
            side_effect=[
                "Job <111> is submitted to queue <normal>.",
                "Job <222> is submitted to queue <normal>.",
            ],
        ):
            a = await executor.submit(
                command="exit 1", name="job-a",
                resources=ResourceSpec(work_dir=work_dir),
            )
            b = await executor.submit(
                command="echo b", name="job-b",
                resources=ResourceSpec(work_dir=work_dir),
                depends_on=[a],
            )
        bjobs_json = json.dumps({
            "RECORDS": [
                {"JOBID": "111", "JOB_NAME": "test-job-a", "STAT": "EXIT",
                 "EXIT_CODE": "1"},
                {"JOBID": "222", "JOB_NAME": "test-job-b", "STAT": "EXIT",
                 "EXIT_CODE": ""},
            ]
        })
        with patch.object(
            executor, "_call", new_callable=AsyncMock, return_value=bjobs_json,
        ):
            await executor.poll()
        assert a.status == JobStatus.FAILED
        assert b.status == JobStatus.FAILED
        assert b.metadata["dependency_failed"] == ["111"]
        assert "dependency_failed" not in a.metadata
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_lsf.py::TestDependencies::test_poll_annotates_dependency_failure -v`
Expected: FAIL with `KeyError: 'dependency_failed'`

- [ ] **Step 3: Implement the annotation**

In `cluster_api/core.py`, in `poll()`, insert immediately before the final `return` statement (after the array-status aggregation loop):

```python
        # Annotate dependency-driven failures so callbacks can distinguish
        # "upstream broke" from "my job broke".
        for record in self._jobs.values():
            if (
                record.depends_on
                and record.status in (JobStatus.FAILED, JobStatus.KILLED)
                and "dependency_failed" not in record.metadata
            ):
                culprits = [
                    d for d in record.depends_on
                    if (dep := self._jobs.get(d)) is not None
                    and dep.status in (JobStatus.FAILED, JobStatus.KILLED)
                ]
                if culprits:
                    record.metadata["dependency_failed"] = culprits
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_lsf.py::TestDependencies -v`
Expected: all PASS

- [ ] **Step 5: Run the full check**

Run: `pixi run check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add cluster_api/core.py tests/test_lsf.py
git commit -m "Annotate dependency_failed metadata when a dependent job dies upstream"
```

---

### Task 5: Integration test and docs

**Files:**
- Modify: `tests/test_integration.py`
- Modify: `README.md` (usage section)
- Modify: `CLAUDE.md` (capabilities paragraph, line 5)

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces: live-LSF verification of `-w`/`-ti` behavior (the one design assumption needing a real cluster); user-facing docs.

- [ ] **Step 1: Add the integration test**

Append to `tests/test_integration.py` (module is already marked `integration` and skipped without `bsub`; it uses the `lsf_executor`, `monitor`, `work_dir` fixtures defined in that file — follow the style of `TestLSFSubmitAndMonitor`):

```python
class TestLSFDependencies:

    async def test_chain_success(self, lsf_executor, monitor, work_dir):
        """B waits for A, then runs and sees A's output file."""
        marker = Path(work_dir) / "chain_marker.txt"
        marker.unlink(missing_ok=True)
        a = await lsf_executor.submit(
            command=f"sleep 5 && echo ok > {marker}", name="chain-a",
            resources=ResourceSpec(work_dir=work_dir),
        )
        b = await lsf_executor.submit(
            command=f"cat {marker}", name="chain-b",
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a],
        )
        await monitor.wait_for(a, b, timeout=300)
        assert a.status == JobStatus.DONE
        assert b.status == JobStatus.DONE

    async def test_chain_failure_terminates_dependent(
        self, lsf_executor, monitor, work_dir
    ):
        """-ti: when A fails, LSF terminates B instead of pending forever."""
        a = await lsf_executor.submit(
            command="sleep 2 && exit 1", name="failchain-a",
            resources=ResourceSpec(work_dir=work_dir),
        )
        b = await lsf_executor.submit(
            command="echo never", name="failchain-b",
            resources=ResourceSpec(work_dir=work_dir),
            depends_on=[a],
        )
        await monitor.wait_for(a, b, timeout=300)
        assert a.status == JobStatus.FAILED
        assert b.status in {JobStatus.FAILED, JobStatus.KILLED}
        assert b.metadata.get("dependency_failed") == [a.job_id]
```

- [ ] **Step 2: Run the integration tests (only if on a machine with LSF)**

Run: `pixi run test-integration`
Expected: `TestLSFDependencies` passes on the Janelia cluster. If not on an LSF host, note that in the commit message and run `pixi run test` instead (integration tests are skipped by default).

- [ ] **Step 3: Update README.md**

Add a subsection to the usage examples in `README.md` (after the existing submit example — match the surrounding style):

````markdown
### Job dependencies

Chain jobs so each stage starts only after the previous one succeeds:

```python
convert = await executor.submit("convert.sh input.tif", name="convert-s0")
pyramid = await executor.submit(
    "build_pyramid.sh", name="pyramid",
    depends_on=[convert],   # JobRecords or raw job-id strings
)
```

`depends_on` accepts multiple jobs (fan-in). On LSF this uses native
`bsub -w "done(...)" -ti`, so chains keep running if your process exits,
and LSF terminates dependents whose upstream failed — no jobs stuck
pending forever. When a dependent is cancelled this way,
`record.metadata["dependency_failed"]` lists the failed upstream job ids.
````

- [ ] **Step 4: Update CLAUDE.md**

In `CLAUDE.md`, extend the capabilities paragraph (currently "Key capabilities beyond submit/poll/cancel: `reconnect()` ... and `cancel_by_name()` ..."), adding:

```
`depends_on` on `submit()`/`submit_array()` chains jobs (native `bsub -w ... -ti` on LSF; poll-driven deferred start on Local) with auto-cancel of dependents when an upstream job fails.
```

- [ ] **Step 5: Run the full check**

Run: `pixi run check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add tests/test_integration.py README.md CLAUDE.md
git commit -m "Add dependency-chain integration tests and docs"
```
