"""Async polling loop and callback dispatch."""

from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ._types import JobExitCondition, JobRecord, JobStatus

if TYPE_CHECKING:
    from .core import Executor

logger = logging.getLogger(__name__)


class JobMonitor:
    """Monitors job status via polling and dispatches callbacks."""

    def __init__(self, executor: Executor, poll_interval: float | None = None) -> None:
        self.executor = executor
        self.poll_interval = poll_interval or executor.config.poll_interval
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        """Start the polling loop."""
        self._stopped.clear()
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("Monitor started (interval=%.1fs)", self.poll_interval)

    async def stop(self) -> None:
        """Stop the polling loop and wait for it to finish."""
        self._stopped.set()
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Monitor stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop."""
        while not self._stopped.is_set():
            try:
                await self.executor.poll()
                await self._dispatch_all_callbacks()
                await self._check_zombies()
                await self._purge_completed()
            except Exception:
                logger.exception("Error in poll loop")

            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self.poll_interval)
                break  # stopped was set
            except asyncio.TimeoutError:
                pass  # normal: just means poll_interval elapsed

    async def _dispatch_all_callbacks(self) -> None:
        """Check all jobs for pending callbacks."""
        for record in list(self.executor._jobs.values()):
            if record.is_terminal and record._callbacks:
                await self._dispatch_callbacks(record)

    async def _dispatch_callbacks(self, record: JobRecord) -> None:
        """Fire matching callbacks for a terminal job, then clear them."""
        condition_map = {
            JobStatus.DONE: JobExitCondition.SUCCESS,
            JobStatus.FAILED: JobExitCondition.FAILURE,
            JobStatus.KILLED: JobExitCondition.KILLED,
        }
        job_condition = condition_map.get(record.status)

        fired: list[int] = []
        for i, (condition, callback) in enumerate(record._callbacks):
            if condition == JobExitCondition.ANY or condition == job_condition:
                try:
                    result = callback(record)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception(
                        "Callback error for job %s", record.job_id
                    )
                fired.append(i)

        # Remove fired callbacks in reverse order to preserve indices
        for i in reversed(fired):
            record._callbacks.pop(i)

    async def _check_zombies(self) -> None:
        """Mark jobs as FAILED if not seen by scheduler for > zombie_timeout."""
        timeout_minutes = self.executor.config.zombie_timeout_minutes
        now = datetime.now(timezone.utc)

        for record in self.executor._jobs.values():
            if record.is_terminal:
                continue
            if record._last_seen is None:
                continue
            elapsed = (now - record._last_seen).total_seconds() / 60.0
            if elapsed > timeout_minutes:
                logger.warning(
                    "Zombie detected: job %s not seen for %.1f minutes",
                    record.job_id, elapsed,
                )
                record.status = JobStatus.FAILED
                record.metadata["zombie"] = True

    async def _purge_completed(self) -> None:
        """Remove terminal jobs older than completed_retention."""
        retention_minutes = self.executor.config.completed_retention_minutes
        now = datetime.now(timezone.utc)

        to_remove = []
        for job_id, record in self.executor._jobs.items():
            if not record.is_terminal:
                continue
            if not record._callbacks:
                # Use finish_time if available, else _last_seen
                ref_time = record.finish_time or record._last_seen
                if ref_time is not None:
                    elapsed = (now - ref_time).total_seconds() / 60.0
                    if elapsed > retention_minutes:
                        to_remove.append(job_id)

        for job_id in to_remove:
            del self.executor._jobs[job_id]
            logger.debug("Purged completed job %s", job_id)

    async def wait_for(
        self, *records: JobRecord, timeout: float | None = None
    ) -> None:
        """Wait until all given jobs reach a terminal state."""
        async def _wait() -> None:
            while True:
                if all(r.is_terminal for r in records):
                    return
                await asyncio.sleep(0.5)

        if timeout is not None:
            await asyncio.wait_for(_wait(), timeout=timeout)
        else:
            await _wait()
