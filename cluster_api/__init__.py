"""py-cluster-api: Generic Python library for running jobs on HPC clusters."""

from __future__ import annotations

from typing import Any

from ._types import JobExitCondition, JobRecord, JobStatus, ResourceSpec
from .config import ClusterConfig, load_config
from .core import Executor
from .executors import get_executor_class
from .executors.local import LocalExecutor
from .executors.lsf import LSFExecutor
from .monitor import JobMonitor

__all__ = [
    "create_executor",
    "Executor",
    "LSFExecutor",
    "LocalExecutor",
    "JobRecord",
    "JobStatus",
    "JobExitCondition",
    "ResourceSpec",
    "ClusterConfig",
    "JobMonitor",
    "load_config",
]


def create_executor(
    profile: str | None = None,
    config_path: str | None = None,
    **overrides: Any,
) -> Executor:
    """Create an executor from config.

    Args:
        profile: Config profile name to use.
        config_path: Explicit path to config YAML.
        **overrides: Override individual config values.

    Returns:
        An Executor instance configured for the specified backend.
    """
    config = load_config(path=config_path, profile=profile, overrides=overrides or None)
    cls = get_executor_class(config.executor)
    return cls(config)
