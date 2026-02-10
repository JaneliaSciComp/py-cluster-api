"""Shared test fixtures."""

from __future__ import annotations

import pytest

from cluster_api.config import ClusterConfig


@pytest.fixture
def default_config(tmp_path):
    """A default ClusterConfig with work_dir set to a temp dir."""
    return ClusterConfig(
        executor="local",
        work_dir=str(tmp_path / "logs"),
        job_name_prefix="test",
        poll_interval=0.5,
        command_timeout=10.0,
        zombie_timeout_minutes=0.1,
        completed_retention_minutes=0.01,
    )


@pytest.fixture
def lsf_config(tmp_path):
    """An LSF ClusterConfig for testing."""
    return ClusterConfig(
        executor="lsf",
        work_dir=str(tmp_path / "logs"),
        job_name_prefix="test",
        queue="normal",
        memory="8 GB",
        walltime="04:00",
        poll_interval=0.5,
        command_timeout=10.0,
        use_stdin=True,
        suppress_job_email=True,
        lsf_units="MB",
    )
