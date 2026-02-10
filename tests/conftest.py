"""Shared test fixtures."""

from __future__ import annotations

import pytest

from cluster_api.config import ClusterConfig


@pytest.fixture
def work_dir(tmp_path):
    """Temporary work directory for job scripts and output."""
    return str(tmp_path)


@pytest.fixture
def default_config():
    """A default ClusterConfig for local testing."""
    return ClusterConfig(
        executor="local",
        job_name_prefix="test",
        poll_interval=0.5,
        command_timeout=10.0,
        zombie_timeout_minutes=0.1,
        completed_retention_minutes=0.01,
    )


@pytest.fixture
def lsf_config():
    """An LSF ClusterConfig for testing."""
    return ClusterConfig(
        executor="lsf",
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
