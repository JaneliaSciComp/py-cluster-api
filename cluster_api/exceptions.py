"""Exception hierarchy for cluster_api."""


class ClusterAPIError(Exception):
    """Base exception for all cluster-api errors."""


class CommandTimeoutError(ClusterAPIError):
    """A subprocess command exceeded its timeout."""


class CommandFailedError(ClusterAPIError):
    """A subprocess command returned a non-zero exit code."""


class SubmitError(ClusterAPIError):
    """Failed to submit a job or parse its ID from scheduler output."""
