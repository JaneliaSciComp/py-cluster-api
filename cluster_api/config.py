"""YAML config loader with Nextflow-style profiles."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


_MEMORY_UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}

_MEMORY_RE = re.compile(r"^\s*([\d.]+)\s*(B|KB|MB|GB|TB)\s*$", re.IGNORECASE)


def parse_memory_bytes(s: str) -> int:
    """Parse a memory string like '8 GB' into bytes."""
    m = _MEMORY_RE.match(s)
    if not m:
        raise ValueError(f"Cannot parse memory string: {s!r}")
    value = float(m.group(1))
    unit = m.group(2).upper()
    return int(value * _MEMORY_UNITS[unit])


@dataclass
class ClusterConfig:
    """Configuration for cluster job execution."""

    executor: str = "local"
    cpus: int | None = None
    memory: str | None = None
    walltime: str | None = None
    queue: str | None = None
    account: str | None = None
    poll_interval: float = 10.0
    shebang: str = "#!/bin/bash"
    script_prologue: list[str] = field(default_factory=list)
    script_epilogue: list[str] = field(default_factory=list)
    extra_directives: list[str] = field(default_factory=list)
    directives_skip: list[str] = field(default_factory=list)
    log_directory: str = "./logs"
    lsf_units: str | None = None
    use_stdin: bool = False
    job_name_prefix: str | None = None
    zombie_timeout_minutes: float = 30.0
    completed_retention_minutes: float = 10.0
    command_timeout: float = 100.0
    suppress_job_email: bool = True


_CONFIG_SEARCH_PATHS = [
    "cluster_api.yaml",
    "~/.config/cluster_api/config.yaml",
]


def _find_config_path() -> Path | None:
    """Search for config file in standard locations."""
    env_path = os.environ.get("CLUSTER_API_CONFIG")
    if env_path:
        p = Path(env_path).expanduser()
        if p.exists():
            return p

    for candidate in _CONFIG_SEARCH_PATHS:
        p = Path(candidate).expanduser()
        if p.exists():
            return p

    return None


def _merge_dicts(base: dict, override: dict) -> dict:
    """Shallow merge of override into base."""
    result = dict(base)
    result.update(override)
    return result


def load_config(
    path: str | Path | None = None,
    profile: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> ClusterConfig:
    """Load configuration from YAML with optional profile and overrides.

    Merges: base config → profile → overrides.
    """
    raw: dict[str, Any] = {}

    config_path = Path(path) if path else _find_config_path()
    if config_path and config_path.exists():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    profiles = raw.pop("profiles", {})

    if profile and profile in profiles:
        raw = _merge_dicts(raw, profiles[profile])

    if overrides:
        raw = _merge_dicts(raw, overrides)

    # Build ClusterConfig from the merged dict, ignoring unknown keys
    known_fields = {f.name for f in ClusterConfig.__dataclass_fields__.values()}
    filtered = {k: v for k, v in raw.items() if k in known_fields}

    return ClusterConfig(**filtered)
