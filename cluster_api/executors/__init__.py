"""Executor registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..core import Executor

_REGISTRY: dict[str, type[Executor]] = {}


def _ensure_builtins() -> None:
    """Lazily register built-in executors."""
    if _REGISTRY:
        return
    from .lsf import LSFExecutor
    from .local import LocalExecutor

    _REGISTRY["lsf"] = LSFExecutor
    _REGISTRY["local"] = LocalExecutor


def get_executor_class(name: str) -> type[Executor]:
    """Get an executor class by name."""
    _ensure_builtins()
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown executor: {name!r}. Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name]


def register_executor(name: str, cls: type[Executor]) -> None:
    """Register a custom executor class."""
    _REGISTRY[name] = cls
