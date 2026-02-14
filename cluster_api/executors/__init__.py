"""Executor registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..core import Executor

_REGISTRY: dict[str, type[Executor]] = {}


def _ensure_builtins() -> None:
    """Lazily register built-in executors."""
    if "lsf" in _REGISTRY and "local" in _REGISTRY:
        return
    from .lsf import LSFExecutor
    from .local import LocalExecutor

    _REGISTRY.setdefault("lsf", LSFExecutor)
    _REGISTRY.setdefault("local", LocalExecutor)


def get_executor_class(name: str) -> type[Executor]:
    """Get an executor class by name."""
    _ensure_builtins()
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown executor: {name!r}. Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name]


