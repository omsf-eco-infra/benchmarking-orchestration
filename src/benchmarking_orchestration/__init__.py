"""Benchmarking orchestration application.

The top-level package is intentionally lazy: importing it must not pull in
optional provider dependencies such as ``boto3``. Access exported names through
attribute lookup (or ``from benchmarking_orchestration import ...``), which
imports them on demand.
"""

from __future__ import annotations

from typing import Any

__all__ = ["create_app", "WorkerCapability"]


def __getattr__(name: str) -> Any:
    if name == "create_app":
        from .commands import create_app

        return create_app
    if name == "WorkerCapability":
        from benchmarking_orchestration_core.capabilities import WorkerCapability

        return WorkerCapability
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
