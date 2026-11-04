"""Dependency-free contracts and shared value types.

This distribution imports only the Python standard library. It exists so third
parties can implement benchmarks (and other extensions) without pulling in the
orchestration application's provider, CLI, or persistence dependencies.
"""

from __future__ import annotations

from .benchmark import AbstractBenchmark
from .benchmark_kind import BenchmarkKind
from .capabilities import WorkerCapability

__all__ = [
    "AbstractBenchmark",
    "BenchmarkKind",
    "WorkerCapability",
]
