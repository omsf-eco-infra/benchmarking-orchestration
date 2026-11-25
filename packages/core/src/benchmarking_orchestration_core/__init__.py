"""Provider-neutral benchmark contracts and workload kinds.

This distribution imports only the Python standard library. Infrastructure
semantics such as instance types, machine images, worker capabilities, and
launch-task formats belong to provider packages, not core.
"""

from __future__ import annotations

from .benchmark import AbstractBenchmark
from .benchmark_kind import BenchmarkKind

__all__ = [
    "AbstractBenchmark",
    "BenchmarkKind",
]
