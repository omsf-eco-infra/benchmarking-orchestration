"""Benchmark extension contract."""

from __future__ import annotations

import abc


class AbstractBenchmark(abc.ABC):
    """Base class for benchmark implementations."""

    @abc.abstractmethod
    def run_benchmark(self, task_id: str) -> None:
        """Run one benchmark task.

        Parameters
        ----------
        task_id : str
            Identifier of the benchmark task.
        """
