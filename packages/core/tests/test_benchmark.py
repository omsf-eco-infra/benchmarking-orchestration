"""Provider-neutral benchmark contract tests."""

import pytest

from benchmarking_orchestration_core import AbstractBenchmark


def test_abstract_benchmark_requires_an_implementation():
    """Require subclasses to implement benchmark execution."""
    with pytest.raises(TypeError, match="abstract"):
        AbstractBenchmark()


def test_benchmark_accepts_an_opaque_task_identifier():
    """Allow benchmark implementations to use non-provider-specific task IDs."""

    class RecordingBenchmark(AbstractBenchmark):
        """Record the identifier passed to a benchmark implementation."""

        def run_benchmark(self, task_id: str) -> None:
            """Record a task without interpreting its identifier.

            Parameters
            ----------
            task_id : str
                Opaque task identifier supplied by the caller.
            """
            self.task_id = task_id

    benchmark = RecordingBenchmark()
    benchmark.run_benchmark("local-task-123")

    assert benchmark.task_id == "local-task-123"
