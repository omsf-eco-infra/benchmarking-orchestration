"""Benchmark workload normalization tests."""

import pytest

from benchmarking_orchestration_core import BenchmarkKind
from benchmarking_orchestration_core.benchmark_kind import _normalize_benchmark_kind


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (" MD ", BenchmarkKind.MD),
        ("RbFe", BenchmarkKind.RBFE),
        ("both", BenchmarkKind.BOTH),
    ],
)
def test_normalize_benchmark_kind(raw_value, expected):
    """Normalize supported workload names.

    Parameters
    ----------
    raw_value : str
        User-supplied workload name.
    expected : BenchmarkKind
        Expected normalized workload.
    """
    assert _normalize_benchmark_kind(raw_value) is expected


@pytest.mark.parametrize("raw_value", [" ", "unknown"])
def test_normalize_benchmark_kind_rejects_invalid_names(raw_value):
    """Reject empty and unsupported workload names.

    Parameters
    ----------
    raw_value : str
        Invalid workload name.
    """
    with pytest.raises(ValueError):
        _normalize_benchmark_kind(raw_value)
