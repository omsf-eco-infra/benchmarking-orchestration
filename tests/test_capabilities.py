import pytest

from benchmarking_orchestration.capabilities import (
    WorkerCapability,
    _resolve_bench_worker_capability,
)


@pytest.mark.parametrize(
    ("instance_type", "capability"),
    [
        ("g3.4xlarge", WorkerCapability.G3),
        ("g5.xlarge", WorkerCapability.G5),
        ("g6.xlarge", WorkerCapability.G6),
        ("p4d.24xlarge", WorkerCapability.P),
        ("vt1.3xlarge", WorkerCapability.VT1),
    ],
)
def test_resolve_bench_worker_capability_keeps_aws_family_behavior(
    instance_type, capability
):
    assert _resolve_bench_worker_capability(instance_type) == capability
