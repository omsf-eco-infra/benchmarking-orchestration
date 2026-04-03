from benchmarking_orchestration.capabilities import (
    WorkerCapability,
    _resolve_bench_worker_capability,
)


def test_resolve_bench_worker_capability_keeps_aws_family_behavior():
    assert _resolve_bench_worker_capability("g5.xlarge") == WorkerCapability.G5
    assert _resolve_bench_worker_capability("p4d.24xlarge") == WorkerCapability.P
