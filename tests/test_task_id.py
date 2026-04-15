from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.task_id import (
    _build_bench_task_id,
    _parse_bench_task_id,
    _parse_bench_task_metadata,
)


def test_build_bench_task_id_omits_mps_metadata_for_default_count():
    launch_task_id = "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"

    task_id = _build_bench_task_id(launch_task_id, BenchmarkKind.MD)

    assert task_id == f"bench:md:{launch_task_id}"


def test_build_bench_task_id_encodes_mps_process_count_when_requested():
    launch_task_id = "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"

    task_id = _build_bench_task_id(
        launch_task_id,
        BenchmarkKind.RBFE,
        mps_process_count=3,
    )

    assert task_id == f"bench:rbfe:mps:3:{launch_task_id}"


def test_parse_bench_task_metadata_returns_encoded_mps_process_count():
    task_id = (
        "bench:rbfe:mps:3:"
        "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )

    benchmark_kind, mps_process_count, launch_task_id = _parse_bench_task_metadata(
        task_id
    )

    assert benchmark_kind is BenchmarkKind.RBFE
    assert mps_process_count == 3
    assert launch_task_id == (
        "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )


def test_parse_bench_task_metadata_defaults_legacy_tasks_to_single_process():
    task_id = (
        "bench:md:us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )

    benchmark_kind, mps_process_count, launch_task_id = _parse_bench_task_metadata(
        task_id
    )

    assert benchmark_kind is BenchmarkKind.MD
    assert mps_process_count == 1
    assert launch_task_id == (
        "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )


def test_parse_bench_task_id_preserves_legacy_two_value_interface():
    task_id = (
        "bench:md:mps:5:"
        "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )

    benchmark_kind, launch_task_id = _parse_bench_task_id(task_id)

    assert benchmark_kind is BenchmarkKind.MD
    assert launch_task_id == (
        "us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )
