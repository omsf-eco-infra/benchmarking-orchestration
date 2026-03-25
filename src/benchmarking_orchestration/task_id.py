from __future__ import annotations

import uuid

from .benchmark_kind import BenchmarkKind, _normalize_benchmark_kind
from .providers.aws_provider import DEFAULT_LAUNCH_AMI_ID


def _build_task_id(
    region: str,
    instance_type: str,
    ami_id: str = DEFAULT_LAUNCH_AMI_ID,
    cloud_init_b64: str | None = None,
) -> str:
    """Build a unique task identifier for EC2 launch orchestration.

    Parameters
    ----------
    region : str
        AWS region where the launch should occur.
    instance_type : str
        Validated EC2 instance type.
    ami_id : str, default=DEFAULT_LAUNCH_AMI_ID
        AMI identifier to use for launch.
    cloud_init_b64 : str, optional
        Base64-encoded cloud-init payload to embed in task metadata.

    Returns
    -------
    str
        Task identifier in ``<region>:<instance_type>:<ami_id>:<uuid4>`` format
        when no cloud-init payload is provided, otherwise
        ``<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>``.
    """
    if cloud_init_b64 is None:
        return f"{region}:{instance_type}:{ami_id}:{uuid.uuid4()}"

    return f"{region}:{instance_type}:{ami_id}:{cloud_init_b64}:{uuid.uuid4()}"


def _parse_launch_task_id(taskid: str) -> tuple[str, str, str, str | None]:
    """Parse a launch task identifier into region and instance type.

    Parameters
    ----------
    taskid : str
        Task identifier in
        ``<region>:<instance_type>:<ami_id>:<uuid4>`` format,
        or ``<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>`` format.

    Returns
    -------
    tuple[str, str, str, str | None]
        Parsed ``(region, instance_type, ami_id, cloud_init_b64)`` values.

    Raises
    ------
    ValueError
        If the task identifier is malformed or missing required parts.
    """
    expected_format_message = (
        "Invalid launch task ID format. Expected "
        "'<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>', "
        "or '<region>:<instance_type>:<ami_id>:<uuid4>'."
    )
    parts = taskid.split(":")
    if len(parts) == 4:
        region, instance_type, ami_id, task_uuid = parts
        cloud_init_b64 = None
    elif len(parts) == 5:
        region, instance_type, ami_id, cloud_init_b64, task_uuid = parts
    else:
        raise ValueError(expected_format_message)

    normalized_region = region.strip()
    normalized_instance_type = instance_type.strip().lower()
    normalized_ami_id = ami_id.strip().lower()
    normalized_cloud_init_b64 = (
        cloud_init_b64.strip() if cloud_init_b64 is not None else None
    )
    normalized_task_uuid = task_uuid.strip()
    if (
        not normalized_region
        or not normalized_instance_type
        or not normalized_ami_id
        or (cloud_init_b64 is not None and not normalized_cloud_init_b64)
        or not normalized_task_uuid
    ):
        raise ValueError(expected_format_message)

    try:
        uuid.UUID(normalized_task_uuid)
    except ValueError as exc:
        raise ValueError(expected_format_message) from exc

    return (
        normalized_region,
        normalized_instance_type,
        normalized_ami_id,
        normalized_cloud_init_b64,
    )


def _build_bench_task_id(
    launch_task_id: str,
    benchmark_kind: BenchmarkKind = BenchmarkKind.MD,
) -> str:
    """Build a benchmark task identifier that encodes benchmark kind.

    Parameters
    ----------
    launch_task_id : str
        Launch task identifier this bench task depends on.
    benchmark_kind : BenchmarkKind, default=BenchmarkKind.MD
        Benchmark workload kind to execute.

    Returns
    -------
    str
        Bench task identifier in ``bench:<benchmark_kind>:<launch_task_id>`` format.
    """
    return f"bench:{benchmark_kind.value}:{launch_task_id}"


def _parse_bench_task_id(taskid: str) -> tuple[BenchmarkKind, str]:
    """Parse a benchmark task identifier into kind and launch task ID.

    Parameters
    ----------
    taskid : str
        Bench task identifier in either ``bench:<benchmark_kind>:<launch_task_id>``
        or legacy ``bench:<launch_task_id>`` format.

    Returns
    -------
    tuple[BenchmarkKind, str]
        Parsed ``(benchmark_kind, launch_task_id)`` values.

    Raises
    ------
    ValueError
        If task identifier is malformed.
    """
    expected_format_message = (
        "Invalid bench task ID format. Expected "
        "'bench:<benchmark_kind>:<launch_task_id>' "
        "or legacy 'bench:<launch_task_id>'."
    )

    if not taskid.startswith("bench:"):
        raise ValueError(expected_format_message)

    remainder = taskid.removeprefix("bench:").strip()
    if not remainder:
        raise ValueError(expected_format_message)

    candidate_kind, separator, candidate_launch_task_id = remainder.partition(":")
    benchmark_kind = BenchmarkKind.MD
    launch_task_id = remainder
    if separator:
        try:
            benchmark_kind = _normalize_benchmark_kind(candidate_kind)
            launch_task_id = candidate_launch_task_id
        except ValueError:
            benchmark_kind = BenchmarkKind.MD
            launch_task_id = remainder

    normalized_launch_task_id = launch_task_id.strip()
    if not normalized_launch_task_id:
        raise ValueError(expected_format_message)

    _parse_launch_task_id(normalized_launch_task_id)
    return benchmark_kind, normalized_launch_task_id
