from __future__ import annotations

import math
import re
import uuid

from .benchmark_kind import BenchmarkKind, _normalize_benchmark_kind
from benchmarking_orchestration.aws import DEFAULT_LAUNCH_AMI_ID

_BREV_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


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


def _normalize_brev_identifier(value: str, field_name: str) -> str:
    """Normalize a portable Brev task identifier field.

    Parameters
    ----------
    value : str
        Candidate field value.
    field_name : str
        Field name used in validation errors.

    Returns
    -------
    str
        Normalized identifier.

    Raises
    ------
    ValueError
        If the value cannot be safely encoded in a task ID.
    """
    normalized = value.strip() if isinstance(value, str) else ""
    if _BREV_IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ValueError(
            f"{field_name} must be 1-128 characters using letters, numbers, '.', '_', or '-'."
        )
    return normalized


def _build_brev_task_id(
    instance_type: str,
    profile: str,
    benchmark_kind: BenchmarkKind,
    mps_process_count: int,
    timeout_seconds: float,
) -> str:
    """Build a self-contained Brev benchmark task identifier.

    Parameters
    ----------
    instance_type : str
        Explicit Brev instance type.
    profile : str
        Explicit benchmark profile.
    benchmark_kind : BenchmarkKind
        Single benchmark workload kind.
    mps_process_count : int
        Number of worker benchmark processes.
    timeout_seconds : float
        Maximum wait for Brev SSH readiness after instance creation.

    Returns
    -------
    str
        Brev task ID containing only credential-free execution metadata.

    Raises
    ------
    ValueError
        If any task metadata is invalid.
    """
    normalized_instance_type = _normalize_brev_identifier(
        instance_type, "instance_type"
    )
    normalized_profile = _normalize_brev_identifier(profile, "profile")
    if benchmark_kind is BenchmarkKind.BOTH:
        raise ValueError("benchmark_kind must identify one workload, not 'both'.")
    if (
        isinstance(mps_process_count, bool)
        or not isinstance(mps_process_count, int)
        or mps_process_count < 1
    ):
        raise ValueError(
            "mps_process_count must be an integer greater than or equal to 1."
        )
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ValueError("timeout_seconds must be a finite number greater than zero.")

    identifier = str(uuid.uuid4())
    return ":".join(
        (
            "brev",
            benchmark_kind.value,
            str(mps_process_count),
            format(timeout_seconds, "g"),
            normalized_profile,
            normalized_instance_type,
            f"job-{identifier}",
            f"brev-{identifier}",
        )
    )


def _parse_brev_task_metadata(
    taskid: str,
) -> tuple[BenchmarkKind, int, float, str, str, str, str]:
    """Parse execution metadata from a Brev task identifier.

    Parameters
    ----------
    taskid : str
        Self-contained Brev task identifier.

    Returns
    -------
    tuple[BenchmarkKind, int, float, str, str, str, str]
        Benchmark kind, MPS count, timeout, profile, instance type, remote job
        ID, and instance name.

    Raises
    ------
    ValueError
        If the task identifier is malformed.
    """
    expected = (
        "Invalid Brev task ID format. Expected "
        "'brev:<benchmark_kind>:<mps_count>:<timeout_seconds>:<profile>:"
        "<instance_type>:<remote_job_id>:<instance_name>'."
    )
    parts = taskid.split(":")
    if len(parts) != 8 or parts[0] != "brev":
        raise ValueError(expected)
    try:
        benchmark_kind = _normalize_benchmark_kind(parts[1])
        if benchmark_kind is BenchmarkKind.BOTH:
            raise ValueError(expected)
        mps_process_count = int(parts[2])
        timeout_seconds = float(parts[3])
        profile = _normalize_brev_identifier(parts[4], "profile")
        instance_type = _normalize_brev_identifier(parts[5], "instance_type")
        remote_job_id, instance_name = parts[6:]
        if not remote_job_id.startswith("job-"):
            raise ValueError(expected)
        identifier = remote_job_id.removeprefix("job-")
        uuid.UUID(identifier)
        if instance_name != f"brev-{identifier}":
            raise ValueError(expected)
        if (
            mps_process_count < 1
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError(expected)
    except (TypeError, ValueError) as exc:
        raise ValueError(expected) from exc
    return (
        benchmark_kind,
        mps_process_count,
        timeout_seconds,
        profile,
        instance_type,
        remote_job_id,
        instance_name,
    )


def _build_bench_task_id(
    launch_task_id: str,
    benchmark_kind: BenchmarkKind = BenchmarkKind.MD,
    mps_process_count: int = 1,
) -> str:
    """Build a benchmark task identifier that encodes benchmark kind.

    Parameters
    ----------
    launch_task_id : str
        Launch task identifier this bench task depends on.
    benchmark_kind : BenchmarkKind, default=BenchmarkKind.MD
        Benchmark workload kind to execute.
    mps_process_count : int, default=1
        Number of MPS benchmark subprocesses encoded in the task identifier.

    Returns
    -------
    str
        Bench task identifier in ``bench:<benchmark_kind>:<launch_task_id>`` format
        when ``mps_process_count`` is ``1``, otherwise
        ``bench:<benchmark_kind>:mps:<mps_process_count>:<launch_task_id>``.

    Raises
    ------
    ValueError
        If ``mps_process_count`` is less than ``1``.
    """
    if mps_process_count < 1:
        raise ValueError("mps_process_count must be greater than or equal to 1.")

    if mps_process_count == 1:
        return f"bench:{benchmark_kind.value}:{launch_task_id}"

    return f"bench:{benchmark_kind.value}:mps:{mps_process_count}:{launch_task_id}"


def _parse_bench_task_metadata(taskid: str) -> tuple[BenchmarkKind, int, str]:
    """Parse a benchmark task identifier into full execution metadata.

    Parameters
    ----------
    taskid : str
        Bench task identifier in ``bench:<benchmark_kind>:<launch_task_id>`` format,
        or ``bench:<benchmark_kind>:mps:<mps_process_count>:<launch_task_id>``
        format.

    Returns
    -------
    tuple[BenchmarkKind, int, str]
        Parsed ``(benchmark_kind, mps_process_count, launch_task_id)`` values.

    Raises
    ------
    ValueError
        If task identifier is malformed.
    """
    expected_format_message = (
        "Invalid bench task ID format. Expected "
        "'bench:<benchmark_kind>:<launch_task_id>' "
        "or 'bench:<benchmark_kind>:mps:<mps_process_count>:<launch_task_id>' "
        "where <benchmark_kind> is one of: md, rbfe."
    )

    if not taskid.startswith("bench:"):
        raise ValueError(expected_format_message)

    remainder = taskid.removeprefix("bench:").strip()
    if not remainder:
        raise ValueError(expected_format_message)

    candidate_kind, separator, trailing_metadata = remainder.partition(":")
    if not separator:
        raise ValueError(expected_format_message)

    try:
        benchmark_kind = _normalize_benchmark_kind(candidate_kind)
    except ValueError as exc:
        raise ValueError(expected_format_message) from exc

    launch_task_id = trailing_metadata
    mps_process_count = 1
    if trailing_metadata.startswith("mps:"):
        count_and_launch = trailing_metadata.removeprefix("mps:")
        count_text, nested_separator, candidate_launch_task_id = (
            count_and_launch.partition(":")
        )
        if not nested_separator or not count_text.strip():
            raise ValueError(expected_format_message)
        try:
            mps_process_count = int(count_text)
        except ValueError as exc:
            raise ValueError(expected_format_message) from exc
        if mps_process_count < 1:
            raise ValueError(expected_format_message)
        launch_task_id = candidate_launch_task_id

    normalized_launch_task_id = launch_task_id.strip()
    if not normalized_launch_task_id:
        raise ValueError(expected_format_message)

    _parse_launch_task_id(normalized_launch_task_id)
    return benchmark_kind, mps_process_count, normalized_launch_task_id


def _parse_bench_task_id(taskid: str) -> tuple[BenchmarkKind, str]:
    """Parse a benchmark task identifier into kind and launch task ID.

    Parameters
    ----------
    taskid : str
        Bench task identifier in ``bench:<benchmark_kind>:<launch_task_id>`` format,
        or ``bench:<benchmark_kind>:mps:<mps_process_count>:<launch_task_id>``
        format.

    Returns
    -------
    tuple[BenchmarkKind, str]
        Parsed ``(benchmark_kind, launch_task_id)`` values.

    Raises
    ------
    ValueError
        If task identifier is malformed.
    """
    benchmark_kind, _mps_process_count, launch_task_id = _parse_bench_task_metadata(
        taskid
    )
    return benchmark_kind, launch_task_id
