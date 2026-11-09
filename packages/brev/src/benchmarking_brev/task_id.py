"""Credential-free task encoding for Brev instance jobs."""

from __future__ import annotations

import math
import re
import uuid

from benchmarking_orchestration_core.benchmark_kind import (
    BenchmarkKind,
    _normalize_benchmark_kind,
)

_BREV_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


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
