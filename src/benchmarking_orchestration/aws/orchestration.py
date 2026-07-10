"""Reusable AWS task orchestration without CLI side effects."""

from __future__ import annotations

import time as _time
from collections.abc import Mapping as _Mapping
from pathlib import Path as _Path

from exorcist.taskdb import _logger

from .. import cloud_init as _cloud_init
from .. import task_id as _task_id
from ..benchmark_kind import BenchmarkKind as _BenchmarkKind
from ..capabilities import (
    WorkerCapability as _WorkerCapability,
    _resolve_bench_worker_capability,
)
from ..tasks import TaskStatusDB as _TaskStatusDB
from . import (
    get_instance_type_vcpu_count as _get_instance_type_vcpu_count,
    get_launch_ami_details as _get_launch_ami_details,
    get_ondemand_g_vcpu_quota as _get_ondemand_g_vcpu_quota,
    get_ondemand_g_vcpus_used as _get_ondemand_g_vcpus_used,
    launch_ec2_instance as _launch_ec2_instance,
    validate_launch_instance_type as _validate_launch_instance_type,
)

_CAPACITY_RETRY_SLEEP_SECONDS = 60 * 15
_WIGGLE_ROOM = 8


def _run_benchmark(**kwargs) -> None:
    """Load and run the benchmark implementation only when needed.

    Parameters
    ----------
    **kwargs
        Explicit benchmark runtime arguments.
    """
    from ..bench import run_benchmark

    run_benchmark(**kwargs)


def _validate_expected_ami(task_ami_id: str, expected_ami_id: str | None) -> None:
    """Validate a task AMI against an explicitly approved AMI.

    Parameters
    ----------
    task_ami_id : str
        AMI identifier embedded in the launch task.
    expected_ami_id : str | None
        Approved AMI identifier, or ``None`` to allow the task AMI.

    Raises
    ------
    RuntimeError
        If the task and approved AMI identifiers differ.
    """
    if expected_ami_id is None:
        return

    if task_ami_id != expected_ami_id.strip().lower():
        raise RuntimeError(
            "Queued launch task AMI does not match the approved AMI. "
            f"Task AMI: '{task_ami_id}'. Approved AMI: '{expected_ami_id}'."
        )


def _wait_for_ondemand_g_vcpu_quota(task: str, instance_type: str) -> None:
    """Wait until On-Demand G/VT quota can accommodate a launch.

    Parameters
    ----------
    task : str
        Launch task identifier.
    instance_type : str
        EC2 instance type requested by the task.
    """
    needed_vcpus = _get_instance_type_vcpu_count(instance_type)
    quota = _get_ondemand_g_vcpu_quota()
    used = _get_ondemand_g_vcpus_used()
    available = max(quota - used - _WIGGLE_ROOM, 0)
    if needed_vcpus <= available:
        return

    _logger.warning(
        f"Insufficient vCPU quota to launch '{task}': "
        f"needed={needed_vcpus}, available={available} "
        f"(quota={quota}, used={used}). Waiting for capacity..."
    )
    while needed_vcpus > available:
        _time.sleep(_CAPACITY_RETRY_SLEEP_SECONDS)
        quota = _get_ondemand_g_vcpu_quota()
        used = _get_ondemand_g_vcpus_used()
        available = max(quota - used - _WIGGLE_ROOM, 0)


def _is_insufficient_instance_capacity_error(exc: BaseException) -> bool:
    """Return whether an exception represents EC2 capacity exhaustion.

    Parameters
    ----------
    exc : BaseException
        Exception raised during EC2 launch.

    Returns
    -------
    bool
        Whether the error code is ``InsufficientInstanceCapacity``.
    """
    if "InsufficientInstanceCapacity" in str(exc):
        return True

    cause = getattr(exc, "__cause__", None)
    response = getattr(cause, "response", {}) if cause is not None else {}
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return error.get("Code") == "InsufficientInstanceCapacity"


def _launch_with_capacity_retry(
    task: str,
    instance_type: str,
    ami_id: str,
    region: str,
    user_data: str | None,
    key_name: str | None,
    instance_profile_name: str | None,
) -> str:
    """Launch an instance after quota waits and retry capacity failures.

    Parameters
    ----------
    task : str
        Launch task identifier.
    instance_type : str
        EC2 instance type.
    ami_id : str
        AMI identifier.
    region : str
        AWS region.
    user_data : str | None
        Optional EC2 user data.
    key_name : str | None
        Optional EC2 key pair name.
    instance_profile_name : str | None
        Optional IAM instance profile name.

    Returns
    -------
    str
        Launched EC2 instance identifier.
    """
    while True:
        _wait_for_ondemand_g_vcpu_quota(task, instance_type)
        try:
            return _launch_ec2_instance(
                instance_type,
                ami_id=ami_id,
                region=region,
                user_data=user_data,
                key_name=key_name,
                instance_profile_name=instance_profile_name,
            )
        except Exception as exc:
            if not _is_insufficient_instance_capacity_error(exc):
                raise
            _logger.warning(
                f"Insufficient EC2 instance capacity while launching '{task}'. "
                f"Retrying in {_CAPACITY_RETRY_SLEEP_SECONDS} seconds. "
                f"Original error: {exc}"
            )
            _time.sleep(_CAPACITY_RETRY_SLEEP_SECONDS)


def queue_aws_tasks(
    task_db: _TaskStatusDB,
    instance_type: str,
    region: str,
    ami_id: str,
    benchmark_kind: _BenchmarkKind,
    mps_process_count: int,
    *,
    cloud_init_file: _Path | None = None,
    cloud_init_template_values: _Mapping[str, str] | None = None,
) -> list[tuple[str, str]]:
    """Validate inputs and queue AWS launch and benchmark task pairs.

    Parameters
    ----------
    task_db : TaskStatusDB
        Database in which tasks are created.
    instance_type : str
        EC2 instance type to validate and schedule.
    region : str
        AWS region for validation and launch metadata.
    ami_id : str
        AMI identifier to validate and schedule.
    benchmark_kind : BenchmarkKind
        Benchmark kind to schedule, including ``both``.
    mps_process_count : int
        Number of benchmark subprocesses encoded in each benchmark task.
    cloud_init_file : Path | None, optional
        Optional cloud-init template path.
    cloud_init_template_values : Mapping[str, str] | None, optional
        Explicit values available to the cloud-init template.

    Returns
    -------
    list[tuple[str, str]]
        Created ``(launch_task_id, benchmark_task_id)`` pairs.

    Raises
    ------
    ValueError
        If a required value is empty or the MPS process count is invalid.
    RuntimeError
        If AWS validation fails.
    """
    normalized_instance_type = instance_type.strip().lower()
    normalized_region = region.strip()
    normalized_ami_id = ami_id.strip().lower()
    if not normalized_region:
        raise ValueError("region cannot be empty.")
    if not normalized_ami_id:
        raise ValueError("ami id cannot be empty.")
    if mps_process_count < 1:
        raise ValueError("mps_process_count must be greater than or equal to 1.")

    _validate_launch_instance_type(normalized_instance_type, normalized_region)
    _get_launch_ami_details(normalized_ami_id, normalized_region)
    capability = _resolve_bench_worker_capability(normalized_instance_type)
    template_values = dict(cloud_init_template_values or {})
    template_values["GPU_CAPABILITY"] = capability.value
    cloud_init_b64 = _cloud_init._read_cloud_init_file_as_base64(
        cloud_init_file,
        template_values=template_values,
    )

    kinds = (
        (_BenchmarkKind.MD, _BenchmarkKind.RBFE)
        if benchmark_kind is _BenchmarkKind.BOTH
        else (benchmark_kind,)
    )
    task_pairs = []
    for kind in kinds:
        launch_task = _task_id._build_task_id(
            normalized_region,
            normalized_instance_type,
            normalized_ami_id,
            cloud_init_b64=cloud_init_b64,
        )
        benchmark_task = _task_id._build_bench_task_id(
            launch_task,
            kind,
            mps_process_count=mps_process_count,
        )
        task_db.add_task_with_capability(
            taskid=launch_task,
            requirements=[],
            max_tries=1,
            capability=_WorkerCapability.LAUNCH.value,
        )
        task_db.add_task_with_capability(
            taskid=benchmark_task,
            requirements=[launch_task],
            max_tries=1,
            capability=capability.value,
        )
        task_pairs.append((launch_task, benchmark_task))

    return task_pairs


def process_aws_launch_task(
    task_db: _TaskStatusDB,
    task: str,
    *,
    expected_ami_id: str | None = None,
    key_name: str | None = None,
    instance_profile_name: str | None = None,
    retry_for_capacity: bool = False,
) -> str:
    """Process one checked-out AWS launch task.

    Parameters
    ----------
    task_db : TaskStatusDB
        Database used to record task completion.
    task : str
        Checked-out launch task identifier.
    expected_ami_id : str | None, optional
        Explicit approved AMI identifier.
    key_name : str | None, optional
        Optional EC2 key pair name.
    instance_profile_name : str | None, optional
        Optional IAM instance profile name.
    retry_for_capacity : bool, default=False
        Whether to wait for quota and retry EC2 capacity errors.

    Returns
    -------
    str
        Launched EC2 instance identifier.
    """
    try:
        region, instance_type, ami_id, cloud_init_b64 = _task_id._parse_launch_task_id(
            task
        )
        _validate_expected_ami(ami_id, expected_ami_id)
        user_data = (
            _cloud_init._decode_cloud_init_base64(cloud_init_b64)
            if cloud_init_b64 is not None
            else None
        )
        instance_id = (
            _launch_with_capacity_retry(
                task,
                instance_type,
                ami_id,
                region,
                user_data,
                key_name,
                instance_profile_name,
            )
            if retry_for_capacity
            else _launch_ec2_instance(
                instance_type,
                ami_id=ami_id,
                region=region,
                user_data=user_data,
                key_name=key_name,
                instance_profile_name=instance_profile_name,
            )
        )
    except Exception:
        task_db.mark_task_completed(task, success=False)
        raise

    task_db.mark_task_completed(task, success=True)
    return instance_id


def process_aws_benchmark_task(
    task_db: _TaskStatusDB,
    task: str,
    *,
    benchmark_repo_path: _Path,
    s3_bucket: str,
) -> tuple[str, _BenchmarkKind]:
    """Process one checked-out AWS benchmark task.

    Parameters
    ----------
    task_db : TaskStatusDB
        Database used to record task completion.
    task : str
        Checked-out benchmark task identifier.
    benchmark_repo_path : Path
        Path to the benchmark repository.
    s3_bucket : str
        S3 bucket for benchmark artifacts.

    Returns
    -------
    tuple[str, BenchmarkKind]
        Processed task identifier and benchmark kind.

    Raises
    ------
    ValueError
        If task parsing, benchmark execution, or completion recording fails.
    """
    try:
        benchmark_kind, mps_process_count, _launch_task_id = (
            _task_id._parse_bench_task_metadata(task)
        )
    except ValueError:
        task_db.mark_task_completed(task, success=False)
        raise

    try:
        _run_benchmark(
            benchmark_repo_path=benchmark_repo_path,
            s3_bucket=s3_bucket,
            task_id=task,
            benchmark_kind=benchmark_kind,
            mps_process_count=mps_process_count,
        )
    except Exception as exc:
        try:
            task_db.mark_task_completed(task, success=False)
        except Exception as mark_exc:
            raise ValueError(
                f"Bench task '{task}' failed and could not be marked as failed "
                f"in database '{task_db}': {mark_exc}. Original error: {exc}"
            ) from exc
        raise ValueError(f"Bench task '{task}' failed: {exc}") from exc

    try:
        task_db.mark_task_completed(task, success=True)
    except Exception as exc:
        raise ValueError(
            f"Bench task '{task}' completed but could not be marked as succeeded "
            f"in database '{task_db}': {exc}"
        ) from exc

    return task, benchmark_kind


__all__ = [
    "process_aws_benchmark_task",
    "process_aws_launch_task",
    "queue_aws_tasks",
]
