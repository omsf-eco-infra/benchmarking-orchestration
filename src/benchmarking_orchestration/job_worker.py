from __future__ import annotations

import json
import os
import re
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .bench import run_local_benchmark
from .benchmark_kind import BenchmarkKind, _normalize_benchmark_kind

_JOB_SCHEMA_VERSION = 1
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


@dataclass(frozen=True)
class JobSpecification:
    """Validated local benchmark job specification."""

    job_id: str
    benchmark_kind: BenchmarkKind
    mps_process_count: int
    profile: str
    benchmark_repo_path: Path
    output_directory: Path


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Atomically replace a JSON marker file.

    Parameters
    ----------
    path : Path
        Destination marker path.
    payload : dict[str, Any]
        JSON object to write.
    """
    temporary_path = path.with_name(f"{path.name}.tmp")
    temporary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary_path, path)


def _validate_identifier(value: object, field_name: str) -> str:
    """Validate a portable opaque identifier.

    Parameters
    ----------
    value : object
        Candidate identifier.
    field_name : str
        Field name used in validation errors.

    Returns
    -------
    str
        Validated identifier.

    Raises
    ------
    ValueError
        If the identifier is not a safe non-empty string.
    """
    if not isinstance(value, str) or _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"{field_name} must be 1-128 characters using letters, numbers, '.', '_', or '-'."
        )
    return value


def _resolve_job_path(
    job_directory: Path,
    value: object,
    field_name: str,
    *,
    must_exist: bool,
) -> Path:
    """Resolve and validate a path contained by a job directory.

    Parameters
    ----------
    job_directory : Path
        Resolved job directory.
    value : object
        Relative path from the job specification.
    field_name : str
        Field name used in validation errors.
    must_exist : bool
        Whether the resolved path must already be a directory.

    Returns
    -------
    Path
        Resolved path within ``job_directory``.

    Raises
    ------
    ValueError
        If the path is invalid, escapes the job directory, or has the wrong
        filesystem type.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty relative path.")
    raw_path = Path(value)
    if raw_path.is_absolute():
        raise ValueError(f"{field_name} must be relative to the job directory.")
    resolved_path = (job_directory / raw_path).resolve()
    if resolved_path == job_directory or job_directory not in resolved_path.parents:
        raise ValueError(f"{field_name} must stay within the job directory.")
    if must_exist and not resolved_path.is_dir():
        raise ValueError(f"{field_name} must identify an existing directory.")
    if not must_exist and resolved_path.exists() and not resolved_path.is_dir():
        raise ValueError(f"{field_name} must identify a directory.")
    return resolved_path


def load_job_specification(job_directory: Path) -> JobSpecification:
    """Load and validate ``job.json`` from a local job directory.

    Parameters
    ----------
    job_directory : Path
        Directory containing the job specification and local inputs.

    Returns
    -------
    JobSpecification
        Validated benchmark job.

    Raises
    ------
    ValueError
        If the directory or job specification is invalid.
    """
    job_directory = job_directory.resolve()
    if not job_directory.is_dir():
        raise ValueError("job_directory must identify an existing directory.")
    job_path = job_directory / "job.json"
    if not job_path.is_file():
        raise ValueError("job_directory must contain a job.json file.")
    try:
        payload = json.loads(job_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unable to read valid JSON from '{job_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("job.json must contain a JSON object.")
    if payload.get("schema_version") != _JOB_SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {_JOB_SCHEMA_VERSION}.")

    job_id = _validate_identifier(payload.get("job_id"), "job_id")
    profile = _validate_identifier(payload.get("profile"), "profile")
    raw_kind = payload.get("benchmark_kind")
    if not isinstance(raw_kind, str):
        raise ValueError("benchmark_kind must be a string.")
    benchmark_kind = _normalize_benchmark_kind(raw_kind)
    if benchmark_kind is BenchmarkKind.BOTH:
        raise ValueError("benchmark_kind must identify one workload, not 'both'.")
    mps_process_count = payload.get("mps_process_count")
    if (
        isinstance(mps_process_count, bool)
        or not isinstance(mps_process_count, int)
        or mps_process_count < 1
    ):
        raise ValueError(
            "mps_process_count must be an integer greater than or equal to 1."
        )

    benchmark_repo_path = _resolve_job_path(
        job_directory,
        payload.get("benchmark_repo_path"),
        "benchmark_repo_path",
        must_exist=True,
    )
    output_directory = _resolve_job_path(
        job_directory,
        payload.get("output_directory"),
        "output_directory",
        must_exist=False,
    )
    if (
        benchmark_repo_path == output_directory
        or benchmark_repo_path in output_directory.parents
        or output_directory in benchmark_repo_path.parents
    ):
        raise ValueError("benchmark_repo_path and output_directory must not overlap.")

    return JobSpecification(
        job_id=job_id,
        benchmark_kind=benchmark_kind,
        mps_process_count=mps_process_count,
        profile=profile,
        benchmark_repo_path=benchmark_repo_path,
        output_directory=output_directory,
    )


def _gpu_provenance() -> dict[str, Any]:
    """Collect runtime GPU details with ``nvidia-smi`` when available.

    Returns
    -------
    dict[str, Any]
        Availability, GPU records, and any collection error.
    """
    command = [
        "nvidia-smi",
        "--query-gpu=name,uuid,driver_version",
        "--format=csv,noheader,nounits",
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "gpus": [], "error": str(exc)}
    if result.returncode != 0:
        return {
            "available": False,
            "gpus": [],
            "error": result.stderr.strip()
            or f"nvidia-smi exited with {result.returncode}",
        }
    gpus = []
    for line in result.stdout.splitlines():
        fields = [field.strip() for field in line.split(",", maxsplit=2)]
        if len(fields) == 3:
            gpus.append(dict(zip(("name", "uuid", "driver_version"), fields)))
    return {"available": True, "gpus": gpus, "error": None}


def _write_heartbeats(
    stop_event: threading.Event,
    interval_seconds: float,
    status_path: Path,
    status: dict[str, Any],
    errors: list[Exception],
) -> None:
    """Write running status updates until benchmark execution finishes.

    Parameters
    ----------
    stop_event : threading.Event
        Event indicating that benchmark execution has finished.
    interval_seconds : float
        Delay between heartbeat updates.
    status_path : Path
        Atomic status marker destination.
    status : dict[str, Any]
        Base running status payload.
    errors : list[Exception]
        Shared collection receiving a heartbeat write failure.
    """
    while not stop_event.wait(interval_seconds):
        try:
            _write_json_atomic(
                status_path,
                {
                    **status,
                    "heartbeat_at_utc": datetime.now(timezone.utc).isoformat(),
                },
            )
        except Exception as exc:
            errors.append(exc)
            return


def run_job_worker(
    job_directory: Path,
    heartbeat_interval_seconds: float = 30.0,
) -> None:
    """Execute one credentialless local benchmark job.

    Parameters
    ----------
    job_directory : Path
        Directory containing ``job.json`` and local benchmark inputs.
    heartbeat_interval_seconds : float, default=30.0
        Seconds between atomic running-status heartbeat updates.

    Raises
    ------
    ValueError
        If the job or heartbeat interval is invalid.
    RuntimeError
        If benchmark execution or heartbeat persistence fails.
    """
    if heartbeat_interval_seconds <= 0:
        raise ValueError("heartbeat_interval_seconds must be greater than zero.")
    job_directory = job_directory.resolve()
    if not job_directory.is_dir():
        raise ValueError("job_directory must identify an existing directory.")
    complete_path = job_directory / "complete.json"
    complete_path.unlink(missing_ok=True)
    specification = load_job_specification(job_directory)
    status_path = job_directory / "status.json"

    started_at = datetime.now(timezone.utc).isoformat()
    running_status = {
        "schema_version": _JOB_SCHEMA_VERSION,
        "job_id": specification.job_id,
        "state": "running",
        "started_at_utc": started_at,
    }
    _write_json_atomic(status_path, {**running_status, "heartbeat_at_utc": started_at})
    stop_event = threading.Event()
    heartbeat_errors: list[Exception] = []
    heartbeat_thread = threading.Thread(
        target=_write_heartbeats,
        args=(
            stop_event,
            heartbeat_interval_seconds,
            status_path,
            running_status,
            heartbeat_errors,
        ),
        daemon=True,
    )
    heartbeat_thread.start()

    execution_error: Exception | None = None
    gpu_provenance: dict[str, Any] = {}
    try:
        gpu_provenance = _gpu_provenance()
        run_local_benchmark(
            specification.benchmark_repo_path,
            specification.output_directory,
            specification.benchmark_kind,
            specification.mps_process_count,
        )
    except Exception as exc:
        execution_error = exc
    finally:
        stop_event.set()
        heartbeat_thread.join()
    if heartbeat_errors and execution_error is None:
        execution_error = heartbeat_errors[0]

    completed_at = datetime.now(timezone.utc).isoformat()
    success = execution_error is None
    error_message = str(execution_error) if execution_error else None
    _write_json_atomic(
        status_path,
        {
            **running_status,
            "state": "succeeded" if success else "failed",
            "heartbeat_at_utc": completed_at,
            "completed_at_utc": completed_at,
            "error_message": error_message,
        },
    )
    _write_json_atomic(
        complete_path,
        {
            "schema_version": _JOB_SCHEMA_VERSION,
            "job_id": specification.job_id,
            "profile": specification.profile,
            "benchmark_kind": specification.benchmark_kind.value,
            "mps_process_count": specification.mps_process_count,
            "success": success,
            "error_message": error_message,
            "output_directory": specification.output_directory.relative_to(
                job_directory
            ).as_posix(),
            "gpu_provenance": gpu_provenance,
            "started_at_utc": started_at,
            "completed_at_utc": completed_at,
        },
    )
    if execution_error is not None:
        raise RuntimeError(
            f"Benchmark job failed: {execution_error}"
        ) from execution_error
