from __future__ import annotations

import json
import os
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import s3fs
import sqlalchemy as sqla
from exorcist.models import TaskStatus

from ..bench import _build_result_s3_prefix
from ..task_id import _parse_brev_task_metadata
from ..tasks import TaskStatusDB
from .transport import BrevTransport

_REMOTE_WORKSPACE = "workspace"
_REMOTE_CLI_PATH = f"{_REMOTE_WORKSPACE}/benchmarking-orchestration"
_REMOTE_BENCH_REPO_PATH = f"{_REMOTE_WORKSPACE}/performance_benchmarks"
_REMOTE_JOBS_PATH = f"{_REMOTE_WORKSPACE}/jobs"
_DEFAULT_INPUT_NAME = "ross_dodecahedron_jacs.json"
_INSTANCE_READY_POLL_INTERVAL_SECONDS = 5
_POLL_INTERVAL_SECONDS = 30
_HEARTBEAT_INTERVAL_SECONDS = 30
_RESULT_MANIFEST_SCHEMA_VERSION = 4


def _touch_exorcist_task(task_db: TaskStatusDB, task: str) -> None:
    """Refresh Exorcist activity when persisted controller state changes.

    Parameters
    ----------
    task_db : TaskStatusDB
        Exorcist task database.
    task : str
        Claimed Brev task identifier.
    """
    statement = (
        sqla.update(task_db.tasks_table)
        .where(task_db.tasks_table.c.taskid == task)
        .where(task_db.tasks_table.c.status == TaskStatus.IN_PROGRESS.value)
        .values(last_modified=datetime.now())
    )
    with task_db.engine.begin() as connection:
        result = connection.execute(statement)
        if result.rowcount != 1:
            raise RuntimeError(f"Unable to persist activity for Brev task '{task}'.")


def _write_controller_state(path: Path, state: dict[str, Any]) -> None:
    """Atomically persist controller lifecycle details beside local results.

    Parameters
    ----------
    path : Path
        Controller marker destination.
    state : dict[str, Any]
        Current controller lifecycle record.
    """
    temporary_path = path.with_name(f"{path.name}.tmp")
    temporary_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(temporary_path, path)


def _transition(
    task_db: TaskStatusDB,
    task: str,
    controller_path: Path,
    state: dict[str, Any],
    lifecycle_state: str,
    **changes: Any,
) -> None:
    """Persist one controller transition and touch its Exorcist task.

    Parameters
    ----------
    task_db : TaskStatusDB
        Exorcist task database.
    task : str
        Claimed Brev task identifier.
    controller_path : Path
        Local durable controller marker.
    state : dict[str, Any]
        Mutable controller state payload.
    lifecycle_state : str
        New detailed lifecycle state.
    **changes : Any
        Additional controller fields to persist.
    """
    now = datetime.now(timezone.utc).isoformat()
    if state.get("lifecycle_state") != lifecycle_state:
        state.setdefault("transitions", []).append(
            {"state": lifecycle_state, "at_utc": now}
        )
    state.update(changes)
    state["lifecycle_state"] = lifecycle_state
    state["updated_at_utc"] = now
    _write_controller_state(controller_path, state)
    _touch_exorcist_task(task_db, task)
    heartbeat = changes.get("heartbeat_at_utc")
    heartbeat_detail = f" heartbeat={heartbeat}" if heartbeat else ""
    print(
        f"[brev] {state['instance_name']}: {lifecycle_state}{heartbeat_detail}",
        flush=True,
    )


def _prepare_job_directory(
    staging_root: Path,
    remote_job_id: str,
    profile: str,
    benchmark_kind: str,
    mps_process_count: int,
) -> Path:
    """Prepare one credentialless worker job specification.

    Parameters
    ----------
    staging_root : Path
        Temporary controller staging directory.
    remote_job_id : str
        Opaque worker job identifier.
    profile : str
        Explicit benchmark profile.
    benchmark_kind : str
        Single benchmark workload kind.
    mps_process_count : int
        Number of benchmark processes.

    Returns
    -------
    Path
        Prepared job directory.
    """
    job_directory = staging_root / remote_job_id
    job_directory.mkdir()
    payload = {
        "schema_version": 1,
        "job_id": remote_job_id,
        "benchmark_kind": benchmark_kind,
        "mps_process_count": mps_process_count,
        "profile": profile,
        "benchmark_repo_path": "benchmark-repo",
        "output_directory": "results",
    }
    (job_directory / "job.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    return job_directory


def _wait_for_instance_ready(
    transport: BrevTransport,
    instance_name: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Wait until Brev reports a healthy instance with a ready shell.

    Parameters
    ----------
    transport : BrevTransport
        Brev CLI transport.
    instance_name : str
        Created Brev instance name.
    timeout_seconds : float
        Maximum time to wait after ``brev create`` returns.

    Returns
    -------
    dict[str, Any]
        Ready Brev instance metadata.

    Raises
    ------
    RuntimeError
        If the instance disappears, stops, or does not become SSH-ready.
    """
    now = time.monotonic()
    deadline = now + timeout_seconds
    next_progress = now
    previous_statuses: tuple[str, str, str] | None = None
    last_ssh_error: str | None = None
    while True:
        instance = transport.inspect(instance_name)
        if instance is None:
            raise RuntimeError(f"Brev instance '{instance_name}' disappeared.")

        status = str(instance.get("status") or "").upper()
        shell_status = str(instance.get("shell_status") or "").upper()
        health_status = str(instance.get("health_status") or "").upper()
        statuses = status, shell_status, health_status
        now = time.monotonic()
        if statuses != previous_statuses or now >= next_progress:
            print(
                f"[brev] {instance_name}: waiting for SSH readiness "
                f"(status={status}, shell={shell_status}, health={health_status})",
                flush=True,
            )
            previous_statuses = statuses
            next_progress = now + _HEARTBEAT_INTERVAL_SECONDS
        if (
            status == "RUNNING"
            and shell_status == "READY"
            and health_status == "HEALTHY"
        ):
            try:
                transport.exec(instance_name, "true")
            except Exception as exc:
                last_ssh_error = str(exc).splitlines()[-1]
                print(
                    f"[brev] {instance_name}: SSH probe failed "
                    f"({last_ssh_error}); retrying",
                    flush=True,
                )
            else:
                print(f"[brev] {instance_name}: SSH connection ready", flush=True)
                return instance
        if status in {"DELETING", "ERROR", "FAILED", "FAILURE", "STOPPED", "STOPPING"}:
            raise RuntimeError(
                f"Brev instance '{instance_name}' entered {status!r} before its "
                "shell became ready."
            )

        remaining = deadline - now
        if remaining <= 0:
            ssh_detail = (
                f" Last SSH probe error: {last_ssh_error}." if last_ssh_error else ""
            )
            raise RuntimeError(
                f"Brev instance '{instance_name}' did not become SSH-ready within "
                f"{timeout_seconds:g} seconds (status={status!r}, "
                f"shell_status={shell_status!r}, health_status={health_status!r})."
                f"{ssh_detail}"
            )
        time.sleep(min(_INSTANCE_READY_POLL_INTERVAL_SECONDS, remaining))


def _detached_worker_command(remote_job_directory: str) -> str:
    """Build the detached worker command passed through ``brev exec``.

    Parameters
    ----------
    remote_job_directory : str
        Credentialless job directory relative to the Brev SSH user's home.

    Returns
    -------
    str
        Remote shell command using ``nohup`` and complete stream redirection.
    """
    command = [
        "run",
        "--manifest-path",
        f"{_REMOTE_CLI_PATH}/pyproject.toml",
        "-e",
        "bench",
        "python",
        "-m",
        "benchmarking_orchestration",
        "worker",
        "job",
        remote_job_directory,
        "--heartbeat-interval-seconds",
        str(_HEARTBEAT_INTERVAL_SECONDS),
    ]
    worker_log = f"{remote_job_directory}/worker.log"
    return (
        f'nohup "$HOME/.pixi/bin/pixi" {shlex.join(command)} '
        f"> {shlex.quote(worker_log)} 2>&1 "
        "< /dev/null & echo $!"
    )


def _read_remote_marker(
    transport: BrevTransport, instance_name: str, remote_job_directory: str
) -> tuple[str, dict[str, Any] | None]:
    """Read the durable completion marker or latest status marker.

    Parameters
    ----------
    transport : BrevTransport
        Brev CLI transport.
    instance_name : str
        Brev instance containing the job.
    remote_job_directory : str
        Absolute remote job directory.

    Returns
    -------
    tuple[str, dict[str, Any] | None]
        Marker kind and parsed JSON payload, or ``("pending", None)``.

    Raises
    ------
    RuntimeError
        If a marker is malformed or has an unexpected framing value.
    """
    complete_path = shlex.quote(f"{remote_job_directory}/complete.json")
    status_path = shlex.quote(f"{remote_job_directory}/status.json")
    command = (
        f"if test -f {complete_path}; then printf 'complete\\n'; cat {complete_path}; "
        f"elif test -f {status_path}; then printf 'status\\n'; cat {status_path}; "
        "else printf 'pending\\n'; fi"
    )
    output = transport.exec(instance_name, command)
    kind, separator, raw_payload = output.partition("\n")
    if kind == "pending" and not separator:
        return kind, None
    if kind not in {"status", "complete"} or not separator:
        raise RuntimeError(f"Unexpected remote marker response: {output!r}")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Remote {kind}.json is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Remote {kind}.json must contain a JSON object.")
    return kind, payload


def _parse_heartbeat(payload: dict[str, Any], remote_job_id: str) -> datetime:
    """Validate and parse a correlated worker heartbeat.

    Parameters
    ----------
    payload : dict[str, Any]
        Remote worker status marker.
    remote_job_id : str
        Expected opaque worker job identifier.

    Returns
    -------
    datetime
        Timezone-aware heartbeat timestamp.

    Raises
    ------
    RuntimeError
        If the marker belongs to another job or has an invalid heartbeat.
    """
    if payload.get("job_id") != remote_job_id:
        raise RuntimeError("Remote status.json job_id does not match the claimed task.")
    raw_heartbeat = payload.get("heartbeat_at_utc")
    if not isinstance(raw_heartbeat, str):
        raise RuntimeError("Remote status.json is missing heartbeat_at_utc.")
    try:
        heartbeat = datetime.fromisoformat(raw_heartbeat.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(
            "Remote status.json has an invalid heartbeat_at_utc."
        ) from exc
    if heartbeat.tzinfo is None:
        raise RuntimeError(
            "Remote status.json heartbeat_at_utc must include a timezone."
        )
    return heartbeat.astimezone(timezone.utc)


def _poll_remote_job(
    task_db: TaskStatusDB,
    task: str,
    controller_path: Path,
    controller_state: dict[str, Any],
    transport: BrevTransport,
    instance_name: str,
    remote_job_id: str,
    remote_job_directory: str,
) -> str | None:
    """Poll durable worker markers until successful completion.

    Parameters
    ----------
    task_db : TaskStatusDB
        Exorcist task database.
    task : str
        Claimed Brev task identifier.
    controller_path : Path
        Local durable controller marker.
    controller_state : dict[str, Any]
        Mutable detailed controller lifecycle state.
    transport : BrevTransport
        Brev CLI transport.
    instance_name : str
        Brev instance containing the worker.
    remote_job_id : str
        Expected worker job identifier.
    remote_job_directory : str
        Absolute remote job directory.

    Returns
    -------
    str | None
        Worker failure detail, or ``None`` after successful completion.

    Raises
    ------
    RuntimeError
        If the instance disappears or a durable marker is invalid.
    """
    while True:
        if transport.inspect(instance_name) is None:
            raise RuntimeError(f"Brev instance '{instance_name}' disappeared.")

        marker_kind, payload = _read_remote_marker(
            transport, instance_name, remote_job_directory
        )
        if marker_kind == "pending":
            print(f"[brev] {instance_name}: worker pending", flush=True)
        if marker_kind == "complete":
            assert payload is not None
            if payload.get("job_id") != remote_job_id:
                raise RuntimeError(
                    "Remote complete.json job_id does not match the claimed task."
                )
            success = payload.get("success")
            if not isinstance(success, bool):
                raise RuntimeError("Remote complete.json is missing boolean success.")
            if not success:
                detail = payload.get("error_message") or "unknown worker error"
                _transition(
                    task_db,
                    task,
                    controller_path,
                    controller_state,
                    "worker_failed",
                    failure_details=str(detail),
                )
                return str(detail)
            _transition(
                task_db,
                task,
                controller_path,
                controller_state,
                "worker_succeeded",
            )
            return None

        if marker_kind == "status":
            assert payload is not None
            heartbeat = _parse_heartbeat(payload, remote_job_id)
            worker_state = payload.get("state")
            if worker_state not in {"running", "succeeded", "failed"}:
                raise RuntimeError(
                    f"Remote status.json has unsupported state {worker_state!r}."
                )
            lifecycle_state = {
                "running": "running",
                "succeeded": "worker_succeeded",
                "failed": "worker_failed",
            }[worker_state]
            changes = {"heartbeat_at_utc": heartbeat.isoformat()}
            if worker_state == "failed":
                changes["failure_details"] = str(
                    payload.get("error_message") or "unknown worker error"
                )
            _transition(
                task_db,
                task,
                controller_path,
                controller_state,
                lifecycle_state,
                **changes,
            )

        time.sleep(_POLL_INTERVAL_SECONDS)


def _validate_retrieved_job(
    local_job_directory: Path,
    task: str,
    remote_job_id: str,
    instance_name: str,
    profile: str,
    benchmark_kind: str,
    mps_process_count: int,
) -> datetime:
    """Validate correlated worker markers, controller state, and artifacts.

    Parameters
    ----------
    local_job_directory : Path
        Retrieved local job directory.
    task : str
        Claimed Exorcist task identifier.
    remote_job_id : str
        Expected worker job identifier.
    instance_name : str
        Expected Brev instance name.
    profile : str
        Expected benchmark profile.
    benchmark_kind : str
        Expected benchmark workload kind.
    mps_process_count : int
        Expected benchmark process count.

    Returns
    -------
    datetime
        Validated benchmark start time used for the S3 date partition.

    Raises
    ------
    RuntimeError
        If any retrieved marker or artifact contract is invalid.
    """
    complete = json.loads(
        (local_job_directory / "complete.json").read_text(encoding="utf-8")
    )
    status = json.loads(
        (local_job_directory / "status.json").read_text(encoding="utf-8")
    )
    controller = json.loads(
        (local_job_directory / "controller.json").read_text(encoding="utf-8")
    )
    results = local_job_directory / "results"
    manifest = json.loads((results / "manifest.json").read_text(encoding="utf-8"))

    expected_complete = {
        "schema_version": 1,
        "job_id": remote_job_id,
        "profile": profile,
        "benchmark_kind": benchmark_kind,
        "mps_process_count": mps_process_count,
        "success": True,
        "error_message": None,
        "output_directory": "results",
    }
    for field, expected in expected_complete.items():
        if complete.get(field) != expected:
            raise RuntimeError(
                f"complete.json {field} does not match the claimed task."
            )
    if not isinstance(complete.get("gpu_provenance"), dict):
        raise RuntimeError("complete.json gpu_provenance must be a JSON object.")

    expected_status = {
        "schema_version": 1,
        "job_id": remote_job_id,
        "state": "succeeded",
        "error_message": None,
    }
    for field, expected in expected_status.items():
        if status.get(field) != expected:
            raise RuntimeError(f"status.json {field} does not match complete.json.")

    if not (
        complete.get("started_at_utc") == status.get("started_at_utc")
        and complete.get("completed_at_utc")
        == status.get("completed_at_utc")
        == status.get("heartbeat_at_utc")
    ):
        raise RuntimeError("status.json and complete.json timestamps are inconsistent.")

    expected_controller = {
        "schema_version": 1,
        "task_id": task,
        "instance_name": instance_name,
        "remote_job_id": remote_job_id,
        "attempt": 1,
        "lifecycle_state": "retrieved",
        "failure_details": None,
        "local_result_path": str(local_job_directory),
    }
    for field, expected in expected_controller.items():
        if controller.get(field) != expected:
            raise RuntimeError(
                f"controller.json {field} does not match the active controller state."
            )

    if manifest.get("schema_version") != _RESULT_MANIFEST_SCHEMA_VERSION:
        raise RuntimeError("results/manifest.json has an unsupported schema_version.")
    if manifest.get("benchmark_kind") != benchmark_kind:
        raise RuntimeError("results/manifest.json benchmark_kind does not match.")
    if manifest.get("mps_process_count") != mps_process_count:
        raise RuntimeError("results/manifest.json mps_process_count does not match.")
    if manifest.get("execution") != {"success": True, "error_message": None}:
        raise RuntimeError(
            "results/manifest.json does not record successful execution."
        )
    manifest_started = datetime.fromisoformat(
        manifest["timestamps"]["started_at_utc"].replace("Z", "+00:00")
    )
    manifest_completed = datetime.fromisoformat(
        manifest["timestamps"]["completed_at_utc"].replace("Z", "+00:00")
    )
    if (
        manifest_started.tzinfo is None
        or manifest_completed.tzinfo is None
        or manifest_started > manifest_completed
    ):
        raise RuntimeError("results/manifest.json timestamps are inconsistent.")

    required_artifacts = (
        results / "input" / _DEFAULT_INPUT_NAME,
        results / "output" / f"{benchmark_kind}_benchmark.out",
        results / "logs" / "stdout.log",
        results / "logs" / "stderr.log",
        results / "manifest.json",
    )
    if any(not artifact.is_file() for artifact in required_artifacts):
        raise RuntimeError("Retrieved result bundle is missing required artifacts.")
    if any(path.is_symlink() for path in results.rglob("*")):
        raise RuntimeError("Retrieved result bundle must not contain symbolic links.")
    return manifest_started


def _upload_results(
    result_directory: Path,
    s3_bucket: str,
    task: str,
    started_at: datetime,
    artifact_output: s3fs.S3FileSystem | None,
) -> str:
    """Upload a validated result bundle to the existing S3 layout.

    Parameters
    ----------
    result_directory : Path
        Validated local artifact directory.
    s3_bucket : str
        Bucket receiving the benchmark artifacts.
    task : str
        Claimed task identifier used for the hashed partition.
    started_at : datetime
        Validated benchmark start time used for the date partition.
    artifact_output : s3fs.S3FileSystem | None
        Optional filesystem override for tests.

    Returns
    -------
    str
        Uploaded date/hash prefix.

    Raises
    ------
    ValueError
        If the bucket name is empty.
    """
    bucket = s3_bucket.strip()
    if not bucket:
        raise ValueError("s3_bucket cannot be empty.")
    prefix = _build_result_s3_prefix(task, started_at)
    manifest_path = result_directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["s3_bucket"] = bucket
    manifest["s3_prefix"] = prefix
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    output = artifact_output if artifact_output is not None else s3fs.S3FileSystem()
    for artifact in sorted(result_directory.rglob("*")):
        if artifact.is_file():
            relative_path = artifact.relative_to(result_directory).as_posix()
            output.put(str(artifact), f"{bucket}/{prefix}/{relative_path}")
    return prefix


def launch_brev_task(
    task_db: TaskStatusDB,
    s3_bucket: str,
    result_directory: Path,
    startup_script: Path,
    transport: BrevTransport | None = None,
    artifact_output: s3fs.S3FileSystem | None = None,
) -> tuple[str, Path] | None:
    """Claim, dispatch, retrieve, upload, and finalize one Brev task.

    Parameters
    ----------
    task_db : TaskStatusDB
        Trusted controller Exorcist database.
    s3_bucket : str
        Bucket receiving validated benchmark artifacts.
    result_directory : Path
        Controller directory receiving the completed local result bundle.
    startup_script : Path
        Credentialless Brev startup script.
    transport : BrevTransport | None, optional
        Brev CLI transport, replaceable by a test double.
    artifact_output : s3fs.S3FileSystem | None, optional
        Artifact filesystem override used by credential-free tests.

    Returns
    -------
    tuple[str, Path] | None
        Claimed task and retrieved local job directory, or ``None`` when the
        queue is empty.

    Raises
    ------
    ValueError
        If task metadata or benchmark inputs are invalid.
    RuntimeError
        If Brev lifecycle, worker execution, retrieval, or cleanup fails.
    """
    task = task_db.check_out_task_with_capability("brev")
    if task is None:
        return None

    try:
        (
            benchmark_kind,
            mps_process_count,
            timeout_seconds,
            profile,
            instance_type,
            remote_job_id,
            instance_name,
        ) = _parse_brev_task_metadata(task)
    except Exception:
        task_db.mark_task_completed(task, success=False)
        raise

    transport = transport or BrevTransport()
    result_directory = result_directory.resolve()
    startup_script = startup_script.resolve()
    local_job_directory = result_directory / remote_job_id
    try:
        local_job_directory.mkdir(parents=True, exist_ok=False)
    except Exception:
        task_db.mark_task_completed(task, success=False)
        raise
    controller_path = local_job_directory / "controller.json"
    controller_state: dict[str, Any] = {
        "schema_version": 1,
        "task_id": task,
        "instance_name": instance_name,
        "remote_job_id": remote_job_id,
        "attempt": 1,
        "heartbeat_at_utc": None,
        "failure_details": None,
        "instance_cleaned_up": False,
        "transitions": [],
    }
    _transition(task_db, task, controller_path, controller_state, "claimed")
    creation_attempted = False
    failure: BaseException | None = None

    try:
        with TemporaryDirectory(prefix="brev-job-") as temporary_directory:
            job_directory = _prepare_job_directory(
                Path(temporary_directory),
                remote_job_id,
                profile,
                benchmark_kind.value,
                mps_process_count,
            )
            _transition(task_db, task, controller_path, controller_state, "creating")
            creation_attempted = True
            transport.create(instance_name, instance_type, startup_script)
            _wait_for_instance_ready(transport, instance_name, timeout_seconds)
            _transition(
                task_db, task, controller_path, controller_state, "instance_ready"
            )
            remote_job_directory = f"{_REMOTE_JOBS_PATH}/{remote_job_id}"
            transport.copy(
                job_directory,
                f"{instance_name}:{remote_job_directory}",
            )
            transport.exec(
                instance_name,
                f"mv {shlex.quote(_REMOTE_BENCH_REPO_PATH)} "
                f"{shlex.quote(f'{remote_job_directory}/benchmark-repo')}",
            )
        _transition(task_db, task, controller_path, controller_state, "staged")

        transport.exec(
            instance_name,
            _detached_worker_command(remote_job_directory),
        )
        _transition(task_db, task, controller_path, controller_state, "running")
        worker_failure = _poll_remote_job(
            task_db,
            task,
            controller_path,
            controller_state,
            transport,
            instance_name,
            remote_job_id,
            remote_job_directory,
        )

        transport.copy(
            f"{instance_name}:{remote_job_directory}/results",
            local_job_directory / "results",
        )
        transport.copy(
            f"{instance_name}:{remote_job_directory}/status.json",
            local_job_directory / "status.json",
        )
        transport.copy(
            f"{instance_name}:{remote_job_directory}/complete.json",
            local_job_directory / "complete.json",
        )
        _transition(
            task_db,
            task,
            controller_path,
            controller_state,
            "retrieved",
            local_result_path=str(local_job_directory),
        )
        if worker_failure is not None:
            raise RuntimeError(f"Brev worker failed: {worker_failure}")
        started_at = _validate_retrieved_job(
            local_job_directory,
            task,
            remote_job_id,
            instance_name,
            profile,
            benchmark_kind.value,
            mps_process_count,
        )
        _transition(task_db, task, controller_path, controller_state, "validated")
        s3_prefix = _upload_results(
            local_job_directory / "results",
            s3_bucket,
            task,
            started_at,
            artifact_output,
        )
        _transition(
            task_db,
            task,
            controller_path,
            controller_state,
            "uploaded",
            s3_bucket=s3_bucket.strip(),
            s3_prefix=s3_prefix,
        )
    except (Exception, KeyboardInterrupt) as exc:
        failure = exc

    cleanup_failure: Exception | None = None
    if creation_attempted:
        try:
            if transport.inspect(instance_name) is not None:
                transport.delete(instance_name)
            _transition(
                task_db,
                task,
                controller_path,
                controller_state,
                str(controller_state["lifecycle_state"]),
                instance_cleaned_up=True,
            )
        except Exception as exc:
            cleanup_failure = exc

    if failure is not None or cleanup_failure is not None:
        details = "; ".join(
            f"{type(error).__name__}: {error}"
            for error in (failure, cleanup_failure)
            if error is not None
        )
        _transition(
            task_db,
            task,
            controller_path,
            controller_state,
            "failed",
            failure_details=details,
        )
        try:
            task_db.mark_task_completed(task, success=False)
        except Exception as exc:
            details = f"{details}; task failure persistence failed: {exc}"
            controller_state["failure_details"] = details
            _write_controller_state(controller_path, controller_state)
        if isinstance(failure, KeyboardInterrupt):
            raise failure
        raise RuntimeError(f"Brev task '{task}' failed: {details}") from failure

    try:
        _transition(task_db, task, controller_path, controller_state, "finalized")
        task_db.mark_task_completed(task, success=True)
    except Exception as exc:
        details = f"task success persistence failed: {type(exc).__name__}: {exc}"
        now = datetime.now(timezone.utc).isoformat()
        controller_state.setdefault("transitions", []).append(
            {"state": "failed", "at_utc": now}
        )
        controller_state.update(
            lifecycle_state="failed",
            updated_at_utc=now,
            failure_details=details,
        )
        _write_controller_state(controller_path, controller_state)
        try:
            task_db.mark_task_completed(task, success=False)
        except Exception as mark_exc:
            controller_state["failure_details"] = (
                f"{details}; task failure persistence failed: {mark_exc}"
            )
            _write_controller_state(controller_path, controller_state)
        raise RuntimeError(f"Brev task '{task}' failed: {details}") from exc

    return task, local_job_directory
