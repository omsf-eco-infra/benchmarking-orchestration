from __future__ import annotations

import json
import os
import shlex
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import sqlalchemy as sqla
from exorcist.models import TaskStatus

from ..task_id import _parse_brev_task_metadata
from ..tasks import TaskStatusDB
from .transport import BrevTransport

_REMOTE_WORKSPACE = "/home/ubuntu/workspace"
_REMOTE_CLI_PATH = f"{_REMOTE_WORKSPACE}/benchmarking-orchestration"
_REMOTE_JOBS_PATH = f"{_REMOTE_WORKSPACE}/jobs"
_DEFAULT_INPUT_NAME = "ross_dodecahedron_jacs.json"
_POLL_INTERVAL_SECONDS = 120
_HEARTBEAT_INTERVAL_SECONDS = 30


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


def _prepare_job_directory(
    staging_root: Path,
    benchmark_repo_path: Path,
    remote_job_id: str,
    profile: str,
    benchmark_kind: str,
    mps_process_count: int,
) -> Path:
    """Prepare one credentialless worker job and its required inputs.

    Parameters
    ----------
    staging_root : Path
        Temporary controller staging directory.
    benchmark_repo_path : Path
        Local performance benchmark repository.
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

    Raises
    ------
    ValueError
        If required benchmark inputs are absent.
    """
    benchmark_scripts = benchmark_repo_path / "benchmark"
    benchmark_input = benchmark_repo_path / "data" / _DEFAULT_INPUT_NAME
    if not benchmark_scripts.is_dir() or not benchmark_input.is_file():
        raise ValueError(
            "benchmark_repo_path must contain 'benchmark/' and "
            f"'data/{_DEFAULT_INPUT_NAME}'."
        )

    job_directory = staging_root / remote_job_id
    staged_repo = job_directory / "benchmark-repo"
    shutil.copytree(benchmark_scripts, staged_repo / "benchmark")
    (staged_repo / "data").mkdir()
    shutil.copy2(benchmark_input, staged_repo / "data" / benchmark_input.name)
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


def _detached_worker_command(remote_job_directory: str) -> str:
    """Build the detached worker command passed through ``brev exec``.

    Parameters
    ----------
    remote_job_directory : str
        Absolute credentialless job directory on the Brev instance.

    Returns
    -------
    str
        Remote shell command using ``nohup`` and complete stream redirection.
    """
    command = [
        "/home/ubuntu/.pixi/bin/pixi",
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
        f"nohup {shlex.join(command)} > {shlex.quote(worker_log)} 2>&1 "
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


def launch_brev_task(
    task_db: TaskStatusDB,
    benchmark_repo_path: Path,
    result_directory: Path,
    startup_script: Path,
    transport: BrevTransport | None = None,
) -> tuple[str, Path] | None:
    """Claim, dispatch, monitor, retrieve, and clean up one Brev task.

    Parameters
    ----------
    task_db : TaskStatusDB
        Trusted controller Exorcist database.
    benchmark_repo_path : Path
        Local benchmark repository supplying credentialless worker inputs.
    result_directory : Path
        Controller directory receiving the completed local result bundle.
    startup_script : Path
        Credentialless Brev startup script.
    transport : BrevTransport | None, optional
        Brev CLI transport, replaceable by a test double.

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
            _timeout_seconds,
            profile,
            instance_type,
            remote_job_id,
            instance_name,
        ) = _parse_brev_task_metadata(task)
    except Exception:
        task_db.mark_task_completed(task, success=False)
        raise

    transport = transport or BrevTransport()
    benchmark_repo_path = benchmark_repo_path.resolve()
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
                benchmark_repo_path,
                remote_job_id,
                profile,
                benchmark_kind.value,
                mps_process_count,
            )
            _transition(task_db, task, controller_path, controller_state, "creating")
            creation_attempted = True
            transport.create(instance_name, instance_type, startup_script)
            _transition(
                task_db, task, controller_path, controller_state, "instance_ready"
            )
            remote_job_directory = f"{_REMOTE_JOBS_PATH}/{remote_job_id}"
            transport.copy(
                job_directory,
                f"{instance_name}:{remote_job_directory}",
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

    return task, local_job_directory
