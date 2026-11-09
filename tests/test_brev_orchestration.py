from __future__ import annotations

import builtins
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import fsspec
import pytest
import s3fs
import sqlalchemy as sqla
from exorcist.models import TaskStatus

import benchmarking_orchestration.brev.orchestration as orchestration
from benchmarking_orchestration_core.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.brev import launch_brev_task, queue_brev_tasks
from benchmarking_brev import BrevTransport
from benchmarking_brev.task_id import _parse_brev_task_metadata
from benchmarking_orchestration.tasks import TaskStatusDB


class _FakeTransport:
    """Record Brev operations and return controlled durable markers."""

    def __init__(
        self,
        markers: list[str],
        inspect_results: list[dict[str, Any] | None] | None = None,
        retrieved_overrides: dict[str, dict[str, Any]] | None = None,
        copy_failure: str | None = None,
        ssh_probe_failures: int = 0,
    ) -> None:
        """Configure marker and optional instance inspection results.

        Parameters
        ----------
        markers : list[str]
            Framed marker responses returned by remote exec calls.
        inspect_results : list[dict[str, Any] | None] | None, optional
            Ordered instance inspection results.
        retrieved_overrides : dict[str, dict[str, Any]] | None, optional
            Field overrides for mocked retrieved JSON files.
        copy_failure : str | None, optional
            Retrieved destination name whose copy should fail.
        ssh_probe_failures : int, default=0
            Number of readiness SSH probes that should fail.
        """
        self.markers = iter(markers)
        self.inspect_results = list(inspect_results or [])
        self.calls: list[tuple[Any, ...]] = []
        self.instance_exists = True
        self.job_payload: dict[str, Any] | None = None
        self.staged_files: set[str] = set()
        self.detached_command = ""
        self.retrieved_overrides = retrieved_overrides or {}
        self.copy_failure = copy_failure
        self.ssh_probe_failures = ssh_probe_failures

    def create(
        self, instance_name: str, instance_type: str, startup_script: Path
    ) -> str:
        """Record instance creation.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        instance_type : str
            Explicit Brev instance type.
        startup_script : Path
            Credentialless startup script.

        Returns
        -------
        str
            Created instance name.
        """
        self.calls.append(("create", instance_name, instance_type, startup_script))
        return instance_name

    def copy(self, source: str | Path, destination: str | Path) -> str:
        """Capture staged inputs or materialize mocked retrieved files.

        Parameters
        ----------
        source : str | Path
            Local or remote source.
        destination : str | Path
            Local or remote destination.

        Returns
        -------
        str
            Static copy result.
        """
        self.calls.append(("copy", source, destination))
        if isinstance(source, Path):
            self.job_payload = json.loads(
                (source / "job.json").read_text(encoding="utf-8")
            )
            self.staged_files = {
                path.relative_to(source).as_posix()
                for path in source.rglob("*")
                if path.is_file()
            }
        elif isinstance(destination, Path):
            if destination.name == self.copy_failure:
                raise RuntimeError(f"retrieval failed for {destination.name}")
            assert self.job_payload is not None
            started_at = "2026-01-02T03:04:05+00:00"
            completed_at = "2026-01-02T03:05:05+00:00"
            if destination.name == "results":
                for directory in ("input", "output", "logs"):
                    (destination / directory).mkdir(parents=True, exist_ok=True)
                (destination / "input" / "ross_dodecahedron_jacs.json").write_text(
                    "{}", encoding="utf-8"
                )
                benchmark_kind = self.job_payload["benchmark_kind"]
                (destination / "output" / f"{benchmark_kind}_benchmark.out").write_text(
                    "{}", encoding="utf-8"
                )
                (destination / "logs" / "stdout.log").write_text("", encoding="utf-8")
                (destination / "logs" / "stderr.log").write_text("", encoding="utf-8")
                payload = {
                    "schema_version": 5,
                    "benchmark_kind": benchmark_kind,
                    "mps_process_count": self.job_payload["mps_process_count"],
                    "execution": {"success": True, "error_message": None},
                    "timestamps": {
                        "started_at_utc": started_at,
                        "completed_at_utc": completed_at,
                    },
                }
                payload.update(self.retrieved_overrides.get("manifest.json", {}))
                (destination / "manifest.json").write_text(
                    json.dumps(payload), encoding="utf-8"
                )
            else:
                common = {
                    "schema_version": 1,
                    "job_id": self.job_payload["job_id"],
                    "started_at_utc": started_at,
                    "completed_at_utc": completed_at,
                }
                if destination.name == "status.json":
                    payload = {
                        **common,
                        "state": "succeeded",
                        "heartbeat_at_utc": completed_at,
                        "error_message": None,
                    }
                else:
                    payload = {
                        **common,
                        "profile": self.job_payload["profile"],
                        "benchmark_kind": self.job_payload["benchmark_kind"],
                        "mps_process_count": self.job_payload["mps_process_count"],
                        "success": True,
                        "error_message": None,
                        "output_directory": self.job_payload["output_directory"],
                        "gpu_provenance": {},
                    }
                payload.update(self.retrieved_overrides.get(destination.name, {}))
                destination.write_text(json.dumps(payload), encoding="utf-8")
        return "copied"

    def exec(
        self, instance_name: str, command: str, timeout: float | None = None
    ) -> str:
        """Start the worker or return the next durable marker.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        command : str
            Remote shell command.
        timeout : float | None, optional
            Remaining readiness budget for the call, ignored by the fake.

        Returns
        -------
        str
            Worker PID or framed marker response.
        """
        self.calls.append(("exec", instance_name, command))
        if command == "test -f workspace/startup-complete":
            if self.ssh_probe_failures:
                self.ssh_probe_failures -= 1
                raise RuntimeError("Connection closed by SSH gateway")
            return ""
        if command.startswith("mv "):
            return ""
        if command.startswith("nohup "):
            self.detached_command = command
            return "1234"
        return next(self.markers)

    def inspect(
        self, instance_name: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        """Return controlled instance presence.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        timeout : float | None, optional
            Remaining readiness budget for the call, ignored by the fake.

        Returns
        -------
        dict[str, Any] | None
            Instance record or ``None``.
        """
        self.calls.append(("inspect", instance_name))
        if self.inspect_results:
            return self.inspect_results.pop(0)
        return (
            {
                "name": instance_name,
                "status": "RUNNING",
                "shell_status": "READY",
                "health_status": "HEALTHY",
            }
            if self.instance_exists
            else None
        )

    def delete(self, instance_name: str) -> str:
        """Record permanent instance deletion.

        Parameters
        ----------
        instance_name : str
            Brev instance name.

        Returns
        -------
        str
            Deleted instance name.
        """
        self.calls.append(("delete", instance_name))
        self.instance_exists = False
        return instance_name


def _queued_tasks(tmp_path: Path, *, count: int = 1) -> tuple[TaskStatusDB, list[str]]:
    """Queue one or two Brev tasks in a local Exorcist database.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    count : int, default=1
        Number of task kinds to queue.

    Returns
    -------
    tuple[TaskStatusDB, list[str]]
        Task database and queued task identifiers.
    """
    task_db = TaskStatusDB.from_filename(tmp_path / "tasks.db")
    task_ids = queue_brev_tasks(
        task_db,
        "nvidia-a100",
        "openfe-gpu",
        BenchmarkKind.BOTH if count == 2 else BenchmarkKind.MD,
        1,
        60,
    )
    return task_db, task_ids


def _startup_script(tmp_path: Path) -> Path:
    """Write a credentialless startup script for controller tests.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.

    Returns
    -------
    Path
        Startup script path.
    """
    path = tmp_path / "brev_startup.sh"
    path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    return path


def _task_row(task_db: TaskStatusDB, task: str) -> dict[str, Any]:
    """Load one Exorcist task row as a mapping.

    Parameters
    ----------
    task_db : TaskStatusDB
        Exorcist task database.
    task : str
        Task identifier.

    Returns
    -------
    dict[str, Any]
        Persisted task row.
    """
    statement = sqla.select(task_db.tasks_table).where(
        task_db.tasks_table.c.taskid == task
    )
    with task_db.engine.connect() as connection:
        return dict(connection.execute(statement).mappings().one())


def test_launch_claims_one_task_stages_detached_job_and_retrieves_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run one successful task without claiming its queued sibling.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    capsys : pytest.CaptureFixture[str]
        Pytest output capture fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path, count=2)
    claimed = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(claimed)[5]
    now = datetime.now(timezone.utc).isoformat()
    transport = _FakeTransport(
        [
            "status\n"
            + json.dumps(
                {
                    "job_id": remote_job_id,
                    "state": "running",
                    "heartbeat_at_utc": now,
                }
            ),
            "complete\n" + json.dumps({"job_id": remote_job_id, "success": True}),
        ],
        inspect_results=[
            {
                "name": "starting-instance",
                "status": "RUNNING",
                "shell_status": "NOT READY",
                "health_status": "UNHEALTHY",
            },
            {
                "name": "ready-instance",
                "status": "RUNNING",
                "shell_status": "READY",
                "health_status": "HEALTHY",
            },
        ],
        ssh_probe_failures=1,
    )
    sleep_calls: list[float] = []
    monkeypatch.setattr(orchestration.time, "sleep", sleep_calls.append)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "controller-secret")
    monkeypatch.setenv("TURSO_AUTH_TOKEN", "controller-token")
    artifacts = fsspec.filesystem("memory")

    result = launch_brev_task(
        task_db,
        "brev-success-bucket",
        tmp_path / "results",
        _startup_script(tmp_path),
        transport=cast(BrevTransport, transport),
        artifact_output=cast(s3fs.S3FileSystem, artifacts),
    )

    assert result is not None
    assert result == (claimed, (tmp_path / "results" / remote_job_id).resolve())
    assert transport.job_payload == {
        "schema_version": 1,
        "job_id": remote_job_id,
        "benchmark_kind": "md",
        "mps_process_count": 1,
        "profile": "openfe-gpu",
        "benchmark_repo_path": "benchmark-repo",
        "output_directory": "results",
    }
    assert transport.staged_files == {"job.json"}
    assert any(
        call[0] == "exec"
        and call[2].startswith("mv workspace/performance_benchmarks ")
        and call[2].endswith("/benchmark-repo")
        for call in transport.calls
    )
    assert 'nohup "$HOME/.pixi/bin/pixi" run ' in transport.detached_command
    assert "nohup " in transport.detached_command
    assert "< /dev/null & echo $!" in transport.detached_command
    assert "worker job" in transport.detached_command
    assert "--heartbeat-interval-seconds 30" in transport.detached_command
    assert "controller-secret" not in transport.detached_command
    assert "controller-token" not in json.dumps(transport.job_payload)
    assert sum(call[0] == "create" for call in transport.calls) == 1
    assert sum(call[0] == "delete" for call in transport.calls) == 1

    controller = json.loads((result[1] / "controller.json").read_text(encoding="utf-8"))
    assert controller["instance_name"] == _parse_brev_task_metadata(claimed)[6]
    assert controller["remote_job_id"] == remote_job_id
    assert controller["attempt"] == 1
    assert controller["heartbeat_at_utc"] == now
    assert "timeout_seconds" not in controller
    assert controller["instance_cleaned_up"] is True
    assert [transition["state"] for transition in controller["transitions"]] == [
        "claimed",
        "creating",
        "instance_ready",
        "staged",
        "running",
        "worker_succeeded",
        "retrieved",
        "validated",
        "uploaded",
        "finalized",
    ]
    assert (result[1] / "results" / "output" / "md_benchmark.out").is_file()
    uploaded = artifacts.find("brev-success-bucket")
    assert any(path.endswith("/output/md_benchmark.out") for path in uploaded)
    uploaded_manifest = json.loads(
        artifacts.cat(
            next(path for path in uploaded if path.endswith("/manifest.json"))
        )
    )
    assert uploaded_manifest["s3_prefix"].startswith("runs/2026-01-02/")
    assert sleep_calls == [5, 5, 30]
    progress = capsys.readouterr().out
    assert "waiting for SSH readiness (status=RUNNING, shell=NOT READY" in progress
    assert (
        "SSH/startup probe failed (Connection closed by SSH gateway); retrying"
        in progress
    )
    assert "SSH connection and startup ready" in progress
    assert f"[brev] {controller['instance_name']}: staged" in progress
    assert f"[brev] {controller['instance_name']}: running heartbeat={now}" in progress

    assert _task_row(task_db, claimed)["status"] == TaskStatus.COMPLETED.value
    assert _task_row(task_db, claimed)["tries"] == 1
    assert _task_row(task_db, task_ids[1])["status"] == TaskStatus.AVAILABLE.value
    assert _task_row(task_db, task_ids[1])["tries"] == 0


def test_launch_times_out_before_copy_when_shell_is_not_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail and clean up without copying to an unhealthy Brev instance.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(
        [],
        inspect_results=[
            {
                "name": "unhealthy-instance",
                "status": "RUNNING",
                "shell_status": "NOT READY",
                "health_status": "UNHEALTHY",
            }
        ],
    )
    times = iter((100.0, 160.0))
    monkeypatch.setattr(orchestration.time, "monotonic", lambda: next(times))

    with pytest.raises(RuntimeError, match="did not become SSH-ready within 60"):
        launch_brev_task(
            task_db,
            "brev-timeout-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    assert not any(call[0] == "copy" for call in transport.calls)
    assert sum(call[0] == "delete" for call in transport.calls) == 1
    controller = json.loads(
        (tmp_path / "results" / remote_job_id / "controller.json").read_text(
            encoding="utf-8"
        )
    )
    assert controller["lifecycle_state"] == "failed"
    assert "shell_status='NOT READY'" in controller["failure_details"]
    assert controller["instance_cleaned_up"] is True
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


@pytest.mark.parametrize(
    ("scenario", "message", "delete_count"),
    [
        ("worker", "worker failed: workload failed", 1),
        ("disappeared", "disappeared", 0),
    ],
)
def test_launch_persists_and_cleans_up_terminal_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    message: str,
    delete_count: int,
) -> None:
    """Detect worker failure and instance disappearance.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    scenario : str
        Failure scenario to emulate.
    message : str
        Expected persisted failure fragment.
    delete_count : int
        Expected explicit instance deletion count.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    markers: list[str] = []
    inspect_results = None
    if scenario == "worker":
        markers = [
            "complete\n"
            + json.dumps(
                {
                    "job_id": remote_job_id,
                    "success": False,
                    "error_message": "workload failed",
                }
            )
        ]
    else:
        inspect_results = [None, None]

    transport = _FakeTransport(markers, inspect_results)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match=message):
        launch_brev_task(
            task_db,
            "brev-failure-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    controller_path = tmp_path / "results" / remote_job_id / "controller.json"
    controller = json.loads(controller_path.read_text(encoding="utf-8"))
    assert controller["lifecycle_state"] == "failed"
    assert message.lower() in controller["failure_details"].lower()
    assert controller["instance_cleaned_up"] is True
    assert sum(call[0] == "delete" for call in transport.calls) == delete_count
    if scenario == "worker":
        assert (
            controller_path.parent / "results" / "output" / "md_benchmark.out"
        ).is_file()
        assert (controller_path.parent / "complete.json").is_file()
    task_row = _task_row(task_db, task)
    assert task_row["status"] == TaskStatus.TOO_MANY_RETRIES.value
    assert task_row["tries"] == 1


def test_launch_preserves_retrieved_validation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject an uncorrelated completion marker before upload.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(
        ["complete\n" + json.dumps({"job_id": remote_job_id, "success": True})],
        retrieved_overrides={"complete.json": {"job_id": "job-other"}},
    )
    artifacts = fsspec.filesystem("memory")
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="complete.json job_id"):
        launch_brev_task(
            task_db,
            "brev-validation-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
            artifact_output=cast(s3fs.S3FileSystem, artifacts),
        )

    controller_path = tmp_path / "results" / remote_job_id / "controller.json"
    controller = json.loads(controller_path.read_text(encoding="utf-8"))
    assert "complete.json job_id" in controller["failure_details"]
    assert artifacts.find("brev-validation-bucket") == []
    assert controller["instance_cleaned_up"] is True
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


def test_launch_preserves_retrieval_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep local controller details when result retrieval fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(
        ["complete\n" + json.dumps({"job_id": remote_job_id, "success": True})],
        copy_failure="results",
    )
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="retrieval failed for results"):
        launch_brev_task(
            task_db,
            "brev-retrieval-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    controller = json.loads(
        (tmp_path / "results" / remote_job_id / "controller.json").read_text(
            encoding="utf-8"
        )
    )
    assert "retrieval failed for results" in controller["failure_details"]
    assert controller["instance_cleaned_up"] is True


def test_launch_preserves_upload_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain validated local artifacts when controller upload fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(
        ["complete\n" + json.dumps({"job_id": remote_job_id, "success": True})]
    )
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        orchestration,
        "_upload_results",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("upload failed")),
    )

    with pytest.raises(RuntimeError, match="upload failed"):
        launch_brev_task(
            task_db,
            "brev-upload-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    controller_path = tmp_path / "results" / remote_job_id / "controller.json"
    controller = json.loads(controller_path.read_text(encoding="utf-8"))
    assert "upload failed" in controller["failure_details"]
    assert (controller_path.parent / "results" / "manifest.json").is_file()
    assert controller["instance_cleaned_up"] is True
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


def test_launch_preserves_success_recording_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist local failure detail when Exorcist success recording fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(
        ["complete\n" + json.dumps({"job_id": remote_job_id, "success": True})]
    )
    artifacts = fsspec.filesystem("memory")
    real_mark_completed = task_db.mark_task_completed

    def _mark_completed(task_id: str, success: bool) -> None:
        """Fail only the successful completion write.

        Parameters
        ----------
        task_id : str
            Claimed task identifier.
        success : bool
            Completion outcome.
        """
        if success:
            raise RuntimeError("database unavailable")
        real_mark_completed(task_id, success)

    monkeypatch.setattr(task_db, "mark_task_completed", _mark_completed)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="task success persistence failed"):
        launch_brev_task(
            task_db,
            "brev-finalization-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
            artifact_output=cast(s3fs.S3FileSystem, artifacts),
        )

    controller = json.loads(
        (tmp_path / "results" / remote_job_id / "controller.json").read_text(
            encoding="utf-8"
        )
    )
    assert controller["lifecycle_state"] == "failed"
    assert "database unavailable" in controller["failure_details"]
    assert controller["instance_cleaned_up"] is True
    assert artifacts.find("brev-finalization-bucket")
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


def test_launch_cleans_up_when_user_interrupts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist failure and delete the instance when polling is interrupted.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(["pending"])

    def _interrupt(_seconds: float) -> None:
        """Interrupt the indefinite polling loop.

        Parameters
        ----------
        _seconds : float
            Static polling interval.
        """
        raise KeyboardInterrupt

    monkeypatch.setattr(orchestration.time, "sleep", _interrupt)

    with pytest.raises(KeyboardInterrupt):
        launch_brev_task(
            task_db,
            "brev-interrupt-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    controller_path = tmp_path / "results" / remote_job_id / "controller.json"
    controller = json.loads(controller_path.read_text(encoding="utf-8"))
    assert controller["lifecycle_state"] == "failed"
    assert "KeyboardInterrupt" in controller["failure_details"]
    assert controller["instance_cleaned_up"] is True
    assert sum(call[0] == "delete" for call in transport.calls) == 1
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


class _RecordingUploadOutput:
    """Record controller upload order and optionally fail one destination."""

    def __init__(self, fail_suffix: str | None = None) -> None:
        """Configure an optional failing destination suffix.

        Parameters
        ----------
        fail_suffix : str | None, optional
            Destination suffix whose upload raises instead of being recorded.
        """
        self.uploads: list[str] = []
        self.fail_suffix = fail_suffix

    def put(self, source: str, destination: str) -> None:
        """Record one upload, or fail it when it matches ``fail_suffix``.

        Parameters
        ----------
        source : str
            Local source path.
        destination : str
            Object store destination key.

        Raises
        ------
        RuntimeError
            If ``destination`` matches the configured failing suffix.
        """
        if self.fail_suffix is not None and destination.endswith(self.fail_suffix):
            raise RuntimeError(f"upload failed for {destination}")
        self.uploads.append(destination)


def _result_bundle(tmp_path: Path) -> Path:
    """Create a minimal validated result bundle for upload tests.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.

    Returns
    -------
    Path
        Local result directory containing a root manifest.
    """
    result_directory = tmp_path / "results"
    (result_directory / "output").mkdir(parents=True)
    (result_directory / "logs").mkdir()
    (result_directory / "output" / "md_benchmark.out").write_text(
        "{}", encoding="utf-8"
    )
    (result_directory / "logs" / "stdout.log").write_text("", encoding="utf-8")
    (result_directory / "manifest.json").write_text(
        json.dumps({"schema_version": 5}), encoding="utf-8"
    )
    return result_directory


def test_upload_results_publishes_root_manifest_last(tmp_path: Path) -> None:
    """Publish the root manifest only after every other artifact uploads.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    artifacts = _RecordingUploadOutput()
    started_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    prefix = orchestration._upload_results(
        _result_bundle(tmp_path),
        "brev-bucket",
        "brev-task",
        started_at,
        cast(s3fs.S3FileSystem, artifacts),
    )

    assert artifacts.uploads[-1] == f"brev-bucket/{prefix}/manifest.json"
    assert sum(key.endswith("/manifest.json") for key in artifacts.uploads) == 1
    assert any(key.endswith("/output/md_benchmark.out") for key in artifacts.uploads)


def test_upload_results_upload_failure_does_not_publish_manifest(
    tmp_path: Path,
) -> None:
    """Skip the manifest and keep it locally when another upload fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    result_directory = _result_bundle(tmp_path)
    artifacts = _RecordingUploadOutput(fail_suffix="/output/md_benchmark.out")

    with pytest.raises(RuntimeError, match="upload failed"):
        orchestration._upload_results(
            result_directory,
            "brev-bucket",
            "brev-task",
            datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
            cast(s3fs.S3FileSystem, artifacts),
        )

    assert not any(key.endswith("/manifest.json") for key in artifacts.uploads)
    local_manifest = json.loads(
        (result_directory / "manifest.json").read_text(encoding="utf-8")
    )
    assert local_manifest["s3_bucket"] == "brev-bucket"


def _fail_terminal_transitions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any terminal transition through ``_transition`` fail hard.

    Terminal states must be persisted without another Exorcist activity update,
    so a lifecycle transition named ``failed`` or ``finalized`` is rejected.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    real_transition = orchestration._transition

    def _transition(*args: Any, **kwargs: Any) -> None:
        """Reject terminal transitions and delegate every other transition.

        Parameters
        ----------
        *args : Any
            Positional ``_transition`` arguments.
        **kwargs : Any
            Keyword ``_transition`` arguments.
        """
        if args[4] in {"failed", "finalized"}:
            raise RuntimeError("terminal activity unavailable")
        real_transition(*args, **kwargs)

    monkeypatch.setattr(orchestration, "_transition", _transition)


def _fail_terminal_marker_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every terminal controller marker write fail.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    real_write = orchestration._write_controller_state

    def _write_controller_state(path: Path, state: dict[str, Any]) -> None:
        """Fail terminal marker writes and delegate every other write.

        Parameters
        ----------
        path : Path
            Controller marker destination.
        state : dict[str, Any]
            Current controller lifecycle record.
        """
        if state.get("lifecycle_state") in {"failed", "finalized"}:
            raise RuntimeError("controller marker unavailable")
        real_write(path, state)

    monkeypatch.setattr(
        orchestration, "_write_controller_state", _write_controller_state
    )


def _failing_completion(
    monkeypatch: pytest.MonkeyPatch,
    task_db: TaskStatusDB,
    failing: set[bool],
) -> list[bool]:
    """Record and fail selected Exorcist completion writes.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    task_db : TaskStatusDB
        Exorcist task database.
    failing : set[bool]
        Completion outcomes whose write should fail.

    Returns
    -------
    list[bool]
        Observed completion outcomes in call order.
    """
    real_mark_completed = task_db.mark_task_completed
    observed: list[bool] = []

    def _mark_completed(task_id: str, success: bool) -> None:
        """Record one completion attempt and fail selected outcomes.

        Parameters
        ----------
        task_id : str
            Claimed task identifier.
        success : bool
            Completion outcome.
        """
        observed.append(success)
        if success in failing:
            raise RuntimeError("database unavailable")
        real_mark_completed(task_id, success)

    monkeypatch.setattr(task_db, "mark_task_completed", _mark_completed)
    return observed


def _completed_worker_transport(task: str, *, success: bool) -> _FakeTransport:
    """Return a transport reporting one finished worker job.

    Parameters
    ----------
    task : str
        Claimed Brev task identifier.
    success : bool
        Worker completion outcome.

    Returns
    -------
    _FakeTransport
        Transport with a single correlated completion marker.
    """
    remote_job_id = _parse_brev_task_metadata(task)[5]
    payload: dict[str, Any] = {"job_id": remote_job_id, "success": success}
    if not success:
        payload["error_message"] = "workload failed"
    return _FakeTransport(["complete\n" + json.dumps(payload)])


@pytest.mark.parametrize("fault", ["activity", "marker"])
def test_launch_fails_without_instance_when_initial_transition_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Mark a claimed task failed when its first transition fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    fault : str
        Fault injected into the initial claim transition.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport([])
    if fault == "activity":

        def _touch_exorcist_task(*_args: Any) -> None:
            """Reject the Exorcist activity update."""
            raise RuntimeError("activity unavailable")

        monkeypatch.setattr(orchestration, "_touch_exorcist_task", _touch_exorcist_task)
    else:

        def _write_controller_state(*_args: Any) -> None:
            """Reject the local controller marker write."""
            raise RuntimeError("controller marker unavailable")

        monkeypatch.setattr(
            orchestration, "_write_controller_state", _write_controller_state
        )

    with pytest.raises(RuntimeError, match="unavailable"):
        launch_brev_task(
            task_db,
            "brev-claim-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    assert not any(call[0] == "create" for call in transport.calls)
    assert not any(call[0] == "delete" for call in transport.calls)
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value
    controller_path = tmp_path / "results" / remote_job_id / "controller.json"
    if fault == "activity":
        controller = json.loads(controller_path.read_text(encoding="utf-8"))
        assert controller["lifecycle_state"] == "failed"
        assert "activity unavailable" in controller["failure_details"]
    else:
        assert not controller_path.exists()


def test_launch_finalizes_without_activity_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Complete a task without a terminal Exorcist activity update.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    capsys : pytest.CaptureFixture[str]
        Pytest output capture fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _completed_worker_transport(task, success=True)
    artifacts = fsspec.filesystem("memory")
    _fail_terminal_transitions(monkeypatch)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    result = launch_brev_task(
        task_db,
        "brev-terminal-bucket",
        tmp_path / "results",
        _startup_script(tmp_path),
        transport=cast(BrevTransport, transport),
        artifact_output=cast(s3fs.S3FileSystem, artifacts),
    )

    assert result is not None
    controller = json.loads(
        (tmp_path / "results" / remote_job_id / "controller.json").read_text(
            encoding="utf-8"
        )
    )
    assert controller["lifecycle_state"] == "finalized"
    assert controller["instance_cleaned_up"] is True
    assert artifacts.find("brev-terminal-bucket")
    assert f"[brev] {controller['instance_name']}: finalized" in capsys.readouterr().out
    assert _task_row(task_db, task)["status"] == TaskStatus.COMPLETED.value


def test_launch_persists_terminal_failure_without_activity_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist a worker failure locally without a terminal activity update.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _completed_worker_transport(task, success=False)
    _fail_terminal_transitions(monkeypatch)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="worker failed"):
        launch_brev_task(
            task_db,
            "brev-terminal-failure-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    controller = json.loads(
        (tmp_path / "results" / remote_job_id / "controller.json").read_text(
            encoding="utf-8"
        )
    )
    assert controller["lifecycle_state"] == "failed"
    assert "worker failed" in controller["failure_details"]
    assert controller["instance_cleaned_up"] is True
    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


def test_launch_preserves_interrupt_when_terminal_writes_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep an interrupting KeyboardInterrupt despite failed terminal writes.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    remote_job_id = _parse_brev_task_metadata(task)[5]
    transport = _FakeTransport(["pending"])
    _fail_terminal_transitions(monkeypatch)
    _fail_terminal_marker_writes(monkeypatch)
    broken = _break_terminal_reporting(monkeypatch)
    observed = _failing_completion(monkeypatch, task_db, failing={True, False})

    def _interrupt(_seconds: float) -> None:
        """Interrupt the indefinite polling loop.

        Parameters
        ----------
        _seconds : float
            Static polling interval.
        """
        raise KeyboardInterrupt

    monkeypatch.setattr(orchestration.time, "sleep", _interrupt)

    with pytest.raises(KeyboardInterrupt):
        launch_brev_task(
            task_db,
            "brev-interrupt-terminal-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    controller = json.loads(
        (tmp_path / "results" / remote_job_id / "controller.json").read_text(
            encoding="utf-8"
        )
    )
    assert controller["lifecycle_state"] == "running"
    assert controller["instance_cleaned_up"] is True
    assert observed == [False]
    assert len(broken) >= 2
    assert broken[-1].endswith(": failed")
    assert sum(call[0] == "delete" for call in transport.calls) == 1
    assert _task_row(task_db, task)["status"] == TaskStatus.IN_PROGRESS.value


def test_launch_completes_when_finalized_marker_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Complete a successful task despite a failed finalized marker write.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    capsys : pytest.CaptureFixture[str]
        Pytest output capture fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    transport = _completed_worker_transport(task, success=True)
    artifacts = fsspec.filesystem("memory")
    _fail_terminal_marker_writes(monkeypatch)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    result = launch_brev_task(
        task_db,
        "brev-finalized-marker-bucket",
        tmp_path / "results",
        _startup_script(tmp_path),
        transport=cast(BrevTransport, transport),
        artifact_output=cast(s3fs.S3FileSystem, artifacts),
    )

    assert result is not None
    assert artifacts.find("brev-finalized-marker-bucket")
    assert "unable to persist finalized controller state" in capsys.readouterr().out
    assert _task_row(task_db, task)["status"] == TaskStatus.COMPLETED.value


def test_launch_preserves_failure_when_terminal_marker_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise the original lifecycle error despite a failed failed-marker write.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    transport = _completed_worker_transport(task, success=False)
    _fail_terminal_marker_writes(monkeypatch)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="worker failed"):
        launch_brev_task(
            task_db,
            "brev-failed-marker-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    assert _task_row(task_db, task)["status"] == TaskStatus.TOO_MANY_RETRIES.value


@pytest.mark.parametrize("failing", [{True}, {True, False}])
def test_launch_guards_completion_fallback_when_marker_writes_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failing: set[bool]
) -> None:
    """Attempt failure completion even when every terminal write fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    failing : set[bool]
        Completion outcomes whose Exorcist write should fail.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    transport = _completed_worker_transport(task, success=True)
    artifacts = fsspec.filesystem("memory")
    _fail_terminal_marker_writes(monkeypatch)
    observed = _failing_completion(monkeypatch, task_db, failing=failing)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(
        RuntimeError,
        match="task success persistence failed: RuntimeError: database unavailable",
    ) as excinfo:
        launch_brev_task(
            task_db,
            "brev-completion-fallback-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
            artifact_output=cast(s3fs.S3FileSystem, artifacts),
        )

    assert "task failure persistence failed" not in str(excinfo.value)
    assert observed == [True, False]
    assert artifacts.find("brev-completion-fallback-bucket")
    expected_status = (
        TaskStatus.IN_PROGRESS.value
        if failing == {True, False}
        else TaskStatus.TOO_MANY_RETRIES.value
    )
    assert _task_row(task_db, task)["status"] == expected_status


def _break_terminal_reporting(
    monkeypatch: pytest.MonkeyPatch, error: type[Exception] = BrokenPipeError
) -> list[str]:
    """Make stdout fail for terminal lifecycle reporting only.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    error : type[Exception], default=BrokenPipeError
        Stream failure raised for every terminal report line.

    Returns
    -------
    list[str]
        Terminal report lines whose printing raised ``error``.
    """
    real_print = builtins.print
    broken: list[str] = []

    def _print(*args: Any, **kwargs: Any) -> None:
        """Fail terminal report lines and delegate every other write.

        Parameters
        ----------
        *args : Any
            Positional ``print`` arguments.
        **kwargs : Any
            Keyword ``print`` arguments.
        """
        message = str(args[0]) if args else ""
        if message.endswith((": failed", ": finalized")) or (
            "unable to persist" in message
        ):
            broken.append(message)
            raise error("stdout closed")
        real_print(*args, **kwargs)

    monkeypatch.setattr(builtins, "print", _print)
    return broken


@pytest.mark.parametrize("error", [BrokenPipeError, ValueError])
def test_launch_completes_when_terminal_reporting_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: type[Exception]
) -> None:
    """Complete a task despite a failed terminal report and marker write.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    error : type[Exception]
        Stream failure raised for every terminal report line.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    transport = _completed_worker_transport(task, success=True)
    artifacts = fsspec.filesystem("memory")
    _fail_terminal_marker_writes(monkeypatch)
    broken = _break_terminal_reporting(monkeypatch, error)
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    result = launch_brev_task(
        task_db,
        "brev-terminal-stdout-bucket",
        tmp_path / "results",
        _startup_script(tmp_path),
        transport=cast(BrevTransport, transport),
        artifact_output=cast(s3fs.S3FileSystem, artifacts),
    )

    assert result is not None
    assert len(broken) == 2
    assert "unable to persist finalized controller state" in broken[0]
    assert broken[1].endswith(": finalized")
    assert artifacts.find("brev-terminal-stdout-bucket")
    assert _task_row(task_db, task)["status"] == TaskStatus.COMPLETED.value


def test_launch_preserves_error_when_terminal_reporting_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise the original lifecycle error despite a failed terminal report.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    task_db, task_ids = _queued_tasks(tmp_path)
    task = task_ids[0]
    transport = _completed_worker_transport(task, success=False)
    _break_terminal_reporting(monkeypatch)
    observed = _failing_completion(monkeypatch, task_db, failing={False})
    monkeypatch.setattr(orchestration.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="worker failed") as excinfo:
        launch_brev_task(
            task_db,
            "brev-terminal-stdout-failure-bucket",
            tmp_path / "results",
            _startup_script(tmp_path),
            transport=cast(BrevTransport, transport),
        )

    assert "BrokenPipeError" not in str(excinfo.value)
    assert "task failure persistence failed: database unavailable" in str(excinfo.value)
    assert observed == [False]
    assert _task_row(task_db, task)["status"] == TaskStatus.IN_PROGRESS.value
