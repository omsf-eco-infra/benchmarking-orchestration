from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sqla
from exorcist.models import TaskStatus

import benchmarking_orchestration.brev.orchestration as orchestration
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.brev import launch_brev_task, queue_brev_tasks
from benchmarking_orchestration.brev.transport import BrevTransport
from benchmarking_orchestration.task_id import _parse_brev_task_metadata
from benchmarking_orchestration.tasks import TaskStatusDB


class _FakeTransport:
    """Record Brev operations and return controlled durable markers."""

    def __init__(
        self,
        markers: list[str],
        inspect_results: list[dict[str, Any] | None] | None = None,
    ) -> None:
        """Configure marker and optional instance inspection results.

        Parameters
        ----------
        markers : list[str]
            Framed marker responses returned by remote exec calls.
        inspect_results : list[dict[str, Any] | None] | None, optional
            Ordered instance inspection results.
        """
        self.markers = iter(markers)
        self.inspect_results = list(inspect_results or [])
        self.calls: list[tuple[Any, ...]] = []
        self.instance_exists = True
        self.job_payload: dict[str, Any] | None = None
        self.staged_files: set[str] = set()
        self.detached_command = ""

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
            if destination.name == "results":
                destination.mkdir(parents=True)
                (destination / "artifact.txt").write_text("result", encoding="utf-8")
            else:
                destination.write_text("{}", encoding="utf-8")
        return "copied"

    def exec(self, instance_name: str, command: str) -> str:
        """Start the worker or return the next durable marker.

        Parameters
        ----------
        instance_name : str
            Brev instance name.
        command : str
            Remote shell command.

        Returns
        -------
        str
            Worker PID or framed marker response.
        """
        self.calls.append(("exec", instance_name, command))
        if command.startswith("nohup "):
            self.detached_command = command
            return "1234"
        return next(self.markers)

    def inspect(self, instance_name: str) -> dict[str, Any] | None:
        """Return controlled instance presence.

        Parameters
        ----------
        instance_name : str
            Brev instance name.

        Returns
        -------
        dict[str, Any] | None
            Instance record or ``None``.
        """
        self.calls.append(("inspect", instance_name))
        if self.inspect_results:
            return self.inspect_results.pop(0)
        return {"name": instance_name} if self.instance_exists else None

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


def _benchmark_repo(tmp_path: Path) -> Path:
    """Create the minimum benchmark input repository.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.

    Returns
    -------
    Path
        Benchmark repository root.
    """
    repository = tmp_path / "benchmark-repo"
    (repository / "benchmark").mkdir(parents=True)
    (repository / "benchmark" / "md_benchmark.py").write_text(
        "# credentialless benchmark input\n", encoding="utf-8"
    )
    (repository / "data").mkdir()
    (repository / "data" / "ross_dodecahedron_jacs.json").write_text(
        "{}", encoding="utf-8"
    )
    return repository


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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run one successful task without claiming its queued sibling.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
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
        ]
    )
    sleep_calls: list[float] = []
    monkeypatch.setattr(orchestration.time, "sleep", sleep_calls.append)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "controller-secret")
    monkeypatch.setenv("TURSO_AUTH_TOKEN", "controller-token")

    result = launch_brev_task(
        task_db,
        _benchmark_repo(tmp_path),
        tmp_path / "results",
        _startup_script(tmp_path),
        transport=cast(BrevTransport, transport),
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
    assert transport.staged_files == {
        "benchmark-repo/benchmark/md_benchmark.py",
        "benchmark-repo/data/ross_dodecahedron_jacs.json",
        "job.json",
    }
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
    ]
    assert (result[1] / "results" / "artifact.txt").is_file()
    assert sleep_calls == [120]

    assert _task_row(task_db, claimed)["status"] == TaskStatus.IN_PROGRESS.value
    assert _task_row(task_db, claimed)["tries"] == 1
    assert _task_row(task_db, task_ids[1])["status"] == TaskStatus.AVAILABLE.value
    assert _task_row(task_db, task_ids[1])["tries"] == 0


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
            _benchmark_repo(tmp_path),
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
        assert (controller_path.parent / "results" / "artifact.txt").is_file()
        assert (controller_path.parent / "complete.json").is_file()
    task_row = _task_row(task_db, task)
    assert task_row["status"] == TaskStatus.TOO_MANY_RETRIES.value
    assert task_row["tries"] == 1


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
            _benchmark_repo(tmp_path),
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
