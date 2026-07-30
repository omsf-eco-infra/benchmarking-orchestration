from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from cyclopts import App

import benchmarking_orchestration.providers.brev_cli as brev_cli_module
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.providers.brev_cli import BrevCLI
from benchmarking_orchestration.providers.cli_protocol import Config
from benchmarking_orchestration.task_id import _parse_brev_task_metadata
from benchmarking_orchestration.tasks import TaskStatusDB


def test_register_cli_registers_brev_create_and_launch() -> None:
    """Register task creation and trusted-controller dispatch commands."""

    class _FakeApp:
        """Record Cyclopts command registrations."""

        def __init__(self) -> None:
            """Create an empty command call ledger."""
            self.calls: list[dict[str, object]] = []

        def command(self, function: object, name: str | None = None) -> None:
            """Record one command registration.

            Parameters
            ----------
            function : object
                Registered command function.
            name : str | None, optional
                Explicit command name.
            """
            self.calls.append({"function": function, "name": name})

    create_app = _FakeApp()
    launch_app = _FakeApp()

    BrevCLI().register_cli(cast(App, create_app), cast(App, launch_app))

    assert [
        (getattr(call["function"], "__name__"), call["name"])
        for call in create_app.calls
    ] == [("create", "brev")]
    assert [
        (getattr(call["function"], "__name__"), call["name"])
        for call in launch_app.calls
    ] == [("launch", "brev")]


def test_create_queues_jobs_in_requested_local_database(tmp_path: Path, capsys) -> None:
    """Map create arguments to persisted Brev jobs and print their IDs.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    capsys : pytest.CaptureFixture[str]
        Pytest output capture fixture.
    """
    database = tmp_path / "tasks.db"
    task_db = TaskStatusDB.from_filename(database)

    BrevCLI().create(
        "nvidia-a100",
        "openfe-gpu",
        benchmark_kind=BenchmarkKind.BOTH,
        mps_process_count=3,
        timeout_seconds=240,
        config=cast(Config, SimpleNamespace(task_db=task_db)),
    )

    output = capsys.readouterr().out
    assert output.count("Created Brev benchmark task") == 2
    task_ids = [
        task_db.check_out_task_with_capability("brev"),
        task_db.check_out_task_with_capability("brev"),
    ]
    metadata = [_parse_brev_task_metadata(task_id) for task_id in task_ids]
    assert {values[0] for values in metadata} == {BenchmarkKind.MD, BenchmarkKind.RBFE}
    assert all(values[1:3] == (3, 240) for values in metadata)


def test_launch_runs_one_controller_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Map CLI paths to one trusted-controller dispatch.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    capsys : pytest.CaptureFixture[str]
        Pytest output capture fixture.
    """
    benchmark_repo = tmp_path / "benchmark-repo"
    benchmark_repo.mkdir()
    startup_script = tmp_path / "brev_startup.sh"
    startup_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    result_directory = tmp_path / "results"
    task_db = object()
    captured: list[tuple[object, ...]] = []

    def _launch(*args: object) -> tuple[str, Path]:
        """Capture the controller invocation.

        Parameters
        ----------
        *args : object
            Positional controller launch arguments.

        Returns
        -------
        tuple[str, Path]
            Representative retrieved task and local directory.
        """
        captured.append(args)
        return "brev-task", result_directory / "job-123"

    monkeypatch.setattr(brev_cli_module, "launch_brev_task", _launch)

    BrevCLI().launch(
        benchmark_repo,
        result_directory,
        startup_script,
        config=cast(Config, SimpleNamespace(task_db=task_db)),
    )

    assert captured == [
        (
            task_db,
            benchmark_repo,
            result_directory,
            startup_script,
        )
    ]
    assert "remains in progress" in capsys.readouterr().out
