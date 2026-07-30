from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.providers.brev_cli import BrevCLI
from benchmarking_orchestration.task_id import _parse_brev_task_metadata
from benchmarking_orchestration.tasks import TaskStatusDB


def test_register_cli_registers_only_brev_create() -> None:
    """Register Part 3 creation without transport or execution commands."""

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

    BrevCLI().register_cli(create_app)

    assert [(call["function"].__name__, call["name"]) for call in create_app.calls] == [
        ("create", "brev")
    ]


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
        config=SimpleNamespace(task_db=task_db),
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
