from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.brev import queue_brev_tasks
from benchmarking_orchestration.task_id import _parse_brev_task_metadata
from benchmarking_orchestration.tasks import TaskStatusDB


def test_queue_brev_tasks_encodes_metadata_and_uses_task_status_db(
    tmp_path: Path,
) -> None:
    """Queue one self-contained Exorcist task per benchmark kind.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    database = tmp_path / "tasks.db"
    task_db = TaskStatusDB.from_filename(database)

    task_ids = queue_brev_tasks(
        task_db,
        " NVIDIA-A100 ",
        " openfe-gpu ",
        BenchmarkKind.BOTH,
        2,
        900,
    )

    assert len(task_ids) == 2
    assert len(set(task_ids)) == 2
    metadata = [_parse_brev_task_metadata(task_id) for task_id in task_ids]
    assert [values[0] for values in metadata] == [BenchmarkKind.MD, BenchmarkKind.RBFE]
    for values in metadata:
        (
            _kind,
            mps_process_count,
            timeout_seconds,
            profile,
            instance_type,
            remote_job_id,
            instance_name,
        ) = values
        assert mps_process_count == 2
        assert timeout_seconds == 900
        assert profile == "openfe-gpu"
        assert instance_type == "NVIDIA-A100"
        assert remote_job_id.startswith("job-")
        assert instance_name == remote_job_id.replace("job-", "brev-", 1)
    assert {
        task_db.check_out_task_with_capability("brev"),
        task_db.check_out_task_with_capability("brev"),
    } == set(task_ids)

    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert tables == {"dependencies", "task_capabilities", "tasks"}


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"instance_type": " "}, "instance_type must be"),
        ({"instance_type": "bad:type"}, "instance_type must be"),
        ({"profile": "bad profile"}, "profile must be"),
        ({"mps_process_count": True}, "mps_process_count must be"),
        ({"mps_process_count": 0}, "mps_process_count must be"),
        ({"timeout_seconds": 0}, "timeout_seconds must be"),
        ({"timeout_seconds": float("inf")}, "timeout_seconds must be"),
    ],
)
def test_queue_brev_tasks_rejects_invalid_inputs(
    tmp_path: Path, overrides: dict[str, object], message: str
) -> None:
    """Reject metadata that cannot be safely encoded in a Brev task ID.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    overrides : dict[str, object]
        Invalid queue argument override.
    message : str
        Expected validation error fragment.
    """
    values = {
        "instance_type": "nvidia-a100",
        "profile": "openfe-gpu",
        "benchmark_kind": BenchmarkKind.MD,
        "mps_process_count": 1,
        "timeout_seconds": 120,
    }
    values.update(overrides)
    task_db = TaskStatusDB.from_filename(tmp_path / "tasks.db")

    with pytest.raises(ValueError, match=message):
        queue_brev_tasks(task_db, **values)


@pytest.mark.parametrize(
    "task_id",
    [
        "invalid",
        "brev:both:1:120:profile:type:job-123:brev-123",
        "brev:md:0:120:profile:type:job-123:brev-123",
        "brev:md:1:120:profile:type:123e4567-e89b-12d3-a456-426614174000:brev-123e4567-e89b-12d3-a456-426614174000",
        "brev:md:1:120:profile:type:job-123e4567-e89b-12d3-a456-426614174000:brev-other",
    ],
)
def test_parse_brev_task_metadata_rejects_malformed_ids(task_id: str) -> None:
    """Reject malformed or inconsistent Brev task identifiers.

    Parameters
    ----------
    task_id : str
        Malformed task identifier.
    """
    with pytest.raises(ValueError, match="Invalid Brev task ID format"):
        _parse_brev_task_metadata(task_id)
