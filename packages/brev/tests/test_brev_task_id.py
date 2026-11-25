"""Brev task identifier validation tests."""

import uuid

import pytest

import benchmarking_brev.task_id as task_id_module
from benchmarking_brev.task_id import _build_brev_task_id, _parse_brev_task_metadata
from benchmarking_orchestration_core import BenchmarkKind


@pytest.mark.parametrize("kind", [BenchmarkKind.MD, BenchmarkKind.RBFE])
def test_brev_task_format_and_metadata_round_trip(kind, monkeypatch):
    """Preserve the source branch task format and case-sensitive identifiers.

    Parameters
    ----------
    kind : BenchmarkKind
        Single benchmark workload encoded in the task.
    monkeypatch : pytest.MonkeyPatch
        Fixture fixing the generated UUID for a byte-for-byte wire check.
    """
    identifier = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
    monkeypatch.setattr(task_id_module.uuid, "uuid4", lambda: identifier)

    task = _build_brev_task_id(" NVIDIA-A100 ", " openfe-gpu ", kind, 2, 240)

    assert task == (
        f"brev:{kind.value}:2:240:openfe-gpu:NVIDIA-A100:"
        f"job-{identifier}:brev-{identifier}"
    )
    assert _parse_brev_task_metadata(task) == (
        kind,
        2,
        240.0,
        "openfe-gpu",
        "NVIDIA-A100",
        f"job-{identifier}",
        f"brev-{identifier}",
    )


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
