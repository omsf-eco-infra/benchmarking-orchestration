from __future__ import annotations

import base64
import builtins
import os
from pathlib import Path
from typing import Iterable

import pytest

import benchmarking_orchestration.aws.orchestration as orchestration
from benchmarking_orchestration.aws import (
    process_aws_benchmark_task,
    process_aws_launch_task,
    queue_aws_tasks,
)
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.tasks import TaskStatusDB


class _FakeTaskDB(TaskStatusDB):
    """Minimal task database fake for orchestration tests."""

    def __init__(self):
        self.add_calls = []
        self.mark_calls = []
        self.mark_error: Exception | None = None

    def __repr__(self) -> str:
        return "_FakeTaskDB()"

    def add_task_with_capability(
        self,
        taskid: str,
        requirements: Iterable[str],
        max_tries: int,
        capability: str,
    ) -> None:
        self.add_calls.append(
            {
                "taskid": taskid,
                "requirements": requirements,
                "max_tries": max_tries,
                "capability": capability,
            }
        )

    def mark_task_completed(self, taskid: str, success: bool) -> None:
        self.mark_calls.append({"taskid": taskid, "success": success})
        if self.mark_error is not None:
            raise self.mark_error


def _launch_task(cloud_init: str | None = None) -> str:
    """Build a stable launch task ID for tests.

    Parameters
    ----------
    cloud_init : str | None, optional
        Optional cloud-init source text.

    Returns
    -------
    str
        Launch task identifier.
    """
    payload = (
        f":{base64.b64encode(cloud_init.encode()).decode()}"
        if cloud_init is not None
        else ""
    )
    return (
        f"us-east-1:g5.xlarge:ami-0abc123456789def0{payload}:"
        "12345678-1234-5678-1234-567812345678"
    )


def _stub_queue_aws(monkeypatch) -> None:
    """Stub AWS validation used by queue tests.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(
        orchestration, "_validate_launch_instance_type", lambda *_args: None
    )
    monkeypatch.setattr(
        orchestration,
        "_get_launch_ami_details",
        lambda *_args: {"Name": "approved"},
    )


def test_queue_aws_tasks_adds_both_dependency_pairs(monkeypatch):
    task_db = _FakeTaskDB()
    _stub_queue_aws(monkeypatch)

    pairs = queue_aws_tasks(
        task_db,
        " G5.XLARGE ",
        " us-east-1 ",
        " AMI-0ABC123456789DEF0 ",
        BenchmarkKind.BOTH,
        4,
    )

    assert len(pairs) == 2
    assert len(task_db.add_calls) == 4
    for index, (launch_task, bench_task) in enumerate(pairs):
        launch_call, bench_call = task_db.add_calls[index * 2 : index * 2 + 2]
        assert launch_call == {
            "taskid": launch_task,
            "requirements": [],
            "max_tries": 1,
            "capability": "launch",
        }
        assert bench_call["taskid"] == bench_task
        assert bench_call["requirements"] == [launch_task]
        assert bench_call["capability"] == "g5"
        assert ":mps:4:" in bench_task
    assert pairs[0][0] != pairs[1][0]
    assert pairs[0][1].startswith("bench:md:")
    assert pairs[1][1].startswith("bench:rbfe:")


def test_queue_aws_tasks_without_cloud_init_does_not_read_none(monkeypatch):
    task_db = _FakeTaskDB()
    _stub_queue_aws(monkeypatch)
    rendered = []
    monkeypatch.setattr(
        orchestration._cloud_init,
        "_read_cloud_init_file_as_base64",
        lambda path, template_values=None: rendered.append((path, template_values)),
    )

    queue_aws_tasks(
        task_db,
        "g5.xlarge",
        "us-east-1",
        "ami-0abc123456789def0",
        BenchmarkKind.MD,
        1,
    )

    assert rendered == [(None, {"GPU_CAPABILITY": "g5"})]


def test_queue_aws_tasks_renders_explicit_template_values(monkeypatch, tmp_path):
    task_db = _FakeTaskDB()
    _stub_queue_aws(monkeypatch)
    template = tmp_path / "cloud-init.sh"
    template.write_text("@TOKEN @GPU_CAPABILITY @S3_BUCKET", encoding="utf-8")

    pairs = queue_aws_tasks(
        task_db,
        "g5.xlarge",
        "us-east-1",
        "ami-0abc123456789def0",
        BenchmarkKind.MD,
        1,
        cloud_init_file=template,
        cloud_init_template_values={"TOKEN": "explicit", "S3_BUCKET": "results"},
    )

    encoded = pairs[0][0].split(":")[-2]
    assert base64.b64decode(encoded).decode() == "explicit g5 results"


def test_queue_orchestration_does_not_read_environment_prompt_or_print(monkeypatch):
    task_db = _FakeTaskDB()
    _stub_queue_aws(monkeypatch)

    def _unexpected(*_args, **_kwargs):
        raise AssertionError("CLI side effect used by orchestration")

    monkeypatch.setattr(os, "getenv", _unexpected)
    monkeypatch.setattr(builtins, "input", _unexpected)
    monkeypatch.setattr(builtins, "print", _unexpected)

    queue_aws_tasks(
        task_db,
        "g5.xlarge",
        "us-east-1",
        "ami-0abc123456789def0",
        BenchmarkKind.MD,
        1,
    )


def test_process_launch_task_decodes_values_and_marks_success(monkeypatch):
    task_db = _FakeTaskDB()
    captured = []
    monkeypatch.setattr(
        orchestration,
        "_launch_ec2_instance",
        lambda *args, **kwargs: captured.append((args, kwargs)) or "i-123",
    )

    task = _launch_task("#!/bin/sh\necho hello\n")
    result = process_aws_launch_task(
        task_db,
        task,
        expected_ami_id="AMI-0ABC123456789DEF0",
        key_name="bench-key",
        instance_profile_name="bench-profile",
    )

    assert result == "i-123"
    assert captured == [
        (
            ("g5.xlarge",),
            {
                "ami_id": "ami-0abc123456789def0",
                "region": "us-east-1",
                "user_data": "#!/bin/sh\necho hello\n",
                "key_name": "bench-key",
                "instance_profile_name": "bench-profile",
            },
        )
    ]
    assert task_db.mark_calls == [{"taskid": task, "success": True}]


@pytest.mark.parametrize(
    ("task", "message"),
    [
        ("invalid", "Invalid launch task ID format"),
        (_launch_task(), "Queued launch task AMI does not match"),
    ],
)
def test_process_launch_task_marks_validation_failures(monkeypatch, task, message):
    task_db = _FakeTaskDB()
    expected = "ami-other" if task != "invalid" else None
    with pytest.raises((ValueError, RuntimeError), match=message):
        process_aws_launch_task(task_db, task, expected_ami_id=expected)
    assert task_db.mark_calls == [{"taskid": task, "success": False}]


def test_process_launch_task_marks_launch_failure(monkeypatch):
    task_db = _FakeTaskDB()
    monkeypatch.setattr(
        orchestration,
        "_launch_ec2_instance",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    task = _launch_task()

    with pytest.raises(RuntimeError, match="boom"):
        process_aws_launch_task(task_db, task)
    assert task_db.mark_calls == [{"taskid": task, "success": False}]


def test_process_launch_task_retries_capacity(monkeypatch):
    """Retry G/VT launches after EC2 reports insufficient capacity."""
    task_db = _FakeTaskDB()
    task = _launch_task()
    launches = []
    sleeps = []
    quota_regions = []
    monkeypatch.setattr(
        orchestration,
        "_get_instance_type_vcpu_count",
        lambda _type, region: 4,
    )
    monkeypatch.setattr(
        orchestration,
        "_get_ondemand_g_vcpu_quota",
        lambda region: quota_regions.append(region) or orchestration._WIGGLE_ROOM + 4,
    )
    monkeypatch.setattr(orchestration, "_get_ondemand_g_vcpus_used", lambda region: 0)

    def _launch(*_args, **_kwargs):
        launches.append(1)
        if len(launches) == 1:
            raise RuntimeError("InsufficientInstanceCapacity")
        return "i-123"

    monkeypatch.setattr(orchestration, "_launch_ec2_instance", _launch)
    monkeypatch.setattr(
        orchestration._time, "sleep", lambda seconds: sleeps.append(seconds)
    )

    assert process_aws_launch_task(task_db, task, retry_for_capacity=True) == "i-123"
    assert len(launches) == 2
    assert quota_regions == ["us-east-1", "us-east-1"]
    assert sleeps == [orchestration._CAPACITY_RETRY_SLEEP_SECONDS]
    assert task_db.mark_calls == [{"taskid": task, "success": True}]


def test_quota_wait_uses_task_region_for_recheck(monkeypatch):
    """Forward the task region to every quota preflight lookup."""
    task_db = _FakeTaskDB()
    task = _launch_task().replace("us-east-1:", "eu-west-1:", 1)
    vcpu_calls = []
    quota_calls = []
    usage_calls = []
    quota_values = iter(
        [orchestration._WIGGLE_ROOM + 3, orchestration._WIGGLE_ROOM + 4]
    )
    sleeps = []
    monkeypatch.setattr(
        orchestration,
        "_get_instance_type_vcpu_count",
        lambda instance_type, region: vcpu_calls.append((instance_type, region)) or 4,
    )
    monkeypatch.setattr(
        orchestration,
        "_get_ondemand_g_vcpu_quota",
        lambda region: quota_calls.append(region) or next(quota_values),
    )
    monkeypatch.setattr(
        orchestration,
        "_get_ondemand_g_vcpus_used",
        lambda region: usage_calls.append(region) or 0,
    )
    monkeypatch.setattr(
        orchestration, "_launch_ec2_instance", lambda *_args, **_kwargs: "i-123"
    )
    monkeypatch.setattr(
        orchestration._time, "sleep", lambda seconds: sleeps.append(seconds)
    )

    assert process_aws_launch_task(task_db, task, retry_for_capacity=True) == "i-123"
    assert vcpu_calls == [("g5.xlarge", "eu-west-1")]
    assert quota_calls == ["eu-west-1", "eu-west-1"]
    assert usage_calls == ["eu-west-1", "eu-west-1"]
    assert sleeps == [orchestration._CAPACITY_RETRY_SLEEP_SECONDS]


def test_p_launch_skips_g_vcpu_quota_preflight(monkeypatch):
    """Launch supported P instances without querying the G/VT quota pool."""
    task_db = _FakeTaskDB()
    task = _launch_task().replace(":g5.xlarge:", ":p4d.24xlarge:", 1)

    def _unexpected(*_args, **_kwargs):
        """Fail if a G/VT quota helper is queried for a P launch."""
        raise AssertionError("G/VT quota preflight queried for P instance")

    monkeypatch.setattr(orchestration, "_get_ondemand_g_vcpu_quota", _unexpected)
    monkeypatch.setattr(orchestration, "_get_ondemand_g_vcpus_used", _unexpected)
    monkeypatch.setattr(
        orchestration, "_launch_ec2_instance", lambda *_args, **_kwargs: "i-123"
    )

    assert process_aws_launch_task(task_db, task, retry_for_capacity=True) == "i-123"


def test_process_benchmark_task_runs_and_marks_success(monkeypatch):
    task_db = _FakeTaskDB()
    task = f"bench:rbfe:mps:3:{_launch_task()}"
    calls = []
    monkeypatch.setattr(
        orchestration, "_run_benchmark", lambda **kwargs: calls.append(kwargs)
    )

    result = process_aws_benchmark_task(
        task_db,
        task,
        benchmark_repo_path=Path("/tmp/bench"),
        s3_bucket="results",
    )

    assert result == (task, BenchmarkKind.RBFE)
    assert calls == [
        {
            "benchmark_repo_path": Path("/tmp/bench"),
            "s3_bucket": "results",
            "task_id": task,
            "benchmark_kind": BenchmarkKind.RBFE,
            "mps_process_count": 3,
        }
    ]
    assert task_db.mark_calls == [{"taskid": task, "success": True}]


def test_process_benchmark_task_marks_malformed_task(monkeypatch):
    task_db = _FakeTaskDB()
    with pytest.raises(ValueError, match="Invalid bench task ID format"):
        process_aws_benchmark_task(
            task_db,
            "bench:invalid",
            benchmark_repo_path=Path("/tmp/bench"),
            s3_bucket="results",
        )
    assert task_db.mark_calls == [{"taskid": "bench:invalid", "success": False}]


def test_process_benchmark_task_marks_benchmark_failure(monkeypatch):
    task_db = _FakeTaskDB()
    task = f"bench:md:{_launch_task()}"
    monkeypatch.setattr(
        orchestration,
        "_run_benchmark",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("workload failed")),
    )

    with pytest.raises(ValueError, match="workload failed"):
        process_aws_benchmark_task(
            task_db,
            task,
            benchmark_repo_path=Path("/tmp/bench"),
            s3_bucket="results",
        )
    assert task_db.mark_calls == [{"taskid": task, "success": False}]


def test_process_benchmark_task_reports_mark_failure(monkeypatch):
    task_db = _FakeTaskDB()
    task_db.mark_error = RuntimeError("database failed")
    task = f"bench:md:{_launch_task()}"
    monkeypatch.setattr(
        orchestration,
        "_run_benchmark",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("workload failed")),
    )

    with pytest.raises(ValueError, match="could not be marked as failed"):
        process_aws_benchmark_task(
            task_db,
            task,
            benchmark_repo_path=Path("/tmp/bench"),
            s3_bucket="results",
        )
