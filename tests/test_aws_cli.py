from __future__ import annotations

import base64
from pathlib import Path

import pytest

import benchmarking_orchestration.providers.aws_cli as aws_cli_module
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.capabilities import WorkerCapability
from benchmarking_orchestration.providers.aws_cli import AwsCLI


class _FakeTaskDB:
    """Minimal fake task DB used by AwsCLI tests."""

    def __init__(self, checkout_results: list[str | None] | None = None):
        self.checkout_results = list(checkout_results or [])
        self.checkout_calls: list[object] = []
        self.mark_calls: list[dict[str, object]] = []
        self.add_calls: list[dict[str, object]] = []

    def check_out_task_with_capability(self, capability: object) -> str | None:
        self.checkout_calls.append(capability)
        if self.checkout_results:
            return self.checkout_results.pop(0)
        return None

    def mark_task_completed(self, taskid_value: str, success: bool) -> None:
        self.mark_calls.append({"taskid": taskid_value, "success": success})

    def add_task_with_capability(
        self,
        taskid: str,
        requirements: list[str],
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


class _FakeConfig:
    """Simple config object exposing only ``task_db``."""

    def __init__(self, task_db: _FakeTaskDB):
        self.task_db = task_db


def test_cloud_init_starts_cuda_mps_daemon():
    cloud_init_text = Path("cloud_init.sh").read_text(encoding="utf-8")

    assert "nvidia-cuda-mps-control -d" in cloud_init_text


def test_register_cli_registers_aws_subcommands():
    class _FakeApp:
        def __init__(self):
            self.calls: list[dict[str, object]] = []

        def command(self, fn, name=None):
            self.calls.append({"fn": fn, "name": name})

    create_app = _FakeApp()
    launch_app = _FakeApp()
    worker_app = _FakeApp()

    AwsCLI().register_cli(
        create_app=create_app,
        launch_app=launch_app,
        worker_app=worker_app,
    )

    assert create_app.calls[0]["name"] == "aws"
    assert create_app.calls[0]["fn"].__name__ == "create"
    assert launch_app.calls[0]["name"] == "aws"
    assert launch_app.calls[0]["fn"].__name__ == "launch"
    assert worker_app.calls[0]["name"] == "aws"
    assert worker_app.calls[0]["fn"].__name__ == "worker"


def test_launch_with_no_tasks_prints_message(capsys):
    fake_db = _FakeTaskDB(checkout_results=[None, None])
    config = _FakeConfig(fake_db)

    AwsCLI().launch(loop=False, config=config)

    out = capsys.readouterr().out
    assert "No available launch tasks" in out


def test_launch_processes_task_and_marks_success(monkeypatch):
    cloud_init_text = "#!/usr/bin/env bash\necho hello\n"
    cloud_init_b64 = base64.b64encode(cloud_init_text.encode("utf-8")).decode("ascii")
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:"
        f"{cloud_init_b64}:"
        "12345678-1234-5678-1234-567812345678"
    )

    fake_db = _FakeTaskDB(checkout_results=[taskid, taskid])
    config = _FakeConfig(fake_db)
    captured: list[object] = []

    class _FakeProvider:
        def submit(self, *launch_values):
            captured.append(launch_values)
            return "i-1234567890abcdef0"

    monkeypatch.setattr(aws_cli_module, "get_provider", lambda _name: _FakeProvider())
    monkeypatch.setenv("EC2_KEY_NAME", "bench-key")
    monkeypatch.setenv("EC2_IAM_INSTANCE_PROFILE", "bench-profile")

    AwsCLI().launch(loop=False, config=config)

    assert fake_db.mark_calls == [{"taskid": taskid, "success": True}]
    assert len(captured) == 1
    assert captured[0] == (
        "g5.xlarge",
        "ami-0abc123456789def0",
        "us-east-1",
        cloud_init_text,
        "bench-key",
        "bench-profile",
    )


def test_launch_marks_failed_when_submit_raises(monkeypatch):
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    )

    fake_db = _FakeTaskDB(checkout_results=[taskid, taskid])
    config = _FakeConfig(fake_db)

    class _BrokenProvider:
        def submit(self, *_launch_values):
            raise RuntimeError("boom")

    monkeypatch.setattr(aws_cli_module, "get_provider", lambda _name: _BrokenProvider())

    with pytest.raises(RuntimeError, match="boom"):
        AwsCLI().launch(loop=False, config=config)

    assert fake_db.mark_calls == [{"taskid": taskid, "success": False}]


def test_loop_launch_retries_when_ec2_capacity_is_unavailable(monkeypatch, capsys):
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    )
    fake_db = _FakeTaskDB(checkout_results=[taskid, None])
    config = _FakeConfig(fake_db)
    submit_calls: list[object] = []
    sleep_calls: list[int] = []

    class _FlakyProvider:
        def submit(self, *launch_values):
            submit_calls.append(launch_values)
            if len(submit_calls) == 1:
                raise RuntimeError(
                    "AWS error while launching instance type 'g5.xlarge' with AMI "
                    "'ami-0abc123456789def0' in region 'us-east-1': "
                    "InsufficientInstanceCapacity"
                )
            return "i-1234567890abcdef0"

    monkeypatch.setattr(aws_cli_module, "get_provider", lambda _name: _FlakyProvider())
    monkeypatch.setattr(
        aws_cli_module, "get_instance_type_vcpu_count", lambda _itype: 4
    )
    monkeypatch.setattr(
        aws_cli_module,
        "get_ondemand_g_vcpu_quota",
        lambda: aws_cli_module.WIGGLE_ROOM * 2,
    )
    monkeypatch.setattr(aws_cli_module, "get_ondemand_g_vcpus_used", lambda: 0)
    monkeypatch.setattr(
        aws_cli_module.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )

    AwsCLI().launch(loop=True, config=config)

    out = capsys.readouterr().out
    assert len(submit_calls) == 2
    assert sleep_calls == [aws_cli_module.CAPACITY_RETRY_SLEEP_SECONDS]
    assert fake_db.mark_calls == [{"taskid": taskid, "success": True}]
    assert "Processed launch task" in out


def test_worker_launch_capability_delegates_to_launch(monkeypatch):
    fake_db = _FakeTaskDB(checkout_results=["dummy-task"])
    config = _FakeConfig(fake_db)
    delegated: list[object] = []
    cli = AwsCLI()

    monkeypatch.setattr(cli, "launch", lambda *, config=None: delegated.append(config))

    cli.worker(WorkerCapability.LAUNCH, config=config)

    assert delegated == [config]
    assert fake_db.checkout_calls == []


def test_worker_bench_success_runs_benchmark_and_marks_success(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(checkout_results=[taskid])
    config = _FakeConfig(fake_db)
    run_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        aws_cli_module, "run_benchmark", lambda **kwargs: run_calls.append(kwargs)
    )
    monkeypatch.setenv("S3_BUCKET", "benchmark-results")

    AwsCLI().worker(
        WorkerCapability.G5, bench_repo_path=Path("/tmp/bench"), config=config
    )

    assert run_calls == [
        {
            "benchmark_repo_path": Path("/tmp/bench"),
            "s3_bucket": "benchmark-results",
            "task_id": taskid,
            "benchmark_kind": BenchmarkKind.MD,
            "mps_process_count": 1,
        }
    ]
    assert fake_db.mark_calls == [{"taskid": taskid, "success": True}]


def test_worker_bench_missing_s3_bucket_marks_failure(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(checkout_results=[taskid])
    config = _FakeConfig(fake_db)

    monkeypatch.delenv("S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="S3_BUCKET environment variable is required"):
        AwsCLI().worker(
            WorkerCapability.G5, bench_repo_path=Path("/tmp/bench"), config=config
        )

    assert fake_db.mark_calls == [{"taskid": taskid, "success": False}]


def test_worker_bench_invalid_task_id_marks_failure():
    fake_db = _FakeTaskDB(checkout_results=["bench:invalid"])
    config = _FakeConfig(fake_db)

    with pytest.raises(ValueError, match="Invalid bench task ID format"):
        AwsCLI().worker(
            WorkerCapability.G5, bench_repo_path=Path("/tmp/bench"), config=config
        )

    assert fake_db.mark_calls == [{"taskid": "bench:invalid", "success": False}]


def test_create_adds_launch_and_bench_tasks(monkeypatch, capsys):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)
    instance_validation_calls: list[dict[str, str]] = []
    ami_details_calls: list[dict[str, str]] = []
    cloud_init_calls: list[dict[str, object]] = []
    bench_task_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        aws_cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region: instance_validation_calls.append(
            {
                "instance_type": instance_type,
                "region": region,
            }
        ),
    )
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda ami_id, region: (
            ami_details_calls.append({"ami_id": ami_id, "region": region})
            or {"Name": f"approved-{ami_id}-{region}"}
        ),
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_resolve_bench_worker_capability",
        lambda _instance_type: WorkerCapability.G5,
    )

    def _fake_read_cloud_init_file_as_base64(cloud_init_file, extra_vars=None):
        cloud_init_calls.append(
            {
                "cloud_init_file": cloud_init_file,
                "extra_vars": extra_vars,
            }
        )
        return "cloud-init-b64"

    monkeypatch.setattr(
        aws_cli_module,
        "_read_cloud_init_file_as_base64",
        _fake_read_cloud_init_file_as_base64,
    )
    monkeypatch.setattr(
        aws_cli_module, "_build_task_id", lambda *_args, **_kwargs: "launch-task"
    )

    def _fake_build_bench_task_id(launch_task_id, benchmark_kind, mps_process_count=1):
        bench_task_calls.append(
            {
                "launch_task_id": launch_task_id,
                "benchmark_kind": benchmark_kind,
                "mps_process_count": mps_process_count,
            }
        )
        return "bench-task"

    monkeypatch.setattr(
        aws_cli_module, "_build_bench_task_id", _fake_build_bench_task_id
    )

    AwsCLI().create(
        instance_type="g5.xlarge",
        region="us-east-1",
        ami_id="ami-0abc123456789def0",
        cloud_init_file=Path("cloud-init.sh"),
        benchmark_kind=BenchmarkKind.MD,
        s3_bucket="bench-results",
        yes=True,
        config=config,
    )

    out = capsys.readouterr().out

    assert instance_validation_calls == [
        {
            "instance_type": "g5.xlarge",
            "region": "us-east-1",
        }
    ]
    assert ami_details_calls == [
        {
            "ami_id": "ami-0abc123456789def0",
            "region": "us-east-1",
        }
    ]
    assert cloud_init_calls == [
        {
            "cloud_init_file": "cloud-init.sh",
            "extra_vars": {
                "GPU_CAPABILITY": "g5",
                "S3_BUCKET": "bench-results",
            },
        }
    ]
    assert bench_task_calls == [
        {
            "launch_task_id": "launch-task",
            "benchmark_kind": BenchmarkKind.MD,
            "mps_process_count": 1,
        }
    ]
    assert fake_db.add_calls == [
        {
            "taskid": "launch-task",
            "requirements": [],
            "max_tries": 1,
            "capability": "launch",
        },
        {
            "taskid": "bench-task",
            "requirements": ["launch-task"],
            "max_tries": 1,
            "capability": "g5",
        },
    ]
    assert "Resolved AMI for AWS queueing" in out
    assert "AMI confirmation skipped because --yes was provided." in out
    assert "Created benchmark task" in out


def test_create_benchmark_task_uses_requested_mps_process_count(monkeypatch):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)
    bench_task_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        aws_cli_module,
        "validate_launch_instance_type",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda *_args, **_kwargs: {"Name": "approved-ami"},
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_resolve_bench_worker_capability",
        lambda _instance_type: WorkerCapability.G5,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_read_cloud_init_file_as_base64",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        aws_cli_module, "_build_task_id", lambda *_args, **_kwargs: "launch-task"
    )

    def _fake_build_bench_task_id(launch_task_id, benchmark_kind, mps_process_count=1):
        bench_task_calls.append(
            {
                "launch_task_id": launch_task_id,
                "benchmark_kind": benchmark_kind,
                "mps_process_count": mps_process_count,
            }
        )
        return f"bench-task-{benchmark_kind.value}"

    monkeypatch.setattr(
        aws_cli_module, "_build_bench_task_id", _fake_build_bench_task_id
    )

    AwsCLI().create(
        instance_type="g5.xlarge",
        ami_id="ami-0abc123456789def0",
        benchmark_kind=BenchmarkKind.RBFE,
        mps_process_count=3,
        yes=True,
        config=config,
    )

    assert bench_task_calls == [
        {
            "launch_task_id": "launch-task",
            "benchmark_kind": BenchmarkKind.RBFE,
            "mps_process_count": 3,
        }
    ]


def test_create_both_tasks_use_requested_mps_process_count(monkeypatch):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)
    build_task_ids = iter(["launch-task-md", "launch-task-rbfe"])
    bench_task_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        aws_cli_module,
        "validate_launch_instance_type",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda *_args, **_kwargs: {"Name": "approved-ami"},
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_resolve_bench_worker_capability",
        lambda _instance_type: WorkerCapability.G5,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_read_cloud_init_file_as_base64",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_build_task_id",
        lambda *_args, **_kwargs: next(build_task_ids),
    )

    def _fake_build_bench_task_id(launch_task_id, benchmark_kind, mps_process_count=1):
        bench_task_calls.append(
            {
                "launch_task_id": launch_task_id,
                "benchmark_kind": benchmark_kind,
                "mps_process_count": mps_process_count,
            }
        )
        return f"bench-task-{benchmark_kind.value}"

    monkeypatch.setattr(
        aws_cli_module, "_build_bench_task_id", _fake_build_bench_task_id
    )

    AwsCLI().create(
        instance_type="g5.xlarge",
        ami_id="ami-0abc123456789def0",
        benchmark_kind=BenchmarkKind.BOTH,
        mps_process_count=4,
        yes=True,
        config=config,
    )

    assert bench_task_calls == [
        {
            "launch_task_id": "launch-task-md",
            "benchmark_kind": BenchmarkKind.MD,
            "mps_process_count": 4,
        },
        {
            "launch_task_id": "launch-task-rbfe",
            "benchmark_kind": BenchmarkKind.RBFE,
            "mps_process_count": 4,
        },
    ]


def test_create_requires_explicit_or_configured_ami_id(monkeypatch):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)

    monkeypatch.delenv("AWS_BENCHMARK_AMI_ID", raising=False)

    with pytest.raises(
        ValueError,
        match="AMI ID is required. Pass --ami-id or set AWS_BENCHMARK_AMI_ID.",
    ):
        AwsCLI().create(
            instance_type="g5.xlarge",
            yes=True,
            config=config,
        )


def test_create_uses_approved_ami_from_environment(monkeypatch):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)
    ami_details_calls: list[dict[str, str]] = []

    monkeypatch.setenv("AWS_BENCHMARK_AMI_ID", "ami-0abc123456789def0")
    monkeypatch.setattr(
        aws_cli_module,
        "validate_launch_instance_type",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda ami_id, region: (
            ami_details_calls.append({"ami_id": ami_id, "region": region})
            or {"Name": f"approved-{ami_id}-{region}"}
        ),
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_resolve_bench_worker_capability",
        lambda _instance_type: WorkerCapability.G5,
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_read_cloud_init_file_as_base64",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        aws_cli_module, "_build_task_id", lambda *_args, **_kwargs: "launch-task"
    )
    monkeypatch.setattr(
        aws_cli_module,
        "_build_bench_task_id",
        lambda *_args, **_kwargs: "bench-task",
    )

    AwsCLI().create(
        instance_type="g5.xlarge",
        yes=True,
        config=config,
    )

    assert ami_details_calls == [
        {"ami_id": "ami-0abc123456789def0", "region": "us-east-1"}
    ]


def test_launch_rejects_task_with_unapproved_ami(monkeypatch):
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    )
    fake_db = _FakeTaskDB(checkout_results=[taskid, taskid])
    config = _FakeConfig(fake_db)

    monkeypatch.setenv("AWS_BENCHMARK_AMI_ID", "ami-0123456789abcdef0")

    with pytest.raises(RuntimeError, match="Queued launch task AMI does not match"):
        AwsCLI().launch(loop=False, config=config)

    assert fake_db.mark_calls == [{"taskid": taskid, "success": False}]
