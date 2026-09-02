from __future__ import annotations

from pathlib import Path

import pytest

import benchmarking_orchestration.providers.aws_cli as aws_cli_module
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.capabilities import WorkerCapability
from benchmarking_orchestration.providers.aws_cli import AwsCLI

_BENCH_TASK = (
    "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:"
    "12345678-1234-5678-1234-567812345678"
)


class _FakeTaskDB:
    """Minimal task database fake for CLI adapter tests."""

    def __init__(self, checkout_results: list[str | None] | None = None):
        self.checkout_results = list(checkout_results or [])
        self.checkout_calls: list[object] = []
        self.mark_calls: list[dict[str, object]] = []

    def check_out_task_with_capability(self, capability: object) -> str | None:
        self.checkout_calls.append(capability)
        return self.checkout_results.pop(0) if self.checkout_results else None

    def mark_task_completed(self, taskid_value: str, success: bool) -> None:
        self.mark_calls.append({"taskid": taskid_value, "success": success})


class _FakeConfig:
    """Config fake exposing an injected task database."""

    def __init__(self, task_db: _FakeTaskDB):
        self.task_db = task_db


def test_cloud_init_starts_cuda_mps_daemon():
    cloud_init_text = Path("cloud_init.sh").read_text(encoding="utf-8")
    assert "nvidia-cuda-mps-control -d" in cloud_init_text


def test_register_cli_registers_aws_subcommands():
    class _FakeApp:
        def __init__(self):
            self.calls = []

        def command(self, fn, name=None):
            self.calls.append({"fn": fn, "name": name})

    create_app = _FakeApp()
    launch_app = _FakeApp()
    worker_app = _FakeApp()
    AwsCLI().register_cli(create_app, launch_app, worker_app)

    assert [(call["fn"].__name__, call["name"]) for call in create_app.calls] == [
        ("create", "aws")
    ]
    assert [(call["fn"].__name__, call["name"]) for call in launch_app.calls] == [
        ("launch", "aws")
    ]
    assert [(call["fn"].__name__, call["name"]) for call in worker_app.calls] == [
        ("worker", "aws")
    ]


def test_create_maps_environment_and_prints_current_messages(monkeypatch, capsys):
    task_db = _FakeTaskDB()
    captured = {}
    monkeypatch.setenv("TURSO_DATABASE_URL", "libsql://example")
    monkeypatch.setenv("TURSO_AUTH_TOKEN", "secret")
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda ami_id, region: {"Name": f"approved-{ami_id}-{region}"},
    )

    def _queue(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return [("launch-task", "bench-task")]

    monkeypatch.setattr(aws_cli_module, "queue_aws_tasks", _queue)
    AwsCLI().create(
        " G5.XLARGE ",
        region=" us-east-1 ",
        ami_id=" AMI-0ABC123456789DEF0 ",
        cloud_init_file=Path("cloud-init.sh"),
        benchmark_kind=BenchmarkKind.MD,
        mps_process_count=3,
        s3_bucket="results",
        yes=True,
        config=_FakeConfig(task_db),
    )

    assert captured["args"] == (
        task_db,
        " G5.XLARGE ",
        "us-east-1",
        "ami-0abc123456789def0",
        BenchmarkKind.MD,
        3,
    )
    values = captured["kwargs"]["cloud_init_template_values"]
    assert values["turso_database_url"] == "libsql://example"
    assert values["turso_auth_token"] == "secret"
    assert values["S3_BUCKET"] == "results"
    assert captured["kwargs"]["cloud_init_file"] == Path("cloud-init.sh")
    out = capsys.readouterr().out
    assert "Resolved AMI for AWS queueing" in out
    assert "AMI confirmation skipped because --yes was provided." in out
    assert "Created benchmark task" in out


def test_create_preserves_optional_cloud_init(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda *_args: {"Name": "approved"},
    )
    monkeypatch.setattr(
        aws_cli_module,
        "queue_aws_tasks",
        lambda *args, **kwargs: captured.update(kwargs) or [],
    )

    AwsCLI().create(
        "g5.xlarge",
        ami_id="ami-0abc123456789def0",
        yes=True,
        config=_FakeConfig(_FakeTaskDB()),
    )

    assert captured["cloud_init_file"] is None


def test_create_requires_explicit_or_configured_ami(monkeypatch):
    monkeypatch.delenv("AWS_BENCHMARK_AMI_ID", raising=False)
    with pytest.raises(ValueError, match="AMI ID is required"):
        AwsCLI().create("g5.xlarge", yes=True, config=_FakeConfig(_FakeTaskDB()))


def test_create_uses_configured_ami(monkeypatch):
    captured = []
    monkeypatch.setenv("AWS_BENCHMARK_AMI_ID", "AMI-0ABC123456789DEF0")
    monkeypatch.setattr(
        aws_cli_module,
        "get_launch_ami_details",
        lambda ami_id, region: (
            captured.append((ami_id, region)) or {"Name": "approved"}
        ),
    )
    monkeypatch.setattr(aws_cli_module, "queue_aws_tasks", lambda *_args, **_kwargs: [])

    AwsCLI().create("g5.xlarge", yes=True, config=_FakeConfig(_FakeTaskDB()))
    assert captured == [("ami-0abc123456789def0", "us-east-1")]


def test_confirmation_rejects_noninteractive_input(monkeypatch):
    monkeypatch.setattr(aws_cli_module.sys.stdin, "isatty", lambda: False)
    with pytest.raises(ValueError, match="without interactive AMI confirmation"):
        aws_cli_module._confirm_ami_choice("ami-1", "approved", "us-east-1", yes=False)


def test_launch_with_no_tasks_prints_message(capsys):
    AwsCLI().launch(loop=False, config=_FakeConfig(_FakeTaskDB([None])))
    assert "No available launch tasks" in capsys.readouterr().out


def test_launch_maps_environment_and_prints_result(monkeypatch, capsys):
    task_db = _FakeTaskDB(["launch-task"])
    captured = {}
    monkeypatch.setenv("AWS_BENCHMARK_AMI_ID", "ami-approved")
    monkeypatch.setenv("EC2_KEY_NAME", "bench-key")
    monkeypatch.setenv("EC2_IAM_INSTANCE_PROFILE", "bench-profile")

    def _process(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "i-123"

    monkeypatch.setattr(aws_cli_module, "process_aws_launch_task", _process)
    AwsCLI().launch(loop=False, config=_FakeConfig(task_db))

    assert captured == {
        "args": (task_db, "launch-task"),
        "kwargs": {
            "expected_ami_id": "ami-approved",
            "key_name": "bench-key",
            "instance_profile_name": "bench-profile",
            "retry_for_capacity": False,
        },
    }
    assert "instance 'i-123'" in capsys.readouterr().out


def test_launch_propagates_orchestration_error(monkeypatch):
    monkeypatch.setattr(
        aws_cli_module,
        "process_aws_launch_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        AwsCLI().launch(loop=False, config=_FakeConfig(_FakeTaskDB(["launch-task"])))


def test_worker_launch_capability_uses_default_non_loop_dispatch(monkeypatch):
    delegated = []
    cli = AwsCLI()
    monkeypatch.setattr(
        cli,
        "launch",
        lambda loop, *, config=None: delegated.append((loop, config)),
    )
    config = _FakeConfig(_FakeTaskDB())

    cli.worker(WorkerCapability.LAUNCH, config=config)
    assert delegated == [(False, config)]


def test_worker_with_no_tasks_prints_message(capsys):
    AwsCLI().worker(
        WorkerCapability.G5,
        bench_repo_path=Path("/tmp/bench"),
        config=_FakeConfig(_FakeTaskDB([None])),
    )
    assert "No available g5 tasks." in capsys.readouterr().out


def test_worker_maps_runtime_values_and_prints_result(monkeypatch, capsys):
    task_db = _FakeTaskDB([_BENCH_TASK])
    captured = {}
    monkeypatch.setenv("S3_BUCKET", "results")

    def _process(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _BENCH_TASK, BenchmarkKind.MD

    monkeypatch.setattr(aws_cli_module, "process_aws_benchmark_task", _process)
    AwsCLI().worker(
        WorkerCapability.G5,
        bench_repo_path=Path("/tmp/bench"),
        config=_FakeConfig(task_db),
    )

    assert captured == {
        "args": (task_db, _BENCH_TASK),
        "kwargs": {
            "benchmark_repo_path": Path("/tmp/bench"),
            "s3_bucket": "results",
        },
    }
    assert f"Processed bench task '{_BENCH_TASK}'" in capsys.readouterr().out


def test_worker_missing_s3_bucket_marks_failure(monkeypatch):
    task_db = _FakeTaskDB([_BENCH_TASK])
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="S3_BUCKET environment variable is required"):
        AwsCLI().worker(
            WorkerCapability.G5,
            bench_repo_path=Path("/tmp/bench"),
            config=_FakeConfig(task_db),
        )
    assert task_db.mark_calls == [{"taskid": _BENCH_TASK, "success": False}]


def test_worker_rejects_malformed_task_before_missing_s3_bucket(monkeypatch):
    task_db = _FakeTaskDB(["bench:invalid"])
    monkeypatch.delenv("S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="Invalid bench task ID format"):
        AwsCLI().worker(
            WorkerCapability.G5,
            bench_repo_path=Path("/tmp/bench"),
            config=_FakeConfig(task_db),
        )

    assert task_db.mark_calls == [{"taskid": "bench:invalid", "success": False}]


def test_worker_propagates_orchestration_error(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "results")
    monkeypatch.setattr(
        aws_cli_module,
        "process_aws_benchmark_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bench failed")),
    )
    with pytest.raises(ValueError, match="bench failed"):
        AwsCLI().worker(
            WorkerCapability.G5,
            bench_repo_path=Path("/tmp/bench"),
            config=_FakeConfig(_FakeTaskDB([_BENCH_TASK])),
        )
