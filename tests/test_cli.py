from __future__ import annotations

import base64
from pathlib import Path

import pytest

import benchmarking_orchestration.commands as cli_module
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.capabilities import WorkerCapability


class _FakeTaskDB:
    """Minimal fake task DB used by command primitive tests."""

    def __init__(self, task_to_checkout: str | None = None):
        """Initialize the fake DB state.

        Parameters
        ----------
        task_to_checkout : str | None
            Task ID returned by ``check_out_task_with_capability``.
        """
        self.task_to_checkout = task_to_checkout
        self.checkout_calls: list[object] = []
        self.mark_calls: list[dict[str, object]] = []
        self.add_calls: list[dict[str, object]] = []
        self.summary_rows: list[dict[str, int | str]] = []

    def check_out_task_with_capability(self, capability: object) -> str | None:
        """Return a preconfigured task while tracking checkout calls.

        Parameters
        ----------
        capability : object
            Capability requested by the command.

        Returns
        -------
        str | None
            Configured task ID or ``None``.
        """
        self.checkout_calls.append(capability)
        return self.task_to_checkout

    def mark_task_completed(self, taskid_value: str, success: bool) -> None:
        """Record mark-completed calls.

        Parameters
        ----------
        taskid_value : str
            Task ID being marked.
        success : bool
            Whether the task completed successfully.
        """
        self.mark_calls.append({"taskid": taskid_value, "success": success})

    def add_task_with_capability(
        self,
        taskid: str,
        requirements: list[str],
        max_tries: int,
        capability: str,
    ) -> None:
        """Record add-task calls.

        Parameters
        ----------
        taskid : str
            Task identifier.
        requirements : list[str]
            Upstream task requirements.
        max_tries : int
            Retry limit.
        capability : str
            Capability label.
        """
        self.add_calls.append(
            {
                "taskid": taskid,
                "requirements": requirements,
                "max_tries": max_tries,
                "capability": capability,
            }
        )

    def get_status_summary(self) -> list[dict[str, int | str]]:
        """Return status summary rows.

        Returns
        -------
        list[dict[str, int | str]]
            Stored status rows.
        """
        return self.summary_rows


def test_setup_task_status_db_uses_filename_when_db_path_is_provided(monkeypatch):
    captured: dict[str, Path] = {}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename: Path):
            captured["filename"] = filename
            return "db-from-file"

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)

    db = cli_module._setup_task_status_db(" custom.db ")

    assert db == "db-from-file"
    assert captured["filename"] == Path("custom.db")


def test_setup_task_status_db_uses_turso_env_when_available(monkeypatch):
    captured: dict[str, str] = {}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename: Path):
            raise AssertionError("from_filename should not be called")

        @classmethod
        def from_environment_variables(cls, db_url: str, auth_token: str):
            captured["db_url"] = db_url
            captured["auth_token"] = auth_token
            return "db-from-env"

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setenv("TURSO_DATABASE_URL", "libsql://example.turso.io")
    monkeypatch.setenv("TURSO_AUTH_TOKEN", "token")

    db = cli_module._setup_task_status_db(None)

    assert db == "db-from-env"
    assert captured == {
        "db_url": "libsql://example.turso.io",
        "auth_token": "token",
    }


def test_aws_launch_with_no_tasks_prints_message(monkeypatch, capsys):
    fake_db = _FakeTaskDB(task_to_checkout=None)
    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)

    cli_module.aws_launch(Path("."))

    out = capsys.readouterr().out
    assert "No available launch tasks" in out


def test_aws_launch_processes_task_and_marks_success(monkeypatch):
    cloud_init_text = "#!/usr/bin/env bash\necho hello\n"
    cloud_init_b64 = base64.b64encode(cloud_init_text.encode("utf-8")).decode("ascii")
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:"
        f"{cloud_init_b64}:"
        "12345678-1234-5678-1234-567812345678"
    )

    fake_db = _FakeTaskDB(task_to_checkout=taskid)
    launch_calls: list[dict[str, object]] = []

    def _fake_launch_ec2_instance(instance_type, **kwargs):
        launch_calls.append({"instance_type": instance_type, **kwargs})
        return "i-1234567890abcdef0"

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setattr(cli_module, "launch_ec2_instance", _fake_launch_ec2_instance)
    monkeypatch.setenv("EC2_KEY_NAME", "bench-key")
    monkeypatch.setenv("EC2_IAM_INSTANCE_PROFILE", "bench-profile")

    cli_module.aws_launch(Path("."))

    assert fake_db.mark_calls == [{"taskid": taskid, "success": True}]
    assert launch_calls == [
        {
            "instance_type": "g5.xlarge",
            "ami_id": "ami-0abc123456789def0",
            "region": "us-east-1",
            "user_data": cloud_init_text,
            "key_name": "bench-key",
            "instance_profile_name": "bench-profile",
            "provider_name": "aws",
        }
    ]


def test_aws_launch_marks_failed_when_launch_raises(monkeypatch):
    taskid = "us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(task_to_checkout=taskid)

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda _instance_type, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        cli_module.aws_launch(Path("."))

    assert fake_db.mark_calls == [{"taskid": taskid, "success": False}]


def test_aws_worker_launch_capability_delegates_to_aws_launch(monkeypatch):
    fake_db = _FakeTaskDB(task_to_checkout="dummy-task")
    delegated: list[Path | None] = []

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setattr(cli_module, "aws_launch", lambda db_path=None: delegated.append(db_path))

    cli_module.aws_worker(WorkerCapability.LAUNCH, Path("."))

    assert delegated == [Path(".")]
    assert fake_db.checkout_calls == [WorkerCapability.LAUNCH.value]


def test_aws_worker_bench_success_runs_benchmark_and_marks_success(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(task_to_checkout=taskid)
    run_calls: list[dict[str, object]] = []

    def _fake_run_benchmark(**kwargs):
        run_calls.append(kwargs)

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setattr(cli_module, "run_benchmark", _fake_run_benchmark)
    monkeypatch.setenv("S3_BUCKET", "benchmark-results")

    cli_module.aws_worker(WorkerCapability.G5, Path("."), Path("/tmp/bench"))

    assert run_calls == [
        {
            "benchmark_repo_path": Path("/tmp/bench"),
            "s3_bucket": "benchmark-results",
            "task_id": taskid,
            "benchmark_kind": BenchmarkKind.MD,
        }
    ]
    assert fake_db.mark_calls == [{"taskid": taskid, "success": True}]


def test_aws_worker_bench_missing_s3_bucket_marks_failure(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(task_to_checkout=taskid)

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.delenv("S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="S3_BUCKET environment variable is required"):
        cli_module.aws_worker(WorkerCapability.G5, Path("."), Path("/tmp/bench"))

    assert fake_db.mark_calls == [{"taskid": taskid, "success": False}]


def test_aws_worker_bench_invalid_task_id_marks_failure(monkeypatch):
    fake_db = _FakeTaskDB(task_to_checkout="bench:invalid")

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)

    with pytest.raises(ValueError, match="Invalid bench task ID format"):
        cli_module.aws_worker(WorkerCapability.G5, Path("."), Path("/tmp/bench"))

    assert fake_db.mark_calls == [{"taskid": "bench:invalid", "success": False}]


def test_aws_worker_bench_failure_wraps_error_and_marks_failure(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(task_to_checkout=taskid)

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setenv("S3_BUCKET", "benchmark-results")
    monkeypatch.setattr(
        cli_module,
        "run_benchmark",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("benchmark exploded")),
    )

    with pytest.raises(ValueError, match="Bench task .* failed: benchmark exploded"):
        cli_module.aws_worker(WorkerCapability.G5, Path("."), Path("/tmp/bench"))

    assert fake_db.mark_calls == [{"taskid": taskid, "success": False}]


def test_aws_worker_bench_failure_and_mark_failed_failure_wraps_both(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(task_to_checkout=taskid)

    def _mark_task_completed(_taskid: str, success: bool) -> None:
        if success is False:
            raise RuntimeError("db down")

    fake_db.mark_task_completed = _mark_task_completed  # type: ignore[method-assign]

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setenv("S3_BUCKET", "benchmark-results")
    monkeypatch.setattr(
        cli_module,
        "run_benchmark",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("benchmark exploded")),
    )

    with pytest.raises(ValueError, match="could not be marked as failed"):
        cli_module.aws_worker(WorkerCapability.G5, Path("."), Path("/tmp/bench"))


def test_aws_worker_bench_success_but_mark_success_failure_raises(monkeypatch):
    taskid = "bench:md:us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    fake_db = _FakeTaskDB(task_to_checkout=taskid)

    def _mark_task_completed(_taskid: str, success: bool) -> None:
        if success is True:
            raise RuntimeError("write failed")

    fake_db.mark_task_completed = _mark_task_completed  # type: ignore[method-assign]

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setenv("S3_BUCKET", "benchmark-results")
    monkeypatch.setattr(cli_module, "run_benchmark", lambda **_kwargs: None)

    with pytest.raises(ValueError, match="could not be marked as succeeded"):
        cli_module.aws_worker(WorkerCapability.G5, Path("."), Path("/tmp/bench"))


def test_create_aws_adds_launch_and_bench_tasks(monkeypatch, capsys):
    fake_db = _FakeTaskDB()
    instance_validation_calls: list[dict[str, str]] = []
    ami_validation_calls: list[dict[str, str]] = []
    cloud_init_calls: list[dict[str, object]] = []

    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)
    monkeypatch.setattr(
        cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region, provider_name="aws": instance_validation_calls.append(
            {
                "instance_type": instance_type,
                "region": region,
                "provider_name": provider_name,
            }
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "validate_launch_ami",
        lambda ami_id, region, provider_name="aws": ami_validation_calls.append(
            {"ami_id": ami_id, "region": region, "provider_name": provider_name}
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "_resolve_bench_worker_capability",
        lambda _instance_type: WorkerCapability.G5,
    )

    def _fake_read_cloud_init_file_as_base64(cloud_init_file, extra_vars=None):
        cloud_init_calls.append(
            {"cloud_init_file": cloud_init_file, "extra_vars": extra_vars}
        )
        return "cloud-init-b64"

    monkeypatch.setattr(
        cli_module,
        "_read_cloud_init_file_as_base64",
        _fake_read_cloud_init_file_as_base64,
    )
    monkeypatch.setattr(cli_module, "_build_task_id", lambda *_args, **_kwargs: "launch-task")
    monkeypatch.setattr(
        cli_module,
        "_build_bench_task_id",
        lambda *_args, **_kwargs: "bench-task",
    )

    cli_module.create_aws(
        instance_type=" G5.XLARGE ",
        region=" us-east-1 ",
        ami_id=" AMI-0ABC123456789DEF0 ",
        cloud_init_file=Path("cloud-init.sh"),
        db_path=Path("."),
        benchmark_kind=BenchmarkKind.MD,
        s3_bucket="bench-results",
    )

    out = capsys.readouterr().out

    assert instance_validation_calls == [
        {
            "instance_type": "g5.xlarge",
            "region": "us-east-1",
            "provider_name": "aws",
        }
    ]
    assert ami_validation_calls == [
        {
            "ami_id": "ami-0abc123456789def0",
            "region": "us-east-1",
            "provider_name": "aws",
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
    assert "launch-task" in out
    assert "Created benchmark task" in out


def test_analyze_requires_s3_bucket():
    with pytest.raises(ValueError, match="S3 bucket is required"):
        cli_module.analyze(s3_bucket=None, s3_prefix="runs/")


def test_analyze_fetches_and_prints_results(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        cli_module,
        "fetch_and_analyze_results",
        lambda bucket, prefix: captured.setdefault("records", [{"bucket": bucket, "prefix": prefix}]),
    )
    monkeypatch.setattr(
        cli_module,
        "print_results_table",
        lambda records: captured.setdefault("printed", records),
    )

    cli_module.analyze(s3_bucket="my-bucket", s3_prefix="runs/2026/")

    assert captured["records"] == [{"bucket": "my-bucket", "prefix": "runs/2026/"}]
    assert captured["printed"] == [{"bucket": "my-bucket", "prefix": "runs/2026/"}]


def test_status_prints_summary_table(monkeypatch, capsys):
    fake_db = _FakeTaskDB()
    fake_db.summary_rows = [
        {
            "capability": "g5",
            "status": cli_module.TaskStatus.AVAILABLE.value,
            "count": 2,
        },
        {
            "capability": "g5",
            "status": cli_module.TaskStatus.COMPLETED.value,
            "count": 1,
        },
    ]
    monkeypatch.setattr(cli_module, "_setup_task_status_db", lambda _db_path: fake_db)

    cli_module.status(db_path="task_status.db")

    out = capsys.readouterr().out
    assert "Capability" in out
    assert "g5" in out


def test_print_status_table_with_no_rows(capsys):
    cli_module._print_status_table([])

    out = capsys.readouterr().out
    assert "No tasks found." in out
