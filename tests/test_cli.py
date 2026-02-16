import uuid
from pathlib import Path

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

import benchmarking_orchestration as cli_module
import benchmarking_orchestration.aws as aws_module


@pytest.fixture(autouse=True)
def _aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def _build_fake_task_db(store):
    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            store["db_paths"].append(filename)
            return cls()

        def add_task_with_type(self, taskid, requirements, max_tries, task_type):
            store["tasks"].append(
                {
                    "taskid": taskid,
                    "requirements": requirements,
                    "max_tries": max_tries,
                    "task_type": task_type,
                }
            )

    return _FakeTaskStatusDB


def test_cli_no_args_shows_help_and_lists_worker():
    runner = CliRunner()
    result = runner.invoke(cli_module.cli, [])

    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "worker" in result.output
    assert "create-launch-task" in result.output
    assert "quota" not in result.output.lower()


def test_worker_with_launch_task_capability_exits_success_when_no_tasks(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "checkout_caps": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            store["db_paths"].append(filename)
            return cls()

        def check_out_task_with_type(self, task_type):
            store["checkout_caps"].append(task_type)
            return None

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    result = runner.invoke(cli_module.cli, ["worker", "--launch-task"])

    assert result.exit_code == 0
    assert store["db_paths"] == [Path("task_status.db")]
    assert store["checkout_caps"] == ["ec2-launch"]
    assert "No available ec2-launch tasks." in result.output


def test_worker_launches_task_and_marks_success(monkeypatch):
    runner = CliRunner()
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:"
        "12345678-1234-5678-1234-567812345678"
    )
    store = {
        "db_paths": [],
        "checkout_caps": [],
        "mark_calls": [],
        "launch_calls": [],
    }

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            store["db_paths"].append(filename)
            return cls()

        def check_out_task_with_type(self, task_type):
            store["checkout_caps"].append(task_type)
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    def _fake_launch_ec2_instance(instance_type, ami_id, region):
        store["launch_calls"].append(
            {"instance_type": instance_type, "ami_id": ami_id, "region": region}
        )
        return "i-1234567890abcdef0"

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(cli_module, "launch_ec2_instance", _fake_launch_ec2_instance)
    result = runner.invoke(cli_module.cli, ["worker", "--launch-task"])

    assert result.exit_code == 0
    assert store["db_paths"] == [Path("task_status.db")]
    assert store["checkout_caps"] == ["ec2-launch"]
    assert store["launch_calls"] == [
        {
            "instance_type": "g5.xlarge",
            "ami_id": "ami-0abc123456789def0",
            "region": "us-east-1",
        }
    ]
    assert store["mark_calls"] == [{"taskid": taskid, "success": True}]
    assert "Processed launch task" in result.output


def test_worker_marks_failure_when_launch_raises(monkeypatch):
    runner = CliRunner()
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:"
        "12345678-1234-5678-1234-567812345678"
    )
    store = {"mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_type(self, task_type):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    result = runner.invoke(cli_module.cli, ["worker", "--launch-task"])

    assert result.exit_code != 0
    assert "Failed to process launch task" in result.output
    assert "boom" in result.output
    assert store["mark_calls"] == [{"taskid": taskid, "success": False}]


def test_worker_marks_failure_when_taskid_is_malformed(monkeypatch):
    runner = CliRunner()
    taskid = "bad-task-id"
    store = {"mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_type(self, task_type):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region: (_ for _ in ()).throw(
            AssertionError("launch helper should not be called for malformed task ID")
        ),
    )
    result = runner.invoke(cli_module.cli, ["worker", "--launch-task"])

    assert result.exit_code != 0
    assert "Invalid launch task ID format" in result.output
    assert store["mark_calls"] == [{"taskid": taskid, "success": False}]


def test_worker_parses_legacy_taskid_and_uses_default_ami(monkeypatch):
    runner = CliRunner()
    taskid = "us-east-1:g5.xlarge:12345678-1234-5678-1234-567812345678"
    store = {"launch_calls": [], "mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_type(self, task_type):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    def _fake_launch_ec2_instance(instance_type, ami_id, region):
        store["launch_calls"].append(
            {"instance_type": instance_type, "ami_id": ami_id, "region": region}
        )
        return "i-1234567890abcdef0"

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(cli_module, "launch_ec2_instance", _fake_launch_ec2_instance)

    result = runner.invoke(cli_module.cli, ["worker", "--launch-task"])

    assert result.exit_code == 0
    assert store["launch_calls"] == [
        {
            "instance_type": "g5.xlarge",
            "ami_id": aws_module.DEFAULT_LAUNCH_AMI_ID,
            "region": "us-east-1",
        }
    ]
    assert store["mark_calls"] == [{"taskid": taskid, "success": True}]


def test_worker_without_capabilities_fails():
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["worker"])

    assert result.exit_code != 0
    assert "At least one worker capability must be enabled. Use --launch-task." in result.output


def test_worker_explicit_no_launch_task_fails():
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["worker", "--no-launch-task"])

    assert result.exit_code != 0
    assert "At least one worker capability must be enabled. Use --launch-task." in result.output


def test_create_launch_task_success_uses_defaults_and_writes_task(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    boto3_calls = []
    real_boto3_client = boto3.client

    with mock_aws():

        def _moto_boto3_client(service_name, region_name):
            boto3_calls.append(
                {"service_name": service_name, "region_name": region_name}
            )
            return real_boto3_client(service_name, region_name=region_name)

        monkeypatch.setattr(aws_module.boto3, "client", _moto_boto3_client)
        monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))
        monkeypatch.setattr(
            cli_module.uuid,
            "uuid4",
            lambda: uuid.UUID("12345678-1234-5678-1234-567812345678"),
        )

        result = runner.invoke(
            cli_module.cli, ["create-launch-task", "--instance-type", "G5.XLARGE"]
        )

        assert result.exit_code == 0
        assert store["db_paths"] == [Path("task_status.db")]
        assert boto3_calls == [{"service_name": "ec2", "region_name": "us-east-1"}]
        assert len(store["tasks"]) == 1

        created = store["tasks"][0]
        assert created["requirements"] == []
        assert created["max_tries"] == 1
        assert created["task_type"] == "ec2-launch"
        assert (
            created["taskid"] == "us-east-1:g5.xlarge:"
            f"{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
            "12345678-1234-5678-1234-567812345678"
        )
        assert (
            "us-east-1:g5.xlarge:"
            f"{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
            "12345678-1234-5678-1234-567812345678"
            in result.output
        )


def test_create_launch_task_rejects_non_g_or_vt_without_aws_or_db_calls(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    boto3_calls = []

    def _fake_boto3_client(service_name, region_name):
        boto3_calls.append({"service_name": service_name, "region_name": region_name})
        raise AssertionError(
            "AWS client should not be called for non G/VT instance types"
        )

    monkeypatch.setattr(aws_module.boto3, "client", _fake_boto3_client)
    monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))

    result = runner.invoke(
        cli_module.cli,
        ["create-launch-task", "--instance-type", "c6a.large"],
    )

    assert result.exit_code != 0
    assert "G/VT family" in result.output
    assert boto3_calls == []
    assert store["db_paths"] == []
    assert store["tasks"] == []


def test_create_launch_task_rejects_invalid_aws_instance_type(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    real_boto3_client = boto3.client

    with mock_aws():
        monkeypatch.setattr(
            aws_module.boto3,
            "client",
            lambda service_name, region_name: real_boto3_client(
                service_name, region_name=region_name
            ),
        )
        monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))

        result = runner.invoke(
            cli_module.cli,
            ["create-launch-task", "--instance-type", "g999.thisdoesnotexist"],
        )

        assert result.exit_code != 0
        assert "Invalid or unavailable instance type" in result.output
        assert store["db_paths"] == []
        assert store["tasks"] == []


def test_create_launch_task_region_override_is_used(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    boto3_calls = []
    real_boto3_client = boto3.client

    with mock_aws():

        def _moto_boto3_client(service_name, region_name):
            boto3_calls.append(
                {"service_name": service_name, "region_name": region_name}
            )
            return real_boto3_client(service_name, region_name=region_name)

        monkeypatch.setattr(aws_module.boto3, "client", _moto_boto3_client)
        monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))
        monkeypatch.setattr(
            cli_module.uuid,
            "uuid4",
            lambda: uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        )

        result = runner.invoke(
            cli_module.cli,
            [
                "create-launch-task",
                "--instance-type",
                "vt1.3xlarge",
                "--region",
                "us-west-2",
                "--db-path",
                "custom.db",
                "--max-tries",
                "3",
            ],
        )

        assert result.exit_code == 0
        assert boto3_calls == [{"service_name": "ec2", "region_name": "us-west-2"}]
        assert store["db_paths"] == [Path("custom.db")]
        assert len(store["tasks"]) == 1

        created = store["tasks"][0]
        assert created["max_tries"] == 3
        assert created["task_type"] == "ec2-launch"
        assert created["taskid"].startswith(
            f"us-west-2:vt1.3xlarge:{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
        )


def test_create_launch_task_ami_override_is_used(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    real_boto3_client = boto3.client

    with mock_aws():
        monkeypatch.setattr(
            aws_module.boto3,
            "client",
            lambda service_name, region_name: real_boto3_client(
                service_name, region_name=region_name
            ),
        )
        monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))
        monkeypatch.setattr(
            cli_module.uuid,
            "uuid4",
            lambda: uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        )

        result = runner.invoke(
            cli_module.cli,
            [
                "create-launch-task",
                "--instance-type",
                "g5.xlarge",
                "--ami-id",
                "ami-0abc123456789def0",
            ],
        )

        assert result.exit_code == 0
        assert len(store["tasks"]) == 1
        assert (
            store["tasks"][0]["taskid"]
            == "us-east-1:g5.xlarge:ami-0abc123456789def0:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        assert "with AMI 'ami-0abc123456789def0'" in result.output


def test_create_launch_task_re_raises_validation_error_as_click_exception(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}

    monkeypatch.setattr(
        cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))

    result = runner.invoke(
        cli_module.cli,
        ["create-launch-task", "--instance-type", "g5.xlarge"],
    )

    assert result.exit_code != 0
    assert "boom" in result.output
    assert store["db_paths"] == []
    assert store["tasks"] == []
