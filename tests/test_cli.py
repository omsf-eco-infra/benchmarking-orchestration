import base64
import uuid
from pathlib import Path
import re

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


def _stub_ami_validation(monkeypatch):
    monkeypatch.setattr(cli_module, "validate_launch_ami", lambda ami_id, region: None)


def _build_fake_task_db(store):
    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            store["db_paths"].append(filename)
            return cls()

        def add_task_with_capability(self, taskid, requirements, max_tries, capability):
            store["tasks"].append(
                {
                    "taskid": taskid,
                    "requirements": requirements,
                    "max_tries": max_tries,
                    "capability": capability,
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


def test_worker_with_launch_capability_exits_success_when_no_tasks(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "checkout_caps": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            store["db_paths"].append(filename)
            return cls()

        def check_out_task_with_capability(self, capability):
            store["checkout_caps"].append(capability)
            return None

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

    assert result.exit_code == 0
    assert store["db_paths"] == [Path("task_status.db")]
    assert store["checkout_caps"] == ["launch"]
    assert "No available launch tasks." in result.output


def test_worker_capability_is_case_insensitive(monkeypatch):
    runner = CliRunner()
    store = {"checkout_caps": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_capability(self, capability):
            store["checkout_caps"].append(capability)
            return None

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    result = runner.invoke(cli_module.cli, ["worker", "--capability", "LAUNCH"])

    assert result.exit_code == 0
    assert store["checkout_caps"] == ["launch"]


def test_worker_launches_task_and_marks_success(monkeypatch):
    runner = CliRunner()
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
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

        def check_out_task_with_capability(self, capability):
            store["checkout_caps"].append(capability)
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    def _fake_launch_ec2_instance(instance_type, ami_id, region, user_data=None):
        store["launch_calls"].append(
            {
                "instance_type": instance_type,
                "ami_id": ami_id,
                "region": region,
                "user_data": user_data,
            }
        )
        return "i-1234567890abcdef0"

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(cli_module, "launch_ec2_instance", _fake_launch_ec2_instance)
    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

    assert result.exit_code == 0
    assert store["db_paths"] == [Path("task_status.db")]
    assert store["checkout_caps"] == ["launch"]
    assert store["launch_calls"] == [
        {
            "instance_type": "g5.xlarge",
            "ami_id": "ami-0abc123456789def0",
            "region": "us-east-1",
            "user_data": None,
        }
    ]
    assert store["mark_calls"] == [{"taskid": taskid, "success": True}]
    assert "Processed launch task" in result.output


def test_worker_marks_failure_when_launch_raises(monkeypatch):
    runner = CliRunner()
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:12345678-1234-5678-1234-567812345678"
    )
    store = {"mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_capability(self, capability):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region, user_data=None: (_ for _ in ()).throw(
            RuntimeError("boom")
        ),
    )
    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

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

        def check_out_task_with_capability(self, capability):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region, user_data=None: (_ for _ in ()).throw(
            AssertionError("launch helper should not be called for malformed task ID")
        ),
    )
    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

    assert result.exit_code != 0
    assert "Invalid launch task ID format" in result.output
    assert store["mark_calls"] == [{"taskid": taskid, "success": False}]


def test_worker_marks_failure_for_legacy_three_part_taskid(monkeypatch):
    runner = CliRunner()
    taskid = "us-east-1:g5.xlarge:12345678-1234-5678-1234-567812345678"
    store = {"mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_capability(self, capability):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region, user_data=None: (_ for _ in ()).throw(
            AssertionError(
                "launch helper should not be called for legacy 3-part task ID"
            )
        ),
    )

    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

    assert result.exit_code != 0
    assert "Invalid launch task ID format" in result.output
    assert store["mark_calls"] == [{"taskid": taskid, "success": False}]


def test_worker_launches_task_with_cloud_init_payload(monkeypatch):
    runner = CliRunner()
    cloud_init_text = "#cloud-config\nruncmd:\n  - echo hello\n"
    cloud_init_b64 = base64.b64encode(cloud_init_text.encode("utf-8")).decode("ascii")
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:"
        f"{cloud_init_b64}:"
        "12345678-1234-5678-1234-567812345678"
    )
    store = {"launch_calls": [], "mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_capability(self, capability):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    def _fake_launch_ec2_instance(instance_type, ami_id, region, user_data=None):
        store["launch_calls"].append(
            {
                "instance_type": instance_type,
                "ami_id": ami_id,
                "region": region,
                "user_data": user_data,
            }
        )
        return "i-1234567890abcdef0"

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(cli_module, "launch_ec2_instance", _fake_launch_ec2_instance)

    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

    assert result.exit_code == 0
    assert store["launch_calls"] == [
        {
            "instance_type": "g5.xlarge",
            "ami_id": "ami-0abc123456789def0",
            "region": "us-east-1",
            "user_data": cloud_init_text,
        }
    ]
    assert store["mark_calls"] == [{"taskid": taskid, "success": True}]


def test_worker_marks_failure_when_cloud_init_payload_is_invalid(monkeypatch):
    runner = CliRunner()
    taskid = (
        "us-east-1:g5.xlarge:ami-0abc123456789def0:"
        "not-valid-base64:"
        "12345678-1234-5678-1234-567812345678"
    )
    store = {"mark_calls": []}

    class _FakeTaskStatusDB:
        @classmethod
        def from_filename(cls, filename):
            return cls()

        def check_out_task_with_capability(self, capability):
            return taskid

        def mark_task_completed(self, taskid_value, success):
            store["mark_calls"].append({"taskid": taskid_value, "success": success})

    monkeypatch.setattr(cli_module, "TaskStatusDB", _FakeTaskStatusDB)
    monkeypatch.setattr(
        cli_module,
        "launch_ec2_instance",
        lambda instance_type, ami_id, region, user_data=None: (_ for _ in ()).throw(
            AssertionError(
                "launch helper should not run for invalid cloud-init payload"
            )
        ),
    )

    result = runner.invoke(cli_module.cli, ["worker", "--capability", "launch"])

    assert result.exit_code != 0
    assert "Invalid cloud-init payload encoding in launch task ID" in result.output
    assert store["mark_calls"] == [{"taskid": taskid, "success": False}]


def test_worker_requires_capability_flag():
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["worker"])

    assert result.exit_code != 0
    assert "Missing option '--capability'" in result.output


def test_worker_rejects_invalid_capability_value():
    runner = CliRunner()

    result = runner.invoke(
        cli_module.cli, ["worker", "--capability", "not-a-real-capability"]
    )

    assert result.exit_code != 0
    assert "Invalid value for '--capability'" in result.output
    assert "launch" in result.output


def test_worker_rejects_legacy_launch_task_flag():
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["worker", "--launch-task"])

    assert result.exit_code != 0
    assert "No such option: --launch-task" in result.output


def test_create_launch_task_success_uses_defaults_and_writes_task(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    boto3_calls = []
    real_boto3_client = boto3.client

    _stub_ami_validation(monkeypatch)

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
        assert len(store["tasks"]) == 2

        launch_task = store["tasks"][0]
        assert launch_task["requirements"] == []
        assert launch_task["max_tries"] == 1
        assert launch_task["capability"] == "launch"
        assert (
            launch_task["taskid"] == "us-east-1:g5.xlarge:"
            f"{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
            "12345678-1234-5678-1234-567812345678"
        )
        bench_task = store["tasks"][1]
        assert bench_task["taskid"] == f"bench:{launch_task['taskid']}"
        assert bench_task["requirements"] == [launch_task["taskid"]]
        assert bench_task["max_tries"] == 1
        assert bench_task["capability"] == "g5"
        assert (
            "us-east-1:g5.xlarge:"
            f"{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
            "12345678-1234-5678-1234-567812345678" in result.output
        )


def test_create_launch_task_with_cloud_init_file_embeds_payload(monkeypatch, tmp_path):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    cloud_init_path = tmp_path / "cloud-init.yaml"
    cloud_init_content = "#cloud-config\nruncmd:\n  - echo hello\n"
    cloud_init_path.write_text(cloud_init_content)

    monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))
    monkeypatch.setattr(
        cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region: None,
    )
    monkeypatch.setattr(cli_module, "validate_launch_ami", lambda ami_id, region: None)
    monkeypatch.setattr(
        cli_module.uuid,
        "uuid4",
        lambda: uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
    )

    result = runner.invoke(
        cli_module.cli,
        [
            "create-launch-task",
            "--instance-type",
            "g5.xlarge",
            "--cloud-init-file",
            str(cloud_init_path),
        ],
    )

    assert result.exit_code == 0
    assert store["db_paths"] == [Path("task_status.db")]
    assert len(store["tasks"]) == 2
    encoded_payload = base64.b64encode(cloud_init_content.encode("utf-8")).decode(
        "ascii"
    )
    expected_taskid = (
        "us-east-1:g5.xlarge:"
        f"{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
        f"{encoded_payload}:"
        "dddddddd-dddd-dddd-dddd-dddddddddddd"
    )
    assert store["tasks"][0]["taskid"] == expected_taskid
    assert store["tasks"][1]["taskid"] == f"bench:{expected_taskid}"
    assert store["tasks"][1]["requirements"] == [expected_taskid]
    assert store["tasks"][1]["capability"] == "g5"
    assert expected_taskid in result.output


def test_create_launch_task_with_missing_cloud_init_file_returns_error(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}

    monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))
    monkeypatch.setattr(
        cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region: None,
    )
    monkeypatch.setattr(cli_module, "validate_launch_ami", lambda ami_id, region: None)

    result = runner.invoke(
        cli_module.cli,
        [
            "create-launch-task",
            "--instance-type",
            "g5.xlarge",
            "--cloud-init-file",
            "does-not-exist-cloud-init.yaml",
        ],
    )

    assert result.exit_code != 0
    assert "Unable to read cloud-init file" in result.output
    assert store["db_paths"] == []
    assert store["tasks"] == []


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

    _stub_ami_validation(monkeypatch)

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
        assert len(store["tasks"]) == 2

        launch_task = store["tasks"][0]
        assert launch_task["max_tries"] == 3
        assert launch_task["capability"] == "launch"
        assert launch_task["taskid"].startswith(
            f"us-west-2:vt1.3xlarge:{aws_module.DEFAULT_LAUNCH_AMI_ID}:"
        )
        bench_task = store["tasks"][1]
        assert bench_task["taskid"] == f"bench:{launch_task['taskid']}"
        assert bench_task["requirements"] == [launch_task["taskid"]]
        assert bench_task["max_tries"] == 3
        assert bench_task["capability"] == "vt1"


def test_create_launch_task_ami_override_is_used(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    real_boto3_client = boto3.client

    _stub_ami_validation(monkeypatch)

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
        assert len(store["tasks"]) == 2
        assert (
            store["tasks"][0]["taskid"]
            == "us-east-1:g5.xlarge:ami-0abc123456789def0:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        assert (
            store["tasks"][1]["taskid"]
            == "bench:us-east-1:g5.xlarge:ami-0abc123456789def0:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        assert store["tasks"][1]["requirements"] == [store["tasks"][0]["taskid"]]
        assert store["tasks"][1]["capability"] == "g5"
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


def test_create_launch_task_validates_ami_with_normalized_values(monkeypatch):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}
    captured = []

    monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))
    monkeypatch.setattr(
        cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region: None,
    )

    def _capture_ami_validation(ami_id, region):
        captured.append({"ami_id": ami_id, "region": region})

    monkeypatch.setattr(cli_module, "validate_launch_ami", _capture_ami_validation)
    monkeypatch.setattr(
        cli_module.uuid,
        "uuid4",
        lambda: uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
    )

    result = runner.invoke(
        cli_module.cli,
        [
            "create-launch-task",
            "--instance-type",
            "g5.xlarge",
            "--region",
            " us-east-1 ",
            "--ami-id",
            " AMI-0ABC123456789DEF0 ",
        ],
    )

    assert result.exit_code == 0
    assert captured == [{"ami_id": "ami-0abc123456789def0", "region": "us-east-1"}]


def test_create_launch_task_re_raises_ami_validation_error_as_click_exception(
    monkeypatch,
):
    runner = CliRunner()
    store = {"db_paths": [], "tasks": []}

    monkeypatch.setattr(
        cli_module,
        "validate_launch_instance_type",
        lambda instance_type, region: None,
    )
    monkeypatch.setattr(
        cli_module,
        "validate_launch_ami",
        lambda ami_id, region: (_ for _ in ()).throw(RuntimeError("ami boom")),
    )
    monkeypatch.setattr(cli_module, "TaskStatusDB", _build_fake_task_db(store))

    result = runner.invoke(
        cli_module.cli,
        ["create-launch-task", "--instance-type", "g5.xlarge"],
    )

    assert result.exit_code != 0
    assert "ami boom" in result.output
    assert store["db_paths"] == []
    assert store["tasks"] == []


def test_smoke_launch_and_teardown_flow_in_moto(tmp_path):
    runner = CliRunner()
    db_path = tmp_path / "eco388-smoke.db"

    with mock_aws():
        ec2_client = boto3.client("ec2", region_name="us-east-1")
        ami_id = ec2_client.register_image(Name="eco388-smoke-ami")["ImageId"]

        create_result = runner.invoke(
            cli_module.cli,
            [
                "create-launch-task",
                "--instance-type",
                "g5.xlarge",
                "--ami-id",
                ami_id,
                "--db-path",
                str(db_path),
            ],
        )
        assert create_result.exit_code == 0

        worker_result = runner.invoke(
            cli_module.cli,
            ["worker", "--capability", "launch", "--db-path", str(db_path)],
        )
        assert worker_result.exit_code == 0
        match = re.search(r"instance '([^']+)'", worker_result.output)
        assert match is not None
        instance_id = match.group(1)

        describe_result = ec2_client.describe_instances(InstanceIds=[instance_id])
        state = describe_result["Reservations"][0]["Instances"][0]["State"]["Name"]
        assert state == "running"

        ec2_client.terminate_instances(InstanceIds=[instance_id])
        waiter = ec2_client.get_waiter("instance_terminated")
        waiter.wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 1, "MaxAttempts": 10}
        )
        final_result = ec2_client.describe_instances(InstanceIds=[instance_id])
        final_state = final_result["Reservations"][0]["Instances"][0]["State"]["Name"]
        assert final_state == "terminated"
