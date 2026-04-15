from __future__ import annotations

import benchmarking_orchestration.providers.salad_cli as salad_cli_module
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.providers.salad_cli import (
    SaladCLI,
    SaladWorkerCapability,
)
from pathlib import Path


class _FakeTaskDB:
    """Minimal fake task DB used by SaladCLI tests."""

    def __init__(self):
        self.add_calls: list[dict[str, object]] = []

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


class _FakeGpuClass:
    """Simple GPU class fixture returned by the fake SDK."""

    def __init__(
        self,
        *,
        id_: str,
        name: str,
        min_vcpu: int = 4,
        min_ram: int = 8192,
        min_storage: int = 2147483648,
    ):
        self.id_ = id_
        self.name = name
        self.min_vcpu = min_vcpu
        self.min_ram = min_ram
        self.min_storage = min_storage


class _FakeContainerResources:
    """Simple container resource wrapper for existing groups."""

    def __init__(self, gpu_classes: list[str]):
        self.gpu_classes = gpu_classes


class _FakeContainer:
    """Simple container wrapper for existing groups."""

    def __init__(self, image: str, gpu_classes: list[str]):
        self.image = image
        self.resources = _FakeContainerResources(gpu_classes)


class _FakeContainerGroup:
    """Simple existing group representation."""

    def __init__(self, name: str, image: str, gpu_classes: list[str]):
        self.name = name
        self.container = _FakeContainer(image, gpu_classes)


class _FakeOrganizationData:
    """Fake organization-data service."""

    def __init__(self, gpu_classes: list[_FakeGpuClass]):
        self.gpu_classes = gpu_classes
        self.list_calls: list[str] = []

    def list_gpu_classes(self, organization_name: str):
        self.list_calls.append(organization_name)
        return type("_GpuClassesList", (), {"items": self.gpu_classes})()


class _FakeContainerGroupsService:
    """Fake container-groups service."""

    def __init__(self, groups: list[_FakeContainerGroup]):
        self.groups = groups
        self.list_calls: list[dict[str, str]] = []
        self.create_calls: list[dict[str, object]] = []

    def list_container_groups(self, organization_name: str, project_name: str):
        self.list_calls.append(
            {
                "organization_name": organization_name,
                "project_name": project_name,
            }
        )
        return type("_ContainerGroupCollection", (), {"items": self.groups})()

    def create_container_group(
        self,
        *,
        request_body,
        organization_name: str,
        project_name: str,
    ):
        self.create_calls.append(
            {
                "request_body": request_body,
                "organization_name": organization_name,
                "project_name": project_name,
            }
        )
        return type("_ContainerGroup", (), {"name": request_body.name})()


class _FakeSaladCloudSdk:
    """Fake Salad SDK root client."""

    def __init__(
        self,
        api_key: str,
        *,
        gpu_classes: list[_FakeGpuClass],
        groups: list[_FakeContainerGroup],
    ):
        self.api_key = api_key
        self.organization_data = _FakeOrganizationData(gpu_classes)
        self.container_groups = _FakeContainerGroupsService(groups)


def test_create_creates_missing_container_group_and_queues_tasks(monkeypatch, capsys):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)
    fake_sdk = _FakeSaladCloudSdk(
        api_key="test-key",
        gpu_classes=[
            _FakeGpuClass(id_="gpu-class-id", name="RTX A5000 (24 GB)"),
        ],
        groups=[],
    )

    monkeypatch.setattr(
        salad_cli_module,
        "SaladCloudSdk",
        lambda api_key: fake_sdk,
    )
    monkeypatch.setattr(
        salad_cli_module.SaladCLI,
        "_launch_task_id",
        classmethod(lambda _cls, _gpu_name, _image_name: "salad-task"),
    )
    monkeypatch.setattr(
        salad_cli_module,
        "_build_bench_task_id",
        lambda *_args, **_kwargs: "bench-task",
    )

    SaladCLI().create(
        gpu_name=SaladWorkerCapability.RTXA5000,
        image_name="ghcr.io/openforcefield/benchmark-worker:latest",
        benchmark_kind=BenchmarkKind.MD,
        config=config,
        salad_api_key="test-key",
        salad_org_name="bench-org",
    )

    out = capsys.readouterr().out

    assert fake_sdk.organization_data.list_calls == ["bench-org"]
    assert fake_sdk.container_groups.list_calls == [
        {"organization_name": "bench-org", "project_name": "default"}
    ]
    assert len(fake_sdk.container_groups.create_calls) == 1
    create_call = fake_sdk.container_groups.create_calls[0]
    request_body = create_call["request_body"]
    assert request_body.name == "salad-rtx-a5000-24-gb"
    assert (
        request_body.container.image == "ghcr.io/openforcefield/benchmark-worker:latest"
    )
    assert request_body.container.resources.gpu_classes == ["gpu-class-id"]
    assert request_body.container.resources.cpu == 4
    assert request_body.container.resources.memory == 8192
    assert request_body.container.resources.storage_amount == 2147483648
    assert request_body.replicas == 1
    assert request_body.autostart_policy is False
    assert fake_db.add_calls == [
        {
            "taskid": "salad-task",
            "requirements": [],
            "max_tries": 1,
            "capability": "salad:launch",
        },
        {
            "taskid": "bench-task",
            "requirements": ["salad-task"],
            "max_tries": 1,
            "capability": "salad:RTX A5000 (24 GB)",
        },
    ]
    assert "Created Salad container group 'salad-rtx-a5000-24-gb'" in out


def test_create_reuses_matching_existing_container_group(monkeypatch, capsys):
    fake_db = _FakeTaskDB()
    config = _FakeConfig(fake_db)
    fake_sdk = _FakeSaladCloudSdk(
        api_key="test-key",
        gpu_classes=[
            _FakeGpuClass(id_="gpu-class-id", name="RTX A5000 (24 GB)"),
        ],
        groups=[
            _FakeContainerGroup(
                name="salad-rtx-a5000-24-gb",
                image="ghcr.io/openforcefield/benchmark-worker:latest",
                gpu_classes=["gpu-class-id"],
            )
        ],
    )

    monkeypatch.setattr(
        salad_cli_module,
        "SaladCloudSdk",
        lambda api_key: fake_sdk,
    )
    monkeypatch.setattr(
        salad_cli_module.SaladCLI,
        "_launch_task_id",
        classmethod(lambda _cls, _gpu_name, _image_name: "salad-task"),
    )
    monkeypatch.setattr(
        salad_cli_module,
        "_build_bench_task_id",
        lambda *_args, **_kwargs: "bench-task",
    )

    SaladCLI().create(
        gpu_name=SaladWorkerCapability.RTXA5000,
        image_name="ghcr.io/openforcefield/benchmark-worker:latest",
        benchmark_kind=BenchmarkKind.MD,
        config=config,
        salad_api_key="test-key",
        salad_org_name="bench-org",
    )

    out = capsys.readouterr().out

    assert fake_sdk.container_groups.create_calls == []
    assert fake_db.add_calls[0]["capability"] == "salad:launch"
    assert fake_db.add_calls[1]["capability"] == "salad:RTX A5000 (24 GB)"
    assert "Reused Salad container group 'salad-rtx-a5000-24-gb'" in out


class _FakeTaskDBWithCheckout:
    """Fake task DB that supports checkout and mark completed."""

    def __init__(self, checkout_result: str | None):
        self.checkout_result = checkout_result
        self.checkout_calls: list[str] = []
        self.mark_completed_calls: list[tuple[str, bool]] = []

    def check_out_task_with_capability(self, capability: str) -> str | None:
        self.checkout_calls.append(capability)
        return self.checkout_result

    def mark_task_completed(self, taskid: str, success: bool) -> None:
        self.mark_completed_calls.append((taskid, success))


def test_worker_launch_capability_delegates_to_launch(monkeypatch, capsys):
    """Test that LAUNCH capability calls the launch method."""
    fake_db = _FakeTaskDBWithCheckout(checkout_result=None)
    config = _FakeConfig(fake_db)

    launch_called = []

    def fake_launch(*, config):
        launch_called.append(config)
        print("Launch called")

    cli = SaladCLI()
    monkeypatch.setattr(cli, "launch", fake_launch)

    cli.worker(
        capability=SaladWorkerCapability.LAUNCH,
        config=config,
    )

    out = capsys.readouterr().out
    assert "Launch called" in out
    assert launch_called == [config]


def test_worker_gpu_capability_no_task_available(monkeypatch, capsys):
    """Test GPU capability when no tasks are available."""
    fake_db = _FakeTaskDBWithCheckout(checkout_result=None)
    config = _FakeConfig(fake_db)

    cli = SaladCLI()
    cli.worker(
        capability=SaladWorkerCapability.RTXA5000,
        config=config,
    )

    out = capsys.readouterr().out
    assert fake_db.checkout_calls == ["salad:RTX A5000 (24 GB)"]
    assert "No available salad:RTX A5000 (24 GB) tasks." in out


def test_worker_gpu_capability_runs_benchmark(monkeypatch, capsys):
    """Test GPU capability runs benchmark and marks completion."""
    fake_db = _FakeTaskDBWithCheckout(
        checkout_result="bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678"
    )
    config = _FakeConfig(fake_db)

    run_benchmark_calls = []

    def fake_run_benchmark(*, benchmark_repo_path, s3_bucket, task_id, benchmark_kind):
        run_benchmark_calls.append(
            {
                "benchmark_repo_path": benchmark_repo_path,
                "s3_bucket": s3_bucket,
                "task_id": task_id,
                "benchmark_kind": benchmark_kind,
            }
        )

    monkeypatch.setattr(salad_cli_module, "run_benchmark", fake_run_benchmark)
    monkeypatch.setenv("S3_BUCKET", "test-bucket")

    cli = SaladCLI()
    cli.worker(
        capability=SaladWorkerCapability.RTXA5000,
        config=config,
        bench_repo_path=Path("/test/path"),
    )

    out = capsys.readouterr().out
    assert fake_db.checkout_calls == ["salad:RTX A5000 (24 GB)"]
    assert len(run_benchmark_calls) == 1
    assert run_benchmark_calls[0]["benchmark_repo_path"] == Path("/test/path")
    assert run_benchmark_calls[0]["s3_bucket"] == "test-bucket"
    assert (
        run_benchmark_calls[0]["task_id"]
        == "bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678"
    )
    assert run_benchmark_calls[0]["benchmark_kind"] == BenchmarkKind.MD
    assert fake_db.mark_completed_calls == [
        (
            "bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678",
            True,
        )
    ]
    assert "Processed bench task" in out


def test_worker_gpu_capability_missing_s3_bucket(monkeypatch, capsys):
    """Test GPU capability fails when S3_BUCKET is missing."""
    fake_db = _FakeTaskDBWithCheckout(
        checkout_result="bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678"
    )
    config = _FakeConfig(fake_db)

    monkeypatch.delenv("S3_BUCKET", raising=False)

    cli = SaladCLI()
    try:
        cli.worker(
            capability=SaladWorkerCapability.RTXA5000,
            config=config,
        )
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "S3_BUCKET environment variable is required" in str(exc)
        assert fake_db.mark_completed_calls == [
            (
                "bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678",
                False,
            )
        ]


def test_worker_gpu_capability_benchmark_failure(monkeypatch, capsys):
    """Test GPU capability marks task failed when benchmark fails."""
    fake_db = _FakeTaskDBWithCheckout(
        checkout_result="bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678"
    )
    config = _FakeConfig(fake_db)

    def fake_run_benchmark(*args, **kwargs):
        raise RuntimeError("Benchmark crashed")

    monkeypatch.setattr(salad_cli_module, "run_benchmark", fake_run_benchmark)
    monkeypatch.setenv("S3_BUCKET", "test-bucket")

    cli = SaladCLI()
    try:
        cli.worker(
            capability=SaladWorkerCapability.RTXA5000,
            config=config,
        )
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "Benchmark crashed" in str(exc)
        assert fake_db.mark_completed_calls == [
            (
                "bench:md:salad:RTX A5000 (24 GB):my-image:12345678-1234-5678-1234-567812345678",
                False,
            )
        ]
