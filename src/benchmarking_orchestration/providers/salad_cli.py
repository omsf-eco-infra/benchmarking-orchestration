from __future__ import annotations

import re
import uuid
from enum import StrEnum
from typing import Annotated, Optional

from cyclopts import App, Parameter
from salad_cloud_sdk import SaladCloudSdk
from salad_cloud_sdk.models.container_configuration import ContainerConfiguration
from salad_cloud_sdk.models.container_group_creation_request import (
    ContainerGroupCreationRequest,
)
from salad_cloud_sdk.models.container_restart_policy import ContainerRestartPolicy
from salad_cloud_sdk.models.create_container_resource_requirements import (
    CreateContainerResourceRequirements,
)
from salad_cloud_sdk.models.gpu_availability import GpuAvailability
from salad_cloud_sdk.models.gpu_availability_prototype import GpuAvailabilityPrototype
from salad_cloud_sdk.models.gpu_class import GpuClass
from salad_cloud_sdk.models.gpu_classes_list import GpuClassesList

from pathlib import Path

from cyclopts import validators

from benchmarking_orchestration.bench import run_benchmark
from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.providers.cli_protocol import Config, ProviderCLI
from benchmarking_orchestration.task_id import (
    _build_bench_task_id,
    _parse_bench_task_id,
)


class SaladWorkerCapability(StrEnum):
    """Supported Salad worker capabilities."""

    RTXA5000 = "salad:RTX A5000 (24 GB)"
    LAUNCH = "salad:launch"


class SaladCLI(ProviderCLI):
    """Provider-owned Cyclopts registration and handlers for Salad commands."""

    provider_name: str = "salad"
    _DEFAULT_PROJECT_NAME = "default"

    def register_cli(
        self,
        create_app: App,
        launch_app: App,
        worker_app: App,
    ) -> None:
        """Register provider-specific subcommands into Cyclopts app groups.

        Parameters
        ----------
        create_app : App
            Cyclopts app group used for task creation commands.
        launch_app : App
            Cyclopts app group used for launch-worker commands.
        worker_app : App
            Cyclopts app group used for worker commands.

        Returns
        -------
        None
            This method mutates app groups by registering command functions.
        """
        create_app.command(self.create, name=self.provider_name)
        launch_app.command(self.launch, name=self.provider_name)
        worker_app.command(self.worker, name=self.provider_name)

    @staticmethod
    def _gpu_display_name(gpu_name: SaladWorkerCapability) -> str:
        """Return the Salad GPU class display name for a capability.

        Parameters
        ----------
        gpu_name : SaladWorkerCapability
            Worker capability whose underlying GPU class should be resolved.

        Returns
        -------
        str
            Salad GPU class display name.
        """
        return gpu_name.value.split(":", maxsplit=1)[1]

    @classmethod
    def _container_group_name(cls, gpu_name: SaladWorkerCapability) -> str:
        """Build a deterministic container group name for a GPU capability.

        Parameters
        ----------
        gpu_name : SaladWorkerCapability
            GPU capability used to derive the group name.

        Returns
        -------
        str
            DNS-safe Salad container group name.
        """
        normalized_name = re.sub(
            r"[^a-z0-9]+", "-", cls._gpu_display_name(gpu_name).lower()
        )
        normalized_name = normalized_name.strip("-")
        group_name = f"salad-{normalized_name}"[:63].rstrip("-")
        if len(group_name) < 2:
            raise ValueError(
                f"Unable to build a valid container group name for '{gpu_name.value}'."
            )
        return group_name

    @classmethod
    def _launch_task_id(cls, gpu_name: SaladWorkerCapability, image_name: str) -> str:
        """Build a Salad launch task identifier.

        Parameters
        ----------
        gpu_name : SaladWorkerCapability
            GPU capability to encode in the task identifier.
        image_name : str
            Container image associated with the launch task.

        Returns
        -------
        str
            Task identifier in ``<capability>:<image_name>:<uuid4>`` format.
        """
        return f"{gpu_name.value}:{image_name}:{uuid.uuid4()}"

    @staticmethod
    def _build_container_group_request(
        *, gpu_class: GpuClass, gpu_name: SaladWorkerCapability, image_name: str
    ) -> ContainerGroupCreationRequest:
        """Create a Salad container-group request for a GPU worker pool.

        Parameters
        ----------
        gpu_class : GpuClass
            Resolved Salad GPU class metadata.
        gpu_name : SaladWorkerCapability
            Worker capability being provisioned.
        image_name : str
            Container image to run inside the group.

        Returns
        -------
        ContainerGroupCreationRequest
            SDK request model ready for ``create_container_group``.
        """
        resource_kwargs: dict[str, int | list[str]] = {
            "cpu": int(getattr(gpu_class, "min_vcpu", 1) or 1),
            "memory": int(getattr(gpu_class, "min_ram", 1024) or 1024),
            "gpu_classes": [gpu_class.id_],
        }
        min_storage = getattr(gpu_class, "min_storage", None)
        if min_storage:
            resource_kwargs["storage_amount"] = int(min_storage)

        display_name = re.sub(
            r"[^ ,-.0-9A-Za-z]+",
            " ",
            f"Benchmark worker - {SaladCLI._gpu_display_name(gpu_name)}",
        ).strip()

        return ContainerGroupCreationRequest(
            autostart_policy=False,
            container=ContainerConfiguration(
                image=image_name,
                image_caching=True,
                resources=CreateContainerResourceRequirements(**resource_kwargs),
            ),
            display_name=display_name,
            name=SaladCLI._container_group_name(gpu_name),
            replicas=1,
            restart_policy=ContainerRestartPolicy.NEVER,
        )

    def launch(
        self,
        *,
        config: Config | None,
        salad_api_key: Annotated[str, Parameter(env_var="SALAD_API_KEY")],
        salad_org_name: Annotated[str, Parameter(env_var="SALAD_ORG_NAME")],
    ) -> None:
        """Check GPU availability for the next queued Salad launch task.

        Parameters
        ----------
        config : Config | None
            Optional shared CLI configuration.
        salad_api_key : str
            Salad API key used to authenticate SDK calls.
        salad_org_name : str
            Salad organization used to resolve GPU classes and availability.
        """
        if config is None:
            config = Config()
        task_db = config.task_db
        sdk = SaladCloudSdk(api_key=salad_api_key)

        try:
            task: str | None = task_db.check_out_task_with_capability(
                SaladWorkerCapability.LAUNCH
            )
        except Exception as exc:
            raise RuntimeError(
                f"Unable to check out task from database '{task_db}': {exc}"
            ) from exc
        if task is None:
            print("No available launch tasks")
            return

        parts = task.split(":")
        gpu = parts[1]

        result: GpuClassesList = sdk.organization_data.list_gpu_classes(
            organization_name=salad_org_name
        )
        gpus = {gpu_item.name: gpu_item.id_ for gpu_item in result.items}
        if gpu not in gpus:
            raise ValueError(f"GPU {gpu} not available")

        request_body = GpuAvailabilityPrototype(gpu_classes=[gpus[gpu]])

        availability: GpuAvailability = sdk.organization_data.get_gpu_availability(
            request_body,
            organization_name=salad_org_name,
        )
        available = availability.available_gpu_high
        if available == 0:
            raise RuntimeError("No GPUs available at this time")

    def create(
        self,
        gpu_name: SaladWorkerCapability,
        image_name: str,
        benchmark_kind: BenchmarkKind = BenchmarkKind.BOTH,
        s3_bucket: Annotated[
            Optional[str], Parameter(env_var="BENCHMARK_S3_BUCKET")
        ] = None,
        *,
        config: Config | None = None,
        salad_api_key: Annotated[str, Parameter(env_var="SALAD_API_KEY")],
        salad_org_name: Annotated[str, Parameter(env_var="SALAD_ORG_NAME")],
    ) -> None:
        """Create launch and benchmark task entries in TaskStatusDB for Salad.

        Parameters
        ----------
        gpu_name : SaladWorkerCapability
            Requested Salad GPU capability.
        image_name : str
            Container image that the worker container group should run.
        benchmark_kind : BenchmarkKind, default=BenchmarkKind.BOTH
            Benchmark workload kind for the dependent bench task.
        s3_bucket : str | None, optional
            Preserved for CLI compatibility. Salad create does not use it yet.
        config : Config | None, optional
            Shared CLI configuration that provides the task database.
        salad_api_key : str
            Salad API key used to authenticate SDK calls.
        salad_org_name : str
            Salad organization name used for GPU and container-group operations.
        """
        del s3_bucket
        if config is None:
            config = Config()
        task_db = config.task_db

        normalized_image_name = image_name.strip()
        if not normalized_image_name:
            raise ValueError("image_name cannot be empty.")

        gpu_display_name = self._gpu_display_name(gpu_name)
        sdk = SaladCloudSdk(api_key=salad_api_key)
        result: GpuClassesList = sdk.organization_data.list_gpu_classes(
            organization_name=salad_org_name
        )
        gpu_class = next(
            (
                candidate
                for candidate in result.items
                if candidate.name == gpu_display_name
            ),
            None,
        )
        if gpu_class is None:
            raise ValueError(f"GPU {gpu_name.value} not available")

        container_groups = sdk.container_groups.list_container_groups(
            organization_name=salad_org_name,
            project_name=self._DEFAULT_PROJECT_NAME,
        )
        group_name = self._container_group_name(gpu_name)
        existing_group = next(
            (group for group in container_groups.items if group.name == group_name),
            None,
        )

        if existing_group is None:
            request_body = self._build_container_group_request(
                gpu_class=gpu_class,
                gpu_name=gpu_name,
                image_name=normalized_image_name,
            )
            sdk.container_groups.create_container_group(
                request_body=request_body,
                organization_name=salad_org_name,
                project_name=self._DEFAULT_PROJECT_NAME,
            )
            group_status = "Created"
        else:
            configured_gpu_classes = list(
                getattr(
                    getattr(existing_group.container, "resources", None),
                    "gpu_classes",
                    [],
                )
                or []
            )
            if gpu_class.id_ not in configured_gpu_classes:
                raise ValueError(
                    f"Existing container group '{group_name}' is not configured for GPU '{gpu_display_name}'."
                )
            configured_image = getattr(existing_group.container, "image", None)
            if configured_image and configured_image != normalized_image_name:
                raise ValueError(
                    f"Existing container group '{group_name}' uses image '{configured_image}', not '{normalized_image_name}'."
                )
            group_status = "Reused"

        tasks: dict[str, str] = {}
        if benchmark_kind is BenchmarkKind.BOTH:
            launch_task_id_md = self._launch_task_id(gpu_name, normalized_image_name)
            tasks[launch_task_id_md] = _build_bench_task_id(
                launch_task_id_md, BenchmarkKind.MD
            )
            launch_task_id_rbfe = self._launch_task_id(gpu_name, normalized_image_name)
            tasks[launch_task_id_rbfe] = _build_bench_task_id(
                launch_task_id_rbfe, BenchmarkKind.RBFE
            )
        else:
            launch_task_id = self._launch_task_id(gpu_name, normalized_image_name)
            tasks[launch_task_id] = _build_bench_task_id(launch_task_id, benchmark_kind)

        for launch_task, bench_task in tasks.items():
            task_db.add_task_with_capability(
                taskid=launch_task,
                requirements=[],
                max_tries=1,
                capability=SaladWorkerCapability.LAUNCH.value,
            )
            task_db.add_task_with_capability(
                taskid=bench_task,
                requirements=[launch_task],
                max_tries=1,
                capability=gpu_name.value,
            )
            print(
                f"{group_status} Salad container group '{group_name}' and queued benchmark task '{bench_task}'."
            )

    def worker(
        self,
        capability: Annotated[
            SaladWorkerCapability, Parameter(env_var="SALAD_WORKER_CAPABILITY")
        ],
        bench_repo_path: Annotated[
            Optional[Path],
            Parameter(
                env_var="BENCHMARK_REPO_PATH",
                show_env_var=True,
                validator=validators.Path(
                    file_okay=False,
                    dir_okay=True,
                ),
            ),
        ] = Path("/opt/dlami/nvme/performance_benchmarks"),
        *,
        config: Config | None = None,
    ) -> None:
        """Run a Salad worker with a selected capability.

        Parameters
        ----------
        capability : SaladWorkerCapability
            Worker capability used to select which tasks to process.
        bench_repo_path : Path
            Path to the cloned ``performance_benchmarks`` repository.
        config : Config | None, optional
            Shared CLI configuration that provides the task database.
        """
        import os

        if config is None:
            config = Config()
        task_db = config.task_db

        match capability:
            case SaladWorkerCapability.LAUNCH:
                self.launch(config=config)
                return
            case _:
                task = task_db.check_out_task_with_capability(capability.value)

                if task is None:
                    print(f"No available {capability.value} tasks.")
                    return

                try:
                    benchmark_kind, mps_process_count, _launch_task_id = (
                        _parse_bench_task_id(task)
                    )
                except ValueError as exc:
                    task_db.mark_task_completed(task, success=False)
                    raise exc

                s3_bucket = os.environ.get("S3_BUCKET")
                if not s3_bucket:
                    try:
                        task_db.mark_task_completed(task, success=False)
                    except Exception:
                        pass
                    raise ValueError(
                        "S3_BUCKET environment variable is required for bench tasks."
                    )

                assert bench_repo_path
                try:
                    run_benchmark(
                        benchmark_repo_path=bench_repo_path,
                        s3_bucket=s3_bucket,
                        task_id=task,
                        benchmark_kind=benchmark_kind,
                        mps_process_count=mps_process_count,
                    )
                except Exception as exc:
                    try:
                        task_db.mark_task_completed(task, success=False)
                    except Exception as mark_exc:
                        raise ValueError(
                            f"Bench task '{task}' failed and could not be marked as failed "
                            f"in database '{task_db}': {mark_exc}. Original error: {exc}"
                        ) from exc
                    raise ValueError(f"Bench task '{task}' failed: {exc}") from exc

                try:
                    task_db.mark_task_completed(task, success=True)
                except Exception as exc:
                    raise ValueError(
                        f"Bench task '{task}' completed but could not be marked as succeeded "
                        f"in database '{task_db}': {exc}"
                    ) from exc

                print(
                    f"Processed bench task '{task}' (kind '{benchmark_kind.value}') "
                    f"with capability '{capability.value}'."
                )
