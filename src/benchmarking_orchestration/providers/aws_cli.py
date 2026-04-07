from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Optional

from cyclopts import App, Parameter, validators

from ..aws import validate_launch_ami, validate_launch_instance_type
from ..bench import run_benchmark
from ..benchmark_kind import BenchmarkKind
from ..capabilities import WorkerCapability, _resolve_bench_worker_capability
from ..cloud_init import _decode_cloud_init_base64, _read_cloud_init_file_as_base64
from ..normalization import (
    _normalize_ami_id,
    _normalize_instance_type,
    _normalize_region,
)
from ..task_id import (
    _build_bench_task_id,
    _build_task_id,
    _parse_bench_task_id,
    _parse_launch_task_id,
)
from . import LaunchSpec, get_provider
from .aws_provider import DEFAULT_LAUNCH_AMI_ID
from .cli_protocol import Config, ProviderCLI


class AwsCLI(ProviderCLI):
    """Provider-owned Cyclopts registration and handlers for AWS commands."""

    provider_name: str = "aws"

    def register_cli(
        self,
        create_app: App,
        launch_app: App,
        worker_app: App,
    ) -> None:
        """Register AWS subcommands under Cyclopts provider groups.

        Parameters
        ----------
        create_app : App
            Cyclopts ``create`` command group.
        launch_app : App
            Cyclopts ``launch`` command group.
        worker_app : App
            Cyclopts ``worker`` command group.
        """
        create_app.command(self.create, name=self.provider_name)
        worker_app.command(self.worker, name=self.provider_name)
        launch_app.command(self.launch, name=self.provider_name)

    def worker(
        self,
        capability: WorkerCapability,
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
    ):
        """Run an aws worker with a selected capability.

        Parameters
        ----------
        capability : WorkerCapability
            Worker capability used to select which tasks to process.
        db_path : str
            Optional filesystem path to the task status database.
        bench_repo_path : Path
            Path to the cloned ``performance_benchmarks`` repository.
        """
        if config is None:
            config = Config()
        task_db = config.task_db

        match capability:
            case WorkerCapability.LAUNCH:
                # launch
                # aws_launch(db_path)
                # TODO:
                self.launch(config=config)
                return
            case _:
                task = task_db.check_out_task_with_capability(capability.value)

                if task is None:
                    print(f"No available {capability.value} tasks.")
                    return
                # Run the benchmark workload then report results.
                try:
                    benchmark_kind, _launch_task_id = _parse_bench_task_id(task)
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

    def create(
        self,
        instance_type: str,
        region: str = "us-east-1",
        ami_id: str = DEFAULT_LAUNCH_AMI_ID,
        cloud_init_file: Annotated[
            Optional[Path],
            Parameter(
                validator=validators.Path(
                    exists=True,
                )
            ),
        ] = None,
        benchmark_kind: BenchmarkKind = BenchmarkKind.BOTH,
        s3_bucket: Annotated[
            Optional[str], Parameter(env_var="BENCHMARK_S3_BUCKET")
        ] = None,
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
        """Create launch and benchmark task entries in TaskStatusDB for AWS.

        Parameters
        ----------
        instance_type : str
            Requested EC2 instance type to validate and schedule.
        region : str
            AWS region used for instance-type validation and task identity.
        ami_id : str
            AMI identifier recorded with task launch metadata.
        cloud_init_file : str, optional
            Cloud-init file path to encode and store with task launch metadata.
        db_path : str | None
            Filesystem path to the task status database.
        benchmark_kind : BenchmarkKind
            Benchmark workload kind for the dependent bench task.
        s3_bucket : str, optional
            S3 bucket name injected into the cloud-init template as ``S3_BUCKET``.
            Falls back to the ``BENCHMARK_S3_BUCKET`` environment variable.
        bench_repo_path : Path, optional
            Path to the benchmark repository. Preserved for CLI compatibility.
        """
        if config is None:
            config = Config()
        task_db = config.task_db

        normalized_instance_type = _normalize_instance_type(instance_type)
        normalized_region = _normalize_region(region)
        normalized_ami_id = _normalize_ami_id(ami_id)

        validate_launch_instance_type(normalized_instance_type, normalized_region)
        validate_launch_ami(normalized_ami_id, normalized_region)

        instance_capability = _resolve_bench_worker_capability(normalized_instance_type)
        extra_vars: dict[str, str] = {"GPU_CAPABILITY": instance_capability.value}
        if s3_bucket is not None:
            extra_vars["S3_BUCKET"] = s3_bucket
        cloud_init_b64 = _read_cloud_init_file_as_base64(
            str(cloud_init_file),
            extra_vars=extra_vars,
        )

        tasks = {}
        if benchmark_kind is BenchmarkKind.BOTH:
            launch_task_id_md = _build_task_id(
                normalized_region,
                normalized_instance_type,
                normalized_ami_id,
                cloud_init_b64=cloud_init_b64,
            )
            md_task = _build_bench_task_id(launch_task_id_md, BenchmarkKind.MD)
            tasks[launch_task_id_md] = md_task
            launch_task_id_rbfe = _build_task_id(
                normalized_region,
                normalized_instance_type,
                normalized_ami_id,
                cloud_init_b64=cloud_init_b64,
            )
            rbfe_task = _build_bench_task_id(launch_task_id_rbfe, BenchmarkKind.RBFE)
            tasks[launch_task_id_rbfe] = rbfe_task
        else:
            task_id = _build_task_id(
                normalized_region,
                normalized_instance_type,
                normalized_ami_id,
                cloud_init_b64=cloud_init_b64,
            )
            bench_task_id = _build_bench_task_id(task_id, benchmark_kind)
            tasks[task_id] = bench_task_id

        for launch_task, bench_task in tasks.items():
            task_db.add_task_with_capability(
                taskid=launch_task,
                requirements=[],
                max_tries=1,
                capability=WorkerCapability.LAUNCH.value,
            )
            task_db.add_task_with_capability(
                taskid=bench_task,
                requirements=[launch_task],
                max_tries=1,
                capability=instance_capability.value,
            )
            print(
                f"Created benchmark task for instance type '{normalized_instance_type}' with AMI '{normalized_ami_id}' in region '{normalized_region}'."
            )

    def launch(self, *, config: Config | None = None):
        """
        Launch an EC2 instance based on a task from the task status database.

        Parameters
        ----------
        config : Config | None, optional
            Configuration object containing the task status database path. If None, a default
            Config object is used.
        """
        if config is None:
            config = Config()
        task_db = config.task_db

        try:
            task = task_db.check_out_task_with_capability(WorkerCapability.LAUNCH)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to check out task from database '{task_db}': {exc}"
            ) from exc
        if task is None:
            print("No available launch tasks")
            return
        try:
            task_region, task_instance_type, task_ami_id, cloud_init_b64 = (
                _parse_launch_task_id(task)
            )
            cloud_init_user_data = None
            if cloud_init_b64 is not None:
                cloud_init_user_data = _decode_cloud_init_base64(cloud_init_b64)
            ec2_key_name = os.environ.get("EC2_KEY_NAME") or None
            instance_profile_name = os.environ.get("EC2_IAM_INSTANCE_PROFILE") or None
            provider = get_provider(self.provider_name)
            instance_id = provider.submit(
                LaunchSpec(
                    instance_type=task_instance_type,
                    region=task_region,
                    ami_id=task_ami_id,
                    user_data=cloud_init_user_data,
                    key_name=ec2_key_name,
                    instance_profile_name=instance_profile_name,
                )
            )
        except Exception as exc:
            task_db.mark_task_completed(task, success=False)
            raise exc

        try:
            task_db.mark_task_completed(task, success=True)
        except Exception as exc:
            raise exc

        print(f"Processed launch task '{task}' with instance '{instance_id}'.")
