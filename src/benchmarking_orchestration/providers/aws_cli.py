from __future__ import annotations
import time
from exorcist.taskdb import _logger
from benchmarking_orchestration.tasks import TaskStatusDB

import os
import sys
from pathlib import Path
from typing import Any, Annotated, Callable, Optional

from cyclopts import App, Parameter, validators

from ..aws import (
    DEFAULT_LAUNCH_AMI_ENV_VAR,
    get_launch_ami_name,
    validate_launch_ami,
    validate_launch_instance_type,
    get_ondemand_g_vcpu_quota,
    get_ondemand_g_vcpus_used,
    get_instance_type_vcpu_count,
)
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
    _parse_bench_task_metadata,
    _parse_launch_task_id,
)
from . import LaunchSpec, get_provider
from .cli_protocol import Config, ProviderCLI

CAPACITY_RETRY_SLEEP_SECONDS = 60 * 15
WIGGLE_ROOM = 8


def _normalize_optional_ami_id(ami_id: str | None) -> str | None:
    """Normalize an optional AMI identifier.

    Parameters
    ----------
    ami_id : str | None
        Raw AMI identifier provided by the caller.

    Returns
    -------
    str | None
        Lowercased, normalized AMI identifier when provided, otherwise ``None``.
    """
    if ami_id is None:
        return None
    return _normalize_ami_id(ami_id)


def _resolve_required_ami_id(ami_id: str | None) -> str:
    """Resolve the required AMI identifier for AWS queueing.

    Parameters
    ----------
    ami_id : str | None
        Explicit AMI identifier argument, or ``None`` to fall back to the
        approved environment variable.

    Returns
    -------
    str
        Normalized AMI identifier.

    Raises
    ------
    ValueError
        If no AMI identifier is provided explicitly or via the approved
        environment variable.
    """
    normalized_ami_id = _normalize_optional_ami_id(ami_id)
    if normalized_ami_id is not None:
        return normalized_ami_id

    configured_ami_id = _normalize_optional_ami_id(
        os.environ.get(DEFAULT_LAUNCH_AMI_ENV_VAR)
    )
    if configured_ami_id is not None:
        return configured_ami_id

    raise ValueError(
        f"AMI ID is required. Pass --ami-id or set {DEFAULT_LAUNCH_AMI_ENV_VAR}."
    )


def _confirm_ami_choice(
    ami_id: str,
    ami_name: str,
    region: str,
    *,
    yes: bool = False,
    input_func: Callable[[str], str] = input,
) -> None:
    """Confirm the AMI selection before queueing new AWS tasks.

    Parameters
    ----------
    ami_id : str
        Normalized AMI identifier selected for queueing.
    ami_name : str
        Human-readable AMI name returned by AWS.
    region : str
        AWS region where the AMI will be used.
    yes : bool, default=False
        When ``True``, skip the interactive confirmation prompt.
    input_func : Callable[[str], str], default=input
        Input function used to prompt for confirmation.

    Raises
    ------
    ValueError
        If the AMI selection is not confirmed.
    """
    print(
        f"Resolved AMI for AWS queueing: '{ami_name}' ({ami_id}) in region '{region}'."
    )

    if yes:
        print("AMI confirmation skipped because --yes was provided.")
        return

    prompt = f"Queue tasks with AMI '{ami_name}' ({ami_id})? [y/N]: "
    if not sys.stdin.isatty():
        raise ValueError(
            "Refusing to queue AWS tasks without interactive AMI confirmation. "
            f"Re-run with --yes after verifying AMI '{ami_name}' ({ami_id})."
        )

    response = input_func(prompt).strip().lower()
    if response not in {"y", "yes"}:
        raise ValueError(
            f"Aborted AWS task creation because AMI '{ami_name}' ({ami_id}) was not confirmed."
        )


def _validate_launch_task_ami_against_expected(task_ami_id: str) -> None:
    """Validate a queued launch task AMI against the approved AMI setting.

    Parameters
    ----------
    task_ami_id : str
        AMI identifier embedded in the queued launch task.

    Raises
    ------
    RuntimeError
        If an approved AMI is configured and the queued task AMI does not match it.
    """
    configured_ami_id = _normalize_optional_ami_id(
        os.environ.get(DEFAULT_LAUNCH_AMI_ENV_VAR)
    )
    if configured_ami_id is None:
        return

    normalized_task_ami_id = _normalize_ami_id(task_ami_id)
    if normalized_task_ami_id != configured_ami_id:
        raise RuntimeError(
            "Queued launch task AMI does not match the approved AMI. "
            f"Task AMI: '{normalized_task_ami_id}'. Approved AMI from "
            f"{DEFAULT_LAUNCH_AMI_ENV_VAR}: '{configured_ami_id}'."
        )


def _wait_for_ondemand_g_vcpu_quota(task: str, instance_type: str) -> None:
    """Wait until On-Demand G/VT quota can accommodate an instance launch.

    Parameters
    ----------
    task : str
        Launch task identifier currently being processed.
    instance_type : str
        EC2 instance type requested by the launch task.
    """
    needed_vcpus = get_instance_type_vcpu_count(instance_type)
    quota = get_ondemand_g_vcpu_quota()
    used = get_ondemand_g_vcpus_used()
    available = quota - used - WIGGLE_ROOM
    if needed_vcpus <= available:
        return

    _logger.warning(
        f"Insufficient vCPU quota to launch '{task}': "
        f"needed={needed_vcpus}, available={available} "
        f"(quota={quota}, used={used}). Waiting for capacity..."
    )
    while needed_vcpus > available:
        time.sleep(CAPACITY_RETRY_SLEEP_SECONDS)
        quota = get_ondemand_g_vcpu_quota()
        used = get_ondemand_g_vcpus_used()
        available = quota - used


def _is_insufficient_instance_capacity_error(exc: BaseException) -> bool:
    """Return whether an exception represents EC2 placement capacity exhaustion.

    Parameters
    ----------
    exc : BaseException
        Exception raised during EC2 launch submission.

    Returns
    -------
    bool
        ``True`` when the error indicates ``InsufficientInstanceCapacity``,
        otherwise ``False``.
    """
    if "InsufficientInstanceCapacity" in str(exc):
        return True

    cause = getattr(exc, "__cause__", None)
    response = getattr(cause, "response", {}) if cause is not None else {}
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return error.get("Code") == "InsufficientInstanceCapacity"


def _submit_loop_launch_spec(
    task: str,
    instance_type: str,
    provider: Any,
    launch_spec: LaunchSpec,
) -> str:
    """Submit a looped AWS launch task with quota waits and capacity retries.

    Parameters
    ----------
    task : str
        Launch task identifier currently being processed.
    instance_type : str
        EC2 instance type requested by the launch task.
    provider : Any
        Provider implementation used to submit the launch request.
    launch_spec : LaunchSpec
        Launch specification sent to the provider.

    Returns
    -------
    str
        Launched EC2 instance identifier.

    Raises
    ------
    Exception
        Re-raises any non-capacity submission error from the provider.
    """
    while True:
        _wait_for_ondemand_g_vcpu_quota(task, instance_type)
        try:
            return provider.submit(launch_spec)
        except Exception as exc:
            if not _is_insufficient_instance_capacity_error(exc):
                raise
            _logger.warning(
                f"Insufficient EC2 instance capacity while launching '{task}'. "
                f"Retrying in {CAPACITY_RETRY_SLEEP_SECONDS} seconds. "
                f"Original error: {exc}"
            )
            time.sleep(CAPACITY_RETRY_SLEEP_SECONDS)


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
                    benchmark_kind, mps_process_count, _launch_task_id = (
                        _parse_bench_task_metadata(task)
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

    def create(
        self,
        instance_type: str,
        region: str = "us-east-1",
        ami_id: Annotated[
            Optional[str],
            Parameter(
                env_var=DEFAULT_LAUNCH_AMI_ENV_VAR,
                show_env_var=True,
            ),
        ] = None,
        cloud_init_file: Annotated[
            Optional[Path],
            Parameter(
                validator=validators.Path(
                    exists=True,
                )
            ),
        ] = None,
        benchmark_kind: BenchmarkKind = BenchmarkKind.BOTH,
        mps_process_count: int = 1,
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
        yes: bool = False,
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
        ami_id : str, optional
            AMI identifier recorded with task launch metadata. When omitted,
            the CLI falls back to the approved ``AWS_BENCHMARK_AMI_ID``
            environment variable.
        cloud_init_file : str, optional
            Cloud-init file path to encode and store with task launch metadata.
        db_path : str | None
            Filesystem path to the task status database.
        benchmark_kind : BenchmarkKind
            Benchmark workload kind for the dependent bench task.
        mps_process_count : int, default=1
            Number of concurrent benchmark subprocesses to encode for bench tasks.
            A value of ``1`` preserves current single-process behavior.
        s3_bucket : str, optional
            S3 bucket name injected into the cloud-init template as ``S3_BUCKET``.
            Falls back to the ``BENCHMARK_S3_BUCKET`` environment variable.
        bench_repo_path : Path, optional
            Path to the benchmark repository. Preserved for CLI compatibility.
        yes : bool, default=False
            Skip the interactive AMI confirmation prompt after the resolved AMI
            name has been displayed.
        """
        if config is None:
            config = Config()
        task_db = config.task_db

        normalized_instance_type = _normalize_instance_type(instance_type)
        normalized_region = _normalize_region(region)
        normalized_ami_id = _resolve_required_ami_id(ami_id)

        if mps_process_count < 1:
            raise ValueError("mps_process_count must be greater than or equal to 1.")

        validate_launch_instance_type(normalized_instance_type, normalized_region)
        validate_launch_ami(normalized_ami_id, normalized_region)
        ami_name = get_launch_ami_name(normalized_ami_id, normalized_region)
        _confirm_ami_choice(
            normalized_ami_id,
            ami_name,
            normalized_region,
            yes=yes,
        )

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
            md_task = _build_bench_task_id(
                launch_task_id_md,
                BenchmarkKind.MD,
                mps_process_count=mps_process_count,
            )
            tasks[launch_task_id_md] = md_task
            launch_task_id_rbfe = _build_task_id(
                normalized_region,
                normalized_instance_type,
                normalized_ami_id,
                cloud_init_b64=cloud_init_b64,
            )
            rbfe_task = _build_bench_task_id(
                launch_task_id_rbfe,
                BenchmarkKind.RBFE,
                mps_process_count=mps_process_count,
            )
            tasks[launch_task_id_rbfe] = rbfe_task
        else:
            task_id = _build_task_id(
                normalized_region,
                normalized_instance_type,
                normalized_ami_id,
                cloud_init_b64=cloud_init_b64,
            )
            bench_task_id = _build_bench_task_id(
                task_id,
                benchmark_kind,
                mps_process_count=mps_process_count,
            )
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

    def _loop_launch(self, task_db: TaskStatusDB):
        while True:
            try:
                task = task_db.check_out_task_with_capability(WorkerCapability.LAUNCH)
            except Exception as exc:
                raise RuntimeError(
                    f"Unable to check out task from database '{task_db}': {exc}"
                ) from exc
            if task is None:
                break
            try:
                task_region, task_instance_type, task_ami_id, cloud_init_b64 = (
                    _parse_launch_task_id(task)
                )
                _validate_launch_task_ami_against_expected(task_ami_id)
                cloud_init_user_data = None
                if cloud_init_b64 is not None:
                    cloud_init_user_data = _decode_cloud_init_base64(cloud_init_b64)
                ec2_key_name = os.environ.get("EC2_KEY_NAME") or None
                instance_profile_name = (
                    os.environ.get("EC2_IAM_INSTANCE_PROFILE") or None
                )
                provider = get_provider(self.provider_name)
                launch_spec = LaunchSpec(
                    instance_type=task_instance_type,
                    region=task_region,
                    ami_id=task_ami_id,
                    user_data=cloud_init_user_data,
                    key_name=ec2_key_name,
                    instance_profile_name=instance_profile_name,
                )
                instance_id = _submit_loop_launch_spec(
                    task=task,
                    instance_type=task_instance_type,
                    provider=provider,
                    launch_spec=launch_spec,
                )
            except Exception as exc:
                task_db.mark_task_completed(task, success=False)
                raise exc
            try:
                task_db.mark_task_completed(task, success=True)
            except Exception as exc:
                raise exc

            print(
                f"Processed launch task '{task}' with instance '{instance_id}'.",
                flush=True,
            )

    def launch(self, loop: bool, *, config: Config | None = None):
        """
        Launch an EC2 instance based on a task from the task status database.

        Parameters
        ----------
        loop: bool
            Whether or not to keep looping until the DB is completed
        config : Config | None, optional
            Configuration object containing the task status database path. If None, a default
            Config object is used.
        """
        if config is None:
            config = Config()
        task_db = config.task_db

        if loop:
            self._loop_launch(task_db)
        else:
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
                _validate_launch_task_ami_against_expected(task_ami_id)
                cloud_init_user_data = None
                if cloud_init_b64 is not None:
                    cloud_init_user_data = _decode_cloud_init_base64(cloud_init_b64)
                ec2_key_name = os.environ.get("EC2_KEY_NAME") or None
                instance_profile_name = (
                    os.environ.get("EC2_IAM_INSTANCE_PROFILE") or None
                )
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
