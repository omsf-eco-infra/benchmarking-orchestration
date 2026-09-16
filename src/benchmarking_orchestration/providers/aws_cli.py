from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Callable, Optional

from cyclopts import App, Parameter, validators

from ..aws import DEFAULT_LAUNCH_AMI_ENV_VAR, get_launch_ami_details
from ..aws.orchestration import (
    process_aws_benchmark_task,
    process_aws_launch_task,
    queue_aws_tasks,
)
from ..benchmark_kind import BenchmarkKind
from ..capabilities import WorkerCapability
from ..task_id import _parse_bench_task_metadata
from .cli_protocol import Config, ProviderCLI


def _resolve_required_ami_id(ami_id: str | None) -> str:
    """Resolve the required AMI identifier for AWS queueing.

    Parameters
    ----------
    ami_id : str | None
        Explicit AMI identifier, or ``None`` to use the approved environment
        variable.

    Returns
    -------
    str
        Normalized AMI identifier.

    Raises
    ------
    ValueError
        If no non-empty AMI identifier is configured.
    """
    candidate_ami_id = ami_id
    if candidate_ami_id is None:
        candidate_ami_id = os.environ.get(DEFAULT_LAUNCH_AMI_ENV_VAR)
    if candidate_ami_id is None:
        raise ValueError(
            f"AMI ID is required. Pass --ami-id or set {DEFAULT_LAUNCH_AMI_ENV_VAR}."
        )

    normalized_ami_id = candidate_ami_id.strip().lower()
    if not normalized_ami_id:
        raise ValueError("ami id cannot be empty.")
    return normalized_ami_id


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
        Selected AMI identifier.
    ami_name : str
        Human-readable AMI name.
    region : str
        AWS region in which the AMI is used.
    yes : bool, default=False
        Whether to skip interactive confirmation.
    input_func : Callable[[str], str], default=input
        Input function used for confirmation.

    Raises
    ------
    ValueError
        If interactive confirmation is unavailable or declined.
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
    if input_func(prompt).strip().lower() not in {"y", "yes"}:
        raise ValueError(
            f"Aborted AWS task creation because AMI '{ami_name}' ({ami_id}) was not confirmed."
        )


def _cloud_init_template_values() -> dict[str, str]:
    """Build explicit cloud-init values from the CLI environment.

    Returns
    -------
    dict[str, str]
        Environment values plus backward-compatible lowercase Turso aliases.
    """
    values = dict(os.environ)
    if "TURSO_DATABASE_URL" in values:
        values["turso_database_url"] = values["TURSO_DATABASE_URL"]
    if "TURSO_AUTH_TOKEN" in values:
        values["turso_auth_token"] = values["TURSO_AUTH_TOKEN"]
    return values


class AwsCLI(ProviderCLI):
    """Provider-owned Cyclopts registration and handlers for AWS commands."""

    provider_name: str = "aws"

    def register_cli(self, create_app: App, launch_app: App, worker_app: App) -> None:
        """Register AWS subcommands under Cyclopts provider groups.

        Parameters
        ----------
        create_app : App
            Cyclopts create command group.
        launch_app : App
            Cyclopts launch command group.
        worker_app : App
            Cyclopts worker command group.
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
                validator=validators.Path(file_okay=False, dir_okay=True),
            ),
        ] = Path("/opt/dlami/nvme/performance_benchmarks"),
        *,
        config: Config | None = None,
    ) -> None:
        """Run an AWS worker with a selected capability.

        Parameters
        ----------
        capability : WorkerCapability
            Capability used to select tasks.
        bench_repo_path : Path
            Path to the benchmark repository.
        config : Config | None, optional
            CLI database configuration.
        """
        config = config or Config()
        task_db = config.task_db
        if capability is WorkerCapability.LAUNCH:
            self.launch(False, config=config)
            return

        task = task_db.check_out_task_with_capability(capability.value)
        if task is None:
            print(f"No available {capability.value} tasks.")
            return

        try:
            _parse_bench_task_metadata(task)
        except ValueError:
            task_db.mark_task_completed(task, success=False)
            raise

        s3_bucket = os.environ.get("S3_BUCKET")
        if not s3_bucket:
            try:
                task_db.mark_task_completed(task, success=False)
            except Exception:
                pass
            raise ValueError(
                "S3_BUCKET environment variable is required for bench tasks."
            )

        assert bench_repo_path is not None
        processed_task, benchmark_kind = process_aws_benchmark_task(
            task_db,
            task,
            benchmark_repo_path=bench_repo_path,
            s3_bucket=s3_bucket,
        )
        print(
            f"Processed bench task '{processed_task}' (kind '{benchmark_kind.value}') "
            f"with capability '{capability.value}'."
        )

    def create(
        self,
        instance_type: str,
        region: str = "us-east-1",
        ami_id: Annotated[
            Optional[str],
            Parameter(env_var=DEFAULT_LAUNCH_AMI_ENV_VAR, show_env_var=True),
        ] = None,
        cloud_init_file: Annotated[
            Optional[Path], Parameter(validator=validators.Path(exists=True))
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
                validator=validators.Path(file_okay=False, dir_okay=True),
            ),
        ] = Path("/opt/dlami/nvme/performance_benchmarks"),
        yes: bool = False,
        *,
        config: Config | None = None,
    ) -> None:
        """Create AWS launch and benchmark tasks.

        Parameters
        ----------
        instance_type : str
            Requested EC2 instance type.
        region : str, default="us-east-1"
            AWS region.
        ami_id : str | None, optional
            AMI identifier, falling back to ``AWS_BENCHMARK_AMI_ID``.
        cloud_init_file : Path | None, optional
            Cloud-init template path.
        benchmark_kind : BenchmarkKind, default=BenchmarkKind.BOTH
            Benchmark workload selection.
        mps_process_count : int, default=1
            Number of benchmark subprocesses.
        s3_bucket : str | None, optional
            S3 bucket injected into cloud-init.
        bench_repo_path : Path | None, optional
            Preserved CLI compatibility argument.
        yes : bool, default=False
            Whether to skip interactive AMI confirmation.
        config : Config | None, optional
            CLI database configuration.
        """
        del bench_repo_path
        config = config or Config()
        normalized_region = region.strip()
        if not normalized_region:
            raise ValueError("region cannot be empty.")
        resolved_ami_id = _resolve_required_ami_id(ami_id)
        ami_details = get_launch_ami_details(resolved_ami_id, normalized_region)
        ami_name = str(
            ami_details.get("Name")
            or ami_details.get("Description")
            or ami_details.get("ImageId")
        )
        _confirm_ami_choice(resolved_ami_id, ami_name, normalized_region, yes=yes)

        template_values = _cloud_init_template_values()
        if s3_bucket is not None:
            template_values["S3_BUCKET"] = s3_bucket
        task_pairs = queue_aws_tasks(
            config.task_db,
            instance_type,
            normalized_region,
            resolved_ami_id,
            benchmark_kind,
            mps_process_count,
            cloud_init_file=cloud_init_file,
            cloud_init_template_values=template_values,
        )
        for _launch_task, _benchmark_task in task_pairs:
            print(
                f"Created benchmark task for instance type '{instance_type.strip().lower()}' "
                f"with AMI '{resolved_ami_id}' in region '{normalized_region}'."
            )

    def _process_launch_task(
        self,
        task_db,
        task: str,
        *,
        retry_for_capacity: bool,
    ) -> None:
        """Adapt one checked-out launch task to orchestration.

        Parameters
        ----------
        task_db : TaskStatusDB
            Database used to record completion.
        task : str
            Checked-out launch task identifier.
        retry_for_capacity : bool
            Whether to retry quota and capacity exhaustion.
        """
        instance_id = process_aws_launch_task(
            task_db,
            task,
            expected_ami_id=os.environ.get(DEFAULT_LAUNCH_AMI_ENV_VAR),
            key_name=os.environ.get("EC2_KEY_NAME") or None,
            instance_profile_name=os.environ.get("EC2_IAM_INSTANCE_PROFILE") or None,
            retry_for_capacity=retry_for_capacity,
        )
        print(
            f"Processed launch task '{task}' with instance '{instance_id}'.",
            flush=retry_for_capacity,
        )

    def _loop_launch(self, task_db) -> None:
        """Process all currently available AWS launch tasks.

        Parameters
        ----------
        task_db : TaskStatusDB
            Database from which launch tasks are checked out.
        """
        while True:
            task = self._check_out_launch_task(task_db)
            if task is None:
                return
            self._process_launch_task(task_db, task, retry_for_capacity=True)

    @staticmethod
    def _check_out_launch_task(task_db) -> str | None:
        """Check out one launch task with translated database errors.

        Parameters
        ----------
        task_db : TaskStatusDB
            Database from which a task is checked out.

        Returns
        -------
        str | None
            Checked-out task identifier, or ``None``.
        """
        try:
            return task_db.check_out_task_with_capability(WorkerCapability.LAUNCH.value)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to check out task from database '{task_db}': {exc}"
            ) from exc

    def launch(self, loop: bool, *, config: Config | None = None) -> None:
        """Launch EC2 instances from queued launch tasks.

        Parameters
        ----------
        loop : bool
            Whether to process all currently available launch tasks.
        config : Config | None, optional
            CLI database configuration.
        """
        config = config or Config()
        task_db = config.task_db
        if loop:
            self._loop_launch(task_db)
            return

        task = self._check_out_launch_task(task_db)
        if task is None:
            print("No available launch tasks")
            return
        self._process_launch_task(task_db, task, retry_for_capacity=False)
