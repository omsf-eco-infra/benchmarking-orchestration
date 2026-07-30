from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter, validators

from ..benchmark_kind import BenchmarkKind
from ..brev import queue_brev_tasks
from ..brev.orchestration import launch_brev_task
from .cli_protocol import Config


class BrevCLI:
    """Register controller-side Brev task creation commands."""

    provider_name = "brev"

    def register_cli(self, create_app: App, launch_app: App) -> None:
        """Register the Brev creation and controller launch commands.

        Parameters
        ----------
        create_app : App
            Cyclopts task creation group.
        launch_app : App
            Cyclopts trusted-controller launch group.
        """
        create_app.command(self.create, name=self.provider_name)
        launch_app.command(self.launch, name=self.provider_name)

    def create(
        self,
        instance_type: str,
        profile: str,
        benchmark_kind: BenchmarkKind = BenchmarkKind.BOTH,
        mps_process_count: int = 1,
        timeout_seconds: float = 3600,
        *,
        config: Config | None = None,
    ) -> None:
        """Queue credential-free Brev benchmark jobs in local SQLite.

        Parameters
        ----------
        instance_type : str
            Explicit Brev instance type.
        profile : str
            Explicit benchmark profile identifier.
        benchmark_kind : BenchmarkKind, default=BenchmarkKind.BOTH
            Benchmark workload selection.
        mps_process_count : int, default=1
            Number of worker benchmark processes.
        timeout_seconds : float, default=3600
            Controller timeout for each job.
        config : Config | None, optional
            Shared task database configuration.
        """
        config = config or Config()
        task_ids = queue_brev_tasks(
            config.task_db,
            instance_type,
            profile,
            benchmark_kind,
            mps_process_count,
            timeout_seconds,
        )
        for task_id in task_ids:
            print(f"Created Brev benchmark task '{task_id}'.")

    def launch(
        self,
        s3_bucket: Annotated[
            str, Parameter(env_var="BENCHMARK_S3_BUCKET", show_env_var=True)
        ],
        result_directory: Path = Path("brev-results"),
        startup_script: Annotated[
            Path, Parameter(validator=validators.Path(file_okay=True, dir_okay=False))
        ] = Path("brev_startup.sh"),
        *,
        config: Config | None = None,
    ) -> None:
        """Run one queued Brev task from the trusted controller.

        Parameters
        ----------
        s3_bucket : str
            S3 bucket receiving validated benchmark artifacts.
        result_directory : Path, default=Path("brev-results")
            Controller directory receiving the local result bundle.
        startup_script : Path, default=Path("brev_startup.sh")
            Credentialless Brev instance startup script.
        config : Config | None, optional
            Shared Exorcist task database configuration.
        """
        config = config or Config()
        result = launch_brev_task(
            config.task_db,
            s3_bucket,
            result_directory,
            startup_script,
        )
        if result is None:
            print("No available Brev tasks.")
            return
        task, local_job_directory = result
        print(
            f"Uploaded and finalized Brev task '{task}' from '{local_job_directory}'."
        )
