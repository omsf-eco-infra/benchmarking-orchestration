from __future__ import annotations

from cyclopts import App

from ..benchmark_kind import BenchmarkKind
from ..brev import queue_brev_tasks
from .cli_protocol import Config


class BrevCLI:
    """Register controller-side Brev task creation commands."""

    provider_name = "brev"

    def register_cli(self, create_app: App) -> None:
        """Register the Brev creation command.

        Parameters
        ----------
        create_app : App
            Cyclopts task creation group.
        """
        create_app.command(self.create, name=self.provider_name)

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
