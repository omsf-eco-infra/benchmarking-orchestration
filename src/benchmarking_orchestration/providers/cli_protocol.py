import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol

from cyclopts import App, Parameter, validators

from ..tasks import TaskStatusDB


class ProviderCLI(Protocol):
    provider_name: str

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


@Parameter(name="*", validator=validators.Path(exists=True))
@dataclass
class Config:
    db: Optional[Path] = None
    "Optional path to the task status database."

    def __post_init__(self):
        self.task_db = _setup_task_status_db(self.db)
        if self.db is not None:
            self.db = Path(self.db)


def _setup_task_status_db(db_path: Path | None) -> TaskStatusDB:
    """Set up the task status database connection.

    Parameters
    ----------
    db_path : str, optional
        Filesystem path to a local task status database. When omitted,
        this function prefers Turso environment variables when both are
        configured and otherwise falls back to ``task_status.db``.

    Returns
    -------
    TaskStatusDB
        Initialized task status database client.
    """
    normalized_db_path = db_path if db_path is not None else Path("task_status.db")
    if db_path is not None:
        return TaskStatusDB.from_filename(normalized_db_path)

    turso_database_url = os.getenv("TURSO_DATABASE_URL")
    turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")
    if (
        turso_database_url
        and turso_auth_token
        and hasattr(TaskStatusDB, "from_environment_variables")
    ):
        return TaskStatusDB.from_environment_variables(
            turso_database_url, turso_auth_token
        )
    return TaskStatusDB.from_filename(normalized_db_path)
