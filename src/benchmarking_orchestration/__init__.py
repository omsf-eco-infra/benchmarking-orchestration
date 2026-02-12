from __future__ import annotations

import uuid
from pathlib import Path

import click
from .tasks import TaskStatusDB

from .aws import launch_ec2_instance, validate_launch_instance_type


def _normalize_required_value(field_name: str, value: str) -> str:
    """Normalize a required CLI string value.

    Parameters
    ----------
    field_name : str
        Human-readable field label for error messages.
    value : str
        Raw value provided by the caller.

    Returns
    -------
    str
        Stripped value.

    Raises
    ------
    click.BadParameter
        If the stripped value is empty.
    """
    normalized = value.strip()
    if not normalized:
        raise click.BadParameter(f"{field_name} cannot be empty.")
    return normalized


def _normalize_instance_type(instance_type: str) -> str:
    """Normalize and lowercase an EC2 instance type value.

    Parameters
    ----------
    instance_type : str
        Raw instance type argument from the CLI.

    Returns
    -------
    str
        Lowercased, stripped instance type.
    """
    return _normalize_required_value("instance type", instance_type).lower()


def _normalize_region(region: str) -> str:
    """Normalize an AWS region value.

    Parameters
    ----------
    region : str
        Raw region argument from the CLI.

    Returns
    -------
    str
        Stripped region value.
    """
    return _normalize_required_value("region", region)


def _normalize_db_path(db_path: str) -> str:
    """Normalize a task database path value.

    Parameters
    ----------
    db_path : str
        Raw database path argument from the CLI.

    Returns
    -------
    str
        Stripped path value.
    """
    return _normalize_required_value("db path", db_path)


def _build_task_id(region: str, instance_type: str) -> str:
    """Build a unique task identifier for EC2 launch orchestration.

    Parameters
    ----------
    region : str
        AWS region where the launch should occur.
    instance_type : str
        Validated EC2 instance type.

    Returns
    -------
    str
        Task identifier in ``<region>:<instance_type>:<uuid4>`` format.
    """
    return f"{region}:{instance_type}:{uuid.uuid4()}"


def _parse_launch_task_id(taskid: str) -> tuple[str, str]:
    """Parse a launch task identifier into region and instance type.

    Parameters
    ----------
    taskid : str
        Task identifier in ``<region>:<instance_type>:<uuid4>`` format.

    Returns
    -------
    tuple[str, str]
        Parsed ``(region, instance_type)`` values.

    Raises
    ------
    ValueError
        If the task identifier is malformed or missing required parts.
    """
    try:
        region, instance_type, task_uuid = taskid.split(":", maxsplit=2)
    except ValueError as exc:
        raise ValueError(
            "Invalid launch task ID format. Expected '<region>:<instance_type>:<uuid4>'."
        ) from exc

    normalized_region = region.strip()
    normalized_instance_type = instance_type.strip().lower()
    if not normalized_region or not normalized_instance_type or not task_uuid.strip():
        raise ValueError(
            "Invalid launch task ID format. Expected '<region>:<instance_type>:<uuid4>'."
        )

    try:
        uuid.UUID(task_uuid.strip())
    except ValueError as exc:
        raise ValueError(
            "Invalid launch task ID format. Expected '<region>:<instance_type>:<uuid4>'."
        ) from exc

    return normalized_region, normalized_instance_type


def _resolve_worker_capabilities(launch_task: bool) -> list[str]:
    """Resolve enabled worker capabilities from CLI flags.

    Parameters
    ----------
    launch_task : bool
        Whether launch-task handling is enabled for this worker instance.

    Returns
    -------
    list[str]
        Enabled worker capability names.
    """
    if launch_task:
        return ["ec2-launch"]
    return []


@click.group(
    invoke_without_command=True,
    help="CLI for benchmarking task orchestration.",
)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Root CLI command group for benchmarking orchestration.

    Parameters
    ----------
    ctx : click.Context
        Click invocation context used to inspect invoked subcommands.
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command("worker", help="Run worker tasks based on enabled capabilities.")
@click.option(
    "--launch-task/--no-launch-task",
    default=False,
    show_default=True,
    type=bool,
)
@click.option("--db-path", default="task_status.db", show_default=True, type=str)
def worker(launch_task: bool, db_path: str) -> None:
    """Run a worker with explicitly enabled task capabilities.

    Parameters
    ----------
    launch_task : bool
        Whether the worker can run launch tasks.

    Raises
    ------
    click.UsageError
        If no worker capability is enabled.
    """
    capabilities = _resolve_worker_capabilities(launch_task=launch_task)
    if not capabilities:
        raise click.UsageError(
            "At least one worker capability must be enabled. Use --launch-task."
        )
    normalized_db_path = _normalize_db_path(db_path)
    cap = capabilities[0]
    try:
        task_db = TaskStatusDB.from_filename(Path(normalized_db_path))
        task = task_db.check_out_task_with_type(cap)
    except Exception as exc:
        raise click.ClickException(
            f"Unable to check out task from database '{normalized_db_path}': {exc}"
        ) from exc

    if task is None:
        click.echo("No available ec2-launch tasks.")
        return

    try:
        task_region, task_instance_type = _parse_launch_task_id(task)
        instance_id = launch_ec2_instance(task_instance_type, region=task_region)
    except Exception as exc:
        try:
            task_db.mark_task_completed(task, success=False)
        except Exception as mark_exc:
            raise click.ClickException(
                f"Failed to process launch task '{task}' and failed to mark it as failed "
                f"in database '{normalized_db_path}': {mark_exc}. Original error: {exc}"
            ) from exc
        raise click.ClickException(f"Failed to process launch task '{task}': {exc}") from exc

    try:
        task_db.mark_task_completed(task, success=True)
    except Exception as exc:
        raise click.ClickException(
            f"Launched instance '{instance_id}' for task '{task}', but failed to mark it "
            f"as completed in database '{normalized_db_path}': {exc}"
        ) from exc

    click.echo(f"Processed launch task '{task}' with instance '{instance_id}'.")


@cli.command("create-launch-task", help="Create a launch task entry in TaskStatusDB.")
@click.option("--instance-type", required=True, type=str)
@click.option("--region", default="us-east-1", show_default=True, type=str)
@click.option("--db-path", default="task_status.db", show_default=True, type=str)
@click.option("--max-tries", default=1, show_default=True, type=click.IntRange(min=1))
def create_launch_task(
    instance_type: str,
    region: str,
    db_path: str,
    max_tries: int,
) -> None:
    """Create a launch task entry in TaskStatusDB.

    Parameters
    ----------
    instance_type : str
        Requested EC2 instance type to validate and schedule.
    region : str
        AWS region used for instance-type validation and task identity.
    db_path : str
        Filesystem path to the task status database.
    max_tries : int
        Maximum total execution attempts for the task.

    Raises
    ------
    click.ClickException
        If AWS validation fails or task insertion into the DB fails.
    """
    normalized_instance_type = _normalize_instance_type(instance_type)
    normalized_region = _normalize_region(region)
    normalized_db_path = _normalize_db_path(db_path)

    try:
        validate_launch_instance_type(normalized_instance_type, normalized_region)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    task_id = _build_task_id(normalized_region, normalized_instance_type)

    try:
        task_db = TaskStatusDB.from_filename(Path(normalized_db_path))
        task_db.add_task_with_type(
            taskid=task_id, requirements=[], max_tries=max_tries, task_type="ec2-launch"
        )
        # task_db.add_task(taskid=task_id, requirements=[], max_tries=max_tries)
    except Exception as exc:
        raise click.ClickException(
            f"Unable to create task in database '{normalized_db_path}': {exc}"
        ) from exc

    click.echo(task_id)
    click.echo(
        f"Created launch task for instance type '{normalized_instance_type}' in region '{normalized_region}'."
    )
