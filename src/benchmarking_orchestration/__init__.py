from __future__ import annotations

import base64
import binascii
import os
import uuid
from enum import StrEnum
from pathlib import Path

import click
from .tasks import TaskStatusDB

from .aws import (
    DEFAULT_LAUNCH_AMI_ID,
    launch_ec2_instance,
    validate_launch_ami,
    validate_launch_instance_type,
)


class WorkerCapability(StrEnum):
    """Supported worker capability names."""

    LAUNCH = "launch"
    G3 = "g3"
    G4DN = "g4dn"
    G5 = "g5"


def _worker_capability_choices() -> tuple[str, ...]:
    """Return supported worker capability values for CLI option choices.

    Returns
    -------
    tuple[str, ...]
        Sorted capability values accepted by the CLI.
    """
    return tuple(sorted(capability.value for capability in WorkerCapability))


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


def _setup_task_status_db(db_path: str | None) -> TaskStatusDB:
    """Set up the task status database connection.

    Parameters
    ----------
    db_path : str
        Normalized filesystem path to the task status database.

    Returns
    -------
    TaskStatusDB
        Initialized task status database client.
    """
    if db_path is None:
        turso_database_url = os.getenv("TURSO_DATABASE_URL")
        turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")
        if not turso_database_url:
            raise ValueError("Missing TURSO_DATABASE_URL")
        if not turso_auth_token:
            raise ValueError("Missing TURSO_AUTH_TOKEN")
        return TaskStatusDB.from_environment_variables(
            turso_database_url, turso_auth_token
        )
    return TaskStatusDB.from_filename(db_path)


def _build_task_id(
    region: str,
    instance_type: str,
    ami_id: str = DEFAULT_LAUNCH_AMI_ID,
    cloud_init_b64: str | None = None,
) -> str:
    """Build a unique task identifier for EC2 launch orchestration.

    Parameters
    ----------
    region : str
        AWS region where the launch should occur.
    instance_type : str
        Validated EC2 instance type.
    ami_id : str, default=DEFAULT_LAUNCH_AMI_ID
        AMI identifier to use for launch.
    cloud_init_b64 : str, optional
        Base64-encoded cloud-init payload to embed in task metadata.

    Returns
    -------
    str
        Task identifier in ``<region>:<instance_type>:<ami_id>:<uuid4>`` format
        when no cloud-init payload is provided, otherwise
        ``<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>``.
    """
    if cloud_init_b64 is None:
        return f"{region}:{instance_type}:{ami_id}:{uuid.uuid4()}"

    return f"{region}:{instance_type}:{ami_id}:{cloud_init_b64}:{uuid.uuid4()}"


def _normalize_ami_id(ami_id: str) -> str:
    """Normalize and lowercase an AMI identifier value.

    Parameters
    ----------
    ami_id : str
        Raw AMI identifier argument from the CLI.

    Returns
    -------
    str
        Lowercased, stripped AMI identifier.
    """
    return _normalize_required_value("ami id", ami_id).lower()


def _parse_launch_task_id(taskid: str) -> tuple[str, str, str, str | None]:
    """Parse a launch task identifier into region and instance type.

    Parameters
    ----------
    taskid : str
        Task identifier in
        ``<region>:<instance_type>:<ami_id>:<uuid4>`` format,
        or ``<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>`` format.

    Returns
    -------
    tuple[str, str, str, str | None]
        Parsed ``(region, instance_type, ami_id, cloud_init_b64)`` values.

    Raises
    ------
    ValueError
        If the task identifier is malformed or missing required parts.
    """
    expected_format_message = (
        "Invalid launch task ID format. Expected "
        "'<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>', "
        "or '<region>:<instance_type>:<ami_id>:<uuid4>'."
    )
    parts = taskid.split(":")
    if len(parts) == 4:
        region, instance_type, ami_id, task_uuid = parts
        cloud_init_b64 = None
    elif len(parts) == 5:
        region, instance_type, ami_id, cloud_init_b64, task_uuid = parts
    else:
        raise ValueError(expected_format_message)

    normalized_region = region.strip()
    normalized_instance_type = instance_type.strip().lower()
    normalized_ami_id = ami_id.strip().lower()
    normalized_cloud_init_b64 = (
        cloud_init_b64.strip() if cloud_init_b64 is not None else None
    )
    normalized_task_uuid = task_uuid.strip()
    if (
        not normalized_region
        or not normalized_instance_type
        or not normalized_ami_id
        or (cloud_init_b64 is not None and not normalized_cloud_init_b64)
        or not normalized_task_uuid
    ):
        raise ValueError(expected_format_message)

    try:
        uuid.UUID(normalized_task_uuid)
    except ValueError as exc:
        raise ValueError(expected_format_message) from exc

    return (
        normalized_region,
        normalized_instance_type,
        normalized_ami_id,
        normalized_cloud_init_b64,
    )


def _normalize_cloud_init_file_path(cloud_init_file: str | None) -> str | None:
    """Normalize an optional cloud-init file path.

    Parameters
    ----------
    cloud_init_file : str, optional
        Raw cloud-init file path from the CLI.

    Returns
    -------
    str | None
        Stripped file path, or ``None`` when no path is provided.

    Raises
    ------
    click.BadParameter
        If a value is provided but is empty after stripping.
    """
    if cloud_init_file is None:
        return None
    return _normalize_required_value("cloud init file", cloud_init_file)


def _read_cloud_init_file_as_base64(cloud_init_file: str | None) -> str | None:
    """Read a cloud-init file and return a base64 payload.

    Parameters
    ----------
    cloud_init_file : str, optional
        Path to a cloud-init file.

    Returns
    -------
    str | None
        Base64-encoded file contents when provided, otherwise ``None``.

    Raises
    ------
    click.ClickException
        If file reading fails or the file is empty.
    """
    normalized_path = _normalize_cloud_init_file_path(cloud_init_file)
    if normalized_path is None:
        return None

    file_path = Path(normalized_path)
    try:
        file_bytes = file_path.read_bytes()
    except OSError as exc:
        raise click.ClickException(
            f"Unable to read cloud-init file '{normalized_path}': {exc}"
        ) from exc

    if not file_bytes:
        raise click.ClickException(f"Cloud-init file '{normalized_path}' is empty.")

    return base64.b64encode(file_bytes).decode("ascii")


def _decode_cloud_init_base64(cloud_init_b64: str) -> str:
    """Decode a base64 cloud-init payload from task metadata.

    Parameters
    ----------
    cloud_init_b64 : str, optional
        Base64 cloud-init payload parsed from the task ID.

    Returns
    -------
    str | None
        Decoded UTF-8 cloud-init content, or ``None`` if not provided.

    Raises
    ------
    ValueError
        If payload encoding is invalid or not UTF-8 text.
    """
    try:
        decoded_bytes = base64.b64decode(cloud_init_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(
            "Invalid cloud-init payload encoding in launch task ID."
        ) from exc

    try:
        return decoded_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Cloud-init payload is not valid UTF-8 text.") from exc


def _parse_worker_capability(
    _ctx: click.Context, _param: click.Parameter, value: str
) -> WorkerCapability:
    """Parse and normalize worker capability option value.

    Parameters
    ----------
    _ctx : click.Context
        Click context (unused).
    _param : click.Parameter
        Click parameter metadata (unused).
    value : str
        Selected worker capability value.

    Returns
    -------
    WorkerCapability
        Parsed worker capability enum value.
    """
    return WorkerCapability(value.lower())


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


@cli.command("worker", help="Run worker tasks for a selected capability.")
@click.option(
    "--capability",
    required=True,
    type=click.Choice(_worker_capability_choices(), case_sensitive=False),
    callback=_parse_worker_capability,
    help="Worker capability to execute.",
)
@click.option("--db-path", default=None, type=str)
def worker(capability: WorkerCapability, db_path: str) -> None:
    """Run a worker with a selected capability.

    Parameters
    ----------
    capability : WorkerCapability
        Worker capability used to select which tasks to process.
    """
    try:
        task_db = _setup_task_status_db(db_path)
        task = task_db.check_out_task_with_capability(capability.value)
    except Exception as exc:
        raise click.ClickException(
            f"Unable to check out task from database '{db_path}': {exc}"
        ) from exc

    if task is None:
        click.echo(f"No available {capability.value} tasks.")
        return

    match capability:
        case WorkerCapability.LAUNCH:
            try:
                task_region, task_instance_type, task_ami_id, cloud_init_b64 = (
                    _parse_launch_task_id(task)
                )
                cloud_init_user_data = None
                if cloud_init_b64 is not None:
                    cloud_init_user_data = _decode_cloud_init_base64(cloud_init_b64)
                instance_id = launch_ec2_instance(
                    task_instance_type,
                    ami_id=task_ami_id,
                    region=task_region,
                    user_data=cloud_init_user_data,
                )
            except Exception as exc:
                try:
                    task_db.mark_task_completed(task, success=False)
                except Exception as mark_exc:
                    raise click.ClickException(
                        f"Failed to process launch task '{task}' and failed to mark it as failed "
                        f"in database '{normalized_db_path}': {mark_exc}. Original error: {exc}"
                    ) from exc
                raise click.ClickException(
                    f"Failed to process launch task '{task}': {exc}"
                ) from exc

            try:
                task_db.mark_task_completed(task, success=True)
            except Exception as exc:
                raise click.ClickException(
                    f"Launched instance '{instance_id}' for task '{task}', but failed to mark it "
                    f"as completed in database '{normalized_db_path}': {exc}"
                ) from exc

            click.echo(f"Processed launch task '{task}' with instance '{instance_id}'.")
        case WorkerCapability.G4DN | WorkerCapability.G3:
            # Do the bench task
            task_db.mark_task_completed(task, success=False)
            click.echo(
                f"Processed bench task '{task}' with capability '{capability.value}'"
            )


@cli.command("create-launch-task", help="Create a launch task entry in TaskStatusDB.")
@click.option("--instance-type", required=True, type=str)
@click.option("--region", default="us-east-1", show_default=True, type=str)
@click.option("--ami-id", default=DEFAULT_LAUNCH_AMI_ID, show_default=True, type=str)
@click.option("--cloud-init-file", default=None, type=str)
@click.option("--db-path", default=None, show_default=False, type=str)
@click.option("--max-tries", default=1, show_default=True, type=click.IntRange(min=1))
def create_launch_task(
    instance_type: str,
    region: str,
    ami_id: str,
    cloud_init_file: str | None,
    db_path: str | None,
    max_tries: int,
) -> None:
    """Create a launch task entry in TaskStatusDB.

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
    max_tries : int
        Maximum total execution attempts for the task.

    Raises
    ------
    click.ClickException
        If AWS validation fails or task insertion into the DB fails.
    """
    normalized_instance_type = _normalize_instance_type(instance_type)
    normalized_region = _normalize_region(region)
    normalized_ami_id = _normalize_ami_id(ami_id)

    try:
        validate_launch_instance_type(normalized_instance_type, normalized_region)
        validate_launch_ami(normalized_ami_id, normalized_region)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    cloud_init_b64 = _read_cloud_init_file_as_base64(cloud_init_file)

    task_id = _build_task_id(
        normalized_region,
        normalized_instance_type,
        normalized_ami_id,
        cloud_init_b64=cloud_init_b64,
    )

    try:
        task_db = _setup_task_status_db(db_path)
        task_db.add_task_with_capability(
            taskid=task_id,
            requirements=[],
            max_tries=max_tries,
            capability=WorkerCapability.LAUNCH.value,
        )
        instance_capability = WorkerCapability(
            normalized_instance_type.split(".", maxsplit=1)[0]
        )

        task_db.add_task_with_capability(
            taskid="bench",
            requirements=[task_id],
            max_tries=max_tries,
            capability=instance_capability.value,
        )
    except Exception as exc:
        raise click.ClickException(
            f"Unable to create task in database '{db_path}': {exc}"
        ) from exc

    click.echo(task_id)
    click.echo(
        f"Created launch task for instance type '{normalized_instance_type}' with AMI "
        f"'{normalized_ami_id}' in region '{normalized_region}'."
    )
