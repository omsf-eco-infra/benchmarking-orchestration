from __future__ import annotations

import os
from pathlib import Path

import click

from .benchmark_kind import (
    BenchmarkKind,
    _benchmark_kind_choices,
    _parse_benchmark_kind,
)
from .capabilities import (
    WorkerCapability,
    _parse_worker_capability,
    _resolve_bench_worker_capability,
    _worker_capability_choices,
)
from .cloud_init import _decode_cloud_init_base64, _read_cloud_init_file_as_base64
from .normalization import (
    _normalize_ami_id,
    _normalize_db_path,
    _normalize_instance_type,
    _normalize_required_value,
    _normalize_region,
)
from .providers import (
    DEFAULT_LAUNCH_AMI_ID,
    DEFAULT_PROVIDER_NAME,
    LaunchSpec,
    get_provider,
    _provider_choices,
)
from exorcist.models import TaskStatus

from .analysis import fetch_and_analyze_results, print_results_table
from .bench import run_benchmark
from .task_id import (
    _build_bench_task_id,
    _build_task_id,
    _parse_bench_task_id,
    _parse_launch_task_id,
)
from .tasks import TaskStatusDB


def _setup_task_status_db(db_path: str | None) -> TaskStatusDB:
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
    normalized_db_path = (
        Path(_normalize_db_path(db_path))
        if db_path is not None
        else Path("task_status.db")
    )
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


def _normalize_provider_name(provider_name: str) -> str:
    """Normalize and lowercase a provider selector.

    Parameters
    ----------
    provider_name : str
        Raw provider value from CLI input.

    Returns
    -------
    str
        Normalized provider value.
    """
    return _normalize_required_value("provider", provider_name).lower()


def validate_launch_instance_type(
    instance_type: str,
    region: str = "us-east-1",
    provider_name: str = DEFAULT_PROVIDER_NAME,
) -> None:
    """Validate launch instance-type fields through the selected provider.

    Parameters
    ----------
    instance_type : str
        Instance type or shape identifier.
    region : str, default="us-east-1"
        Provider region used for validation.
    provider_name : str, default="aws"
        Provider selector.
    """
    provider = get_provider(provider_name)
    provider.validate_spec(LaunchSpec(instance_type=instance_type, region=region))


def validate_launch_ami(
    ami_id: str,
    region: str = "us-east-1",
    provider_name: str = DEFAULT_PROVIDER_NAME,
) -> None:
    """Validate launch image fields through the selected provider.

    Parameters
    ----------
    ami_id : str
        Image identifier to validate.
    region : str, default="us-east-1"
        Provider region used for validation.
    provider_name : str, default="aws"
        Provider selector.
    """
    provider = get_provider(provider_name)
    provider.validate_spec(LaunchSpec(ami_id=ami_id, region=region))


def launch_ec2_instance(
    instance_type: str,
    ami_id: str = DEFAULT_LAUNCH_AMI_ID,
    region: str = "us-east-1",
    user_data: str | None = None,
    key_name: str | None = None,
    instance_profile_name: str | None = None,
    provider_name: str = DEFAULT_PROVIDER_NAME,
) -> str:
    """Submit a launch request through the selected provider.

    Parameters
    ----------
    instance_type : str
        Instance type or shape identifier.
    ami_id : str, default=DEFAULT_LAUNCH_AMI_ID
        Image identifier for launch.
    region : str, default="us-east-1"
        Provider region for launch.
    user_data : str, optional
        Startup script payload.
    key_name : str, optional
        Optional SSH key pair name.
    instance_profile_name : str, optional
        Optional provider identity/profile attachment name.
    provider_name : str, default="aws"
        Provider selector.

    Returns
    -------
    str
        Provider launch handle.
    """
    provider = get_provider(provider_name)
    return provider.submit(
        LaunchSpec(
            instance_type=instance_type,
            ami_id=ami_id,
            region=region,
            user_data=user_data,
            key_name=key_name,
            instance_profile_name=instance_profile_name,
        )
    )


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
@click.option(
    "--provider",
    default=DEFAULT_PROVIDER_NAME,
    show_default=True,
    type=click.Choice(_provider_choices(), case_sensitive=False),
    help="Compute provider used for launch tasks.",
)
@click.option("--db-path", default=None, type=str)
@click.option(
    "--bench-repo-path",
    default="/opt/dlami/nvme/performance_benchmarks",
    show_default=True,
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    envvar="BENCHMARK_REPO_PATH",
    help=(
        "Path to the cloned performance_benchmarks repo (or set BENCHMARK_REPO_PATH)."
    ),
)
def worker(
    capability: WorkerCapability,
    provider: str,
    db_path: str,
    bench_repo_path: Path,
) -> None:
    """Run a worker with a selected capability.

    Parameters
    ----------
    capability : WorkerCapability
        Worker capability used to select which tasks to process.
    provider : str
        Compute provider used for launch tasks.
    db_path : str
        Optional filesystem path to the task status database.
    bench_repo_path : Path
        Path to the cloned ``performance_benchmarks`` repository.
    """
    normalized_provider = _normalize_provider_name(provider)
    db_path_label = db_path if db_path is not None else "task_status.db"

    try:
        task_db = _setup_task_status_db(db_path)
        task = task_db.check_out_task_with_capability(capability.value)
    except Exception as exc:
        raise click.ClickException(
            f"Unable to check out task from database '{db_path_label}': {exc}"
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
                ec2_key_name = os.environ.get("EC2_KEY_NAME") or None
                instance_profile_name = (
                    os.environ.get("EC2_IAM_INSTANCE_PROFILE") or None
                )
                instance_id = launch_ec2_instance(
                    task_instance_type,
                    ami_id=task_ami_id,
                    region=task_region,
                    user_data=cloud_init_user_data,
                    key_name=ec2_key_name,
                    instance_profile_name=instance_profile_name,
                    provider_name=normalized_provider,
                )
            except Exception as exc:
                try:
                    task_db.mark_task_completed(task, success=False)
                except Exception as mark_exc:
                    raise click.ClickException(
                        f"Failed to process launch task '{task}' and failed to mark it as failed "
                        f"in database '{db_path_label}': {mark_exc}. Original error: {exc}"
                    ) from exc
                raise click.ClickException(
                    f"Failed to process launch task '{task}': {exc}"
                ) from exc

            try:
                task_db.mark_task_completed(task, success=True)
            except Exception as exc:
                raise click.ClickException(
                    f"Launched instance '{instance_id}' for task '{task}', but failed to mark it "
                    f"as completed in database '{db_path_label}': {exc}"
                ) from exc

            click.echo(f"Processed launch task '{task}' with instance '{instance_id}'.")
        case _:
            # Run the benchmark workload then report results.
            try:
                benchmark_kind, _launch_task_id = _parse_bench_task_id(task)
            except ValueError as exc:
                try:
                    task_db.mark_task_completed(task, success=False)
                except Exception:
                    pass
                raise click.ClickException(str(exc)) from exc

            s3_bucket = os.environ.get("S3_BUCKET")
            if not s3_bucket:
                try:
                    task_db.mark_task_completed(task, success=False)
                except Exception:
                    pass
                raise click.ClickException(
                    "S3_BUCKET environment variable is required for bench tasks."
                )

            bench_repo = bench_repo_path
            try:
                run_benchmark(
                    benchmark_repo_path=bench_repo,
                    s3_bucket=s3_bucket,
                    task_id=task,
                    benchmark_kind=benchmark_kind,
                )
            except Exception as exc:
                try:
                    task_db.mark_task_completed(task, success=False)
                except Exception as mark_exc:
                    raise click.ClickException(
                        f"Bench task '{task}' failed and could not be marked as failed "
                        f"in database '{db_path_label}': {mark_exc}. Original error: {exc}"
                    ) from exc
                raise click.ClickException(
                    f"Bench task '{task}' failed: {exc}"
                ) from exc

            try:
                task_db.mark_task_completed(task, success=True)
            except Exception as exc:
                raise click.ClickException(
                    f"Bench task '{task}' completed but could not be marked as succeeded "
                    f"in database '{db_path_label}': {exc}"
                ) from exc

            click.echo(
                f"Processed bench task '{task}' (kind '{benchmark_kind.value}') "
                f"with capability '{capability.value}'."
            )


@cli.command(
    "create-launch-task",
    help="Create launch and benchmark task entries in TaskStatusDB.",
)
@click.option("--instance-type", required=True, type=str)
@click.option("--region", default="us-east-1", show_default=True, type=str)
@click.option("--ami-id", default=DEFAULT_LAUNCH_AMI_ID, show_default=True, type=str)
@click.option("--cloud-init-file", default=None, type=str)
@click.option(
    "--provider",
    default=DEFAULT_PROVIDER_NAME,
    show_default=True,
    type=click.Choice(_provider_choices(), case_sensitive=False),
    help="Compute provider used for launch task orchestration.",
)
@click.option("--db-path", default=None, show_default=False, type=str)
@click.option("--max-tries", default=1, show_default=True, type=click.IntRange(min=1))
@click.option(
    "--benchmark-kind",
    default=BenchmarkKind.MD.value,
    show_default=True,
    type=click.Choice(_benchmark_kind_choices(), case_sensitive=False),
    callback=_parse_benchmark_kind,
    help="Benchmark workload kind for the dependent bench task.",
)
@click.option(
    "--s3-bucket",
    default=None,
    show_default=False,
    type=str,
    envvar="BENCHMARK_S3_BUCKET",
    help="S3 bucket for benchmark result uploads (or set BENCHMARK_S3_BUCKET env var).",
)
def create_launch_task(
    instance_type: str,
    region: str,
    ami_id: str,
    cloud_init_file: str | None,
    provider: str,
    db_path: str | None,
    max_tries: int,
    benchmark_kind: BenchmarkKind,
    s3_bucket: str | None,
) -> None:
    """Create launch and benchmark task entries in TaskStatusDB.

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
    provider : str
        Compute provider used for launch validation and submission.
    db_path : str | None
        Filesystem path to the task status database.
    max_tries : int
        Maximum total execution attempts for each created task.
    benchmark_kind : BenchmarkKind
        Benchmark workload kind for the dependent bench task.
    s3_bucket : str, optional
        S3 bucket name injected into the cloud-init template as ``S3_BUCKET``.
        Falls back to the ``BENCHMARK_S3_BUCKET`` environment variable.

    Raises
    ------
    click.ClickException
        If AWS validation fails or task insertion into the DB fails.
    """
    normalized_instance_type = _normalize_instance_type(instance_type)
    normalized_region = _normalize_region(region)
    normalized_ami_id = _normalize_ami_id(ami_id)
    normalized_provider = _normalize_provider_name(provider)

    try:
        validate_launch_instance_type(
            normalized_instance_type,
            normalized_region,
            provider_name=normalized_provider,
        )
        validate_launch_ami(
            normalized_ami_id,
            normalized_region,
            provider_name=normalized_provider,
        )
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    instance_capability = _resolve_bench_worker_capability(normalized_instance_type)
    extra_vars: dict[str, str] = {"GPU_CAPABILITY": instance_capability.value}
    if s3_bucket is not None:
        extra_vars["S3_BUCKET"] = s3_bucket
    cloud_init_b64 = _read_cloud_init_file_as_base64(
        cloud_init_file,
        extra_vars=extra_vars,
    )

    task_id = _build_task_id(
        normalized_region,
        normalized_instance_type,
        normalized_ami_id,
        cloud_init_b64=cloud_init_b64,
    )
    bench_task_id = _build_bench_task_id(task_id, benchmark_kind)

    db_path_label = db_path if db_path is not None else "task_status.db"

    try:
        task_db = _setup_task_status_db(db_path)
        task_db.add_task_with_capability(
            taskid=task_id,
            requirements=[],
            max_tries=max_tries,
            capability=WorkerCapability.LAUNCH.value,
        )
        task_db.add_task_with_capability(
            taskid=bench_task_id,
            requirements=[task_id],
            max_tries=max_tries,
            capability=instance_capability.value,
        )
    except Exception as exc:
        raise click.ClickException(
            f"Unable to create task in database '{db_path_label}': {exc}"
        ) from exc

    click.echo(task_id)
    click.echo(
        f"Created launch task for instance type '{normalized_instance_type}' with AMI "
        f"'{normalized_ami_id}' in region '{normalized_region}'."
    )


@cli.command(
    "analyze-results",
    help="Fetch S3 benchmark manifests and summarize results by instance type.",
)
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket containing benchmark results.",
)
@click.option(
    "--s3-prefix",
    default="runs/",
    show_default=True,
    help="S3 key prefix to search under.",
)
def analyze_results(s3_bucket: str, s3_prefix: str) -> None:
    """Fetch S3 benchmark manifests and summarize results by instance type.

    Parameters
    ----------
    s3_bucket : str
        S3 bucket containing benchmark results.
    s3_prefix : str
        S3 key prefix to search under.
    """
    try:
        records = fetch_and_analyze_results(s3_bucket, s3_prefix)
    except Exception as exc:
        raise click.ClickException(f"Failed to fetch results: {exc}") from exc
    print_results_table(records)


_STATUS_COLUMNS = [
    ("BLOCKED", TaskStatus.BLOCKED),
    ("AVAILABLE", TaskStatus.AVAILABLE),
    ("IN_PROGRESS", TaskStatus.IN_PROGRESS),
    ("COMPLETED", TaskStatus.COMPLETED),
    ("FAILED", TaskStatus.TOO_MANY_RETRIES),
    ("ERROR", TaskStatus.ERROR),
]


def _print_status_table(rows: list[dict]) -> None:
    """Print a pivot table of task counts by capability and status.

    Parameters
    ----------
    rows : list[dict]
        List of dicts with keys ``capability``, ``status``, and ``count``,
        as returned by :meth:`.TaskStatusDB.get_status_summary`.
    """
    # Build a mapping: capability -> status_value -> count
    counts: dict[str, dict[int, int]] = {}
    for row in rows:
        cap = row["capability"]
        counts.setdefault(cap, {})
        counts[cap][row["status"]] = row["count"]

    if not counts:
        click.echo("No tasks found.")
        return

    col_labels = [label for label, _ in _STATUS_COLUMNS]
    col_width = max(len(label) for label in col_labels) + 2
    cap_width = max(len(cap) for cap in counts) + 2

    header = f"{'Capability':<{cap_width}}" + "".join(
        f"{label:>{col_width}}" for label in col_labels
    )
    click.echo(header)
    click.echo("-" * len(header))

    for cap in sorted(counts):
        row_str = f"{cap:<{cap_width}}"
        for _, status in _STATUS_COLUMNS:
            n = counts[cap].get(status.value, 0)
            row_str += f"{n:>{col_width}}"
        click.echo(row_str)


@cli.command("status", help="Show task status summary grouped by capability.")
@click.option("--db-path", default=None, type=str)
def task_status(db_path: str | None) -> None:
    """Show a summary of task counts grouped by capability and status.

    Parameters
    ----------
    db_path : str, optional
        Filesystem path to the task status database.
    """
    db_path_label = db_path if db_path is not None else "task_status.db"
    try:
        task_db = _setup_task_status_db(db_path)
        rows = task_db.get_status_summary()
    except Exception as exc:
        raise click.ClickException(
            f"Unable to read database '{db_path_label}': {exc}"
        ) from exc
    _print_status_table(rows)
