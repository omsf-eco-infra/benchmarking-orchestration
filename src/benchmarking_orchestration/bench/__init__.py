from __future__ import annotations

import abc
import contextlib
import hashlib
import importlib
import io
import json
import shutil
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import fsspec
import s3fs
from fsspec.implementations.github import GithubFileSystem
from fsspec.implementations.local import LocalFileSystem

from ..benchmark_kind import BenchmarkKind, _normalize_benchmark_kind

_DEFAULT_BENCHMARK_JSON = "ross_dodecahedron_jacs.json"
_RESULT_MANIFEST_SCHEMA_VERSION = 4


def _stage_benchmark_inputs(
    benchmark_script_fs: fsspec.AbstractFileSystem,
    source_root: str,
    input_directory: Path,
) -> Path:
    """Copy the benchmark specification and files it references.

    Parameters
    ----------
    benchmark_script_fs : fsspec.AbstractFileSystem
        Filesystem containing the benchmark repository.
    source_root : str
        Repository root prefix including its trailing separator.
    input_directory : Path
        Local artifact directory receiving benchmark inputs.

    Returns
    -------
    Path
        Local benchmark specification path.

    Raises
    ------
    ValueError
        If a referenced input path is unsafe or malformed.
    """
    input_file = input_directory / _DEFAULT_BENCHMARK_JSON
    benchmark_script_fs.get(
        f"{source_root}data/{_DEFAULT_BENCHMARK_JSON}", str(input_file)
    )
    payload = json.loads(input_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or any(
        not isinstance(system, dict) for system in payload.values()
    ):
        raise ValueError("Benchmark input must map system names to JSON objects.")
    for system in payload.values():
        for field in ("protein", "edge", "cofactors"):
            value = system.get(field)
            if value is None:
                continue
            if not isinstance(value, str):
                raise ValueError(f"Benchmark input {field} must be a string.")
            relative_path = PurePosixPath(value)
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise ValueError(
                    f"Benchmark input {field} must be a safe relative path."
                )
            destination = input_directory.joinpath(*relative_path.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            benchmark_script_fs.get(
                f"{source_root}data/{relative_path.as_posix()}", str(destination)
            )
    return input_file


def _build_result_s3_prefix(task_id: str, run_started_at: datetime) -> str:
    """Build the S3 prefix for a benchmark run.

    Parameters
    ----------
    task_id : str
        Benchmark task identifier.
    run_started_at : datetime
        Benchmark start time.

    Returns
    -------
    str
        Date-partitioned prefix with a hashed task identifier.
    """
    date = run_started_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return f"runs/{date}/{hashlib.sha256(task_id.encode()).hexdigest()}"


def _isoformat_utc(timestamp: datetime) -> str:
    """Format a timestamp as an ISO-8601 UTC string.

    Parameters
    ----------
    timestamp : datetime
        Timestamp to format.

    Returns
    -------
    str
        Timestamp with a ``Z`` UTC suffix.
    """
    return (
        timestamp.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _resolve_benchmark_runner(
    benchmark_dir: Path, benchmark_kind: BenchmarkKind
) -> tuple[Any, str, Any]:
    """Import the requested benchmark command from a staged repository.

    Parameters
    ----------
    benchmark_dir : Path
        Local directory containing benchmark modules.
    benchmark_kind : BenchmarkKind
        Benchmark workload to import.

    Returns
    -------
    tuple[Any, str, Any]
        Click command, output filename, and imported module.
    """
    module_name = f"{benchmark_kind.value}_benchmark"
    sys.path.insert(0, str(benchmark_dir))
    try:
        sys.modules.pop(module_name, None)
        module = importlib.import_module(module_name)
    finally:
        sys.path.pop(0)

    return module.run_benchmark, f"{module_name}.out", module


def _invoke_benchmark_command(
    run_command: Any, input_file: Path, output_file: Path
) -> tuple[str, str, Exception | None, str | None]:
    """Run a Click benchmark command while capturing its output.

    Parameters
    ----------
    run_command : Any
        Click command to execute.
    input_file : Path
        Input JSON file.
    output_file : Path
        Output JSON file.

    Returns
    -------
    tuple[str, str, Exception | None, str | None]
        Standard output, standard error, exception, and traceback.
    """
    stdout, stderr = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            run_command.main(
                ["--input_file", str(input_file), "--output_file", str(output_file)],
                standalone_mode=False,
            )
    except Exception as exc:
        return stdout.getvalue(), stderr.getvalue(), exc, traceback.format_exc()
    return stdout.getvalue(), stderr.getvalue(), None, None


def _aggregate_child_outputs(
    _input_payload: object,
    child_output_files: list[Path],
    aggregate_output_file: Path,
    kind: BenchmarkKind,
) -> None:
    """Sum output values from MPS benchmark processes.

    Parameters
    ----------
    _input_payload : object
        Unused legacy argument retained for API compatibility.
    child_output_files : list[Path]
        JSON output files from each child process.
    aggregate_output_file : Path
        Destination for the summed JSON output.
    kind : BenchmarkKind
        Benchmark workload type.
    """
    outputs = [
        json.loads(path.read_text(encoding="utf-8")) for path in child_output_files
    ]
    if not outputs:
        raise RuntimeError("No child benchmark outputs were produced.")

    total = outputs[0]
    for output in outputs[1:]:
        for system, value in output.items():
            if kind is BenchmarkKind.RBFE:
                for component, result in value.items():
                    total[system][component] = float(total[system][component]) + float(
                        result
                    )
            else:
                total[system] = float(total[system]) + float(value)
    aggregate_output_file.write_text(
        json.dumps(total, indent=2, sort_keys=True), encoding="utf-8"
    )


def _run_benchmark_subprocess_entrypoint(
    benchmark_dir: str,
    benchmark_kind: str,
    input_file: str,
    output_file: str,
    stdout_log: str,
    stderr_log: str,
    exception_log: str,
) -> int:
    """Run a benchmark process and write its logs.

    Parameters
    ----------
    benchmark_dir : str
        Local staged benchmark directory.
    benchmark_kind : str
        Benchmark kind value.
    input_file : str
        Input JSON path.
    output_file : str
        Output JSON path.
    stdout_log : str
        Standard output log path.
    stderr_log : str
        Standard error log path.
    exception_log : str
        Exception traceback log path.

    Returns
    -------
    int
        Zero on success, otherwise one.
    """
    command, _name, _module = _resolve_benchmark_runner(
        Path(benchmark_dir), _normalize_benchmark_kind(benchmark_kind)
    )
    stdout, stderr, error, trace = _invoke_benchmark_command(
        command, Path(input_file), Path(output_file)
    )
    Path(stdout_log).write_text(stdout, encoding="utf-8")
    Path(stderr_log).write_text(stderr, encoding="utf-8")
    if trace:
        Path(exception_log).write_text(trace, encoding="utf-8")
    return int(error is not None)


class AbstractBenchmark(abc.ABC):
    """Base class for benchmark implementations."""

    @abc.abstractmethod
    def run_benchmark(self, task_id: str) -> None:
        """Run one benchmark task.

        Parameters
        ----------
        task_id : str
            Identifier of the benchmark task.
        """


def _run_benchmark_to_directory(
    benchmark_script_fs: fsspec.AbstractFileSystem,
    benchmark_root: str,
    output_directory: Path,
    benchmark_kind: BenchmarkKind,
    mps_process_count: int,
    started_at: datetime,
) -> None:
    """Execute a benchmark and write its artifacts to a local directory.

    Parameters
    ----------
    benchmark_script_fs : fsspec.AbstractFileSystem
        Filesystem containing the benchmark repository.
    benchmark_root : str
        Repository root within ``benchmark_script_fs``.
    output_directory : Path
        Caller-owned root for the local input, output, log, and manifest bundle,
        keeping benchmark execution independent from provider upload.
    benchmark_kind : BenchmarkKind
        Workload to execute.
    mps_process_count : int
        Number of benchmark processes.
    started_at : datetime
        Benchmark start time.

    Raises
    ------
    RuntimeError
        If benchmark execution fails or does not produce an output file.
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    input_directory = output_directory / "input"
    result_directory = output_directory / "output"
    log_directory = output_directory / "logs"
    for artifact_directory in (input_directory, result_directory, log_directory):
        shutil.rmtree(artifact_directory, ignore_errors=True)
    (output_directory / "manifest.json").unlink(missing_ok=True)
    input_directory.mkdir(exist_ok=True)
    result_directory.mkdir(exist_ok=True)
    log_directory.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Path(tmpdir)
        benchmark_dir = workspace / "benchmark"
        source_root = f"{benchmark_root.rstrip('/')}/" if benchmark_root else ""
        benchmark_script_fs.get(
            f"{source_root}benchmark", str(benchmark_dir), recursive=True
        )
        input_file = _stage_benchmark_inputs(
            benchmark_script_fs, source_root, input_directory
        )

        command, output_name, _module = _resolve_benchmark_runner(
            benchmark_dir, benchmark_kind
        )
        output_file = result_directory / output_name
        stdout_log = log_directory / "stdout.log"
        stderr_log = log_directory / "stderr.log"
        exception_log = log_directory / "exception_traceback.log"

        if mps_process_count == 1:
            stdout, stderr, error, trace = _invoke_benchmark_command(
                command, input_file, output_file
            )
            stdout_log.write_text(stdout, encoding="utf-8")
            stderr_log.write_text(stderr, encoding="utf-8")
            if trace:
                exception_log.write_text(trace, encoding="utf-8")
        else:
            children = workspace / "children"
            children.mkdir()
            processes: list[tuple[subprocess.Popen[str], Path]] = []
            for index in range(mps_process_count):
                child_output = children / f"{output_file.stem}.process-{index}.out"
                processes.append(
                    (
                        subprocess.Popen(
                            [
                                sys.executable,
                                "-c",
                                "from benchmarking_orchestration.bench import _run_benchmark_subprocess_entrypoint as run; import sys; raise SystemExit(run(*sys.argv[1:]))",
                                str(benchmark_dir),
                                benchmark_kind.value,
                                str(input_file),
                                str(child_output),
                                str(children / f"stdout.process-{index}.log"),
                                str(children / f"stderr.process-{index}.log"),
                                str(
                                    children
                                    / f"exception_traceback.process-{index}.log"
                                ),
                            ]
                        ),
                        child_output,
                    )
                )
            failed = next(
                (process for process, _output in processes if process.wait()), None
            )
            error = RuntimeError("A benchmark subprocess failed.") if failed else None
            trace = str(error) if error else None
            stdout_log.write_text("", encoding="utf-8")
            stderr_log.write_text("", encoding="utf-8")
            if error is None:
                _aggregate_child_outputs(
                    None,
                    [output for _process, output in processes],
                    output_file,
                    benchmark_kind,
                )
            else:
                exception_log.write_text(trace, encoding="utf-8")

        if error is None and not output_file.exists():
            error = RuntimeError("Benchmark did not produce an output file.")
            exception_log.write_text(str(error), encoding="utf-8")

    completed_at = datetime.now(timezone.utc)
    manifest = {
        "schema_version": _RESULT_MANIFEST_SCHEMA_VERSION,
        "benchmark_kind": benchmark_kind.value,
        "mps_process_count": mps_process_count,
        "execution": {
            "success": error is None,
            "error_message": str(error) if error else None,
        },
        "timestamps": {
            "started_at_utc": _isoformat_utc(started_at),
            "completed_at_utc": _isoformat_utc(completed_at),
        },
    }
    manifest_path = output_directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    if error:
        raise RuntimeError(
            f"{benchmark_kind.value.upper()} benchmark failed: {error}"
        ) from error


def run_local_benchmark(
    benchmark_repo_path: Path,
    output_directory: Path,
    benchmark_kind: BenchmarkKind = BenchmarkKind.MD,
    mps_process_count: int = 1,
) -> None:
    """Run a benchmark and create artifacts on the local filesystem.

    Parameters
    ----------
    benchmark_repo_path : Path
        Local performance benchmark repository.
    output_directory : Path
        Caller-owned root for the local input, output, log, and manifest bundle,
        keeping benchmark execution independent from provider upload.
    benchmark_kind : BenchmarkKind, default=BenchmarkKind.MD
        Workload to run.
    mps_process_count : int, default=1
        Number of processes to run.

    Raises
    ------
    RuntimeError
        If benchmark execution fails or does not produce an output file.
    """
    _run_benchmark_to_directory(
        LocalFileSystem(),
        str(benchmark_repo_path),
        output_directory,
        benchmark_kind,
        mps_process_count,
        datetime.now(timezone.utc),
    )


class AWSOpenFEBenchmark(AbstractBenchmark):
    """Run OpenFE benchmarks using fsspec source and artifact filesystems."""

    def __init__(
        self,
        s3_bucket: str,
        benchmark_kind: BenchmarkKind = BenchmarkKind.MD,
        mps_process_count: int = 1,
        benchmark_script_fs: fsspec.AbstractFileSystem | None = None,
        artifact_output: fsspec.AbstractFileSystem | None = None,
        benchmark_root: str = "",
    ) -> None:
        """Configure an AWS benchmark runner.

        Parameters
        ----------
        s3_bucket : str
            Bucket that receives benchmark artifacts.
        benchmark_kind : BenchmarkKind, default=BenchmarkKind.MD
            Workload to execute.
        mps_process_count : int, default=1
            Number of benchmark processes.
        benchmark_script_fs : fsspec.AbstractFileSystem | None, optional
            Filesystem containing the benchmark repository.
        artifact_output : fsspec.AbstractFileSystem | None, optional
            Filesystem receiving benchmark artifacts.
        benchmark_root : str, default=""
            Repository root within ``benchmark_script_fs``.
        """
        self.s3_bucket = s3_bucket
        self.benchmark_kind = benchmark_kind
        self.mps_process_count = mps_process_count
        self.benchmark_script_fs = benchmark_script_fs or GithubFileSystem(
            org="OpenFreeEnergy",
            repo="performance_benchmarks",
        )
        self.artifact_output = artifact_output or s3fs.S3FileSystem()
        self.benchmark_root = benchmark_root.rstrip("/")

    def run_benchmark(self, task_id: str) -> None:
        """Stage, execute, and upload one benchmark task.

        Parameters
        ----------
        task_id : str
            Identifier used to partition uploaded artifacts.
        """
        started_at = datetime.now(timezone.utc)
        prefix = _build_result_s3_prefix(task_id, started_at)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_directory = Path(tmpdir) / "artifacts"
            execution_error = None
            try:
                _run_benchmark_to_directory(
                    self.benchmark_script_fs,
                    self.benchmark_root,
                    output_directory,
                    self.benchmark_kind,
                    self.mps_process_count,
                    started_at,
                )
            except Exception as exc:
                if not (output_directory / "manifest.json").exists():
                    raise
                execution_error = exc

            manifest_path = output_directory / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["s3_bucket"] = self.s3_bucket
            manifest["s3_prefix"] = prefix
            try:
                from benchmarking_orchestration.aws.info import MetadataService

                metadata = MetadataService()
                manifest["instance_id"] = metadata.instance_id()
                manifest["instance_type"] = metadata.instance_type()
                manifest["ami_id"] = metadata.ami_id()
            except Exception:
                pass
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            for artifact in sorted(output_directory.rglob("*")):
                if artifact.is_file():
                    relative_path = artifact.relative_to(output_directory).as_posix()
                    self.artifact_output.put(
                        str(artifact),
                        f"{self.s3_bucket}/{prefix}/{relative_path}",
                    )

            if execution_error is not None:
                raise execution_error


def run_benchmark(
    benchmark_repo_path: Path,
    s3_bucket: str,
    task_id: str,
    benchmark_kind: BenchmarkKind = BenchmarkKind.MD,
    mps_process_count: int = 1,
) -> None:
    """Run a locally staged benchmark through the s3fs implementation.

    Parameters
    ----------
    benchmark_repo_path : Path
        Local performance benchmark repository.
    s3_bucket : str
        Artifact bucket.
    task_id : str
        Benchmark task identifier.
    benchmark_kind : BenchmarkKind, default=BenchmarkKind.MD
        Workload to run.
    mps_process_count : int, default=1
        Number of processes to run.
    """
    AWSOpenFEBenchmark(
        s3_bucket,
        benchmark_kind,
        mps_process_count,
        LocalFileSystem(),
        s3fs.S3FileSystem(),
        str(benchmark_repo_path),
    ).run_benchmark(task_id)
