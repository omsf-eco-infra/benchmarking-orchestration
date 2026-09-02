from __future__ import annotations

import abc
import contextlib
import hashlib
import importlib
import io
import json
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fsspec
import s3fs
from fsspec.implementations.github import GithubFileSystem
from fsspec.implementations.local import LocalFileSystem

from benchmarking_orchestration.aws.info import MetadataService

from ..benchmark_kind import BenchmarkKind, _normalize_benchmark_kind

_DEFAULT_BENCHMARK_JSON = "ross_dodecahedron_jacs.json"
_RESULT_MANIFEST_SCHEMA_VERSION = 5


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


def _combine_text_files(destination: Path, source_paths: list[Path]) -> None:
    """Combine labeled child logs into one artifact.

    Parameters
    ----------
    destination : Path
        Output log path.
    source_paths : list[Path]
        Child log paths in process order.
    """
    parts = []
    for source_path in source_paths:
        if source_path.exists():
            content = source_path.read_text(encoding="utf-8")
            parts.append(f"===== {source_path.name} =====\n{content}")
            if not content.endswith("\n"):
                parts.append("\n")
    destination.write_text("".join(parts), encoding="utf-8")


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
    stdout = stderr = ""
    error = None
    trace = None
    try:
        command, _name, _module = _resolve_benchmark_runner(
            Path(benchmark_dir), _normalize_benchmark_kind(benchmark_kind)
        )
        stdout, stderr, error, trace = _invoke_benchmark_command(
            command, Path(input_file), Path(output_file)
        )
    except Exception as exc:
        error = exc
        trace = traceback.format_exc()
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
            sha="industry_benchmarks",
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
            workspace = Path(tmpdir)
            benchmark_dir = workspace / "benchmark"
            input_file = workspace / "data" / _DEFAULT_BENCHMARK_JSON
            input_file.parent.mkdir()
            source_root = f"{self.benchmark_root}/" if self.benchmark_root else ""
            self.benchmark_script_fs.get(
                f"{source_root}benchmark", str(benchmark_dir), recursive=True
            )
            self.benchmark_script_fs.get(
                f"{source_root}data/{_DEFAULT_BENCHMARK_JSON}", str(input_file)
            )

            command, output_name, _module = _resolve_benchmark_runner(
                benchmark_dir, self.benchmark_kind
            )
            output_file = workspace / output_name
            stdout_log, stderr_log = workspace / "stdout.log", workspace / "stderr.log"
            exception_log = workspace / "exception_traceback.log"

            if self.mps_process_count == 1:
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
                processes: list[tuple[subprocess.Popen[bytes], Path]] = []
                for index in range(self.mps_process_count):
                    child_output = children / f"{output_file.stem}.process-{index}.out"
                    processes.append(
                        (
                            subprocess.Popen(
                                [
                                    sys.executable,
                                    "-c",
                                    "from benchmarking_orchestration.bench import _run_benchmark_subprocess_entrypoint as run; import sys; raise SystemExit(run(*sys.argv[1:]))",
                                    str(benchmark_dir),
                                    self.benchmark_kind.value,
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
                return_codes = [process.wait() for process, _output in processes]
                failed = any(return_codes)
                error = (
                    RuntimeError("A benchmark subprocess failed.") if failed else None
                )
                _combine_text_files(
                    stdout_log,
                    [
                        children / f"stdout.process-{index}.log"
                        for index in range(self.mps_process_count)
                    ],
                )
                _combine_text_files(
                    stderr_log,
                    [
                        children / f"stderr.process-{index}.log"
                        for index in range(self.mps_process_count)
                    ],
                )
                child_exception_logs = [
                    children / f"exception_traceback.process-{index}.log"
                    for index in range(self.mps_process_count)
                ]
                if any(path.exists() for path in child_exception_logs):
                    _combine_text_files(exception_log, child_exception_logs)
                if error is None:
                    try:
                        _aggregate_child_outputs(
                            None,
                            [output for _process, output in processes],
                            output_file,
                            self.benchmark_kind,
                        )
                    except Exception as exc:
                        error = exc
                        exception_log.write_text(
                            traceback.format_exc(), encoding="utf-8"
                        )
                elif not exception_log.exists() or not exception_log.stat().st_size:
                    exception_log.write_text(str(error), encoding="utf-8")

            if error is None and not output_file.exists():
                error = RuntimeError("Benchmark did not produce an output file.")
                exception_log.write_text(str(error), encoding="utf-8")

            completed_at = datetime.now(timezone.utc)
            artifacts = [input_file, stdout_log, stderr_log]
            if output_file.exists():
                artifacts.append(output_file)
            if exception_log.exists():
                artifacts.append(exception_log)
            for artifact in artifacts:
                category = (
                    "input"
                    if artifact == input_file
                    else "output"
                    if artifact == output_file
                    else "logs"
                )
                self.artifact_output.put(
                    str(artifact),
                    f"{self.s3_bucket}/{prefix}/{category}/{artifact.name}",
                )

            manifest = {
                "schema_version": _RESULT_MANIFEST_SCHEMA_VERSION,
                "benchmark_kind": self.benchmark_kind.value,
                "mps_process_count": self.mps_process_count,
                "s3_bucket": self.s3_bucket,
                "s3_prefix": prefix,
                "execution": {
                    "success": error is None,
                    "error_message": str(error) if error else None,
                },
                "timestamps": {
                    "started_at_utc": _isoformat_utc(started_at),
                    "completed_at_utc": _isoformat_utc(completed_at),
                },
            }
            try:
                metadata = MetadataService()
                manifest["instance_id"] = metadata.instance_id()
                manifest["instance_type"] = metadata.instance_type()
                manifest["ami_id"] = metadata.ami_id()
            except Exception:
                pass
            manifest_path = workspace / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            self.artifact_output.put(
                str(manifest_path), f"{self.s3_bucket}/{prefix}/manifest.json"
            )

            if error:
                raise RuntimeError(
                    f"{self.benchmark_kind.value.upper()} benchmark failed: {error}"
                ) from error


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
