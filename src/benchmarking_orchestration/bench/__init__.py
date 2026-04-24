from __future__ import annotations
from benchmarking_orchestration.aws.info import MetadataService

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

import boto3

from ..benchmark_kind import BenchmarkKind, _normalize_benchmark_kind
from ..task_id import _parse_bench_task_metadata, _parse_launch_task_id


#: Default benchmark input JSON, relative to the data/ directory of the
#: performance_benchmarks repo (industry_benchmarks branch).
_DEFAULT_BENCHMARK_JSON = "ross_dodecahedron_jacs.json"
_RESULT_MANIFEST_SCHEMA_VERSION = 4


def _build_result_s3_prefix(task_id: str, run_started_at: datetime) -> str:
    """Build a safe and deterministic S3 prefix for one benchmark run.

    Parameters
    ----------
    task_id : str
        Bench task identifier associated with this run.
    run_started_at : datetime
        Run start timestamp used to derive the UTC date partition.

    Returns
    -------
    str
        Prefix in ``runs/<yyyy-mm-dd>/<sha256(task_id)>`` format.

    Notes
    -----
    This avoids embedding base64 cloud-init payloads directly in S3 keys,
    which can introduce slashes and very long paths.
    """
    run_date = run_started_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
    task_id_digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()
    return f"runs/{run_date}/{task_id_digest}"


def _sha256_file(file_path: Path) -> str:
    """Compute the SHA-256 digest for a file.

    Parameters
    ----------
    file_path : Path
        Path to file content to hash.

    Returns
    -------
    str
        Hex-encoded SHA-256 digest.
    """
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _isoformat_utc(timestamp: datetime) -> str:
    """Format a timezone-aware UTC timestamp as ISO-8601 ``Z``.

    Parameters
    ----------
    timestamp : datetime
        Timestamp to serialize.

    Returns
    -------
    str
        ISO-8601 string using ``Z`` suffix.
    """
    return (
        timestamp.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_output_against_input(
    input_payload: object,
    output_payload: object,
) -> tuple[bool, str | None]:
    """Validate output payload shape against benchmark input payload.

    Parameters
    ----------
    input_payload : object
        Parsed benchmark input JSON payload.
    output_payload : object
        Parsed benchmark output JSON payload.

    Returns
    -------
    tuple[bool, str | None]
        Validation result as ``(is_valid, message)``.
    """
    if not isinstance(input_payload, dict):
        return False, "Input JSON must be a top-level object."
    if not isinstance(output_payload, dict):
        return False, "Output JSON must be a top-level object."

    input_keys = set(input_payload.keys())
    output_keys = set(output_payload.keys())
    if input_keys != output_keys:
        return (
            False,
            "Output top-level keys do not match benchmark input systems.",
        )
    return True, None


def _resolve_benchmark_runner(
    benchmark_dir: Path,
    benchmark_kind: BenchmarkKind,
) -> tuple[Any, str, Any]:
    """Resolve the selected benchmark Click command and output filename.

    Parameters
    ----------
    benchmark_dir : Path
        Directory containing benchmark scripts.
    benchmark_kind : BenchmarkKind
        Benchmark workload kind to execute.

    Returns
    -------
    tuple[Any, str, Any]
        Triple of ``(click_command, output_filename, benchmark_module)``.

    Raises
    ------
    RuntimeError
        If benchmark module import or command resolution fails.
    """
    module_name = f"{benchmark_kind.value}_benchmark"
    output_filename = f"{module_name}.out"

    sys.path.insert(0, str(benchmark_dir))
    try:
        importlib.invalidate_caches()
        sys.modules.pop(module_name, None)
        benchmark_module = importlib.import_module(module_name)
    except Exception as exc:
        raise RuntimeError(
            f"Unable to import benchmark module '{module_name}': {exc}"
        ) from exc
    finally:
        sys.path.pop(0)

    run_command = getattr(benchmark_module, "run_benchmark", None)
    if run_command is None or not hasattr(run_command, "main"):
        raise RuntimeError(
            f"Benchmark module '{module_name}' does not expose a Click command "
            "named 'run_benchmark'."
        )
    return run_command, output_filename, benchmark_module


def _patch_rbfe_performance_reader_for_openfe_compat(benchmark_module: Any) -> None:
    """Patch RBFE benchmark performance parsing for OpenFE protocol-unit changes.

    Parameters
    ----------
    benchmark_module : Any
        Imported benchmark module object (for example ``rbfe_benchmark``).

    Notes
    -----
    ``openfe>=1.9`` can return multiple protocol-unit results per repeat.
    Older ``rbfe_benchmark.py`` code assumes the first protocol-unit output
    contains an ``"nc"`` key, which can raise ``KeyError`` when setup/analysis
    units are ordered before run units. This patch replaces
    ``benchmark_module.get_performance`` with a compatible implementation that
    searches all unit outputs for ``"nc"``.
    """
    get_performance = getattr(benchmark_module, "get_performance", None)
    if get_performance is None:
        return

    yaml_module = getattr(benchmark_module, "yaml", None)
    if yaml_module is None:
        return

    def _compat_get_performance(dagres: Any, protocol: Any) -> float:
        """Get final ns/day value from protocol results.

        Parameters
        ----------
        dagres : Any
            Protocol DAG execution result object.
        protocol : Any
            Protocol object exposing ``gather``.

        Returns
        -------
        float
            Final ``ns_per_day`` value from real-time analysis YAML.

        Raises
        ------
        KeyError
            If no protocol unit exposes an ``"nc"`` output key.
        """
        protocol_results = protocol.gather([dagres])
        protocol_data = getattr(protocol_results, "data", {})

        def _iter_path_like_values(value: Any):
            """Yield path-like values from nested output structures."""
            if isinstance(value, (str, Path)):
                yield Path(value)
                return
            if hasattr(value, "resolve"):
                try:
                    yield Path(value)
                except TypeError:
                    pass
                return
            if isinstance(value, dict):
                for nested_value in value.values():
                    yield from _iter_path_like_values(nested_value)
                return
            if isinstance(value, (list, tuple, set)):
                for nested_value in value:
                    yield from _iter_path_like_values(nested_value)

        def _extract_ns_per_day_from_yaml(log_path: Path) -> float | None:
            """Return final ns/day from a real-time analysis YAML file."""
            if not log_path.exists():
                return None
            with log_path.open(encoding="utf-8") as stream:
                data = yaml_module.safe_load(stream)
            if not isinstance(data, list) or not data:
                return None
            timing_data = data[-1].get("timing_data")
            if not isinstance(timing_data, dict):
                return None
            ns_per_day = timing_data.get("ns_per_day")
            if ns_per_day is None:
                return None
            return float(ns_per_day)

        nc_path = None
        observed_output_keys: set[str] = set()
        path_candidates: list[Path] = []
        for protocol_unit_results in protocol_data.values():
            for unit_result in protocol_unit_results:
                outputs = getattr(unit_result, "outputs", {})
                if not isinstance(outputs, dict):
                    continue
                observed_output_keys.update(str(key) for key in outputs.keys())
                for output_value in outputs.values():
                    path_candidates.extend(_iter_path_like_values(output_value))
                candidate = outputs.get("nc")
                if candidate is not None:
                    nc_path = candidate
                    break
            if nc_path is not None:
                break

        if nc_path is not None:
            resolved_nc_path = (
                nc_path.resolve() if hasattr(nc_path, "resolve") else Path(nc_path)
            )
            log_path = resolved_nc_path.parent / "simulation_real_time_analysis.yaml"
            ns_per_day = _extract_ns_per_day_from_yaml(log_path)
            if ns_per_day is not None:
                return ns_per_day

        # openfe>=1.9 can omit an explicit "nc" output key. Fall back to
        # path-like outputs such as checkpoint/trajectory and look for the
        # real-time analysis YAML nearby.
        log_filenames = (
            "simulation_real_time_analysis.yaml",
            "real_time_analysis.yaml",
        )
        for path_candidate in path_candidates:
            candidate_dirs = []
            if path_candidate.is_dir():
                candidate_dirs.append(path_candidate)
            candidate_dirs.append(path_candidate.parent)
            candidate_dirs.append(path_candidate.parent.parent)
            for candidate_dir in candidate_dirs:
                for log_filename in log_filenames:
                    ns_per_day = _extract_ns_per_day_from_yaml(
                        candidate_dir / log_filename
                    )
                    if ns_per_day is not None:
                        return ns_per_day

        observed = ", ".join(sorted(observed_output_keys))
        if not observed:
            observed = "<none>"
        raise KeyError(
            "Unable to resolve benchmark performance from gathered protocol "
            "unit outputs. "
            f"Observed output keys: {observed}."
        )

    benchmark_module.get_performance = _compat_get_performance


def _write_text_file(file_path: Path, content: str) -> None:
    """Write UTF-8 text content to a file path.

    Parameters
    ----------
    file_path : Path
        Destination file path.
    content : str
        Text content to write.
    """
    file_path.write_text(content, encoding="utf-8")


def _invoke_benchmark_command(
    run_command: Any,
    input_file: Path,
    output_file: Path,
) -> tuple[str, str, Exception | None, str | None]:
    """Invoke one benchmark Click command and capture its outputs.

    Parameters
    ----------
    run_command : Any
        Benchmark Click command exposing ``main``.
    input_file : Path
        Benchmark input JSON path.
    output_file : Path
        Benchmark output JSON path to create.

    Returns
    -------
    tuple[str, str, Exception | None, str | None]
        Captured ``(stdout_text, stderr_text, run_exception,
        exception_traceback_text)``.
    """
    stdout_stream = io.StringIO()
    stderr_stream = io.StringIO()
    run_exception: Exception | None = None
    exception_traceback_text: str | None = None

    try:
        with (
            contextlib.redirect_stdout(stdout_stream),
            contextlib.redirect_stderr(stderr_stream),
        ):
            run_command.main(
                [
                    "--input_file",
                    str(input_file),
                    "--output_file",
                    str(output_file),
                ],
                standalone_mode=False,
            )
    except Exception as exc:
        run_exception = exc
        exception_traceback_text = traceback.format_exc()

    return (
        stdout_stream.getvalue(),
        stderr_stream.getvalue(),
        run_exception,
        exception_traceback_text,
    )


def _combine_text_files(destination: Path, source_paths: list[Path]) -> None:
    """Combine multiple text files into a single destination file.

    Parameters
    ----------
    destination : Path
        Output file path.
    source_paths : list[Path]
        Source text files to concatenate in order.
    """
    combined_parts: list[str] = []
    for source_path in source_paths:
        if not source_path.exists():
            continue
        combined_parts.append(f"===== {source_path.name} =====\n")
        combined_parts.append(source_path.read_text(encoding="utf-8"))
        if not combined_parts[-1].endswith("\n"):
            combined_parts.append("\n")
    _write_text_file(destination, "".join(combined_parts))


def _coerce_output_value_to_float(
    value: object,
    output_file: Path,
    system_name: str,
    component_name: str | None = None,
) -> float:
    """Coerce one benchmark output value to a float.

    Parameters
    ----------
    value : object
        Raw JSON value to validate.
    output_file : Path
        Output file being parsed.
    system_name : str
        Benchmark system name associated with the value.
    component_name : str | None, optional
        Optional RBFE component name such as ``"solvent"`` or ``"complex"``.

    Returns
    -------
    float
        Validated numeric value.

    Raises
    ------
    RuntimeError
        If the value cannot be interpreted as a numeric benchmark result.
    """
    value_label = f"system '{system_name}'"
    if component_name is not None:
        value_label = f"system '{system_name}' component '{component_name}'"

    if isinstance(value, bool):
        raise RuntimeError(
            f"Benchmark output file '{output_file.name}' has a non-numeric value "
            f"for {value_label}."
        )
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as exc:
            raise RuntimeError(
                f"Benchmark output file '{output_file.name}' has a non-numeric "
                f"value for {value_label}."
            ) from exc
    raise RuntimeError(
        f"Benchmark output file '{output_file.name}' has a non-numeric value for "
        f"{value_label}."
    )


def _load_numeric_output_payload(
    input_payload: object,
    output_file: Path,
    kind: BenchmarkKind,
) -> dict[str, float | dict[str, float]]:
    """Load one benchmark output JSON and validate numeric payload values.

    Parameters
    ----------
    input_payload : object
        Parsed benchmark input payload used for key validation.
    output_file : Path
        Output JSON file to parse.
    kind: BenchmarkKind,
        Which benchmark was executed

    Returns
    -------
    dict[str, float]
        Validated numeric payload converted to floats.

    Raises
    ------
    RuntimeError
        If the output file is missing, invalid JSON, mismatched against input,
        or contains non-numeric values.
    """
    if not output_file.exists():
        raise RuntimeError(f"Benchmark output file was not produced: '{output_file}'.")

    try:
        output_payload = json.loads(output_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Benchmark output file '{output_file.name}' is not valid JSON: {exc}"
        ) from exc
    # Validate that the input/output keys both exist
    output_validation_ok, output_validation_message = _validate_output_against_input(
        input_payload,
        output_payload,
    )
    if not output_validation_ok:
        raise RuntimeError(
            f"Benchmark output file '{output_file.name}' is invalid: "
            f"{output_validation_message}"
        )

    # Merge
    assert isinstance(output_payload, dict)
    normalized_payload: dict = {}
    match kind:
        case BenchmarkKind.MD:
            for system_name, value in output_payload.items():
                normalized_payload[str(system_name)] = _coerce_output_value_to_float(
                    value,
                    output_file,
                    str(system_name),
                )
        case BenchmarkKind.RBFE:
            for system_name, value in output_payload.items():
                if not isinstance(value, dict):
                    raise RuntimeError(
                        f"Benchmark output file '{output_file.name}' has an invalid "
                        f"RBFE payload for system '{system_name}'."
                    )
                try:
                    solvent_result = value["solvent"]
                    complex_result = value["complex"]
                except KeyError as exc:
                    raise RuntimeError(
                        f"Benchmark output file '{output_file.name}' is missing RBFE "
                        f"component '{exc.args[0]}' for system '{system_name}'."
                    ) from exc
                normalized_payload[str(system_name)] = {
                    "solvent": _coerce_output_value_to_float(
                        solvent_result,
                        output_file,
                        str(system_name),
                        "solvent",
                    ),
                    "complex": _coerce_output_value_to_float(
                        complex_result,
                        output_file,
                        str(system_name),
                        "complex",
                    ),
                }

    return normalized_payload


def _aggregate_child_outputs(
    input_payload: object,
    child_output_files: list[Path],
    aggregate_output_file: Path,
    kind: BenchmarkKind,
) -> None:
    """Aggregate validated child benchmark outputs into one canonical JSON file.

    Parameters
    ----------
    input_payload : object
        Parsed benchmark input payload used for validation.
    child_output_files : list[Path]
        Per-process output JSON files to sum.
    aggregate_output_file : Path
        Destination JSON file for the aggregated output.

    Raises
    ------
    RuntimeError
        If any child output is missing, invalid, or cannot be aggregated.
    """
    if not child_output_files:
        raise RuntimeError("No child benchmark outputs were produced for aggregation.")

    aggregated_payload: dict = {}
    for child_output_file in child_output_files:
        child_payload = _load_numeric_output_payload(
            input_payload, child_output_file, kind
        )
        if not aggregated_payload:
            aggregated_payload = dict(child_payload)
            continue
        match kind:
            case BenchmarkKind.MD:
                for system_name, value in child_payload.items():
                    aggregated_payload[system_name] += value
            case BenchmarkKind.RBFE:
                for system_name, values in child_payload.items():
                    aggregated_payload[system_name]["complex"] += values["complex"]
                    aggregated_payload[system_name]["solvent"] += values["solvent"]

    aggregate_output_file.write_text(
        json.dumps(aggregated_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _run_benchmark_subprocess_entrypoint(
    benchmark_dir_text: str,
    benchmark_kind_text: str,
    input_file_text: str,
    output_file_text: str,
    stdout_log_text: str,
    stderr_log_text: str,
    exception_log_text: str,
) -> int:
    """Execute one benchmark child process and persist its logs to disk.

    Parameters
    ----------
    benchmark_dir_text : str
        Benchmark script directory path.
    benchmark_kind_text : str
        Benchmark kind value.
    input_file_text : str
        Benchmark input JSON path.
    output_file_text : str
        Benchmark output JSON path.
    stdout_log_text : str
        Child stdout log path.
    stderr_log_text : str
        Child stderr log path.
    exception_log_text : str
        Child exception traceback log path.

    Returns
    -------
    int
        Process exit code. ``0`` indicates success.
    """
    benchmark_dir = Path(benchmark_dir_text)
    benchmark_kind = _normalize_benchmark_kind(benchmark_kind_text)
    input_file = Path(input_file_text)
    output_file = Path(output_file_text)
    stdout_log_path = Path(stdout_log_text)
    stderr_log_path = Path(stderr_log_text)
    exception_log_path = Path(exception_log_text)

    stdout_text = ""
    stderr_text = ""
    run_exception: Exception | None = None
    exception_traceback_text: str | None = None
    try:
        run_command, _output_filename, benchmark_module = _resolve_benchmark_runner(
            benchmark_dir,
            benchmark_kind,
        )
        if benchmark_kind is BenchmarkKind.RBFE:
            _patch_rbfe_performance_reader_for_openfe_compat(benchmark_module)
        stdout_text, stderr_text, run_exception, exception_traceback_text = (
            _invoke_benchmark_command(run_command, input_file, output_file)
        )
        if run_exception is None and not output_file.exists():
            run_exception = RuntimeError(
                f"{benchmark_kind.value.upper()} benchmark did not produce output file "
                f"'{output_file.name}'."
            )
            exception_traceback_text = str(run_exception)
    except Exception as exc:
        run_exception = exc
        exception_traceback_text = traceback.format_exc()
    finally:
        _write_text_file(stdout_log_path, stdout_text)
        _write_text_file(stderr_log_path, stderr_text)
        if exception_traceback_text is not None:
            _write_text_file(exception_log_path, exception_traceback_text)

    return 0 if run_exception is None else 1


def run_benchmark(
    benchmark_repo_path: Path,
    s3_bucket: str,
    task_id: str,
    benchmark_kind: BenchmarkKind = BenchmarkKind.MD,
    mps_process_count: int = 1,
) -> None:
    """Run a benchmark and upload auditable artifacts to S3.

    Imports benchmark scripts directly from the cloned ``performance_benchmarks``
    repo (``industry_benchmarks`` branch) and invokes the selected Click
    entry-point via ``standalone_mode=False`` so that failures propagate as
    Python exceptions.

    Input/output files, execution logs, and a manifest are uploaded to S3 under
    a deterministic run prefix partitioned by UTC date and task-id hash.

    Parameters
    ----------
    benchmark_repo_path : Path
        Absolute path to the root of the cloned performance_benchmarks repo.
    s3_bucket : str
        Name of the S3 bucket to upload result files to.
    task_id : str
        Task ID used to construct the deterministic hashed S3 key prefix.
    benchmark_kind : BenchmarkKind, default=BenchmarkKind.MD
        Benchmark workload kind to execute.
    mps_process_count : int, default=1
        Number of concurrent benchmark subprocesses requested by the worker.

    Raises
    ------
    FileNotFoundError
        If the benchmark script directory or default input JSON does not exist.
    RuntimeError
        If benchmark execution fails, input/output JSON parsing fails, or S3
        artifact upload fails.
    """
    if isinstance(benchmark_kind, str):
        benchmark_kind = _normalize_benchmark_kind(benchmark_kind)

    if mps_process_count < 1:
        raise RuntimeError("mps_process_count must be greater than or equal to 1.")

    try:
        task_benchmark_kind, task_mps_process_count, launch_task_id = (
            _parse_bench_task_metadata(task_id)
        )
        launch_region, launch_instance_type, launch_ami_id, launch_cloud_init_b64 = (
            _parse_launch_task_id(launch_task_id)
        )
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    if task_benchmark_kind != benchmark_kind:
        raise RuntimeError(
            "Bench task benchmark kind does not match execution kind. "
            f"Task ID kind: '{task_benchmark_kind.value}', "
            f"requested kind: '{benchmark_kind.value}'."
        )
    if task_mps_process_count != mps_process_count:
        raise RuntimeError(
            "Bench task MPS process count does not match execution configuration. "
            f"Task ID count: '{task_mps_process_count}', "
            f"requested count: '{mps_process_count}'."
        )

    started_at = datetime.now(timezone.utc)

    benchmark_dir = benchmark_repo_path / "benchmark"
    if not benchmark_dir.is_dir():
        raise FileNotFoundError(
            f"Benchmark script directory not found: {benchmark_dir}"
        )

    input_file = benchmark_repo_path / "data" / _DEFAULT_BENCHMARK_JSON
    if not input_file.exists():
        raise FileNotFoundError(f"Benchmark input file not found: {input_file}")

    try:
        input_payload = json.loads(input_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Benchmark input JSON is invalid: {exc}") from exc

    run_command, output_filename, benchmark_module = _resolve_benchmark_runner(
        benchmark_dir,
        benchmark_kind,
    )
    if benchmark_kind is BenchmarkKind.RBFE:
        _patch_rbfe_performance_reader_for_openfe_compat(benchmark_module)

    s3_prefix = _build_result_s3_prefix(task_id, started_at)
    s3_client = boto3.client("s3")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        output_file = tmpdir_path / output_filename
        stdout_log_path = tmpdir_path / "stdout.log"
        stderr_log_path = tmpdir_path / "stderr.log"
        exception_traceback_path = tmpdir_path / "exception_traceback.log"
        child_artifacts_dir = tmpdir_path / "children"
        child_artifacts_dir.mkdir(exist_ok=True)

        run_exception: Exception | None = None
        exception_traceback_text: str | None = None
        child_artifacts: list[dict[str, Path | int]] = []

        if mps_process_count == 1:
            stdout_text, stderr_text, run_exception, exception_traceback_text = (
                _invoke_benchmark_command(run_command, input_file, output_file)
            )
            if run_exception is None and not output_file.exists():
                run_exception = RuntimeError(
                    f"{benchmark_kind.value.upper()} benchmark did not produce output "
                    f"file '{output_file.name}'."
                )
                exception_traceback_text = str(run_exception)

            _write_text_file(stdout_log_path, stdout_text)
            _write_text_file(stderr_log_path, stderr_text)
            if exception_traceback_text is not None:
                _write_text_file(exception_traceback_path, exception_traceback_text)
        else:
            benchmark_subprocess_code = (
                "from benchmarking_orchestration.bench import "
                "_run_benchmark_subprocess_entrypoint; "
                "import sys; "
                "raise SystemExit(_run_benchmark_subprocess_entrypoint(*sys.argv[1:]))"
            )
            benchmark_output_path = Path(output_filename)
            child_processes: list[
                tuple[subprocess.Popen[str], dict[str, Path | int]]
            ] = []
            for process_index in range(mps_process_count):
                child_output_path = child_artifacts_dir / (
                    f"{benchmark_output_path.stem}.process-{process_index}"
                    f"{benchmark_output_path.suffix}"
                )
                child_stdout_log_path = (
                    child_artifacts_dir / f"stdout.process-{process_index}.log"
                )
                child_stderr_log_path = (
                    child_artifacts_dir / f"stderr.process-{process_index}.log"
                )
                child_exception_log_path = (
                    child_artifacts_dir
                    / f"exception_traceback.process-{process_index}.log"
                )
                child_record: dict[str, Path | int] = {
                    "process_index": process_index,
                    "output_path": child_output_path,
                    "stdout_log_path": child_stdout_log_path,
                    "stderr_log_path": child_stderr_log_path,
                    "exception_log_path": child_exception_log_path,
                }
                child_process = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        benchmark_subprocess_code,
                        str(benchmark_dir),
                        benchmark_kind.value,
                        str(input_file),
                        str(child_output_path),
                        str(child_stdout_log_path),
                        str(child_stderr_log_path),
                        str(child_exception_log_path),
                    ]
                )
                child_processes.append((child_process, child_record))

            for child_process, child_record in child_processes:
                child_record["returncode"] = child_process.wait()
                child_artifacts.append(child_record)

            _combine_text_files(
                stdout_log_path,
                [
                    child_artifact["stdout_log_path"]
                    for child_artifact in child_artifacts
                    if isinstance(child_artifact["stdout_log_path"], Path)
                ],
            )
            _combine_text_files(
                stderr_log_path,
                [
                    child_artifact["stderr_log_path"]
                    for child_artifact in child_artifacts
                    if isinstance(child_artifact["stderr_log_path"], Path)
                ],
            )
            child_exception_paths = [
                child_artifact["exception_log_path"]
                for child_artifact in child_artifacts
                if isinstance(child_artifact["exception_log_path"], Path)
                and child_artifact["exception_log_path"].exists()
            ]
            if child_exception_paths:
                _combine_text_files(exception_traceback_path, child_exception_paths)

            failed_child = next(
                (
                    child_artifact
                    for child_artifact in child_artifacts
                    if child_artifact.get("returncode") != 0
                ),
                None,
            )
            if failed_child is not None:
                process_index = int(failed_child["process_index"])
                returncode = int(failed_child["returncode"])
                run_exception = RuntimeError(
                    f"Benchmark subprocess {process_index} failed with exit code "
                    f"{returncode}."
                )
                if not exception_traceback_path.exists():
                    exception_traceback_text = str(run_exception)
                    _write_text_file(exception_traceback_path, exception_traceback_text)
            else:
                try:
                    _aggregate_child_outputs(
                        input_payload,
                        [
                            child_artifact["output_path"]
                            for child_artifact in child_artifacts
                            if isinstance(child_artifact["output_path"], Path)
                        ],
                        output_file,
                        benchmark_kind,
                    )
                except Exception as exc:
                    run_exception = exc
                    exception_traceback_text = traceback.format_exc()
                    if not exception_traceback_path.exists():
                        _write_text_file(
                            exception_traceback_path,
                            exception_traceback_text,
                        )

        output_json_parse_ok = False
        output_validation_ok = False
        output_validation_message = None
        output_s3_key: str | None = None
        output_sha256: str | None = None
        if output_file.exists():
            output_s3_key = f"{s3_prefix}/output/{output_file.name}"
            output_sha256 = _sha256_file(output_file)
            try:
                output_payload = json.loads(output_file.read_text(encoding="utf-8"))
                output_json_parse_ok = True
                output_validation_ok, output_validation_message = (
                    _validate_output_against_input(
                        input_payload,
                        output_payload,
                    )
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                output_validation_message = "Output file is not valid JSON."
        else:
            output_validation_message = "Output file was not produced."

        input_s3_key = f"{s3_prefix}/input/{input_file.name}"
        stdout_s3_key = f"{s3_prefix}/logs/stdout.log"
        stderr_s3_key = f"{s3_prefix}/logs/stderr.log"
        exception_traceback_s3_key: str | None = None
        if exception_traceback_path.exists():
            exception_traceback_s3_key = (
                f"{s3_prefix}/logs/{exception_traceback_path.name}"
            )
        manifest_s3_key = f"{s3_prefix}/manifest.json"

        completed_at = datetime.now(timezone.utc)
        manifest = {
            "schema_version": _RESULT_MANIFEST_SCHEMA_VERSION,
            "benchmark_kind": benchmark_kind.value,
            "mps_process_count": mps_process_count,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "input": {
                "source_name": input_file.name,
                "s3_key": input_s3_key,
                "sha256": _sha256_file(input_file),
            },
            "output": {
                "source_name": output_file.name,
                "s3_key": output_s3_key,
                "sha256": output_sha256,
                "json_parse_ok": output_json_parse_ok,
                "top_level_keys_match_input": output_validation_ok,
                "validation_message": output_validation_message,
            },
            "logs": {
                "stdout_s3_key": stdout_s3_key,
                "stderr_s3_key": stderr_s3_key,
                "exception_traceback_s3_key": exception_traceback_s3_key,
            },
            "execution": {
                "success": run_exception is None,
                "error_type": type(run_exception).__name__ if run_exception else None,
                "error_message": str(run_exception) if run_exception else None,
            },
            "timestamps": {
                "started_at_utc": _isoformat_utc(started_at),
                "completed_at_utc": _isoformat_utc(completed_at),
            },
        }
        try:
            metadata = MetadataService()
            instance_id = metadata.instance_id()
            if instance_id:
                manifest["instance_id"] = instance_id
            instance_type = metadata.instance_type()
            if instance_type:
                manifest["instance_type"] = instance_type
            ami_id = metadata.ami_id()
            if ami_id:
                manifest["ami_id"] = ami_id
            manifest["compute_provider"] = "aws"
        except Exception:
            print("MetadataService doesn't exist")

        manifest_path = tmpdir_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        try:
            s3_client.upload_file(str(input_file), s3_bucket, input_s3_key)
            s3_client.upload_file(str(stdout_log_path), s3_bucket, stdout_s3_key)
            s3_client.upload_file(str(stderr_log_path), s3_bucket, stderr_s3_key)
            if output_s3_key is not None:
                s3_client.upload_file(str(output_file), s3_bucket, output_s3_key)
            for child_artifact in child_artifacts:
                child_output_path = child_artifact["output_path"]
                if isinstance(child_output_path, Path) and child_output_path.exists():
                    s3_client.upload_file(
                        str(child_output_path),
                        s3_bucket,
                        f"{s3_prefix}/output/children/{child_output_path.name}",
                    )
                child_stdout_log_path = child_artifact["stdout_log_path"]
                if (
                    isinstance(child_stdout_log_path, Path)
                    and child_stdout_log_path.exists()
                ):
                    s3_client.upload_file(
                        str(child_stdout_log_path),
                        s3_bucket,
                        f"{s3_prefix}/logs/children/{child_stdout_log_path.name}",
                    )
                child_stderr_log_path = child_artifact["stderr_log_path"]
                if (
                    isinstance(child_stderr_log_path, Path)
                    and child_stderr_log_path.exists()
                ):
                    s3_client.upload_file(
                        str(child_stderr_log_path),
                        s3_bucket,
                        f"{s3_prefix}/logs/children/{child_stderr_log_path.name}",
                    )
                child_exception_log_path = child_artifact["exception_log_path"]
                if (
                    isinstance(child_exception_log_path, Path)
                    and child_exception_log_path.exists()
                ):
                    s3_client.upload_file(
                        str(child_exception_log_path),
                        s3_bucket,
                        f"{s3_prefix}/logs/children/{child_exception_log_path.name}",
                    )
            if exception_traceback_s3_key is not None:
                s3_client.upload_file(
                    str(exception_traceback_path),
                    s3_bucket,
                    exception_traceback_s3_key,
                )
            s3_client.upload_file(str(manifest_path), s3_bucket, manifest_s3_key)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to upload benchmark artifacts to s3://{s3_bucket}/{s3_prefix}: "
                f"{exc}"
            ) from exc

        if run_exception is not None:
            failure_prefix = f"{benchmark_kind.value.upper()} benchmark failed"
            raise RuntimeError(
                f"{failure_prefix}: {run_exception}. "
                f"Manifest: s3://{s3_bucket}/{manifest_s3_key}. "
                f"Stdout log: s3://{s3_bucket}/{stdout_s3_key}. "
                f"Stderr log: s3://{s3_bucket}/{stderr_s3_key}."
            ) from run_exception
