from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3


#: Default benchmark input JSON, relative to the data/ directory of the
#: performance_benchmarks repo (industry_benchmarks branch).
_DEFAULT_BENCHMARK_JSON = "ross_dodecahedron_jacs.json"
_RESULT_MANIFEST_SCHEMA_VERSION = 1


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


def run_benchmark(
    benchmark_repo_path: Path,
    s3_bucket: str,
    task_id: str,
) -> None:
    """Run MD benchmark and upload auditable artifacts to S3.

    Imports ``rbfe_benchmark`` and ``md_benchmark`` directly from the cloned
    ``performance_benchmarks`` repo (``industry_benchmarks`` branch) and
    invokes their Click entry-points via ``standalone_mode=False`` so that
    errors propagate as Python exceptions rather than ``SystemExit``.

    The benchmark input file, output file, and a manifest are uploaded to S3
    under a deterministic run prefix partitioned by UTC date and task-id hash.

    Parameters
    ----------
    benchmark_repo_path : Path
        Absolute path to the root of the cloned performance_benchmarks repo.
    s3_bucket : str
        Name of the S3 bucket to upload result files to.
    task_id : str
        Task ID used to construct the deterministic hashed S3 key prefix.

    Raises
    ------
    FileNotFoundError
        If the benchmark script directory or default input JSON does not exist.
    RuntimeError
        If benchmark execution fails or input JSON cannot be parsed.
    """
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

    # Temporarily prepend the benchmark/ directory to sys.path so that
    # rbfe_benchmark and md_benchmark can be imported directly.
    sys.path.insert(0, str(benchmark_dir))
    try:
        import md_benchmark
        # import rbfe_benchmark
    finally:
        sys.path.pop(0)

    s3_prefix = _build_result_s3_prefix(task_id, started_at)
    s3_client = boto3.client("s3")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # rbfe_out = tmpdir_path / "rbfe_benchmark.out"
        # try:
        #     rbfe_benchmark.run_benchmark.main(
        #         ["--input_file", str(input_file), "--output_file", str(rbfe_out)],
        #         standalone_mode=False,
        #     )
        # except Exception as exc:
        #     raise RuntimeError(f"RBFE benchmark failed: {exc}") from exc
        #
        md_out = tmpdir_path / "md_benchmark.out"
        try:
            md_benchmark.run_benchmark.main(
                ["--input_file", str(input_file), "--output_file", str(md_out)],
                standalone_mode=False,
            )
        except Exception as exc:
            raise RuntimeError(f"MD benchmark failed: {exc}") from exc

        output_json_parse_ok = False
        output_validation_ok = False
        output_validation_message = None
        output_payload: object | None = None
        try:
            output_payload = json.loads(md_out.read_text(encoding="utf-8"))
            output_json_parse_ok = True
            output_validation_ok, output_validation_message = (
                _validate_output_against_input(
                    input_payload,
                    output_payload,
                )
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            output_validation_message = "Output file is not valid JSON."

        input_s3_key = f"{s3_prefix}/input/{input_file.name}"
        output_s3_key = f"{s3_prefix}/output/{md_out.name}"
        manifest_s3_key = f"{s3_prefix}/manifest.json"

        launch_task_id = task_id.removeprefix("bench:")
        completed_at = datetime.now(timezone.utc)
        manifest = {
            "schema_version": _RESULT_MANIFEST_SCHEMA_VERSION,
            "bench_task_id": task_id,
            "launch_task_id": launch_task_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "input": {
                "source_name": input_file.name,
                "s3_key": input_s3_key,
                "sha256": _sha256_file(input_file),
            },
            "output": {
                "source_name": md_out.name,
                "s3_key": output_s3_key,
                "sha256": _sha256_file(md_out),
                "json_parse_ok": output_json_parse_ok,
                "top_level_keys_match_input": output_validation_ok,
                "validation_message": output_validation_message,
            },
            "timestamps": {
                "started_at_utc": _isoformat_utc(started_at),
                "completed_at_utc": _isoformat_utc(completed_at),
            },
        }

        manifest_path = tmpdir_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        s3_client.upload_file(str(input_file), s3_bucket, input_s3_key)
        s3_client.upload_file(str(md_out), s3_bucket, output_s3_key)
        s3_client.upload_file(str(manifest_path), s3_bucket, manifest_s3_key)
