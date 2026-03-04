from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import boto3


#: Default benchmark input JSON, relative to the data/ directory of the
#: performance_benchmarks repo (industry_benchmarks branch).
_DEFAULT_BENCHMARK_JSON = "ross_dodecahedron_jacs.json"


def run_benchmark(
    benchmark_repo_path: Path,
    s3_bucket: str,
    task_id: str,
) -> None:
    """Run RBFE and MD benchmarks and upload results to S3.

    Imports ``rbfe_benchmark`` and ``md_benchmark`` directly from the cloned
    ``performance_benchmarks`` repo (``industry_benchmarks`` branch) and
    invokes their Click entry-points via ``standalone_mode=False`` so that
    errors propagate as Python exceptions rather than ``SystemExit``.

    Both output files are uploaded to S3 under a prefix derived from
    ``task_id`` with colons replaced by forward-slashes.

    Parameters
    ----------
    benchmark_repo_path : Path
        Absolute path to the root of the cloned performance_benchmarks repo.
    s3_bucket : str
        Name of the S3 bucket to upload result files to.
    task_id : str
        Task ID used to construct the S3 key prefix (colons become slashes).

    Raises
    ------
    FileNotFoundError
        If the benchmark script directory or default input JSON does not exist.
    RuntimeError
        If either benchmark script raises during execution.
    """
    benchmark_dir = benchmark_repo_path / "benchmark"
    if not benchmark_dir.is_dir():
        raise FileNotFoundError(
            f"Benchmark script directory not found: {benchmark_dir}"
        )

    input_file = benchmark_repo_path / "data" / _DEFAULT_BENCHMARK_JSON
    if not input_file.exists():
        raise FileNotFoundError(f"Benchmark input file not found: {input_file}")

    # Temporarily prepend the benchmark/ directory to sys.path so that
    # rbfe_benchmark and md_benchmark can be imported directly.
    sys.path.insert(0, str(benchmark_dir))
    try:
        import md_benchmark
        # import rbfe_benchmark
    finally:
        sys.path.pop(0)

    s3_prefix = task_id.replace(":", "/")
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

        s3_key = f"{s3_prefix}/{md_out.name}"
        s3_client.upload_file(str(md_out), s3_bucket, s3_key)
