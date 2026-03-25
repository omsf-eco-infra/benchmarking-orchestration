from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from benchmarking_orchestration.benchmark_kind import BenchmarkKind
from benchmarking_orchestration.bench import run_benchmark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_benchmark_repo(tmp_path: Path) -> Path:
    """Create a minimal fake performance_benchmarks repo layout."""
    repo = tmp_path / "performance_benchmarks"
    (repo / "benchmark").mkdir(parents=True)
    (repo / "data").mkdir(parents=True)

    input_json = {
        "system_a": {
            "protein": "protein.pdb",
            "edge": "edge.json",
            "waters": {"solvent": 1000, "complex": 2000},
        }
    }
    (repo / "data" / "ross_dodecahedron_jacs.json").write_text(json.dumps(input_json))
    return repo


def _write_fake_benchmark_script(benchmark_dir: Path, script_name: str) -> None:
    """Write a minimal fake benchmark script that creates its output file."""
    script = benchmark_dir / script_name
    script.write_text(
        "import click\nimport json\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    with open(output_file, 'w') as f:\n"
        "        json.dump({'system_a': 42.0}, f)\n\n"
        "if __name__ == '__main__':\n"
        "    run_benchmark()\n"
    )


# ---------------------------------------------------------------------------
# run_benchmark() unit tests
# ---------------------------------------------------------------------------


def test_run_benchmark_raises_when_benchmark_dir_missing(tmp_path):
    repo = tmp_path / "performance_benchmarks"
    repo.mkdir()
    # no benchmark/ subdir
    (repo / "data").mkdir()
    (repo / "data" / "ross_dodecahedron_jacs.json").write_text("{}")

    with pytest.raises(FileNotFoundError, match="Benchmark script directory not found"):
        run_benchmark(repo, s3_bucket="my-bucket", task_id="bench:us-east-1:g5.xlarge")


def test_run_benchmark_raises_when_input_json_missing(tmp_path):
    repo = tmp_path / "performance_benchmarks"
    (repo / "benchmark").mkdir(parents=True)
    (repo / "data").mkdir()
    # input JSON not written

    with pytest.raises(FileNotFoundError, match="Benchmark input file not found"):
        run_benchmark(repo, s3_bucket="my-bucket", task_id="bench:us-east-1:g5.xlarge")


def test_run_benchmark_wraps_md_failure_as_runtime_error(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"

    _write_fake_benchmark_script(benchmark_dir, "rbfe_benchmark.py")

    failing_script = benchmark_dir / "md_benchmark.py"
    failing_script.write_text(
        "import click\nimport json\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    with open(output_file, 'w') as f:\n"
        "        json.dump({}, f)\n"
        "    raise RuntimeError('MD exploded')\n"
    )

    with patch("benchmarking_orchestration.bench.boto3"):
        with pytest.raises(RuntimeError, match="MD benchmark failed"):
            run_benchmark(repo, s3_bucket="bucket", task_id="bench:task")

    for mod in ("rbfe_benchmark", "md_benchmark"):
        sys.modules.pop(mod, None)


def test_run_benchmark_uses_dated_hashed_prefix_for_cloud_init_task_id(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_benchmark_script(benchmark_dir, "md_benchmark.py")

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:us-east-1:g5.xlarge:ami-1234:"
        "IyEvYmluL2Jhc2gKZWNobyAiaGVsbG8iCg==:"
        "123e4567-e89b-12d3-a456-426614174000"
    )
    expected_hash = hashlib.sha256(task_id.encode("utf-8")).hexdigest()

    with patch(
        "benchmarking_orchestration.bench.boto3.client",
        return_value=_FakeS3Client(),
    ):
        run_benchmark(repo, s3_bucket="bucket", task_id=task_id)

    manifest_key = next(
        key for key in uploaded_by_key if key.endswith("/manifest.json")
    )
    manifest = json.loads(uploaded_by_key[manifest_key])
    run_date = manifest["timestamps"]["started_at_utc"].split("T")[0]
    expected_prefix = f"runs/{run_date}/{expected_hash}"

    input_key = f"{expected_prefix}/input/ross_dodecahedron_jacs.json"
    output_key = f"{expected_prefix}/output/md_benchmark.out"
    stdout_key = f"{expected_prefix}/logs/stdout.log"
    stderr_key = f"{expected_prefix}/logs/stderr.log"
    expected_manifest_key = f"{expected_prefix}/manifest.json"

    assert set(uploaded_by_key) == {
        input_key,
        output_key,
        stdout_key,
        stderr_key,
        expected_manifest_key,
    }

    assert manifest["bench_task_id"] == task_id
    assert manifest["s3_prefix"] == expected_prefix
    assert manifest["benchmark_kind"] == "md"
    assert manifest["input"]["s3_key"] == input_key
    assert manifest["output"]["s3_key"] == output_key
    assert manifest["output"]["json_parse_ok"] is True
    assert manifest["output"]["top_level_keys_match_input"] is True
    assert manifest["logs"]["stdout_s3_key"] == stdout_key
    assert manifest["logs"]["stderr_s3_key"] == stderr_key
    assert manifest["logs"]["exception_traceback_s3_key"] is None
    assert manifest["execution"]["success"] is True

    for mod in ("md_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_executes_rbfe_when_requested(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_benchmark_script(benchmark_dir, "rbfe_benchmark.py")

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:rbfe:us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )

    with patch(
        "benchmarking_orchestration.bench.boto3.client",
        return_value=_FakeS3Client(),
    ):
        run_benchmark(
            repo,
            s3_bucket="bucket",
            task_id=task_id,
            benchmark_kind=BenchmarkKind.RBFE,
        )

    manifest_key = next(
        key for key in uploaded_by_key if key.endswith("/manifest.json")
    )
    manifest = json.loads(uploaded_by_key[manifest_key])

    assert manifest["benchmark_kind"] == "rbfe"
    assert manifest["execution"]["success"] is True
    assert manifest["output"]["source_name"] == "rbfe_benchmark.out"
    assert manifest["output"]["json_parse_ok"] is True

    for mod in ("rbfe_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_uploads_logs_and_manifest_when_failure_occurs(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"

    failing_script = benchmark_dir / "md_benchmark.py"
    failing_script.write_text(
        "import click\nimport json\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    print('about to fail')\n"
        "    with open(output_file, 'w') as f:\n"
        "        json.dump({'system_a': 42.0}, f)\n"
        "    raise RuntimeError('md failed')\n"
    )

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:md:us-east-1:g5.xlarge:ami-1234:123e4567-e89b-12d3-a456-426614174000"
    )

    with patch(
        "benchmarking_orchestration.bench.boto3.client",
        return_value=_FakeS3Client(),
    ):
        with pytest.raises(RuntimeError, match="MD benchmark failed"):
            run_benchmark(
                repo,
                s3_bucket="bucket",
                task_id=task_id,
            )

    manifest_key = next(
        key for key in uploaded_by_key if key.endswith("/manifest.json")
    )
    stdout_key = next(
        key for key in uploaded_by_key if key.endswith("/logs/stdout.log")
    )
    stderr_key = next(
        key for key in uploaded_by_key if key.endswith("/logs/stderr.log")
    )
    exception_key = next(
        key for key in uploaded_by_key if key.endswith("/logs/exception_traceback.log")
    )

    manifest = json.loads(uploaded_by_key[manifest_key])
    assert manifest["execution"]["success"] is False
    assert manifest["execution"]["error_type"] == "RuntimeError"
    assert manifest["logs"]["stdout_s3_key"] == stdout_key
    assert manifest["logs"]["stderr_s3_key"] == stderr_key
    assert manifest["logs"]["exception_traceback_s3_key"] == exception_key
    assert "about to fail" in uploaded_by_key[stdout_key]
    assert "md failed" in uploaded_by_key[exception_key]

    for mod in ("md_benchmark",):
        sys.modules.pop(mod, None)
