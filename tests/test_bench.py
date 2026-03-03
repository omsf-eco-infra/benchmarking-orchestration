from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

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


def test_run_benchmark_calls_scripts_and_uploads_to_s3(tmp_path, monkeypatch):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_benchmark_script(benchmark_dir, "rbfe_benchmark.py")
    _write_fake_benchmark_script(benchmark_dir, "md_benchmark.py")

    upload_calls: list[dict] = []

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            upload_calls.append({"filename": filename, "bucket": bucket, "key": key})

    with patch("benchmarking_orchestration.bench.boto3") as mock_boto3:
        mock_boto3.client.return_value = _FakeS3Client()

        run_benchmark(
            repo,
            s3_bucket="my-results-bucket",
            task_id="bench:us-east-1:g5.xlarge:abc123",
        )

    assert len(upload_calls) == 2
    uploaded_names = {Path(c["filename"]).name for c in upload_calls}
    assert uploaded_names == {"rbfe_benchmark.out", "md_benchmark.out"}

    expected_prefix = "bench/us-east-1/g5.xlarge/abc123"
    for call in upload_calls:
        assert call["bucket"] == "my-results-bucket"
        assert call["key"].startswith(expected_prefix)

    # Clean up dynamically imported modules so they don't pollute other tests
    for mod in ("rbfe_benchmark", "md_benchmark"):
        sys.modules.pop(mod, None)


def test_run_benchmark_wraps_rbfe_failure_as_runtime_error(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"

    # rbfe script that raises
    failing_script = benchmark_dir / "rbfe_benchmark.py"
    failing_script.write_text(
        "import click\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    raise RuntimeError('CUDA not available')\n"
    )
    _write_fake_benchmark_script(benchmark_dir, "md_benchmark.py")

    with patch("benchmarking_orchestration.bench.boto3"):
        with pytest.raises(RuntimeError, match="RBFE benchmark failed"):
            run_benchmark(repo, s3_bucket="bucket", task_id="bench:task")

    for mod in ("rbfe_benchmark", "md_benchmark"):
        sys.modules.pop(mod, None)


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


def test_run_benchmark_s3_key_uses_slash_separated_task_id(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_benchmark_script(benchmark_dir, "rbfe_benchmark.py")
    _write_fake_benchmark_script(benchmark_dir, "md_benchmark.py")

    upload_calls: list[dict] = []

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            upload_calls.append({"key": key})

    with patch("benchmarking_orchestration.bench.boto3") as mock_boto3:
        mock_boto3.client.return_value = _FakeS3Client()
        run_benchmark(
            repo,
            s3_bucket="bucket",
            task_id="bench:us-east-1:g5.xlarge:deadbeef",
        )

    for call in upload_calls:
        assert ":" not in call["key"], "S3 keys must not contain colons"
        assert call["key"].startswith("bench/us-east-1/g5.xlarge/deadbeef/")

    for mod in ("rbfe_benchmark", "md_benchmark"):
        sys.modules.pop(mod, None)
