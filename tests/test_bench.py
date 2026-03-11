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
