from __future__ import annotations

import json
from pathlib import Path

import fsspec
import pytest

from benchmarking_orchestration.bench import (
    AWSOpenFEBenchmark,
    _aggregate_child_outputs,
    run_local_benchmark,
)
from benchmarking_orchestration.benchmark_kind import BenchmarkKind


def _benchmark_repo(tmp_path: Path) -> Path:
    """Create a minimal local benchmark repository.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.

    Returns
    -------
    Path
        Repository root.
    """
    (tmp_path / "benchmark").mkdir(parents=True)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "ross_dodecahedron_jacs.json").write_text(
        "{}", encoding="utf-8"
    )
    (tmp_path / "benchmark" / "md_benchmark.py").write_text(
        "import json\n"
        "class Command:\n"
        "    def main(self, args, standalone_mode=False):\n"
        "        with open(args[3], 'w') as output:\n"
        "            json.dump({'system_a': 42.0}, output)\n"
        "run_benchmark = Command()\n",
        encoding="utf-8",
    )
    return tmp_path


def test_local_benchmark_writes_artifacts_without_provider_metadata(
    tmp_path: Path,
) -> None:
    """Write benchmark artifacts to a caller-provided local directory.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    benchmark_repo = _benchmark_repo(tmp_path / "benchmark-repo")
    output_directory = tmp_path / "artifacts"

    run_local_benchmark(benchmark_repo, output_directory)

    assert json.loads(
        (output_directory / "output" / "md_benchmark.out").read_text(encoding="utf-8")
    ) == {"system_a": 42.0}
    assert (output_directory / "input" / "ross_dodecahedron_jacs.json").is_file()
    assert (output_directory / "logs" / "stdout.log").is_file()
    assert (output_directory / "logs" / "stderr.log").is_file()
    manifest = json.loads(
        (output_directory / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["execution"] == {"success": True, "error_message": None}
    assert "s3_bucket" not in manifest
    assert "s3_prefix" not in manifest
    assert "instance_id" not in manifest


def test_local_benchmark_keeps_failure_logs_and_manifest(tmp_path: Path) -> None:
    """Keep local diagnostic artifacts when benchmark execution fails.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    benchmark_repo = _benchmark_repo(tmp_path / "benchmark-repo")
    (benchmark_repo / "benchmark" / "md_benchmark.py").write_text(
        "class Command:\n"
        "    def main(self, args, standalone_mode=False):\n"
        "        raise RuntimeError('workload failed')\n"
        "run_benchmark = Command()\n",
        encoding="utf-8",
    )
    output_directory = tmp_path / "artifacts"

    with pytest.raises(RuntimeError, match="workload failed"):
        run_local_benchmark(benchmark_repo, output_directory)

    manifest = json.loads(
        (output_directory / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["execution"] == {
        "success": False,
        "error_message": "workload failed",
    }
    assert "workload failed" in (
        output_directory / "logs" / "exception_traceback.log"
    ).read_text(encoding="utf-8")


def test_local_benchmark_removes_stale_artifacts_before_rerun(tmp_path: Path) -> None:
    """Prevent stale output from making an incomplete rerun appear successful.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    benchmark_repo = _benchmark_repo(tmp_path / "benchmark-repo")
    (benchmark_repo / "benchmark" / "md_benchmark.py").write_text(
        "class Command:\n"
        "    def main(self, args, standalone_mode=False):\n"
        "        pass\n"
        "run_benchmark = Command()\n",
        encoding="utf-8",
    )
    output_directory = tmp_path / "artifacts"
    stale_output = output_directory / "output" / "md_benchmark.out"
    stale_exception = output_directory / "logs" / "exception_traceback.log"
    stale_output.parent.mkdir(parents=True)
    stale_exception.parent.mkdir(parents=True)
    stale_output.write_text("stale", encoding="utf-8")
    stale_exception.write_text("stale", encoding="utf-8")

    with pytest.raises(RuntimeError, match="did not produce an output file"):
        run_local_benchmark(benchmark_repo, output_directory)

    assert not stale_output.exists()
    assert stale_exception.read_text(encoding="utf-8") == (
        "Benchmark did not produce an output file."
    )


def test_benchmark_stages_and_uploads_via_fsspec(tmp_path: Path) -> None:
    """Stage a local source filesystem and upload artifacts to a memory filesystem.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    artifacts = fsspec.filesystem("memory")
    benchmark = AWSOpenFEBenchmark(
        "bucket",
        benchmark_script_fs=fsspec.filesystem("file"),
        artifact_output=artifacts,
        benchmark_root=str(_benchmark_repo(tmp_path)),
    )

    benchmark.run_benchmark("bench-task")

    keys = artifacts.find("bucket")
    output_key = next(key for key in keys if key.endswith("/output/md_benchmark.out"))
    manifest_key = next(key for key in keys if key.endswith("/manifest.json"))
    assert json.loads(artifacts.cat(output_key)) == {"system_a": 42.0}
    manifest = json.loads(artifacts.cat(manifest_key))
    assert manifest["execution"]["success"] is True
    assert manifest["s3_bucket"] == "bucket"
    assert manifest["s3_prefix"] in manifest_key


def test_aggregate_child_outputs_sums_numeric_values(tmp_path: Path) -> None:
    """Aggregate MPS output without validating it against the input payload.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    first, second, output = (
        tmp_path / "first.json",
        tmp_path / "second.json",
        tmp_path / "output.json",
    )
    first.write_text(json.dumps({"system_a": {"solvent": "1.5", "complex": 2}}))
    second.write_text(json.dumps({"system_a": {"solvent": 3, "complex": "4.5"}}))

    _aggregate_child_outputs(None, [first, second], output, BenchmarkKind.RBFE)

    assert json.loads(output.read_text()) == {
        "system_a": {"solvent": 4.5, "complex": 6.5}
    }
