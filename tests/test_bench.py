from __future__ import annotations

import json
from pathlib import Path

import fsspec
import pytest

from benchmarking_orchestration.bench import (
    AWSOpenFEBenchmark,
    _aggregate_child_outputs,
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
    (tmp_path / "benchmark").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "ross_dodecahedron_jacs.json").write_text("{}")
    (tmp_path / "benchmark" / "md_benchmark.py").write_text(
        "import json, sys\n"
        "class Command:\n"
        "    def main(self, args, standalone_mode=False):\n"
        "        print('child stdout')\n"
        "        print('child stderr', file=sys.stderr)\n"
        "        with open(args[3], 'w') as output:\n"
        "            json.dump({'system_a': 42.0}, output)\n"
        "run_benchmark = Command()\n"
    )
    return tmp_path


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
    manifest = json.loads(artifacts.cat(manifest_key))
    assert json.loads(artifacts.cat(output_key)) == {"system_a": 42.0}
    assert manifest["schema_version"] == 5
    assert manifest["execution"] == {"success": True, "error_message": None}


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


def test_mps_logs_are_combined_and_uploaded(tmp_path: Path) -> None:
    """Upload labeled stdout and stderr from every MPS child.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    """
    artifacts = fsspec.filesystem("memory")
    benchmark = AWSOpenFEBenchmark(
        "mps-logs",
        mps_process_count=2,
        benchmark_script_fs=fsspec.filesystem("file"),
        artifact_output=artifacts,
        benchmark_root=str(_benchmark_repo(tmp_path)),
    )

    benchmark.run_benchmark("mps-log-task")

    keys = artifacts.find("mps-logs")
    stdout = artifacts.cat(next(key for key in keys if key.endswith("/stdout.log")))
    stderr = artifacts.cat(next(key for key in keys if key.endswith("/stderr.log")))
    assert stdout.count(b"child stdout") == 2
    assert stderr.count(b"child stderr") == 2
    assert b"stdout.process-0.log" in stdout
    assert b"stdout.process-1.log" in stdout


def test_mps_aggregation_failure_uploads_manifest_and_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist MPS diagnostics before raising an aggregation failure.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """

    def _fail_aggregation(*_args) -> None:
        """Raise a representative child-output parsing error."""
        raise RuntimeError("malformed child output")

    monkeypatch.setattr(
        "benchmarking_orchestration.bench._aggregate_child_outputs",
        _fail_aggregation,
    )
    artifacts = fsspec.filesystem("memory")
    benchmark = AWSOpenFEBenchmark(
        "mps-failure",
        mps_process_count=2,
        benchmark_script_fs=fsspec.filesystem("file"),
        artifact_output=artifacts,
        benchmark_root=str(_benchmark_repo(tmp_path)),
    )

    with pytest.raises(RuntimeError, match="malformed child output"):
        benchmark.run_benchmark("mps-failure-task")

    keys = artifacts.find("mps-failure")
    manifest = json.loads(
        artifacts.cat(next(key for key in keys if key.endswith("/manifest.json")))
    )
    traceback_text = artifacts.cat(
        next(key for key in keys if key.endswith("/exception_traceback.log"))
    )
    assert manifest["execution"] == {
        "success": False,
        "error_message": "malformed child output",
    }
    assert b"RuntimeError: malformed child output" in traceback_text
