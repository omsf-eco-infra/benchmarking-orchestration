from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest

import benchmarking_orchestration.job_worker as job_worker_module
from benchmarking_orchestration.job_worker import (
    load_job_specification,
    run_job_worker,
)


def _job_directory(tmp_path: Path) -> Path:
    """Create a valid local benchmark job directory.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.

    Returns
    -------
    Path
        Job directory containing a specification and benchmark repository.
    """
    job_directory = tmp_path / "job"
    benchmark_repo = job_directory / "benchmark-repo"
    (benchmark_repo / "benchmark").mkdir(parents=True)
    (benchmark_repo / "data").mkdir()
    (benchmark_repo / "data" / "ross_dodecahedron_jacs.json").write_text(
        "{}", encoding="utf-8"
    )
    (benchmark_repo / "benchmark" / "md_benchmark.py").write_text(
        "import json\n"
        "class Command:\n"
        "    def main(self, args, standalone_mode=False):\n"
        "        with open(args[3], 'w') as output:\n"
        "            json.dump({'system_a': 42.0}, output)\n"
        "run_benchmark = Command()\n",
        encoding="utf-8",
    )
    _write_job(job_directory)
    return job_directory


def _write_job(job_directory: Path, **overrides: object) -> None:
    """Write a job specification with optional field overrides.

    Parameters
    ----------
    job_directory : Path
        Job directory receiving ``job.json``.
    **overrides : object
        Job fields to replace.
    """
    payload = {
        "schema_version": 1,
        "job_id": "job-123",
        "benchmark_kind": "md",
        "mps_process_count": 1,
        "profile": "openfe-gpu",
        "benchmark_repo_path": "benchmark-repo",
        "output_directory": "results",
    }
    payload.update(overrides)
    job_directory.mkdir(parents=True, exist_ok=True)
    (job_directory / "job.json").write_text(json.dumps(payload), encoding="utf-8")


def test_job_worker_writes_heartbeats_results_and_complete_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run a local job with periodic status and a final completion marker.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    job_directory = _job_directory(tmp_path)
    real_run_local_benchmark = job_worker_module.run_local_benchmark
    real_replace = job_worker_module.os.replace
    replaced_names: list[str] = []

    def _slow_benchmark(*args: object) -> None:
        """Delay execution long enough for periodic heartbeats."""
        time.sleep(0.04)
        real_run_local_benchmark(*args)

    def _record_replace(source: Path, destination: Path) -> None:
        """Record atomic marker replacement order."""
        replaced_names.append(Path(destination).name)
        real_replace(source, destination)

    monkeypatch.setattr(job_worker_module, "run_local_benchmark", _slow_benchmark)
    monkeypatch.setattr(job_worker_module.os, "replace", _record_replace)
    monkeypatch.setattr(
        job_worker_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, "NVIDIA A100, GPU-123, 570.1\n", ""
        ),
    )

    run_job_worker(job_directory, heartbeat_interval_seconds=0.01)

    status = json.loads((job_directory / "status.json").read_text(encoding="utf-8"))
    complete = json.loads((job_directory / "complete.json").read_text(encoding="utf-8"))
    output = json.loads(
        (job_directory / "results" / "output" / "md_benchmark.out").read_text(
            encoding="utf-8"
        )
    )
    assert status["state"] == "succeeded"
    assert complete["success"] is True
    assert complete["profile"] == "openfe-gpu"
    assert complete["gpu_provenance"]["gpus"] == [
        {"name": "NVIDIA A100", "uuid": "GPU-123", "driver_version": "570.1"}
    ]
    assert output == {"system_a": 42.0}
    assert replaced_names.count("status.json") >= 3
    assert replaced_names[-1] == "complete.json"


def test_job_worker_records_failure_and_complete_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist failed completion before propagating a benchmark error.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    job_directory = _job_directory(tmp_path)
    real_replace = job_worker_module.os.replace
    replaced_names: list[str] = []

    def _fail_benchmark(*_args: object) -> None:
        """Raise a representative benchmark failure."""
        raise RuntimeError("workload failed")

    def _record_replace(source: Path, destination: Path) -> None:
        """Record atomic marker replacement order."""
        replaced_names.append(Path(destination).name)
        real_replace(source, destination)

    monkeypatch.setattr(job_worker_module, "run_local_benchmark", _fail_benchmark)
    monkeypatch.setattr(job_worker_module, "_gpu_provenance", lambda: {})
    monkeypatch.setattr(job_worker_module.os, "replace", _record_replace)

    with pytest.raises(RuntimeError, match="workload failed"):
        run_job_worker(job_directory)

    status = json.loads((job_directory / "status.json").read_text(encoding="utf-8"))
    complete = json.loads((job_directory / "complete.json").read_text(encoding="utf-8"))
    assert status["state"] == "failed"
    assert complete["success"] is False
    assert complete["error_message"] == "workload failed"
    assert replaced_names[-1] == "complete.json"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"job_id": "bad/job"}, "job_id must be"),
        ({"profile": ""}, "profile must be"),
        ({"benchmark_kind": "both"}, "one workload"),
        ({"benchmark_kind": "unknown"}, "Unsupported benchmark kind"),
        ({"mps_process_count": True}, "must be an integer"),
        ({"mps_process_count": 0}, "must be an integer"),
        ({"benchmark_repo_path": "../repo"}, "must stay within"),
        ({"output_directory": "/tmp/results"}, "must be relative"),
        ({"output_directory": "benchmark-repo/results"}, "must not overlap"),
    ],
)
def test_job_specification_rejects_invalid_values(
    tmp_path: Path, overrides: dict[str, object], message: str
) -> None:
    """Reject invalid identifiers, workload values, and paths.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    overrides : dict[str, object]
        Invalid job field override.
    message : str
        Expected validation error fragment.
    """
    job_directory = _job_directory(tmp_path)
    _write_job(job_directory, **overrides)

    with pytest.raises(ValueError, match=message):
        load_job_specification(job_directory)


def test_job_worker_records_unavailable_nvidia_smi(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Complete a CPU-visible job when GPU provenance is unavailable.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory.
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    job_directory = _job_directory(tmp_path)

    def _missing_nvidia_smi(*_args: object, **_kwargs: object) -> None:
        """Represent a runtime without ``nvidia-smi``."""
        raise FileNotFoundError("nvidia-smi")

    monkeypatch.setattr(job_worker_module.subprocess, "run", _missing_nvidia_smi)

    run_job_worker(job_directory)

    complete = json.loads((job_directory / "complete.json").read_text(encoding="utf-8"))
    assert complete["success"] is True
    assert complete["gpu_provenance"]["available"] is False
    assert "nvidia-smi" in complete["gpu_provenance"]["error"]
