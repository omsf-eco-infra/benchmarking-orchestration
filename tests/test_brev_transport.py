from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from benchmarking_orchestration.brev import BrevTransport


def test_transport_runs_required_brev_argument_lists(monkeypatch) -> None:
    """Run each required operation without shell command construction.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    calls: list[tuple[list[str], dict[str, object]]] = []
    outputs = iter(
        (
            "brev-job\n",
            "copied\n",
            "remote outputbrev-job\n",
            '{"workspaces": [{"name": "brev-job", "status": "RUNNING"}]}',
            "brev-job\n",
        )
    )

    def _run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        """Record a mocked subprocess invocation.

        Parameters
        ----------
        command : list[str]
            Command argument list.
        **kwargs : object
            Subprocess options.

        Returns
        -------
        subprocess.CompletedProcess[str]
            Successful mocked command result.
        """
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout=next(outputs), stderr="")

    monkeypatch.setattr(subprocess, "run", _run)
    transport = BrevTransport()

    assert (
        transport.create("brev-job", "g5.xlarge", Path("brev_startup.sh")) == "brev-job"
    )
    assert transport.copy(Path("job"), "brev-job:/home/ubuntu/workspace/job") == (
        "copied"
    )
    assert transport.exec("brev-job", "nvidia-smi") == "remote output"
    assert transport.inspect("brev-job") == {
        "name": "brev-job",
        "status": "RUNNING",
    }
    assert transport.delete("brev-job") == "brev-job"

    assert [command for command, _kwargs in calls] == [
        [
            "brev",
            "create",
            "brev-job",
            "--type",
            "g5.xlarge",
            "--startup-script",
            "@brev_startup.sh",
        ],
        ["brev", "copy", "job", "brev-job:/home/ubuntu/workspace/job"],
        ["brev", "exec", "brev-job", "nvidia-smi"],
        ["brev", "ls", "--json"],
        ["brev", "delete", "brev-job"],
    ]
    assert all(
        kwargs == {"capture_output": True, "check": True, "text": True}
        for _command, kwargs in calls
    )


def test_inspect_returns_none_for_missing_instance(monkeypatch) -> None:
    """Return no metadata when the named Brev instance is absent.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(
            command, 0, stdout='{"workspaces": null}', stderr=""
        ),
    )

    assert BrevTransport().inspect("missing") is None


def test_transport_reports_brev_error_output(monkeypatch) -> None:
    """Include Brev's standard error in command failures.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **_kwargs: (_ for _ in ()).throw(
            subprocess.CalledProcessError(
                1, command, output="", stderr="No capacity for this instance type.\n"
            )
        ),
    )

    with pytest.raises(RuntimeError, match="No capacity for this instance type"):
        BrevTransport().create("brev-job", "g5.xlarge", Path("brev_startup.sh"))


def test_brev_startup_is_credentialless_and_workspace_rooted() -> None:
    """Keep controller credentials and job execution out of Brev startup."""
    script = Path("brev_startup.sh").read_text(encoding="utf-8")

    assert 'BASE_PATH="$HOME/workspace"' in script
    assert 'mkdir -p "$CACHE_PATH" "$BASE_PATH/jobs"' in script
    assert "nvidia-cuda-mps-control -d" in script
    assert 'touch "$BASE_PATH/startup-complete"' in script
    assert (
        "git clone -b feat/brev --single-branch \\\n"
        '  https://github.com/omsf-eco-infra/benchmarking-orchestration.git "$CLI_PATH"'
        in script
    )
    assert (
        "git clone -b industry_benchmarks --single-branch \\\n"
        '  https://github.com/OpenFreeEnergy/performance_benchmarks.git "$BENCH_REPO_PATH"'
        in script
    )
    assert "TURSO_" not in script
    assert "S3_BUCKET" not in script
    assert "AWS_" not in script
    assert "python -m benchmarking_orchestration worker" not in script
