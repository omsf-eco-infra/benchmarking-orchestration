"""Regression tests for package boundaries and lazy imports.

These run each import in a subprocess so that modules imported by the main test
session cannot mask a boundary violation.
"""

from __future__ import annotations

import subprocess
import sys

from cyclopts import App

from benchmarking_orchestration.commands import create_app

_HEAVY_MODULES = {"boto3", "fsspec", "s3fs", "cyclopts", "exorcist", "sqlalchemy"}


def _run_isolated(code: str) -> subprocess.CompletedProcess[str]:
    """Execute Python source in a fresh interpreter.

    Parameters
    ----------
    code : str
        Python source to execute.

    Returns
    -------
    subprocess.CompletedProcess[str]
        Completed process with captured output.
    """
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )


def test_core_imports_without_provider_cli_or_db_dependencies():
    code = (
        "import sys\n"
        "from benchmarking_orchestration_core import (\n"
        "    AbstractBenchmark,\n"
        "    BenchmarkKind,\n"
        "    WorkerCapability,\n"
        ")\n"
        "bad = {'boto3', 'fsspec', 's3fs', 'cyclopts', 'exorcist', 'sqlalchemy'}\n"
        "overlap = bad & set(sys.modules)\n"
        "assert not overlap, sorted(overlap)\n"
    )

    result = _run_isolated(code)

    assert result.returncode == 0, result.stderr


def test_package_import_does_not_pull_provider_dependencies():
    code = (
        "import sys\n"
        "import benchmarking_orchestration\n"
        "overlap = {'boto3', 'botocore'} & set(sys.modules)\n"
        "assert not overlap, sorted(overlap)\n"
    )

    result = _run_isolated(code)

    assert result.returncode == 0, result.stderr


def test_create_app_registers_injected_providers():
    seen: list[tuple[App, App, App]] = []

    class _FakeProvider:
        provider_name = "fake"

        def register_cli(
            self,
            create_app: App,
            launch_app: App,
            worker_app: App,
        ) -> None:
            seen.append((create_app, launch_app, worker_app))

    app = create_app(providers=[_FakeProvider()])

    assert len(seen) == 1
    assert all(isinstance(group, App) for group in seen[0])
    # The built-in AWS provider is still registered alongside injected providers.
    assert "aws" in list(app["create"])
    assert "aws" in list(app["launch"])
    assert "aws" in list(app["worker"])
