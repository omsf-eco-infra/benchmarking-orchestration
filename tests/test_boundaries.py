"""Regression tests for package boundaries and lazy imports.

These run each import in a subprocess so that modules imported by the main test
session cannot mask a boundary violation.
"""

from __future__ import annotations

import ast
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
from cyclopts import App

from benchmarking_orchestration.commands import create_app


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
    """Import every core module without loading provider or app dependencies."""
    code = (
        "import importlib, pkgutil, sys\n"
        "import benchmarking_orchestration_core as core\n"
        "assert set(core.__all__) == {'AbstractBenchmark', 'BenchmarkKind'}\n"
        "for module in pkgutil.walk_packages(core.__path__, core.__name__ + '.'):\n"
        "    importlib.import_module(module.name)\n"
        "bad = {'benchmarking_aws', 'benchmarking_orchestration', 'boto3', "
        "'fsspec', 's3fs', 'cyclopts', 'exorcist', 'sqlalchemy'}\n"
        "overlap = bad & set(sys.modules)\n"
        "assert not overlap, sorted(overlap)\n"
    )

    result = _run_isolated(code)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("package", ["benchmarking_orchestration", "benchmarking_aws"])
def test_package_import_does_not_pull_provider_dependencies(package):
    """Keep top-level imports independent of the AWS SDK."""
    code = (
        "import sys\n"
        f"import {package}\n"
        "overlap = {'boto3', 'botocore'} & set(sys.modules)\n"
        "assert not overlap, sorted(overlap)\n"
    )

    result = _run_isolated(code)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("package", "dependencies"),
    [
        ("benchmarking_orchestration_core", set()),
        (
            "benchmarking_aws",
            {"benchmarking_orchestration_core", "boto3", "botocore", "requests"},
        ),
    ],
)
def test_library_imports_follow_package_direction(package, dependencies):
    """Reject app imports and undeclared libraries, even in lazy code paths."""
    spec = importlib.util.find_spec(package)
    allowed = sys.stdlib_module_names | dependencies | {package}
    for directory in spec.submodule_search_locations:
        for path in Path(directory).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Import):
                    imports = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0:
                    imports = [node.module]
                else:
                    continue
                assert all(name.split(".")[0] in allowed for name in imports), (
                    path,
                    imports,
                )


@pytest.mark.parametrize("module", ["task_id", "capabilities", "cloud_init"])
def test_aws_semantic_modules_are_not_in_core(module):
    """Keep EC2 task formats, capabilities, and bootstrap helpers out of core."""
    assert importlib.util.find_spec(f"benchmarking_orchestration_core.{module}") is None
    assert importlib.util.find_spec(f"benchmarking_aws.{module}") is not None


def test_aws_primitives_do_not_import_the_application():
    """Use all AWS library modules without the CLI, task DB, or runner."""
    code = (
        "import importlib, pkgutil, sys\n"
        "import benchmarking_aws as aws\n"
        "for module in pkgutil.walk_packages(aws.__path__, aws.__name__ + '.'):\n"
        "    importlib.import_module(module.name)\n"
        "bad = {'benchmarking_orchestration', 'cyclopts', 'exorcist', 'sqlalchemy', 'fsspec', 's3fs'}\n"
        "overlap = bad & set(sys.modules)\n"
        "assert not overlap, sorted(overlap)\n"
    )
    result = _run_isolated(code)
    assert result.returncode == 0, result.stderr


def test_create_app_registers_injected_providers():
    """Register injected adapters alongside the built-in AWS adapter."""
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
