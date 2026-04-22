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


def _write_fake_rbfe_script_with_openfe19_style_outputs(benchmark_dir: Path) -> None:
    """Write RBFE script that mimics OpenFE>=1.9 protocol unit output ordering."""
    script = benchmark_dir / "rbfe_benchmark.py"
    script.write_text(
        "import click\nimport json\nimport pathlib\nimport yaml\n\n"
        "def get_performance(dagres, protocol):\n"
        "    protocol_results = protocol.gather([dagres])\n"
        "    nc = [purs[0].outputs['nc'] for purs in protocol_results.data.values()][0]\n"
        "    filepath = nc.resolve().parent\n"
        "    log = filepath / 'simulation_real_time_analysis.yaml'\n"
        "    with open(log) as stream:\n"
        "        data = yaml.safe_load(stream)\n"
        "    return data[-1]['timing_data']['ns_per_day']\n\n"
        "class _UnitResult:\n"
        "    def __init__(self, outputs):\n"
        "        self.outputs = outputs\n\n"
        "class _ProtocolResults:\n"
        "    def __init__(self, nc_path):\n"
        "        self.data = {'repeat_0': [_UnitResult({}), _UnitResult({'nc': nc_path})]}\n\n"
        "class _Protocol:\n"
        "    def __init__(self, nc_path):\n"
        "        self._nc_path = nc_path\n"
        "    def gather(self, _results):\n"
        "        return _ProtocolResults(self._nc_path)\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    perf_dir = pathlib.Path(output_file).resolve().parent / 'perf'\n"
        "    perf_dir.mkdir(parents=True, exist_ok=True)\n"
        "    nc_path = perf_dir / 'simulation.nc'\n"
        "    nc_path.write_text('')\n"
        "    (perf_dir / 'simulation_real_time_analysis.yaml').write_text('- timing_data:\\n    ns_per_day: 12.3\\n', encoding='utf-8')\n"
        "    ns_per_day = get_performance(object(), _Protocol(nc_path))\n"
        "    with open(output_file, 'w') as f:\n"
        "        json.dump({'system_a': ns_per_day}, f)\n\n"
        "if __name__ == '__main__':\n"
        "    run_benchmark()\n"
    )


def _write_fake_rbfe_script_without_nc_output(benchmark_dir: Path) -> None:
    """Write RBFE script with no ``nc`` key but with checkpoint outputs."""
    script = benchmark_dir / "rbfe_benchmark.py"
    script.write_text(
        "import click\nimport json\nimport pathlib\nimport yaml\n\n"
        "def get_performance(dagres, protocol):\n"
        "    protocol_results = protocol.gather([dagres])\n"
        "    nc = [purs[0].outputs['nc'] for purs in protocol_results.data.values()][0]\n"
        "    filepath = nc.resolve().parent\n"
        "    log = filepath / 'simulation_real_time_analysis.yaml'\n"
        "    with open(log) as stream:\n"
        "        data = yaml.safe_load(stream)\n"
        "    return data[-1]['timing_data']['ns_per_day']\n\n"
        "class _UnitResult:\n"
        "    def __init__(self, outputs):\n"
        "        self.outputs = outputs\n\n"
        "class _ProtocolResults:\n"
        "    def __init__(self, checkpoint_path, trajectory_path):\n"
        "        self.data = {'repeat_0': [_UnitResult({'checkpoint': checkpoint_path, 'trajectory': trajectory_path})]}\n\n"
        "class _Protocol:\n"
        "    def __init__(self, checkpoint_path, trajectory_path):\n"
        "        self._checkpoint_path = checkpoint_path\n"
        "        self._trajectory_path = trajectory_path\n"
        "    def gather(self, _results):\n"
        "        return _ProtocolResults(self._checkpoint_path, self._trajectory_path)\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    perf_dir = pathlib.Path(output_file).resolve().parent / 'perf'\n"
        "    perf_dir.mkdir(parents=True, exist_ok=True)\n"
        "    checkpoint_path = perf_dir / 'simulation_checkpoint.nc'\n"
        "    checkpoint_path.write_text('')\n"
        "    trajectory_path = perf_dir / 'simulation.xtc'\n"
        "    trajectory_path.write_text('')\n"
        "    (perf_dir / 'simulation_real_time_analysis.yaml').write_text('- timing_data:\\n    ns_per_day: 34.5\\n', encoding='utf-8')\n"
        "    ns_per_day = get_performance(object(), _Protocol(checkpoint_path, trajectory_path))\n"
        "    with open(output_file, 'w') as f:\n"
        "        json.dump({'system_a': ns_per_day}, f)\n\n"
        "if __name__ == '__main__':\n"
        "    run_benchmark()\n"
    )


def _write_fake_mps_benchmark_script(
    benchmark_dir: Path,
    script_name: str,
    *,
    mismatch_process_index: int | None = None,
) -> None:
    """Write a fake benchmark script with output values keyed by process index."""
    script = benchmark_dir / script_name
    mismatch_index_text = (
        "None" if mismatch_process_index is None else str(mismatch_process_index)
    )
    script.write_text(
        "import click\nimport json\nimport pathlib\n\n"
        f"MISMATCH_PROCESS_INDEX = {mismatch_index_text}\n"
        f"IS_RBFE = {str(script_name == 'rbfe_benchmark.py')}\n\n"
        "def _process_index(output_file):\n"
        "    name = pathlib.Path(output_file).name\n"
        "    if '.process-' not in name:\n"
        "        return 0\n"
        "    suffix = name.split('.process-', 1)[1]\n"
        "    return int(suffix.split('.out', 1)[0])\n\n"
        "@click.command()\n"
        "@click.option('--input_file', required=True)\n"
        "@click.option('--output_file', required=True)\n"
        "def run_benchmark(input_file, output_file):\n"
        "    process_index = _process_index(output_file)\n"
        "    if IS_RBFE:\n"
        "        payload = {\n"
        "            'system_a': {\n"
        "                'solvent': float(process_index + 1),\n"
        "                'complex': float(process_index + 1),\n"
        "            }\n"
        "        }\n"
        "    else:\n"
        "        payload = {'system_a': float(process_index + 1)}\n"
        "    if MISMATCH_PROCESS_INDEX is not None and process_index == MISMATCH_PROCESS_INDEX:\n"
        "        payload = {'unexpected_system': float(process_index + 1)}\n"
        "    print(f'child process {process_index}')\n"
        "    with open(output_file, 'w') as f:\n"
        "        json.dump(payload, f)\n\n"
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
        run_benchmark(
            repo,
            s3_bucket="my-bucket",
            task_id=(
                "bench:md:us-east-1:g5.xlarge:ami-1234:"
                "123e4567-e89b-12d3-a456-426614174000"
            ),
        )


def test_run_benchmark_raises_when_input_json_missing(tmp_path):
    repo = tmp_path / "performance_benchmarks"
    (repo / "benchmark").mkdir(parents=True)
    (repo / "data").mkdir()
    # input JSON not written

    with pytest.raises(FileNotFoundError, match="Benchmark input file not found"):
        run_benchmark(
            repo,
            s3_bucket="my-bucket",
            task_id=(
                "bench:md:us-east-1:g5.xlarge:ami-1234:"
                "123e4567-e89b-12d3-a456-426614174000"
            ),
        )


def test_run_benchmark_rejects_legacy_untyped_bench_task_id(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_benchmark_script(benchmark_dir, "md_benchmark.py")

    with pytest.raises(RuntimeError, match="Invalid bench task ID format"):
        run_benchmark(
            repo,
            s3_bucket="bucket",
            task_id=(
                "bench:us-east-1:g5.xlarge:ami-1234:"
                "123e4567-e89b-12d3-a456-426614174000"
            ),
        )

    for mod in ("md_benchmark",):
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
            run_benchmark(
                repo,
                s3_bucket="bucket",
                task_id=(
                    "bench:md:us-east-1:g5.xlarge:ami-1234:"
                    "123e4567-e89b-12d3-a456-426614174000"
                ),
            )

    for mod in ("rbfe_benchmark", "md_benchmark"):
        sys.modules.pop(mod, None)


def test_run_benchmark_uses_dated_hashed_prefix_for_cloud_init_task_id(tmp_path, capfd):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_benchmark_script(benchmark_dir, "md_benchmark.py")

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:md:us-east-1:g5.xlarge:ami-1234:"
        "IyEvYmluL2Jhc2gKZWNobyAiaGVsbG8iCg==:"
        "123e4567-e89b-12d3-a456-426614174000"
    )
    expected_hash = hashlib.sha256(task_id.encode("utf-8")).hexdigest()

    with patch(
        "benchmarking_orchestration.bench.boto3.client",
        return_value=_FakeS3Client(),
    ):
        run_benchmark(repo, s3_bucket="bucket", task_id=task_id)

    stdout, _ = capfd.readouterr()
    assert stdout == "MetadataService doesn't exist\n"

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

    assert manifest["schema_version"] == 4
    assert "bench_task_id" not in manifest
    assert "launch_task_id" not in manifest
    assert manifest["s3_prefix"] == expected_prefix
    assert manifest["benchmark_kind"] == "md"
    assert manifest["mps_process_count"] == 1
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

    assert manifest["schema_version"] == 4
    assert manifest["benchmark_kind"] == "rbfe"
    assert manifest["mps_process_count"] == 1
    assert manifest["execution"]["success"] is True
    assert manifest["output"]["source_name"] == "rbfe_benchmark.out"
    assert manifest["output"]["json_parse_ok"] is True

    for mod in ("rbfe_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_rbfe_compat_handles_openfe19_style_protocol_units(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_rbfe_script_with_openfe19_style_outputs(benchmark_dir)

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

    output_key = next(
        key for key in uploaded_by_key if key.endswith("/output/rbfe_benchmark.out")
    )
    output_payload = json.loads(uploaded_by_key[output_key])

    assert output_payload["system_a"] == 12.3

    for mod in ("rbfe_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_rbfe_compat_handles_outputs_without_nc_key(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_rbfe_script_without_nc_output(benchmark_dir)

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

    output_key = next(
        key for key in uploaded_by_key if key.endswith("/output/rbfe_benchmark.out")
    )
    output_payload = json.loads(uploaded_by_key[output_key])

    assert output_payload["system_a"] == 34.5

    for mod in ("rbfe_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_aggregates_mps_outputs_for_md(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_mps_benchmark_script(benchmark_dir, "md_benchmark.py")

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:md:mps:3:us-east-1:g5.xlarge:ami-1234:"
        "123e4567-e89b-12d3-a456-426614174000"
    )

    with patch(
        "benchmarking_orchestration.bench.boto3.client",
        return_value=_FakeS3Client(),
    ):
        run_benchmark(
            repo,
            s3_bucket="bucket",
            task_id=task_id,
            benchmark_kind=BenchmarkKind.MD,
            mps_process_count=3,
        )

    manifest_key = next(
        key for key in uploaded_by_key if key.endswith("/manifest.json")
    )
    output_key = next(
        key for key in uploaded_by_key if key.endswith("/output/md_benchmark.out")
    )
    manifest = json.loads(uploaded_by_key[manifest_key])
    output_payload = json.loads(uploaded_by_key[output_key])

    assert manifest["schema_version"] == 4
    assert manifest["benchmark_kind"] == "md"
    assert manifest["mps_process_count"] == 3
    assert manifest["execution"]["success"] is True
    assert manifest["output"]["s3_key"] == output_key
    assert output_payload == {"system_a": 6.0}
    assert any(
        "/output/children/md_benchmark.process-0.out" in key for key in uploaded_by_key
    )
    assert any(
        "/output/children/md_benchmark.process-1.out" in key for key in uploaded_by_key
    )
    assert any(
        "/output/children/md_benchmark.process-2.out" in key for key in uploaded_by_key
    )
    assert any("/logs/children/stdout.process-0.log" in key for key in uploaded_by_key)

    for mod in ("md_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_aggregates_mps_outputs_for_rbfe(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_mps_benchmark_script(benchmark_dir, "rbfe_benchmark.py")

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:rbfe:mps:2:us-east-1:g5.xlarge:ami-1234:"
        "123e4567-e89b-12d3-a456-426614174000"
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
            mps_process_count=2,
        )

    output_key = next(
        key for key in uploaded_by_key if key.endswith("/output/rbfe_benchmark.out")
    )
    manifest_key = next(
        key for key in uploaded_by_key if key.endswith("/manifest.json")
    )
    output_payload = json.loads(uploaded_by_key[output_key])
    manifest = json.loads(uploaded_by_key[manifest_key])

    assert output_payload == {
        "system_a": {"solvent": 3.0, "complex": 3.0}
    }
    assert manifest["benchmark_kind"] == "rbfe"
    assert manifest["mps_process_count"] == 2
    assert manifest["execution"]["success"] is True

    for mod in ("rbfe_benchmark",):
        sys.modules.pop(mod, None)


def test_run_benchmark_mps_aggregation_fails_when_child_keys_mismatch(tmp_path):
    repo = _make_benchmark_repo(tmp_path)
    benchmark_dir = repo / "benchmark"
    _write_fake_mps_benchmark_script(
        benchmark_dir,
        "md_benchmark.py",
        mismatch_process_index=1,
    )

    uploaded_by_key: dict[str, str] = {}

    class _FakeS3Client:
        def upload_file(self, filename, bucket, key):
            assert bucket == "bucket"
            uploaded_by_key[key] = Path(filename).read_text(encoding="utf-8")

    task_id = (
        "bench:md:mps:2:us-east-1:g5.xlarge:ami-1234:"
        "123e4567-e89b-12d3-a456-426614174000"
    )

    with patch(
        "benchmarking_orchestration.bench.boto3.client",
        return_value=_FakeS3Client(),
    ):
        with pytest.raises(RuntimeError, match="invalid"):
            run_benchmark(
                repo,
                s3_bucket="bucket",
                task_id=task_id,
                benchmark_kind=BenchmarkKind.MD,
                mps_process_count=2,
            )

    manifest_key = next(
        key for key in uploaded_by_key if key.endswith("/manifest.json")
    )
    manifest = json.loads(uploaded_by_key[manifest_key])

    assert manifest["mps_process_count"] == 2
    assert manifest["execution"]["success"] is False
    assert manifest["output"]["s3_key"] is None
    assert any(
        "/output/children/md_benchmark.process-0.out" in key for key in uploaded_by_key
    )
    assert any(
        "/output/children/md_benchmark.process-1.out" in key for key in uploaded_by_key
    )

    for mod in ("md_benchmark",):
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
