"""Credentialless Brev startup integration contract."""

from pathlib import Path


def test_brev_startup_is_credentialless_and_workspace_rooted() -> None:
    """Keep controller credentials and job execution out of Brev startup."""
    script = Path("brev_startup.sh").read_text(encoding="utf-8")

    assert 'BASE_PATH="$HOME/workspace"' in script
    assert 'mkdir -p "$CACHE_PATH" "$BASE_PATH/jobs"' in script
    assert "nvidia-cuda-mps-control -d" in script
    assert 'touch "$BASE_PATH/startup-complete"' in script
    assert (
        "git clone -b feat/brev-monorepo --single-branch \\\n"
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
