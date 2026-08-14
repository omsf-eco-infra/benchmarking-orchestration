#!/usr/bin/env bash
set -euo pipefail

BASE_PATH="$HOME/workspace"
CACHE_PATH="$BASE_PATH/pixi-cache"
CLI_PATH="$BASE_PATH/benchmarking-orchestration"
BENCH_REPO_PATH="$BASE_PATH/performance_benchmarks"

mkdir -p "$CACHE_PATH" "$BASE_PATH/jobs"
curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=0.64.0 bash
export PATH="$HOME/.pixi/bin:$PATH"
export PIXI_CACHE_DIR="$CACHE_PATH"

git clone -b feat/brev --single-branch \
  https://github.com/omsf-eco-infra/benchmarking-orchestration.git "$CLI_PATH"
git clone -b industry_benchmarks --single-branch \
  https://github.com/OpenFreeEnergy/performance_benchmarks.git "$BENCH_REPO_PATH"
pixi install --manifest-path "$CLI_PATH/pyproject.toml" -e bench
if command -v nvidia-cuda-mps-control >/dev/null 2>&1; then
  nvidia-cuda-mps-control -d
fi
touch "$BASE_PATH/startup-complete"
