#!/usr/bin/env bash
set -euo pipefail

TURSO_DATABASE_URL="@TURSO_DATABASE_URL"
TURSO_AUTH_TOKEN="@TURSO_AUTH_TOKEN"
GPU_CAPABILITY="@GPU_CAPABILITY"
S3_BUCKET="@S3_BUCKET"

: "${TURSO_DATABASE_URL:?TURSO_DATABASE_URL is required}"
: "${TURSO_AUTH_TOKEN:?TURSO_AUTH_TOKEN is required}"
: "${GPU_CAPABILITY:?GPU_CAPABILITY is required}"
: "${S3_BUCKET:?S3_BUCKET is required}"

trap 'shutdown -h now' EXIT

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git nvtop

sudo -u ubuntu -i bash <<EOF
set -euo pipefail
BASE_PATH="/opt/dlami/nvme"
CACHE_PATH="\$BASE_PATH/pixi-cache"
CLI_PATH="\$BASE_PATH/benchmarking-orchestration"
BENCH_REPO_PATH="\$BASE_PATH/performance_benchmarks"
mkdir "\$CACHE_PATH"

curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=0.64.0 bash
export PATH="\$HOME/.pixi/bin:\$PATH"
export PIXI_CACHE_DIR="\$CACHE_PATH"


git clone https://github.com/omsf-eco-infra/benchmarking-orchestration.git "\$CLI_PATH"
git clone -b industry_benchmarks --single-branch https://github.com/OpenFreeEnergy/performance_benchmarks.git "\$BENCH_REPO_PATH"
pixi install --manifest-path "\$CLI_PATH/pyproject.toml" -e bench

export TURSO_DATABASE_URL="${TURSO_DATABASE_URL}"
export TURSO_AUTH_TOKEN="${TURSO_AUTH_TOKEN}"
export S3_BUCKET="${S3_BUCKET}"

pixi run --manifest-path "\$CLI_PATH/pyproject.toml" -e bench python -m benchmarking_orchestration worker aws --capability "${GPU_CAPABILITY}" --bench-repo-path "\$BENCH_REPO_PATH"
EOF
