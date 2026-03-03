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

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git

sudo -u ubuntu -i bash <<EOF
set -euo pipefail

curl -fsSL https://pixi.sh/install.sh | bash
export PATH="\$HOME/.pixi/bin:\$PATH"

git clone https://github.com/omsf-eco-infra/benchmarking-orchestration.git
git clone -b industry_benchmarks --single-branch https://github.com/OpenFreeEnergy/performance_benchmarks.git
CLI_PATH="\$HOME/benchmarking-orchestration"
pixi install --manifest-path "\$CLI_PATH/pyproject.toml" -e bench

export TURSO_DATABASE_URL="${TURSO_DATABASE_URL}"
export TURSO_AUTH_TOKEN="${TURSO_AUTH_TOKEN}"
export S3_BUCKET="${S3_BUCKET}"

pixi run --manifest-path "\$CLI_PATH/pyproject.toml" -e bench python -m benchmarking_orchestration worker --capability "${GPU_CAPABILITY}"
EOF
