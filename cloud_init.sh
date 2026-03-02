#!/usr/bin/env bash
set -euo pipefail

apt-get install -y git

TURSO_DATABASE_URL="@TURSO_DATABASE_URL"
TURSO_AUTH_TOKEN="@TURSO_AUTH_TOKEN"
GPU_CAPABILITY="${gpu:-${GPU_CAPABILITY:-}}"

: "${TURSO_DATABASE_URL:?TURSO_DATABASE_URL is required}"
: "${TURSO_AUTH_TOKEN:?TURSO_AUTH_TOKEN is required}"
: "${GPU_CAPABILITY:?GPU capability is required}"

sudo -u ubuntu -i bash <<EOF
set -euo pipefail

curl -fsSL https://pixi.sh/install.sh | sh
export PATH="\$HOME/.pixi/bin:\$PATH"

git clone https://github.com/omsf-eco-infra/benchmarking-orchestration.git
CLI_PATH="\$HOME/benchmarking-orchestration"
pixi install --manifest-path "\$CLI_PATH/pyproject.toml" -e bench

export TURSO_DATABASE_URL="${TURSO_DATABASE_URL}"
export TURSO_AUTH_TOKEN="${TURSO_AUTH_TOKEN}"

pixi run --manifest-path "\$CLI_PATH/pyproject.toml" -e bench python -m benchmarking_orchestration worker --capability "${GPU_CAPABILITY}"
EOF
