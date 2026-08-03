# Benchmarking Orchestration

Provider-oriented CLI for queuing launch work, dispatching benchmark workers, and storing task state in an `exorcist` task database.

The repository currently supports one provider flow:

- **AWS EC2**: validate an instance type and AMI, queue a launch task plus one or more dependent benchmark tasks, launch the instance, then run MD/RBFE benchmarks and upload artifacts to S3.

This repo is no longer just a small AWS quota helper; the main CLI is now:

```bash
pixi run python -m benchmarking_orchestration
```

## Current CLI shape

```text
create aws   ...
launch aws   ...
worker aws   ...
```

The top-level groups are:

- `create`: queue launch + benchmark tasks
- `launch`: process queued launch tasks
- `worker`: process benchmark tasks for a specific capability

## What the repo does today

### AWS flow

- Validates EC2 instance types in the `g`, `vt`, and `p` families.
- Validates the launch AMI in the selected region.
- Queues one launch task and one dependent benchmark task per requested benchmark kind.
- Supports `md`, `rbfe`, or `both` benchmark kinds.
- Stores optional rendered cloud-init user-data in the launch task ID.
- Launches one EC2 instance per launch task.
- Runs benchmark tasks by worker capability (`g3`, `g4-dn`, `g5`, `g6`, `g6-e`, `p`, `vt1`).
- Uploads benchmark inputs, outputs, logs, and a manifest to S3.

### Result handling

Benchmark workers upload artifacts to:

```text
runs/<yyyy-mm-dd>/<sha256(task_id)>/
```

Each run includes:

- the benchmark input JSON
- the benchmark output JSON
- `stdout.log`
- `stderr.log`
- `exception_traceback.log` when execution fails
- `manifest.json`

## Requirements

- `pixi`
- Python 3.11+ at the package level
- AWS credentials when using the AWS provider

The pinned Pixi environment currently uses Python 3.13.

## Install

For normal CLI use:

```bash
pixi install
```

If you want to run benchmark workloads locally, install the bench environment too:

```bash
pixi install -e bench
```

## External dependency: benchmark repo

Benchmark execution expects a checked-out copy of the OpenFE benchmark repo and imports benchmark scripts directly from it.

Expected layout:

- benchmark scripts under `benchmark/`
- benchmark input JSON under `data/`

The repo is typically cloned from:

```bash
git clone -b industry_benchmarks --single-branch \
  https://github.com/OpenFreeEnergy/performance_benchmarks.git
```

By default, workers look for it at:

```text
/opt/dlami/nvme/performance_benchmarks
```

Override that with `--bench-repo-path` or `BENCHMARK_REPO_PATH`.

## Task database

Task state is stored in `benchmarking_orchestration.tasks.TaskStatusDB`, which extends `exorcist.TaskStatusDB` with per-task capabilities.

Database selection works like this:

- if `--db` is provided, use that local SQLite file
- otherwise, if `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are set, use Turso
- otherwise, fall back to local `task_status.db`

## Common environment variables

### Shared / task-db

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- `BENCHMARK_REPO_PATH`

### AWS

- `AWS_PROFILE` or the usual AWS credential chain
- `AWS_BENCHMARK_AMI_ID` as the approved AMI for `create aws`
- `EC2_KEY_NAME` for optional SSH/debug access on launched instances
- `EC2_IAM_INSTANCE_PROFILE` for the launched EC2 instance
- `BENCHMARK_S3_BUCKET` as a default for `create aws --s3-bucket`
- `S3_BUCKET` at benchmark worker runtime

## Quick start: AWS

Queue AWS launch + benchmark tasks:

```bash
export AWS_BENCHMARK_AMI_ID=ami-0123456789abcdef0

pixi run python -m benchmarking_orchestration create aws g5.xlarge \
  --region us-east-1 \
  --cloud-init-file cloud_init.sh \
  --benchmark-kind both \
  --s3-bucket my-benchmark-results
```

The CLI prints the resolved AMI name and AMI ID, then asks for confirmation before queueing. For automation, pass `--yes` after you have verified the configured AMI.

Process one queued launch task:

```bash
pixi run python -m benchmarking_orchestration launch aws
```

When `AWS_BENCHMARK_AMI_ID` is set, queued launch tasks must match that approved AMI or the launch is rejected.

Run a benchmark worker locally for a capability:

```bash
S3_BUCKET=my-benchmark-results \
pixi run -e bench python -m benchmarking_orchestration worker aws g5 \
  --bench-repo-path /path/to/performance_benchmarks
```

Show help:

```bash
pixi run python -m benchmarking_orchestration --help
pixi run python -m benchmarking_orchestration create aws --help
pixi run python -m benchmarking_orchestration worker aws --help
```

## Cloud-init

`cloud_init.sh` is an example AWS bootstrap script. It currently:

- requires Turso credentials, `GPU_CAPABILITY`, and `S3_BUCKET`
- installs Pixi on the instance
- clones this repo
- clones `performance_benchmarks` (`industry_benchmarks` branch)
- installs the `bench` environment
- runs `worker aws --capability <GPU_CAPABILITY>`
- shuts the instance down on exit

When you pass `--cloud-init-file`, the file is rendered as a template using environment variables plus extra values injected by the CLI, such as:

- `GPU_CAPABILITY`
- `S3_BUCKET`
- lowercase Turso aliases (`turso_database_url`, `turso_auth_token`)

## Docker

The included `Dockerfile` builds a CUDA-enabled Pixi image, installs the `bench` environment, and clones `performance_benchmarks` into `/app/performance_benchmarks`.

## Current limitations / notable behavior

- Benchmark workers require `S3_BUCKET` in the runtime environment.
- AWS launch task IDs may embed base64-encoded cloud-init content.
- Benchmark execution imports code directly from the external `performance_benchmarks` checkout.
- Result analysis is currently a Python API, not a first-class CLI command.

## Development

Run tests:

```bash
pixi run test
```

Format:

```bash
pixi run fmt
```

Lint:

```bash
pixi run check
```
