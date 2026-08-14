# Benchmarking Orchestration

Provider-oriented CLI for queuing launch work, dispatching benchmark workers, and storing task state in an `exorcist` task database.

The repository currently supports two provider flows:

- **AWS EC2**: validate an instance type and AMI, queue a launch task plus one or more dependent benchmark tasks, launch the instance, then run MD/RBFE benchmarks and upload artifacts to S3.
- **NVIDIA Brev**: queue credentialless benchmark jobs locally, then use a trusted controller to create one Brev instance per task, retrieve its results, upload them to S3, and delete the instance.

This repo is no longer just a small AWS quota helper; the main CLI is now:

```bash
pixi run python -m benchmarking_orchestration
```

## Usage

Install the environment with `pixi install`, then choose either the [AWS](#quick-start-aws) or [NVIDIA Brev](#quick-start-nvidia-brev) workflow. Both follow the same basic sequence:

1. Run `create <provider>` to queue benchmark work.
2. Run `launch <provider>` with the same task database to process queued work.
3. Repeat `launch` when more queued tasks remain.

Use `--db <path>` on both `create` and `launch` to keep task state in an explicit local SQLite file. Without `--db`, the CLI uses the database selection rules described below.

AWS instances run `worker aws` through cloud-init. The Brev controller starts `worker job` remotely after staging a credentialless job. New users normally do not invoke either worker command by hand.

Run the top-level help to discover commands, then the provider help for required arguments and options:

```bash
pixi run python -m benchmarking_orchestration --help
pixi run python -m benchmarking_orchestration create aws --help
pixi run python -m benchmarking_orchestration launch aws --help
pixi run python -m benchmarking_orchestration create brev --help
pixi run python -m benchmarking_orchestration launch brev --help
```

The command groups are:

- `create`: queue launch or benchmark tasks
- `launch`: process queued tasks
- `worker`: execute an AWS benchmark task or a staged provider-neutral job

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

### Brev flow

- Queues one independent task per requested benchmark kind in the controller's task database.
- Claims one task and creates one Brev instance per `launch brev` invocation.
- Moves the benchmark checkout created by `brev_startup.sh` into the staged credentialless job and copies only `job.json` from the controller.
- Starts the provider-neutral worker detached and polls its durable `status.json` and `complete.json` markers.
- Retrieves and validates the completed result bundle on the controller.
- Uploads the validated artifacts to S3 from the controller and deletes the instance.
- Records Exorcist success only after both upload and instance cleanup succeed.

### Result handling

AWS workers and the trusted Brev controller upload artifacts to:

```text
runs/<yyyy-mm-dd>/<sha256(task_id)>/
```

The date comes from the validated benchmark start time. Each run includes:

- the benchmark input JSON
- the benchmark output JSON
- `stdout.log`
- `stderr.log`
- `exception_traceback.log` when execution fails
- `manifest.json`

## Requirements

- `pixi`
- Python 3.11+ at the package level
- AWS credentials when using the AWS provider or uploading Brev results to S3
- The official `brev` CLI, authenticated on the trusted controller, when using Brev

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

By default, AWS workers look for it at:

```text
/opt/dlami/nvme/performance_benchmarks
```

Override the AWS path with `--bench-repo-path` or `BENCHMARK_REPO_PATH`. Brev does not require a controller-local checkout: `brev_startup.sh` clones only the `industry_benchmarks` branch into `/home/ubuntu/workspace/performance_benchmarks`, and `launch brev` moves that checkout into the staged job before starting the worker.

## Task database

Task state is stored in `benchmarking_orchestration.tasks.TaskStatusDB`, which extends `exorcist.TaskStatusDB` with per-task capabilities.

Database selection works like this:

- if `--db` is provided, use that local SQLite file
- otherwise, if `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are set, use Turso
- otherwise, fall back to local `task_status.db`

Use an explicit local `--db` for the Brev controller. The SQLite database is controller-local state and must not be placed on shared or network storage. Brev lifecycle details remain in each job's local `controller.json`; no separate lifecycle table is created.

## Common environment variables

### Shared / task-db

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- `BENCHMARK_REPO_PATH`

### AWS / trusted Brev controller

- `AWS_PROFILE` or the usual AWS credential chain
- `AWS_BENCHMARK_AMI_ID` as the approved AMI for `create aws`
- `EC2_KEY_NAME` for optional SSH/debug access on launched instances
- `EC2_IAM_INSTANCE_PROFILE` for the launched EC2 instance
- `BENCHMARK_S3_BUCKET` as a default for `create aws --s3-bucket`
- `S3_BUCKET` at AWS benchmark worker runtime
- `BENCHMARK_S3_BUCKET` as the default destination for `launch brev`

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

## Quick start: NVIDIA Brev

### Prerequisites

On the trusted controller:

- install and authenticate the official `brev` CLI
- configure AWS credentials that can write to the result bucket
- install this project with `pixi install`
- choose an explicit Brev instance type and benchmark profile; the controller does not infer a GPU/profile mapping

The Brev instance must have outbound access for `brev_startup.sh` to install Pixi and clone both repositories. For now, the script clones the `feat/brev` branch of this repository and the `industry_benchmarks` branch of `performance_benchmarks`, both with `--single-branch`. Those branches must be available from their public remotes before launch. The startup script and staged worker job contain no AWS, S3, Turso, or Brev credentials.

Search the authenticated Brev catalog for an available GPU type:

```bash
brev search --gpu-name A100 --sort price
```

Copy the value from the search result's `TYPE` column, such as `g5.xlarge`, and pass it as `INSTANCE-TYPE` to `create brev`.

The second positional argument is `PROFILE`. It is a controller-chosen metadata label, such as `openfe-gpu`, that is stored in the task, worker job, and completion marker for correlation. It does not select Brev hardware, configure the benchmark, or change worker execution. Profile labels must be 1-128 characters using letters, numbers, `.`, `_`, or `-`.

```bash
pixi run python -m benchmarking_orchestration create brev \
  g5.xlarge openfe-gpu \
  --benchmark-kind both \
  --mps-process-count 1 \
  --db brev-tasks.db
```

This selects a type, not an existing instance from `brev ls`. The controller generates a new instance name and later runs `brev create <generated-name> --type g5.xlarge`; reusing an existing Brev instance is intentionally unsupported. `both` creates separate MD and RBFE tasks. Each `launch brev` invocation atomically claims one available Exorcist task, creates one instance for it, and processes no other task:

```bash
pixi run python -m benchmarking_orchestration launch brev \
  my-benchmark-results \
  --db brev-tasks.db \
  --result-directory brev-results \
  --startup-script brev_startup.sh
```

You can set `BENCHMARK_S3_BUCKET` instead of passing the bucket argument. Run `launch brev` again to process the next queued task.

After `brev create` reports the instance running, the controller prints flushed lifecycle updates while waiting for Brev to report both `shell_status=READY` and `health_status=HEALTHY`. It then requires a successful `brev exec true` SSH probe before copying the job, retrying transient gateway failures within `--timeout-seconds`. The probe also establishes Brev's persistent SSH control connection for the copy. The controller then starts the remote worker with `nohup`, prints each observed worker heartbeat, and polls durable worker markers every 30 seconds without using a long-running SSH session as a completion signal. Worker polling has no timeout or stale-heartbeat cutoff.

### Trust and credentials

The machine running `launch brev` is the trusted boundary. It owns the local SQLite database, authenticated Brev CLI, AWS credentials, instance lifecycle, result validation, and S3 upload. The Brev worker receives a local `job.json` and uses the benchmark checkout cloned by the credentialless startup script, writes `status.json` heartbeats, and writes `complete.json` last using an atomic rename. It does not access SQLite, Turso, Brev credentials, AWS credentials, or S3.

Do not put credentials in task IDs, startup scripts, benchmark inputs, or the result directory. Anyone able to modify the controller database, staged inputs, or local result bundle is inside the trusted boundary.

### Local results, cleanup, and recovery

The default local path for a claimed task is:

```text
brev-results/<remote-job-id>/
```

A successful retrieval contains `results/`, `status.json`, `complete.json`, and `controller.json`. The controller retains this directory after upload. `controller.json` is written atomically and records the task, instance and remote job IDs, attempt, lifecycle transitions, latest observed heartbeat, local result path, S3 destination, cleanup state, and failure details. Partial files and failure details are retained when retrieval, validation, upload, cleanup, or Exorcist finalization fails.

On normal completion, worker failure, controller failure, or `Ctrl-C`, the controller deletes the Brev instance when it still exists. Exorcist success is recorded only after the result bundle has been validated and uploaded and instance cleanup has succeeded. Upload or cleanup failure records task failure instead of success; the single-attempt task is not retried automatically.

If the controller is killed before its cleanup handler runs, inspect `controller.json` and `brev ls --json`, then remove any orphaned instance with:

```bash
brev delete <instance-name>
```

Validated artifacts are uploaded by the controller under the existing `runs/<yyyy-mm-dd>/<sha256(task_id)>/` layout. A failed launch does not automatically resume or re-upload retained local files; diagnose the persisted failure, clean up any orphaned instance, and explicitly create a replacement task when another attempt is wanted.

Show Brev help without creating an instance or contacting AWS:

```bash
pixi run python -m benchmarking_orchestration create brev --help
pixi run python -m benchmarking_orchestration launch brev --help
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

- AWS benchmark workers require `S3_BUCKET` in the runtime environment; Brev workers are credentialless and upload nothing directly.
- AWS launch task IDs may embed base64-encoded cloud-init content.
- Brev uses one attempt and one task per instance, with no automatic retries or GPU/profile mapping.
- Brev worker polling is fixed at 30 seconds and has no timeout or stale-heartbeat cutoff; `--timeout-seconds` applies only to post-creation SSH readiness.
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
