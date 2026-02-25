# Benchmarking Orchestration

Small CLI for scheduling AWS EC2 launch tasks and follow-on benchmark tasks.

## What it does

- Creates a launch task plus a dependent benchmark task in the task database.
- Validates EC2 instance type and AMI before queuing tasks.
- Runs workers by capability (`launch`, `g3`, `g4dn`, `g5`, `vt1`).
- Launch worker checks out launch tasks and launches one EC2 instance per task.
- Focuses on G/VT instance families (for example `g5.xlarge`, `vt1.3xlarge`).

## Requirements

- Python 3.11+
- `pixi` (used for environment and task execution)
- AWS credentials available in your shell (`AWS_PROFILE` or standard AWS env vars)

## Quick start

```bash
pixi install
```

Create a launch task:

```bash
pixi run python -m benchmarking_orchestration create-launch-task \
  --instance-type g5.xlarge
```

Run the worker to process launch tasks:

```bash
pixi run python -m benchmarking_orchestration worker --capability launch
```

Show CLI help:

```bash
pixi run python -m benchmarking_orchestration --help
```

## Common options

- `--region` (default: `us-east-1`)
- `--ami-id` (default: `ami-0ec16471888b25545`)
- `--cloud-init-file` (optional path to a cloud-init script for EC2 user-data)
- `--db-path` (optional; when omitted, uses Turso if `TURSO_DATABASE_URL` and
  `TURSO_AUTH_TOKEN` are set, otherwise `task_status.db`)
- `--max-tries` for retry attempts on created tasks

Task IDs are created in this format:

- `<region>:<instance_type>:<ami_id>:<uuid4>`
- `<region>:<instance_type>:<ami_id>:<cloud_init_b64>:<uuid4>` when `--cloud-init-file` is set

## Assumptions

- Launch scheduling is limited to G/VT instance families.
- Cloud-init payloads are stored inside task IDs as base64 and must decode to UTF-8 text.
- `create-launch-task` always creates both launch and benchmark task records.
- Benchmark worker capabilities are family-based (`g3`, `g4dn`, `g5`, `vt1`), and
  the non-launch worker path is currently a placeholder.

## Turso

Turso support is used to back `TaskStatusDB` with a remote database instead of a
local `task_status.db` file.

- Purpose: share task queue state across multiple machines/workers and avoid
  relying on one local SQLite file.
- Auto-selection: when `--db-path` is omitted and both `TURSO_DATABASE_URL` and
  `TURSO_AUTH_TOKEN` are set, the CLI connects to Turso.
- Local override: when `--db-path` is provided, the CLI uses that local DB file
  path instead.

## Development

Run tests:

```bash
pixi run --environment dev test
```

Format:

```bash
pixi run fmt
```

Lint:

```bash
pixi run check
```

## Notes

- Service quotas and EC2 validation are region-specific.
- Ensure IAM permissions include EC2 and Service Quotas read access as needed.
- Output and exact behavior can vary by AWS account configuration.
