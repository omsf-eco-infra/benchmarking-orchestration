# Benchmarking Orchestration

Small CLI for scheduling and running AWS EC2 launch tasks for benchmarking workflows.

## What it does

- Creates launch tasks in a local task database (`task_status.db` by default).
- Validates EC2 instance type and AMI before queuing tasks.
- Runs a worker that checks out launch tasks and launches one EC2 instance per task.
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
- `--db-path` (default: `task_status.db`)
- `--max-tries` for retry attempts on created tasks

Task IDs are created in this format:

`<region>:<instance_type>:<ami_id>:<uuid4>`

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
