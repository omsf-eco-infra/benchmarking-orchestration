# AGENTS.md

Practical guidance for coding agents working in this repository.

## Package Layout
- Package boundaries follow semantics, not just dependency weight.
- `packages/core/` is the `benchmarking-orchestration-core` distribution
  (import name `benchmarking_orchestration_core`). It holds only provider-neutral
  `AbstractBenchmark` and `BenchmarkKind`, with zero runtime dependencies.
  Never add AMI IDs, instance families, provider-specific task formats, or
  provider, CLI, or persistence imports to core.
- `packages/aws/` is the `benchmarking-aws` distribution (import name
  `benchmarking_aws`). It owns EC2/AMI/quota operations, instance metadata,
  AWS worker capabilities, AWS task IDs, and bootstrap rendering. It depends on
  core and AWS/HTTP libraries, never on the application.
- `src/benchmarking_orchestration/` is the application distribution: CLI
  (`commands`, `__main__`), provider adapters, task DB, and benchmark runner.
  `aws/orchestration.py` connects AWS primitives to app persistence and execution.
  Its top-level `__init__` is lazy, so importing the app must not require `boto3`.
- `tests/test_boundaries.py` guards semantic ownership, import direction, and
  lazy-import behavior.
- Current boundaries are recorded in `docs/package-boundaries.md`; the earlier
  exploration in `docs/monorepo-exploration.md` is historical.

## Environment and Commands
- Use `pixi` for all runs so dependencies resolve correctly (notably `exorcist` and `boto3`).
- Install/sync env:
  - `pixi install`
- Run the app:
  - `pixi run python -m benchmarking_orchestration`

## Testing
- Run the full unit test suite:
  - `pixi run --environment dev --clean-env test`

## Formatting
- `pixi run --environment dev fmt`

## Linting
- `pixi run --environment dev check`

## AWS Runtime Notes
- The quota query uses AWS `service-quotas` in `us-east-1` by default.
- Ensure AWS credentials are available in the shell/session (`AWS_PROFILE`, env vars, or default credential chain).
- If output is empty, verify:
  - Correct AWS account/profile
  - Correct region
  - IAM permission to call `servicequotas:ListServiceQuotas`

## Current Behavior
- The script currently prints On-Demand `G` instance quota entries (for example, `Running On-Demand G and VT instances`) and their quota code/value.
- Quota values are regional pooled limits, not per-instance values.

## Editing Guidelines
- Keep changes small and focused; this repo is minimal and easy to regress with broad refactors.
- Prefer defensive access patterns (`dict.get`) for AWS responses.
- Preserve simple stdout output format unless explicitly changing UX/CLI behavior.
- If expanding behavior (new instance families, region input, CLI args), keep backward compatibility with current default run command.
- Always write numpy docstrings from all functions.

## Validation Before Hand-off
- Re-run:
  - `pixi run python -m benchmarking_orchestration`
- Re-run tests:
  - `pixi run --environment dev --clean-env test`
- Confirm expected printed quotas for the target filter.
- If behavior depends on account-specific quotas, call that out in the summary.
- Ensure that all functions have proper documentation.
