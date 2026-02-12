# AGENTS.md

Practical guidance for coding agents working in this repository.

## Environment and Commands
- Use `pixi` for all runs so dependencies resolve correctly (notably `exorcist` and `boto3`).
- Install/sync env:
  - `pixi install`
- Run the app:
  - `pixi run python -m benchmarking_orchestration`

## Testing
- Run the full unit test suite:
  - `pixi run test`
- Run a single test file:
  - `pixi run test-aws`

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

## Validation Before Hand-off
- Re-run:
  - `pixi run python -m benchmarking_orchestration`
- Re-run tests:
  - `pixi run test`
- Confirm expected printed quotas for the target filter.
- If behavior depends on account-specific quotas, call that out in the summary.
