# Brev stacked port

## Branches

- PR head: `feat/brev-monorepo`.
- PR base: `explore/monorepo-subpackages` at `ae21d32`.
- Source: local `feat/brev` at `6f40e07` (not just `origin/feat/brev`).

This is a semantic port of the complete source-branch changes onto the package
layout, not a merge of the old layout or an in-place rebase. The source and base
branches are left unchanged. Submit this PR against the package-layout branch
so the review diff contains only the Brev work; retarget/rebase after that parent
PR merges. No branches are pushed and no PR is opened by the local port itself.

## Adaptation

- New independently installable `benchmarking-brev` distribution in
  `packages/brev/`, depending only on core. Task encoding, Brev CLI transport,
  and deadline-bounded readiness policy are provider-owned.
- The app owns the controller lifecycle, task persistence, provider CLI adapter,
  and credentialless `worker job` execution. Local benchmark execution is
  separated from artifact upload, as on the source branch.
- Brev unit tests are package-local; app/DB/lifecycle/startup/worker integration
  tests stay at the root. One Pixi environment and CI command still run all tests.
- `BrevCLI` follows the current three-group provider registration protocol and
  coexists with AWS and injected adapters. No Brev-specific worker is registered.
- Core remains provider-neutral, AWS modules stay in `benchmarking_aws`, and no
  old mixed-provider `benchmarking_orchestration.task_id` module is recreated.

Brev task IDs, job/marker schemas, manifest schema version 5, CLI commands, and
controller trust boundaries are retained. The five local fixes beyond the remote
Brev tip are included: manifest-last publication, bounded readiness probes,
clock refresh after slow probes, and guarded terminal persistence/reporting.

## Startup and validation limits

`brev_startup.sh` clones `feat/brev-monorepo`, not the old `feat/brev` layout.
Push this branch before using that script against the public remote. It installs
the monorepo `bench` environment and clones the benchmark repository under the
SSH user's `$HOME/workspace`; it does not receive controller credentials.

Validation uses mocked Brev/AWS transport, local task databases and benchmark
fixtures, package-boundary checks, CLI help, and standalone wheel installation.
No Brev instances are created, no result buckets are contacted, and no live GPU
benchmark is run. A real Brev/CUDA startup remains an infrastructure smoke test.
