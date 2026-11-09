# Package boundaries

Packages are bounded by their semantics, not merely by their dependencies.
A standard-library-only module can still be provider-specific: AMI identifiers,
EC2 instance families, and AWS launch-task formats do not belong in core.
This decision supersedes the two-distribution recommendation in
[the initial exploration](monorepo-exploration.md).

## Distributions

| Distribution | Import | Responsibility | Runtime dependencies |
| --- | --- | --- | --- |
| `benchmarking-orchestration-core` (`packages/core/`) | `benchmarking_orchestration_core` | `AbstractBenchmark` and `BenchmarkKind` | None |
| `benchmarking-aws` (`packages/aws/`) | `benchmarking_aws` | Reusable AWS semantics and operations | Core, boto3, requests |
| `benchmarking-brev` (`packages/brev/`) | `benchmarking_brev` | Brev task formats, CLI transport, and readiness policy | Core |
| `benchmarking-orchestration` (root) | `benchmarking_orchestration` | CLI, task persistence, benchmark execution, and integration | Core, AWS, CLI/storage/database libraries |

Dependency direction:

```text
application -> benchmarking_aws  -> core
application -> benchmarking_brev -> core
application ---------------------> core
```

Core must not import a provider or the application. Provider libraries must
not import each other, the application, its task database, or its CLI framework.
All libraries can be installed separately; each declares its own dependencies
in its `pyproject.toml`. Pixi wires all four distributions together as local
editable dependencies. Until the libraries are published, standalone installation
must supply core alongside the selected provider project/wheel; the app requires
all four distributions.

## AWS ownership

- `benchmarking_aws.client`: EC2 instance validation and launch, AMI resolution,
  and regional On-Demand quota/usage queries.
- `benchmarking_aws.launch`: approved-AMI checks, regional quota preflight,
  and capacity retry policy, independent of the task database.
- `benchmarking_aws.info`: EC2 instance metadata.
- `benchmarking_aws.capabilities`: EC2-family worker capability names and mapping.
- `benchmarking_aws.task_id`: launch/benchmark task encoding that embeds AWS
  region, instance type, AMI, and optional user-data.
- `benchmarking_aws.cloud_init`: the bootstrap rendering/base64 helpers currently
  used by the AWS flow. This ownership does not imply cloud-init itself is
  AWS-exclusive; no shared bootstrap package is needed for the current callers.

Importing the AWS package or its task/capability helpers does not load boto3.
The SDK is imported only through `benchmarking_aws.client`.

## Brev ownership

- `benchmarking_brev.transport`: subprocess adapter for the official Brev CLI.
- `benchmarking_brev.task_id`: credential-free Brev execution metadata and
  instance/job name correlation, separate from AWS task formats.
- `benchmarking_brev.readiness`: deadline-bounded instance inspection and SSH
  startup-marker probes, independent of task persistence.
- `benchmarking_brev.BREV_TASK_CAPABILITY`: the provider-specific queue token.

This library depends only on core and the standard library. It does not need
boto3, S3, a task database, or Cyclopts. The external `brev` executable is needed
only when the trusted controller invokes a transport operation.

## Application integration

`benchmarking_orchestration.aws.orchestration` still coordinates application
operations: queueing/checking out tasks, updating their state, and invoking the
benchmark runner. It uses the AWS package for provider primitives. The concrete
`TaskStatusDB`, Cyclopts adapters, and OpenFE/S3 benchmark runner stay in the app;
there is no reverse dependency from either provider library and no new plugin system.

`benchmarking_orchestration.brev` owns task queueing and its controller lifecycle:
staging jobs, polling worker markers, recording state, validating retrieved
artifacts, uploading to S3, and cleaning up instances. `job_worker.py` and the
local benchmark runner execute the provider-neutral staged job without accessing
controller credentials, databases, Brev, or S3. The application owns both ends
of the job/marker contract; it is not a Brev primitive or a new core abstraction.
`worker job` is registered at app level, while `create brev` and `launch brev`
use the existing provider CLI adapter protocol.

See [the stacked Brev port](brev-monorepo-port.md) for source/base refs and scope.

## Compatibility and checks

Task wire formats, enum values, CLI commands, and default AWS behavior are
unchanged. Imports of AWS helpers must now use `benchmarking_aws`; the removed
core modules have no forwarding aliases, so AWS concepts cannot leak back into
the core API. The application retains its lazy `WorkerCapability` export for
existing app callers; that enum is owned by the AWS package.

## Test ownership

- `packages/core/tests/`: provider-neutral benchmark contract and workload kinds.
- `packages/aws/tests/`: AWS client, task encoding, capability mapping, and
  standalone launch policy.
- `packages/brev/tests/`: task formats, CLI transport, and bounded readiness.
- Root `tests/`: app CLI, persistence and runner tests, plus cross-package
  integration and boundary checks. AWS task orchestration tests stay here because
  they exercise the application database and its integration with AWS. Brev
  lifecycle, startup, and credentialless worker tests also remain at the root.

`tests/test_boundaries.py` checks the known semantic modules stay out of core,
all library source imports obey dependency direction, and importing library
modules does not pull in the application.

Root `pyproject.toml` explicitly configures all four test directories. The
existing `pixi run --environment dev --clean-env test` command runs everything;
package-local tests do not introduce separate environments or CI jobs.
