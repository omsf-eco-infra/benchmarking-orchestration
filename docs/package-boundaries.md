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
| `benchmarking-orchestration` (root) | `benchmarking_orchestration` | CLI, task persistence, benchmark execution, and integration | Core, AWS, CLI/storage/database libraries |

Dependency direction:

```text
application -> benchmarking_aws -> core
application --------------------> core
```

Core must not import a provider or the application. The AWS library must not
import the application, its task database, or its CLI. Both libraries can be
installed separately; each declares its own dependencies in its `pyproject.toml`.
Pixi wires all three distributions together as local editable dependencies.
Until the libraries are published, standalone installation must supply both
local projects/wheels when installing AWS; the app requires all three.

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

## Application integration

`benchmarking_orchestration.aws.orchestration` still coordinates application
operations: queueing/checking out tasks, updating their state, and invoking the
benchmark runner. It uses the AWS package for provider primitives. The concrete
`TaskStatusDB`, Cyclopts adapters, and OpenFE/S3 benchmark runner stay in the app;
there is no reverse dependency from the AWS library and no new plugin system.

## Compatibility and checks

Task wire formats, enum values, CLI commands, and default AWS behavior are
unchanged. Imports of AWS helpers must now use `benchmarking_aws`; the removed
core modules have no forwarding aliases, so AWS concepts cannot leak back into
the core API. The application retains its lazy `WorkerCapability` export for
existing app callers; that enum is owned by the AWS package.

`tests/test_boundaries.py` checks the known semantic modules stay out of core,
all library source imports obey dependency direction, and importing library
modules does not pull in the application. Existing AWS tests exercise the moved
client, task encoding, and capability mapping.
