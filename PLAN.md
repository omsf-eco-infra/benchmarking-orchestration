# MPS RBFE Aggregation Plan

## Progress

- Overall: 10 / 10 plan items completed
- Implemented scope note:
  - MPS support now applies to AWS benchmark tasks for both `md` and `rbfe`
  - Salad-specific worker code was intentionally left without MPS-specific changes

1. [x] Confirm the current CLI-to-manifest path that must carry the MPS setting.
   - `create aws` queues the bench task.
   - `worker aws` parses that task and calls `run_benchmark(...)`.
   - `run_benchmark(...)` writes the manifest.
   - Because execution happens later on the instance, the MPS process count must be preserved with the task, not just passed ad hoc at worker runtime.

2. [x] Extend the CLI in the smallest possible way.
   - Added `--mps-process-count <int>` to `create aws`.
   - Default remains `1` so current behavior stays unchanged.
   - Implemented semantics:
     - `1` = current single-process behavior
     - `>1` = run that many concurrent benchmark subprocesses and aggregate outputs

3. [x] Carry the MPS process count through task identity.
   - Extended the bench task ID format to encode the MPS process count.
   - Added a metadata parser that returns:
     - `benchmark_kind`
     - `mps_process_count`
     - `launch_task_id`
   - This lets the AWS worker reconstruct the intended execution mode from the queued task alone.

4. [x] Keep backward compatibility in the parser and default behavior.
   - Legacy bench task IDs without MPS metadata still parse.
   - Old task IDs are treated as `mps_process_count = 1`.
   - The legacy 2-value `_parse_bench_task_id(...)` interface was preserved for existing callers such as Salad.

5. [x] Pass the parsed MPS process count into benchmark execution.
   - Extended `run_benchmark(...)` to accept `mps_process_count`.
   - Implemented behavior:
     - `1` → existing single-process path
     - `>1` → launch N concurrent subprocesses, collect their outputs, sum them, and write one aggregate JSON result

6. [x] Keep the benchmark output JSON schema unchanged while aggregating.
   - The aggregate output remains the same schema as before: top-level `dict[str, float]`.
   - Values are summed per system key across all child outputs.
   - Aggregation fails fast if any child output is missing, invalid JSON, non-numeric, or has mismatched top-level keys.

7. [x] Extend the manifest with MPS metadata and increment the schema version.
   - Added `mps_process_count` to the manifest.
   - Incremented `_RESULT_MANIFEST_SCHEMA_VERSION`.
   - Kept the manifest change minimal aside from the added field and schema bump.

8. [x] Update `cloud_init.sh` to actually start MPS when needed.
   - Added the minimal MPS startup step:
     - `nvidia-cuda-mps-control -d`
   - No extra env vars were introduced.
   - Kept the bootstrap simple and made startup effectively harmless when the command is unavailable.

9. [x] Keep child outputs/logs for debugging, but make the aggregate result canonical.
   - Per-process outputs/logs are preserved.
   - The manifest’s main `output` entry points to the aggregated JSON file.

10. [x] Add focused tests around the CLI/task/manifest/cloud-init path.
   - Added coverage that `create aws --mps-process-count N` encodes the count.
   - Added parser coverage for encoded and legacy task IDs.
   - Added aggregation tests that preserve the old JSON schema.
   - Added manifest coverage for `mps_process_count` and schema version bump.
   - Added a cloud-init test asserting MPS startup is present.
