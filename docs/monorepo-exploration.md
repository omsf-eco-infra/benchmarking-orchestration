# Monorepo / subpackage exploration

Status: **historical design record of the initial extraction.** The later
[semantic package boundaries](package-boundaries.md) supersede this document:
AWS task formats, EC2 capabilities, and bootstrap helpers are no longer core;
they now live with the AWS client in the `benchmarking_aws` distribution.

This document records the exploration
and the resulting package boundaries for the `benchmarking-orchestration`
repository. The recommended two-distribution layout has now been built on branch
`explore/monorepo-subpackages` (base commit `0fa3f9f`, equal to `main`); see
Appendix B for what was actually changed and verified.

Revision history / decisions folded in:

1. First draft: keep everything in one distribution.
2. Requirement added: *the core of abstract classes must be installable by
   others*.
3. Requirement refined: *the CLI must not be part of core.*

Decision 3 is the important simplification: core is **not** the provider/CLI
contract. It is the benchmark-author contract plus the shared value types. That
makes core standard-library-only, which in turn makes it genuinely installable
with no provider, CLI, or database dependencies.

Scope boundaries observed while producing this document:

- No source files were moved, added, or edited (only this document was added).
- No dependencies were installed or synced. `.pixi/envs` does not exist in this
  worktree, so `pixi install`/`pixi run` were deliberately not invoked.
- No AWS calls, benchmark runs, or live-infrastructure access were performed.
- `.env.prod` is tracked in the repository; it was **not** read.
- Other worktrees were not modified.

---

## 1. What was inspected (evidence)

| Surface | What was read |
| --- | --- |
| Guidance | `AGENTS.md` |
| Build/packaging | `pyproject.toml`, `pixi.lock` (header + package index), `.gitattributes` |
| Layout | `find`, `git ls-tree` |
| Source | all modules under `src/benchmarking_orchestration/` |
| Abstract surface | `grep` for `abc.ABC`, `Protocol`, `@abstractmethod`, class/def headers |
| Tests | `tests/*.py` (1527 lines total) |
| CI | `.github/workflows/ci.yml`, `.github/dependabot.yml` |
| Runtime/deploy | `cloud_init.sh`, `README.md` |
| Import graph | `grep` over `^from`/`^import` in `src/` and `tests/` |
| Workspace state | `pixi info` (read-only), `pixi install --help` |
| Sibling branches | `git ls-tree -r` for `refactor/extensible`, `refactor/library`, `feat/brev`, `feat/salad`, `feat/modal`, `chore/normalization-colocation-example` |

Checks **not** run, and why: `pixi install`, `pixi run test`,
`pixi run python -m benchmarking_orchestration`, and `pixi run check` all
require a solved/installed Pixi environment (`.pixi/envs` is absent). Those are
listed as proposed verification in §9 rather than executed here.

---

## 2. Current layout

```
benchmarking-orchestration/
├── pyproject.toml                 # single project "benchmarking-orchestration" v0.1.0
├── pixi.lock                      # single lockfile (15,176 lines)
├── cloud_init.sh                  # EC2 bootstrap: clones repo, pixi install -e bench, runs worker
├── AGENTS.md, README.md, LICENSE  # MIT
├── .github/workflows/ci.yml       # one job: setup-pixi (dev, frozen) + pixi run ... test
└── src/benchmarking_orchestration/
    ├── __init__.py                # public surface: create_app, WorkerCapability
    ├── __main__.py                # python -m benchmarking_orchestration
    ├── commands.py                # cyclopts app assembly; hardcodes AwsCLI
    ├── benchmark_kind.py          # BenchmarkKind enum + normalize (leaf)
    ├── capabilities.py            # WorkerCapability enum + resolver (leaf)
    ├── cloud_init.py              # template render + base64 (leaf)
    ├── task_id.py                 # build/parse launch+bench task IDs (imports aws!)
    ├── tasks/__init__.py          # TaskStatusDB (exorcist + sqlalchemy)
    ├── providers/
    │   ├── __init__.py
    │   ├── cli_protocol.py        # ProviderCLI protocol, Config, db setup (cyclopts)
    │   └── aws_cli.py             # AwsCLI cyclopts adapter (AWS-specific)
    ├── aws/
    │   ├── __init__.py            # boto3 wrappers (AMI/vCPU/quota/launch) + re-exports
    │   ├── info.py                # EC2 IMDS MetadataService (requests)
    │   └── orchestration.py       # queue/process launch + bench tasks
    └── bench/__init__.py          # AbstractBenchmark + AWSOpenFEBenchmark (fsspec/s3fs)
└── tests/                         # flat: one test module per source area
```

Observations that constrain the design:

- **Single distribution, single version, single lockfile, single CI job.** There
  is no `[project.scripts]`; the only entry point is `python -m
  benchmarking_orchestration`.
- **`[project]` declares no dependencies.** Runtime dependencies live only in
  `[tool.pixi.dependencies]` / feature tables, and the package itself is wired
  in as an editable PyPI dependency:
  `benchmarking_orchestration = { path = ".", editable = true }`.
  A plain `pip install` of the project would therefore pull no runtime deps.
- **Dependency variation is already expressed as Pixi features**, not packages:
  `aws` (boto3 is the only member), `test`, `dev`, `bench` (openfe/pandas).
- **Python is pinned to 3.13** (`sqlalchemy-libsql` does not work on 3.14 in
  testing); `[project].requires-python` is `>= 3.11`.
- Tests are flat and import the package by its current dotted paths.
- `cloud_init.sh` depends on the repo root manifest path and the `bench`
  environment (`pixi install --manifest-path "$CLI_PATH/pyproject.toml" -e bench`).

### Import / dependency graph (as written today)

```
__main__ ─▶ package __init__ ─▶ commands ─▶ providers.aws_cli
                                              ├─▶ aws            (boto3, botocore)
                                              ├─▶ aws.orchestration
                                              ├─▶ benchmark_kind
                                              ├─▶ capabilities
                                              ├─▶ task_id ──▶ aws (leak: core → provider)
                                              └─▶ providers.cli_protocol ─▶ tasks (cyclopts)

aws.orchestration ─▶ cloud_init, task_id, benchmark_kind, capabilities, tasks
                  └─▶ bench            (lazy import inside _run_benchmark)
bench ─▶ aws.info (MetadataService)     (leak: workload → provider, at import time)
        └─▶ benchmark_kind
tasks ─▶ exorcist, sqlalchemy
```

Two direction violations are visible in the current code:

1. `src/benchmarking_orchestration/task_id.py:6`
   `from benchmarking_orchestration.aws import DEFAULT_LAUNCH_AMI_ID` — the
   task-ID helper (provider-agnostic) cannot be imported without boto3.
   `_build_task_id` is only ever called with an explicit `ami_id`
   (`aws/orchestration.py:264`), so the default is unused in production paths.
2. `src/benchmarking_orchestration/bench/__init__.py:22`
   `from benchmarking_orchestration.aws.info import MetadataService` — the
   benchmark runner imports the AWS metadata client at module import time, even
   though the use site is already wrapped in `try/except` and is best-effort.

Also note: `import benchmarking_orchestration` currently pulls `boto3` through
`__init__ → commands → providers.aws_cli → aws`, so boto3 is not actually
optional at runtime despite being scoped to the `aws` Pixi feature.

### Sibling-branch context (read-only)

- `refactor/extensible` and `refactor/library` keep the same single-package
  `src/benchmarking_orchestration/` layout; `refactor/library` adds
  `dependencies = ["cyclopts>=4.10.1"]` to `[project]` and moves boto3 into base
  Pixi deps. That is the closest existing attempt at making the package declare
  its own install metadata.
- `feat/brev`, `feat/salad`, `feat/modal`, and
  `chore/normalization-colocation-example` add provider code **inside the same
  package**, e.g. `providers/brev_cli.py`, `providers/salad_cli.py`,
  `providers/aws_provider.py`, `providers/base.py`.
- No existing branch uses a multi-distribution (`packages/*`) layout.

---

## 3. The abstract / contract surface, layer by layer

The ABC/Protocol inventory is small. Crucially, the CLI pieces are separable
from the benchmark pieces.

| Symbol | File / line | Kind | Runtime deps | In core? |
| --- | --- | --- | --- | --- |
| `AbstractBenchmark` | `bench/__init__.py:242` | `abc.ABC` + `@abstractmethod run_benchmark(task_id)` | none (but trapped in a module that imports fsspec/s3fs and aws.info) | **yes** |
| `BenchmarkKind` | `benchmark_kind.py:6` | `StrEnum` + normalize | none | **yes** |
| `WorkerCapability` | `capabilities.py:6` | `StrEnum` + resolver | none | **yes** |
| task-ID format | `task_id.py` | build/parse wire format | `aws` (leak, fixable) | **yes** |
| cloud-init render | `cloud_init.py` | template + base64 | none | **yes** |
| `ProviderCLI` | `providers/cli_protocol.py:11` | `typing.Protocol` (`provider_name`, `register_cli(create_app, launch_app, worker_app)`) | `cyclopts.App` | no — CLI |
| `Config` | `providers/cli_protocol.py:39` | dataclass + `_setup_task_status_db` | `cyclopts`, `tasks` | no — CLI |
| `TaskStatusDB` | `tasks/__init__.py:12` | concrete base extending `exorcist.TaskStatusDB` | `exorcist`, `sqlalchemy` | no — persistence |
| `AwsCLI`, `commands.create_app` | `providers/aws_cli.py`, `commands.py` | Cyclopts adapters/assembly | `cyclopts` | no — CLI |
| `AWSOpenFEBenchmark` | `bench/__init__.py:256` | concrete benchmark | `fsspec`, `s3fs` | no — provider impl |

Result: **core = `AbstractBenchmark`, `BenchmarkKind`, `WorkerCapability`, the
task-ID format, and cloud-init rendering — standard library only.** That is the
set a third party needs to write a benchmark. Excluded by decision 3: the
provider/CLI Protocol, `Config`, the CLI assembly, and the task database.

A third party writing a *provider* (not a benchmark) still needs the CLI
Protocol. That contract stays in the app (`providers/cli_protocol.py`), so
provider authors depend on `benchmarking-orchestration`, not on core. External
provider plugins are out of scope: providers are added as in-tree modules (see
Appendix A).

Why core must be extracted at all: none of the core symbols is cleanly
importable today.

- `import benchmarking_orchestration.bench` pulls `fsspec`, `s3fs`, and
  `aws.info` just to read the `AbstractBenchmark` base class.
- `import benchmarking_orchestration` (and therefore `...task_id`) pulls `boto3`.
- `AbstractBenchmark` is co-located with the concrete AWS implementation.

---

## 4. What "installable by others" requires

1. **A dependency-free core.** With CLI and persistence excluded, core imports
   only the standard library. This is the strongest possible answer to "others
   can install and use it".
2. **The package `__init__` must not eagerly import providers.** Today
   `import benchmarking_orchestration` pulls boto3. Even `import
   benchmarking_orchestration.core` executes the parent `__init__.py`. So the
   top-level `__init__` must become lazy (module `__getattr__`) before core is
   usable in a provider-free environment.
3. **A distribution that actually ships core.** Two ways, discussed in §5.

The packaging constraint that shapes the split: two distributions cannot both
own the same regular package prefix (`benchmarking_orchestration/`) without
namespace machinery. Whichever distribution ships
`benchmarking_orchestration/__init__.py` owns `__path__`. Sharing the prefix
requires PEP 420 namespace packages (no parent `__init__.py`) or
`pkgutil.extend_path`. Both are workable but fragile with editable installs and
with Pixi's path-based `pypi-dependencies`, which is exactly this repo's setup.

---

## 5. Options for shipping core

### Option A — single distribution, dependency-free `core` subpackage

Add `benchmarking_orchestration.core` and fix the import graph, but keep one
`pyproject.toml`.

Pros: minimal churn; one lockfile/CI; all paths preserved (with shims); no
namespace issues.

Cons: third parties still install the whole `benchmarking-orchestration`
distribution (and its declared deps). Core is dependency-free *as a module*, but
the distribution is not. No independent versioning.

### Option B1 — core as its own distribution, distinct import name (recommended target)

A monorepo with the app at the root and core under `packages/core/`:

- distribution `benchmarking-orchestration-core` ships
  `src/benchmarking_orchestration_core/` (or a shorter name — naming is
  bikesheddable), with **zero runtime dependencies**.
- the app depends on it in `pyproject.toml` and imports
  `benchmarking_orchestration_core` directly. There is **no**
  `benchmarking_orchestration.core` alias: the pyproject dependency installs the
  package under its real name and does not create a second path (§6.1).
- third parties do `pip install benchmarking-orchestration-core` and implement
  `AbstractBenchmark` without pulling boto3, fsspec, cyclopts, or exorcist.

Pros: true dependency isolation; independent versioning; cleanest external
story; no namespace tricks because the import name is distinct.

Cons: two `pyproject.toml` files, extra editable wiring, CI/build step.

### Option B2 — shared `benchmarking_orchestration.*` namespace (not recommended)

Core ships `benchmarking_orchestration/core/` with no parent `__init__.py`; the
app contributes the rest. Preserves import paths exactly, but is fragile under
editable installs/Pixi and muddies `python -m` ownership.

### Verdict

Because the stated goal is external installability, treat **B1 as the target and
A as the required internal preparation.** The work to make core leak-free and
lazily importable (A) is a prerequisite for B1 regardless. Keep the rest of the
application — `aws`, `bench`, `tasks`, `providers`, `commands`, `__main__` — as a
single app distribution; splitting providers or the bench runner into their own
distributions is not justified by anything in the current code.

---

## 6. Proposed directory trees

### Stage A target — one distribution, dependency-free `core` subpackage (prep)

```
benchmarking-orchestration/
├── pyproject.toml                 # EDIT: [project.dependencies] (+ extras)
├── pixi.lock                      # UNCHANGED unless deps change
├── docs/monorepo-exploration.md   # this document
├── src/benchmarking_orchestration/
│   ├── __init__.py                # EDIT: lazy re-export (no eager provider import)
│   ├── __main__.py                # UNCHANGED
│   ├── commands.py                # UNCHANGED (CLI stays in app)
│   ├── core/                      # NEW: stdlib-only contract surface
│   │   ├── __init__.py            # re-exports the "yes" rows in §3
│   │   ├── benchmark_kind.py      # MOVED from top level
│   │   ├── capabilities.py        # MOVED from top level
│   │   ├── task_id.py             # MOVED + leak fixed
│   │   ├── cloud_init.py          # MOVED from top level
│   │   └── benchmark.py           # AbstractBenchmark, SPLIT from bench/__init__.py
│   ├── benchmark_kind.py          # old-path shim (OPTIONAL; see §6.1)
│   ├── capabilities.py            # old-path shim (OPTIONAL)
│   ├── task_id.py                 # old-path shim (OPTIONAL)
│   ├── cloud_init.py              # old-path shim (OPTIONAL)
│   ├── tasks/__init__.py          # UNCHANGED (persistence, not core)
│   ├── providers/                 # UNCHANGED (CLI contract + AWS adapter)
│   ├── aws/                       # UNCHANGED
│   └── bench/__init__.py          # EDIT: re-export AbstractBenchmark from core
└── tests/                         # UNCHANGED if shims kept; else update imports
```

### Stage B1 target — core promoted to its own dependency-free distribution

```
benchmarking-orchestration/
├── pyproject.toml                 # app dist; depends on benchmarking-orchestration-core
├── pixi.lock                      # single workspace lockfile
├── packages/
│   └── core/
│       ├── pyproject.toml         # dist "benchmarking-orchestration-core", no deps
│       └── src/benchmarking_orchestration_core/
│           ├── __init__.py
│           ├── benchmark_kind.py
│           ├── capabilities.py
│           ├── task_id.py
│           ├── cloud_init.py
│           └── benchmark.py       # AbstractBenchmark
└── src/benchmarking_orchestration/
    ├── __init__.py                # lazy
    ├── __main__.py
    ├── commands.py
    └── tasks/ ... providers/ ... aws/ ... bench/ ...
                                   # app code imports benchmarking_orchestration_core
                                   # (no core/ alias — see §6.1)
```

Recommended dependency direction (both stages):

```
benchmarking_orchestration_core ─▶ stdlib only
tasks ─▶ exorcist, sqlalchemy
providers ─▶ cyclopts + tasks
aws ─▶ benchmarking_orchestration_core + tasks
providers.aws ─▶ aws + benchmarking_orchestration_core + tasks
aws.orchestration ─▶ bench (lazy only)
bench ─▶ benchmarking_orchestration_core; metadata lazily/injected; never aws at import time
commands / __main__ ─▶ providers (composition root)
```

### 6.1 Shim policy (old paths vs. the core alias)

**Shims preserve import paths; `pyproject.toml` governs what gets installed.**
Adding `benchmarking-orchestration-core` as a dependency makes
`import benchmarking_orchestration_core` work; it does **not** make
`import benchmarking_orchestration.core` work. A `core` alias shim living in the
app would therefore be:

- redundant with the real path, and
- unavailable to anyone who installed only core — the exact audience the split
  exists for.

So no alias is created. `benchmarking_orchestration_core` is the one canonical
core import.

The only shims worth considering are for **existing** paths whose modules move
(`benchmarking_orchestration.task_id`, `.benchmark_kind`, `.capabilities`,
`.cloud_init`, and `benchmarking_orchestration.bench.AbstractBenchmark`). Options:

- **No shims (recommended):** update the app's own imports and the flat tests.
  There are no known external consumers and the app is `0.1.0`; the mechanical
  import churn is small.
- **Short-lived shims (conservative):** keep the old modules as one-line
  re-exports for a release, then delete. Preserves strict backward compatibility
  at the cost of a second import name for the same objects.

The app's documented entry points are unaffected either way:
`python -m benchmarking_orchestration`, `__main__.py`, and the top-level
`create_app`/`WorkerCapability` re-exports stay in the app distribution.

---

## 7. Tradeoffs

| Concern | Stage A (single dist + core subpackage) | Stage B1 (core distribution) |
| --- | --- | --- |
| **Core contents** | `AbstractBenchmark`, enums, task-id, cloud-init — stdlib only. | Same, packaged on its own. |
| **CLI in core?** | No. `ProviderCLI`/`Config`/`commands` stay in the app. | No. |
| **Installable by others** | Importable only after installing the app distribution (with its deps). | `pip install benchmarking-orchestration-core` gives a zero-dependency core. |
| **Independent versioning** | No. | Yes. |
| **Local editable installs** | One path (`.`). | Two paths (`"."`, `"packages/core"`), both editable. |
| **Lockfiles** | One `pixi.lock`. | One workspace lockfile, two manifests. |
| **CI** | Unchanged. | Adds a workspace build/install step. |
| **Backward compat** | Old paths preserved by shims, or updated in place. | Old paths via optional shims; no `core` alias. |
| **Risk/effort** | Low. | Medium (but no namespace tricks). |

---

## 8. Staged migration plan (proposed; not applied here)

### Stage 0 — document + guardrails

- Add `docs/monorepo-exploration.md` (this file).
- Add `[tool.ruff.lint.flake8-tidy-imports.banned-api]` rules forbidding
  `benchmarking_orchestration.aws` / `bench` / `providers` imports from `core`.
- Touched: `pyproject.toml`. No new deps (ruff already in `dev`).

### Stage 1 — remove the two direction leaks (prerequisite)

- `src/benchmarking_orchestration/task_id.py`: drop the `aws` import; make
  `ami_id` required in `_build_task_id` (only caller already passes it).
- `src/benchmarking_orchestration/bench/__init__.py`: move the `aws.info`
  `MetadataService` import inside the existing `try/except`, or inject a
  metadata provider.
- Touched: `task_id.py`, `bench/__init__.py`.
- Verify: `pixi run test`; `task_id` and `bench` import without boto3.

### Stage 2 — split `AbstractBenchmark` out of the heavy bench module

- New `src/benchmarking_orchestration/core/benchmark.py` with the ABC (stdlib).
- `bench/__init__.py` imports it from `core` and re-exports it, so
  `from benchmarking_orchestration.bench import AbstractBenchmark` still works.
- Touched: new `core/benchmark.py`, `bench/__init__.py`. No test changes.

### Stage 3 — introduce `core` and make the top-level import lazy

- Move `benchmark_kind.py`, `capabilities.py`, `task_id.py`, `cloud_init.py` into
  `core/`; leave top-level shim modules.
- Make `src/benchmarking_orchestration/__init__.py` lazy (module `__getattr__`)
  so importing `benchmarking_orchestration.core` does not run
  `commands → providers.aws_cli → aws`.
- Do **not** move `providers/cli_protocol.py`, `tasks/`, or `commands.py` into
  core.
- Touched: `__init__.py`, `core/*` (new), the four moved modules (or their
  shims), `aws/orchestration.py` (relative import targets).
- Verify: `pixi run test`; `python -m ... --help`; core imports with boto3,
  fsspec, cyclopts, and exorcist all absent.

### Stage 4 — declare install metadata

- Add `[project.dependencies]` and `[project.optional-dependencies]`.
  Once core is its own distribution (Stage 5), the app's base deps are
  `cyclopts` + `exorcist` (+ the local core), with `aws`/`bench` as extras.
- Confirm whether Pixi's `pypi-dependencies` accepts `extras`; otherwise extras
  stay `pip`-only.
- Touched: `pyproject.toml`; regenerate `pixi.lock` only if resolution changes.

### Stage 5 — promote core to its own distribution (B1)

- Create `packages/core/{pyproject.toml,src/benchmarking_orchestration_core/}`
  (zero deps) and move the `core/` modules there.
- Delete `src/benchmarking_orchestration/core/`; app code imports
  `benchmarking_orchestration_core` directly. **No alias shim.**
- Add both packages to `[tool.pixi.pypi-dependencies]`; update CI to install the
  workspace.
- Touched: `pyproject.toml`, new `packages/core/`,
  `src/benchmarking_orchestration/` (import updates; delete `core/`),
  `.github/workflows/ci.yml`, `pixi.lock`.
- Verify: full suite, `python -m ... --help`, and a clean-env
  `pip install benchmarking-orchestration-core` (proposed).

---

## 9. Verification

### Checks actually run for this document (read-only)

- `git branch --show-current` → `explore/monorepo-subpackages`.
- `git status --short` → clean except this new document.
- `git worktree list` → original checkout and other worktrees untouched.
- `find` / `git ls-tree -r --name-only` → current and sibling-branch layouts.
- `grep` over `src/` and `tests/` → import graph, call sites, ABC/Protocol
  inventory, module-path string literals.
- `pixi info` → workspace name/version, environments, dependency counts;
  confirmed no `.pixi/envs`.
- `pixi install --help` → confirmed `--frozen` / `--locked`.
- `wc -l`, `python3 --version` (3.14.5, host only — not the pinned 3.13).

No tests, app runs, installs, or AWS calls were executed.

### Proposed checks after each stage

```bash
# Full suite (CI parity)
pixi run --environment dev --clean-env test

# CLI still resolves through python -m
pixi run python -m benchmarking_orchestration --help
pixi run python -m benchmarking_orchestration create aws --help
pixi run python -m benchmarking_orchestration worker aws --help

# Formatting / linting / lockfile
pixi run fmt
pixi run check
pixi install --locked

# Backward-compatibility smoke checks for preserved dotted paths
pixi run python -c "import benchmarking_orchestration; \
from benchmarking_orchestration.aws import DEFAULT_LAUNCH_AMI_ID; \
from benchmarking_orchestration.aws.orchestration import queue_aws_tasks; \
from benchmarking_orchestration.providers.aws_cli import AwsCLI; \
from benchmarking_orchestration.providers.cli_protocol import ProviderCLI; \
from benchmarking_orchestration.task_id import _parse_launch_task_id; \
from benchmarking_orchestration.bench import AbstractBenchmark"

# Core must import WITHOUT provider/CLI/DB deps
# Stage A (subpackage):
pixi run python -c "import sys; \
from benchmarking_orchestration.core import AbstractBenchmark, BenchmarkKind, WorkerCapability; \
bad = {'boto3','fsspec','s3fs','cyclopts','exorcist','sqlalchemy'} & set(sys.modules); \
assert not bad, bad"
# Stage B1 (distribution; no shim):
pixi run python -c "import sys; \
from benchmarking_orchestration_core import AbstractBenchmark, BenchmarkKind, WorkerCapability; \
bad = {'boto3','fsspec','s3fs','cyclopts','exorcist','sqlalchemy'} & set(sys.modules); \
assert not bad, bad"

# Top-level import must stay lazy (Stage 3 goal)
pixi run python -c "import sys; import benchmarking_orchestration; \
assert 'boto3' not in sys.modules, 'package __init__ eagerly imported providers'"
```

If Stage 5 is attempted:

```bash
pip install -e packages/core     # isolated env: prove zero-dependency core
python -c "import benchmarking_orchestration_core as c; print(c.AbstractBenchmark)"
```

---

## 10. Open questions / unknowns

- Distribution/import naming for the extracted core
  (`benchmarking-orchestration-core` / `benchmarking_orchestration_core` vs a
  shorter name). Naming is bikesheddable; the distinct-name property matters.
- Does Pixi accept `extras` inside `[tool.pixi.pypi-dependencies]` for editable
  path installs? (Not verified; would require a solve/install.)
- Is independent versioning of core required, or is "publishable core" enough?
  This is the A-vs-B1 trigger. External installability already favors B1.
- Should `AbstractBenchmark` be a true ABC (subclassing) or a structural
  `Protocol`, so third parties can implement it without importing the package at
  runtime?
- Is there a private index / release process, or is git/path install enough for
  now?
- External provider plugins: **resolved — not needed.** `ProviderCLI` stays an
  internal app seam; providers are in-tree modules (Appendix A).
- CI currently runs tests only; adding `pixi run check` would catch boundary
  violations.

---

## 11. Summary of recommendation

- **Core is CLI-free.** It contains only `AbstractBenchmark`, `BenchmarkKind`,
  `WorkerCapability`, the task-ID format, and cloud-init rendering — standard
  library only. `ProviderCLI`, `Config`, `commands`, and `TaskStatusDB` are
  explicitly **out** of core.
- **Target: a small monorepo with a dependency-free core distribution**
  (`benchmarking-orchestration-core`, import name `benchmarking_orchestration_core`)
  plus the existing app distribution. The canonical core import is the
  distribution's own name; the app does **not** re-export it as
  `benchmarking_orchestration.core`.
- **Prepare with Stage A first**: fix the two import-direction leaks, split
  `AbstractBenchmark` out of the fsspec-heavy module, move the shared value types
  into `core/` (old-path shims optional), make the top-level `__init__` lazy,
  and declare
  `[project.dependencies]`/extras. Then promote core to its own distribution
  (Stage 5).
- **Do not split providers, bench, tasks, or the CLI** into their own
  distributions. And do not put the CLI/provider protocol into core.
- **No external plugin system.** Keep `ProviderCLI` as an internal seam and keep
  providers as in-tree modules in the app. Entry-point discovery and a plugin
  distribution are documented as considered-and-declined in Appendix A.

Report path: `docs/monorepo-exploration.md`.

---

## Appendix B. Implementation status (what was built)

The recommended layout was implemented directly on `explore/monorepo-subpackages`
with no interim shims (per §6.1).

Built:

- New zero-dependency distribution `benchmarking-orchestration-core` in
  `packages/core/`, shipping `benchmarking_orchestration_core`:
  `AbstractBenchmark` (new `benchmark.py`), `BenchmarkKind`, `WorkerCapability`,
  the task-ID format, and cloud-init rendering.
- Moved `benchmark_kind.py`, `capabilities.py`, `cloud_init.py`, `task_id.py`
  from `src/benchmarking_orchestration/` into core; app and test imports updated.
- `task_id._build_task_id` now requires `ami_id`; the core → aws import is gone.
- `AbstractBenchmark` split out of `bench/__init__.py`; `bench` imports it from
  core and lazily imports `aws.info.MetadataService`.
- `benchmarking_orchestration/__init__.py` is lazy (`__getattr__`), so importing
  the package no longer pulls `boto3`.
- `commands.create_app(*, providers=None)` accepts injected provider adapters
  (internal seam; no entry-point discovery).
- Root `pyproject.toml` declares the core path dependency in
  `[tool.pixi.pypi-dependencies]`; `pixi.lock` updated.
- `tests/test_boundaries.py` added to guard dependency isolation and laziness.
- No shims added for the old module paths (see §6.1); tests were updated
  instead.

Verified (actually run):

- `pixi install --environment dev` → both `benchmarking_orchestration` and
  `benchmarking_orchestration_core` installed as editable PyPI packages.
- `pixi run --environment dev --clean-env test` → **82 passed**.
- `pixi run --environment dev check` → All checks passed.
- `pixi run python -m benchmarking_orchestration --help`, `create aws --help`,
  and `worker aws --help` → resolve.
- Subprocess checks: core imports with no `boto3`/`fsspec`/`cyclopts`/`exorcist`/
  `sqlalchemy`; importing `benchmarking_orchestration` does not import `boto3`.

Not done / out of scope: publishing core to an index; `[project.dependencies]`
metadata for the app distribution; provider plugins/entry points (Appendix A).

---

## Appendix A. Provider seam (no external plugin system)

Decision: **do not build an external plugin system.** There is one in-tree
provider, a Salad provider was previously added and removed, and no external
consumer has been identified. `ProviderCLI` stays an internal seam; entry-point
discovery and a `benchmarking-orchestration-plugin` distribution are not
recommended. The remainder of this appendix is a rejected-design record.

### A.1 The seam already exists

The repo already has every element of a plugin system except discovery:

- `providers/cli_protocol.py:11` defines `ProviderCLI` (`provider_name` +
  `register_cli(create_app, launch_app, worker_app)`), a structural
  `typing.Protocol`.
- `commands.create_app()` (`commands.py:8`) is the composition root, but it
  hardcodes `AwsCLI()`:

  ```python
  _aws_cli = AwsCLI()
  _aws_cli.register_cli(create_app=..., launch_app=..., worker_app=...)
  ```

- `providers/aws_cli.py:118` is the only implementation; sibling branches add
  `providers/brev_cli.py`, `providers/salad_cli.py`, etc.

So "a plugin system" here is mostly: **make `create_app` take a list of
providers and optionally discover more from installed distributions.**

### A.2 Contract

Keep `ProviderCLI` as the contract (it is small and CLI-shaped). Its home
follows the same layering decision as core:

| Tier | `ProviderCLI` lives in | Plugin author installs |
| --- | --- | --- |
| 1 (in-tree) | `providers/cli_protocol.py` in the app | full app distribution |
| 2 (external) | a `benchmarking-orchestration-plugin` dist: `cyclopts` + core + tasks | plugin dist only (no boto3/fsspec) |

Because `ProviderCLI` takes `cyclopts.App`, it can never be standard-library
only. That is exactly why it is **not** part of core.

### A.3 Discovery via entry points

Standard `importlib.metadata` entry points, group
`benchmarking_orchestration.providers`:

```python
# benchmarking_orchestration/plugins.py
from importlib.metadata import entry_points

PROVIDER_GROUP = "benchmarking_orchestration.providers"

def load_provider_plugins(group: str = PROVIDER_GROUP):
    plugins = []
    # sorted() keeps registration order deterministic across environments
    for ep in sorted(entry_points(group=group), key=lambda e: e.name):
        obj = ep.load()
        plugins.append(obj() if isinstance(obj, type) else obj)
    return plugins
```

`create_app` becomes injectable (which also makes it testable without
monkeypatching entry points):

```python
def create_app(*, providers=None) -> App:
    app = App()
    groups = {n: App(name=n) for n in ("create", "launch", "worker")}
    selected = list(providers) if providers is not None \
        else [AwsCLI(), *load_provider_plugins()]
    seen = set()
    for provider in selected:
        name = provider.provider_name
        if name in seen:
            raise ValueError(f"Duplicate provider '{name}'")
        seen.add(name)
        provider.register_cli(groups["create"], groups["launch"], groups["worker"])
    for group in groups.values():
        app.command(group)
    return app
```

### A.4 Plugin packaging

```toml
# benchmarking-orchestration-brev/pyproject.toml
[project]
name = "benchmarking-orchestration-brev"
dependencies = ["benchmarking-orchestration-plugin"]

[project.entry-points."benchmarking_orchestration.providers"]
brev = "benchmarking_orchestration_brev:BrevCLI"
```

Policy questions to decide with the first external plugin:

- **Duplicate names**: raise (shown above) vs last-wins vs first-wins.
- **Import failure**: abort vs log-and-skip. A `--strict-plugins` flag or
  `PIXI_...`/env escape hatch gives reproducible CI behavior.
- **Built-ins**: register AWS first, or ship AWS itself as an entry point so the
  built-in dogfoods the plugin path.
- **Order**: sort by entry-point name; do not rely on filesystem order.
- **Trust**: entry points execute installed code. Installed implies trusted;
  document that adding a provider is a normal dependency install.
- **Config**: keep the current env-var + Cyclopts `Parameter(env_var=...)`
  convention; do not invent a plugin config schema yet.

### A.5 Benchmarks are a different kind of plugin

The current benchmark path is **not** entry-point based: `bench._resolve_benchmark_runner`
(`bench/__init__.py:70`) does `importlib.import_module(f"{kind}_benchmark")`
from a *staged checkout* of the external `performance_benchmarks` repo. That
code is not installed, so it cannot be discovered via entry points. Options:

1. Keep the staged-repo import (status quo) — it is already a dynamic plugin
   mechanism, just not a registered one.
2. Add an entry-point group for **installed** benchmark factories that return an
   `AbstractBenchmark`, and keep the staged import as a fallback.

Recommendation: keep (1) for now. Formalize benchmark plugins only when a
second, installable benchmark implementation exists. `AbstractBenchmark` in core
is the extension point either way.

### A.6 Decision and what to keep

- **Keep** `ProviderCLI` in the app (`providers/cli_protocol.py`) as an internal
  protocol; new providers are added as in-tree modules
  (`providers/<name>_cli.py`) and registered explicitly in
  `commands.create_app()`.
- **Keep (optional)** a `create_app(providers=...)` parameter purely so tests can
  inject a fake provider. This is an internal seam, not a public plugin API.
- **Do not add** entry-point discovery, a provider entry-point group, or a
  `benchmarking-orchestration-plugin` distribution.
- **Revisit only if** a provider is genuinely maintained in another repository
  and cannot depend on `benchmarking-orchestration`.
