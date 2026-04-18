# Bazel-Based Dependency Management for Legend

Proposes a Bazel-managed monorepo for Legend (platform + projects), with an incremental path that lets the 1000s of existing external Legend repos participate in the same build ecosystem — Bazel fetches them as source and builds them with the same rules as monorepo projects, no separate extraction pipeline. Whether in the monorepo or external, users still write `.pure` files exactly as they do today; the build automatically shreds each `PackageableElement` into its own output file, giving Bazel per-element dependency tracking, faster incremental builds, and lazy loading of cross-project elements.

## How It Works in 60 Seconds

- **Artifact boundary** — each `PackageableElement` becomes its own self-contained JSON file. No shapes/impl split; no separate shape-extraction step.
- **Element discovery** — a Bazel module extension regex-scans `.pure` files pre-analysis and emits per-project element FQN lists, so `legend_library` can declare one output file per element before the compiler runs.
- **Bazel graph stays coarse** — one `legend_library` node per project with human-declared `deps`. Bazel does not build a per-element DAG; per-property refs inside Pure bodies would require running the TypeChecker to extract, which is exactly the work the compile action already does.
- **Per-element precision comes from `unused_inputs_list`** — the compiler writes the set of dep element files it never opened. Bazel uses that list to skip rebuilds even when an upstream project rebuilds, so consumers whose used-set is unchanged are cache hits.
- **Cross-project resolution is Java-style** — the compiler lazy-loads dep element files on first `findClass` / `findProperty` miss via `Files.exists()` on `fqn.replace("::", "__") + ".json"`. The filesystem is the index — no manifest, no pre-scan.
- **Executable bodies travel as resolved AST** — function, service, and derived-property bodies serialize with every name canonicalized to FQN. Load-time needs no import context.
- **Builds are semantically validated** — a `validateElement(fqn)` primitive runs the right checkers per element kind. A green `bazel build` guarantees a type-correct project, not just parse-correct.

## Table of Contents

1. [Why Bazel?](#1-why-bazel)
2. [The Key Insight: Per-Element Files, Not Header/Impl Split](#2-the-key-insight-per-element-files-not-headerimpl-split)
2.5. [Project-Qualified FQNs](#25-project-qualified-fqns)
3. [Monorepo Structure](#3-monorepo-structure)
4. [Bazel Rules](#4-bazel-rules)
5. [External Repos: Just Another legend_library](#5-external-repos-just-another-legend_library)
6. [How the Compiler Adapts](#6-how-the-compiler-adapts)
7. [End-to-End Walkthrough](#7-end-to-end-walkthrough-what-happens-when-you-change-a-class)
8. [Rebuild Optimization & Breaking Changes](#8-rebuild-optimization--breaking-changes)
9. [Experimental Validation](#9-experimental-validation)
10. [Developer Experience](#10-developer-experience)
11. [Implementation Plan](#11-implementation-plan)
- [Appendix](#appendix)

---

## 1. Why Bazel?

The original proposal builds custom infrastructure for caching, dependency resolution, diamond resolution, wavefront validation, sparse checkout, and CLI tooling. **Bazel provides all of this for free.** What remains is the Pure-specific domain logic: element serialization and compatibility checking.

| Custom Code (Original Proposal) | Bazel Equivalent | Status |
|---|---|---|
| Content-addressed parse cache | Bazel action cache (input hash to cached output) | **Free** |
| Wavefront.yaml (validated version set) | Bazel's resolved dep graph (`MODULE.bazel`) | **Free** |
| Diamond resolution | Bazel enforces single version per target | **Free** |
| Transitive closure computation | Bazel's built-in dep resolution | **Free** |
| Sparse checkout / download | Bazel fetches only what actions need | **Free** |
| `legend compile/publish/sync` CLI | `bazel build / test / run` | **Free** |
| CI validation ("build the world") | `bazel build //...` | **Free** |
| Parallel builds | Bazel parallelizes independent targets | **Free** |
| Remote caching | Bazel remote cache (GCS, S3, disk) | **Free** |
| Element serialization | Still needed (one file per PackageableElement) | ~400 lines |
| Upcast compatibility check | Still needed as domain-specific | ~100 lines |

**Bazel eliminates ~70% of the custom code** from the original proposal. The per-element file approach eliminates the shape extraction step entirely.

Given Bazel handles the infrastructure layer, the core design decision becomes the artifact boundary.


---

## 2. The Key Insight: Per-Element Files, Not Header/Impl Split

The original approach proposed two outputs per project: a shapes file (like a C++ `.h`) and a full artifact (like a `.cpp`). **We can do better: no separate shape extraction at all.** Instead, every `PackageableElement` gets its own self-contained file.

A `PackageableElement` is Legend's fundamental metamodel unit -- every class, enum, association, function, mapping, database, runtime, connection, and service is one. During a build, the compiler automatically serializes each to its own output file:

```
legend_library(name = "products")
        |
        +-- elements/                          <-- one file per PackageableElement
            +-- products__Instrument.json      <-- class: properties, supers, constraints
            +-- products__InstrumentType.json   <-- enum: values
            +-- products__Instrument_Sector.json <-- association: ends
            +-- products__activeInstruments.json <-- function: signature + body
            +-- products__ProductMapping.json    <-- mapping: full definition
            +-- products__ProductDB.json         <-- database: tables, joins
            +-- products__ProductRuntime.json    <-- runtime: mapping + connection refs
```

**No shape extraction step.** Each element file IS self-describing -- it contains everything about that element. The consumer decides what to load:

- **For compilation**: downstream loads only model elements (classes, enums, associations, function signatures). Mappings, databases, runtimes are never loaded -- they're implementation details invisible to the compiler.
- **For runtime**: test/execution additionally loads mapping/database/runtime/connection elements.
- **For Bazel**: each file is a separate input. `unused_inputs_list` tracks exactly which elements were referenced. Change to an unused element = cache hit.

This eliminates:
- `ShapeExtractor` tool (no separate shape extraction step)
- `shapes.json` vs `.legend` artifact distinction (one format: element files)
- Shape serialization/deserialization code (~200 lines gone)
- The entire "shapes/impl split" concept

**Element categories:**

| Category | Elements | Needed for compilation? | Needed for runtime? |
|---|---|---|---|
| **Model** | Class, Enum, Association | Yes (type resolution) | Yes |
| **Functions** | Function (signature) | Yes (type-checking calls) | Yes (body for execution) |
| **Extension** (derived-only) | `Extension` with purely derived properties | Yes (target class only) | Yes |
| **Extension** (shadow-mapped) | `Extension` with store-bound properties | Yes (target class + consumer's own shadow store) | Yes |
| **Store** | Database, Table, Join | No | Yes (SQL generation) |
| **Mapping** | Mapping, PropertyMapping | No | Yes (query routing) |
| **Runtime** | Runtime, Connection | No | Yes (execution) |
| **Service** | Service | No | Yes (service execution) |

Downstream compilation only touches the **Model + Function + Extension (derived-only)** elements. A mapping change in `refdata` doesn't touch any model element files, so zero downstream rebuilds -- without any shape extraction step.

> **Caveat: cross-project joins.** The table above assumes the target state where cross-project joins use XStore model joins (Association + local properties). In that model, Store and Mapping are truly runtime-only. However, many existing projects use Database/Mapping `include` for cross-project joins — a leaky abstraction that pulls Store and Mapping into compile scope. Legend-lite **must support both** approaches: legacy includes (where Store/Mapping become compile deps) and model joins (where they don't). New projects should use XStore model joins exclusively. See [Cross-Project Joins](./CROSS_PROJECT_JOINS.md) for the full analysis, migration path, and legend-lite compiler design.
>
> **Recommended migration step:** publish a `DataSpace` alongside each mapping. Consumers `include` the DataSpace instead of the raw mapping, giving the data owner a clean API surface while keeping the underlying mapping as an implementation detail. Once all consumers move to DataSpace-based includes, the raw mapping can be restructured without downstream impact. This is an incremental step toward the full model-join target state where no mapping includes are needed at all.
>
> **Caveat: Extensions.** Legend-lite adds a new packageable element — `Extension` — enabling consumer projects to augment foreign classes with derived or (tactically) shadow-mapped properties without modifying the dependency. Derived-only Extensions are cleanly model-tier. Shadow-mapped Extensions reintroduce a local impl-time dep on the consumer's own shadow store but NOT on any foreign store at compile time; foreign coupling is deferred to deploy-time validation via the compile-as-validation safety model. See [Cross-Project Joins §8-§9](./CROSS_PROJECT_JOINS.md#8-consumer-side-class-extensions-a-trait-style-escape-hatch) for the full mechanism, visibility rules, and safety guarantees.

**When are implementation elements needed?** Compile actions only load model elements; implementation elements are inputs to `legend_test` actions only:

```
Compile time (TypeChecker):
  "Does products::Instrument have a property 'ticker' of type String?"
  --> Load products__Instrument.json (class element only)

Runtime (MappingResolver + PlanGenerator + Execute):
  "What table does products::Instrument map to? What columns? What joins?"
  --> Load products__ProductMapping.json + products__ProductDB.json
```

**Lazy loading for IDE/LSP:** Instead of loading all dependency elements upfront, the model builder lazy-loads on first reference via `addDepElementDir()` (see §6 for details). A project with 10,000 dependency classes but only using 50 loads 50 element files on demand. First-keystroke latency is proportional to what you USE, not what EXISTS.


---

## 2.5 Project-Qualified FQNs (Internal Only)

Legend's grammar doesn't enforce global FQN uniqueness. Two projects can declare the same source-level FQN — deliberately (legacy projects with known duplicates) or accidentally (someone copies `refdata::Sector` into their own project without realizing it shadows an upstream class). There is no way to prevent this at the source level, so **every element must be identified internally by `(projectId, fqn)` — not by `fqn` alone**.

**This is a compiler-internal mechanism. Users never see it, never type it, and the grammar never accepts it.** The project id is a separate dimension carried alongside the source FQN, not a new namespace segment. Rendered form inside the compiler and on disk: `<projectId>__<sourceFqn>`. The `__` separator is deliberately not valid Pure FQN syntax, which is exactly what keeps this form out of the language surface.

- **Source:** `Class refdata::Sector { ... }` — unchanged, forever
- **Declared dependencies:** project-level in `BUILD.bazel` (`deps = ["//projects/refdata:lib"]`) — Bazel tracks these
- **Internal canonical form:** `refdata__refdata::Sector` (declaring project: `refdata`, source FQN: `refdata::Sector`)
- **On-disk filename:** `refdata__refdata__Sector.json`
- **LSP / hover / error messages:** strip `<projectId>__` before display — user always sees `refdata::Sector`
- **Second project copying the same source FQN** (`other__refdata::Sector`) serializes to a distinct file — no collision, no silent shadowing

The qualifier is **always applied**, including for projects whose source FQN root happens to equal their project id. We cannot rely on that convention because it isn't enforced, and a downstream project that copies one of our classes breaks the invariant without our knowledge.

### Where project-qualification is load-bearing

- **Filenames.** The per-element filesystem in §6 only works because filenames are globally unique.
- **NameResolver output.** Every resolved reference carries `(projectId, fqn)`, so lazy load opens the right file even when two projects define the same source FQN.
- **Back-reference fragment keys (§6 Tier 1).** Cross-project contributions to the same target class must agree on the canonical key to merge.
- **Resolved AST references.** A walk like `$x.sector.name` expands to canonical form at serialization time — no import context needed at load.

### What doesn't change

- **Authoring.** Users write `Class refdata::Sector`. No new syntax, no project-id ceremony in sources.
- **Grammar.** The parser does not recognize `__` — you can't type the canonical form, and you can't accidentally refer to another project's class by it.
- **Runtime type identity.** A class is identified by `(declaringProjectId, sourceFqn)`. Wherever `refdata::Sector` is referenced from project `refdata`, it's the same type.
- **Display surface.** Every UI/LSP path strips the project-id prefix — canonical form is an internal detail.

### Implementation

- **`NameResolver`** is the single place where project id gets prepended. Given a source reference and the declaring project context, it resolves short/unqualified/imported names against the project's declared Bazel deps (first match wins, ambiguity is a compile error) and emits `(resolvedProjectId, resolvedFqn)` for every reference. This is the canonical form for everything downstream.
- **`ElementSerializer`** uses canonical form for filenames and for every FQN it emits into resolved AST / back-reference fragments. Mechanical once `NameResolver` hands it the pair.
- **LSP / display layer** has a single `stripProjectId()` helper applied at the presentation boundary — errors, hovers, go-to-def labels, rename previews.
- Bazel already tracks project-level `deps`; no new dependency-declaration syntax.

---

## 3. Monorepo Structure

legend-lite platform code and all new Legend projects live in ONE Bazel-managed monorepo. External repos (1000s of existing ones) are fetched by Bazel as source and built with the same `legend_library` rule — no separate extraction, no special "external" concept.

```
legend/                                    # THE monorepo
+-- MODULE.bazel                           # Bazel module definition + external repo pins
+-- BUILD.bazel                            # root build file
+-- external_repos.bzl                     # git_repository declarations for external Legend repos
|
+-- platform/                              # legend-lite engine (what exists today)
|   +-- engine/
|   |   +-- BUILD.bazel                    # java_library for the compiler/planner
|   |   +-- src/main/java/...             # current engine code
|   +-- nlq/
|   |   +-- BUILD.bazel
|   |   +-- src/...
|   +-- pct/
|       +-- BUILD.bazel
|       +-- src/...
|
+-- tools/                                 # Bazel rule definitions + CLI tools
|   +-- BUILD.bazel
|   +-- legend.bzl                         # legend_library, legend_test rules
|   +-- element_serializer/                # serializes PackageableElements to JSON
|   |   +-- BUILD.bazel
|   |   +-- ElementSerializer.java
|   +-- compat_checker/                    # upcast compatibility checks
|       +-- BUILD.bazel
|       +-- ElementCompat.java
|
+-- projects/                              # NEW Legend projects (teams create dirs here)
|   +-- trading/
|   |   +-- BUILD.bazel                    # legend_library(deps = [...])
|   |   +-- src/
|   |       +-- model/Trade.pure
|   |       +-- mapping/TradingMapping.pure
|   |       +-- store/TradingDB.pure
|   +-- risk/
|   |   +-- BUILD.bazel
|   |   +-- src/*.pure
|   +-- products/
|   |   +-- BUILD.bazel
|   |   +-- src/*.pure
|   +-- refdata/
|       +-- BUILD.bazel
|       +-- src/*.pure
|
+-- external/                              # BUILD overlays for external repos (no source files)
|   +-- com_gs_legacy_products/
|   |   +-- BUILD.overlay                  # legend_library applied to fetched repo
|   +-- com_gs_legacy_risk/
|   |   +-- BUILD.overlay
|   +-- ... (one subdirectory per external repo)
|
+-- .github/
    +-- workflows/
        +-- ci.yml                         # bazel build //... && bazel test //...
```

External repos are pinned via `git_repository` in `MODULE.bazel` and wrapped with the same `legend_library` rule via BUILD overlays in `external/`. No special extraction pipeline — see §5 for the full external repo workflow.

**Scaling external repo pins.** At 1000+ external repos, hand-editing `external_repos.bzl` becomes untenable. Generate it from a flat manifest (`external_repos.yaml`) via a repository rule or codegen step. The YAML manifest stays human-readable and diff-friendly; the generated `.bzl` is never edited directly. This keeps PRs reviewable and enables batch operations (e.g., "bump all repos under org `gs-legacy/`").

### Why One Monorepo?

- **Single `bazel build //...`** validates the entire world -- no custom wavefront CI
- **Single dep graph** -- Bazel resolves all transitive deps, detects cycles, enforces single-version
- **Shared action cache** -- build `products` once, every consumer gets a cache hit
- **Atomic cross-project changes** -- rename a class in `refdata`, fix all consumers in one commit
- **IDE support** -- IntelliJ/VS Code Bazel plugins give cross-project navigation for free
- **New team onboarding** -- `mkdir projects/my-team` and write a BUILD.bazel

---

## 4. Bazel Rules

### Element Discovery: Module Extension

Bazel requires all output filenames to be declared **before** the compiler runs (analysis phase). But element names (e.g., `refdata::Sector`) are only discovered when the compiler parses `.pure` files (execution phase). This chicken-and-egg problem is solved by a **module extension** that scans `.pure` files with a fast regex **before** analysis begins.

Module extensions (and repository rules) run in a special pre-analysis phase where they CAN read file contents — unlike rule implementations which can only see file paths.

```python
# tools/legend_ext.bzl — Module extension for element discovery

_ELEMENT_PATTERN = r"^(Class|Enum|Association|Mapping|Function|Database|Runtime|Connection|Service|Profile)\s+([\w:]+)"

def _scan_pure_files(mctx, src_dir):
    """Scan .pure files and extract PackageableElement FQNs via regex."""
    elements = []
    for f in src_dir.readdir():
        if str(f).endswith(".pure"):
            content = mctx.read(f)
            for line in content.splitlines():
                match = _regex_match(_ELEMENT_PATTERN, line)
                if match:
                    elements.append(match.group(2))
        elif f.is_dir:
            elements.extend(_scan_pure_files(mctx, f))
    return elements

def _legend_impl(mctx):
    # Scan local projects
    for tag in mctx.modules[0].tags.project:
        src_dir = mctx.path(Label("@@//:MODULE.bazel")).dirname.get_child(tag.path)
        elements = _scan_pure_files(mctx, src_dir)
        mctx.file(tag.name + "/elements.bzl", "ELEMENTS = " + repr(elements))

    # Scan external repos (fetched via git_repository)
    for tag in mctx.modules[0].tags.external:
        repo_root = mctx.path(Label("@" + tag.repo + "//:BUILD.overlay")).dirname
        src_dir = repo_root.get_child(tag.src_path if tag.src_path else "src")
        elements = _scan_pure_files(mctx, src_dir)
        mctx.file(tag.repo + "/elements.bzl", "ELEMENTS = " + repr(elements))

_project_tag = tag_class(attrs = {
    "name": attr.string(mandatory = True),
    "path": attr.string(mandatory = True),  # e.g., "projects/refdata/src"
})
_external_tag = tag_class(attrs = {
    "repo": attr.string(mandatory = True),   # e.g., "com_gs_legacy_products"
    "src_path": attr.string(default = "src"),
})

legend = module_extension(
    implementation = _legend_impl,
    tag_classes = {"project": _project_tag, "external": _external_tag},
)
```

Registration in `MODULE.bazel`:

```python
legend = use_extension("//tools:legend_ext.bzl", "legend")

# Local projects
legend.project(name = "refdata", path = "projects/refdata/src")
legend.project(name = "products", path = "projects/products/src")
legend.project(name = "trading", path = "projects/trading/src")

# External repos
legend.external(repo = "com_gs_legacy_products")
legend.external(repo = "com_gs_refdata")

use_repo(legend, "refdata", "products", "trading", "com_gs_legacy_products", "com_gs_refdata")
```

**Performance:** Each project's scan only re-runs when `.pure` files in that project change (~5ms for 50 files). External repos only re-scan on commit bump. No global re-scanning.

**At monorepo scale:** 1000 projects × ~5ms = ~5s analysis-phase overhead on a cold `bazel build //...`. This is a worst-case number — in practice, `mctx.read()` results are cached per-project and only invalidated when that project's `.pure` files change, so steady-state overhead is proportional to the number of *changed* projects, not total projects. Worth validating with a scale test at target project count (see §9 Experiment 4).

**Function overloads:** Pure allows multiple functions with the same FQN but different parameter signatures. The regex deduplicates by FQN — all overloads of `refdata::transform` produce a single `refdata__transform.json` file containing all overloads. This mirrors how `PureFunctionRegistry` already groups overloads by FQN. Changing any overload changes the file, which is the correct semantic — overloads are a cohesive unit.

**Regex safety:** The regex only matches top-level element **definitions** (keyword at start of line + FQN). It does NOT need to find references, property types, imports, or anything inside element bodies. Pure's grammar guarantees all `PackageableElement` definitions start with an uppercase keyword (`Class`, `Enum`, `Mapping`, etc.). Body-level keywords like `include` (mapping/database includes) are lowercase and don't match. No false positives.

**Cycle handling:** Bazel forbids dependency cycles between targets. Pure also forbids element-level cycles (no circular inheritance or imports). However, two projects might accidentally have mutual references (A uses B's class, B uses A's class) — this is a project organization smell, not a language feature. Bazel catches it immediately with a clear error. The fix: merge the two projects into a single `legend_library` target, since they are logically one unit. The module extension could optionally detect cross-project reference cycles during scanning and warn or auto-merge.

### `legend_library` -- The Core Build Rule

Every Legend project is a `legend_library` target. It declares **one output file per element** (names from the module extension), enabling per-element Bazel cache tracking via `unused_inputs_list`.

**Experimentally verified:** `unused_inputs_list` works with individually declared files but NOT with files inside tree artifacts (directories are opaque to Bazel). See `experiments/tree_artifact_test/` for the proof.

```python
# tools/legend.bzl

LegendElementsInfo = provider(
    doc = "Per-PackageableElement files for this project",
    fields = {
        "element_files": "depset of individual element .json files",
    },
)

def _legend_library_impl(ctx):
    srcs = ctx.files.srcs
    compiler = ctx.executable._legend_compiler

    # Collect all dep element files (individually declared — NOT tree artifacts)
    dep_element_files = []
    for d in ctx.attr.model_deps + ctx.attr.impl_deps:
        dep_element_files.extend(d[LegendElementsInfo].element_files.to_list())

    # Declare one output file per element (names from module extension scan)
    output_files = []
    for fqn in ctx.attr.elements:
        filename = fqn.replace("::", "__") + ".json"
        output_files.append(ctx.actions.declare_file(
            ctx.label.name + "_elements/" + filename))

    unused = ctx.actions.declare_file(ctx.label.name + ".unused_inputs")

    # SINGLE ACTION: parse, compile, serialize each element to its own file
    ctx.actions.run(
        executable = compiler,
        arguments = ["compile",
                     "--elements-dir", output_files[0].dirname if output_files else "",
                     "--track-unused", unused.path,
                     "--srcs"] + [f.path for f in srcs]
                     + ["--dep-elements"] + [f.path for f in dep_element_files],
        inputs = srcs + dep_element_files,
        outputs = output_files + [unused],
        unused_inputs_list = unused,       # Bazel skips rebuild if unused deps change
        mnemonic = "LegendCompile",
    )

    return [
        LegendElementsInfo(
            element_files = depset(
                direct = output_files,     # individual files — per-element tracking!
                transitive = [d[LegendElementsInfo].element_files
                              for d in ctx.attr.model_deps + ctx.attr.impl_deps],
            ),
        ),
        DefaultInfo(files = depset(output_files)),
    ]

legend_library = rule(
    implementation = _legend_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".pure"]),
        "elements": attr.string_list(),    # from module extension scan
        "model_deps": attr.label_list(providers = [LegendElementsInfo]),  # classes/enums/associations only
        "impl_deps": attr.label_list(providers = [LegendElementsInfo]),   # + mappings/stores/runtimes
        "_legend_compiler": attr.label(
            default = "//tools/artifact_compiler",
            executable = True,
            cfg = "exec",
        ),
    },
)
```

**`model_deps` vs `impl_deps`:** Two dep attributes make the compile-time contract explicit. `model_deps` is the target state — the compiler only loads model elements (Class, Enum, Association, Function) from these deps, which is sufficient for XStore model joins. `impl_deps` is for legacy Database/Mapping `include` patterns that pull Store and Mapping elements into compile scope. Both produce identical `LegendElementsInfo` depsets, but the distinction is visible in BUILD files and code review — making it easy to track migration from legacy includes to model joins. New projects should use `model_deps` exclusively. See [Cross-Project Joins](./CROSS_PROJECT_JOINS.md) for the full analysis.

**Why this works:** The module extension discovers element FQNs via regex before analysis. The rule declares one output `File` per element. `unused_inputs_list` enables per-element rebuild avoidance — see §8 for the full invalidation semantics.

**Fallback:** If the module extension is too complex for initial implementation, use one `declare_file` per `.pure` source file (derived from `ctx.files.srcs` filenames). This gives per-source-file granularity with zero tooling. Upgrade to per-element later.

### `legend_test` -- Query Execution Tests

```python
def _legend_test_impl(ctx):
    # Tests load ALL element files (model + mappings + databases + runtimes)
    # because they actually execute queries
    all_elements = ctx.attr.library[LegendElementsInfo].element_files
    # ... run queries against element files using legend-lite engine ...

legend_test = rule(
    implementation = _legend_test_impl,
    attrs = {
        "queries": attr.label_list(allow_files = [".pure"]),
        "library": attr.label(providers = [LegendElementsInfo]),
        "deps": attr.label_list(providers = [LegendElementsInfo]),
    },
    test = True,
)
```

**Eager loading tradeoff:** Tests load ALL element files (model + implementation) because they execute actual queries. This means any mapping or store change invalidates all tests that transitively depend on that project — no `unused_inputs_list` filtering at the test level.

**Future work: per-test `unused_inputs_list`.** At v2, the test runner could extract the set of (mapping, store, runtime) elements actually touched by each test's query plan and emit those as the used set. A mapping change in one class would then only re-run tests whose query plans route through that mapping. Not on the v1 critical path — test execution time is dominated by query execution, not element loading.

### Example BUILD Files

```python
# projects/refdata/BUILD.bazel
load("//tools:legend.bzl", "legend_library")
load("@refdata//:elements.bzl", "ELEMENTS")   # auto-generated by module extension

legend_library(
    name = "refdata",
    srcs = glob(["src/**/*.pure"]),
    elements = ELEMENTS,
    visibility = ["//visibility:public"],
)
```

```python
# projects/products/BUILD.bazel
load("//tools:legend.bzl", "legend_library")
load("@products//:elements.bzl", "ELEMENTS")

legend_library(
    name = "products",
    srcs = glob(["src/**/*.pure"]),
    elements = ELEMENTS,
    model_deps = ["//projects/refdata"],
    visibility = ["//visibility:public"],
)
```

```python
# projects/trading/BUILD.bazel -- depends on monorepo + external repos (same syntax!)
load("//tools:legend.bzl", "legend_library", "legend_test")
load("@trading//:elements.bzl", "ELEMENTS")

legend_library(
    name = "trading",
    srcs = glob(["src/**/*.pure"]),
    elements = ELEMENTS,
    model_deps = [
        "//projects/products",                    # monorepo dep (model elements only)
        "//projects/risk",                        # monorepo dep
        "@com_gs_legacy_products//:legacy_products",  # external dep (same rule!)
    ],
    visibility = ["//visibility:public"],
)

legend_test(
    name = "trading_test",
    queries = glob(["test/**/*.pure"]),
    library = ":trading",
)
```

**No manual element lists.** The `ELEMENTS` list is auto-generated by the module extension's regex scan. When you add a new class to a `.pure` file, the module extension detects it on next build (~5ms). No tool to run, no manifest to update.

**External and monorepo deps are interchangeable.** Both are `legend_library` targets producing per-element files. The only difference is where the `.pure` source files come from (local vs fetched).

---

## 5. External Repos: Just Another `legend_library`

External repos (1000s of existing ones that can't be modified) are treated identically to monorepo projects. Bazel fetches their `.pure` source files via `git_repository`, and a BUILD overlay wraps them in the same `legend_library` rule.

### How It Works

```
Monorepo MODULE.bazel says:
  git_repository(name = "com_gs_legacy_products", commit = "abc123", ...)

Developer runs: bazel build //projects/trading
        |
        v
Bazel sees trading depends on @com_gs_legacy_products
        |
        v
Bazel fetches legacy-products repo at commit abc123 (cached after first fetch)
        |
        v
Bazel builds @com_gs_legacy_products//:legacy_products using legend_library
  → parses .pure source files → produces per-element .json files in bazel-out/
        |
        v
Trading's compile action uses legacy_products' element files as inputs
  → same unused_inputs_list tracking as any other dep
```

**Same pipeline as monorepo projects. No extraction, no registry, no webhook.**

### Registering a New External Repo (One-Time)

```bash
# 1. Create the external repo directory
mkdir -p external/com_gs_legacy_products

# 2. Write the BUILD overlay (one-time, ~5 lines)
cat > external/com_gs_legacy_products/BUILD.overlay << 'EOF'
load("@legend//tools:legend.bzl", "legend_library")
load("@com_gs_legacy_products_elements//:elements.bzl", "ELEMENTS")  # auto-generated

legend_library(
    name = "legacy_products",
    srcs = glob(["src/**/*.pure"]),
    elements = ELEMENTS,
    deps = ["@com_gs_refdata//:refdata"],
    visibility = ["//visibility:public"],
)
EOF

# 3. Add git_repository to external_repos.bzl
cat >> external_repos.bzl << 'EOF'
git_repository(
    name = "com_gs_legacy_products",
    remote = "https://github.com/gs/legacy-products.git",
    commit = "abc123def456",
    build_file = "//external/com_gs_legacy_products:BUILD.overlay",
)
EOF

# 4. Register with module extension for element scanning (in MODULE.bazel)
#    legend.external(repo = "com_gs_legacy_products")

# 5. Add to your project's deps and build
#    deps = [..., "@com_gs_legacy_products//:legacy_products"]
bazel build //projects/trading

# 6. Commit
git add external/com_gs_legacy_products/ external_repos.bzl MODULE.bazel
git commit -m "Add external dep: com.gs.legacy_products"
git push
```

### Bumping an External Repo Version

Bazel requires **pinned commits** for reproducible builds. Three options for keeping external repos up to date:

**Option A: Bazel-native version checking (recommended — experimentally proven)**

Uses the same module extension mechanism that powers element discovery. A `version_check` extension runs `git ls-remote` to detect outdated commits, generating a status report as a build target. See §9 Experimental Validation.

```python
# MODULE.bazel — register repos to monitor
vc = use_extension("//tools:version_check.bzl", "version_check")
vc.repo(name = "com_gs_refdata", remote = "https://github.com/gs/refdata.git",
        current_commit = "abc123...", branch = "main")
```

```yaml
# .github/workflows/bump_external.yml — runs nightly
jobs:
  bump:
    steps:
      - uses: actions/checkout@v4
      - name: Check for outdated deps (Bazel-native)
        run: bazel build //:check_versions
      - name: Create PR if outdated
        run: |
          # Read status from Bazel-generated report
          # Update commit hashes in MODULE.bazel
          # PR triggers CI → bazel build //... → if it passes, safe to merge
```

**Zero custom Python scripts.** Bazel itself runs `git ls-remote`, compares commits, generates the report. Cached like any other action.

**Option B: Traditional CI script**

```yaml
# .github/workflows/bump_external.yml — runs nightly or on webhook
on:
  schedule:
    - cron: '0 2 * * *'
  repository_dispatch:
    types: [legend-repo-updated]

jobs:
  bump:
    steps:
      - uses: actions/checkout@v4
      - name: Check for new commits in external repos
        run: python tools/bump_external_versions.py external_repos.bzl
      - name: Create PR if anything changed
        run: |
          if ! git diff --quiet external_repos.bzl; then
            git checkout -b auto/bump-externals
            git add external_repos.bzl
            git commit -m "Bump external repo versions"
            git push -u origin auto/bump-externals
            gh pr create --title "Bump external repos" --body "Auto-generated"
          fi
```

Both options give you: **automatic updates, gated by CI**. If an external repo makes a breaking change, the PR fails and a human investigates.

**Option C: Manual bump**

```bash
# Update the commit hash in external_repos.bzl
sed -i 's/commit = "abc123"/commit = "def456"/' external_repos.bzl

# Build — Bazel re-fetches the repo, rebuilds its elements,
# and only rebuilds downstream projects whose USED elements changed.
bazel build //...
```

**What happens when you bump a version:**

```
1. Bazel sees commit hash changed in external_repos.bzl
2. Bazel re-fetches the external repo at the new commit
3. Module extension re-scans .pure files (~5ms), regenerates elements.bzl if needed
4. Bazel re-runs legend_library for that repo (parses .pure → individual element files)
5. Element files that CHANGED → downstream consumers with those in USED set rebuild
6. Element files that are UNCHANGED → byte-identical output → no downstream impact
7. Element files that are NEW → not in anyone's input set → no downstream impact
8. Element files in downstream's unused_inputs_list → skip rebuild even if changed
```

No polling, no "detect what changed" — Bazel handles it all from the pinned commit hash. Per-element `unused_inputs_list` tracking means even within a changed project, only consumers that reference the specific changed elements rebuild.

### Outbound: Monorepo Projects Consumed by External Repos

External repos that need to depend on monorepo projects can use the same mechanism in reverse. The monorepo is itself a `git_repository` from the external repo's perspective, or element files can be published to a lightweight store:

```yaml
# .github/workflows/publish_elements.yml
on:
  push:
    branches: [main]
    paths: ['projects/*/src/**/*.pure']

jobs:
  publish:
    steps:
      - uses: actions/checkout@v4
      - name: Build all projects
        run: bazel build //projects/...
      - name: Publish element files to registry
        run: |
          for project in projects/*/; do
            PKG=$(basename $project)
            tar czf /tmp/$PKG-elements.tar.gz -C bazel-bin/$project ${PKG}_elements/
          done
          # Upload to S3, Artifactory, or a Git-based element registry
```

With the Bazel shape defined — module extension for discovery, `legend_library` for per-element output, `git_repository` for externals — the remaining question is how the compiler consumes and produces these files.

---

## 6. How the Compiler Adapts

The per-element architecture requires the compiler to adapt in several areas: name resolution for cross-project imports, lazy element loading, string-based property types, resolved AST serialization for executable bodies, pre-parsed M2M/XStore expressions, lazy superclass resolution, and a three-tier back-reference system (target-side fragments, caller-side manifests, deferred IDE indexer). Validation is exposed as an opt-in `validateElement` primitive invoked by Bazel and LSP at different scopes. The Bazel and LSP paths share the compiler core and element-file format, differing only in entry points and caching — matching how javac/rustc/tsc split batch vs language service. An eager-loading path is also provided for `legend_test` actions. See the Complete Dependency Reference Matrix below for every cross-element reference type and how the plan addresses it.

### Complete Dependency Reference Matrix

Every cross-element reference in legend-lite falls into one of two mechanical categories based on how the dependency is recorded:

- **Category A — Syntactic FQN refs**: appear directly in element record fields, serialized as strings, FQN-canonicalized by `NameResolver` before serialization.
- **Category B — Expression-embedded refs**: appear inside function/service/derived-property/constraint/M2M expression bodies, canonicalized via resolved-AST serialization.

This table enumerates every reference type and maps it to the plan element that makes it work across project boundaries.

**Category A — Syntactic FQN refs**

| Reference | Carrier | Plan coverage |
|---|---|---|
| Superclass | `ClassDefinition.superClasses: List<String>` | Lazy Superclass Resolution |
| Class property type (primitive or class) | `PropertyDefinition.type` | `typeFqn` refactor (Step 2.5) |
| Derived property type + parameter types | `DerivedPropertyDefinition`, `ParameterDefinition.type` | `typeFqn` refactor |
| Function return type + parameter types | `FunctionDefinition.returnType`, parameter `type` | `typeFqn` refactor |
| Association end target classes | `AssociationEndDefinition.targetClass` | Natural (already FQN, canonicalized today) |
| Mapping class + source class | `ClassMappingDefinition.className / sourceClassName` | Natural (canonicalized today) |
| Mapping include | `MappingInclude.includedMappingPath` | Expand `NameResolver` |
| Store substitutions inside a mapping include | `MappingInclude.StoreSubstitution(originalStore, substituteStore)` | Expand `NameResolver` |
| Database include | `DatabaseDefinition.includes: List<String>` | Expand `NameResolver` |
| Service mapping / runtime refs | `ServiceDefinition.mappingRef`, `ServiceDefinition.runtimeRef` | Expand `NameResolver` |
| Runtime mapping list + connection bindings | `RuntimeDefinition.mappings`, `connectionBindings` values | Expand `NameResolver` |
| Connection store ref | `ConnectionDefinition.storeName` | Expand `NameResolver` |
| Enumeration mapping enum type | `EnumerationMappingDefinition.enumType` | Expand `NameResolver` |
| Profile refs in stereotypes / tagged values (on classes, properties, derived properties, functions) | `StereotypeApplication.profileName`, `TaggedValue.profileName` | Expand `NameResolver` |

**Category B — Expression-embedded refs** (resolved AST path)

| Reference | Carrier | Plan coverage |
|---|---|---|
| Property access chains (`$x.sector.name`) | resolved AST nodes in bodies | Resolved AST + lazy `findClass` / `findProperty` |
| Function-to-function calls (`myproj::helper(...)`) | `AppliedFunction.function` inside bodies | Resolved AST (`NameResolver` canonicalizes this today) |
| Class literal refs (`Trade.all()`) | `AppliedFunction` with class arg | Resolved AST |
| Derived property expression body | stored text today → resolved AST | Resolved AST for Executable Bodies |
| Class constraint body | stored text today → resolved AST | Resolved AST for Executable Bodies |
| Service query body | `ServiceDefinition.functionBody` | Resolved AST for Executable Bodies |
| M2M property expressions | `ClassMappingDefinition.m2mPropertyExpressions: Map<String, String>` | Pre-Parsed M2M and XStore Expressions |
| XStore cross expression | `AssociationPropertyMapping.crossExpression: String` | Pre-Parsed M2M and XStore Expressions |
| Mapping relational filter expressions | `RelationalOperation` (already structured) | Natural; any class/DB refs inside follow Category A rules |
| Mapping join chain refs (`@OrdCust`) | join name scoped to the owning Database | Natural via Database includes |

The rest of §6 explains the specific compiler changes that implement this matrix. Validation (`validateElement`) closes the loop by running each element's checker over the FQN-resolved model, catching any remaining missing refs at build time rather than at query time.

### Name Resolution with Dependency Elements (Java-Style)

The key design question: when a Pure source file writes `import refdata::*` and then uses `Sector`, how does the compiler resolve that to `refdata::Sector` and find the right element file?

**Answer: the same way Java has done it for 30 years.** The compiler constructs candidate FQNs from imports and checks if the corresponding element file exists on disk. No pre-scanning, no manifest, no global symbol index.

#### How it works today (single-project, no deps)

The current `ImportScope.resolve()` takes a simple name + `knownTypes` set:

```java
// Current code in ImportScope.resolve():
for (String pkg : wildcardImports) {
    String candidate = pkg + "::" + name;       // constructs "refdata::Sector"
    if (knownTypes.contains(candidate)) {        // checks in-memory set
        matches.add(candidate);
    }
}
```

This works when all definitions are in one `addSource()` call, because Phase 0 interns all FQNs into the symbol table before name resolution runs. But with cross-project deps, the dependency FQNs are not in `knownTypes`.

#### How it works with lazy loading (the change)

Instead of checking an in-memory set, check if the element file exists on disk:

```java
// Updated ImportScope.resolve() — or a new overload used by PureModelBuilder:
for (String pkg : wildcardImports) {
    String candidate = pkg + "::" + name;       // constructs "refdata::Sector"
    if (knownTypes.contains(candidate)           // local symbols (same as before)
            || elementFileExists(candidate)) {   // NEW: check dep element dirs
        matches.add(candidate);
    }
}
```

Where `elementFileExists` is:

```java
private boolean elementFileExists(String fqn) {
    String filename = fqn.replace("::", "__") + ".json";
    for (Path depDir : depElementDirs) {
        if (Files.exists(depDir.resolve(filename))) {
            return true;
        }
    }
    return false;
}
```

This is exactly how `javac` resolves `import java.util.*` + `List` — it constructs `java/util/List.class` and checks if that file exists on the classpath.

#### Why this scales to millions of files

| Approach | 30K files | 1.5M files |
|---|---|---|
| **Scan all filenames into memory** | ~5ms | ~3-7 seconds + 90MB RAM |
| **File-exists per candidate (Java-style)** | ~0.3ms | ~0.3ms |

The file-exists approach scales with **names actually used** (typically ~300 per project), not with total dependency count. Each `Files.exists()` check is ~1-5μs (kernel dentry cache hit). 20 imports × 15 simple names = 300 checks = ~0.3ms regardless of how many element files are in the directory.

### Lazy Element Loading

Once name resolution has resolved `Sector` → `refdata::Sector`, the model builder lazy-loads the element body on first access:

```java
// In PureModelBuilder -- lazy loading infrastructure

/** Directories containing dependency element files */
private final List<Path> depElementDirs = new ArrayList<>();
private final Set<String> loadedElements = new HashSet<>();

/**
 * Register a dependency element directory.
 * No elements are loaded — just records where to find them.
 */
public PureModelBuilder addDepElementDir(Path elementsDir) {
    depElementDirs.add(elementsDir);
    return this;
}

/**
 * findClass with lazy-load fallback.
 * Same pattern applies to findEnum, findAssociation, findFunction.
 */
@Override
public Optional<PureClass> findClass(String fqn) {
    var local = super.findClass(fqn);
    if (local.isPresent()) return local;

    // Cache miss — try lazy loading from dep element dirs
    if (!loadedElements.contains(fqn)) {
        loadElementIfExists(fqn);
        return super.findClass(fqn);  // retry after loading
    }
    return Optional.empty();
}

private void loadElementIfExists(String fqn) {
    loadedElements.add(fqn);
    String filename = fqn.replace("::", "__") + ".json";
    for (Path depDir : depElementDirs) {
        Path file = depDir.resolve(filename);
        if (Files.exists(file)) {
            var element = ElementSerializer.deserialize(Files.readString(file));
            addElement(element);
            return;
        }
    }
}
```

The full flow for `import refdata::*` + `sector: Sector[1]`:

```
1. Parser produces PropertyDefinition(type="Sector")
2. NameResolver.resolve("Sector", knownTypes):
   → candidate = "refdata::Sector"
   → knownTypes.contains("refdata::Sector")? No (not a local def)
   → elementFileExists("refdata::Sector")? Check refdata__Sector.json → Yes ✓
   → resolved to "refdata::Sector"
3. resolveType("refdata::Sector"):
   → findClass("refdata::Sector") → cache miss
   → loadElementIfExists("refdata::Sector")
   → opens refdata__Sector.json, deserializes, calls addClass()
   → retry findClass → found ✓
```

No scanning. No manifest. No index. The filesystem is the index.

### Required Change: String-Based Property Types

**Problem discovered:** The current `resolveType()` in `PureModelBuilder` resolves property type strings to actual `PureClass` references **at class registration time** (during `addClass()`). When lazy-loading `Sector.json`, `resolveType("refdata::Region")` fails because Region hasn't been loaded yet:

```java
// Current resolveType() — called during addClass()
private Type resolveType(String typeName) {
    return switch (typeName) {
        case "String" -> PrimitiveType.STRING;
        // ... other primitives ...
        default -> {
            PureClass classType = idGet(classes, symbols.resolveId(typeName));
            if (classType != null) yield classType;
            // Region NOT loaded yet → throws!
            throw new IllegalStateException("Unknown type: " + typeName);
        }
    };
}
```

**Fix: store property types as FQN strings, not resolved references.** The TypeChecker already converts `Property.genericType()` back to an FQN string via `GenericType.fromType()` → `ClassType(qualifiedName)`. It only uses the string to produce `ClassType`, never the `PureClass` object's internal structure. So the round-trip through resolved `PureClass` references is unnecessary.

```java
// Property currently stores a resolved Type (PureClass/PrimitiveType/PureEnumType)
public record Property(String name, Type genericType, Multiplicity multiplicity, ...) {}

// Change to: store just the FQN string
public record Property(String name, String typeFqn, Multiplicity multiplicity, ...) {}
```

With this change:
- `addClass()` never needs to resolve property types → **no ordering dependency between classes**
- `resolveType()` becomes trivial: just pass through the string for non-primitives
- TypeChecker's `GenericType.fromType()` becomes `GenericType.fromTypeName(typeFqn)` — already exists
- Elements can be loaded in any order, one at a time, with zero dependency on what's already in the model
- The only time `findClass()` is called is during TypeChecker property access (e.g., `$t.sector`) which triggers lazy loading naturally

**Scope of change (revised estimate):** The `Property.genericType()` → `Type` reference is consumed throughout the compiler and planner. A realistic migration path:

1. Add `String typeFqn` field to `Property` alongside existing `genericType` (~20 lines in `Property.java`)
2. Incrementally migrate consumers across `TypeChecker.java`, `checkers/*` (~36 files), `PlanGenerator.java`, `MappingResolver.java`, `MappingNormalizer.java`, `GenericType.java` to read `typeFqn` instead of `genericType()` (~200-500 lines across ~30 files)
3. Flag day: remove `genericType` field, all consumers on `typeFqn`

**This is the largest single prerequisite for the Bazel migration.** Recommended approach: do this refactor in the main engine first (Step 2.5 in §11), validate with existing 2100+ tests, then wire into lazy loading. The `PureClass` record itself doesn't change — just what `Property` stores for its type.

### Per-Element Import Scopes (Optional Safety Net)

**Context:** `PureModelBuilder.addSource()` today merges every file's `ImportScope` into a single global scope (see `PureModelBuilder.java:62`, merge at `:181-194`). That's fine for whole-project builds but becomes ambiguous when element files load standalone: an element's body references names that were resolved in the original file's import context, not the current builder's.

With resolved-AST serialization (below), bodies carry FQNs directly and don't need import context at load time — this isn't a functional blocker.

**Optional hardening:** serialize the element's original `ImportScope` as a side field so the loader has a fallback when re-resolution is ever needed (e.g., loading an old-schema element that wasn't yet AST-resolved). ~10 lines in the serializer. Skip for v1; add in v2 if backwards-compat tests demand it.

### Transitive Loading Chains

With string-based property types, lazy loading chains resolve naturally:

```
Trading source: $t.sector.region.name

1. TypeChecker: $t.sector → type is ClassType("refdata::Sector")
   → findClass("refdata::Sector") → lazy-loads Sector.json → addClass() works (no resolveType needed!)
   → findProperty("sector") → returns Property(name="sector", typeFqn="refdata::Sector", ...)

2. TypeChecker: .sector → GenericType.fromTypeName("refdata::Sector") → ClassType("refdata::Sector")
   → findClass("refdata::Sector") → already loaded ✓
   → findProperty("region") → returns Property(name="region", typeFqn="refdata::Region", ...)

3. TypeChecker: .region → GenericType.fromTypeName("refdata::Region") → ClassType("refdata::Region")
   → findClass("refdata::Region") → lazy-loads Region.json
   → findProperty("name") → returns Property(name="name", typeFqn="String", ...)

4. TypeChecker: .name → GenericType.fromTypeName("String") → Primitive.STRING ✓
```

Each `findClass()` call triggers exactly one lazy load. No recursive loading chains during `addClass()`. The TypeChecker drives loading naturally through property access paths — it only loads classes it actually navigates into.

### Required Change: Resolved AST for Executable Bodies

**Problem:** Functions, services, derived properties, and constraints store executable bodies as raw source text. Imports are **file-scoped**, not element-scoped. When an element is serialized to its own `.json` file, the original file's imports are lost.

Example — a file `trading/src/functions.pure`:

```pure
import refdata::*;
import products::*;

Function trading::sectorName(t: Trade[1]): String[1]
{
  $t.instrument.sector.name
}
```

If we serialize the function body as raw text `$t.instrument.sector.name`, the downstream loader has no way to resolve `Trade` → `trading::Trade` or `Instrument` → `products::Instrument` — the import context that made those resolutions possible is gone.

**Affected element types:**
- `FunctionDefinition.body` — raw expression text (`resolvedBody` is null until Phase 6)
- `ServiceDefinition.functionBody` — raw query text
- `DerivedPropertyDefinition.expression` — raw expression text
- `ConstraintDefinition` expressions
- Mapping filter/join expressions

**Fix: serialize resolved AST, not raw text.** The compiler already has a name resolution phase (`NameResolver`) that converts simple names to FQNs. The serialization boundary should be **after name resolution but before type-checking**:

```
Parser → NameResolver → [SERIALIZE HERE] → TypeChecker
                          ↑
                   All names are FQNs.
                   Import context no longer needed.
                   TypeChecker can work from this.
```

Each executable body should be serialized as a **resolved AST** where:
- All type names are canonical FQNs (`Sector` → `refdata::Sector`)
- All function call targets are FQNs (`transform` → `refdata::transform`)
- All class references are FQNs (`Trade.all()` → `trading::Trade.all()`)
- The AST structure is preserved (not flattened back to text)

This is **not** typed AST — no `TypeInfo`, no store resolution, no mapping metadata. Just names canonicalized to FQNs. The TypeChecker still owns all type inference and runs from this resolved AST during compilation.

**Why this boundary is right:**
- Parser work is paid once at serialization time
- Import context is no longer needed at load time
- Lazy loading remains correct — the loader doesn't need to reconstruct imports
- TypeChecker still drives all type inference (no premature typing in the artifact)

**Impact on `NameResolver`:** Today `NameResolver` canonicalizes class types, superclasses, association ends, property types, derived-property types, function return/parameter types, function-to-function calls in resolved bodies, and mapping class/source class refs. The `default` arm at `NameResolver.java:68` passes through every other element type unchanged — that's a list of missing canonicalizations that must land before serialization. Concrete checklist (matches the Category A table above):

- [ ] `DatabaseDefinition.includes` — every included DB FQN
- [ ] `MappingDefinition.includes` (`MappingInclude.includedMappingPath`) — every included mapping FQN
- [ ] `MappingInclude.storeSubstitutions` — both `originalStore` and `substituteStore` FQNs per pair
- [ ] `ServiceDefinition.mappingRef` and `ServiceDefinition.runtimeRef`
- [ ] `RuntimeDefinition.mappings` list + `connectionBindings` values
- [ ] `ConnectionDefinition.storeName`
- [ ] `EnumerationMappingDefinition.enumType`
- [ ] `StereotypeApplication.profileName` and `TaggedValue.profileName` on every carrier (class, property, derived property, function) — these point at `Profile` packageable elements

Each bullet is a new case arm in `resolveDefinition()` plus a field walk that may allocate a new record when a ref resolves. Estimated scope: ~100-150 lines in `NameResolver.java` plus ~1-3 lines per affected record's constructor. Most additions are mechanical.

**Why this matters:** every unresolved ref is an invisible way for a cross-project reference to silently keep pointing at a short name after serialization. Whatever loads the element later has no import context to fix it up. Missing any one of these bullets is a latent bug class.

**Impact on `ElementSerializer`:** The serializer must capture the resolved AST (`ValueSpecification` tree with FQN names), not just the raw body string. The deserializer reconstructs the AST directly — no re-parsing or re-resolution needed.

**Schema evolution & versioning.** The resolved-AST JSON format is a serialization boundary that must survive across compiler versions. If the element file format changes (new AST node types, renamed fields, structural changes), old-format files become unreadable. Required safeguards:

- **`schemaVersion` field** on every element file (e.g., `"schemaVersion": 1`). The deserializer checks this first and fails fast on unrecognized versions.
- **Documented node-type taxonomy** — enumerated, not open-ended. Every AST node type that can appear in a serialized body must be listed in a schema spec. New node types require bumping the schema version.
- **Backwards-compat test harness** — load prior-version fixture files with the current compiler. Ensures old element files remain readable after compiler upgrades.
- **Forward-compat policy** — additive fields only within a major version (new optional fields are ignored by old deserializers). Breaking changes bump major version and require monorepo-wide re-serialization — Bazel handles this automatically since all outputs regenerate when the compiler binary changes.

Recommend: **publish the JSON schema spec before writing any serializer code** (see §11 Step 2).

### Required Change: Pre-Parsed M2M and XStore Expressions

**Problem:** Two mapping constructs store Pure expressions as raw text rather than structured AST:

- `MappingDefinition.ClassMappingDefinition.m2mPropertyExpressions: Map<String, String>` — M2M property transforms (`MappingDefinition.java:184`)
- `AssociationMappingDefinition.AssociationPropertyMapping.crossExpression: String` — XStore cross-class conditions (`AssociationMappingDefinition.java:60`)

Both are parsed on-the-fly inside `addMapping()` via `PureParser.parseQuery()` (see `PureModelBuilder.java:907-936`). For a normal whole-project build this is fine — the parser is already loaded. For per-element load paths it means every mapping file forces the Pure parser onto the load path, even when the loader never re-parses source.

**Fix:** add `parsedM2MExpressions: Map<String, ValueSpecification>` and `parsedCrossExpression: ValueSpecification` fields alongside the existing text fields, analogous to how `FunctionDefinition` carries both `body` (text) and `resolvedBody` (AST). Parse once at serialization time; deserialize AST directly on load. Fall back to re-parsing the text when the parsed field is absent — graceful handling of older element files.

**Scope of change:** ~6 hours total. Adds one field to each record, updates the serializer and the existing `PureModelBuilder.registerM2MClassMapping()` code path.

**Why it matters at scale:** without this, every mapping element file's load invokes the full Pure parser. Negligible for a single build, but O(N) at 30K elements.

### Required Change: Lazy Superclass Resolution

**Problem:** The current `PureModelBuilder.resolveSuperclasses()` eagerly resolves superclass **names** to actual `PureClass` **objects**. With lazy loading, the superclass element might not be loaded yet when a subclass is first encountered.

```java
// Current resolveSuperclasses() — called after all addClass() calls
private void resolveSuperclasses() {
    for (var entry : pendingClassDefinitions.entrySet()) {
        var classDef = entry.getValue();
        var superClasses = classDef.superClasses().stream()
            .map(name -> findClass(name).orElseThrow())  // ← superclass must exist!
            .toList();
        // Replace class with version that has resolved supers
    }
}
```

This is the same category of problem as `resolveType()` — eager resolution that assumes the world is fully loaded.

**Fix: store superclass FQNs as strings, resolve on demand.** The class element artifact stores:

```json
{
  "fqn": "trading::Swap",
  "superClasses": ["products::Instrument"],
  "localProperties": [
    { "name": "notional", "type": "Decimal", "mult": "[1]" }
  ]
}
```

When the TypeChecker navigates a property access on `Swap` and misses locally, it walks the `superClasses` FQN list, lazy-loading each superclass on demand:

```
1. TypeChecker: $s.ticker → findClass("trading::Swap")
   → local property miss for "ticker"
   → superClasses = ["products::Instrument"]
   → findClass("products::Instrument") → lazy-load
   → findProperty("ticker") → found ✓
```

**Why NOT flatten inherited properties into the subclass file:** Invalidation cost. If `Swap.json` included all inherited properties from `Instrument`, then every change to `Instrument` would change `Swap.json`, forcing ALL consumers of `Swap` to rebuild — even consumers that only use `Swap.notional` and never touch inherited members. Keeping inheritance lazy preserves per-element invalidation precision.

**Scope of change:** ~20 lines in `PureModelBuilder` — replace `resolveSuperclasses()` with a lazy `findProperty()` that walks the superclass chain on demand, plus ~5 lines in `PureClass` to store `List<String> superClassFqns` instead of `List<PureClass> superClasses`.

### Required Change: Back-Reference Fragments (Three-Tier)

**Problem:** Some properties of a class are not declared inside the class file. They're *contributed* by other elements — an `Association` injects `Person.addresses`; a future `Extension` injects `Sector.displayName`; a subclass relationship means `Trade.all()` should find `Swap` instances. When the TypeChecker resolves `$p.addresses` on `Person`, it only knows `(classFqn="refdata::Person", propertyName="addresses")`. It does NOT know which association or extension contributed that property. "The filesystem is the index" breaks for reverse lookups.

A naïve fix — embed all contributed properties directly in `Person.json` — causes write amplification: a change to any contributor (even in another project) would change `Person.json`, invalidating every consumer of `Person` including those that never touch the contributed property.

The elegant fix comes from real compilers. Rustc, Swift, Kotlin, and javac each handle back-references differently depending on **semantics**, not uniformly. Legend-lite adopts the same split into **three tiers**, each with a distinct mechanism matched to its scope.

#### The three tiers

| Tier | Semantics | Legend-lite kinds | Mechanism | Model |
|---|---|---|---|---|
| **Tier 1** | Target-side injection (always visible once loaded) | Association properties, (future) subclasses | Per-target-FQN fragment files, merged at load | Pure's `BackReference` |
| **Tier 2** | Caller-side resolution (scoped by import) | Extensions (§8 of CROSS_PROJECT_JOINS) | Per-project manifest; NameResolver builds an in-scope registry per compile | Kotlin extensions, Swift extensions, Rust traits |
| **Tier 3** | IDE-only queries (not needed at compile or runtime) | find-references, rename, call hierarchy | Out-of-band indexer, deferred; NOT in build outputs | Swift's SourceKit `.indexstore` |

Each tier is explained below. Tier 1 is the v1 critical path (replaces the old "nav sidecar" design). Tier 2 is the infrastructure that makes the Extension feature from [Cross-Project Joins §8](./CROSS_PROJECT_JOINS.md#8-consumer-side-class-extensions-a-trait-style-escape-hatch) architecturally clean. Tier 3 is explicitly **deferred** — it's not in scope for the Bazel migration, and it would live in a separate indexer pipeline if ever built.

#### Why three mechanisms instead of one

Pure (see `@/Users/neema/legend/legend-pure/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/serialization/compiler/metadata/BackReference.java`) uses a **single** target-side mechanism for all back-reference kinds. This works but has two costs relevant here:

1. **Lost scoping for extensions.** Kotlin-style extensions require import-scoped visibility (§8 of CROSS_PROJECT_JOINS specifies a full visibility matrix with `<<Exported>>`). A target-side back-ref is universal-once-loaded; it cannot express "visible only if imported." Grafting scope rules onto a target-side mechanism requires filtering logic that doesn't exist in Pure.
2. **IDE-tier write amplification.** Pure maintains `ReferenceUsage` for editor features (find-references). Every reference in every body produces a fragment. Build outputs become ~2× larger than necessary for non-editor use. Swift's separation of `.swiftmodule` from `.indexstore` avoids this.

Splitting the tiers preserves target-side simplicity where it belongs (associations) while adopting caller-side resolution where semantics demand it (extensions) and deferring IDE-only tracking to a separate pipeline.

#### Tier 1: Target-side back-reference fragments

**Use when** the back-reference is **always visible once the target is loaded** — the target's users should see it regardless of import scope.

**In scope today:** association-injected properties (`propertiesFromAssociations`), association-injected qualified properties (`qualifiedPropertiesFromAssociations`).

**In scope if/when added:** subclass enumeration (needed if `SuperClass.all()` polymorphic queries are added).

**File format.** Each contributing project emits one fragment file per target class it contributes to, co-located with its own element outputs:

```
products_elements/
  products__PersonAccount.json        ← authoritative association element (Category A source)
  refdata__Person.backrefs.json       ← fragment: contributions from products TO refdata::Person
  refdata__Account.backrefs.json      ← fragment: contributions from products TO refdata::Account
```

The filename encodes the **target** (`refdata::Person` → `refdata__Person`); the **containing directory** encodes the **contributor** (`products_elements/`). Multiple contributors each produce their own fragment with the same target filename, distinguished by directory.

**Fragment payload** uses Pure's `BackReference` sealed hierarchy (see `@/Users/neema/legend/legend-pure/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/serialization/compiler/metadata/BackReference.java`):

```json
{
  "schemaVersion": 1,
  "targetFqn": "refdata::Person",
  "contributorProject": "products",
  "backReferences": [
    {
      "kind": "propertyFromAssociation",
      "associationFqn": "products::PersonAccount",
      "propertyName": "accounts",
      "targetClassFqn": "products::Account",
      "multiplicity": { "lower": 0, "upper": null }
    }
  ]
}
```

The v1 `kind` values populated are `propertyFromAssociation` and `qualifiedPropertyFromAssociation`. The hierarchy is extensible — adding a `specialization` kind later for subclass enumeration requires adding a case arm to the serializer and deserializer, no file format change.

**Load-time merging.** When `PureModelBuilder` lazy-loads `refdata::Person`, it also loads every `refdata__Person.backrefs.json` found across registered dep element dirs (zero, one, or many). The contributions merge into a single polymorphic `List<BackReference>` attached to the loaded `PureClass`. `findProperty("accounts")` on `Person` consults this list after local-property and superclass-chain misses.

```
1. findProperty("accounts") on Person
   → check local properties in Person.json → miss
   → check superclass chain → miss
   → for depDir in depElementDirs: load depDir/refdata__Person.backrefs.json if exists
   → merged back-refs include propertyFromAssociation("accounts", ...)
   → return Property(name="accounts", typeFqn="products::Account", mult=[0..*])
```

**Why NOT embed in `Person.json`:** Invalidation. Embedding `accounts` into `Person.json` means every change to `products::PersonAccount` (multiplicity, rename, etc.) changes `Person.json`, invalidating every consumer of `Person` — even consumers that only use `Person.name` in a project that never depends on `products`. With fragments:

- Consumer using `Person.name` only → depends on `Person.json` → unaffected by `PersonAccount` changes
- Consumer using `Person.accounts` via a path through `products` → depends on `Person.json` + `products_elements/refdata__Person.backrefs.json` → rebuilds correctly
- Consumer in a project that doesn't depend on `products` → never sees the fragment → correct behavior (the property isn't in scope for them)

**Cross-project contribution.** Project A defines `refdata::Person`. Project B (`products`) declares `Association products::PersonAccount` between `refdata::Person` and `products::Account`. Project B's compile emits:
- `products_elements/products__PersonAccount.json` — the association element itself
- `products_elements/refdata__Person.backrefs.json` — a fragment contributing `accounts` to `refdata::Person`
- `products_elements/products__Account.backrefs.json` — a fragment contributing `person` to `products::Account` (reverse direction of the same association)

Consumer C that depends on both `refdata` and `products` loads `Person.json` from the refdata dir, merges in the fragment from the products dir, and sees `Person.accounts`. Consumer D that depends only on `refdata` sees `Person` without `accounts` — which is correct: `accounts` is only defined when `products` is in the dependency graph.

**Scope of change:** ~60 lines in `ElementSerializer` (emit fragments, one per target class with contributions), ~40 lines in `PureModelBuilder` (merge fragments at load), ~10 lines in the module extension (declare fragment output files alongside element files). Sealed hierarchy borrows Pure's `BackReference` record names verbatim.

#### Tier 2: Caller-side resolution (per-project manifest)

**Use when** the back-reference is **import-scoped** — only visible to callers that explicitly depend on the contributing project.

**In scope:** Extensions (see [CROSS_PROJECT_JOINS §8](./CROSS_PROJECT_JOINS.md#8-consumer-side-class-extensions-a-trait-style-escape-hatch)). The Extension design explicitly specifies a visibility matrix (same-package, default-not-exported, `<<Exported>>` opt-in) that is **structurally equivalent to Kotlin/Swift/Rust extension scoping** — resolution happens at the caller's compile site, not on the target class.

**Why NOT Tier 1:** Tier 1 back-refs are always visible once the target is loaded. Extensions are explicitly scoped: Project D can load `refdata::Sector` without seeing Project C's `SectorDisplay` extension unless D directly imports it. Target-side injection would universally leak extensions to every downstream consumer of Sector — violating the §8 visibility rules.

**File format.** Each project emits one manifest file summarizing what it contributes at the caller side:

```
products_elements/
  project_manifest.json
```

```json
{
  "schemaVersion": 1,
  "projectId": "products",
  "extensions": [
    {
      "extensionFqn": "products::AccountHelpers",
      "targetFqn": "refdata::Account",
      "exported": false,
      "flavor": "derived-only"
    },
    {
      "extensionFqn": "products::SectorDisplay",
      "targetFqn": "refdata::Sector",
      "exported": true,
      "flavor": "derived-only"
    }
  ]
}
```

`flavor` enforces the §8 rule that shadow-mapped extensions cannot cross package boundaries (`exported: true` + `flavor: shadow-mapped` is a compile error in the emitting project).

**Caller-side resolution.** When Consumer C compiles, its `PureModelBuilder` loads `project_manifest.json` from every dep dir (O(1) per dep, tiny files). It builds an in-memory registry `Map<targetFqn, List<ExtensionRef>>` scoped to C's own compile unit:

```
At C's compile-time, NameResolver encounters $sector.displayName:
  1. findClass("refdata::Sector") → lazy-loads refdata__Sector.json
  2. findProperty("displayName") on Sector → local miss, superclass miss, Tier 1 fragment miss
  3. Consult caller-scoped extension registry: registry.get("refdata::Sector")
     → [{extensionFqn: "products::SectorDisplay", exported: true}, ...]
  4. For each candidate, check visibility:
     - Same package as C? visible.
     - C explicitly imports this extension? visible.
     - Extension is <<Exported>>? visible.
     - Otherwise: skip.
  5. Among visible candidates, find one with property "displayName"
     → products::SectorDisplay declares displayName
     → lazy-load products__SectorDisplay.json for its definition
  6. Resolve property expression against SectorDisplay's body
```

The target class `refdata::Sector` never knows about extensions. Extensions register themselves with their project's manifest; callers scan deps' manifests at their own compile time. This naturally enforces scoping: Project D that depends on `refdata` but NOT on `products` never sees `products`'s manifest, and `Sector.displayName` is unresolvable for D — which is the correct behavior per §8.

**Conflict detection is free.** Two extensions contributing the same property name to the same target, both visible to the same caller, is a registry-build duplicate-key error. Matches the §8 rule without extra logic.

**Export broadcast.** `<<Exported>>` extensions are visible to every direct dependent. A consumer D that depends on C (which depends on `products`) transitively sees `products`'s exported extensions through C's manifest inclusion. Non-exported extensions stop at direct dep boundaries — exactly the Kotlin extension-library pattern.

**Scope of change:** ~30 lines in `ElementSerializer` (emit per-project manifest), ~50 lines in `PureModelBuilder` (load manifests, build caller-scoped registry, consult on property miss), ~10 lines in `NameResolver` (visibility filter at resolution time). The Extension element itself is a separate piece of work tracked in `CROSS_PROJECT_JOINS.md` §8.

**When to build:** Tier 2 infrastructure can land incrementally. A v1 without any Extension elements still emits an empty-extensions manifest and builds the registry (cost: zero). When the Extension feature lands, the registry is already wired.

#### Tier 3: IDE indexer (deferred)

**Use when** the back-reference is **only needed at edit time** — find-references, rename, call hierarchy, goto-implementations. These features exist in Pure's `ReferenceUsage` taxonomy but are **not** needed by the compiler or runtime.

**Not in scope for the Bazel migration.** Build outputs stay lean; IDE-only back-refs do NOT live in element files or fragments.

**Future architecture (illustrative).** A separate indexer — possibly an LSP extension, possibly a daemon — walks the emitted element files after each build and maintains its own index store:

```
.legend-index/
  find_references_index.db    ← reverse-ref index keyed by target FQN
  callers_index.db            ← function-call reverse-ref index
  ...
```

This matches Swift's SourceKit-LSP model (`.swiftmodule` for the compiler; `.indexstore` for the IDE). The build system (Bazel) doesn't know about the index; the index can be stale without breaking builds; rebuilding the index is always derivable from element files.

**Why defer:** Tier 3 features are productivity wins but not correctness-critical. Adding them to every build inflates outputs and creates churn-sensitive rebuilds. Deferring lets v1 ship without paying for features that have no immediate user.

#### Tier summary table

| Aspect | Tier 1 (target-side fragments) | Tier 2 (caller-side manifest) | Tier 3 (IDE indexer) |
|---|---|---|---|
| **v1 status** | Required (replaces nav sidecar) | Scaffolded (no extensions yet) | Deferred |
| **Kinds populated** | `propertyFromAssociation`, `qualifiedPropertyFromAssociation` | (none until Extensions land) | N/A |
| **Visibility** | Universal once target is loaded | Scoped by caller's import graph | Global index per workspace |
| **File format** | One `{target}.backrefs.json` per contributor per target | One `project_manifest.json` per project | Separate `.legend-index/*` store |
| **Bazel integration** | Declared as additional outputs; `unused_inputs_list` tracks usage | Manifest is always loaded (tiny); no per-fragment tracking | Out of band |
| **Invalidation** | Consumer rebuilds only if it accessed the specific target | Consumer rebuilds if a manifest it loads changes | Index rebuilds independently |
| **Inspiration** | Pure's `BackReference` | Kotlin / Swift / Rust extensions | Swift SourceKit, Google Kythe |

This split is a deliberate improvement over Pure's single-mechanism design. Pure conflates all back-reference kinds into one serialized taxonomy, which works for legend-engine's eager-load runtime but loses the scoping semantics needed for extensions and pays the IDE-tier write cost unconditionally. Splitting into three tiers gets each kind into its correct mechanism.

### Whole-Project Validation

A subtle point not obvious from reading `PureModelBuilder`: **`addSource()` does not type-check anything.** It parses, resolves names, registers metadata, and (in Phase 6 — `PureModelBuilder.java:363-378`) parses function bodies into AST. It never invokes `TypeChecker`. Semantic errors in executable bodies only surface when a query happens to run and triggers `TypeChecker.check()`.

That's fine for ad-hoc query execution but insufficient for a build action that must declare the project valid or invalid. Bazel's compile action needs to know, for example, that a function body `Person.all()->project(~[x: p | $p.thisFieldDoesntExist])` is broken — even if no test ever calls that function.

**The solution: a `validateElement(fqn)` primitive** that runs the appropriate checkers over one element and aggregates errors. No new pipeline stages; just a new entry point over existing machinery:

| Element kind | Validation passes |
|---|---|
| `ClassDefinition` | `TypeChecker.check()` on each derived-property expression and constraint body |
| `FunctionDefinition` | `TypeChecker.check()` on `resolvedBody` |
| `MappingDefinition` (Relational) | `MappingNormalizer` synthesizes `sourceSpec`; `TypeChecker.check()` stamps it |
| `MappingDefinition` (M2M / Pure) | `MappingNormalizer` synthesizes the `getAll → filter → extend` chain; `TypeChecker.check()` stamps it |
| `AssociationMappingDefinition` | `MappingNormalizer` integrates association extends into the owning mapping's `sourceSpec`; validated with it |
| `ServiceDefinition` | `TypeChecker.check()` on the service's query body |
| `DatabaseDefinition`, `RuntimeDefinition`, `ConnectionDefinition`, `EnumDefinition`, `ProfileDefinition` | Structural checks only (no expressions to type-check) |

**Scope is the caller's concern.** Same primitive, different scopes:

| Caller | Scope |
|---|---|
| Query execution (`PlanGenerator.generate`) | Implicitly per-query; only touches elements the query references — unchanged from today |
| LSP `didChangeTextDocument` | Call `validateElement(edited)` + over reverse-deps for in-editor diagnostics |
| Bazel compile action | Call for every element in the target's own elements — failed validation = failed build |
| `legend validate` CLI (future) | Call for every element in the workspace |

This keeps `addSource()` fast (no per-keystroke whole-project type check) while giving the compile action the guarantee that a successful build means a valid project.

**Scope of change:** ~100 lines. Add `PureModelBuilder.validateElement(String fqn)` that dispatches on element kind and invokes the right pass. The Bazel `ArtifactCompiler` (§4) invokes it in a loop after `addSource()` on all own elements. Errors are collected with source locations and reported as build failures.

### Shared Compiler Core: IDE vs Bazel

The Bazel path and the LSP/IDE path are not parallel implementations — they share the compiler core (`PureModelBuilder`, `TypeChecker`, `MappingNormalizer`, `PlanGenerator`) and the element-file format. They differ only in entry points and caching strategy, matching the split every mainstream language toolchain uses:

| Layer | Batch CLI (javac, rustc, tsc, Bazel action) | Language service (IntelliJ, rust-analyzer, tsserver, Legend LSP) |
|---|---|---|
| **Entry point** | One-shot: read sources + deps → emit artifacts | Incremental: edit events → update model → publish diagnostics |
| **Source input** | `.pure` for current project, element files for deps | `.pure` opened in editor, element files for deps |
| **Caching** | Action cache keyed on input hashes | In-memory `PureModelBuilder` kept alive; `ParseCache` across edits |
| **Parser invocation** | Once per file on cold build | Once per file, reused from `ParseCache` on unchanged files |
| **Validation** | `validateElement` on every own element; build fails on error | `validateElement` on edited element + reverse-deps; publish LSP diagnostics |
| **Output** | Per-element JSON + `unused_inputs_list` | LSP messages: diagnostics, hover, completions, go-to-def |

**What is shared:**
- `PackageableElement` record definitions in `model/def/` — the serialization target
- `ValueSpecification` AST for expressions — the structured body format
- Element-file format on disk — both paths read the same per-element JSON for upstream deps
- `NameResolver` with filesystem-indexed dep lookup
- `TypeChecker`, `MappingNormalizer`, `PlanGenerator` — untouched by the Bazel split

**What differs:**
- Batch path constructs a `PureModelBuilder`, populates it from element files, runs `validateElement` on own elements, writes outputs, discards the builder.
- LSP path constructs a `PureModelBuilder` once at workspace-open and keeps it resident, mutating through `ParseCache`-backed incremental re-parse on every `didChangeTextDocument`.

**The crucial property:** element files produced by Bazel are directly consumable by the LSP. Monorepo-open editing works as follows — the LSP reads current-project sources directly (with full parse) and reads upstream projects' Bazel-emitted element files (skipping parse entirely). The IDE never re-parses dep sources. This matches IntelliJ's model (`.class` files from jars) and `rust-analyzer`'s model (`.rlib` metadata from `target/`).

**For cross-workspace edits** (editing an upstream project in the same workspace), two strategies, same as real IDEs:
1. **Bazel-rebuilds-on-save** — editor writes, background `bazel build` produces new element files, LSP picks them up. Simple, one-way, relies on fast incremental Bazel.
2. **Shared in-memory model** — LSP detects upstream source change, re-parses into the shared `PureModelBuilder` so downstream sees the change instantly. More work, no Bazel in the edit loop.

Either can be added later; neither affects the element-file format.

### Eager Loading (for `legend_test` actions)

For `legend_test` actions that need ALL elements (model + implementation) for query execution. In eager mode, all elements are loaded upfront, so `resolveSuperclasses()` works normally — the lazy-resolution change above only matters for the lazy-loading path:

```java
public PureModelBuilder addElementFiles(Path elementsDir) {
    try (var files = Files.list(elementsDir)) {
        var elementFiles = files.filter(f -> f.toString().endsWith(".json")).toList();

        // Pass 1: Class stubs + enums (needed before property resolution)
        for (var file : elementFiles) {
            var element = ElementSerializer.deserialize(Files.readString(file));
            if (element instanceof ClassDefinition cd) registerClassStub(cd);
            if (element instanceof EnumDefinition ed) addEnum(ed);
        }

        // Pass 2: Full class properties + associations + functions
        for (var file : elementFiles) {
            var element = ElementSerializer.deserialize(Files.readString(file));
            if (element instanceof ClassDefinition cd) addClass(cd);
            if (element instanceof AssociationDefinition ad) addAssociation(ad);
            if (element instanceof FunctionDefinition fd) addFunction(fd);
        }

        resolveSuperclasses();

        // Pass 3: Implementation elements (mappings, databases, runtimes, etc.)
        for (var file : elementFiles) {
            var element = ElementSerializer.deserialize(Files.readString(file));
            if (element instanceof DatabaseDefinition dd) addDatabase(dd);
            if (element instanceof MappingDefinition md) addMapping(md);
            if (element instanceof RuntimeDefinition rd) addRuntime(rd);
            if (element instanceof ConnectionDefinition cd) addConnection(cd);
            if (element instanceof ServiceDefinition sd) addService(sd);
        }
    }
    return this;
}
```

---

## 7. End-to-End Walkthrough: What Happens When You Change a Class

This section walks through the complete sequence of events when a developer adds a property to an existing class, from editor to CI.

### Assumptions

```
Monorepo: ~200 projects, each defining ~300 PackageableElements.
Each project has ~100 transitive deps.
Max element files per action: ~30,000 (100 deps × 300 elements).
```

### Step 1: Developer Edits `refdata/src/Sector.pure`

Adds a new optional property `region` to `Sector`:

```pure
Class refdata::Sector
{
  name: String[1];
  code: String[1];
  region: refdata::Region[0..1];    // ← NEW
}
```

Saves the file. Runs `bazel build //projects/trading`.

### Step 2: Bazel Analysis Phase (~1-2 seconds)

Bazel reads all `BUILD.bazel` files and constructs the action graph. For `//projects/trading`, it transitively resolves deps:

```
trading → products → refdata
trading → products → shared_models
trading → risk → refdata
trading → risk → market_data
... (100 projects total in transitive closure)
```

For each dep, Bazel knows it produces a set of element files (from the `LegendElementsInfo` provider). It constructs the compile action with ~15 own source files + ~30,000 dep element files as inputs.

### Step 3: Refdata Builds First

Before trading can build, `refdata` must rebuild (its source changed). Bazel schedules it first.

```
Action: LegendCompile for //projects/refdata
  Inputs:
    - refdata/src/Sector.pure             (CHANGED)
    - refdata/src/Region.pure
    - refdata/src/Currency.pure
    - ... (~50 own .pure files)
    - core_elements/...                   (~5,000 individual dep element files)
  Outputs (individually declared by module extension scan):
    - refdata_elements/refdata__Sector.json
    - refdata_elements/refdata__Region.json
    - ... (300 individual element files)
    - refdata.unused_inputs
```

The compiler:
1. Parses all refdata `.pure` files
2. Loads dep elements as needed (lazy — maybe opens ~100 of the 5K)
3. Type-checks everything
4. Serializes each PackageableElement to its own `.json` file in `refdata_elements/`
5. Writes `refdata.unused_inputs` listing the ~4,900 dep elements it didn't touch

**Output**: `refdata_elements/refdata__Sector.json` is updated (now includes the `region` property). All other element files are byte-identical to before. Time: ~200-500ms.

### Step 4: Bazel Checks If Trading Can Be Skipped

Bazel has a cached result for trading from the last build. It checks:

1. Reads `trading.unused_inputs` from last build — lists ~29,700 element files that weren't used.
2. Of the ~300 USED element files, did any change? `refdata__Sector.json` changed.
3. Sector was in the USED set (trading references `refdata::Sector`).
4. **Action must re-run.**

If we had instead changed `refdata::SomeObscureEnum` that trading never references, it would be in the UNUSED set → Bazel skips the action entirely. **Cache hit. Zero work.**

### Step 5: Trading Compiles

```
Action: LegendCompile for //projects/trading
  Inputs:
    - trading/src/*.pure                  (~15 own source files)
    - individual dep element files        (~30,000 from 100 transitive deps)
  Outputs (individually declared):
    - trading_elements/trading__Trade.json
    - trading_elements/trading__TradeMapping.json
    - ... (~50 individual element files)
    - trading.unused_inputs
```

What the compiler does:

```
1. Parse own .pure files (~15 files, ~50ms)
   → Discovers: Trade, TradeMapping, TradingDB, etc.

2. Record dep element directories (just paths — ZERO I/O, ~0ms)
   → "look in these dirs when you need a dep element"

3. Name resolution + type-check (Java-style lazy loading):
   - Trade has "import products::*" and property "instrument: Instrument[1]"
     → NameResolver constructs candidate "products::Instrument"
     → File.exists(products__Instrument.json)? Yes ✓ → resolved
     → resolveType("products::Instrument") → findClass → cache miss
     → lazy-loads products__Instrument.json (~1μs exists + ~50μs read)
   - Function body: |$t: Trade[1]| $t.instrument.sector.name
     → needs Sector → File.exists(refdata__Sector.json)? Yes ✓
     → lazy-loads refdata__Sector.json
   - TradeMapping references InstrumentMapping
     → lazy-loads products__InstrumentMapping.json
   ... each dep element loaded exactly once, on first reference

4. Compiler knows exactly which dep elements it opened:
   ~300 used out of 30,000 available

5. Writes trading.unused_inputs (the 29,700 untouched elements)

6. Serializes trading's own elements to trading_elements/
```

Total I/O: ~300 file-exists checks (~0.3ms) + ~300 file reads (1KB each = 300KB, ~15ms). Total compile time: ~200-500ms.

### Step 6: Other Projects Build in Parallel

200 projects depend on refdata. Bazel evaluates all of them:

```
For each of the 200 projects:
  1. Read its unused_inputs from last build
  2. Was refdata__Sector.json in the USED or UNUSED set?

  ~150 projects: Sector was UNUSED → skip rebuild (cache hit, zero work)
  ~50 projects: Sector was USED → must rebuild (~500ms each, in parallel)
```

**Without `unused_inputs_list`:** all 200 rebuild. **With it:** only 50 do. 75% reduction.

### Step 7: Developer Pushes, CI Runs

On PR, CI runs `bazel build //...`. Same process across all projects. Bazel's remote cache means most actions are cache hits (already built locally or by a previous CI run). Only affected projects rebuild.

**Wall-clock for full CI build**: ~30-60 seconds (limited by critical path through dep graph, not total work).

### The Numbers

| Metric | Value |
|---|---|
| Bazel stat overhead per action (~30K inputs) | ~210ms |
| Files actually opened by compiler | ~300 |
| Compile time per project | ~200-500ms |
| `unused_inputs` file size | ~30K lines, ~1MB |
| Projects skipped on single-class change | ~75% (`unused_inputs` hit) |
| Total wall-clock for incremental build | ~2-5 seconds |
| Total wall-clock for full CI build | ~30-60 seconds |

### Scaling

| Deps per project | Elements per dep | Inputs per action | Stat overhead | Status |
|---|---|---|---|---|
| 100 | 300 | 30K | ~210ms | Comfortable |
| 200 | 300 | 60K | ~420ms | Fine |
| 500 | 300 | 150K | ~1.0s | Noticeable |
| 1000 | 300 | 300K | ~2.1s | Slow |

The practical limit is ~500K inputs per action. Below that, per-element files just work.

**Mitigations as project count grows:**

- **Project size discipline** — target 100-500 elements per `legend_library`. Split "god models" (e.g., a single `refdata` project with 5000 classes) into domain sub-libraries (`refdata-legal`, `refdata-geo`, `refdata-instrument`). Consumers only depend on the sub-library they need, reducing input count per action.
- **Hierarchical aggregation** — for consumers that genuinely need a broad cross-section of a domain, introduce an umbrella `legend_library` that re-exports a curated subset of sub-library elements. The consumer depends on the umbrella; Bazel stats only the umbrella's element files, not the underlying sub-libraries' full transitive closures. This bounds per-action input count even as the total element count grows.
- **Scale test before commit** — validate at target scale (100 projects / 30K elements) with realistic dep ratios before finalizing the per-element file granularity. See §9 Experiment 4 (planned).

The 30K-input comfort zone covers most real-world scenarios (100 deps × 300 elements). The mitigation ladder (project splitting → hierarchical aggregation → per-property shredding) gives room to scale beyond that without architectural changes.

---

## 8. Rebuild Optimization & Breaking Changes

### Upcast Compatibility (Same as Original Proposal)

The compatibility rules are domain-specific and unchanged:

| Change | Compatible? | Why |
|---|---|---|
| Add optional property (`[0..1]`/`[*]`) | Yes | Existing code doesn't reference it |
| Add new class/enum/association | Yes | No existing code references it |
| Add new enum value | Yes | Existing switches on subset still work |
| Remove/rename property or class | No | Existing references break |
| Change property type | No | Type mismatch |
| Narrow multiplicity (`[0..1]` to `[1]`) | No | Constraint violation |

### How Bazel Detects Breakage

When `refdata` removes `Sector.code`:

1. `refdata__Sector.json` element file changes (class signature changed)
2. Bazel invalidates all targets depending on `refdata__Sector.json`
3. `products` rebuild fails: TypeChecker throws "Property 'code' not found on refdata::Sector"
4. `bazel build //...` fails with a clear error
5. PR to main is blocked

Strengths over the original proposal's wavefront validation:
- Uses the real compiler (TypeChecker), not a separate compatibility checker
- Catches ALL breakage, not just symbol-level mismatches
- Error message is the exact same one a developer would see locally

### The Rebuild-on-Compatible-Change Tradeoff

**Without `unused_inputs_list`, any element change triggers downstream rebuilds.** If `refdata` adds an optional property to `Sector`, the `refdata__Sector.json` element file changes, and all dependents that list it as an input rebuild. The rebuilds **succeed** (adding a property is compatible), but the work still happens.

With `unused_inputs_list` (described above), only consumers that actually USED `refdata__Sector.json` rebuild. Consumers that only reference `refdata__Currency.json` are unaffected. This is the primary mitigation and should be enabled from the start.

### Granular Rebuild Avoidance: `unused_inputs_list`

The `legend_library` rule (§4) integrates Bazel's `unused_inputs_list` — each compile action writes a file listing dep elements it didn't touch, and Bazel skips future rebuilds when only those elements change. Per-element files give us natural per-PackageableElement granularity by default. For even finer control, element files can be further shredded:

**Per-class shredding** (recommended starting point):

```
bazel-out/refdata_elements/               # output of legend_library for refdata
  refdata__Sector.json                    # one file per class
  refdata__Region.json
  refdata__Currency.json
```

If `trading` only uses `Sector` and `Region`, the compile action writes `unused_inputs = [refdata__Currency.json]`. Next build, if only `Currency` changes, Bazel skips the `trading` rebuild entirely.

**Per-property shredding** (maximum precision):

```
bazel-out/refdata_elements/               # output of legend_library for refdata
  refdata__Sector/
    __class.json                          # class exists + supers
    name.json                             # {name: "name", type: "String", mult: [1,1]}
    code.json
    region.json
  refdata__Region/
    __class.json
    name.json
```

If `trading` uses `Sector.name` but not `Sector.code`, and `refdata` changes `Sector.code`, Bazel skips the rebuild because `code.json` was in the unused list.

> **Note:** `unused_inputs_list` only works with individually declared files — NOT with files inside tree artifacts (`declare_directory`). See §9 Experimental Validation for the proof.

**The chicken-and-egg resolves itself**: the first compile pays full cost and writes the unused list. Subsequent builds use the cached list. The list is only stale when the project's own sources change -- but that triggers a rebuild anyway.

### Dependency Analysis: Two Tiers (Parser vs TypeChecker)

The unused inputs list must reflect ALL cross-project references in the project. A Legend project may have ZERO executing queries -- it might be a pure model project, or it might define services and functions that are only invoked from outside. The dependency analysis must walk everything.

**The key insight: the parser can resolve class-level refs, but per-property refs in expressions require TypeChecker.**

Consider this chain in a function body or service query:

```pure
|Trade.all()->project([col(t|$t.instrument.sector.name, 'sectorName')])
```

The parser sees three property names: `instrument`, `sector`, `name`. But which classes do they belong to?

- `.instrument` -- property on what class? Parser knows `$t` is a lambda param, but its **type** (`Trade`) comes from TypeChecker resolving `Trade.all()` return type
- `.sector` -- property on what class? Only TypeChecker knows `instrument` returns `products::Instrument`, so `.sector` is `products::Instrument.sector`
- `.name` -- property on what class? Only TypeChecker knows `sector` returns `refdata::Sector`, so `.name` is `refdata::Sector.name`

Without TypeChecker, you see property names but not which classes they belong to. You can't write a meaningful `unused_inputs_list` at per-property granularity from the parser alone.

**Tier 1: Parser-level analysis (class-level refs -- no TypeChecker needed)**

FQNs appear as literal syntax in these locations:

| Source Element | Example | What parser sees |
|---|---|---|
| Property type | `instrument: products::Instrument[1]` | FQN `products::Instrument` |
| Superclass | `extends products::BaseTrade` | FQN `products::BaseTrade` |
| Association end | `portfolio: products::Portfolio[1]` | FQN `products::Portfolio` |
| Mapping source | `Mapping trading::TradingMapping ( ... )` | FQN in mapping class ref |
| getAll class arg | `Trade.all()` | Class name (resolved to FQN by NameResolver) |
| Enum type | `status: products::Status[1]` | FQN `products::Status` |
| Function param type | `func(x: products::Instrument[1])` | FQN `products::Instrument` |
| Connection store ref | `store: trading::TradingDB` | FQN |
| Runtime mapping ref | `mappings: [trading::TradingMapping]` | FQN |

This is sufficient for **per-class shredding** -- you know exactly which cross-project classes are referenced, without running TypeChecker.

**Tier 2: TypeChecker pass over ALL expressions (per-property refs)**

For per-property shredding, you need TypeChecker to resolve property access chains. This means type-checking the whole project -- not just ad-hoc queries:

- **Function bodies** -- a function might never be called in this project, but its body references cross-project properties
- **Service query lambdas** -- the service is defined here but executed externally
- **Derived property expressions** -- computed properties that navigate cross-project chains
- **Mapping expressions** -- filter conditions, join predicates that contain property chains
- **Constraint expressions** -- validation rules referencing cross-project properties

```java
/**
 * Analyzes ALL cross-project references in a Legend project.
 *
 * Two tiers:
 *   Tier 1 (parser): Extracts class-level FQN refs from syntax.
 *                     Sufficient for per-class shredding.
 *   Tier 2 (TypeChecker): Resolves property access chains in ALL expressions
 *                          (function bodies, service queries, derived props, etc.)
 *                          Required for per-property shredding.
 *
 * Output drives Bazel's unused_inputs_list for granular rebuild avoidance.
 */
public class ProjectDependencyAnalyzer {

    public record UsedSymbols(
        Set<String> classes,          // FQN: "refdata::Sector"
        Set<String> properties,       // "refdata::Sector.name"
        Set<String> enums,            // "refdata::Rating"
        Set<String> enumValues        // "refdata::Rating.AAA"
    ) {}

    /**
     * Tier 1: Parser-level class refs. No TypeChecker needed.
     * Walks parsed definitions for syntactically explicit FQNs.
     */
    public static UsedSymbols analyzeClassRefs(PureModelBuilder builder, String projectPackage) {
        var classes = new LinkedHashSet<String>();
        var enums = new LinkedHashSet<String>();

        // Class definitions: property types + superclasses
        for (var cls : builder.getLocalClassDefs()) {
            for (var prop : cls.properties()) {
                if (isCrossProject(prop.type(), projectPackage))
                    classes.add(prop.type());
            }
            for (var superFqn : cls.superClasses()) {
                if (isCrossProject(superFqn, projectPackage))
                    classes.add(superFqn);
            }
        }

        // Association definitions: both ends
        for (var assoc : builder.getLocalAssociationDefs()) {
            if (isCrossProject(assoc.end1().targetClass(), projectPackage))
                classes.add(assoc.end1().targetClass());
            if (isCrossProject(assoc.end2().targetClass(), projectPackage))
                classes.add(assoc.end2().targetClass());
        }

        // Mapping source classes, function param/return types,
        // connection store refs, runtime mapping refs, enum type refs...
        // (all syntactically explicit FQNs)

        return new UsedSymbols(classes, Set.of(), enums, Set.of());
    }

    /**
     * Tier 2: Full project type-check for per-property refs.
     * Runs TypeChecker on ALL expressions in the project:
     *   - function bodies (even if never called in this project)
     *   - service query lambdas (defined here, executed externally)
     *   - derived property expressions
     *   - mapping filter/join expressions
     *   - constraint expressions
     *
     * This is the ONLY way to resolve property access chains like
     * $t.instrument.sector.name to their owning classes.
     */
    public static UsedSymbols analyzePropertyRefs(PureModelBuilder builder,
                                                   String projectPackage) {
        // Start with Tier 1 class refs
        var result = analyzeClassRefs(builder, projectPackage);
        var properties = new LinkedHashSet<String>();
        var enumValues = new LinkedHashSet<String>();

        // Superclass inheritance: depend on ALL superclass properties
        for (var cls : builder.getLocalClassDefs()) {
            for (var superFqn : cls.superClasses()) {
                if (isCrossProject(superFqn, projectPackage)) {
                    var superCls = builder.findClass(superFqn);
                    for (var p : superCls.properties())
                        properties.add(superFqn + "." + p.name());
                }
            }
        }

        // Type-check ALL expressions in the project.
        // TypeChecker records classPropertyAccesses as it resolves chains.
        var typeChecker = new TypeChecker(builder);

        // Function bodies
        for (var func : builder.getLocalFunctionDefs())
            typeChecker.checkExpression(func.body());

        // Service query lambdas
        for (var svc : builder.getLocalServiceDefs())
            typeChecker.checkExpression(svc.queryBody());

        // Derived property expressions
        for (var cls : builder.getLocalClassDefs())
            for (var prop : cls.derivedProperties())
                typeChecker.checkExpression(prop.body());

        // Mapping expressions (filters, join conditions)
        for (var mapping : builder.getLocalMappings())
            for (var propMapping : mapping.propertyMappings())
                if (propMapping.hasExpression())
                    typeChecker.checkExpression(propMapping.expression());

        // Constraint expressions
        for (var cls : builder.getLocalClassDefs())
            for (var constraint : cls.constraints())
                typeChecker.checkExpression(constraint.body());

        // Collect per-property refs from TypeChecker's accumulated accesses
        for (var access : typeChecker.classPropertyAccesses()) {
            if (isCrossProject(access.classFqn(), projectPackage))
                properties.add(access.classFqn() + "." + access.propertyName());
        }

        return new UsedSymbols(result.classes(), properties, result.enums(), enumValues);
    }
}
```

**What each source element contributes and what resolves it:**

| Source Element | Reference Kind | Granularity | Resolved by |
|---|---|---|---|
| Property type (`x: Instrument[1]`) | Class exists | Class-level | **Parser** (FQN in syntax) |
| Superclass (`extends Base`) | Class + ALL properties | Per-property | **Parser** (FQN) + model lookup |
| Association end | Class exists | Class-level | **Parser** (FQN in syntax) |
| Mapping source class | Class exists | Class-level | **Parser** (FQN in syntax) |
| Mapping property (`ticker -> COL`) | Specific property | Per-property | **Parser** (name in mapping syntax) |
| Mapping join chain (`@OrdCust`) | Join/association | Class-level | **Parser** (name in syntax) |
| Function body (`$x.instrument.sector.name`) | Specific properties | Per-property | **TypeChecker** (chain resolution) |
| Service query (`\|Trade.all()->project(...)`) | Specific properties | Per-property | **TypeChecker** (chain resolution) |
| Derived property body | Specific properties | Per-property | **TypeChecker** (chain resolution) |
| Constraint expression | Specific properties | Per-property | **TypeChecker** (chain resolution) |
| Mapping filter/condition | Specific properties | Per-property | **TypeChecker** (chain resolution) |
| Enum reference (`Rating.AAA`) | Enum + value | Per-value | **Parser** (FQN in syntax) |
| getAll/Class reference | Class exists | Class-level | **Parser** (name in syntax) |

**The `UsedSymbols` output drives the `unused_inputs_list`:**
- For per-class shredding: Tier 1 (parser) -- any class file not in `UsedSymbols.classes` is unused
- For per-property shredding: Tier 2 (TypeChecker) -- any property file not in `UsedSymbols.properties` is unused

**Cost of Tier 2**: TypeChecker runs on ALL expressions in the project -- function bodies, service queries, derived properties, mapping expressions, constraints. This is real work, but it happens inside the same Bazel compile action that already parses and builds the model. The dependency analysis is a byproduct of compilation, not a separate step. The TypeChecker work is cached by Bazel across builds just like everything else.

**Recommendation**: Start with per-class shredding + Tier 1 (parser-only) analysis. This handles the most common case (project uses subset of a large dependency's classes) with zero TypeChecker cost. Graduate to per-property shredding + Tier 2 (full TypeChecker pass) if profiling shows intra-class property changes causing significant unnecessary rebuilds.

### Pre-Publish Impact Analysis

```bash
# "What breaks if I remove Sector.code?"
# Just edit the file and run:
bazel build //... 2>&1 | grep "ERROR"

# Bazel tells you exactly which targets fail and why.
# No custom `legend impact` tool needed.
```

### Staged Rollout for Breaking Changes

Same deprecation pattern as original proposal, but managed through normal git workflow:

```
# Step 1: Add Sector.codeV2 alongside Sector.code (additive, compatible)
# Commit to main. bazel build //... passes. Everyone keeps working.
# (Downstream targets rebuild but succeed -- element changed but compatible)

# Step 2: Consumers migrate from code to codeV2 at their own pace.
# Each consumer commits when ready. bazel build //... keeps passing.

# Step 3: Remove Sector.code. All consumers already migrated.
# bazel build //... passes. Done.
```


---

## 9. Experimental Validation

All key claims in this proposal have been empirically verified with working Bazel prototypes. Source code is in `experiments/`.

### Experiment 1: Per-Element Cache Tracking (`experiments/tree_artifact_test/`)

**Question:** Does Bazel's `unused_inputs_list` work at per-element granularity?

**Setup:** A `producer` rule creates element files from source. A `consumer` rule reads one element file, lists the others as unused. Two variants tested:

| Variant | Output mechanism | `unused_inputs_list` content |
|---|---|---|
| **Tree artifact** | `declare_directory()` → all files in one dir | Individual file paths inside the dir |
| **Individual files** | `declare_file()` per element | Individual file paths |

**Results:**

| Scenario | Tree artifact | Individual files |
|---|---|---|
| No change | Cached ✓ | Cached ✓ |
| Change **unused** dep element | **Rebuilt ✗** | **Cache hit ✓** |
| Change **used** dep element | Rebuilt ✓ | Rebuilt ✓ |

**Conclusion:** `unused_inputs_list` does NOT work with files inside tree artifacts — Bazel treats directories as opaque blobs. It DOES work with individually declared files. **Per-element tracking requires `declare_file()` per element, not `declare_directory()`.**

### Experiment 2: Full Pipeline (`experiments/legend_rules_test/`)

**Question:** Does the full architecture work end-to-end? Module extension → element discovery → `legend_library` → per-element caching?

**Setup:** Two projects: `refdata` (3 elements across 2 `.pure` files) and `trading` (1 element, depends on refdata). Module extension scans `.pure` files with keyword matching, generates `elements.bzl` per project.

**Results:**

| Test | Expected | Actual |
|---|---|---|
| Initial build | Both projects compile | ✓ 2 actions (refdata + trading) |
| No-change rebuild | Fully cached | ✓ 0 actions |
| Change unused dep (`Region.pure`) | Refdata rebuilds, trading cache hit | ✓ `1 action cache hit` |
| Change used dep (`Sector.pure`) | Both rebuild | ✓ 2 actions |
| Add new element (`Currency.pure`) | Module ext detects, new file declared | ✓ `refdata__Currency.json` appeared |

**Bazel output hashing:** Bazel checks output content hashes, not just "did the action execute." If refdata re-runs but produces byte-identical element files (e.g., a comment-only change in the source), downstream consumers skip rebuilding. This is free. Note: in a real compiler, adding a property to `Sector` WOULD change `refdata__Sector.json` (the full class definition is serialized), correctly triggering downstream rebuilds.

### Experiment 3: Auto Version-Bump via Module Extension (`experiments/legend_rules_test/version_check.bzl`)

**Question:** Can Bazel module extensions auto-detect outdated external repo commit hashes?

**Setup:** A `version_check` module extension that runs `git ls-remote` against a real GitHub repo, compares the latest commit with the pinned commit, and generates a status report.

```python
# In MODULE.bazel:
vc.repo(
    name = "example_repo",
    remote = "https://github.com/bazelbuild/bazel-skylib.git",
    current_commit = "0000000000000000000000000000000000000000",  # fake old commit
)

# Build:
$ bazel build //:check_versions && cat bazel-bin/version_report.txt
Repo: example_repo
Status: OUTDATED
Current: 0000000000000000000000000000000000000000
Latest: 390ecf872568a9fc1752cbade56be52cd4263758
```

**How this integrates with CI:**

```yaml
# .github/workflows/bump_external.yml
steps:
  - run: bazel build //:check_versions
  - run: |
      STATUS=$(cat bazel-bin/version_report.txt | grep Status | cut -d' ' -f2)
      if [ "$STATUS" = "OUTDATED" ]; then
        LATEST=$(cat bazel-bin/version_report.txt | grep Latest | cut -d' ' -f2)
        # Update MODULE.bazel with new commit hash
        # Create PR, CI validates, human merges
      fi
```

**Key insight:** Repository rules and module extensions can execute arbitrary programs (`rctx.execute()`). This means Bazel itself can be the version-checking infrastructure — no separate Python scripts, no external CI plugins. The version check runs as part of `bazel build`, is cached like any other action, and only re-runs when explicitly requested.

### Experiment 4 (Planned): Scale Validation

**Question:** Does the per-element architecture hold at target scale (100+ projects / 30K+ elements)?

**Setup:** Synthetic monorepo generator producing N projects, each with ~300 elements (classes, enums, associations, functions), with realistic dep ratios (10-100 transitive deps per project). Generator creates `.pure` source files with cross-project references: property types pointing to dep classes, function bodies referencing dep properties, associations spanning project boundaries.

**What to measure:**

| Metric | Target | Why it matters |
|---|---|---|
| Analysis phase (cold) | ≤10s | Module extension regex scans across all projects |
| Analysis phase (warm, 1 project changed) | ≤1s | Only changed project rescanned |
| Initial full build | ≤60s | All projects compile in parallel |
| Incremental build (1 class changed) | ≤5s | Only affected projects rebuild |
| Stat overhead per action at 30K inputs | ≤300ms | Per-element file viability |
| `unused_inputs_list` effectiveness | ≥70% skip rate | Single-class change should skip most downstream |

**How to run:**

```bash
# Generate synthetic monorepo
python tools/gen_scale_test.py --projects 100 --elements-per-project 300 --dep-ratio 0.3

# Full build
time bazel build //scale_test/...

# Incremental: change one class in one project, rebuild
echo 'new_prop: String[0..1];' >> scale_test/project_42/src/model.pure
time bazel build //scale_test/...

# Check unused_inputs effectiveness
wc -l bazel-out/*/bin/scale_test/project_*/unused_inputs
```

**Status:** Not yet run. Recommended before committing to the per-class-file granularity default. Add to `experiments/scale_test/`. See §11 Step 0.

### Summary of Validated Claims

| Claim | Status | Evidence |
|---|---|---|
| Per-element `unused_inputs_list` tracking | ✅ Proven | Experiment 1: individual files |
| Tree artifacts are opaque to `unused_inputs_list` | ✅ Proven | Experiment 1: tree artifact variant |
| Module extension can scan `.pure` files | ✅ Proven | Experiment 2: regex extraction |
| Auto-discovery of new elements | ✅ Proven | Experiment 2: added `Currency.pure` |
| Full unused-dep cache skipping | ✅ Proven | Experiment 2: Region change → trading cached |
| Byte-identical output optimization | ✅ Bazel feature | Output hash check skips downstream if content unchanged (e.g., comment-only source change) |
| `git ls-remote` in module extension | ✅ Proven | Experiment 3: real GitHub repo |
| Auto-detect outdated commits | ✅ Proven | Experiment 3: status report |
| Per-element architecture at scale | ⬜ Planned | Experiment 4: synthetic 100-project monorepo |


---

## 10. Developer Experience

### First Time Setup

```bash
# Clone the monorepo (or sparse-clone just your project + deps)
git clone https://github.com/gs/legend.git
cd legend

# Build your project (Bazel fetches and caches everything)
bazel build //projects/trading

# Run tests
bazel test //projects/trading:trading_test
```

### Daily Development

```bash
# Edit Pure files in Studio Lite (or any editor with LSP)
# LSP uses ParseCache + lazy element loading for instant feedback
# Cross-project diagnostics work because dep elements are loaded on demand

# Build (incremental -- only rebuilds what changed)
bazel build //projects/trading

# Test
bazel test //projects/trading:trading_test

# If you need to see the generated SQL:
bazel run //projects/trading:trading_test -- --verbose
```

### Creating a New Project

```bash
mkdir -p projects/my-new-project/src

# Write your Pure files
cat > projects/my-new-project/src/model.pure << 'EOF'
Class myproject::Order {
    id: Integer[1];
    instrument: products::Instrument[1];
}
EOF

# Write BUILD.bazel (elements list is auto-generated by module extension)
cat > projects/my-new-project/BUILD.bazel << 'EOF'
load("//tools:legend.bzl", "legend_library")
load("@my-new-project//:elements.bzl", "ELEMENTS")

legend_library(
    name = "my-new-project",
    srcs = glob(["src/**/*.pure"]),
    elements = ELEMENTS,
    deps = ["//projects/products"],
    visibility = ["//visibility:public"],
)
EOF

# Build
bazel build //projects/my-new-project

# Commit and push -- CI validates against everything
git add projects/my-new-project/
git commit -m "Add my-new-project"
git push
```

### Depending on an External Legacy Repo

```bash
# Check if the external repo is already registered
grep "com_gs_legacy_thing" external_repos.bzl

# If not, register it (one-time — add git_repository + BUILD overlay):
# See "External Repos" section above for details.

# Add to your BUILD.bazel deps:
#   deps = [..., "@com_gs_legacy_thing//:legacy_thing"]

# Build -- Bazel fetches the repo, builds it with legend_library,
# TypeChecker now sees legacy_thing's classes
bazel build //projects/my-project
```

### CI Pipeline

#### On Every PR to Monorepo

```yaml
# .github/workflows/ci.yml
on: [pull_request]

jobs:
  build:
    steps:
      - uses: actions/checkout@v4
      - uses: bazelbuild/setup-bazelisk@v3

      # Build everything -- validates full dep graph
      - run: bazel build //...

      # Test everything
      - run: bazel test //...

      # Check for breaking element changes (optional explicit gate)
      - run: bazel run //tools/compat_checker -- --against main
```

`bazel build //...` IS the wavefront validation. If any target fails, the PR is blocked.

#### Remote Cache for Fast CI

```python
# .bazelrc
build --remote_cache=grpc://cache.internal:9090
build --remote_upload_local_results
```

With remote caching, CI reuses cached outputs from previous builds. A typical PR that changes one project rebuilds only that project + its dependents -- everything else is a cache hit.

---

## 11. Implementation Plan

> **Detailed per-step guide:** [`BAZEL_IMPLEMENTATION_PLAN.md`](BAZEL_IMPLEMENTATION_PLAN.md) covers Steps 2, 2.5, and 3 (the risky engine refactors) with per-file checklists, JSON schema spec, contract definitions, test gates, and done criteria. Read that doc **during execution**; read this §11 for the step ordering and Bazel-native work.

### Step 0: Scale Validation (0.5 session)

**Goal**: Validate the per-element architecture at target scale before committing to implementation.

**Files**: `experiments/scale_test/gen_scale_test.py`, `experiments/scale_test/MODULE.bazel`

- Build a synthetic monorepo generator: N projects × ~300 elements, realistic dep ratios (10-100 transitive deps per project), cross-project property types and function references
- Run Experiment 4 (§9): measure analysis phase, build time, incremental rebuild, stat overhead, unused_inputs effectiveness
- **Go/no-go gate**: if stat overhead exceeds 500ms at 30K inputs or analysis phase exceeds 15s at 100 projects, revisit granularity (consider per-source-file instead of per-element, or hierarchical aggregation)
- Document results in `experiments/scale_test/RESULTS.md`

### Step 1: Bazel-ify the Monorepo (1 session)

**Goal**: Get `bazel build //platform/engine` working.

- Add `MODULE.bazel` to repo root
- Add `BUILD.bazel` to `platform/engine/` (wraps existing Maven sources as `java_library`)
- Move `engine/` under `platform/engine/` (or symlink)
- Verify: `bazel build //platform/engine` compiles the engine
- Verify: `bazel test //platform/engine:all` runs existing tests

**Note**: Maven and Bazel can coexist during migration. Maven continues to work unchanged. Bazel is additive.

### Step 2: Element Serialization (1-2 sessions)

**Files**: `tools/element_serializer/ElementSerializer.java`, `NameResolver.java` (expand FQN canonicalization)

- **Prerequisite: publish the resolved-AST JSON schema first.** Document node taxonomy, `schemaVersion` field, versioning rules, forward/backward compat policy. Review the schema spec before writing any serializer code (see "Schema evolution & versioning" in §6).
- `ElementSerializer.serialize(PackageableElement)` -- one element to one JSON file
- `ElementSerializer.deserialize(String json)` -- JSON back to definition record
- Handles all element types: ClassDefinition, EnumDefinition, AssociationDefinition, FunctionDefinition, DatabaseDefinition, MappingDefinition, RuntimeDefinition, ConnectionDefinition, ServiceDefinition
- **Resolved AST serialization** -- executable bodies (functions, services, derived properties, constraints, mapping expressions) are serialized as resolved AST with all names canonicalized to FQNs, not raw source text. This eliminates the need to reconstruct import context at load time (see "Required Change: Resolved AST for Executable Bodies")
- **Tier 1 back-reference fragments** -- after serializing all association elements, group contributions by target class and emit one `{target}.backrefs.json` fragment per target with `propertyFromAssociation` / `qualifiedPropertyFromAssociation` entries using Pure's `BackReference` schema (see "Required Change: Back-Reference Fragments (Three-Tier)")
- **Tier 2 project manifest** -- emit `project_manifest.json` listing this project's extensions with their target FQN, export flag, and flavor (v1: empty until the Extension element lands; infrastructure stays wired)
- Expand `NameResolver` to FQN-canonicalize all Category A refs in the Complete Dependency Reference Matrix (§6): database includes, mapping includes, `MappingInclude.storeSubstitutions`, service mapping/runtime refs, runtime mapping/connection bindings, connection store ref, enumeration mapping enum type, and Profile refs in stereotypes/taggedValues (~100-150 lines in `NameResolver.java`)
- **Pre-parsed M2M and XStore expressions** -- serialize `parsedM2MExpressions` and `parsedCrossExpression` as `ValueSpecification` AST alongside the existing text fields, so mapping element file load paths don't pull in the full Pure parser (see "Required Change: Pre-Parsed M2M and XStore Expressions")
- **Backwards-compat test harness** -- save fixture element files as golden tests; verify current deserializer can load them after compiler changes
- CLI wrapper: `java ElementExtractor --srcs *.pure --out-dir elements/`
- Test: extract elements from sales-trading model, verify round-trip for all element types including resolved AST bodies, Tier 1 back-reference fragments, and the Tier 2 project manifest

### Step 2.5: `typeFqn` Preparatory Refactor (1-2 sessions)

**Goal**: Migrate `Property.genericType()` from resolved `Type` references to `String typeFqn` in the main engine, validated by existing tests. This is the largest single prerequisite for lazy loading (Step 3).

**Files**: `Property.java`, `PureModelBuilder.java`, `TypeChecker.java`, `checkers/*.java` (~36 files), `PlanGenerator.java`, `MappingResolver.java`, `MappingNormalizer.java`, `GenericType.java`

- Add `String typeFqn` field to `Property` alongside existing `genericType()` (~20 lines)
- Incrementally migrate consumers to read `typeFqn` instead of `genericType()` (~200-500 lines across ~30 files)
- Flag day: remove `genericType` field, all consumers on `typeFqn`
- Similarly: `PureClass.superClasses` from `List<PureClass>` to `List<String> superClassFqns` — prerequisite for lazy superclass resolution
- Validate: all 2100+ existing tests pass with zero changes to test definitions
- **Do NOT wire into lazy loading yet** — this step only changes the storage format, not the loading strategy

### Step 3: Lazy Element Loading in PureModelBuilder (1-2 sessions)

**Files**: `PureModelBuilder.java` (add lazy loading), `PureClass.java`, `PureLspServer.java` (wire up)

- Add `addDepElementDir(Path elementsDir)` — records where to find dep elements (zero I/O)
- Override `findClass/findEnum/findAssociation/findFunction` with lazy-load fallback
- String-based property types and lazy superclass resolution already landed in Step 2.5 — this step adds the lazy *loading* mechanism on top
- **Tier 1 fragment merging** -- `findProperty()` on local+superclass miss loads `{target}.backrefs.json` from every dep element dir and merges contributions (see "Required Change: Back-Reference Fragments (Three-Tier)")
- **Tier 2 manifest loading** -- on `PureModelBuilder` init, load `project_manifest.json` from each dep dir and build a caller-scoped extension registry; consult on property-lookup miss after Tier 1
- Add `addElementFiles(Path dir)` for eager loading (Bazel actions)
- Wire `addDepElementDir()` into `PureLspServer.rebuildAndPublishAll()`
- **Add `validateElement(String fqn)` primitive** — dispatches by element kind and runs the appropriate `MappingNormalizer` + `TypeChecker` passes (see "Whole-Project Validation"). This is the API both the Bazel compile action and the LSP call for diagnostics; `addSource()` does NOT invoke it (keeps the per-keystroke path fast)
- Test: two-project test — project A's elements on disk, project B lazy-loads A's class on first `findClass` call, TypeChecker resolves cross-project refs including inherited and association-backed properties
- Test: `validateElement` catches a broken function body (`$p.nonExistentProp`) and a broken mapping filter, reports source locations correctly

### Step 4: Module Extension + `legend_library` Bazel Rule (1-2 sessions)

**Files**: `tools/legend_ext.bzl`, `tools/legend.bzl`, `tools/artifact_compiler/ArtifactCompiler.java`

- Define module extension: regex scan of `.pure` files → generates `elements.bzl` per project
- Define `LegendElementsInfo` provider with `element_files` depset of individual `File` objects
- Define `legend_library` rule: `declare_file()` per element (from `ctx.attr.elements`), single compile action
- **`model_deps` / `impl_deps` attributes** — two dep attributes making the compile-time contract explicit (see §4). `model_deps` for XStore model joins (model elements only), `impl_deps` for legacy Database/Mapping includes
- `ArtifactCompiler` CLI: `--elements-dir` output, `--dep-elements` input, `--track-unused` output
- **`ArtifactCompiler` invokes `validateElement()` on every own element** after loading sources. Failed validation = build error with aggregated source locations. This gives `bazel build` the guarantee that a green build = a type-correct project (see "Whole-Project Validation")
- `unused_inputs_list` integration for per-element rebuild avoidance (experimentally verified: requires individually declared files, NOT tree artifacts)
- Register projects in `MODULE.bazel` via `legend.project()` / `legend.external()` tags
- Test: `bazel build //projects/refdata //projects/products` where products depends on refdata
- Verify: change unused dep element → downstream cache hit (no rebuild)

### Step 5: External Repo Integration (0.5 session)

**Files**: `external_repos.bzl` (or generated from `external_repos.yaml`), `external/*/BUILD.overlay`, `MODULE.bazel`

- Add `git_repository` declarations for pilot external repos
- Register external repos with module extension (`legend.external(repo = "...")`)
- Write BUILD overlays that load auto-generated `elements.bzl`
- **Manifest-based generation**: at scale (1000+ repos), generate `external_repos.bzl` from `external_repos.yaml` via a repository rule or codegen step — keeps PRs reviewable and enables batch operations
- Script to bulk-generate `git_repository` entries + BUILD overlays from a repo manifest
- Auto-version-bump CI job (see "Bumping an External Repo Version")
- Test: `bazel build @com_gs_legacy_products//:legacy_products` produces individual element files

### Step 6: `legend_test` Rule (1 session)

**Files**: `tools/legend.bzl` (add `legend_test`)

- `legend_test` rule: loads ALL element files (model + implementation) for query execution
- Uses `addElementFiles()` (eager mode) to load everything including mappings/databases/runtimes
- Test: end-to-end query execution across project boundary
- **Future work (v2)**: per-test `unused_inputs_list` — extract mapping/store/runtime elements actually touched by each test's query plan. Not on v1 critical path.

### Step 7: Compat Checker + CI Hardening (0.5 session)

**Files**: `tools/compat_checker/ElementCompat.java`, `.github/workflows/ci.yml`

- `ElementCompat.check(oldElements, newElements)` -- upcast compatibility on model elements
- CI workflow: `bazel build //...` + compat check on element changes
- Test: detect removed property, added optional property, type change

### Step 8: Frontend Multi-File Support (2 sessions)

Same as original proposal Step 6:
- File tree sidebar, tab-per-file editing
- Each file tracked via LSP protocol
- Model rebuilt from all files on change (ParseCache + lazy dep element loading)

---

## Appendix

### A. Migration Path: Maven to Bazel

Bazel and Maven coexist. No big-bang migration needed.

```
Phase 0 (Step 0):     Scale validation — synthetic monorepo proves per-element viability.
Phase 1 (now):         Maven builds everything. No Bazel.
Phase 1.5 (Step 2.5): typeFqn refactor lands in main engine — prerequisite for lazy loading.
Phase 2 (Step 1):      Bazel builds platform/engine alongside Maven. Both work.
Phase 3 (Step 4):      New projects use legend_library rules. Platform still Maven.
Phase 4 (later):       Platform migrates to Bazel. Maven removed.
```

Developers can use either build system during the transition. CI runs both until Maven is retired.

### B. Performance Projections

| Scenario | Expected |
|---|---|
| `bazel build //projects/trading` (cold, no cache) | ~3s (parse + compile + deps) |
| `bazel build //projects/trading` (warm, cached deps) | ~200ms (only local recompile) |
| `bazel build //...` (500 projects, remote cache) | ~30s (parallel, mostly cache hits) |
| Edit one mapping in `refdata` (mapping element changes) | 0 downstream rebuilds (only mapping .json changed, unused by dependents) |
| Add optional property to `refdata::Sector` | Only consumers that USE Sector rebuild (`unused_inputs_list`) |
| Remove property from `refdata::Sector` | Consumers of Sector rebuild, TypeChecker FAILS, PR blocked |
| Add new class to `refdata` | 0 downstream rebuilds (new file, not in anyone's input set) |
| IDE keystroke latency (50-file project) | ~15ms (ParseCache: 1 re-parse + 49 cache hits) |
| IDE with 10K dep classes, use 50 | ~5ms lazy load (50 x ~1KB element files on demand) |
| Bump external repo version | Bazel re-fetches, rebuilds that repo's elements, only affected downstream rebuilds |

### C. Comparison: Original Proposal vs Bazel Proposal

| Dimension | Original (Custom) | Bazel + Per-Element |
|---|---|---|
| **Lines of custom code** | ~3000 (est.) | ~500 (est.) |
| **Dep resolution** | Custom wavefront + diamond resolver | Bazel built-in |
| **Artifact format** | Monolithic `.legend` per project | Per-PackageableElement .json files |
| **Shape extraction** | Separate ShapeExtractor tool | Not needed (element file IS the shape) |
| **Caching** | Custom ParseCache + custom artifact cache | Bazel action cache + ParseCache (IDE only) |
| **Rebuild granularity** | Per-project (or per-symbol with change tokens) | Per-element via `unused_inputs_list` *(proven: Experiment 1-2)* |
| **Element discovery** | Manual manifest | Module extension regex scan *(proven: Experiment 2)* |
| **CI validation** | Custom wavefront CI workflow | `bazel build //...` |
| **CLI** | Custom `legend` CLI (5 subcommands) | `bazel build/test/run` |
| **External repo support** | Sparse checkout from artifact monorepo | `git_repository` + same `legend_library` rule |
| **External version bumping** | Manual or custom script | Bazel-native `git ls-remote` in module extension *(proven: Experiment 3)* |
| **Breaking change detection** | Custom upcast + change tokens | Bazel rebuild + TypeChecker errors |
| **Parallel builds** | Not addressed | Bazel built-in |
| **Remote caching** | Not addressed | Bazel built-in |
| **IDE integration** | ParseCache + addDependency (eager) | ParseCache + lazy element loading |
| **IDE dep latency (10K classes)** | Load all 10K upfront | Load ~50 on demand |
| **Monorepo** | Separate artifact monorepo | Single unified monorepo |
| **ModelLayer extraction** | Required (~400 lines) | Not needed |
| **New project setup** | `legend init` + `pure-project.yaml` | `mkdir` + `BUILD.bazel` (3 lines) |

### D. Comparison with Original `addDependency()`

| Aspect | Original `addDependency()` | New per-element approach |
|---|---|---|
| Input | Full `LegendArtifact` (monolithic) | Individual element .json files |
| Loading strategy | Eager (load everything upfront) | Lazy (load on first reference) or eager |
| Granularity | All-or-nothing per dependency | Per-PackageableElement |
| Requires ModelLayer extraction | Yes (separate local/dep layers) | No (elements into same collections) |
| Requires shape extraction | Yes (separate shapes from impl) | No (each file IS the element) |
| Bazel rebuild tracking | Per-artifact (coarse) | Per-element (fine-grained via `unused_inputs_list`) |
| Name resolution | All dep FQNs pre-registered in memory | Java-style: file-exists check per candidate (scales to 1.5M files) |
| IDE latency (10K dep classes, use 50) | Load all 10K upfront | Load 50 on demand |

### E. Out of Scope

- **Incremental compilation** -- Bazel's action cache + per-element `unused_inputs_list` handles this
- **Registry server** -- `git_repository` + Bazel fetching replaces the need for any registry
- **Hot reloading** -- lazy element loading in LSP means deps load on demand (near-instant)
- **Access control** -- all exports are public; `visibility` in BUILD.bazel provides basic scoping
- **Version ranges** -- Bazel's single-version-per-target eliminates the need
- **Custom CLI** -- `bazel build/test/run` replaces `legend compile/publish/sync`

