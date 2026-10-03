# Bazel audit, 2026-10-02: the evidence

This folder is the evidence behind [`../BAZEL_FIRST_CLASS_PLAN_2026_10_02.md`](../BAZEL_FIRST_CLASS_PLAN_2026_10_02.md). The goal is a build that is fully Bazel first class: no scripts, hand-run recipes or host tools in the build; every derived file produced by an action and diff-tested; hermetic tests; policy in BUILD files.

## How it was made

The audit was done at `origin/main` @ `16c8120d5`, by seven auditors working in parallel, each over one slice.

- **Read in full:**
  - every Bazel file;
  - every test and tool source file;
  - every harness, including all of `datacube/demo/verify-features.mjs`;
  - every script's header and I/O;
  - CI;
  - the human-facing build documentation.
- **Swept, not read line by line.** Product code (`core/src/main`, `datacube/src`, `query/src`) was swept with grep patterns, with the surrounding code read for every hit. Each report states its own coverage, including what it only skimmed.
- **Verified by the plan's author.** The most serious claims were checked directly in the code. The plan lists them in Part 1. The rest are the auditors' readings, each with `file:line` evidence.
- **Not executed.** Nothing was run, apart from read-only `bazel query`/`cquery`/`mod` commands and one in-memory comparison of stress files against their generators.

The reports describe the tree at `16c8120d5`. The plan's Part 0 records which items PR #14 (`23b441852`) has since changed.

## Contents

| File | Slice |
|---|---|
| [`01-core-tests.md`](01-core-tests.md) | `core/src/test`, `core/src/main/duckdb`, `core/BUILD.bazel` |
| [`02-spec-pct-parser-equivalence-tools.md`](02-spec-pct-parser-equivalence-tools.md) | `spec/`, `pct/`, `parser-equivalence/`, `testing/`, the Java under `tools/` |
| [`03-warehouse-wasm-core-main.md`](03-warehouse-wasm-core-main.md) | `warehouse/`, `wasm/`, `tools/teavm`, `json/`, `base/`, a sweep of `core/src/main` |
| [`04-datacube-protocol-store.md`](04-datacube-protocol-store.md) | `datacube/` (BUILD, `src`, `test`, `tools`, `bench`), `pure-protocol/`, `query-store/` |
| [`05-harnesses-query-site.md`](05-harnesses-query-site.md) | `datacube/demo` harnesses, `query/`, `site/`, `fixtures/` |
| [`06-scripts-docs-ci.md`](06-scripts-docs-ci.md) | `scripts/`, non-Java `tools/`, scripts under `docs/`, `experiments/`, `repro/`, `projects/`, CI workflows, the human-facing docs |
| [`07-bazel-files.md`](07-bazel-files.md) | An independent fresh-eyes review of every Bazel file, grounded with `bazel query`, `bazel mod` and `--nobuild` runs under upcoming incompatible flags |
| [`spikes/`](spikes/) | Time-boxed experiments that test the plan's riskiest assumptions, each ending in GO / NO-GO with evidence |
| [`script-review.md`](script-review.md) | One row per script, with a recommendation; the user decides each row |

Severity throughout: **P0** breaks hermeticity or correctness; **P1** is a non-Bazel workflow; **P2** is hygiene.
