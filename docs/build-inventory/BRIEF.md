# Build inventory deep dive: the brief every area follows

## Why
legend-lite's Bazel build has grown into a mess. Compiling all our Java should take about 30 seconds from clean,
but `bazel build //...` does far more than compile: it runs dozens of programs, rewrites committed files and runs
corpus checks, and it reruns most of that on every engine edit. We are rebuilding the build's shape from first
principles. Step one is a complete, exact account of every target: what it is, what it needs, what needs it, and
what change should make it run. This is NOT a timing exercise and NOT a sample. Every target in your list gets a row.

## Where
- Repo (read it, never change it): the bazel/exec checkout (branch bazel/exec = main + local Phase 4 commits)
- Facts already extracted from Bazel's graph, in runs/inventory/ (in the audited checkout):
  - `areaN_targets.tsv`: YOUR targets (columns: label, kind, macro, manual, testonly, size, nsrcs, ndeps, nusers, reaches, loc)
  - `inventory.json`: every target's kind, tags, srcs, outs, data, direct deps and direct users (within our packages)
  - `reach_<x>.txt`: every target that transitively depends on x. The `reaches` column lists them:
    upstream_src (legend-engine/legend-pure source archives), upstream_jars (@maven_upstream/@maven_runner),
    oracle_pins, core_libs (any non-test //core java_library), core_next, teavm, native_image, wasm_planner,
    npm_any, chromium, python, projects, warehouse_server
  - `targets.jsonl`: Bazel's raw `query --output=streamed_jsonproto` of everything
- The CI lanes: .github/workflows/gates-run.yml (lane matrix near line 45-66; the `build` lane runs `bazel build //...`)
- The local gate: gates/BUILD.bazel (`//gates:local`)
- History and intent: `git -C <repo> log` on the files; the workplan at
  docs/ (plan branch) BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md and the log
  docs/BAZEL_EXECUTION_LOG.md beside it

## Rules
- Read-only. Never edit the repo. Never run `bazel build`, `bazel test` or `bazel run` (other work shares this
  machine). You MAY run `bazel query`, `bazel cquery` and `bazel aquery` in the repo dir (use `env -C <repo> bazel ...`,
  never `cd`), git, grep, and read any file.
- No guessing. Every claim cites evidence: `path:line`, a query you ran (give it), or a git commit. If you cannot
  settle something, write OPEN and say exactly what would settle it.
- Cover EVERY target in your areaN_targets.tsv. Targets that are mechanical copies of one pattern (e.g. 56 model
  projects with the same three targets, or one macro's expansion) may share a row IF you name every member and
  show they are identical in shape (same macro, same deps pattern).
- Write your report to runs/inventory/areaN_report.md (the only file you write). Return to the caller only a
  10-line summary: counts per verdict and the 3-5 most important findings.

## The report
### Part A: one row per target (or per proven-identical group)
| target | kind | what it is (one line, cite where you learned it) | what it reads that matters (source dirs, generated inputs, upstream, other generators) | what it produces | who uses it (targets, tests, lanes, humans via `bazel run`) | what change SHOULD make it run | what makes it run TODAY (in `bazel build //...`? in `//gates:local`? which CI lane? manual?) | verdict | note |

Verdicts (use exactly these words):
- COMPILE: compiles product code people ship or run (Java library/binary, TS bundle, wasm, native). Belongs in the everyday build.
- TOOL: compiles a program whose only job is to run inside another action or by hand. Should build only when something that needs it builds.
- GEN-BUILD: a generator whose output the build itself consumes (not committed). Name the consumer.
- GEN-COMMITTED: a generator whose output is committed to the tree (write_source_file/diff_test pair). State its TRUE trigger (upstream bump / grammar change / spec change / corpus change / engine change / never) and whether that trigger is what actually reruns it today.
- CHECK-DIFF: a diff test that compares a committed file with a generator's output.
- CHECK-GUARD: a policy/structure check (guards, layering queries, lock checks, reports).
- TEST-UNIT, TEST-INTEGRATION, TEST-BROWSER, TEST-CORPUS, TEST-STRESS: tests by kind. Say what code change should run each.
- WIRING: filegroups, aliases, config_settings, test_suites, launchers: name what they group and whether that grouping is still used.
- DEAD: nothing uses it, no lane runs it, no doc tells a human to run it. Prove all three (users = 0 per inventory.json AND no reference in .github/, gates/, docs, READMEs; give the grep).
- OPEN: you could not settle it; say what would.

### Part B: the problems in your area, each with evidence
For example: a generator whose true trigger is an upstream bump but which depends on all of core, so every engine
edit reruns it; a committed file regenerated in every build; a test hidden inside a build action; a tool compiled by
`bazel build //...` that nothing runs; duplicated compiles; things in the wrong package.

### Part C: the right shape for your area
How this area should be carved, from first principles, so that (1) compiling everything we ship stays fast and
contains only compiles, (2) generators run only when their true trigger changes and say so, (3) tests run where and
when their kind says, (4) nothing depends on more than it needs. Be concrete: name targets, the changes, and anything
you depend on another area deciding. Mark each proposal with the evidence that supports it. No timings needed.
