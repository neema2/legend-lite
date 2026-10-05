# wrongrows: legend-engine's rows against legend-lite's, on the seed data and on damaged data

Plan `docs/EXECUTION_PLAN_2026_09_26.md` §4 Phase 1 step 2, ruled D23 (2026-09-29). The base is the stress
corpus (`core/src/test/resources/stress`, engine grammar, 4,745 service tests). The seed `###Data` elements are
never edited; a damaged data set is a separate file whose elements REPLACE the seed of the same name at load time,
on both sides. Every test runs on both, and the two engines' rows are compared as multisets.

## The two runners, rows mode

The engine (the real legend-engine at the pinned release, `tools/engine-runner`):

```
bazel build //tools/engine-runner:testable
bazel-bin/tools/engine-runner/testable <project files in order> <stress files> \
    --testable=stress::S1 --testable=stress::S2 ... \
    --rows=<dir>                       # every test's actual rows, one <suite>___<test>.rows.json each
    [--data=<damaged.pure> ...]        # elements here replace the model's of the same path
```

`--rows` replaces every `EqualToJson` expectation with a sentinel before the run, so the framework reports each
test's ACTUAL; the summary counts total and errored, never passed. Project files first, in
`StressCorpus.LINKED_PROJECTS` order (model, store, mapping per project), then the stress files.

Lite (`//core:stress_tool`, the same suites through `ServiceTestRunner`; the gate's knobs moved here, Bazel workplan
P3-12):

```
bazel run //core:stress_tool -- --rows <dir> [--data <damaged.pure>[,<more.pure>]] [--only <substring>] [--backend h2]
```

With `--data` the tool judges nothing (the corpus expectations describe the seeds); the rows are the output.
The override works because `Compiler.compileModel(List<ModelSource>)` keeps the FIRST definition of an element and
reports the dropped one; the override files go first.

## The comparison

```
python3 tools/wrongrows/compare.py <engine-rows-dir> <lite-rows-dir> [--report out.tsv]
```

Classes: `EQUAL`; `SPELLING` (equal once numbers and midnight timestamps are normalised: a registered difference, not
a defect); `COUNT`; `VALUES` (the wrong-rows candidates); `SHAPE` (one side is not a row list); `ENGINE-ONLY` /
`LITE-ONLY`. Every `COUNT` and `VALUES` row is attributed to a stage (H, I or J) with the files a fix would touch;
that list orders Phase 3.

## Damaged data

`damage.py` (next slice) writes a `###Data` file from the seeds: orphans on every join, NULL in every nullable column,
duplicate keys, several milestone versions with boundary dates, ties on sort keys, empty tables, non-BMP strings,
extreme numerics. Deterministic; regenerable; never written by hand.
