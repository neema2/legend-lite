# tools/metrics: the baseline

The compiler plan's W1.0b (`docs/EXECUTION_PLAN_2026_09_26.md`), as amended by D25: the stress corpus and the
eager probe are the measure, nothing new is built to measure. One receipt, in markdown, of the numbers every later
compiler push must move; its first copy is `docs/build-inventory/program/evidence/compiler/BASELINE_2026_10_08.md`.

```
bazel build //spec:eager_corpus_compile //wasm:planner      # the two outputs the receipt reads from bazel-bin
bazel run //tools/metrics:baseline -- [--out FILE] [--skip-latency] [--probe FILE] [--wasm FILE]
```

Run it alone on a quiet machine: it times things, so it is never a test (the core lane only builds it). The receipt
records the load averages at its start; whether the machine was otherwise quiet is stated in the GATES entry that
pins the receipt. The sections, and where each number comes from:

1. **Product lines**: Java under `*/src/main` per top-level directory with a total (rule 0b.17's number, counted
   as `wc -l` counts), and per core package; the other source files there (`.pure` resources such as the prelude,
   `.ts`, `.py`, `.bzl`) in their own table.
2. **The whole-world compile**: `//spec:eager_corpus_compile`'s output, read from `bazel-bin` (every body of
   core_relational's world typed: bodies, failures by reason, the build and typing milliseconds as recorded when
   that output was produced, which may have been beside other actions, in another worktree or on CI through the
   shared caches; the alone number is a Flight Recorder run of the probe, see the evidence folder's profiles).
3. **Compile-only latency per query**, no database: `//core:compile_latency` run from the runfiles, over the stress
   corpus's service tests (each built exactly as the service test runner builds it and planned for the service's
   declared runtime) and over the DataCube-shaped set `wasm/corpus/queries.tsv`; four stages per query (names, or
   parse+names for the set; type; lower = inline + store resolution + lowering; render), p50/p95/p99/max/mean/sum
   of the last pass (the model's demand caches filled: a warm session's query), the first pass's wall clock and its
   first case for the cold number. Percentiles are the value at rank floor(p·(n−1)) of the sorted times. The
   per-query timings go to a temporary folder named on stderr; they are not part of the receipt.
4. **The reference lane's buckets**, read from the committed golden
   (`spec/src/test/resources/reference-lane/core_relational.txt`; never re-run for metrics).
5. **The corpus rosters' sizes** (`spec/src/test/resources/rcorpus/*-roster.txt`).
6. **The planner module's bytes** (`//wasm:planner`, read from `bazel-bin`).

The phase shares (where the time goes inside a compile) are not here: they are the Flight Recorder profiles in
`docs/build-inventory/program/evidence/compiler/` (`jfr_phases.py` and its companions), taken by hand.
