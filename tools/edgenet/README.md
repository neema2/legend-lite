# The edge golden net

The net pins what the compatibility edge answers, every digit and every row order, so that a change which moves an
upstream audience's answer is seen as a listed move. It records PCT channels A and B on DuckDB (5 suites), H2
(relation) and SQLite (5 suites); the corpus's host ledgers on DuckDB and H2; pure/v1's answers and plan text; and the
missing cases (`EdgeCasesGoldenTest`). Each recording is diffed against its golden.

- **Where it runs.** `//gates:edge_net` runs on every push, on linux, macOS and Windows (CP-9, ruled by the user
  2026-11-19). It covers PCT, pure/v1 and the missing cases. The corpus ledgers' diff tests run in `//gates:corpus_duckdb`
  and `//gates:corpus_h2`, beside the host passes they read. `//pct:edge_contract_test` is the whole net, for a hand
  run.
- **Re-recording.** A ruled move re-records with `bazel run //pct:update_edge_contract`,
  `//spec:update_edge_corpus`, `//core:update_edge_pure_v1` or `//core:update_edge_cases`, citing the ruling in the
  commit.
- **This directory.** `EdgeNormalize` puts each PCT recording in one order before the diff: rows in order across
  distinct sort keys, sorted only within ties. The PCT module holds no ordering of its own (`PctDisciplineTest`).
  `PlatformOverlay` applies the platform overlays (below). Their planted-change proofs are
  `//tools/guards:edge_normalize_test`.

## The platform overlays

The goldens are Linux's. Measured on PR #123's first Windows run (2026-11-19), the one platform difference is this:
- DuckDB and SQLite take `sinh`, `cosh`, `tanh`, `cbrt` and `pow` from the platform's C maths library (glibc on
  Linux, the MSVC runtime on Windows), so lite's answers for those differ in the last place between Linux and Windows;
- the two databases' hyperbolic results agree with each other on each OS;
- H2, which computes in Java, does not differ. Nor does any row order, locale, path or line ending.

The net once hid this: until S0 (2026-11-18) its mask hid a Float's 16th digit on.

Every digit stays pinned on every OS. `pct/src/test/resources/edge/overlay_<os>.tsv` holds, for one OS, exactly
those values, one row per fact: suite, test, case, databases, function, the Linux value and that OS's value.
- On its own OS, each row rewrites its line from that OS's value to Linux's before the diff against the Linux golden.
  A row that finds no line, or a line with another value, fails the run.
- On every other OS, each row's Linux value is checked against the recording. On Linux that is the golden's value,
  so an overlay cannot drift from the golden.
- Any difference no row names still fails the diff test.

The overlay files are pinned like the goldens: a row moves only by a ruled re-record.

| OS | overlay | rows |
|---|---|---|
| Windows | `overlay_windows.tsv` | 17: `sinh`, `cosh`, `tanh` (13, on both databases), `cbrt` (3), `pow` (1) |
| macOS | none yet: its first `edge_net` run decides | - |
| Linux arm | unmeasured: no `edge_net` lane runs on arm | - |
