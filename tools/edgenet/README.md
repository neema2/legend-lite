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

The goldens are Linux's. Measured on the first Windows and macOS runs of the net (PR #123's and #124's, 2026-11-19),
the one platform difference is this:
- DuckDB and SQLite take some maths functions from the platform's C maths library (glibc on Linux, the MSVC runtime
  on Windows, macOS's libm), so lite's answers for them differ in the last place: on Windows `sinh`, `cosh`, `tanh`,
  `cbrt` and `pow`; on macOS `acos`, `asin`, `cot`, `sinh`, `cosh` and `cbrt`. Each moved value is 1 or 2 units in
  the last place on Windows, and exactly 1 on macOS;
- the two databases' results agree with each other on each OS, except for `cbrt` and `pow`, which differ by database;
- H2, which computes in Java, does not differ. Nor does any row order, locale, path or line ending.

The net once hid this: until S0 (2026-11-18) its mask hid a Float's 16th digit on.

Every digit stays pinned on every OS. `pct/src/test/resources/edge/overlay_<os>.tsv` holds, for one OS, exactly
those values, one row per fact: suite, test, case, databases, function, the Linux value and that OS's value.
- On its own OS, each row rewrites its line from that OS's value to Linux's before the diff against the Linux golden.
  A row that finds no line, or a line with another value, fails the run.
- On every other OS, each row's Linux value is checked against the recording. On Linux that is the golden's value,
  so an overlay cannot drift from the golden.
- An OS's own rows apply before the other OSes' rows are checked: two OSes can move the same fact (both move
  `testSinH_Eval` 003), and the other OS's row finds Linux's value only after this OS's row has rewritten the line.
- Any difference no row names still fails the diff test.

The overlay files are pinned like the goldens: a row moves only by a ruled re-record.

| OS | overlay | rows |
|---|---|---|
| Windows | `overlay_windows.tsv` | 17 facts, 30 lines: `sinh`, `cosh`, `tanh` (13, on both databases), `cbrt` (3), `pow` (1) |
| macOS | `overlay_macos.tsv` | 16 facts, 31 lines: `sinh`, `cosh` (12, on both databases), `acos`, `asin`, `cot` (3, on both), `cbrt` (1, DuckDB) |
| Linux arm | unmeasured: no `edge_net` lane runs on arm | - |

## The corpus's platform overlay

The relational corpus's goldens are Linux's too (measured 2026-11-26 on PR #141's macOS run 45788970603, approved by
the user the same day). On macOS, DuckDB's `acos`, `asin`, `atan2` and `tan` differ in the last place. A corpus test
compares the answer with a value written in the test, which is Linux's, so on macOS the test FAILS. No value moves; three
files do: the host ledger, the fail roster and the unordered register. `CorpusOverlay` reads
`spec/src/test/resources/rcorpus/overlay_<os>.tsv`, one row per test: test, databases (corpus lanes), function, the
Linux value and that OS's value.
- On its own OS, a row requires exactly the flip: one ledger line with the first assert failing and naming both values
  once, the test in the fail roster, and no unordered-chain line. Then it puts the Linux golden's lines for the test
  back in their place. Anything else fails the run.
- On every other OS, a row requires the test to pass in the recording. On Linux that is the golden, so a row cannot
  outlive its fact.
- It sits between the host pass and the diff tests (`spec/corpus.bzl`'s `corpus_overlay`), and is no input of the pass:
  an overlay edit reruns no corpus. Its planted-change proofs are in `//tools/guards:edge_normalize_test`.

| OS | overlay | rows |
|---|---|---|
| macOS | `overlay_macos.tsv` | 5 tests, DuckDB: `acos`, `asin`, `atan2` (1 each), `tan` (2) |
| Windows | none: the corpus did not move | - |
