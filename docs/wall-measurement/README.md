# The wall measurement (2026-11-06)

The user, 2026-11-06: "this IS the invariant we need to ship". Before the wall (`base/RunScope`) was made total
(`759c9e1d`), every boundary was marked report-only and measured over core's matrix and lanes, PCT channel A
(DuckDB, H2, SQLite) and channel B (DuckDB, H2, SQLite), and both corpus lanes (`//gates:corpus_duckdb`,
`//gates:corpus_h2`), at `6b23002c` plus the star fix (`ad6ad27a`).

- `wall_measure.py.txt`: the patch (applied with `python3`, from the repository's root, to the tree of that day;
  never committed). It marks `[wall-touch]` (a database touch -- a session opened or checked, a statement, a load, a
  metadata read, a transaction -- outside a plan's run; `[wall-touch-planning]` inside a planning scope),
  `[wall-nested]` (a run started inside a run) and `[run-wall-unguarded]` (a planning act -- typing, inlining, store
  resolution, rendering -- inside a run at a seam the strict wall did not guard). Every mark only reports, under
  `-Dlegend.wall.measure=true` (tests: `--jvmopt`; the corpus passes: `spec/corpus.bzl`'s `_pass_flags`).
- `wall_report.py.txt`: groups the report lines of the test logs and the corpus passes' logs
  (`bazel-bin/spec/judge_*/host.log`, `database.log`) by kind and by the nearest callers outside the seam.
- `crossings_2026_11_06.txt`: the list it printed (the walk's and the test-data generator's own sites included).

What it found, and what became of each: `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md` §7.3, and the commit
message of `759c9e1d`. Since that commit the marks are the wall's own rules, strict: a crossing the measurement
would report is now refused by name (`RunScopeTest` tries each).
