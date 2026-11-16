# The lean program's standing timing set (E-SQL item 4, 2026-11-16)

One before/after pair per form the lean program landed, per in-process database (`pairs-<Database>.jsonl`, each line
`{"case", "before", "after"}`): the form it replaced and the form it writes now, over data the statement makes itself
(DuckDB's `range`, H2's `SYSTEM_RANGE`, SQLite's recursive CTE), so the pair needs no load. "None slower" is checked by
a command:

    bazel run //core:product_sql_measure -- time $PWD/core/src/test/resources/lean-sql/timing <out dir>

It writes `timing.tsv` (each pair's characters, median times, and its verdict: `same`, `SLOWER` -- the after more than
15% and more than a millisecond longer, medians of 15 runs -- `differ`, `after-failed`, or `before-raises`, a guard's statement counted but
not timed) and prints a summary per database; it exits non-zero when any pair is slower, differs or fails (E-v2's
review, S1). Each form is timed at two sizes (a `-large` twin: 3M-20M rows on DuckDB, 3M on H2 and SQLite, but H2's memo pair at 100K rows and 500 groups: H2 joins two derived tables as a nested loop, so the 5,000-group form ran past half an hour, 2026-11-16). Postgres
is not in-process: its forms are timed on the unprivileged server and recorded in the plan's decision log. Run once per
staged PR (E-v2 ...), and add a pair with each form a step lands.
