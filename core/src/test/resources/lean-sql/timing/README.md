# The lean program's standing timing set (E-SQL item 4, 2026-11-16)

One before/after pair per form the lean program landed, per in-process database (`pairs-<Database>.jsonl`, each line
`{"case", "before", "after"}`): the form it replaced and the form it writes now, over data the statement makes itself
(DuckDB's `range`, H2's `SYSTEM_RANGE`, SQLite's recursive CTE), so the pair needs no load. "None slower" is checked by
a command:

    bazel run //core:product_sql_measure -- time $PWD/core/src/test/resources/lean-sql/timing <out dir>

It writes `timing.tsv` (each pair's characters and median times, and whether the rows are the same) and prints a
summary per database. A pair whose rows differ, or whose "after" is slower beyond noise, is a defect of the form.
Run once per staged PR (E-v2 ...), and add a pair with each form a step lands.
