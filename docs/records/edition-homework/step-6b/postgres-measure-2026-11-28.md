# The position matrix on Postgres, first run (2026-11-28, slice 3's code `4b77dadaf`)

*Run by the Postgres lane (measure only), values and functions matrices; the store matrix is in-process only. The
suites of that lane (PCT both channels on Postgres and Trino, the three time-series tests): no outcome differs.*

`Postgres: 107 right everywhere of 139, 15 answered only in some places, 12 failing somewhere, 5 contradicting.`

How it was run (the documented command alone fails: the measure's target carries no embedded Postgres): the lane
copied Bazel's pinned Postgres 16 to a directory its unprivileged user reads and ran
`TEST_TMPDIR=<dir> bazel run //core:equality_measure --run_under="<as_unprivileged> ubuntu" --
--jvm_flag=-Dmeasure.backends=postgres --jvm_flag=-Dembedded.postgres.root=<copy>/PG_ROOT --positions <dir>`.
To make the documented command work the target needs the embedded Postgres as data and that flag: not done.

## What is wrong on Postgres, and why (the SQL read by planning the same bodies for Postgres in-process)

| What | Rows | Cause |
|---|---|---|
| Two lists compared inside a list's function, each side reading the element: fails when the query runs | an Integer and a Decimal list, a list typed `Number`, a computed list of Strings, a list with an empty slot | Postgres holds typed arrays and compares two of ONE type; the instruction's writing for two list VALUES there is the base's plain `=`, and `bigint[] = numeric[]`, `text[] = varchar[]` have no operator. **Postgres needs its own writing of the instruction over two values: both cast to one array type** (the plan's "Postgres's list writing", never built). |
| Two tables compared inside a list's function, each side reading the element: TRUE where the rows are in another order, or a column's name differs | `tables rows other order`, `tables a column's name differs` | an older lowering of a table comparison where it stands (its rows numbered by a sort over every column, the names not compared). DuckDB refuses the place by name; Postgres runs it. Slice 4's. |
| `[1, 2]->filter(x \| $x > 1)->first() == 2` false inside a list's filter | `optional one and value` | not yet read (the statement planned for Postgres on slice 5's code looks right: to be run there). |
| `contains`, `indexOf`: an Integer found among Floats; a day found among date-times; a date built to the hour fails | the functions matrix | slice 5's subject (measured on slice 3's code here). |
| The written empty list, a mixed list, maps inside a list's function: refused by name | several | as on DuckDB. |

Next: the same measure on slice 5's final commit, with each failing place's error text; then Postgres's writing of
the instruction over two values.
