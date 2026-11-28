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

## The second run: slice 5's code (`568c12139`), with each failing place's error text (about 05:30 UTC)

`Postgres: 162 right everywhere of 210, 26 answered only in some places, 15 failing somewhere, 2 wrong somewhere,
5 contradicting.` The functions matrix is as on DuckDB now (an Integer is no longer found among Floats, a day
among date-times; a date built to the hour is found): slice 5's changes hold on Postgres.

The errors, verbatim from the database:

| Error | Where |
|---|---|
| `operator does not exist: bigint[] = numeric[]` | an Integer list against a Decimal list where both sides read the element (five rows); `list([1, 2]) == list([1D, 2D])` in almost every place |
| `operator does not exist: text[] = character varying[]` | a computed list of Strings against a written one, both reading the element |
| `operator does not exist: integer[] = bigint[]` | a list with an empty slot against a written list, both reading the element |
| `CASE types jsonb and character varying cannot be matched` | `contains` over a computed list of pairs |

All four are one thing: **Postgres compares two arrays only of ONE type, and lite writes the instruction over two
list values there as a plain `=`** (task 32: Postgres's own writing, both sides cast to one array type).

Wrong answers on Postgres:

| Row | Place | Answer | Note |
|---|---|---|---|
| `[1, 2]->filter(x \| $x > 1)->first() == 2` | inside a list's filter | false | the statement planned for Postgres in-process reads right; the SQL the lane's run sent is not captured yet (the tool's `--sql` does not plan for a server) |
| two tables, rows in another order; a column's name differs | both sides reading the element | true | the older lowering of a table comparison where it stands (rows numbered by a sort over every column, names not compared); DuckDB refuses the place by name |
| `[1, 2]->at(5)->cast(@Number) == 1.0->cast(@Number)` | both sides reading the element | false, where it must raise | "never equal, both evaluated" does not make Postgres evaluate the side there |
| a `map` over a list built with empty slots | everywhere | as on every database | task 33 |

None of these is slice 5's or slice 6's; they are Postgres's own piece of work, to do next with the lane measuring
each change (its workaround now sets `TEST_TMPDIR` for the binary alone).
