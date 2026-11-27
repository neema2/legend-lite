# Step 6B's measure: one comparison asked in every place (2026-11-27)

Made while CI ran on step 6's closing batch, by the working session itself. It replaces opinions about "how many
comparison codes are there" with a count anyone can rerun: **does `L == R` answer the same wherever it is written?**

## What it is

*The tool's code is on the scratch branch `claude/step-6-numbers` (commit `6bf041cb6`), not on the program branch:
its model text entered the parser-equivalence corpus and turned `//parser-equivalence:parser_parity` red, so it was
taken off the program branch the same evening. It comes back with step 6B's first slice, with that corpus's
recorded files regenerated.*

`tools/equality-measure` (its README, "The position matrix"): 72 comparisons, each asked in 17 places, on DuckDB, H2
and SQLite; an assert under each checker, so 19 outcomes a comparison and database; 4,104 runs in about 40 seconds.

    bazel run //core:equality_measure -- --positions <out dir>
    python3 tools/equality-measure/positions.py <out dir>

The places: a query's answer; under `!`; an `if`'s condition; an operand of `&&` and of `||`; over two `let`s; a
kept value, a filter's condition, an `exists` and a `forAll` inside a list's lambda; a side that reads the lambda's
element; inside a function whose parameters are `Any`, and one whose parameters are a type variable; a table's
filter; a table's column; `assertEquals(L, R)`; `assert(L == R)`.

The comparisons: single values of each type, exact numbers of two types and scales, dates at each written precision,
values typed only when the query runs, empty sides, written and computed lists, mixed lists, maps, pairs, instances,
tables. Nothing is expected of an answer. A comparison is counted as:
- **one answer**: every place says true, or every place says false;
- **refused everywhere**;
- **answered only in some places**: some places answer and others refuse or fail;
- **contradicts**: true in one place and false in another.

A place a database cannot answer even for `1 == 1` is that database's own gap and is left out of its counts (H2 has
four: every place inside a list's lambda).

## The counts

| | program branch today (`9adb84bf4`, before step 6) | step 6 as it lands (`b0caf8ea9`, the code of `2e684d821`) |
|---|---|---|
| DuckDB: contradicts | 19 | 9 |
| DuckDB: answered only in some places | 6 | 28 |
| DuckDB: one answer | 47 | 35 |
| H2: contradicts | 13 | 5 |
| H2: answered only in some places | 16 | 6 |
| H2: one answer (or refused everywhere) | 43 | 61 |
| SQLite: contradicts | 33 | 12 |
| SQLite: answered only in some places | 15 | 13 |
| SQLite: one answer (or refused everywhere) | 24 | 47 |

The two full reports are in `positions/`. Step 6 more than halves the contradictions on each database. **Step 6B is
done when "contradicts" and "answered only in some places" read zero on every database** (and on each server and
cloud database, once the tool runs there).

## What step 6 makes worse (found here, after the landing checks)

**On DuckDB, a comparison of two computed lists or maps that stands inside a list's lambda and reads nothing of the
lambda's element fails when the query runs:** `Binder Error: subqueries in lambda expressions are not supported`.
Example: `[1]->map(x | [1, 2]->map(y | $y + 1) == [2, 3])->at(0)` answered `true` before step 6 and is a database
error now. 23 of the 72 comparisons, in the four list-lambda places (kept value, filter, exists, forAll).

- **Cause:** since step 6 such a comparison is planned as a statement of its own and read as one value
  (`Lowerer.wholeSides`, `planner.RootComparison.insideAnExpression`). Inside a list's lambda that value is a
  subquery, which DuckDB's lambdas do not allow. (Trino, Databricks and Snowflake write list lambdas too and were not
  measured: assume the same until they are.)
- **Reach:** the comparison must not read the lambda's element, so it is a constant of the statement; no test of
  core, PCT or either corpus holds one, which is why every lane was green. It is an error, never a wrong answer.
- **Fix:** the comparison computed once, outside the lambda, and read by name inside it. That is step 6B's own rule
  ("each side computed once and named"), so it is the first piece of 6B, with this measure as its check.

## What is still wrong at the landing (step 6B's work list, as measured)

Contradictions at `b0caf8ea9`:

| Comparison | Where it differs | Databases |
|---|---|---|
| `1->cast(@Number) == 1.0->cast(@Number)`, `1->cast(@Any) == '1'->cast(@Any)`, a day against a date-time under `cast(@Any)` or `cast(@Date)` | false everywhere, but **true over two `let`s** (and where a side reads the lambda's element, on DuckDB). Already so before step 6 | DuckDB, H2, SQLite (`Number`); DuckDB, H2 (the others) |
| `%2020 == %2020`, `%2020-01 == %2020-01`, `date(2015, 4, 16, 14) == %2015-04-16T14:00:00` | true everywhere, **false where a side reads the lambda's element** | DuckDB, SQLite |
| `[1.0D, 10.125D] == [1.000D, 10.125D]` (written lists) | `assertEquals` passes, **the plain `==` is false** | H2, SQLite |
| `%2020-01-01T10:00:00.000 == %2020-01-01T10:00:00` | `assertEquals` passes, **the plain `==` is false** | SQLite |
| `1 == 1.0` | a compile error everywhere, but **true where a side reads the lambda's element and false inside a function of `Any`** | DuckDB, SQLite |
| `[1, 2]->cast(@Number) == [1.0, 2.0]->cast(@Number)` | false everywhere, **true where a side reads the lambda's element** | DuckDB |
| a computed list of Decimals at two scales; nested lists; two tables whose rows differ in order; an optional value against a value inside a filter | one place disagrees with the other eighteen | SQLite |

Each row is a place where the comparison of two single values (`lowering/Equalities`, `lowering/TaggedLists`), the
comparison of two whole sides (`lowering/SideEqualities`) and the test checker's own path (`testplan/DatabaseJudge`)
still decide differently. My reading of that code, file by file, is in `reading-notes-2026-11-27.md`.

## Limits

- Three in-process databases only. The server and cloud databases are not measured yet.
- `==` only. `contains`, `in`, `indexOf`, the deduplications, ordering and `hasSameElements` are rows to add.
- The places are run through the test door (`TestExecution.runProgram`), which plans a body with no assert as a
  product query; the product's own door is not called directly.
- The "reads the lambda's element" place passes the left side through a filter that keeps everything
  (`L->filter(e | $x == 0)`), so it also measures a value that has passed through a filter.
