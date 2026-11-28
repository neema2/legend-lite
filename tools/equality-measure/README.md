# The equality measurement (step 6.0 of the edition program)

Every assert-holding test body run as written and rewritten to its plain comparison
(`docs/records/edition-homework/r3/research-step6-equality.md` §4), under the HOST and DATABASE judges, on DuckDB, H2
and SQLite. Tooling only: it edits no product code and is no test (its target `//core:equality_measure` is manual).

**Run it all:** `tools/equality-measure/run.sh <out dir> [<body id regex>]` (about 5 minutes after a warm build;
the first run clones the pinned upstream `.pure` files into `<out dir>/upstream`). It writes, in `<out dir>`:

| File | What |
|---|---|
| `bodies.tsv` | every body collected: id, source file, status (`rewritten`, `unchanged`, `excluded:<why>`), imports, model, both forms |
| `results.tsv` | one row per body, database, form, judge: `PASS`, `FAIL`, `UNJUDGED`, `REFUSED`, `ERROR`, `TIMEOUT`, `SAME` (an unchanged body's rewritten form), with the first line of the message |
| `census.tsv` | each comparing call of each body (assert pairs, `==`, `eq`, `is`, `contains`, `in`, `indexOf`) with its two sides' static types |
| `probes.tsv` | the product's own answers to the fixed expressions of the brief's 4e, per database |
| `helpers.txt` | how many bodies and PCT tests hold asserts inside helper functions |
| `summary.md` | the report: 4a to 4e |

**The pieces**, each runnable alone:
- `collect.py <repo> <out> <pure src> <engine src>`: the bodies. Core: every string literal (adjacent `+` literals
  joined) that is a lambda holding an assert, in the test files that run a body through the test door; its model the
  file's text block declaring every element the body names. PCT: each `<<PCT.test>>` function, `$f->eval(|e)` replaced
  by `e`, the file's and its imported test packages' declarations as the model, its imports as the body's scope.
- `rewrite.py`: the rewrite, on the text (brackets and quotes respected, recursive into lambdas); a body holding a
  non-plain assert (JSON, TDS, error asserts, `assertIs`, instance-of) is left out and counted by name.
- `//core:equality_measure` (`core/src/test/java/com/legend/equalitymeasure/EqualityMeasure.java`): runs each body
  through `TestExecution.planBody` (a throw is `REFUSED`) and `TestExecution.runProgram` on a fresh session per use.
- `helpers.py`, `report.py`: the census and the summary.

Compare two commits by running each into its own `<out dir>` and diffing `results.tsv` (sorted).

## The position matrix (step 6B's measure)

One comparison, `L == R`, asked in every place a comparison can stand, per in-process database: a query's answer,
under `!`, an `if`'s condition, an operand of `&&` and `||`, over two lets, a kept value and a condition inside a
list's lambda, a side that reads the lambda's element, inside a function whose parameters are typed when it is called
(`Any`, and a type variable), a table's filter and a table's column, and a test's `assertEquals` and `assert` under
each judge. Nothing is expected of an answer: the measure is whether the answers agree.

    bazel run //core:equality_measure -- --positions <out dir> [<comparison name regex>]

`positions.tsv` and `positions-store.tsv` hold every outcome as it came. The report is written by the same run
(`PositionReport`; `--report <out dir>` writes it again from a run already made):

- `positions.md`: per database, how many comparisons are right everywhere, refused by name everywhere, answered only
  in some places, failing somewhere, wrong somewhere, or contradicting themselves (two answers in two places), then
  each comparison that is not right everywhere with its places;
- `positions-counts.tsv`: those counts, one line per database;
- `positions-kinds.tsv`: one line per comparison that is not right everywhere. This is the form the pin holds.

Each comparison carries the answer the equality rule gives it (`T`, `F`, the ids of a store's rows, `N` for never
true, `?` where the rule does not say). The comparisons and the places are the lists in `EqualityMeasure.java`
(`sidesList`, `positionList`, `storeSidesList`, `storePositionList`): add a row to widen the measure. "One equality"
is done when every comparison is right everywhere on every database.

**The pin.** `//core:equality_positions_pin_test` (CI's core lane) runs the matrix and requires its
`positions-kinds.tsv` to equal `positions-baseline.tsv` line for line. A comparison that gets worse fails the test;
one that gets better fails it too, until the baseline is regenerated in the same commit, so every move of the measure
is a line of a reviewed diff:

    bazel run //core:equality_measure -- --positions <out dir>
    cp <out dir>/positions-kinds.tsv tools/equality-measure/positions-baseline.tsv

The baseline is a guard file: a line that says a comparison got worse moves only with a dated reason in the commit.

**On a server database** the matrix does not run yet: its runtime is the storeless one, which a server's connector
declares only with a server's coordinates, so the measure has to start the server and declare it. Until it does, the
in-process databases (DuckDB, H2, SQLite) are what the measure and its pin hold.

**Two probes** over the matrix's model (`positions-model.txt`), per database: `--sql '<body>'` prints the product's
statement for a query; `--assert-sql '<body>'` prints the statements a test body sends under the database judge.

## The product's SQL between two commits

`tools/equality-measure/sql-changes.sh <commit A> <commit B> <out dir>` (the tree clean; it checks each commit out
detached, lays this harness over it from `BASE`, runs `bazel run //core:sql_dump`, and returns the tree). The dump
plans every query of `core/src/test/resources/lean-sql/` by the product's one entry (`TypedQuery.executionPlan`, lite's
own door): `channel_a.jsonl` on DuckDB, H2 and SQLite, `core.jsonl` on each query's captured runtime. `sql_diff.py`
writes `sql-changes-<A>-<B>.md`: (i) SQL text differs, (ii) refused before and planned now, (iii) planned before and
refused now, each grouped by cause (acos/asin set apart), the queries whose change differs in kind across databases,
and the totals. `--dirs <dump A> <dump B> <out>` diffs two dumps already taken.
