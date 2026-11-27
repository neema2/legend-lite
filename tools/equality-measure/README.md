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
    python3 tools/equality-measure/positions.py <out dir>

`positions.tsv` holds every outcome as it came; `positions.md` counts, per database, the comparisons with one answer
everywhere, those refused everywhere, those **answered only in some places** and those that **contradict** (true in
one place and false in another), lists each of the last two with its places, and lists any place that answers
differently on two databases. The comparisons and the places are the two lists in `EqualityMeasure.java`
(`sidesList`, `positionList`): add a row to either to widen the measure. "One equality" is done when the last two
columns read zero on every database.

## The product's SQL between two commits

`tools/equality-measure/sql-changes.sh <commit A> <commit B> <out dir>` (the tree clean; it checks each commit out
detached, lays this harness over it from `BASE`, runs `bazel run //core:sql_dump`, and returns the tree). The dump
plans every query of `core/src/test/resources/lean-sql/` by the product's one entry (`TypedQuery.executionPlan`, lite's
own door): `channel_a.jsonl` on DuckDB, H2 and SQLite, `core.jsonl` on each query's captured runtime. `sql_diff.py`
writes `sql-changes-<A>-<B>.md`: (i) SQL text differs, (ii) refused before and planned now, (iii) planned before and
refused now, each grouped by cause (acos/asin set apart), the queries whose change differs in kind across databases,
and the totals. `--dirs <dump A> <dump B> <out>` diffs two dumps already taken.
