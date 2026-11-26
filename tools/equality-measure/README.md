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
