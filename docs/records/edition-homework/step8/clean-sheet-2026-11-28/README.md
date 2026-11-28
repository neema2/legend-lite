# Step 8, taken back to a clean sheet (2026-11-28)

*Written by the step 8 session after the user stopped the work ("take a giant step back... do the real homework and
research with real examples from the corpus and PCT so it's grounded in reality"). It records what step 8 is for, the
five tenets the user accepted, what was measured on real tests that day, and what step 8 needs from step 7 (the
doors). Nothing here is built. A session doing step 7's research reads this file; it can also message the step 8
session (`session_01XmdJ7kbAUzi9qn2bcF72eY`, the same account).*

## 1. What step 8 is for

Lite prints values its own way: a Float with six decimals, a date-time as ISO text ending in `Z`, tables in its own
layout. legend-pure's and legend-engine's tests compare against text written their way. Step 8 takes the imitation
of that text out of the product's SQL. Where a test actually looks at upstream's text, Java at the edge writes it
from the typed value the database already returned. Everywhere else the text is lite's own, and the difference is a
register row.

## 2. The tenets (the user, 2026-11-28: "I like the tenets")

1. **The product's SQL never imitates upstream's text.**
2. **Nothing is run in order to print.** Java prints from the typed result of a statement that runs anyway.
3. **Upstream's text is written only where a test compares text:** the answer of a query, or one side of an assert.
   Anywhere else it is lite's text, with a register row.
4. **One printer per upstream format, in one place.** No per-function special case unless a real test needs it.
5. **What legend-engine sends to the database is meaning, not printing** (its cast of a Float or a date-time inside a
   store query; its `TDSNull` word). Those are rows of the door's table.

## 3. What was measured (2026-11-28, on `claude/edition-step8` at `dd169bec`, the program branch `c7babd506` merged in)

### 3.1 Where real tests print (counted by script over the pinned sources; the full reports are beside this file)

| | Tests | The print is the whole answer of a query | The print is compared in an assert | The print is per row, or inside the query |
|---|---|---|---|---|
| PCT (`census-pct.md`) | 1,357 | 97 places in 34 tests, all literal operands (`134.21e6->toString()`), all in legend-pure's `toString.pure`, `format.pure`, `joinStrings.pure` | 478 places in 457 tests; 432 are `$res->sort(...)->toString()` of a table against a `'#TDS...'` text | 60 places in 48 tests |
| The relational corpus (`census-corpus.md`) | 2,613 runnable | none | 624 places in 463 tests (`$result.values->toCSV()`, a table's `toString()`); the only call after a print is `->replace('\n', ',')`, 41 places | about 560 (`rows->map(r\|$r.values->makeString(','))` is 300 of them) |

- No PCT test and no corpus test follows a print with `length`, `trim`, `startsWith`, `endsWith`, `contains` or
  `indexOf`, joins a compared print to other text with `+`, or passes a computed Float to `format` (PCT's 42 `format`
  places all have literal arguments). The censuses are lexical: they do not follow helper functions (167 corpus tests
  call a helper that prints), so this is to be confirmed by measurement before anything is deleted.

### 3.2 How many tests need upstream's text at all

Measured by switching the test-body mechanism off (`UpstreamPrints.read` returning the body unchanged, a temporary
edit never committed) and re-running `bazel run //spec:update_edge_corpus` and `//pct:update_edge_contract`; the diff
of the recorded files is `pre-read-off-recordings.diff.txt`.

| Suite | Tests whose verdict moves PASS to FAIL | What the failures are |
|---|---|---|
| PCT channel B, DuckDB | 34 of about 1,250 (essential 1, relation 30, standard 3, grammar 0, unclassified 0) | 33: a date-time cell in a printed table (`2024-01-29T00:32:34.000+0000` against lite's text): `asOfJoin` 4, `over::testRangeInterval_*` 26, `max::testExtendMaxDate`, `min::testExtendMinDate`, `date::testDayOfWeek_Relation`. 1: `toString::testFloatToStringWithNegativeExponent` (`0.000000013421` against `0.0`) |
| PCT channel B, SQLite | the same 34 | the same |
| PCT channel B, H2 (relation only) | 34 | the 30 above, and 4 that fail as `LIST_GET reached a dialect without a list encoding` (H2 cannot print the table in SQL) |
| The corpus, DuckDB | 18 of 2,412 | row order of an unsorted printed table; an empty cell's and an empty table's text (`"name,firm\n\n"` against `"name,firm\n"`); the validation showcase's messages |
| The corpus, H2 | 28 of 2,266 | the same, plus Float text (`1E+1` against `10.0`, `3` against `3.0`, `0.258999999999999991` against `0.259`) |

Every other test passes with lite's own text. In the corpus 111 asserts on DuckDB (103 on H2) change KIND alone
(`assertSameElements` back to `assertEquals`, the unordered-table rewrite gone) with the same verdict. Not measured:
PCT's query door (channel A) with its lift off; any server or cloud database.

### 3.3 What actually runs (every statement printed by a temporary line in `exec/JdbcStatements.send`, never committed)

The real corpus test `meta::relational::tests::query::function::round::testFilterUsingRoundFunctionWithScale`
(`testWithFunction.pure`:264-274: one `execute`, one `toCSV` assert, one `assertSameSQL`), DuckDB, through
`bazel run //spec:corpus_one -- duckdb host <test>`:

| | The user's query is sent | By whom (the call stack printed) |
|---|---|---|
| Today (the pre-read on) | 4 times | the pre-read's own program twice (`UpstreamPrints.print` > `TestExecution.runBody`: `PlanRunner.check`, then `PlanRunner.result`); the body's check of the `let` (`PlanRunner.check`); the SQL-text appeal (`SqlTextRuns.decideAppeal`) |
| The pre-read off | 3 times | the body's check of the `let`; the printed side (the CSV composed in SQL over the query, `TestRun.sideRows`); the SQL-text appeal. Plus `SELECT concat('roundedQuantity ', ...)` for the golden literal |

`testDayOfWeekFunction` (the same file, :486-493) gave the same counts. The real PCT relation test
`meta::pure::functions::relation::tests::filter::testSimpleFilterShared` (channel B, DuckDB,
`-Dlegend.diagnostics=chb-only=...`): today 2 statements (the pre-read's read of the table, then one statement that
compares two literal texts); with the pre-read off, 1 (the table's text composed in SQL and compared).

PCT's query door (channel A), `//pct:pct_route_print_test`: one statement a case (`SELECT 1.0E-5 * 1.0E0 AS value`),
Java prints; a Float literal's print sends none.

### 3.4 Lite's own runner, the two judges (the ladder model, `core/src/test/java/com/legend/ladder/LadderRender.java`, DuckDB)

One body, `let r = execute(...project...sort...)`, a `toCSV` assert, an `assertSize` and a cell assert (rung
`r08_positionalCell` with a `toCSV` assert and an `assertSize` added for the measurement, never committed):

| Judge | Statements | How often the user's query is written |
|---|---|---|
| DATABASE | 1 (3,225 characters) | once, as `WITH frame_r AS MATERIALIZED (...)`, read by all three asserts |
| HOST | 5 | 4 times: once for the `let`, then inside each assert's side; plus `SELECT 'a' AS value` for a literal side |

`r09_twoAssertsOneLet` as committed: 1 statement against 4; `r05_countOneLet`: 1 against 2; `r12_classLetManyReaders`:
1 against 6. So the single statement is the DATABASE judge's. The HOST judge reads one statement a side
(`testexec/TestRun.hostSides` > `sideRows`); its batch (`TestRun.hostBatch`, `TestPlan.Verdicts.fused`) did not apply
to these bodies: 3.5 says why. The compatibility doors use the
HOST judge alone (tenet 5 of the six; `TestPlanner.refuseTheDatabaseJudgeInCompatibility`), which is why they send
several statements whether or not anything is printed.

### 3.5 Why the host judge's batch did not apply (measured and read, later the same day)

The host judge has a batch (`testplan/HostSideBatching`, `lowering/HostSideBatch`): the two sides of ONE plain assert
read by one statement, never across asserts (its class note: a raise in the statement must be that assert's). A
temporary line at each place it declines a side, over the ladder's twelve bodies under the HOST judge (never
committed), printed:

- The three store-free bodies (`r01` to `r03`, literals against literals) are batched: both sides "batchable".
- **No assert that reads an `execute()` result is batched.** Its literal side is batchable or a constant; the side
  that reads the result is declined every time: `HostSideBatch.side answered none` for a table's or a cell's read
  (`r07`, `r08`, `r09`), or `typed=null ... kind=GRAPH` for a read of class instances (`r04`, `r05`, `r10`, `r12`).
- Read in `HostSideBatch.side`: a side joins a batch only as a single value with no FROM, or where it "reads no
  stored table" (`typedByItsTree`, the caller's `w.storeFree()`), because a UNION ALL's column takes one type and "a
  stored table's column is the database's DDL, which the model's type need not be"; a WITH is refused too.

So the batch is for store-free sides by design, and a side that reads a store is always a statement of its own. A
single-statement host judge is therefore not a matter of widening a condition: it needs a layout that carries sides
read from stored tables, and the per-assert rule would have to be looked at again.

### 3.6 PCT's query door with its lift off (measured, later the same day)

`PctPlanRoute.execute` with `lifted = null` (a temporary edit, never committed), `bazel run
//pct:update_edge_contract` (PCT channel A on DuckDB and SQLite, all five suites, and H2's relation suite); the diff
is `query-door-lift-off-recordings.diff.txt`.

- **One test moves:** `meta::pure::functions::string::tests::toString::testFloatToStringWithNegativeExponent`, on
  DuckDB and on SQLite, from SUCCESSFUL to FAILED (its answer `0.0` where legend-pure writes `0.000000013421`).
- Nothing else in any recorded file: every other PCT case at the query door answers the same with the lift off
  (a date-time literal's text is the lowering's constant; a relation is returned as rows, and upstream's own harness
  prints it).

So at PCT's query door the lift of a value's print serves one test a database. Not measured: H2's other suites and
every server and cloud database at the query door.

## 4. How it should work, by place

| Place | Mechanism | State |
|---|---|---|
| A query whose answer is a print (PCT channel A; legend-engine's API for a table) | the door takes the final print off, the query runs once, Java prints (`edge/PrintedRelations.lift`, `PureRelationTextWriter`) | exists; one statement, measured |
| A side of an assert in a test body (PCT channel B, the corpus) | the side is fetched as its table or value by the statement that fetches that side anyway; Java prints it and compares with the golden text | NOT how it works today: today `edge/UpstreamPrints.read` runs the printed thing as a program of its own before the body and puts the text back as a literal (the 4 against 3 above), which breaks tenet 2 |
| A print per row, or inside the store query | the database prints lite's text; legend-engine's `TDSNull` word and its casts are rows of the door's table | the casts are rows (`edge/EngineTable`, `lowering/NamedRule.FLOAT_TEXT_BY_CAST`, `DATE_TIME_TEXT_BY_CAST`); `TDSNull` is still a pass of the edge's dialects (`edge/PurePrintForms`) |

The target for the second row, as the user put it ("why doesn't this just use the product host judge to send single
statement and then fix some stuff in java?"): the HOST judge plans a body as the DATABASE judge does (each `execute`
computed once, every assert side returned from that one statement as typed rows), Java compares, and at a
compatibility door Java prints a printed side first. That is a change to the product's host judge, in the test
planner. An interim that only stops adding a run (fetch the printed side as its table with today's per-side
statements) is three statements in the corpus example, not one; almost none of it would be thrown away later (the
door's mark on the side, the plan's data, Java's print and compare stay).

## 5. What step 8 needs from step 7 (the doors)

1. **A printed assert is door behaviour that reaches the host judge's planning.** The one existing example is the
   SQL-text assert: `edge/EdgeEngineTestText` implements `testplan/EngineTestText`, a facet of the plan style, found
   through `testplan/EngineTextGates.facet(env)`; its arm `edge/SqlTextVerdicts.plan` plans the assert as
   `HostJudgment.EdgeJudged(Extension judgment, ...)` with its two sides as asks, through the pinned facade
   `testplan/EngineTextPlanning` (`ArchitectureTest.PLANNER_ENTRIES`, `OpenClassMembersTest`); the edge judges it when
   the assert runs (`testexec/EdgeHearing.judged`). A printed assert needs the same kind of entry: the door says
   "this side is printed as upstream's table text (or CSV, or a value's text), with these literal replaces, lines in
   order or in any order", the plan carries that as data, and the edge prints the typed rows and compares. The
   question for step 7's plan: once "a door is passed in and nothing after one early point can see or guess it", how
   does a door's behaviour reach TEST PLANNING, for the SQL-text assert and for a printed assert alike? Building a
   second facet on the plan style would add to the machinery steps 7 and 10 remove.
2. **The door's print rows ride the lowering lookup step 7 removes for comparisons.** `Scalars.doorsRule` >
   `EditionTable.lowersAs` answers `NamedRule.FLOAT_TEXT_BY_CAST`, `FLOAT_TEXT_BY_CAST_OF_A_DOUBLE` and
   `DATE_TIME_TEXT_BY_CAST` (rows of `edge/EngineTable`, inside a store query, per database). The drafted `TDSNull`
   row (`tdsnull-row-design-research.md`; the draft is commit `bd9d9132` on the scratch branch
   `claude/step-8-edit`, not built, audited in `audit-tdsnull-row-and-trim.md`) adds one rule that both doors' tables
   answer inside and outside a store query on every database, and one more reader of the lookup in
   `Lowerer.scalarValueTailArms` (a row's cells joined). If the lookup's shape changes, these rows move with it: they
   belong in "the rest of the door's table" of step 7's plan.
3. **The host judge sends one statement a side** (3.4). Not step 7's to fix, but whoever plans the doors' reach into
   test planning should know the single-statement host judge is wanted, since it changes how a side's typed rows
   arrive.

## 6. The state of step 8's code

- `claude/edition-step8` holds the paused work of 2026-11-26 with the program branch merged in (`dd169bec`: the fast
  set passed, the five recordings re-run with no file moved) and this record.
- `claude/step-8-edit` (scratch, NOT built, NOT for merge) holds drafts written before the stop: the second review's
  three fixes to the pre-read's guards; a last-step `format` lift; `+` in the print's tail; a `trim` that strips
  Pure's white space; the `TDSNull` row; register rows S437 to S440. Two outside audits of them are beside this file.
  Under the tenets the pre-read goes, so the three guard fixes go with it; the format lift and `+` serve no real test
  (3.1); the `trim` fix is a product fix apart from printing, and its audit found it would also change `trim` inside
  the Postgres and IQ SQL islands (`protocol/sql/PostgresFunctions.java`:64-78, `IqFunctions.java`:4509).

## 7. Open, for the user

1. The printed assert: the single-statement host judge as the target, with or without the interim first (the user
   leaned to the interim; it waits for step 7's plan either way).
2. Drop the format lift, `+`, and the six follow-up functions no real test uses (reverses the D7 ruling for `%s` and
   `%r`; to be confirmed by re-recording before anything is deleted).
3. The `trim` fix: the product alone, or the SQL islands too.

## 8. Not verified

- PCT's query door with its lift off on H2 (beyond its relation suite) and on every server and cloud database; the
  test-body measurement (3.2) on any server or cloud database.
- That a printed assert planned as an edge-judged assert moves no recorded answer: nothing of it is built.
- The censuses beyond what a lexical script sees (helper functions).
