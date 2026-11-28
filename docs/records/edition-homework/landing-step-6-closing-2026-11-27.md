# The landing of step 6 after slice 6.2 ("A"): what it is, every recorded file it moves, and why (2026-11-27)

*The edition program's step 6, from the pull request's branch (`836de4901`, slice 6.2) to the closing batch's last
commit. This page is the dated reason for each pin, golden and ledger the landing re-records, and the one summary the
closing batch promised: every answer that moves, every pin, every register row, every limit. In plain words, what a
user sees: `step-6-what-changes-2026-11-27.md` (sections 1, 2 and 7).*

## What lands, and what it may be called

**A, as the user ruled** ("I guess let's do A and then fast follow with B"): what is real, named as it is.

- **One set of rules** decides whether two values are the same, held by the compiler for `==`, `!=`, `eq`, `in`,
  `contains`, `indexOf`, the deduplications, the orderings, `max`/`min` and every assert.
- **One piece of code compares two WHOLE sides** (a computed list, a map, a table, an instance) wherever the
  comparison stands at lite's own door: a query's answer, an `if`'s condition, under `&&`, `||`, `!`, a test's
  `assert(...)`. It is the code the test checker's database comparison was.
- **A second piece of code still compares two SINGLE values where they stand** (`lowering.Equalities`), and the test
  checker still plans an assert itself. The two are meant to agree; the measure below says where they do not.
- So this landing is NOT "one equality" as the program is named. The architecture review's verdict on that promise
  was "partly" (`step-6-closing-reviews/review-4-architecture-audit-one-equality.md`). Making the two one is step 6B,
  which starts when this lands, its design reviewed before any code.

**Nothing of step 6 goes to `main` before step 6B** (the user: "I'm super nervous we're just going to be stuck with a
bunch of garbage A code if we merge to main"): written into the program's landing step
(`open-decisions-2026-11-26.md`, step 11), with the three things that step checks.

Size, against the pull request's branch: 121 files, 11,889 lines added and 2,889 removed; the product's own code
5,759 added and 2,612 removed.

## The rulings it stands on (the user's words, 2026-11-27 unless dated)

- Numbers: "Yes I like the rule, keep ordering strict" (exact with exact by value; a Float only with a Float).
- A type only a column gives: "great do it"; for dates "You can do the error way if it's only for lite mode", and "We
  don't really want date vs datetime to work for greater than/less than vs not for equal".
- Inside a function whose types the caller gives: "the split with compile error when we know".
- Two tables: "Yeah or ~= if want to be cute, or setCompare()", then "Should it be hasSameElements?".
- Wherever it stands: "why cant we do the work we promised to get to one equality?".
- An empty side: "Yes do the empty side fix"; faithful to both doors: "1 okay".
- A Float literal in a Decimal's arithmetic: "good let's do it"; at every door: "2 okay A".
- `max`/`min` at legend-engine's door: "great to match engine but only if not too complex and costly".
- Sizes: "1 okay on size"; "3 row sizes fine"; "Time series budget fine".
- The closing batch: "1 lowerer one lookup yes"; "2 okay" (the planner's older reads of the door go at the end of the
  program); "Java checker seems ok for now"; "per row refused okay"; "Build 3"; "Assert( == ) only if it's
  simple/easy"; the rename: "Okay to both".
- The process: "How can we batch up all the remaining step 6 issues and do them all at once"; "Don't start all the
  work if it's not done that mean we have to do it again"; "do we need to remember to only run the subset of heavy
  and manual that we need/doesnt overlap with other stuff running?".

## Product SQL size (`core/src/test/resources/lean-sql/baseline.tsv`)

Against the pull request's branch the whole table is **155,524 characters smaller** (13,629,084 to 13,473,560).

- 20 rows go down; 12 rows are new.
- 33 rows go up because a query that was refused now runs (an Integer against a Decimal, on each database).
- **4 rows go up with longer SQL, each put to the user and accepted** ("1 okay on size", then "3 row sizes fine"):
  PCT `let` on H2 (126,502 to 136,762), core `equal` on SQLite (24,280 to 27,271), core `let` on DuckDB (136,469 to
  138,047) and on SQLite (29,965 to 31,150).
- **The closing batch itself moves no row**: regenerated on its last commits, the table is unchanged.

The test checker's own SQL on PCT (`pct/.../channelb/ratchets.tsv`, the judge's characters): essential 901,459 to
903,151, grammar 375,048 to 370,108, standard 780,331 to 793,352: 9,773 more in all, the figure accepted with the
rows above (six tests that sort or take the extreme of a list mixing number types, and one test that now runs).

The time-series bundle (`datacube/test/bundle-budget.test.ts`): the pin 148,200 to 148,500 by the user's word; the
library's comments were shortened three times to fit that figure and no higher one. Measured: see "What judged it".

## Tests whose answer moves

- PCT, lite's own rules, on DuckDB: `meta::pure::functions::lang::tests::compare::testCompareDecimalAndLongTypes`
  from an expected failure ("compare compares Decimal with Integer") to pass: two exact numbers, compared by value
  (`Test_LegendLite_LiteRules_GrammarFunctions_PCT`, `channelb/ledger.tsv`).
- The relational corpus at legend-engine's own tests' door, the edge golden
  (`spec/src/test/resources/edge/corpus_*_host_judge-host.tsv`), three lines, every one still PASS: in
  `meta::relational::tests::groupBy::testJoinStringsTwiceWithAssociation` (DuckDB and H2) and
  `meta::relational::tests::query::filter::exists::testAssociationTwoLevelDeep` (DuckDB) one `assertEquals` is now
  judged as the any-order assert. The order rule no longer takes a side whose order it cannot tell for ordered, and
  the door reads such an `assertEquals` as `assertSameElements` (the user, 2026-11-27: "when there is no sort order
  in upstream, we want to check multiset"). Found by CI's corpus lanes: the edge corpus goldens were not on the
  regeneration list.
- **The time-series library against gs-quant** (`tools/gs-quant-oracle/ledger-adversarial-raw.tsv` and
  `-adjusted.tsv`, one row each, moved by the test's own `-Dts.gsquant.update`; found by CI's `core` lane, the test
  not being in the local round): `vol_swap_volatility default @BTCUSD` from REFUSED to PASS. The line has two rows on
  one date; the word's answer is one point at the line's last date, which lite now gives, equal to gs-quant's.
  **This is a refusal LOST, for the user's word (item 5 below).** Read in the SQL before and after
  (`836de4901`, `d0a70c4f5`): one thing differs, the filter that keeps the last date, `date = MAX(date) OVER (...)`
  before and `date IS NOT DISTINCT FROM MAX(date) OVER (...)` now (two sides that may both be empty are compared
  null-safely in a filter too: this landing's empty-side rule). The refusal for the duplicate date stands in the same
  condition, joined by AND, and DuckDB no longer evaluates it for the duplicate date's row, which the filter drops.
  The refusal fired before only by the order DuckDB evaluated the two in. The reports made from the ledgers
  (`summary.tsv`, `two-rows.tsv`, `unruled.tsv`, `TWO_ROWS.md`) are regenerated with them.
- No other recorded suite: PCT's expected failures on DuckDB and H2 and the corpus rosters are unchanged. What CI's corpus lanes and the database lanes show is under "What
  judged it".
- The acceptance measure (every assert of lite's own tests against its plain comparison; 1,733 bodies on DuckDB, H2
  and SQLite): 176 differed when step 6 began, 99 before the closing batch, 85 after it and 80 on the final code;
  read body by body in the batch list.

New and extended tests: `exec/RootComparisonTest`, `RunTimeComparisonTest`, `NumberFamiliesTest`,
`EmptySideEqualityTest`, `EqualityInExpressionsTest`, `DatesTypedByTheirColumnsTest`, `OneValueWithinOneTypeTest`;
`edge/UpstreamTiesTest`, `EngineTestsStyleTest`; `testexec/InstanceIdentityTest`, `EqualityWorldsConformanceTest`;
`testplan/AssertBarTest`; `compiler/spec/CrossTypeComparisonsTest`, `NumberLiteralsTest`.

## Guards moved, each dated where it stands

| Guard | What moves | Why |
|---|---|---|
| `JavaEvalLedgerTest` | `lowering/VerdictSql.java` 2,263 to 1,119; `testplan/JudgmentFacts.java` 355 to 237; `testplan/AssertVerdicts.java` 1,335 to 1,334; `testexec/Equality.java` 319 to 324; the planner's register + `RootComparison.java` | the test checker's comparison of two sides moved, as it stood, into the lowering (`SideEqualities`), where the product now calls it; the run hook says whether two exact numbers are equal by value (lite yes, the edge no) |
| `SqlFnSortedTest`, the writer sets | + `lowering.SideEqualities`, `lowering.Equalities`; − `lowering.VerdictSql` | the same move; the empty-side guard is written by `Equalities` |
| `CodeShapeGuardrailTest` | new: `aScalarNativesRuleIsPickedInOnePlace`; + `Typer.comparedIn`, `Lowerer.wholeComparisons` | a scalar native's rule is picked by `Lowerer.written` alone, so the empty-side rule cannot be bypassed |
| `ArchitectureTest` | new: `thePlannersReadsOfTheEditionAreItsListedOnes` (nine listed reads, shrink-only; lite's own edition closed by name), its planted class and `everyListedEditionReadIsStillMade`; the planner's entries + `NamedRule#EXTREME_OF_TWO_IN_THE_DATABASES_TYPE`; `#NUMBERS_OF_THREE_TYPES` renamed `#THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION` | the user: "We shouldn't land with the planner knowing editions"; the rename by the user's word, the name by the rule type's own contract (what it writes) |
| `EdgeOnlyMembersTest` | − three members the doors no longer read | the doors' rewrites moved to the type-checker's rule |
| `NativeCatalogGovernanceTest` | lite's own surface 9 to 10 | `hasSameElements` (register S430) |
| `spec/.../ratchets.tsv` | intrinsics 738 to 739 | the same function |
| `parser-equivalence/.../ratchets.tsv` | own-corpus matched 3,361 to 3,364 | three test models added by step 6 join and match (the last: the door test's) |
| `builtin/native-claims.tsv` | twelve lines | which classes name each native: `EqualityClasses` reads `hasSameElements`; `Lowerer` reads `exists` and `find` (a positive condition); `Equalities` reads the date constructor (a date built to the hour); `OrderView` reads the table sort; `RootComparison` no longer reads `not` |
| `datacube` bundle pin | 148,200 to 148,500 | above |

## Recorded differences (register, `docs/SEMANTICS_REGISTER.md`; the page `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md`)

- **S127, S428** amended: numbers of two families; where equal exact numbers land; lite keeps the first of an
  Integer and a Decimal in a list's `max`/`min` where upstream keeps the last (run on upstream).
- **S429** (ruled): a Float literal written into a Decimal's arithmetic is read as a Decimal. Difference 24.
- **S430** (ruled): `==` on two tables is strict; `hasSameElements`. Difference 25.
- **S431** (drafted, the direction the user's): where lite cannot tell that a side's order is fixed, it refuses an
  ordered comparison. Difference 26.
- **S432** (drafted): the type of the first-day functions and of a day moved by a whole unit (run on upstream).
- **S433** (drafted, FOR THE USER'S RULING): the type of `max` and `min` of two number columns at legend-engine's
  door. Difference 27.
- **S399** amended: the one day-against-date-time form still measured.
- Difference 28: an equality with an empty side answers as Pure does, wherever it stands.

## For the user's word with this landing

1. **S433**: at legend-engine's door, `max`/`min` of an Integer column and a Float column answer a Float inside a
   query sent to a database (built; measured on H2). The other pairs keep the winner's own type. Accept as recorded?
2. **A sort, then a `distinct`** (S431's second limit): lite reads the result as still sorted, and the SQL it writes
   is `SELECT DISTINCT` over the sorted rows, which no database promises to keep in order. Refuse it under `==`
   (strict), or leave it trusted?
3. **`assert(t1 == t2)` over two tables in an upstream-style test body** is read as written (ordered), no longer as
   the any-order assert: rewriting it changed what two tables' comparison means. What moved by it is under "What
   judged it".
4. **Dates written to the hour or the minute, at the two compatibility doors** (run on legend-engine 2026-11-27,
   `upstream-step6/answers-request-5-2026-11-27.md`):
   - a date BUILT to the hour (`date(2015, 4, 16, 14)`) against a date-time column: lite answers false, which is
     Pure's answer; legend-engine refuses to plan it inside a query it sends to a database. Kept as it is.
   - a date-time LITERAL written to the hour (`%2015-04-16T14`) against a date-time: lite compares by the instant
     at every door, so a 14:00:00 row matches. Pure says they are not the same; legend-engine inside a store query
     compares against the day's midnight and returns no row. Older than step 6, no recorded test holds it, and it
     is NOT changed in this landing: fix it with the doors' date rule in step 6B, or sooner?

5. **A refusal that fired only where the database happened to evaluate a row its filter drops** (the time-series
   case above). In Pure's strict reading the row's refusal is raised before the filter, so the query is refused;
   lite's SQL merges the two into one condition, where a database may skip it. Options: (a) accept the answer for
   this case (it equals gs-quant's, and no value on the duplicate date is in it); (b) restore the refusal by
   keeping a value that may raise below a filter that drops rows, in the pass that merges SQL levels (the
   time-series SQL grows; its own step, not step 6's); (c) withdraw the null-safe comparison in filters for computed
   sides, which restores the old SQL and the old luck. Recorded as it is until ruled; (b) is the one that makes the
   refusal true on every database.

## Limits, recorded and not built (each in the batch list)

- A list compared for each row of a query: the database's own list comparison where it has lists; refused where it
  has none. A map there is refused by name. Two tables there are the database's own comparison.
- Two lists of exact numbers written at two scales, on SQLite, MariaDB and MySQL: refused by name where the lists'
  types say so; compared as their texts where they state one scale or none, and on H2.
- Whole lists of two column-held number kinds answer false where two single values of those kinds are refused.
- A comparison in a lambda the query's author hands to their own function is checked as that function's code.
- `$p.n == 0.5` where `n` is declared `Number` over a DECIMAL column is refused; over a property declared `Decimal`
  it compiles.
- The order rule's two limits (S431). A variable no statement reads, whose `let` would raise, raises nothing.
- A day read out of a written list of days and date-times is compared with a date-time as midnight (S399).
- The false of two single values of two types casts each to text behind a mark (older than step 6).
- `hasSameElements` refuses a table's cells read as one list.
- Two date literals of different written precision are equal at every door (upstream not run for that pair).
- SQLite's date-times as text; H2's missing list forms; H2's `ascii('')`: the database follow-up.
- The planner's older reads of the door (nine, listed and shrink-only) and its other per-door forms: steps 10 and 11.

## Found after the landing checks, by a new measure (2026-11-27, 20:20 UTC)

While CI's third run went, the working session built a measure that asks one comparison in 17 places of a query on
DuckDB, H2 and SQLite and counts the comparisons whose answer depends on the place
(`docs/records/edition-homework/step-6b/measure-positions-2026-11-27.md`). Against the program branch before step 6:
comparisons that are true in one place and false in another go from 19 to 9 on DuckDB, 13 to 5 on H2 and 33 to 12
on SQLite. It also found one thing this landing makes worse:

- **On DuckDB, a comparison of two computed lists or maps inside a list's lambda, reading nothing of the lambda's
  element, is a database error when the query runs** (`Binder Error: subqueries in lambda expressions are not
  supported`), where it answered before step 6. Example:
  `[1]->map(x | [1, 2]->map(y | $y + 1) == [2, 3])->at(0)`. It is an error, never a wrong answer; no test of core,
  PCT or either corpus holds the shape. Trino, Databricks and Snowflake also write list lambdas and were not measured
  for it.
- **Not fixed in this landing.** The fix (the comparison computed once outside the lambda and read by name) is the
  first piece of step 6B, checked by the same measure. Nothing of step 6 goes to `main` before 6B.

## What judged it

*(filled in as each check reports; nothing is claimed before it has run)*

The final commit is `2e684d821`: the code of `508aaef6f`, the regenerated files (`3bee40e22`), the three edge corpus golden lines CI's first run named (`d0a70c4f5`), the gs-quant parity ledgers and their reports CI's second run named, and the records. No product code has changed since `508aaef6f`. The lanes ran at `3bee40e22`: the three lines are a compatibility door's recorded file, read by no lane.

| Check | Where | Result |
|---|---|---|
| The full round, 91 targets (core, the guards, PCT on DuckDB and H2 at every door, channel B, parser equivalence, the time-series library) | here, at `38a12c97a` (before the final review's fixes) | 87 pass, 1 skipped, 3 fail: the bundle 2 bytes over its pin and the parser count, both fixed by the regeneration; no product code |
| Three independent reviews, each given the diff and the rules, never the author's conclusions | subagents | the code audit of pieces 1 to 5 (2 blockers, fixed); the architecture audit ("partly": the verdict this landing is named by); the final review of the last fixes (2 blockers and 7 smaller, each read in the code, fixed or recorded: the batch list) |
| The final review's fixes, narrow: exec, edge, lowering, sql, the guards, the census, the two SQLite integration targets | here, at `508aaef6f` | 10 of 10 targets pass; each new test seen to have run |
| Regeneration (seven update targets) | here, on `0dd18a3ee` | three files move, each as predicted (the commit names them); no SQL size row, no PCT ratchet |
| The confirming round (the same 91 targets) | here, at `3bee40e22` | **90 pass, 1 skipped, none fail** |
| The acceptance measure | here, at `3bee40e22` | **80 of 1,733 bodies differ** (DuckDB 22, H2 53, SQLite 49), from 85 before the last fixes, 99 before the batch and 176 when step 6 began. The year and year-month fix took six bodies off every database ("dates and written precision": 7 to 1 on DuckDB and on H2, 14 to 8 on SQLite, those eight SQLite's date-times as text). On SQLite two of the Decimal list bodies are now refused by name where they answered a wrong false. The one verdict that still moves on DuckDB is register S399's known gap (a day read out of a mixed list against a date-time) |
| CI on the draft pull request #141: every per-push lane on Linux, macOS and Windows (the product build, core, the guards, both corpora, PCT on DuckDB, H2 and Postgres, channel B, parser equivalence, stress, the warehouse, DataCube, the UI) | GitHub | first run, on `3bee40e22`: red on the corpus lanes alone (Linux H2 and DuckDB, Windows H2), each on the edge corpus golden's three lines (above: "Tests whose answer moves"); every other lane that finished was green (all of Linux but `core`, still running when the fix was pushed; Linux arm; eight Windows lanes). Second run, on `d0a70c4f5` (the same code and those three golden lines): 51 of 55 lanes finished, 50 green -- every corpus lane, every PCT lane, the guards, stress, the warehouse, DataCube and the UI on each platform that ran them -- and one red, `windows / core`, on `//core:ts_gsquant_parity_test` alone (the time-series case above; reproduced here on Linux); Linux and macOS `core` were still running the same test when the fix was pushed. Third run, on `2e684d821` (the same code and the parity ledgers with their reports, five files; run 46060956867, 20:03 to 21:01 UTC): **53 of 54 lanes green** -- `core` on Linux, macOS and Windows (the lane that was red), every corpus lane, every PCT lane, the guards, parser equivalence, stress, the warehouse, the product build, the UI, and DataCube on macOS and Windows. One red: `linux / datacube`, on `//datacube:verify_charts_test` alone -- opening the example page charts-gallery passed its 120 s limit and the seven page checks after it failed with it. Read as a slow runner, not this commit: the same lane was green on Linux and Windows in the second run on the same product code; none of the five files changed since is an input of DataCube; the test passes here at the exact commit (15 of 15 in 122 s); and Linux `core` took 58 minutes on the same runners where it takes 35. The lane could not be re-run alone (GitHub refuses this session the re-run: 403), so the summary check `gates green` stays red on `2e684d821`; the next push to the pull request (the landed head, which adds the measuring tool and records) runs every lane again |
| The database lanes: PCT's two channels on Trino, MariaDB, MySQL, ClickHouse, SQLite, Snowflake, BigQuery, Databricks; channel A on MongoDB; channel B on Postgres; the time-series replays on Postgres, ClickHouse and Snowflake; MongoDB's plan test | six sessions, measure only, at `3bee40e22` | as each reports, below |
| -- MariaDB, MySQL, ClickHouse | | **no outcome differs** but for MariaDB channel B's known flake: channel A 1,251 cases pass on each; channel B on MySQL (18 not passing) and ClickHouse (9) the same ledgered sets; MariaDB's channel B fails on list ORDER alone, a different set of tests in each of its eight runs across four commits, the base included (the same 16 ledgered failures every time; never a value, a type or a compile error). The new refusal for lists of exact numbers at two scales touches no test. The time-series replay and documents on ClickHouse and the guarded-raise test pass |
| -- Postgres and Trino | | **no outcome differs**: Trino channel A 1,249 cases pass, its test lists identical to the last round's and the base's; channel B on Postgres and on Trino the same line for line; the time-series replay on Postgres passes ("215 of 215 series as on DuckDB") |
| -- Databricks | | **no outcome differs**: channel A 1,249 cases pass; channel B 1,228 pass and 21 refused or failed, the same 21 as the last round's and the base's, their texts the same |
| -- BigQuery | | **no outcome differs**: channel A 1,249 cases pass; channel B's outcome lines are the same as the last round's and the base's, the 19 tests that fail on `main` ("UNION ALL has incompatible types") the same 19 |
| -- Snowflake | | **no outcome differs** in PCT: channel A 1,249 cases pass; channel B's outcome lines the same as the last round's and the base's. The three manual time-series tests pass, each beside the record of 2026-11-09 (`docs/TS_DECISIONS_2026_11_07.md`, item 18b): the replay all 215 series in 1,396 s (recorded 2,017 s; `macd C` 314 s against 315 s, `ramp C` 108 s against 213 s, `sharpe A` 71 s against 156 s: none slower); the island test 67 of 67 in 2,475 s with no timeout (recorded: a timeout at 3,600 s, and 57 tests in about 2,020 s at an earlier commit); the documents test 619 s (no earlier run to compare). The island and the replay ran at the same time on one account, so each duration holds some waiting on the other (final report, 20:05 UTC) |
| -- SQLite and MongoDB | | **no outcome differs** from the last round or the base: SQLite channel A 1,249 cases and channel B pass; MongoDB channel A 1,249 pass; MongoDB's plan test 21 of 21. Every log is the same line for line as before, each plan's SQL and each result included. The new refusal for lists of exact numbers at two scales touches no test |
| The manual tests no lane holds: lite's own PCT channel A, channel B on H2, channel B twice | the heavy session, at `3bee40e22` | **all pass** (12 of 12 targets). Lite's own PCT: essential 345, grammar 137, relation 469, standard 205, unclassified 93, none failed. Channel B on H2: 440 pass, 29 refused, each already in the ledger (H2's missing list forms); nothing new refused. Channel B twice: the two runs the same |
