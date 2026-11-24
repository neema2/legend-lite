# B3: test checking moves out of the planner -- progress record

The plan of record: `docs/records/compat-stage-b-plan-2026-11-20.md` on `claude/compat-program`, section B3 ("option
N"), every decision ruled there with its time; the 2026-11-24 additions (legend-engine's door rewriting `==`, `!=`,
`in` and `contains` to the database's comparison; the one equality, strict typing, as B3's last step):
`docs/records/compat-stage-b-plan-2026-11-24.md`. Branch `claude/compat-b3-nufdlk`, stacked on item 3 (PR #132).

## The one rulebook, measured (2026-11-24)

The judge has one rulebook, lite's (E1 withdrawn, 2026-11-20 ~21:00). The edge's run hook (`EdgeHearing`, answered by
`edge/Appeals`) still brings compatibility's own rules into the run, through five hooks: `judged` (the SQL-text
appeals), `json()` (S144, json-simple's grammar), `renderedTexts()` (S131, S132), `hostForms()` (compiled legend-pure's
host equality, `edge/CompiledPureHostForms`, and the error wording `caught`) and `writing()`. The product answers
lite's to each (`testexec/NoEdgeRun`).

**Run 1: `hostForms()`'s four equality rules switched to lite's** (`LiteHostForms.STRICT`; the wording `caught`
kept, it is presentation). The rules that differ:

| Rule | compiled legend-pure (the edge's) | lite's |
|---|---|---|
| an Integer against a Decimal | equal by their texts (`1D == 1`) | never equal (S127) |
| two dates | equal as written | by kind and instant (S399) |
| two Decimals | scale part of the value | by value (S139) |
| two Floats | within 2 ULP (S15/S132) | exact |

Lanes, on item 3's head 96e7c1fc with the switch applied (temporarily, never committed): PCT channel B on the
compatibility rules, DuckDB's and SQLite's five suites and H2's relation suite; the corpus on DuckDB and H2; the edge
net. **Result: no PCT verdict moves; the H2 corpus does not move; the DuckDB corpus moves 2 tests, both S15** --
`meta::relational::tests::mapping::sqlFunction::log::testFilter` and `::testProject`: the expected value is
`0.09531017980432493` (testSqlFunctionsInMapping.pure:571, :578; the correctly rounded ln of the double 1.1), DuckDB's
`ln(1.1)` read by lite `0.09531017980432492` (one ULP apart; corrected 2026-11-24 by step 1b's research, the two had
been written the other way round); PASS before, FAIL with lite's exact Float.
The date, Decimal and number-kind rules move nothing on any lane. This matches the plan of record's H6 count for S15
(DuckDB 2).

Next, by the plan: ask the oracle (the engine runner, legend-engine 4.145.0) for legend-engine's own answer to the two
on DuckDB, class them (agrees with lite / float printing / a real difference), and bring the counts and names to the
user before the rule is deleted.

**Run 2: `renderedTexts()` switched to lite's as well (S131, S132),** on the same lanes and head. **No PCT verdict
moves on any lane. The corpus moves 28 DuckDB tests and 3 H2 tests**, every one from PASS to FAIL:

| Leniency | DuckDB corpus | H2 corpus |
|---|---|---|
| S131 (an unsorted actual compared as a multiset) | 12: the right rows in another order -- `advanced::structure::testQualifierWithForkAndOrWithInline`, `::testQualifierWithLineAndOrWithInLine`, `mapping::classMappingFilterWithInnerJoin::testMergeForClassMappingInnerJoinFilter`, `::testMergeForClassMappingInnerJoinFilter1`, `mapping::distinct::testDistinctMappingWithFullDenormSelfJoins`, `mapping::union::specialUnion::testGroupBy`, `::testProjectWithPostTdsOperations`, `postProcessor::testSqlRealiasFreeMarker`, `projection::function::concatenate::testConcatenateWithFilter`, `projection::view::testUnionOnViewsMapping`, `::testViewOnView`, `validation::showcase::standalone::validateAllConstraints` | 0 |
| S132 (Float tokens in a rendered text within 2 ULP) | 14: `functions::pureToSqlQuery::calendarAggregations::*` -- DuckDB's Float arithmetic printed with its last digits (`6.840000000000002` against the golden's `6.84`) | 3: `calendarAggregations::testAnnualizedValue`, `::testCmeValue`, `::testPmaValue` -- H2 2.5 prints the Decimal with more digits (`9.81285067873303158` against `9.812850678733032`) |
| S15 (two Floats within 2 ULP) | 2: `mapping::sqlFunction::log::testFilter`, `::testProject` | 0 |

(The plan of record's H6, 2026-11-19: DuckDB S131 38 decisions, S132 14, S15 2; H2 S131 4, S132 3 -- S131's count has
fallen since, the rest as measured then.)

**The oracle step, by the plan:** legend-engine's own answer to each moved test on that database. These are
legend-engine's relational tests, written for its H2 (2.1.214); legend-engine does not run them on DuckDB, so no answer
is on record and the engine runner (which runs testable suites) does not run them as they are: asking means a small
harness that runs the test functions through legend-engine 4.145.0 on DuckDB. The expected classes, to be confirmed by
that answer: S131's twelve compare an unordered result as ordered text, so any engine on DuckDB returns DuckDB's order
(agrees with lite); S15's two and S132's fourteen DuckDB rows are DuckDB's own Float results (the same double from
any engine on DuckDB: agrees with lite); S132's three H2 rows are a printing difference (H2 2.5's Decimal text), which
B2b's Java printer from typed rows may fix.

## The plan, agreed with the user (2026-11-24, afternoon)

Written at the user's ask ("please write down here very clearly and in detail in plain English the full b3 plan"),
from the plan of record (2026-11-20) and the user's order of 2026-11-24, with the user's two corrections of the same
afternoon. Calls marked *proposed* were made while building and are not yet ruled: the user reads them here.

**What B3 is for.** Lite plans and judges every test body as lite, with one rulebook. Compatibility may (a) rewrite a
test's code at the door, before planning; (b) format output after the run (B2); (c) do extra checking in test tooling,
never the product. Finish line: both corpus result lists byte-identical, no PCT verdict moving that the user has not
ruled, about 3,449 lines out of core, the run hook (`EdgeHearing`) gone. Each step: measure first, build, an
independent review, the lanes and the warm lanes at each push, this record.

**The user's two corrections (2026-11-24):**
- **Words.** "lite tried to stay as close to pure as possible where it is cheap and easy and lean and fast, it can
  diverge when needed, then pure compatibility is hopefully small tweaks so we don't need to go running around chasing
  error messages and swapping back to lite words, then re-overwriting them again in pure words"; "Agree on reword
  catalog - keep errors close to pure when cheap/fast so don't have to reword everything on compatibility". So Q15 = A
  is read as: lite's words ARE Pure's where cheap; recorded where they differ. B2b's edge rewording of assert words
  mostly goes (the B2 session told, 2026-11-24).
- **Order and multiset.** "my ideal state for product is that we don't let platform devs or users write tests that do
  not have a sort ... But we still need multi-set compare for backwards compatibility ... if we can [get both]
  awesome!" So S131 is not deleted: the product refuses an order-dependent comparison of a result with no defined
  order, at planning, naming the fix (a sort, or `assertSameElements`); the compatibility door rewrites upstream's such
  asserts to the explicitly order-free form before planning; the judge's one rulebook compares in order where the test
  asks for order and as a multiset where the test says order does not matter.

**The check cadence (2026-11-24, after the user's "Are we doing test runs and audits too frequently? Or right pace for
fast and safe dev loop"):** (1) a fast check right after editing -- the guards, the census and the touched packages,
5-15 minutes -- before any full round; (2) one full round per step (PCT channel B on DuckDB, SQLite and H2 under both
rule sets, both corpora, the lean-SQL measure, the edge net, the guards, the touched core packages and the edge tests),
after its review's fixes, not before and after; (3) one independent review per step, started at its push and run beside
the next work, its fixes batched into the next commit; design reviews for the large designs (the lift, order, the one
equality); (4) the warm lanes (Snowflake, Databricks, BigQuery) only for a change that could differ by database --
dialect or SQL rendering, a judge's rule over values a database carries, a tolerance deleted -- and at each milestone;
(5) CI on every operating system at a milestone, through B3's pull request.

**The steps, in order:**
1. **One rulebook in the judge.** 1a: compiled legend-pure's Integer-against-Decimal, as-written date and Decimal-scale
   rules go (S127, S399, S139) -- done, `3994b835`. 1b: the Float leniencies S15 (2 ULP) and S132 (Float digits in
   printed text): 16 DuckDB and 3 H2 corpus tests move; each is placed against legend-engine's own answer (*proposed*:
   legend-engine 4.145.0's plan executor, the existing oracle tool, on the same DuckDB data; for H2, legend-engine's own
   golden on its H2 2.1.214), classed (agrees with lite / printing / a real difference), brought to the user by name,
   then deleted.
2. **The door rewrites, before planning** (a public edge function, `edge.UpstreamTestBodies.admitted`, called by each
   compatibility harness): S137 (a relation counted as one value; *proposed*: the existing rewrite form kept, the
   relation replaced by one value, `assertEmpty($rel)` refused by name), S144 (*proposed*: the one stray trailing quote
   dropped wherever strict JSON reads the text only without it; lite stays strict JSON, json-simple's port deleted) --
   built. Moved by the user's word of 2026-11-24 ("Agreed"), so nothing is built twice: legend-engine's `==`, `!=`,
   `in`, `contains` to the database's comparison (with list `==` refused as legend-engine refuses it) becomes step 5's
   first slice, where `==`'s lowering is split anyway; the corpus's held appeal (`testToSqlGenerationFirstDayOfWeek`,
   a SQL-text appeal that cannot read a failure's message after the run) is fixed in step 6, where the appeal itself
   is replaced by the harness's row comparison. *Proposed:* the corpus runner reaches the door through a `TestObserver` hook
   until step 6's prepare/run split lets the harness compose it itself.
2b. **The lift mechanism, pulled forward (the user, 2026-11-24: "Do whatever order and split of work makes the most
   sense"; and on B2's Q8 and string entry: "I don't think we should keep adding random flags to plan? I thought we
   already have mechanism for this that substring/indexof use").** The door's walker (A3's, over the typed query before
   planning) lifts an assert out of a test body; the body runs as lite; the test harness checks the lifted assert after
   the body by running its side through lite's public door -- with B2's legend-pure printer as the output receiver for
   a relation's own `toString`/`toCSV` side (B2's S4, replacing its plan fact), the referee's golden rows for a
   SQL-text assert (step 6), and the lines in any order for an unordered printed table (step 4: the harness is test
   tooling; the product judge never compares a multiset of lines). The test runner's public prepare and run halves come
   with it. **The lifted side is printed, then substituted** (the user, 2026-11-24: "I think toString and toCSV with
   more string functions like replace should also be all considered part of the toString rewrite"): where a side
   starts with a relation's own `toString`/`toCSV` and goes on through more string functions, the harness prints the
   relation's rows in legend-pure's text (B2's printer), substitutes that text as a String literal for the
   `toString`/`toCSV` call, and runs the rest of the assert as lite (the database computes the `replace` and the
   rest; lite's judge compares). It amends the ~18:12 rule ("text used further is the database's lite text") for a
   compatibility test body alone; lite's route and pure/v1's queries keep it. **Refined the same afternoon (the user:
   "No I'm saying also do the replace in java too it's really just more string functions on the CSV"):** every one of
   the corpus's 41 cases is exactly `toCSV($result.values)->replace('\n', ',')`, so a print followed by `replace`s
   with literal arguments is ONE printing step done in Java at the edge (legend-pure's CSV written, then the literal
   replace applied to that text: formatting the printed text, G6); only a different string function after a print
   (none in the corpus today) falls back to substituting the printed text and running the rest as lite, so no Pure
   function grows a second evaluator in Java (G1). Expected: the H2 calendar tests whose
   difference is printing (`testAnnualizedValue`, `testCmeValue`, `testPmaValue`) pass; the DuckDB ones, where DuckDB
   computes a different double, do not (step 1b's answers confirm). To count first: an unordered printed table that
   further string functions reshape (no longer comparable line by line in any order). A lifted side the harness cannot re-run after the body (it depends on the body's lets or writes in a way the
   public door cannot spell) is refused by name and counted before it lands. No new plan fact.
3. **The catalog in Pure's words.** Each `AssertKind` sentence becomes legend-pure's own, read from the pinned
   legend-pure sources, where cheap; a divergence a register row. Before the equality, so messages are re-pinned once.
   The values inside a sentence: ruled by the user, 2026-11-24 ("In product they should print what we decided for lite
   to be consistent and nice, if that matches pure great; if not matching AND changes tests, then we can overwrite in
   compatibility to match tests - if nothing checks these in compatibility, then can also keep as lite in
   compatibility") -- lite's own value forms in every product message; after the reword, the compatibility tests whose
   checked message moves are counted, and only those get an edge overwrite; a message no compatibility test checks
   stays lite's there too. Ruled with it (the user, 2026-11-24, "Agree with both proposals, keep going"): the kind-labelled sentence (`EQUALS_KINDS`) stays lite's (lite's value form has
   no `D` to tell `1` from `1D`), and lite's messages stay on one line (`expected: ..., actual: ...`; the edge adds
   Pure's leading line break where a compatibility test checks it). The homework:
   `docs/records/compat-b3-homework/catalog-words-vs-legend-pure.md`.
4. **Order.** Measure first: how many of lite's own bodies (core; PCT under lite's rules) the refusal would refuse, and
   how many compatibility tests lean on a multiset today (S131's printed tables, 12 DuckDB corpus tests; an unsorted
   `assertEquals`; a graph-fetch JSON root read with no sort). Then: the planner's refusal (from `OrderView`), the
   door's order-free rewrites (rows and printed lines to `assertSameElements`; the JSON root needs an explicit
   order-free assert, to be designed from the count), and the judge's built-in multiset shortcuts deleted
   (`HostJudgment.Equals.incidental`, `HostJudgment.Json.incidentalRoot`, `edge/UpstreamRenderedTexts`, the
   `renderedTexts()` hook). Any PCT count that drops comes to the user by name before it lands.
5. **The one equality** (strict typing; `docs/records/compat-stage-b-plan-2026-11-24.md`, its design and decisions).
6. **The SQL-text fallback as test tooling** (the plan of record's text): the test runner's public prepare and run
   halves; on a failed SQL-text assert the harness runs the assert's query through the public `from()` after the body
   and compares rows with the referee's goldens; the 67 matches the public door does not rebuild today placed one by
   one (15 plan-text, 7 shapes reclaimed by Q16b -- R1's post-processor runtimes, 3 inline mappings -- 20 shapes and 32
   to read); Q16c; `assertError` catching the size refusals; `SqlTextVerdicts`, `Appeals`, `EngineTextPlanning`,
   `SqlTextRuns`, the referee's ledger and seed copies in `CompatEdge` and the appeal plumbing deleted.
   *Proposed:* the edge's caught-error wording (`HostForms.caught`, the run hook's last piece) moves to B2's after-run
   formatting here, so the hook goes whole.

## Step 1a -- built and pushed (`3994b835`, 2026-11-24)

Its round on the Bazel tree: PCT channel B (DuckDB and SQLite, five suites each under lite's and the compatibility
rules; H2's relation suite), the corpus (DuckDB, H2), the edge net, core's testexec and testplan packages under every
judge, the checks: 275 of 276 targets green, no verdict moved. The one red, `VerdictChannelRegisterTest` (the register
of `Equality`'s callers still listed the three files that no longer call it), shrunk in the same commit and re-run
green. At the push: an independent review and the Databricks, Snowflake and BigQuery warm lanes started; CI not run
(the branch has no pull request, and a workflow dispatch is refused to this session's integration: 403).

**Its independent review (read-only, 2026-11-24)** -- the code a no-op on lite's routes and lite's rule on the
compatibility routes, as asked; and:
- HIGH: three edge tests (`//core:core_edge_tests`, which the round did not run) still pinned the deleted rules --
  `EqualityCompatEdgeTest`, `CompiledPureNumbersEdgeTest`, `DecimalKindsTest`'s edge half. Fixed: each now pins lite's
  rule on the edge's door (`CompiledPureNumbersEdgeTest` renamed `NumberKindsEdgeTest`; the Decimal property against an
  Integer now fails `assertEq`/`assertIs`, as in the product); `core_edge_tests` joins every round from step 2 on.
- MEDIUM: registers S127, S139, S399 and the differences page still promised compiled Pure on the compatibility
  routes. Amended: each names B3 step 1a's change and its measurement.
- LOW: the dead Decimal-scale census (`CanonicalDivergence.decimalScaleOnly`, `Census.Key.DECIMAL_SCALE_ONLY`, the
  corpus print) deleted; stale comments describing the deleted rules corrected (Equality, LiteHostForms, HostForms,
  HostRules, Appeals, NoEdgeRun, the guard notes, three tests). Recorded for step 5 (the one equality rebuilds these
  rules), not changed now: lite's date rule stays in `LiteHostForms` (no longer a hook form; moving it into `Equality`,
  with the named refusal of a non-finite value declared Decimal, grew `Equality` past its ledger pin, 333 > 320 -- tried
  and reverted the same day rather than asking for the pin), a non-Pure-date carrier compared by its own equals, and an
  Integer-declared non-integral carrier answered false -- older than 1a;
  exec decodes every date into a Pure date and every declared Integer into an integral carrier. Confirm on the
  Postgres and MariaDB lanes (the reviewer's L1): pending.

## Step 2 -- the test-body door (built 2026-11-24, round pending)

**The door:** `edge/UpstreamTestBodies.admitted(TypedQuery)`, public, renamed from `UpstreamRelationCounting` (so the
edge's file census swaps one entry, adds none). The compatibility harnesses call it once, after typing and before
planning: PCT channel B (`ChannelB.typed`, under the compatibility rules only), the corpus (`TestObserver.admitted`, a
new hook with no default -- every observer states its audience, as `edgeRun` since K6 -- the corpus observer answering
the door, the product's `NONE` the body itself; `PureTestRunner` passes each body and setup through it before facts
and planning), the lean-SQL corpus measure, and the edge's test helpers (`EdgeProgram` hands the door to `Program`,
whose product overloads pass the identity). The test planner asks no style for a body any more.

- **S137** moved out of the planner: `TestPlanner.lifted` calls the product's refusal
  (`RelationCountAsserts.refuse`, now a plain class) on every route; the two-form kind `RelationCounting` is deleted,
  with its `TWO_FORMS` entry, `PureText`'s arm and the hook guards' pins (`HookLambdasTest`'s twoForm caller and class
  hook, `HookRulingTest`'s exact set). The rewrite is unchanged: the relation replaced by one value (`assertSize($rel, n)`
  holds for n = 1 alone, failing with lite's `expected size n, got 1`), `assertEmpty($rel)` refused by name (upstream
  fails it printing the relation itself; with Q15 = A the words would be lite's, but `expected no values, got 1` would
  print a value the test never wrote, so the refusal stays -- a failure either way).
- **S144** as an input rewrite: the judges read strict JSON on every route (`JudgeJson.STRICT`); the door drops the one
  trailing `"` from an `assertJsonStringsEqual` expected text written as literals (one, or joined by `+`) when strict
  JSON reads the text only without it -- the two milestoned graph-fetch goldens' shape
  (`testGraphFetchMilestoning.pure:277`, `testGraphFetchEmbeddedOtherwise.pure:304`, read in the pinned 4.145.0
  sources). Deleted: `edge/JsonSimpleGrammar` (the port of json-simple 1.1.1), the run hook's `json()` (`EdgeHearing`,
  `NoEdgeRun`, `Appeals`), the S144 ArchUnit rule and its plant (left with their subject), the grammar's census and
  field-shape entries, `JudgeJsonTest`'s json-simple cases; `JudgeJsonTest` pins the strict reading and the door.

**Its independent review (47786d4c, read-only, 2026-11-24): no HIGH.**
- MEDIUM M1: nothing checks that a body planned under the compatibility rules went through the door -- the style, the
  run hook and the door are chosen apart; six test-only routes plan in upstream's style without the door (none holds a
  body the door would change, so nothing fails today), and a harness could admit a product-rules body. Fix in step
  2b, with the prepare/run split: ONE audience choice for a harness (the style, the run hook and the door together),
  so they cannot be chosen apart; until then the gap is recorded here.
- LOW, fixed: L1, a trailing empty literal crashed S144's cleaning (the quote now dropped from the last piece that
  holds it; pinned in `JudgeJsonTest`); L2, S137 rewrote a relation at any multiplicity (now only at `[1]`; any other
  refused by name -- the same behaviour the deleted two-form had, older than step 2). L3, facts stated on the admitted
  body: consistent with the plan's own facts; no verdict moves. L4, the `TestObserver.admitted` hook: an interim seam
  until step 2b replaces it with the audience choice (the user's "do whatever order and split of work makes the most
  sense", 2026-11-24).
- NITs, fixed: register S137's message text and S144's narrow reach written out; the edge census line for
  `UpstreamTestBodies` names S144's literal parse. Left: `JsonGrammar` has one implementer now (removed with step 5,
  which rebuilds the judges' readers); the step's commit message says "the test planner asks no style for a body any
  more" -- for S137; it still asks for the dialect and the test-data printing.

**Found while building (open, for step 2's engine rewrites):** legend-engine's door rewrite of today
(`edge/PureMeaningRewrite`, A3: store-bound `substring`, `indexOf`, `substr`, `toOne` sent to the database's own
function, as legend-engine sends them) runs on the server's pure/v1 routes alone; no test body reaches it, so the corpus
-- legend-engine's audience, ruled to run at legend-engine's meaning (Q14) -- does not get it. The `==`, `!=`, `in`,
`contains` rewrite is the same kind (store-bound code only: legend-engine computes host-side values with Pure's
meaning), so the door will need two entries, Pure's audience (PCT: S137, S144) and legend-engine's (the corpus: those
two plus the meaning rewrites). To measure before it is built: which corpus verdicts move when the corpus bodies go
through the meaning rewrite.

**Found while building:** the product's host judge also compares a graph-fetch JSON root read with no sort as a
multiset (`HostJudgment.Json.incidentalRoot`, planned from `OrderView`) and an unsorted `Equals` by its `incidental`
flag -- step 4 (order) replaces them.

## Step 1b -- legend-engine's own answers (2026-11-24)

Prepared by a read-only research pass (every value read from the pinned sources), then asked of legend-engine 4.145.0
itself, on its DuckDB 1.3.0, on lite's DuckDB 1.5.6 and on its H2 2.1.214, from the tests' own model, data and lambdas
(`docs/records/compat-b3-homework/oracle-1b/`: the answers, the requests, the scripts). Upstream compares exactly
(`CompiledSupport.eq`, no tolerance) and prints a Float as its shortest digits (`primitiveToString(double)`).

| Tests | legend-engine's answer | Class |
|---|---|---|
| DuckDB, the 14 `calendarAggregations` tests (`testAnnualizedValue`, `testCmeValue`, `testP12MtdValue`, `testP12WtdValue`, `testP4WaValue`, `testP4WtdValue`, `testP52WtdValue`, `testPmaValue`, `testPrioryearValue`, `testPymtdValue`, `testPyqtdValue`, `testPytdValue`, `testQtdValue`, `testYtdValue`) | FAILS each on DuckDB 1.3.0 and 1.5.6, with the digits lite gets (`6.840000000000002`, ...); passes on its H2 | agrees with lite: the goldens are H2's decimal arithmetic |
| H2 `testAnnualizedValue`, `testPmaValue` | passes; lite's value read as a double is the golden, its database prints more digits | printing: B2's printer through the lift (step 2b) |
| H2 `testCmeValue` | passes; Campus printing, Lateral 2 ULP apart | Lateral a real difference: lite's IEEE division (the user, 2026-09-19, "keep it ieee") |
| DuckDB `mapping::sqlFunction::log::testProject`, `::testFilter` | PASSES: DuckDB's `ln(1.1)` is `0.09531017980432493`, the golden | a LITE BUG, below |

**The lite bug the tolerance hid.** Measured with the corpus runner's SQL dump: lite's SQL is legend-engine's
(`SELECT ln(t0.float1)`), and the database answers `...493`; the assert's expected Float was sent as
`CAST(0.09531017980432493 AS DOUBLE)` (the compatibility route writes a Float literal as legend-engine does, a decimal),
and DuckDB's DECIMAL-to-DOUBLE cast is not correctly rounded -- `...492`, one ULP low (probed on DuckDB 1.5.6:
`CAST(0.09531017980432493 AS DOUBLE)` gives `...492`; `9.531017980432493e-2` and `CAST('0.09531017980432493' AS DOUBLE)`
give `...493`). Fixed in DuckDB's dialect (product and edge): a Float literal cast to DOUBLE is written as the double
itself, its exponent form (`0.09531017980432493E0`), which DuckDB reads exactly; the corpus's `log::testProject` then
passes by exact equality (the tolerance no longer fires). Pinned: `DuckDbRenderTest.aFloatLiteralCastToDoubleIsTheDouble`.

**Brought to the user (2026-11-24) before deleting:** S15 (the 2-ULP Float tolerance) after the literal fix (no test
depends on it then); roster entries with reasons for the 14 DuckDB calendar tests ("an H2-specific golden; legend-engine
on DuckDB fails it the same way") and `testCmeValue`'s Lateral cell (the IEEE ruling); S132 (the Float tolerance in a
printed text) deleted once the lift and B2's printer land, so the two H2 printing tests pass rather than being rostered.

