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

**Step 1b landed and checked (2026-11-24):** pushed `a770240b` (the literal fix, legend-engine's answers, S15 deleted,
the lean-SQL baseline re-pinned: 21 rows, every one a shrink). Its round green on the Bazel tree; the warm lanes on
a770240b -- Snowflake, Databricks, BigQuery, ten suites each under both rule sets: nothing moved, every ledger row
matching, every canon line `ulp-policy=0` (no cloud Float sat one or two ULP from a test's expected value). Its
independent review: no HIGH; MEDIUM M1 (the division operator's cast text bypassed DuckDB's literal form) and M2
(`edge.EqualToJson`'s own 2-ULP tolerance, where legend-engine compares exactly) fixed in `5450ac7a` with the LOWs
(registers S15 and S132 amended, DuckDB's own decimal-to-double coercion of a 16-17-digit literal recorded as a gap, the
dead ULP census removed); Trino's compatibility route and ClickHouse's product route, whose Float literals are
decimals, to be probed.


## Step 3 -- the assert catalog in legend-pure's words (built 2026-11-24)

The user's rulings of 2026-11-24: "keep errors close to pure when cheap/fast so don't have to reword everything on
compatibility"; the values in the sentences in lite's own forms, and one line ("Agree with both proposals"). Each
sentence read from the pinned legend-pure sources (`docs/records/compat-b3-homework/catalog-words-vs-legend-pure.md`).
`error/AssertKind`'s templates, on every route:

| Kind | Sentence |
|---|---|
| EQUALS | `expected: {expected}, actual: {actual}` |
| EQUALS_KINDS | `expected: {expected} ({expectedKind}), actual: {actual} ({actualKind})` (lite's kinds kept, ruled) |
| NOT_EQUALS | `{expected} should not equal {actual}` |
| SIZE / NOT_SIZE | `expected size: {expected}, actual size: {actual}` / `size should not equal: {size}` |
| CONTAINS / NOT_CONTAINS | `{collection} does not contain {value}` / `{collection} should not contain {value}` |
| EMPTY / NOT_EMPTY | `{collection} is not empty` / `Expected non-empty collection` |
| TRUE / FALSE | `Assert failed` |
| IS / IS_NOT | `expected: {expected}, actual: {actual}` / `Should not be identical to: {actual}` |
| JSON_EQUALS | `JSON strings don't represent semantically same object, expected: {expected}, actual: {actual}` |
| ERROR / NO_ERROR | `Execution error message mismatch. The actual message was {actual} where the expected message was:{expected}` (NO_ERROR's actual `"No error was thrown"`) |
| ERROR_LINE / ERROR_COLUMN | `Execution error line mismatch. Actual: {actual} where expected: {expected}` (column alike) |

Kept lite's own (legend-pure has no sentence of its own, or lite's carries more): SAME_ELEMENTS, SAME_INSTANCE,
INSTANCE_OF, TDS_EQUIVALENT, WITHIN_TOLERANCE, ERROR_NO_POSITION. NOT_EQUALS now carries both sides, as legend-pure's
"X should not equal Y" prints them (the record's builders pass the expected side; `HostJudgments` 659 -> 660, dated).

**Guards:** the nine legend-pure spellings are RULED rows of the spelling register (`CompatSpellingGuardTest`) and of
the frozen terminal set (`TerminalRowsFrozenTest`, ruling `ASSERT_CATALOG`); the two assertError sentences written
across two lines are pinned in the constant-pool guard (`CompatConstantPoolTest`, folded). **Re-recorded:** about 20
core tests' expected sentences; channel B's ledger (63 rows, eleven lanes: the rows' texts converted frame by frame,
nothing else moved); the corpus's accepted-divergence witnesses (39 rows: `expected "select top 20` ->
`expected: "select top 20`, and one JSON witness `got [` -> `actual: [`); the generated goldens (the edge contract, the
Python and engine-client generated files, the edge corpus and relational corpus outputs).

**With it, step 1b's review follow-up:** the review's fix wrote a negative Float literal cast to DOUBLE in parentheses
on DuckDB so a minus before it never writes `--` (a comment) -- 2 characters per negative literal, which grew three
lean-SQL families. Replaced by the precise rule: a negation parenthesizes an operand whose text starts with a minus (a
negative literal, a negation, DuckDB's bare double), every dialect; the literal itself bare, as every negative literal
is written. The lean-SQL baseline and query capture unchanged; pinned in `DuckDbRenderTest`.

**And the audience object** (step 2's review M1, the lift design's "one audience choice"): `com.legend.test.Audience`
(the plan options, the run hook, the door, together); `TestObserver.audience(test)` with no default (the product's is
`Audience.PRODUCT`); `PureTestRunner` threads it to the setups, the body and the judge; the corpus's observer returns
legend-engine's (the recorder and the referee, the upstream style, the door); `EdgeProgram.upstream` refuses a plan
style that is not upstream's.

**Step 3's independent review (read-only, of `7619acb4`):** no HIGH -- every template checked against the pinned
legend-pure source, the slot orders, the builders, the re-records (every converted row re-derived by script: words only,
no key, status or row moved), the guards and the negation rule found sound. Fixed in the next commit: M1 the expected
side of `X should not equal Y` printed without its class (`? {...}`; `AssertRecords.notEqual` takes both classes, a
failing instance probe pinned in `AssertBarTest`); M2 the differences page's section 17 rewritten; L1 the stale
`expected size n, got 1` (S137, the door's comment), S398's citation (`assertNotEquals.pure:18`), the spelling guard's
comment amended; L3 a negation over a cast written as its value alone (the tagged carrier) parenthesized too, and the
rule pinned on seven dialects (`NegationLeadingMinusTest`); L4 the ledger's dated re-record note; L5 recorded in S398
(an in-body `assertEquals` guard raises `Assert failed`). L2 recorded, not unified: PCT's channel B and the edge's test
helper choose the plan style, the run hook and the door from one rule-set flag each, outside the runner's audience
object (`Audience`'s comment says so).

**Step 3 landed and checked (2026-11-24):** pushed `7619acb4`; its round green on the Bazel tree (292 of 293, one
skipped); the warm lanes on 7619acb4 -- Snowflake, Databricks, BigQuery, ten suites each: no verdict moved, only the two
expected messages re-worded (`testArcCosineError`, `testArcSineError`: the assertError sentence), every ledger row of
each lane matching (Snowflake 11, Databricks 23, BigQuery's two re-worded rows as prefixes of the detail); BigQuery's two
census failures main's own (the UNION ALL and coalesce_AllEmpty rows); the canon line's `ulp-policy` field gone with
step 1b's census, its counts unchanged.

## Step 2b -- the lift (built 2026-11-24, round pending)

The user's option (B), substitution before planning ("Great let's do it!"), on B2's printer (B2b's S1,
`edge/PureRelationTextWriter`, merged into B3 with B2's S2). `edge/UpstreamPrints.read(typed, writes, reads)`: each
assert side that prints a relation (its own `toString`, the corpus helper's `toCSV`, a TDS's `.csv`, followed by literal
`replace`s) is read before the body through the public test door on the body's own session, once its setups have run
(`Audience.read`, the runner's reader; PCT's channel B reads on its connection), printed by the edge's printer with the
replaces applied in Java, and substituted as a String literal; lite's judge compares at the assert's place. A body that
writes is left as written (the design review's gate: its facts' effects, the caller's). The read is one shape the
program road plans: the relation with the lets it reads substituted, each `execute(...)` bound to a let of its own
before it (`readBody`; the round's catches: PCT's `eval::testSimpleEval`, a let holding a to-one reader, and the
corpus's `testProjectAssociationTdsV2` / `testUnionTwoRelationMappings_EmbeddedFirmProject`, an `execute(...)` read where
it stands). A printed relation with no defined order compared with a golden becomes an explicit `assertSameElements` of
the two texts' lines (split at the line break, or at what a literal `replace` put there), typed by the typer: S131's
multiset made explicit (step 4's printed tables, pulled in).

**Measured on the first round (b3s7, before the read fixes above):** PCT channel B under the compatibility rules: one
regression (`eval::testSimpleEval` on DuckDB, SQLite and H2, the read's shape: fixed) and four H2 gains
(`project::testSimpleProject`, `testSimpleProjectList`, `testSimpleProjectWithEmpty`, `composition::testFilterPostProject`:
the edge's SQL text needed a list function H2 has no encoding for; printed in Java they pass; their ledger rows left).
The corpus: DuckDB's 14 `calendarAggregations` tests fail as legend-engine fails them on DuckDB (step 1b's oracle: the
goldens are H2's decimal arithmetic; brought to the user with step 1b); H2's `testCmeValue` fails on its Lateral cell
(lite's IEEE division, the user's ruling of 2026-09-19); H2 gains `testMtdValue`, `testSubclassValue` and
`groupBy::datePeriods::testGroupByWithFilterFunction_noDatePath`; two reads refused by the program road (fixed above).

**Rulings relayed to B2 (the user, 2026-11-24, "Int as float agree, parser bumps are always okay"):** an integer-held
Float's text on lite's route (`1000000.0`) kept; the parser-equivalence floor's moves approved, now and from here on
(B2 records both beside S311 and the floor's pin).

**The fast dev loop, written down (the user, 2026-11-24: "Are we optimized ... seems like every time I ask for update we
have not made huge progress", then "Where can we write this down so we stick to this loop"):** step 2b cost about six
hours, about half of it avoidable -- five full corpus regenerations (40 minutes each), one per failure, and three
guesses at a typed tree before a one-run probe answered it, while the single-test corpus runner (2 minutes) was there
the whole time. The rules are in AGENTS.md, "The fast dev loop", where every session reads them: the narrowest runner
first; probe, don't guess; read the whole round before fixing; one regeneration and one round per step; never idle
while a run goes.

**Ruled by the user, 2026-11-24 ("Love it"), for step 5 (the one equality):** strict typing stays lite's alone, and
upstream's looseness is carried at the doors -- never a warning mode in the core (tenet 6 of the six: no modes in the
core). The typer does not throw on a comparison across types (`==`, `!=`, `in`, `contains`, `indexOf`, ordering); it
types it as a marker node, "a comparison across types". Lite's own programs: nothing removes the marker, so lite reports
it as a compile error before planning, naming the fix. legend-engine's door replaces the marker with engine's
comparison (the database's own `=`; an enumeration against a String by its stored source value, decision 3);
legend-pure's door with Pure's answer, `false`. The core does the same thing for every audience. **And a migration
report:** each door lists every comparison it loosened -- the expression, the two types, upstream's answer and lite's fix
(e.g. `my::Order.status == 'OPEN'`: an enumeration against a String, answered as legend-engine does; under lite a compile
error -- write `my::Order.status == my::Status.OPEN`) -- so a team bringing an engine project sees exactly what to change
before moving to lite's strict rules; no answer changes. (Asked by the user: "strict type in lite only or strict type
everywhere but in engine the strict is a warning ... or something else smart"; the warning became the door's report.)

**Step 2b landed and reviewed (2026-11-24):** pushed `9b1e194f` (round green, 292 of 293). Its independent review: no
HIGH. Fixed in step 4's commit: M1 a print inside a lambda (its own scope, maybe never run) left as written; M2 the join
rewrite -- an empty golden left as written, a list moved from its let never past a write, a golden read through its let
(a separator inside a value recorded as a known narrowing: the golden's pieces against the list's values); M3 a joined
unordered table compared by rows (the golden's cells cut by each printed line's cell count), never by cells; M4
`preval`'s lambda read by the order view (`tds::sort::testSimpleSortAscWithPreval`'s sort was out of sight); L1 the
rewritten assert keeps its position; L2 the gates log's engine-order line corrected; L5 tests pin a body that writes, a
lambda's print, the joined rows. Recorded: L3 the lean-SQL baseline's net move from 7619acb4 is B2's 24 dated rows up
(B3's step 1b shrinks taken with the merge's regeneration); L4 the effects gate does not count the relation `write`
native (latent: no printed side after one). Still compared exactly (the review's item 2, recorded): two prints compared
with each other, a print under `assertNotEquals` or a three-argument `assertEquals`, a body that writes, prints the
side rule does not lift (`toString(rel, true)`, other toCSV formats, raw grids, dynamic pivots, a print inside a further
function), joins over non-String lists or bare cells, the four-argument `makeString`.

## Step 4 -- an order-dependent comparison of rows with no defined order (built 2026-11-24, round pending)

The user's ruling (2026-11-24: "my ideal state for product is that we don't let platform devs or users write tests that
do not have a sort ... But we still need multi-set compare for backwards compatibility"). Built:

- **The product refuses it.** `compiler/spec/OrderedAsserts.refuse`, called by the test planner after the lift
  (`TestPlanner.lifted`): an `assertEquals` or `assertNotEquals` whose side is rows (a relation, or a collection rather
  than one value) with no defined order (`OrderView.INCIDENTAL`: a store or frame read with no sort, or past a
  grouping, a join or a union) is a TYPE-phase error naming the two fixes -- sort it, or `assertSameElements` when the
  order does not matter.
- **The compatibility door makes upstream's explicit.** `edge/UpstreamTestBodies.rowsOrderFree`: such an `assertEquals`
  becomes `assertSameElements` of the same two sides (built by the typer, `EngineSubstitutes.call`); the lift's printed
  tables and the join rewrite (step 2b) are the same rule for texts.
- **The judge's flag is deleted.** `HostJudgment.Equals.incidental` and its row multiset (the host judge compared an
  incidental side as a multiset of rows): an `assertEquals` is in order everywhere.
- **The order view, corrected where the refusal found it wrong:** an `aggregate` with no grouping is one row (DEFINED),
  as the native and as the typer's `TypedAggregate` node; `preval`'s lambda is read inside an execute frame.
- **Lite's own tests that compared unsorted rows in order now sort** (the parity frames, the catalog body, the decimal
  lists, a filtered relation): the product's rule applies to lite's tests first.

Measured with a second, wider half (a print or a join of such rows: `toString`, `toCSV`, `joinStrings`, `makeString`):
30 corpus tests a lane and a handful of PCT tests pass only by legend-engine's scan order, and the door has no explicit
order-free form yet for a join of a list that is not Strings, of bare cells, or for a print the lift does not read. That
half is deferred, for the user's word.

**A TDS's flattened cells at the door:** upstream's `assertEquals(golden, $tds.rows.values)` -- a TDS's cells
flattened -- with no defined order is compared as ROWS since step 5a (the user, 2026-11-24, "should we somehow tuple
compare? Even if no real tuple type?"): the door cuts a golden of literal cells into rows of the actual's columns (a
typed table literal) and makes the assert `assertSameElements` of the two tables, a multiset of rows with the columns
checked (`UpstreamTestBodies.asRows`; `flatCellsRowCohesion` pins that a cell moved between rows fails). A golden it
cannot cut -- not literal, a width that does not divide, a cell of another kind than its column (a day in a date-time
column: step 5a's review, H1) -- stays Pure's element multiset of cells, recorded: a cell moved between rows of a wrong
answer is not caught there. The product refuses the shape; sorted, it compares in order.

**Step 4's review (bb95cdc7, no confirmed wrong verdict on a test that exists) -- fixed with step 5's first slice:** H1
an assert inside a helper is refused where the judges inline it (`BodyDriver.helperAssertRoot`); M1 every `assertEquals`
overload has its `assertSameElements` twin at the door (`assertNotEquals` has none in Pure: left as written, so lite
refuses it by name -- none in the corpus or PCT); M2 one value on either side is no order (no refusal, no rewrite); M3
the walk's scope (a lambda's parameters are values, its lets read in order; an `assertError`'s lambda not walked), one
walk shared by the planner and the door (`OrderedAsserts.rewrite`); M4 an `assertEquals` of two grids compares their
rows in order, and S131 amended (rows and grids built; JSON roots and prints or joins to follow); M5 an
`assertSameElements` of two grids is the grid pair's row multiset with its columns checked, on both judges (it was a
pool of loose cells on the host and unjudged on the database judge); M6 the test plan's format 41 -> 42. LOWs: the
stale descriptions fixed; unit tests for the walk (`OrderedAssertsTest`). Recorded: a single value picked from
unordered rows, `assert($a == $b)`, natives OrderView does not list.

## Step 5 -- the one equality (built in slices from 2026-11-24)

**Slice 5a, comparisons across known types (the user's ruling of 2026-11-24, "Love it"):**
- `compiler/spec/EqualityClasses`: the rule's types -- Integer, Float, Decimal (any precision) each a type; String;
  Boolean; Byte; a day; a date-time; a time; each enumeration; Variant; a class, crossing another only when neither is a
  subtype of the other. Known only at run time, never marked: Number, the abstract Date, LatestDate, Any, a type
  variable, a generic, a function, a relation. The comparisons, known by their declarations' identities: `==`, `!=`
  (its `equal`), `eq`, `<`, `<=`, `>`, `>=`, `compare`, two-argument `max`/`min`; `in`, `contains`, `indexOf` against
  the collection's element type.
- The typer types such a call as the marker `TypedCrossTypeComparison` (its kind, its operator, the two types) around
  the call -- **in the query or test body a door receives** (`SpecCompiler.typeQueryBodyMarked`, `TypedQuery.body`).
  Measured 2026-11-25: marking everywhere the typer runs broke about 32 corpus tests a database, the marker reaching
  passes after every door -- a function's body typed when the planner inlines it, an `execute(...)` frame typed at
  planning, a relational mapping's normalized operations. Those keep the database's comparison for everyone, by
  construction (`Typer.inFunctionScope` types unmarked); lite's strictness over a function's body comes with the
  compile route (the next sub-slice). An island (`#SQL{...}#`, Cypher) compares by its own language's rule, unmarked.
- Lite refuses the marker by name (TYPE phase): the test planner (`TestPlanner.lifted`) and every query's planning
  entry (`PlanMaker.plan`) before anything is planned; the lowering for one that reached it another way.
- legend-engine's door (`edge/EngineComparisons`: the pure/v1 routes, the corpus's test-body door, PCT's engine
  audience) rewrites each marker to the call itself, the database's comparison; legend-pure's door
  (`edge/PureComparisons`: PCT's channels A and B) to Pure's answer by the types alone -- an equality `false` (refused
  by name where both sides may be empty: Pure says true then), a membership `false` or -1, an ordering across number
  kinds the comparison itself.
- **Step 5a's review (2026-11-25), fixed before the commit:** H1 a date cell of another kind than its column is not
  cut into rows (a day lowers to midnight, so it could pass a wrong date-time); M1 an argument written in the received
  body keeps its markers when the call's body is inlined (`Overloads.inlineNormalized` hands the body the argument as
  the caller typed it); M2 the host halves' execute answers legend-engine's meaning (`HostAnswers.hosted` checks it as
  pure/v1's execute does) and a test-data seed refuses a marker by name (`ProductStatements.lowerTestDataSeed`); the
  service test runner runs the product's meaning (census C1), so it refuses by name -- if it becomes legend-engine's
  door, both rewrites come with it; M3 measured, not changed: refusing legend-pure's orderings of two non-numbers failed four
  PCT tests the database's comparison passes (testCompareMixedTypes, testDateCompare, testMax_Date, testMin_Date), so
  the door keeps the database's comparison and the difference is recorded -- a day against a date-time at one instant,
  which legend-pure orders the day first and the database ties; M4 `max`/`min` over two Dates
  marked; an enumeration against the `Enum` class and two classes that share a subclass are not crossings. LOWs:
  PlanMaker refuses before it reads the runtime; an Integer cell beyond a long is not cut; stale text fixed.
  Recorded: a refusal of `!=` names `==` (the parser writes `!=` as `not(equal(...))`); legend-pure's door does not
  evaluate a side its types answer (legend-pure does, so a side that raises raises there).
- Still to build: the migration report (each door lists what it loosened), the IDE's compile route, and the measured
  re-pins. Then 5b (types known only at run time: the tag beside the value), 5c (the asserts as their Pure bodies,
  VerdictSql's comparison layer deleted).
