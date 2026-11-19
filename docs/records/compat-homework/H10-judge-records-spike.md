# H10: the judge-records spike (2.2b under CP-14 (iii))

The JUDGES session, 2026-11-19, for the program owner. **A spike, never for merge**: branch
`claude/compat-judges-spike`, cut from `claude/compat-judges-homework` at `491f6464`. The refactor is commit `beacf4c9`.
The R-S131 trial (with its stand-in door and census switches, all off by default) is commit `228a6a18`.
The design is H6 §7's, under CP-14 (iii) (ruled 2026-11-19). The product's host judge decides and builds a failure
RECORD. A presenter words it. One run interface replaces `EdgeRun`, `EdgeHearing`, `HostWords` and `HostForms`.

## 0. Counts first

Base `491f6464` against the spike `beacf4c9`, outcomes AND messages:

| Check | Rows | Outcomes moved | Messages moved | Why |
|---|---|---|---|---|
| channel B DuckDB, 5 suites, COMPAT rules, BOTH judges | 1,249 | **0** | **0** | |
| channel B DuckDB, COMPAT rules, HOST judge | 1,249 | **0** | **0** | |
| channel B DuckDB, LITE rules, HOST judge: 462 failures (32 worded by lite's presenter; 426 lite's typing refusals, 4 database errors) | 1,249 | **0** | **0** | |
| channel B DuckDB, LITE rules, BOTH judges | 1,249 | **0** | 4 | DuckDB's own error text for the database side's rejected verdict statement, which varies from run to run (§2.3); not the spike |
| corpus DuckDB, host pass: fail roster 108 | 5,786 asserts | **2 tests** (4 asserts) | the same 2 | the S15 pair: the scalar-Float leniency left the judge by design, and R-S15, the door rewrite that takes it, is not built (§2.4) |
| corpus H2, host pass: fail roster 276 | 5,533 asserts | **0** | **0** | |

Everything else is byte-identical: the 106 other DuckDB FAIL lines, all 276 H2 FAIL lines, both lanes' skipped
rosters, their unordered and engine-order registers, and the per-assert ledgers outside the S15 pair.

**R-S131, end to end (§4):** the edge's door rewrite plus the product judge's typed S131 off.
- **DuckDB corpus: 21 of the 22 typed-S131 tests are fixed** (values 14, cells 7, including all three two-column
  cases). **H2: 3 of 4.**
- **The one left on each lane is the JSON-root test** (`graphFetch::tests::union::propertyLevel::test6`), which no
  rewrite reaches.
- No new failure, no test passing that failed before, and both registers and the skipped rosters are identical.
- One DuckDB message moved: an S15-pair assert, rewritten, is now worded as `assertSameElements`. So R-S15 must run
  before R-S131.
- The rewrite touches 448 asserts in 374 tests. Every one keeps its verdict, but the per-assert ledger renames them.

**A new core hook is needed (stop and tell, §5):** the corpus has no door. `PureTestRunner` plans the typed body
itself (`Compiler.queryResolved` → `TestExecution.planBody`, `test/PureTestRunner.java:349`), so a door rewrite such as
R-S131 has nowhere to stand on the corpus's road. The trial used a marked stand-in, `TestObserver.door`, on the scratch
branch only. The rewrite itself reaches the facts it needs through public entries alone.

## 1. What was built (commit `beacf4c9`)

**Deleted** (265 lines): `testexec/EdgeRun.java` (2 methods), `EdgeHearing.java` (10), `HostWords.java` (27: 25
failure kinds, `name`, `caught`), `HostForms.java` (4) and `LiteHostForms.java`. That is 43 methods over four
interfaces.

**Added** (202 lines):
- `testexec/VerdictRecord.java`: a sealed interface of 25 records, one per kind `HostWords` named. Each record's
  fields are that method's arguments: the facts the judge decided, and no words.
- `testexec/JudgeRun.java`: the ONE run interface, 13 methods.
  - `JudgeRun`: `open(plan, callers, execution)`, `wordsAlike()`.
  - `JudgeRun.Hearing`: `wordsName`, `listener`, `present(VerdictRecord)`, `caughtMessage(RuntimeException)`,
    `judged`, `rowAppealed`, `json`, `renderedTexts`, `writing`, `decide`, `afterFailure`.

**Changed** (26 files, +410/−489 in all):
- **The judge.** `HostJudgments`' 23 wording sites, `AssertErrorRuns`' 4 and `DatabaseVerdicts`' 1 now build a
  record and hand it to the run's `present`. The judge never names whose words they are.
  `HostJudgments.present(rules, record)` is `rules.presenter().apply(record)`, and the presenter is the hearing's
  `present`, bound at `HostJudgments.java:317`.
- **The presenters.** `LiteHostWords` and `EdgeHostWords` keep their 25 methods unchanged as plain methods, each with a
  `static present(VerdictRecord)` switch over the 25 records that dispatches to them. Neither implements an
  interface.
- **The two runs.**
  - `NoEdgeRun` (the product's) implements `JudgeRun`: lite's name, lite's presenter, a raise's own message.
  - `AppealRun` and `Appeals` (the edge's) implement `JudgeRun` and its `Hearing`: legend-pure's name and presenter,
    and a planned raise site's words from the plan's records (`Appeals.raisedWords`, now package-private). The
    SQL-text appeal's methods (`judged`, `rowAppealed`, `writing`, `decide`, `afterFailure`) are untouched.
- **The rules.** Lite's rules are inline in `Equality`: two number kinds are never equal; a Decimal compares by value;
  a Float is exact. S127 and S139 are deleted (no lane uses them, H6 §6.1). S15 on a scalar Float is left to the door
  rewrite R-S15. `CompiledPureHostForms` keeps only `FLOAT_ULPS` and `withinUlps`, which the rendered-text kind and
  the edge's SQL verdict numbers still use.
- **The callers.** Retyped from `EdgeRun` to `JudgeRun`: `TestExecution` (4 signatures), `TestRunner`,
  `PureTestRunner`, `TestObserver`, `ChannelB` and `MinimalCorpus`. `TestRunner`'s mixed-run refusal now reads
  `hearing.wordsName()`.
- **`testexecutionplan.Edge`** (the planning interface) is untouched. Its records still ride the plan through the
  decoded-plan round trip.

**Stubs, said plainly:**
1. **`VerdictRecord.AtIndex(String sentence, int index)`** carries its inner failure already worded. A quantified
   assert's element fails by throwing `AssertFailed(text)`, and the quantifier rewords that text. A build carries the
   inner record (`AtIndex(VerdictRecord inner, int index)`), which needs `AssertFailed` to carry a record.
2. **The record is presented at its failure site, through the run interface**, and `Verdicts.fail(String)` still
   throws `AssertFailed(text)`. H6 §7.4 had the listener and `AssertFailed` carry the record and the door present it
   after the run. The spike shows that is not needed for the goal (the product never knows whose words), see §6.
3. **`HostRules` stays, as the carrier of the presenter**: `record HostRules(Function<VerdictRecord, String>
   presenter)`, so the ~25 judgment functions keep their signatures. Five of them have no run record in scope
   (`tdsEquivalentOf`, `conditionOf`, `subsetOf`, `isOf`, `jsonOf`). A build passes the hearing instead and deletes
   `HostRules`.
4. **`Hearing.json()`** is kept: R-S144 (json-simple read at the door) is not built.
5. **`EdgeHostWords.cellMultisetDiffers` still compares** (it picks its sentence by whether the sorted pools hold),
   now under lite's rules (no 2-ULP). A build puts that fact in the record. The check moved nothing on it.
6. **Kept by sequence, not stubs:**
   - `renderedTexts()`: the `Rendered` kind's rule is the edge's under (iii), by design.
   - `rowAppealed` and `wordsAlike`: 2.2a deletes them.
   - `wordsName`: 1.2 deletes it, with the words in the plan.
   - `Edge.seeded`/`effectRan`: still step events on the planning interface; H6 §7.4 moves them to the hearing.
   - The engine-text claim (`EngineTestText`, `EngineTextGates`, `judgesEngineText`): 2.2b's planning line, not in
     this spike.
   - `ExecuteOptions.recording`/`edge()`/`NoEdge`.
7. **Core's test sources do not compile on the branch.** Eighteen test files and one guard list name the deleted
   types:
   - guards: `ArchitectureTest` (`:2350`, `:2397`, `:2927`, `:2958`, `:4079-4084`), `JavaEvalLedgerTest` (pins and
     comments), `HookRulingTest`, `HookRulingsAgreeTest`, `HookLambdasTest`, `EdgeSeparationTest`,
     `EdgeAuditOpenTest`, `VerdictChannelRegisterTest`, `CompatSpellingGuardTest`, `UpstreamBranchesTest`,
     `PlannerSurfaceTest`, `RefereeAppealsCensusTest`;
   - unit tests: `MixedRunRefusalTest`, `AssertErrorNativeTest`, `EqualityCompatEdgeTest`,
     `EqualityWorldsConformanceTest`, `PureAssertsTest`, `testing/Program`;
   - the comment in `tools/deps/core-layers.txt:136`.

   Five pinned residue files changed size (`AssertErrorRuns`, `AssertRuns`, `DatabaseVerdicts`, `Equality`,
   `HostJudgments`). Every guard line moves with the user's word. None was touched here.

## 2. The check

### 2.1 Method

The base runs are H6's, made at `491f6464`'s product code. The H6 measurement probes are present on both sides and
off by default, so they print nothing and change nothing.
- Channel B: `//pct:pct_channel_b_{essential,grammar,relation,standard,unclassified}` under
  `-Dpct.channelb.rules=COMPAT|LITE` and `-Dpct.channelb.judge=HOST` (BOTH when unset). The test log's per-test lines
  (`[chB-h6]`: suite, outcome, test, message) are compared whole after a sort.
- The corpus: `bazel build //spec:judge_host_{duckdb,h2}`. Compared: the four measured files
  (`*-fail-roster.txt`, `*-skipped-roster.txt`, `*-unordered-register.txt`, `*-engine-order-register.txt`), the
  per-assert ledger (`judge-host.tsv`) and every `[corpus2] FAIL` line of `host.log` (the test and its whole message).
- Run times: channel B 38-79 s a setting; the corpus 559 s on DuckDB and 171 s on H2.

### 2.2 What the check covers

- **Lite's presenter:** the 462 LITE/HOST failures are byte-identical. Only 32 of them are the judge's words
  (`AssertFailed`); 426 are lite's typing refusals (`ModelException`) and 4 database errors, which the spike does
  not touch.
- **The edge's presenter:** the corpus's 108 DuckDB and 276 H2 failures (every message identical but the S15 pair's),
  the COMPAT channel B failures, and every PASS. A pass whose assert's words decide the verdict is assertError's,
  covered in §3.
- The census of which record kinds each presenter actually worded is in §3.1.

### 2.3 LITE/BOTH's 4 messages

`date::testAdjustBy{Days,Hours,Months,Weeks}BigNumber` are ERROR (JudgesDisagree) on both sides. The host's words are
identical. What differs is the database side's rejected-statement text: one verdict statement holds two failing
casts, and DuckDB reports whichever it meets first. Two more spike runs of the essential suite gave a third and a
fourth mix:
- base: Conversion ×2, Invalid Input ×2;
- spike run 1: Invalid Input ×2, Out of Range ×1, Conversion ×1;
- spike re-runs: Invalid Input ×3 and Out of Range ×1; then Conversion ×1, Invalid Input ×2 and Out of Range ×1.

This is DuckDB's own nondeterminism. The host judge's words, which the spike changed, are the same on every run.

### 2.4 The S15 pair (DuckDB only; H2 meets none)

The S15 pair, as expected and as H6 §6.1 measured it:
- `mapping::sqlFunction::log::testFilter`: `assertEquals(0.09531017980432492, ...)` against DuckDB's
  `0.09531017980432493`, one ULP apart;
- `mapping::sqlFunction::log::testProject`: the same value in a list.

They passed under the judge's 2-ULP leniency (`HostForms.equalFloatsApart`). Under (iii) that leniency is gone from
the judge, and the door rewrite R-S15 (H6 §7.5) takes it back. R-S15 is not in this spike, so the pair fails, worded
by the edge's presenter (`expected: 0.09531017980432492 | actual:   0.09531017980432493`). Their second asserts
(`assertEqualsH2Compatible` in `testFilter`, the second `assertEquals` in `testProject`) are not reached, hence 4
ledger lines moved for 2 tests.

## 3. The presenters and assertError, as exercised

### 3.1 Which record kinds the check worded

A second run with a census switch on (`-Dlegend.measure.present`) prints one line per presented record and per
assertError raise. Its outcomes, messages, rosters, registers and ledgers are identical to the first run's, on both
corpus lanes and channel B.

| Road | Presenter | Records worded, by kind |
|---|---|---|
| corpus DuckDB | legend-pure | 37: SidesNotEqual 23, Condition 8, SameElements 2, RowValuesDiffer 2, NotEquivalent 1, JsonDiffers 1 |
| corpus H2 | legend-pure | 40: SidesNotEqual 24, Condition 8, SameElements 6, NotEquivalent 1, JsonDiffers 1 |
| channel B DuckDB, COMPAT/HOST | legend-pure | 5: SidesNotEqual 3, NoError 2 |
| channel B DuckDB, LITE/HOST | lite | 32: ErrorDiffers 16, SidesNotEqual 7, NotEq 7, NoError 2 |

So the check exercises **9 of the 25 kinds**. The other 16 word failures that no lane has today. For those, the
mapping was checked statically over the source:
- each of the 25 records' fields equals the deleted `HostWords` method's parameters, name, type and order (the
  `HostRules` argument of `cellMultisetDiffers` dropped on purpose);
- each presenter's 25 switch cases call the method of the record's name with the record's fields in that order;
- the 28 judge sites were rewritten mechanically (`words(rules).x(args)` → `present(rules, new VerdictRecord.X(args))`,
  the arguments carried as written).

### 3.2 assertError through the run interface

assertError matches its expected text against the words of the raise. On `main` the judge asked
`hearing.hostForms().words().caught(e, hearing)` (`BodyRun.java:39`). The edge's `caught` then called back into the
hearing for a planned site's words.

On the spike the judge asks `hearing.caughtMessage(e)` and nothing else. Its three callers, all through
`BodyRun.caughtMessage`, are unchanged: `AssertErrorRuns.java:37`, `:54` and `TestRunner.java:179`.
- **The product's** (`NoEdgeRun`) answers the raise's own message.
- **The edge's** (`Appeals`) answers the plan's legend-pure words of a planned raise site (by the raising statement's
  text and the site's number), and any other error's own message.

The product holds no reference to either set of words. assertError's four failure words (`NoError`, `ErrorDiffers`,
`LineDiffers`, `ColumnDiffers`) are records presented the same way.

Exercised by the census run:
- channel B COMPAT/HOST: 14 raises, 8 by planned-site words and 6 by own message;
- channel B LITE/HOST: 12 raises, lite's presenter;
- the corpus: none on either lane.

The PCT assertError tests pass identically on every setting (§0), and their messages are identical where they fail
(LITE's ErrorDiffers 16, NoError 2).

## 4. R-S131 end to end (commit `228a6a18`)

### 4.1 What runs

- **The rewrite:** `edge/IncidentalOrderAsserts.rewrite(TypedQuery)`, H6 §7.5's R-S131, typed half. It walks the
  typed body statement by statement, each statement's `letPrefix` being the statements before it, and does not enter
  lambdas. It rewrites `assertEquals(e, a, ...)` into `assertSameElements` with the same arguments, using the overload
  of the same parameters, when all of these hold:
  - `OrderView.of(a, letPrefix) == INCIDENTAL`;
  - `a` is many-valued (`Multiplicity.isMany`);
  - the pair is not two relations.
- **The door:** a stand-in, `TestObserver.door`, called at `PureTestRunner.java:349` before `planBody`.
  `MinimalCorpus`'s observer applies the rewrite under `-Dlegend.spike.rS131`. This is a new core hook (§5).
- **The product's typed S131 off:** `-Dlegend.measure.leniency.off=S131T` turns off the four typed multiset sites,
  leaving the rendered text's S131 (the `Rendered` kind, the edge's) on:
  - `HostJudgments`' cells fallback, `:128-135`;
  - its values compare, `equalsOf`;
  - its JSON root array, `jsonOf`;
  - `TdsCompare`'s grid rows.
- **Three corpus runs** on both lanes, all with the census on:
  - **P:** the refactor (§0);
  - **R0:** typed S131 off, no rewrite;
  - **RS131:** typed S131 off and the rewrite.

  Each takes 565-575 s on DuckDB and 171-195 s on H2.

### 4.2 Results

| | DuckDB | H2 |
|---|---|---|
| fail roster P / R0 / RS131 | 110 / 131 / **111** | 276 / 280 / **277** |
| newly failing under R0 (the product's typed S131 off alone) | 21: exactly the H6 typed-S131 list (values 14, cells 6, the JSON root 1) | 4: exactly the list (values 3, the JSON root 1) |
| passing again under RS131 | **20** | **3** |
| still failing under RS131 | 1: `graphFetch::tests::union::propertyLevel::test6`, the JSON root (`assertJsonStringsEqual: FIRST DIFF at $[0].legalName expected Firm B, got Firm X`) | 1: the same test |
| newly failing only with the rewrite (the weaker cell pool) | **0** | **0** |
| passing under RS131 but failing under P | 0 | 0 |
| skipped roster, unordered register, engine-order register vs P | identical | identical |
| asserts rewritten | 448 in 374 tests | 448 in 374 tests (the same bodies) |
| rewritten tests whose verdict moved | the 20 above | the 3 above |
| per-assert ledger lines renamed `assertEquals` → `assertSameElements` | 446 (444 PASS, 2 FAIL) | 433 (all PASS) |
| FAIL messages moved among tests failing under both | 1: `sqlFunction::log::testFilter`, `expected: 0.0953…2 \| actual: 0.0953…3` → `expected: [0.0953…2] \| actual: [0.0953…3]` | 0 |

**DuckDB's 22.** H6 §6.1 counted 22 DuckDB tests with a typed S131 decision: values 14, cells 7, the JSON root 1.
- **21 pass with R-S131:** the 20 above, and `projection::function::concatenate::testConcatenateClassJoin`. That
  test's cells were rewritten and pass. Under R0 it passed only because DuckDB returned its rows in the golden's order
  on that run, as it did in H6's leniency-off run.
- **The 22nd is the JSON root,** which a door rewrite cannot reach: an `assertJsonStringsEqual` is not
  `assertSameElements`.

So, for (iii): **the typed half of S131 needs no leniency in the product's host judge.** The JSON root needs a
whole-kind rule of the edge's (beside `Rendered`), or a line in the "not promised" list.

**The weaker cell pool.** For a TDS of two or more columns, `assertSameElements` pools cells, where today's fallback
matches row tuples. It passed nothing that fails today: 0 tests move on either lane. H6 §7.5's phase-3 check is
answered for the corpus as it stands.

### 4.3 What it shows for the build

1. **Order the door rewrites: R-S15 before R-S131.** `testFilter`'s assertEquals of a Float over an incidental actual
   was taken by R-S131 here, so its failure is worded as `assertSameElements`, not as upstream's `assertEquals`. Under
   R-S15 first, it becomes `assertEqWithinTolerance` and passes.
2. **The rewrite is broad.** 448 asserts in 374 tests take it, almost all passing positionally today (DuckDB's rows
   arrive in the golden's order, helped by `StableScanOrder`). Their verdicts do not move. But the per-assert ledger
   (`judge-host.tsv`) renames them, and a future failure in any of them speaks `assertSameElements`' words. A build
   could carry the original assert's name for the ledger and the words (a field of the rewritten call), or accept the
   rename as the door's visible trace.
3. **No new judge hook.** Everything the rewrite reads is public, and the product judge loses four leniency sites. The
   only core change is where the rewrite stands (§5).

## 5. New core hooks (stop and tell)

**The refactor needed none.** `JudgeRun` replaces four existing hooks with one. It is the run hook the test door
already took (`TestExecution.runProgram`'s `referee`), retyped, and it adds no call site. Its `present` and
`caughtMessage` are the old `HostForms.words()` and `HostWords.caught` reached in one step instead of two.

**R-S131 at the corpus door needs one, and the spike stopped there.** Every door rewrite of H6 §7.5 (R-S137, R-S144,
R-S131, R-S15) runs on the TYPED body after `Compiler.queryResolved` and before planning. Each road compares as
follows:
- **PCT channel B has a door.** `ChannelB` builds the `TypedQuery` and hands it to `TestExecution.runProgram`, so an
  edge rewrite slots in there with no core change.
- **The corpus has none.** `MinimalCorpus` hands its tests to `PureTestRunner` (core's `com.legend.test`), which types
  and plans each body itself, at `test/PureTestRunner.java:349`:
  `TestExecution.planBody(Compiler.queryResolved(ctx, resolved), runtimeFqn, options)`. No caller can reach the typed
  body in between.

The spike measured R-S131 through a stand-in, `TestObserver.door(TypedQuery)` (default: the body unchanged), called at
that line, with `MinimalCorpus`'s observer calling `edge.IncidentalOrderAsserts.rewrite`. That stand-in IS a new core
hook and is for the scratch branch only. Three ways forward, for the owner to rule:
- **(a) A door on the shared runner:** `TestObserver.door` (or a `UnaryOperator<TypedQuery>` given to
  `PureTestRunner`), the four rewrites behind it in the edge. One method and one call: the smallest change, but a new
  extension point in core.
- **(b) The existing body-rewrite slot widened.** `compiler.spec.RelationCounting.counted(List<TypedSpec>)` (the
  style's two-form facet that carries R-S137 today, called at `TestPlanner.java:227`) already sees the lifted body at
  planning. It cannot host R-S131 as it stands: the rewrite must look up `assertSameElements`' typed callee
  (`ModelContext.findFunction`), and the slot gets no `ModelContext`. No context-free lookup of a typed native exists.
  So this changes an existing hook's signature, and it keeps alive a two-form facet that (iii) meant to delete.
- **(c) The corpus's runner owned by the edge**, a copy of `PureTestRunner`'s run loop on the edge's side. No core
  change, but a second runner (G1's "one road" pulls against it).

My recommendation is **(a)**: one named door, used by the corpus alone, whose rewrites are ordinary lite functions.

The rewrite itself needs nothing private. `edge/IncidentalOrderAsserts.java` uses `TypedQuery.body()`,
`TypedQuery.context()`, `TypedQuery.withBody(...)`, `ModelContext.findFunction`, `OrderView.of(TypedSpec, letPrefix)`
(`compiler/spec/OrderView.java:97`), `NativeFn.Verdict.of` with `Calls.calleeIdOf`, and the public `TypedNativeCall`
record. The edge reaches `compiler.spec` and `compiler.spec.typed` already (`UpstreamRelationCounting` does). The guard
suite was not run on the branch (core's tests do not compile there, §1 item 7).

## 6. Revised sizes

**2.2b under (iii): about 4.5 days, against H6's 7** (H6 §6.5, §7.4).

| Part | H6 | Now | Why |
|---|---|---|---|
| the run seams and the words: one `JudgeRun`, `VerdictRecord`, two presenters, the 28 judge sites, assertError | 2 + 2.5 | **1** | done in spike form, byte-identical. Left: stubs 1, 3 and 5 of §1 (the quantified element's inner record, the hearing threaded instead of `HostRules`, the multiset fact into its record) |
| the step events (`Edge.seeded`/`effectRan`) into the hearing; the door's options (`ExecuteOptions.recording`, `edge()`, `PlanningOptions.edge`, `NoEdge`) | (in the seams) | **0.5** | not spiked; small, mechanical |
| the engine-text claim (`EngineTestText`'s 13 methods into one, `EngineTextGates`, `judgesEngineText`) | 1 | **1** | not spiked; unchanged |
| the leniency code | 0.5 | **0.25** | done: S127 and S139 deleted, S15 out of the judge, the `Rendered` rule kept as the edge's. Left: `CompiledPureHostForms` renamed for what remains (the Float tolerance) |
| core's tests and guards | (in checks) | **1** | 18 test files name the deleted types, 12 of them guards (§1 item 7); every guard line with the user's word |
| checks | 1 | **0.5** | the check is scripted: channel B's 4 settings take about 4 minutes, the corpus 12 |
| **2.2b** | **7** (6 with 1.2) | **≈ 4.25, say 4.5** | 1.2's records no longer matter to it |

The design choice behind most of the saving is to **present at the failure site, through the hearing**, rather than
carry records to the listener and present after the run (H6 §7.4). If the owner wants the records carried (for a
consumer of the facts, of which there is none today), add **1 day**: `AssertFailed` carries a record, `VerdictRecord`
moves to a package `com.legend.error` can see, `AssertListener.verdict`'s detail becomes a record, and five listeners
change.

Two sequencing facts the spike shows:
- **S15 out of the judge moves the DuckDB corpus by 2 tests** until R-S15 lands at a door. Either 2.2b waits for the
  door rewrites (phase 3, which needs §5's ruling), or the two rows are listed, with the user's word, until it does.
- **2.2b does not depend on 2.2a.** The spike left `rowAppealed` and `wordsAlike` in place; 2.2a deletes them in
  either order.

**2.2a: 3.5 days, unchanged.** It was not spiked, and nothing here moves it.

**Phase 3's door rewrites (H6 §7.5, about 1.5 days)** gain the door itself: 0.25 day for (a) of §5, once ruled.
R-S131's own cost is unchanged by the trial (its code is 106 lines, done in spike form). Two things are added:
- ordering the rewrites (R-S15 first);
- the choice of how the ledger names a rewritten assert.

## 7. What I would do differently

1. **Find the door before writing a door rewrite.** R-S131's code was the easy half; whether the corpus's road lets an
   edge rewrite the typed body was the real question, and a caller census (who holds the `TypedQuery` before
   `planBody`) answers it in minutes.
2. **Thread the hearing, not a function in `HostRules`, from the first edit.** The carrier was chosen to keep 25
   signatures still; five of them lack the run record, and a build pays for that twice.
3. **Keep the presenters' methods and add the switch as a thin adapter**, as here. The words could not move (the
   byte-identity and the static check show it). Rewriting the presenters to match on records directly is a separate
   change, worth making only behind a guard that compares the two.
4. **Give the check a fixture per kind.** The lanes fail on only 9 of the 25 kinds; the other 16 rest on the static
   check. A build should add one failing assert per kind under each presenter (a small fixture, LITE and COMPAT), so
   every kind's words are compared.
5. **Measure the base's noise first.** LITE/BOTH's database error text varies from run to run on DuckDB; two base runs
   before the comparison would have shown it at once.

## 8. Method and commands

The branch: `claude/compat-judges-spike`.
- `beacf4c9`: the refactor.
- `228a6a18`: the R-S131 trial, plus the switches, all off by default.

The run scripts are scratch, not committed. What they run:
- **Channel B**, per setting:
  `bazel test //pct:pct_channel_b_{essential,grammar,relation,standard,unclassified} --jvmopt=-Dpct.channelb.rules=<COMPAT|LITE> [--jvmopt=-Dpct.channelb.judge=HOST] --nocache_test_results`.
  Then each suite's `test.log` `[chB-h6]` lines are compared, sorted, with H6's runs of `491f6464`
  (`runs/COMPAT_BOTH`, `len/DUCK_COMPAT_HOST_LOG`, `runs/LITE_HOST`, `runs/LITE_BOTH`).
- **The corpus**, per run:
  - `spec/corpus.bzl`'s `_SPIKE` list set to the run's switches;
  - `bazel build //spec:judge_host_{duckdb,h2}`;
  - the four measured files, `judge-host.tsv` and `host.log`'s `[corpus2] FAIL` lines compared with H6's LOG run
    (its probes print only `[lenient]` lines, filtered out) and between P, R0 and RS131.
- **The switches:**
  - `-Dlegend.measure.present=1`: the census (`[present]`, `[caught]` lines);
  - `-Dlegend.measure.leniency.off=S131T`: the product's typed S131 off;
  - `-Dlegend.spike.rS131=1`: the door rewrite, with one `[rS131]` line per rewritten test.
- **The static check of §3.1:** the 25 records' fields against `git show beacf4c9~1:…/HostWords.java`'s parameters,
  and each presenter's switch cases against the records' fields.
- **Base noise:** LITE/BOTH's essential suite was run twice more on the spike to show DuckDB's error text varying.

Nothing was changed on `main`. No pin, ceiling, ledger, roster or allowlist moved. Core's test sources do not compile
on the spike branch, and the record says so (§1 item 7).
