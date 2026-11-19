# H10: the judge-records spike (2.2b under CP-14 (iii))

The JUDGES session, 2026-11-19, for the program owner. **A spike, never for merge**: branch
`claude/compat-judges-spike`, cut from `claude/compat-judges-homework` at `491f6464`. The refactor is commit `beacf4c9`.
The R-S131 trial (with its stand-in door and census switches, all off by default) is the next commit (pending).
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

R-S131, end to end (§4): pending, measuring.

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
