# EXEC verification

## Verdict on the unit: agree (WEAK)

All three S1 findings survive checking. The column-type grammar is re-parsed in eight classes, and exec itself
round-trips a typed `SqlType` through that string. There is a second runner with its own shape pass and bracket, and
G1 names "ONE runner (`PlanRunner`)". Session acquisition sits behind two source types that every door takes side by
side. Together they cover typing and decoding, transactions and routing, and session opening: most of the unit.

The strengths are real: sealed dispatch, connectors, the one JDBC send site, thin doors, careful failure paths. That
makes this a borderline WEAK/MIXED call, but "design problems that slow every change in the area" fits better than
"sound design, notable debt". None of the findings is refuted. Five need factual corrections, and none of the
corrections changes a severity.

## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| EXEC-1 | S1 | CONFIRMED | S1 | `ExecutionPlan.java:661` has a string `slot`, while `store` is typed. `ColumnFacts.java:182-208` writes the grammar. I counted 24 parse-call lines, about 28 calls; the slot-word ladders are the ones cited; Pure-type matches are about 61-69 in the same 8 files. `Kind.of` has `default -> BY_PURE_TYPE` (`JdbcDecode.java:150`). The per-cell parses are as cited (`:171,176,285,503,540`); `RowDecode.cell` calls `slotWord` on every cell (`RowDecode.java:108`). The "{"/"[" check is at `JdbcDecode.java:633`. Small fix: the readers are 8 classes, not 6 (add `HostRows` and `WireTypes`); `CellText.typeArguments` parses the Pure type, not the slot. Stronger evidence below (Missed 1). TeaVM does not apply: `//wasm` depends on `//core:plan_side`, not on exec or result. |
| EXEC-2 | S1 | CONFIRMED | S1 | `PlanRunner.sequence` `:378-475` and `ProgramRunner.shapeOf` `:355-437` are near-twins. The divergences are verified: Effect is a change at `:417-420` but not in `shapeOf`; exactly one result (`:449`) vs at most one (`:429`); `inTransaction` is unconditional at `:462-470` against the javadoc at `:373-374`; `effect` vs `script`. Drift was ruled on (`JdbcStatements.java:343-345`). The re-encoding to `Callback(int id)` and the two impossible arms are at `TestRun.java:76-117`. The duplicated block is at `ProgramRunner.java:219-233/238-249`. G1 (`GOALS_OF_RECORD_2026_11_07.md:26-27`) literally says "ONE runner (`PlanRunner`)". Some divergences are documented policy (the effect rule, R17), as the reviewer allows. |
| EXEC-3 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Verified: `Source` has 8 variants; `SessionSource` has 5 plus a switch; the `instanceof` checks are at `ProductRun.java:79,104`; `openShared`, `openOwn` and `projectDatabase` are three ladders; project extraction is repeated at `:213-217, 389-393, 474-478`; there are four refusal helpers; `boolean[] state` is at `:250-269` and `made`/`broken` at `SystemDatabase.java:59-60`. Two corrections. First, `H2Connector.java:49` is an id counter (`PRIVATE_IDS`), not a session cache. Second, "no close or clear" is a stated design: `HandleStore`'s javadoc says no eviction, because evicting an in-memory connection drops its tables. That justifies no eviction, but not the missing lifecycle owner, so the finding stands. |
| EXEC-4 | S2 | CONFIRMED | S2 | 1,209 lines. One-element arrays at `:104,199,221`. 21 distinct package-private statics are called from other classes (grep). `dateTimeText` has one caller, `JdbcStatements.java:705`. `deliver` nests deeper than five levels (`:630-716`). 52 commits (re-counted with `git log`). |
| EXEC-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The two brackets (`Sessions.java:412-443`, `JdbcStatements.java:473-546`) and the tunnels (`PlanRunner.java:462-473`, `ProgramRunner.java:464-475`) are verified. One claimed difference is not real: "sets autocommit `true` rather than restoring it". `inTransaction` only gets that far when `ownsTransaction` (autocommit was true), so setting true is restoring. The real differences are isolation and the commit-failure read-back path. |
| EXEC-6 | S2 | CONFIRMED | S2 | 12 abstract methods plus a default `mark`; `Marked` forwards all 12 (`Statements.java:156-222`). `query(...Rows)` has one caller, which passes `row -> { }` (`PlanRunner.java:536`). 17 `sent` pairings (13 in PlanRunner, 4 in JdbcStatements). `legend.test.current` is read at `StatementOrigin.java:~93`. Detail: MongoDB refuses 7 methods outright and `inSnapshot` only for a non-NONE snapshot. |
| EXEC-7 | S2 | CONFIRMED | S2 | `Send` enum, sealed `Answered`, four `*Of` downcasts and two "not reached" throws (`JdbcStatements.java:551-661`). The `held(Connection, RaiseReadBack, SQLException)` overload builds `new SessionState()` and is called only from `RaiseReadBackTest`. `getParameterCount() == 1` is at `:319`. |
| EXEC-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | `Node permits` 8 kinds (`ExecutionPlan.java:186`); the cited refusals are verified, and TS repeats "a sequence inside a sequence" (`engine-client/src/plan-runner.ts:770`). Three corrections. (1) "`writes` is implied by the root's shape" is false: a reading Sequence of staging Effects plus a result exists (`PlanMaker.java:160-179`), and writing Sequences are also Effects plus a result (`OutputPlan.java:95-103`, `SnapCopy.java:79-86`). So `writes()` cannot be derived from the step kinds without a typed staging-vs-store Effect. (2) `JdbcStatements.java:625-627` (the `Send` enum) and `PureV1Writer.java:198-200` (the objects envelope) are not plan-shape arms; about 15 are. (3) There are 3 more of the same kind that were missed: `LoadStep.Bound/Stream/Attach` throw in exec (`PlanRunner.java:703-712`). |
| EXEC-9 | S2 | CONFIRMED | S2 | A repo-wide grep (main, test, pct, tools; Java/TS/Python) finds no caller of `PlanRunner.update(Update,Source)`, `effect(Effect,Source)`, `columns` or `describe`. `ProgramRunner.java:525` calls the 3-argument overload. The only mentions are stale javadoc in `TestDataGenerator.java:43-45` and comments in guard tests. `describe` is reached only through `Statements::describe` in the dead `PlanRunner.describe`. `SeedKey(String)` is the only production `Record` key (`PlanRunner.java:596,719`). G1 already lists this as a known remnant (`GOALS_OF_RECORD_2026_11_07.md:38`). |
| EXEC-10 | S2 | CONFIRMED | S2 (low end) | 136-line descent parser; strings and scalars are re-parsed through `Json.parse(substring)` (`NonFiniteJson.java:114,128`). Its only product use is DuckDB (`DuckDbConnector.java:75` is the one `nonFiniteJson=true`). The drift surface is limited to structure and whitespace. It is real, but one stable file. |
| EXEC-11 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Counts re-checked (`new IllegalStateException` 146, NIE 29, IAE 30, DataError 20: same order as reported). The six direct `new DataError(e.getMessage(), e)` in Sessions (`:401,419,440,453,469,497`) and the false "the one class" javadoc (`RaisedErrors.java:153-155`) are verified. Correction: those six wrap failures of `getAutoCommit`, `setAutoCommit`, `rollback` and the product read, which never carry a raised envelope. Skipping the unwrap therefore loses nothing today; the defect is the inconsistency and the false claim. The cross-reference "(EXEC-17)" should read EXEC-18. The core point (one exception type for plan defects, refusals and bugs) stands. |
| EXEC-12 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Verified at `RunScope.java`: the shared `SCOPE` singleton (`:43`); `judging`/`callingBack` share `JUDGING` and one type; stack-frame filters hard-code `AnsiSqlRenderer.`, `Lowerer.` and `SpecCompiler.`; "four rules" (`:6`) is followed by five listed. The call-site counts match exactly (64/15/14/7/6). The double-close effect is worse than "disabled": at depth -1, `statement()` (`== 0`) passes outside any run, and a later run sits at depth 0, where `statement()` refuses. The alternative's last bullet ("treat the runtime wall as a debug assertion") reopens G4, which names `RunScope` as a run-time proof. It is out of bounds under the rubric; the rest of the alternative is sound. TeaVM forces the Throwable stack walk (javadoc `:29-31`), not the hard-coded names. |
| EXEC-13 | S2 | CONFIRMED | S2 | `shape == OBJECTS` branches at `PureV1Writer.java:216,243,282`; the "not reached" envelope lambda at `:198-200`; `ColumnType`'s three nullables are validated in its constructor; the envelope is built and discarded at `Output.java:83`; the Engine and Arrow arms are near-duplicates. |
| EXEC-14 | S2 | CONFIRMED | S2 | The five mechanisms in `SessionState` are verified. The `AtomicReference` sits in single-threaded `UserBeside` (`ProgramRunner.java:612`) and inside `synchronized` `Lazy` (`Sessions.java:191-199`). `locked()` unwraps `View` only, while `closed()` unwraps both (`Sessions.java:262-268, 322-342`). Every outside caller of `state()` and its mutators is a test in package `com.legend.exec`. |

## Details

- **EXEC-3:** The `H2Connector.java:49` citation is `PRIVATE_IDS`, an `AtomicInteger` name counter, not a session
  store. The three `HandleStore`s (`PlanSessions.java:94,97,367`) and `SystemDatabase.CURRENT` (`:49`) are the real
  process-global caches. `cache/HandleStore.java:25-28` gives a reason for "no eviction". It is not a reason for having
  no owner that can close every handle at shutdown, so the finding stands at S1.
- **EXEC-5:** At `Sessions.java:412-416`, `inTransaction` returns early unless `ownsTransaction(session)` (autocommit
  on). The `setAutoCommit(true)` at `:437-441` therefore restores the original state. The remaining differences
  (isolation is set only in `inSnapshot`, and the commit-failure paths differ) still make it two copies of one
  protocol.
- **EXEC-8:** The proposed `writes()` derived from the steps is infeasible as sketched.
  - `PlanMaker.java:160-179` returns `ExecutionPlan.reading(..., new Sequence([Effect…, answer, Effect…]))`.
  - `OutputPlan.java:95-103` returns `ExecutionPlan.writing(..., new Sequence([Effect, Effect, Check/Tds]))`.
  - Both are the same step kinds, so the split needs a typed distinction between staging and store-changing Effects
    first.

  The `Answer`/`Step` split of the hierarchy is still right. Two cited sites do not belong to this finding:
  `JdbcStatements.java:625-627` is EXEC-7's `Send` enum and `PureV1Writer.java:198-200` is EXEC-13's envelope.
  `PlanRunner.java:703-712` should be added: three `LoadStep` kinds that exec can only refuse.
- **EXEC-11:** See the table. The substance (exception-type conflation, message-text tests) is unaffected.
- **EXEC-12:** Keep the fresh-token, phase-stack and caller-supplied-filter parts of the alternative. Drop "debug
  assertion": G4 says separation "is proven at run time (`RunScope`)", and the rubric forbids reopening goals.

## S3 and strengths spot-checks

- **EXEC-16 (578 inline FQNs): confirmed.** Excluding comment, import and package lines and trailing `//` comments, I
  count 567. The per-file counts match exactly: JdbcDecode 66, Sessions 55, JdbcStatements 44. 125 of the FQNs are
  `@com.legend.base.Nullable`; nothing forces that, since the type-use form `ExecutionPlan.@Nullable Target` works
  with an import. `PlanRunner.java:646,659` use `com.legend.error.DataError` and `java.util.Objects` although both
  are imported at `:7,:28`, as stated.
- **EXEC-15 (327 dates): confirmed.** I count 324 over 11,055 lines of exec, result, the three doors and RunScope; the
  reviewer's file set is slightly larger. The stale comments check out: `PlanRunner.java:35-37` claims a version check
  and type writing, neither present (`Sessions.java:385-391` checks the product only); `Statements.java:42` holds an
  orphan javadoc; `RunScope.java:6` says "four rules"; `SessionState.java:11` refers to "a body driver".
- **EXEC-18 counts: exact.** 47 `assertTrue(…contains(…))` in the exec tests, 32 `new ExecutionPlan.Target(`, and 9
  `Planted*.java` files in the exec tests.
- **Strength "connector abstraction" (substance right, citations wrong).** `Connectors.java` has 129 lines, so
  `:185-206` and `:249-291` do not exist. The exhaustive enum switch with no `default` is at `Connectors.java:22-45`,
  and the unbuilt-connector refusal object is at about `:81-128`.
- **Strength "one JDBC send site": confirmed.** `JdbcStatements.send` (`:596-629`) is the only
  `createStatement`/`prepareStatement`, all in try-with-resources. **Strength "thin doors": confirmed.** Each
  `Execution.run*` is one `executionPlan`/`projectPlan` call plus one `ProductRun` call. Every door also takes both
  `SessionSource` and `PlanSessions.Source`, which supports EXEC-3.

## Missed

1. **Exec itself round-trips the typed form through the string (EXEC-1's strongest evidence).**
   `WireTypes.column` (`WireTypes.java:91-93`) takes the `SqlType` that JDBC metadata gave (`JdbcStatements.java:306`).
   It stringifies it with `ColumnFacts.slot`, then maps it back with an upper-case, strip-parens string switch in
   `pureTypeNameOrNull` (`:66-80`).
2. **Ladder arms that no Java producer writes (suspected dead).** The slot-word ladders accept `SMALLINT`, `TINYINT`,
   `FLOAT`, `REAL`, `NUMERIC`, `CHAR` and `TEXT` (`JdbcDecode.java:139-142`, `RowDecode.java:127,465`,
   `MongoDbDecode.java:115`, `ArrowWriter.java:128-140`). `SqlType.Scalar` has none of these names, and
   `ColumnFacts.slot` writes `Scalar.name()`. `PlanJson.java:1192` reads `slot` unvalidated, so these arms are
   reachable only from a foreign-host plan, if ever. The string form lets the ladders grow arms nobody can check.
3. **More impossible arms for EXEC-8.** `PlanRunner.deliver`'s `LoadStep.Bound/Stream/Attach` arms
   (`PlanRunner.java:703-712`) are load kinds that exec never runs but the plan type admits.
4. **A third counting path (EXEC-6).** `JdbcStatements.readsFile` increments `Census SQL_ROUND_TRIPS` itself
   (`:317`), beside the runner's `StatementOrigin.sent`.
