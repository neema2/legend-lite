# B3 step 6: the SQL-text fallback as test tooling -- design (read-only, 2026-11-25, on 50b33970)

Inputs: the map (`docs/records/compat-b3-homework/step6-sql-text-map.md`); the plan of record §B3, Q16b, Q16c (`docs/records/compat-stage-b-plan-2026-11-20.md` on `claude/compat-program`); H13 §27-28 (the program log at eafc8272:3279-3367); step 2b's lift (`edge/UpstreamPrints`, `com.legend.test.Audience`).

## 0. In one paragraph
The edge's legend-engine door does two things before planning. It rewrites the dual golden (Q16c). It then LIFTS each top-level SQL-text assert out of the body, leaving a marker assert in its place. The body runs as lite, with no compatibility code in its run. After the body, on the still-open session and before the attempt is committed or rolled back, the corpus harness decides each lifted assert:
- our text is read through lite's public test door;
- where the text differs, our rows are read the same way and the referee replays the golden on its H2 2.1.214 mirror;
- the verdict goes into the marker's slot, with first-failure sequencing.

The harness seeds the mirror from plans it asks the public door for, never from a product hook. The product core (`testplan`, `testexec`, `exec`, `planner`) ends holding nothing of SQL text, appeals or the referee.

## 1. Target architecture

### 1.1 Who does what
| Piece | Home | Does |
|---|---|---|
| `PureTestRunner` public halves | core `com.legend.test` (product-neutral; ruled in the plan of record §B3, "split into two public halves") | `prepare(t, audience)` returns `Prepared` (the admitted typed body, its facts, the session, the options, `setupsRun()`) — today's `run()` from line 273 through `runSetups`. `run(prepared, body, listener)` returns `Ran` (the verdict log, the failure, the plan) and never closes the session — today's `judge()` from line 331 minus scoring. `result(prepared, verdicts, failure)` returns `Result` (today's PASS/FAIL/SKIPPED rules). `finish(prepared)`. `run(t)` composes the four, as today. |
| `edge/UpstreamTestBodies.admittedForEngine` | core edge (the door) | Q16c's rewrite (§3), then today's S137, S144 and order rewrites. legend-engine's entry, beside legend-pure's (the progress record's "two door entries"). |
| `edge/UpstreamSqlTexts.lift` (new, ~250 lines) | core edge | Finds the SQL-text asserts among the top-level statements and replaces each with a call to the caller's marker. Returns `Lifted(body, List<LiftedAssert>)`: typed data only, no reads. |
| `harness/SqlTextChecks` (new) | spec | After the body: the reads, the referee, text-then-rows, the refusals, the reports (§1.3). |
| `harness/LiftedVerdicts` (new) | spec | Splices the decisions into the product's verdict log; first-failure sequencing. |
| `harness/MirrorSeeds` (new) | spec | The mirror's ledger from referee-planned programs (§2). |
| `ReplayReferee`, `RefereeVerify`, `PlanReplay` | spec (existing) | Comparison unchanged. Take `SqlReplayReferee`, `ReplayGates`, `RawLedger` and `SqlTextReferee`'s strip and bindings from the edge. The static `MIRROR` becomes an instance, so slice 1 can run two. |
| `MinimalCorpus` | spec | Composes: prepare, admittedForEngine, lift, `Audience.read` (2b's prints), run, SqlTextChecks, LiftedVerdicts, result, commit or rollback, finish. The attempt bracket (MinimalCorpus.java:639-673) moves from the observer hooks into this sequence. |

### 1.2 The lift (the door)
**What lifts.** A top-level statement, outside any lambda (UpstreamPrints' scope rule), that is one of:
- `assertSameSQL` (both overloads);
- `assertEquals` whose side reaches an upstream text producer (`EdgeEngineTestText.reachesUpstreamText`'s rule, through lets and helpers; `assertNotEquals` was never an arm: `tryArm` returns null for it);
- a rewritten dual golden.

Foreign-dialect ones lift too, so the census keeps its `foreign-dialect:*` counts.

**The record.** `LiftedAssert(k, statementIndex, listenerName, golden, message, producer, lets, followedByWrite)`:
- `listenerName` is the call as spelled (`assertSameSQL`, `assertEqualsH2Compatible`, `assertEquals`), so ledger names are unchanged;
- `producer` is one of: ToSql (the lambda, the mapping, the DatabaseType or runtime expression, nonExecutable); Frame (the `execute(...)` behind `$r`); SqlRead (frame, statement n); PlanText (the plan producer, its lambda, mapping, runtime);
- `followedByWrite` = `facts.shape()` holds an `E` after `statementIndex`.

**The producer's query.** `SqlTextInputs.of(producer, lets)` stays in the edge (pure/v1's `EnginePlans` shares it). It gives the let-chased lambda, the mapping and the dialect or runtime. The harness builds the reads as typed calls (`EngineSubstitutes.call` over typed args, as `rowsOrderFree` does), never re-parsed:
- **Rows:**
  - the general read is `from(<lambda's last statement>, <mapping>, <runtime>)`, with the lambda's leading lets before it and the body's lets inlined (`UpstreamPrints.inlined` / `readBody`);
  - the runtime is the producer's runtime expression (R1's post-processor form) or the lane's `rcorpus::Rt`;
  - an inline `^Mapping` reads the lambda's last statement as written (it binds its own mapping and runtime: H13 §28.4);
  - a lambda with statement lets reads let 0's value (SqlTextVerdicts:541-560);
  - `toNonExecutableSQLString` reads `->take(0)` (zero rows, computed by the database);
  - the Frame forms read `$r.values` with each `execute(...)` bound to a let of its own (`readBody`).
- **Text:** the producer side itself (`toSQLString(...)`, `sqlRemoveFormatting($r)`, `sql($r, n)`, the plan navigation), read through the same `readBody`.

**The marker.** The harness adds `rcorpus::lifted::sqlText():Boolean[1] { assert(true) }` to the corpus model, as it adds `rcorpus::Rt` (MinimalCorpus.java:302-307), and passes its FQN to the door. The product plans it as any assert helper: `BodyDriver.helperAssertRoot` folds it to `assert(true)`, and it is reported under its own FQN (`AssertVerdicts.listenerName`, :318). A lifted statement is top-level, so it runs at most once and in order: the j-th marker verdict is the j-th lifted assert. The body's facts are unchanged (shape letter A), and a body whose only assert is lifted is never SKIPPED.

### 1.3 After the body (the harness), per marker that ran, in body order
1. **Golden.** A literal gives its text; anything else is read through the door. `stripChainedPlanWarning` is applied.
2. **Our text.** Read through `Audience.Reads` (`TestExecution.planBody` + `runBody` with `Output.Rows`). A raise fails the verdict with its words at its slot, as today's "raised at the assert's turn".
3. **Equal text:** PASS. A foreign dialect reports `foreign-dialect:X` first and then holds by text alone, failing in today's words (SqlTextRuns.java:101-102).
4. **Refusals, by name, in today's words:**
   - `followedByWrite`, when rows are needed (Appeals.java:189-195);
   - the run failed and the body wrote (:148-155). "Wrote" is any `E` statement before the first lifted statement that did not run (conservative, measured);
   - the caller's message: no longer refused. The harness reads the message expression after the body, which fixes the held appeal `testToSqlGenerationFirstDayOfWeek`.
5. **Rows.** Our rows (plus the population read for a paged chain) become a `TypedSide` through a harness sink. Facts:
   - `ReplayGates.of(query, lets)`, which supplies extentSubset for `testQualifierQueryWithOr`;
   - the in-list seed;
   - the statement route and population shape from the golden text. The golden is known before the route is chosen, which is better than today's refusal (SqlTextRuns.java:62-69).
   Then the mirror is brought up to date (§2) and the referee runs `verify` or `verifyPlan`:
   - MATCH: pass;
   - DIVERGED or FAULT: fail;
   - DECLINED: `oracle-declined`, and the text decides;
   - a read failure: `rows-underivable`, and the text decides (SqlTextRuns.java:139-146).
6. **Splice.** The first lifted failure at marker slot s that precedes the run's own failure (its first failed verdict, or its exception after k verdicts) wins: the log is `verdicts[0..s)` plus the lifted failure, and the reason is `assert #(s+1) <listenerName>: <detail>`. Otherwise each marker is replaced by its decision and the run's outcome stands. This is Appeals.decide/failed (:181-262) without a hook.
7. **Reports.** Each decision's `declinedReason` and `refereeOutcome` ride in the harness's own annotation beside the product's `Verdict`. `textDecided` (MinimalCorpus.java:688) and `refereeMatched` are fed from them, so `pinChannels`, the DIFFERENTIAL floors (990 DuckDB, 944 H2) and the text-decided ceilings read the same numbers. `JudgeLedger.record` is unchanged: it reads only name, pass, unjudged and reason.
8. **Close.** Commit or roll back the attempt; MirrorSeeds advances the session ledger for a committed body; finish.

## 2. The referee's seeds (the biggest risk)
**What the mirror needs.** Today TestDoor.java:140-155 plans the copies, TestRunner.java:434-474 reports them and CompatEdge records them. The mirror needs:
- every setup's data: BeforePackage and shared fixtures (`executeInDb`, the DDL effects, `loadCsvToDbTable`, `from()` test-data seeds);
- committed effectful bodies on the shared session;
- the current body's seeds and effects: 48 DuckDB appealing bodies write before their asserts (Appeals.java:30-31), and inline-CSV bodies run on private workspaces.

**Proposal: plan each data-changing program again for the referee's H2, through the public door, and read the plan's data.**
- **Referee context:** `ctx.withExecutionOverlay(rcorpus::Rt, rcorpus::Conn typed H2)`, a view over the already-built module (MinimalCorpus.java:302-307). No second compile.
- **For each program** — each setup in `Prepared.setupsRun()`, each committed body whose lane plan has a Load or Effect step, and the current body:
  1. `Compiler.queryResolved(refereeCtx, resolved)`, then `admittedForEngine`, then `TestExecution.planBody(typed, RUNTIME, options in UPSTREAM style)`;
  2. walk `TestPlan.steps` for `Run(Load)` and `Run(Effect)` in order: a `LoadStep.Statement`'s SQL; a `Statements` blob decoded from `Load.content()`; a `File`'s `Delivery.Reads` statements over the bytes from `DataStores`, digest checked (CompatEdge.java:69-112 moved verbatim); an `Effect`'s `Script.statements` with queries dropped (`RawSql.isSingleQuery`);
  3. those become `RawLedger` entries (RawLedger moves to spec), and `ReplayReferee` replays them as today. Built lazily, at a session's first rows decision.
- **Why it matches today:** today's copies are lite's H2 dialect too (`Databases.dialect(REPLAY_REFEREE)`; EffectPlanner.java:59-63, TestKinds.java:390-393, CsvSeed.java:142-170). Slice 1 checks the three possible differences:
  - the edge's H2 dialect against the product's;
  - `Step.Sql.runOn` translating an author's H2 text, where today's copy runs it as written;
  - a load the lane skipped as held clean and the mirror replays: harmless if a load is Clear plus Rows, which is to be confirmed.
- **Cost:**
  - No new public API, except `Prepared.setupsRun()` (data, product-neutral).
  - Planning: one re-typing and one `planBody` per data-changing program, in sessions that need a replay.
  - The mirror receives the same statements as today.
  - About 200 harness lines replace: CompatEdge, RefereeLoad, EffectTexts, the run's `seeded`/`effectRan` hooks, and the planner's copies (TestKinds' seeds map, `EffectPlanner.recorded`/`refereeDdl`, `CsvSeed.Planned.referee`, LoadPlanner.seeds' referee parameter, `Databases.REPLAY_REFEREE`, which moves to TestBackends).
- **Residue:** a body that changes data but does not plan for H2 (a query H2's dialect cannot lower) leaves its seeds unknown. That session's later rows decisions decline by name (a new counted reason, "referee seeds unplannable"). Fallback, if slice 1 counts more than a handful: the door's data-only projection of a body (its lets, its `E` statements, each test-data `from()` as `->take(0)`), still through the public door.
- **Rejected:**
  - copying the lane's tables into the mirror: identifier case, schemas, and DuckDB-to-H2-2.1 type mapping lose fidelity;
  - running the lane's own plan on the mirror: its text is DuckDB's;
  - keeping the planner's copies: compatibility logic stays in the planner — only an interim if slice 1's ledger diff fails.

## 3. Q16c: the dual golden at the edge door
- **The rewrite.** `assertEqualsH2Compatible(legacy, upgraded, actual)` becomes `assertEquals(upgraded, actual)` everywhere in a corpus body, typed by `EngineSubstitutes.call` (as `rowsOrderFree`), on legend-engine's door entry only. The lift then sees an equality-family SQL-text assert, and its record keeps the helper's name.
- **Deleted from the product:** the AssertVerdicts.java:836-851 rewrite; `gateOf`'s branch (:180-190); `VerdictQueries.assertEqualsOf`; `PlatformTypes.ASSERT_EQUALS_DUAL_GOLDEN` and `ASSERT_SAME_SQL` (move to the edge).
- **Register row:** the named harness rule (Q16c = A, ruled 2026-11-20 ~19:45).
- **A name moves:** an arm-declined dual golden that is not lifted (DE5's four corpus bodies) reports `assertEquals` instead of the helper's name (Q-F).

## 4. Slices (each ends with one green round)
**S1 — beside the old; no verdict can move.**
- Builds:
  - the `PureTestRunner` halves (a refactor, `run(t)` unchanged);
  - MirrorSeeds, with a per-test diff of its ledger against today's `recorder.seeds()`;
  - `UpstreamSqlTexts.lift` dry (records only, body unchanged);
  - SqlTextChecks deciding every lifted assert on a second mirror; the result goes to a TSV only.
- Measures:
  - per assert, today's (verdict, refereed, declined) against the harness's, both corpora;
  - the seed-ledger diff;
  - the residue placed by name (§5);
  - R1 read through the test door;
  - cost (`SQL_ROUND_TRIPS`, wall time).
- Gate: MinimalCorpusTest, the edge net and PCT all byte-identical; agreement 100% except a named list for the user.

**S2 — the switch.**
- MinimalCorpus composes through the halves.
- The door does Q16c and the lift with markers.
- No `ExecuteOptions.recording` and no CompatEdge; the corpus run hook becomes `AppealRun.with(null)`, which still supplies `hostForms` (the error wording).
- `PureText.UPSTREAM.judgesEngineText()` becomes false.
- The arm's unit tests are re-pointed to the harness's tests.
- Measures:
  - fail, skipped and accepted rosters byte-identical on both lanes;
  - `PLATFORM_STRENGTH` {990,22,27} and `REFEREE_STRENGTH` {944,22,27};
  - the text-decided and leniency ceilings (shrink only); referee faults at 0;
  - the corpus edge-net ledgers identical except one ruled re-record (Q-F);
  - PCT channel B and `core_edge_tests`;
  - lean SQL: re-pin, dated, if it counts the harness's reads.

**S3 — delete the planning side.**
- Deletes: SqlTextVerdicts; EngineTextPlanning, EngineTextGates; EngineTestText and its `PlanStyleFacets.KINDS` row; TextCollector, TextAsk, TextPlanned, NumberedExtensions; EdgeEngineTestText's facet (its constants go to SqlTextInputs); AssertVerdicts' routing and `holdsEdgeJudged`; HostAsserts:231, DatabaseAsserts:312, TestPlanner:130-215, JudgePlans:163; ExecEnv's `engineText`/`appealOn`/`appeals`; `PlanStyle.judgesEngineText`; TestDoor:144-148 and the planner's referee copies (§2).
- Re-pins:
  - ArchitectureTest: the ~38 PLANNER_ENTRIES rows at :1175-1263, :2289, :2336, :5054-5069, :5147;
  - JavaEvalLedgerTest EVICT_SIZE (AssertVerdicts 1329 down; EngineTextPlanning, SqlTextVerdicts, SqlTextRuns gone);
  - HookRulingTest, HookLambdasTest, HookRulingsAgreeTest;
  - DialectBoundaryTest (`Databases.java` 2 → 1);
  - GuardNamesExistTest:67.
- Measure: byte-identical to S2, plus the core line count.

**S4 — delete the run side.**
- Deletes:
  - edge: Appeals, SqlTextRuns, Appeal, AppealJson, PlannedAppeals, EngineTextJudgment, NotAConstant, CompatEdge, RefereeLoad, EffectTexts; EdgeCodec's appeal arms;
  - judgment: `HostJudgment.EdgeJudged` and its HostJson arms; `TestPlan.extensionsByVerdict`/`extensionsByStep`; `RootLoop.NO_APPEAL`; `testexecutionplan.Edge`/`NoEdge`/`Extension`;
  - exec: `ProgramSink.seeded`/`effectRan`; `StatementOrigin.REFEREE_*` and `MIRROR_SEED` (a harness census); `ExecuteOptions.recording`/`edge()`;
  - testexec: `BodyRun.judged`/`writing`, HostJudgments:310, AssertRuns:44; EdgeHearing keeps `hostForms` only;
  - listener and runner: `AssertListener.declined`/`refereed`, with IsolatedRun's and PureTestRunner's forwards and `PureTestRunner.Verdict`'s two fields.
- Re-pins: `theListenerIsOutputOnly` (:2866, :2893-2897), the Appeals.reader row (:4833-4836); RefereeAppealsCensusTest deleted whole.
- Registers: S380, the differences page.
- Measure: byte-identical to S2.

**S5 — with B2b.** `HostForms.caught` moves to B2's after-run formatting, then `EdgeRun`, `EdgeHearing`, `NoEdgeRun` and `Audience.edgeRun` are deleted whole. R1's product-door fix (`Execution.run` applies `runtimeWithTableReplace` or refuses it; PlanMaker ignores it today) lands as its own slice at any time; step 6 does not depend on it, because the harness reads through the test door, where BodyPlanner.java:245-258 merges a `from()`'s tableReplace. The `assertError`-catches-size-refusals item (Q4) is separate.

## 5. The residue (H2's 67; H13 named the 20 and the 32 only by count)
| Group | What it is (read from the arms) | Placement |
|---|---|---|
| 15 plan-text | golden = a plan's text (`tryArmPlanText` :1055); replayed by `verifyPlan`/PlanReplay with the referee's bindings | **rebuild**: the record carries the plan text and bindings; rows via `from(plan lambda)`; `plan-params-unbindable` kept |
| 4 post-processor runtimes | `toSQLStringPretty(f, m, runtimeWithTableReplace(..), ext)` | **rebuild** through the test door (BodyPlanner merges the renames); measured in S1; R1 is separate |
| 3 inline `^Mapping` | the producer's mapping is a placeholder; the lambda binds its own | **rebuild**: read the lambda's statement as written (H13: 3 MATCH) |
| 1 `testQualifierQueryWithOr` | needs the extentSubset fact | **rebuild** (ReplayGates moves) |
| 20 other shapes | routes the probe carried no facts for: statement-per-let (`sql($r, n)`, readK > 0, `statementRoute` :918); statement-let lambdas (:541); population and in-list temps (:963, :1263); page membership; `toNonExecutableSQLString` (`legContext` :574); `assertSameSQL`'s String overload (:612) | **rebuild** each: static facts of the typed tree plus the golden's text, which the harness holds; any read the door cannot make is refused by name and counted |
| 32 unaligned | asserts not at the top level, whose index the probe could not pair: in map/forAll lambdas (`testToSqlGenerationFirstDayOfWeek`'s literal map), planned branches, loops over drivers (dialect "unresolved", a CONTRACT today), helpers asserting twice | literal-collection map/forAll: **door unrolls** (`compiler.spec.LiteralUnroll`, the planner's own rewrite) **and lifts**; CONTRACT routes: **stay in the body** as lite's text assert (same verdict; words and decline counts move, Q-F); any other that is ROWS today: **refuse by name**, listed for the user (Q-B) |

## 6. Risks and open questions
**Risks:**
- **R1 seeds (§2):** the S1 ledger diff is the gate; the interim fallback keeps the planner's copies.
- **R2 our text:** a producer that the program road does not plan as a body's answer (`toSQLString` or a plan navigation through the door) is named in S1 and becomes a refusal by name.
- **R3 class results:** an object answer read into `TypedSide.Graph` may lose a type. If so, a typed objects receiver beside `Output.Rows` (public, product-neutral).
- **R4 cost:** one more read per lifted assert (the text) plus lazy re-planning. Today the appeal already re-reads the rows.
- **R5:** "wrote" for a failed body is static and conservative (the dynamic `writing()` hook goes); measured.
- **R6:** spec reading `compiler.spec.typed` trees: check spec's BUILD dependencies. Otherwise the facts stay in `edge/UpstreamSqlTexts`.

**Open questions (recommendation first):**
- **Q-A** Seed the mirror by planning again for the referee, accepting a named residue for bodies that do not plan on H2, rather than keeping the copies in the planner: **re-plan**.
- **Q-B** Non-top-level SQL-text asserts: **unroll literal maps and lift them; keep CONTRACT ones as lite's text assert; refuse the rest by name.**
- **Q-C** The verdict slot from a harness marker function in the corpus model, versus a statement index on the product's verdict events: **the marker** (no product change).
- **Q-D** Confirm the public runner halves for step 6 (2b's review found them unneeded under substitution; step 6 needs an after-body point): **yes**.
- **Q-E** `HostForms.caught` moves to B2 so the run hook goes whole: **yes, after B2b**.
- **Q-F** One ruled re-record at S2 (the dual-golden names, the CONTRACT words, the held appeal's ledger row returning), no verdict moving: **yes**.

### Critical files for implementation
- /home/user/s2/core/src/main/java/com/legend/edge/SqlTextVerdicts.java
- /home/user/s2/core/src/main/java/com/legend/edge/Appeals.java
- /home/user/s2/core/src/main/java/com/legend/test/PureTestRunner.java
- /home/user/s2/spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java
- /home/user/s2/spec/src/test/java/com/legend/harness/ReplayReferee.java
- /home/user/s2/core/src/main/java/com/legend/edge/CompatEdge.java and /home/user/s2/core/src/main/java/com/legend/testplan/TestKinds.java (the referee copies)
- /home/user/s2/core/src/main/java/com/legend/edge/UpstreamPrints.java (the model for the lift)
## 7. Input measured 2026-11-25: the step-2b print lift's SQL (B2's finding; probe patch scratchpad/lift-probe.patch)
DuckDB corpus, planning only, 2,420 bodies: unlifted 2,307 statements; lifted 2,307 (body, unchanged) + 568 (318 reads
in 228 bodies) = 2,875 (+25%). Of the 568: 174 repeat an earlier read's statement in the same body (each read's own
frame let is planned as an eager FrameRun, the product's M9 semantics, so every read re-sends the frame), 225 repeat a
statement the body sends anyway (the body still runs `let result = execute(...)` though its only reader was a print the
lift replaced), 169 are new (the sorted read itself; a `#TDS` golden read as a VALUES statement). Example,
mapping::relation::testDateTimeFilter: 1 statement unlifted -> body 1 + reads 3. Practical cost under a second (tiny
tables); not product SQL; the lean baseline measures unlifted bodies (B2's option A). Step 6 should fix it where it
reshapes the lift: read each frame once per body (one read body for all of a body's printed sides of one frame, or the
after-body read the harness makes), drop a let the lift made dead (never one the author left unused), and print a
`#TDS` golden's literal rows without a round trip if the tenets allow it (constants, edge formatting).
