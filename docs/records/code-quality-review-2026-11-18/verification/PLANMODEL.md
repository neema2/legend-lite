# PLANMODEL verification

## Verdict on the unit: agree (MIXED)

The type core is good: sealed roots, checks in constructors, deterministic maps, and a reflective guard that the plan
holds only data. The serialization layer and the comments carry the debt. After my corrections one S1 remains
(PLANMODEL-1). PLANMODEL-2 drops to S2, and PLANMODEL-8 and -9 drop to S3. That still describes "sound design, notable
quality debt", so MIXED stands. I would not move it to the weaker end of MIXED on the strength of PLANMODEL-2.

## Findings

| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| PLANMODEL-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Line counts are exact: 1,423 + 1,146 + 1,295 + 405 + 271 = 4,540. Checked the six key-sniffing walkers (PlanJson.java:300-442, 1005-1024), the enum lookup written four times (PlanJson:174, MirJson:772, ConnectionWire:56, TestPlanJson:57) plus the judge loop (TestPlanJson:233), the duplicate `store`/`columnType` codec (PlanJson:456-497 vs the public MirJson:748-768, 1138-1145), the duplicate multiplicity codec (HostJson:49-60), `Load.canonical` (ExecutionPlan:327-392, also re-checked by exec at PlanRunner.java:1200), and `Fields` with its one adapter (PlanJson:1385). Corrections: (a) ConnectionWire's tags are an enum matched by `named()`, with no `default ->`; only HostJson uses string literals with a `default` (HostJson:1180-1206). (b) The phrase "generated once from the record list and kept as source" is in the design doc that dc655af87 adds, not in its commit message. That doc also says the first codec was reflective and F1.11 refused it. (c) The plan-level `loads` section is part of the wire contract the TypeScript runner reads (engine-client/src/plan-runner.ts:145-171), so the kit must keep that shape or migrate the hosts. (d) Asymmetries are not caught only "if a round-trip test happens to cover it": every test body is round-tripped in production (TestPlanner.java:167). Constraint check: the hand-written form is forced by the reflection ban (ArchitectureTest.java:2670-2692, which also bans `java.lang.invoke`) and by TeaVM/GraalVM. A codec kit built from method references and lambdas does not touch that ban, and TeaVM already compiles lambdas in Boundary.java, so the kit is feasible. A build-time generator is also feasible: the build already has `java_plugin` and java_run actions. Size: large. |
| PLANMODEL-2 | S1 | OVERSTATED | S2 | The facts are right: six components, the field-by-field compare (ExecutionPlan:974-980), `bare()` (:989), the positional `new Target(...)` in `writeHeldDatabase` (PlanJson:725), and Keyed's same-target check. The S1 framing does not hold. By-value targets are decision A: equal targets share a database, and exec keys a session by `PlanJson.writeTarget` (PlanSessions.java:300, :449). They keep every node self-contained, which the test plan relies on (it reuses product nodes inside sides, verdicts and branches), and the TypeScript and Python hosts read each statement's `target`. "Adding a field means editing the sniffers' keys" is wrong: the sniffers key on `database`, `session` and `loads` only. `bare()` and `writeHeldDatabase` fail to compile until they are edited, so the only silent hazard is the hand-written compare, a one-line fix (`load.target().equals(bare())`). The id-table alternative touches about 100 `.target()` reads in the plan, planner, exec and testexec packages (36 in exec and testexec alone) plus both hosts' readers, and makes nodes depend on their context. Large cost for a moderate gain. |
| PLANMODEL-3 | S2 | CONFIRMED | S2 | 44 `.slot()`/`slotWord(` reads in exec, result and testexec, recounted. RowDecode parses slots with `inside(c,"ARRAY")` and `topLevel` (:373-500) and even rebuilds Columns from parsed substrings (:373). JdbcDecode:425-429 parses `DECIMAL(`. Exec itself turns a typed `SqlType` back into a string and then compares strings (PlanRunner.java:882, WireTypes.java:87). `CSVJOIN:` is built at AssertVerdicts:1981 and decoded at TdsCompare:204. The carrier switch is at HostJudgments:679-688. `Verdict.arm` is only copied and serialized; nothing in testexec reads it. No host forces the string: neither plan-runner.ts nor the Python runner reads `slot`. Moving to `SqlType` needs one layer-map line (`result` gains `:sql`). Size: medium. |
| PLANMODEL-4 | S2 | CONFIRMED | S2 | `Appeal.Read`'s guard (Appeal.java:129-136), `RootLoop` NO_VERDICT=-1 and NO_APPEAL=0 (:29, :32), the sentinel compare (PlannedAppeals:59-60), Failure's per-kind nullables (Failure.java:25-38), and the three `default -> "not reached"` arms (ExecutionPlan:52, :65, :89) are all as described. Test plans are Java-only, so no host constraint forces this shape. |
| PLANMODEL-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 (the highest-value S2) | No runtime reads the tree, in any host. Java: `.tree()` on a plan statement appears only in the writer (PlanJson:672), and there is no record deconstruction of `Sql`. TypeScript: plan-runner.ts's `Sql`/`Step` types never mention it. Python: `_planned.py:238` skips `'tree'` explicitly. wasm and native only write plans. The edge, testexec and server have no reader. The only consumer is the test gate PlanRoundTrip.java:99-126. The tree is nonetheless hashed at run: `ProgramRunner.java:180` puts nodes in a `HashSet`, and a record's hashCode walks the whole tree. Correction: "It makes `:execution_plan` depend on `:sql`" is wrong. ColumnFacts walks the MIR, and `Column.store` uses `SqlDdl.ColumnType`, so that dependency stays. "Lockstep with every MIR change" is real but modest: MirJson.java has 8 commits since 2026-11-01. It is also stronger than stated: see Details. |
| PLANMODEL-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The BUILD comment says "Data alone" (core/BUILD.bazel:81-84). Verdicts' statics are there, and `fail()` returns `TypedSide` but always throws (Verdicts.java:76-79). JudgeJson parses LITE text twice (:40-45) and carries an "edge's when the edge package exists" comment while `com.legend.edge` exists. `TestPlanJson` special-cases `instanceof Appeal`. Corrections: (a) `:error` is needed by the data too (`Failure.phase`). Only `:values` serves behaviour alone (Verdicts.java:202). (b) The move is bigger than proposed. Appeal and JudgeJson are read by both testplan and testexec (SqlReplayReferee, SqlTextRuns, RootLoops, AgreementRuns, HostJudgments). testexec cannot depend on `edge` (tools/deps/core-layers.txt:136). So moving Appeal means moving the referee behind the `Edge` hook as well. That is G6's "the referee only at the edge", a cross-unit job of medium to large size, not a record move. |
| PLANMODEL-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Confirmed: the answer logic is duplicated (TestPlan:96-121, :167 vs ExecutionPlan:47-112). The two Column records are identical, with a converter (SideShapes.java:190). AuthenticationSpec is byte-identical (diff from `public sealed` on: 0 lines). The round-trip guard is written twice. Corrections: (a) the two Constants are not the same record. Product: exactly one `value` column and one cell per row. Test: n≥1 columns plus `folded`. Only the cell-copy loop is shared. (b) Transaction, Effect.Script (a pre-bracketed script plus its abort) and Savepoint (set, rollbackTo, release) are three different SQL constructs. One shared record would be forced. (c) The faithful connection copy is a user ruling ("ruled 2026-11-08", docs/PLAN_AS_DATA_2026_11_07.md:136). A shared `:connection` library is compatible with its intent, but it reverses a ruled choice and needs the user. ConnectionSpecification is not identical either (68 diff lines: the model's form carries test data). (d) The two SHA-256s are not "in one package". `cache.Hash` is in `com.legend.cache`. `DataStores.sha256` hashes on-disk files on the JVM, where the JDK's intrinsic `MessageDigest` is a reasonable choice. |
| PLANMODEL-8 | S2 | OVERSTATED | S3 | The facts are right: `only()` at exactly the 10 sites listed, raw `valueOf` at the 7 PlanJson lines, 18 and 20 bare casts, and the stale "csv or rows" message (PlanJson:1039-1040). The G7 framing fails because the Java plan reader reads no foreign JSON in production. `PlanJson.read` has no production caller except the round-trip guard behind `-Dlegend.plan.roundtrip` (TypedQuery.java:249-251). Test plans are read only by `requireRoundTrip`, or under a diagnostics switch (TestPlanner.java:167-172), and always from their own writer's output. A misspelled key is therefore a writer/reader asymmetry, and the round trip on every test body catches it. The one untrusted input is `PlanJson.readColumns` over client-sent `resultColumns` (PureV1Api.java:603, :817). That is a small hardening item, and it belongs with PLANMODEL-1's kit. |
| PLANMODEL-9 | S2 | OVERSTATED | S3 | The facts are right: `VERSION = 51` with a changelog of about 120 lines (PlanJson:31-151), the history repeated in PlanJsonTest:185-212 and TestPlanJson:25-46, and PlanVersionsAgreeTest pinning only the TypeScript constant by regex. Corrections: Python does not "drift silently". `_runs.py:39/126` refuses any plan whose version differs from `PLAN_VERSION`, and the Python suite runs real native-planned plans (python/tests/test_engine.py:388-399, `compiler.snap_stream` → `run_host_load`), so a drift fails behaviourally, though without a static pin. Exact-match versioning is right for a monorepo where the planner and runner ship together, as the reviewer concedes. What is left is mostly comment practice (S3, already PLANMODEL-10) plus a one-test pin. |

Counts: CONFIRMED 2, CONFIRMED-WITH-CORRECTION 4, OVERSTATED 3, REFUTED 0.

## Details

**PLANMODEL-1.** The substance stands. Five codecs follow three tag conventions and three null conventions. An untyped
rewrite pass runs after the typed one: `hoist` and `inline` find a target as "an object with database, session and
loads", and `collectLoads` finds a load by "identity, cadence and steps". Those walkers run over a test plan's
extensions too (TestPlanJson.write puts `extensions` into the tree before `PlanJson.hoistLoads(out)`). HostJson is
generator-shaped: 239 `v_` tokens and 93 `new LinkedHashMap<>()`. The reviewer's constraint analysis is correct. The ban
is on runtime reflection (F1.11, the user's "no pardons"), and TeaVM and GraalVM need plain code. Neither rules out a kit
built from lambdas, or a generator in the build. Two framing fixes:
- The TypeScript runner reads the hoisted `loads` section as its contract, so hoisting is a wire requirement and not an
  invention of the walkers. The kit should keep that shape.
- The Java reader is mostly guard infrastructure (see PLANMODEL-8 and Missed), so most of the cost sits on the writer
  side, which the hosts consume.

S1 is defensible because this is the area with the most churn (70 and 56 commits since 2026-11-01, recounted). It sits at
the lower edge of S1: for each field, the writer half and the reader half are a line each.

**PLANMODEL-2.** Real smells: the Target → Load → Target back-pointer, the hand-written six-component compare, and
positional `new Target(db, List.of(), List.of(), null, settings, null)`. All are local. The by-value design is what
decision A and the session keys rely on, so I would not call it a wrong abstraction. The reviewer also says "the JSON
size cost" is suspected. The project measured where the bulk is (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md:232): the plan
is a median 4 KB without the tree and 27 KB with it. Per-statement targets are a small share. S2.

**PLANMODEL-5.** The finding is stronger than the reviewer wrote. The ratified design
(docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §1.2 P3 and the §1.3 table) says:
- "The tree travels only when asked … On the wire a plan carries the statement text; the tree is written only by the
  full rendering."
- There is a "Lite JSON, wire" renderer, "the record without its tree".
- The tree was measured at a median 5.5 times the rest of the plan, and up to 40 times (§8, line 232).

The code always writes it (PlanJson.java:672). So every plan the browser planner sends to the TypeScript runner carries
it (wasm Boundary.java:185, :231 and others). So does every `plan` the server answers with (PureV1Api.java:714, :770), and
the round trip that every test body makes in production (TestPlanner.java:167). Nothing reads it.

The design's stated uses do not need it on the wire. They were inspection, the round-trip gate and "the engine-protocol
projection", and the projection does not read the tree. So the alternative is sound: keep the tree on the planner's side,
or write it only on request. Size: medium. The `Sql` constructors and the PlanRoundTrip gate move to the planner side.
Removing MirJson also releases its pins in ParkedWorkLedgerTest, TestLaneOrderGuardrailTest, RawSqlLedgerTest and
CodeShapeGuardrailTest.

The one fact to drop is the `:sql` dependency. The stale javadoc (ExecutionPlan.java:713-714) and the misnamed test
`theTypedTreeIsNotWrittenYet` (PlanJsonTest.java:181) are verified.

**PLANMODEL-6.** The cohesion point stands, and so does the fact that the edge's extension point is bypassed by its main
tenant. The proposed fix understates the coupling. The product's test runner still executes the appeals and the referee
(testexec/SqlReplayReferee, SqlTextRuns, RootLoops, AgreementRuns), so their data has to sit in a library that both
testplan and testexec reach. `test_plan` is that library, and `edge` is not. Moving the records alone would break the
layering. The referee must move behind `Edge` with them.

**PLANMODEL-7.** It stays S2 on four items: the duplicated answer logic, the identical Column pair, the byte-identical
AuthenticationSpec and the copied round-trip guard. Two sub-claims (the identical Constants and the shared transaction
record) are overstated. The connection copy was the user's ruling: present the alternative as a question for the user,
not as a defect.

**PLANMODEL-8 and -9.** These are real hygiene issues, but the G7 consequence in -8 and the "silent drift" in -9 do not
happen in this repository as it runs. Both belong with S3, and -8's remedy is already part of PLANMODEL-1's kit.

## S3 and strengths spot-checks

- **PLANMODEL-10 counts.** 236 ISO dates and 68 `docs/*.md` paths across the two packages, recounted exactly. Per file I
  get PlanJson 65 and ExecutionPlan 60, against 64 and 59 reported, which is trivial. The stale items I checked are
  true:
  - the Sequence javadoc still names verdict groups (ExecutionPlan:223-224);
  - ConnectionSpecification says "three … flavors" and "Mirrors engine's com.gs.legend…" (:3-8), against ten variants;
  - ConnectionWire says "The model depends on no JSON" (:14-15);
  - plan-runner.ts:2 says "version 22".
- **PLANMODEL-13 counts.** I get 234 inline qualified names in code lines, against the reported 229, which is close.
  `@com.legend.base.Nullable` appears inline 99 times. The reported 110 also counts the 11 `import` lines.
- **PLANMODEL-11.** The forwarders at the nine listed lines are all one-line delegations, verified (the text says ten).
- **Strength: `PlannedSql`.** Partly overstated. The constructor is private, but `fromPlan(String)` is a public static
  that any class can call with any text (PlannedSql.java:34-36). What stops ad-hoc text is the ArchUnit rule
  `onlyThePlansReadersMintPlannedSql`, not the type. The type makes the mint explicit and greppable, which is good. It
  is not "a compile-time property".
- **Strength: `PlanTypesAreDataTest`.** Confirmed. It walks record components, permitted subclasses, collection
  arguments, enum fields and constant bodies, plus every `Extension` implementation. `PINNED` is empty.

## Missed

1. **Production-dead bundle path.** `PlanJson.writeBundle`, `read(String, Map)`, `Bundle`, `collectBundles`,
   `utf8Length` and `INLINE_LIMIT_BYTES` (PlanJson.java:222-361, about 110 lines) have no production caller in any
   module. The only callers are ContentBundleTest and ProjectRowsTest. Every host gets `PlanJson.write`, with content
   inline. The "blob beside the plan" delivery is a half-migration with no consumer.
2. **Java's product-plan reader is guard-only.** `PlanJson.read(String)` and the roughly 500-line reader under it run in
   production only behind `-Dlegend.plan.roundtrip`. The TypeScript and Python runners are the real readers of a
   product plan. That should shape any codec redesign: write once, and generate or verify the readers in the hosts.
3. **The tree's run-time cost** (an addition to PLANMODEL-5): the round trip on every test body (TestPlanner.java:167)
   writes, parses and deep-compares every statement's MIR, and exec hashes nodes in `HashSet<Node>`
   (ProgramRunner.java:180). Both cost CPU for data nobody reads.
