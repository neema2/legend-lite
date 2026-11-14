# M10b code map — the engine-text asserts (SqlTextVerdicts) as the code stands, 2026-11-01

Evidence for `M10B_BRIEF.md` (the test-is-the-product plan's M10b), written by a read-only survey at aec8b44b; the line
numbers are that commit's.

Read-only survey of `/home/user/legend-lite` at HEAD. Every claim cites `file:line` (line numbers verified by `grep -n` on the exact statement). Abbreviations:

| Abbrev | Path |
|---|---|
| STV | `core/src/main/java/com/legend/driver/SqlTextVerdicts.java` (1,720 lines) |
| AV | `core/src/main/java/com/legend/driver/AssertVerdicts.java` |
| BD | `core/src/main/java/com/legend/driver/BodyDriver.java` |
| BR | `core/src/main/java/com/legend/driver/BodyRouter.java` |
| JP | `core/src/main/java/com/legend/driver/JudgePlans.java` |
| SRR | `core/src/main/java/com/legend/judge/SqlReplayReferee.java` |
| VQ | `core/src/main/java/com/legend/compiler/spec/VerdictQueries.java` |
| EP | `core/src/main/java/com/legend/planner/EnginePlans.java` |
| STI | `core/src/main/java/com/legend/planner/SqlTextInputs.java` |
| RR | `spec/src/test/java/com/legend/harness/ReplayReferee.java` (the harness's referee) |
| RV | `spec/src/test/java/com/legend/harness/RefereeVerify.java` (the compare policy) |
| ENGINE | `/root/.cache/bazel/_bazel_root/e8817d619d2dbe75028d8b0114f08e2c/external/+http_archive+legend_engine_src/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational` |

Governing doc: `docs/SQLTEXT_ROW_VERDICT_CHARTER.md` (§0 "rows never text" :11, §4 foreign dialect :76, §5 plan replayer :88, §6 normalization :106, §7 order rule :174). Note up front: the charter's §0 and STV's class javadoc (STV:17-36) say "judges on ROWS, never on text", but the code since the user ruling of 2026-09-21 (option 1) makes a BYTE-EQUAL TEXT the verdict and the rows the APPEAL on a failed text only (STV:1394-1401, STV:1039-1042; see §2.1 below).

## How the arms are reached today (the walk)

- The walk's statement loop: `BodyCompiler.java:182-184` calls `AssertVerdicts.tryAdjudicate(bare, letPrefix, specs, BodyDriver.frameReplaceEnv(stmt, execFrames, seg.env(), letPrefix, specs), BodyDriver.spliceHook(execFrames, letPrefix, specs, seg.env()))`; a second call site for an inlined verdict root at BD:332-334.
- `tryAdjudicate` (AV:52-98): a verdict batch (database judge) → `batched` (AV:188-224); a listener → `CanonicalDivergence.sqlEnter()` + verdict reporting around `adjudicate`.
- `adjudicate` (AV:234-): under `ExecuteOptions.JudgeMode.HOST` it tries `planHost` first and runs it in place (AV:240-247); then the lineage/quantified forms; then the three ROOT arms by exact callee FQN: `assertSameSQL` → `SqlTextVerdicts.tryArmSameSql` (AV:300-307), `assertSqlEquals` → `tryArmTdgRoot` (AV:309-316), `assertEqualsH2Compatible` → `tryArmDualGolden` (AV:318-324; on null the rewrite `VerdictQueries.assertEqualsOf(args[1], args[2], specs)` is adjudicated as a plain assertEquals, AV:330-337); then the `NativeFn.Verdict` switch, whose `ASSERT_EQUALS/ASSERT_NOT_EQUALS` arm calls `SqlTextVerdicts.tryArm` FIRST (AV:370-384) and counts `CanonicalDivergence.sqlRoute(name, "sql-text")` when it answers (AV:382).
- Program-road refusal: `planHost` returns null for the three root FQNs (AV:702-706) and for any assertEquals/assertNotEquals where `SqlTextVerdicts.mayArm(name, wantEqual, args, letPrefix, specs)` is true (AV:747); `HostAsserts.planAssert` → null (`HostAsserts.java:160-163`) → `HostAsserts.plan` null → `BodyRouter`: `assertsPlanned && asserts == null` → `Census.STAYED_KINDS.add(BodyKinds.of(typed, runtimeFqn, mode.name().toLowerCase(Locale.ROOT) + "-unplanned-assert"))` and `BodyDriver.execute` (BR:89-96). The database judge refuses the same way: `DatabaseAsserts.plan` → `AssertVerdicts.planDatabase` → `databasePlansWithoutReading` → `planHost` null → `UNPLANNED` (AV:151-157; `DatabaseAsserts.java:168-172`). `BodyClass.ASSERT_PROGRAM`'s javadoc names "the engine-text arms" as the router's walk-kept rule (`core/src/main/java/com/legend/testplan/BodyClass.java:107-108`); a body containing a TDG generator is never an ASSERT_PROGRAM at all (`BodyClass.java:457`, `StatementEffects.containsTdgGenerator`).

---

## 1. The arms

### 1.1 Entry-point table

| Entry (STV line) | Visibility / caller | Assert shape claimed (Pure spelling) | Corpus example |
|---|---|---|---|
| `mayArm(name, wantEqual, args, letPrefix, specs)` :52-66 | package; AV:747 (planHost), transitively AV:154 | static SUPERSET: `wantEqual && args>=2` (:54) and on either of the first two sides `findProducer != null \|\| hasTdgProducer \|\| findSqlRead != null \|\| findPlanProducer != null \|\| (name=="assertEquals" && lookThroughPlanProducer != null)` (:57-63) | — |
| `tryArm(name, wantEqual, args, letPrefix, specs, env, hook)` :69-229 | package; AV:379 | `assertEquals(golden, toSQLString({\|q}, Mapping, DatabaseType.X, ext))`, `toSQLStringPretty(f, mapping, runtime, ext)`, `toNonExecutableSQLString(...)`, `toSQL(f, m, rt, ext).toSQLString(dbType, tz, quote, fmt)`; the producer may be let-bound. Exactly ONE side carries a producer; neither → `tryArmExecRead` (:83); both → null (:86-88) | ENGINE/../sybaseiq/.../testSybaseIQIsEmpty.pure:34 `assertEquals('select case when ...', toSQLString(\|TestClass.all()->project(...), TestMapping, DatabaseType.SybaseIQ, relationalExtensions()))`; let-bound: ENGINE/transform/fromPure/tests/testToSQLString.pure:76 |
| `tryArmSameSql(root, letPrefix, specs, env, hook)` :254-314 | package; AV:302 | `assertSameSQL(golden, $result)` (Result overload; engine testAssert.pure:18-21 is `assertSameSQL($sqlString, $result->sqlRemoveFormatting())`); the String overload `assertSameSQL(golden, <String>)` re-enters `tryArm("assertSameSQL", ...)` (:275) | ENGINE/calendarAggregation/tests/testCalendarFunctions.pure:826 `assertSameSQL('select "root".hireType ...', $result);` |
| `tryArmDualGolden(root, letPrefix, specs, env, hook)` :326-409 | package; AV:320 | `assertEqualsH2Compatible(legacy, upgraded, actual)` (engine h2Extension.pure:29-42 picks one golden by H2 version); actual = `$result` (Result), `$result->sqlRemoveFormatting()`/`(k)`, a plan-text navigation, or a TDG `$td.sqls->at(i)->sqlRemoveFormatting()` | ENGINE/tests/query/testQualifier.pure:68-71 (`$result->sqlRemoveFormatting()`); ENGINE/tests/query/datePeriods.pure:63-73 (`$result->sqlRemoveFormatting(0)` then `(1)`); javaPlatformBinding `executionPlanTest.pure:121-125` (`$res->planToString(...)`) |
| `dualGoldenVerdict(golden, ours, resultArg, readK, ...)` :414-454 | private; from tryArmDualGolden :361, :408 | the H2Compatible tail: statement routing + rows leg | — |
| `tryArmExecRead(name, args, letPrefix, specs, env, hook)` :471-600 | private; from tryArm :83 | `assertEquals(golden, $res->sqlRemoveFormatting())`, `sql($res)`, indexed `sqlRemoveFormatting($res, k)`; also the TDG and plan-text dispatch when no exec read is found | ENGINE/router/tests/testRouting.pure:40 `assertEquals('select "root".LASTNAME ...', $result->sqlRemoveFormatting());` |
| `tryArmPlanText(name, goldenSide, actualSide, ...)` :782-887 | private; from tryArmExecRead :509, tryArmDualGolden :368 | `assertEquals(golden, $plan.rootExecutionNode->...->cast(@SQLExecutionNode).sqlQuery)` where `$plan = executionPlan(lambda, mapping?, runtime?, ext)`; the producer may sit behind a user helper (look-through, assertEquals only) | ENGINE (xt-sql-expression) tests/testQuery.pure:25; ENGINE/executionPlan/tests/executionPlanTest.pure:1776 |
| `tryArmTdgRoot(root, letPrefix, specs, env, hook)` :978-997 | package; AV:311 | `assertSqlEquals(golden, $testData.sqls->at(n))` (engine testDataGeneration.pure:1772-1775: `assertEquals($s1->sqlRemoveFormatting(), $s2->sqlRemoveFormatting())`) | ENGINE/testDataGeneration/tests/testDataGeneration.pure:91-95 |
| `tryArmTdgSql(name, goldenSide, actualSide, ...)` :1005-1044 | private; from tryArmTdgRoot :991, tryArmExecRead :485, tryArmDualGolden :377 | fetch-text compare: actual side `hasTdgProducer` | testDataGeneration.pure:47 `assertEquals('select top 20 "root".ID ...', $testData.sqls->at(0));` |
| `tdgRowsNow(name, golden, ours, textEqual, referee, actualSide, ...)` :1047-1084 | private; from tryArmTdgSql :1036 (appeal), :1043 | the TDG rows leg | — |

### 1.2 Typed-tree gates (all BFS over `TypedSpec.children()`, let-aware through `Lets.binding(letPrefix, name)`)

| Finder | Detects | Lines |
|---|---|---|
| `findProducer` | a `TypedNativeCall` whose callee FQN is `PlatformTypes.TO_SQL_STRING`, `TO_SQL_STRING_PRETTY` or `TO_NON_EXECUTABLE_SQL_STRING` (→ `NativeDispatch.RoutineCall.of(nc)`, :1525), OR a native call whose callee is an implemented derived property (`NativeFn.JavaRoutine.ofDerived(fqn).isPresent()`: the `toSQL(...).toSQLString(...)` receiver form, :1532) | STV:1510-1546; FQNs `PlatformTypes.java:320-342` |
| `findSqlRead` | a `TypedUserCall` to `ResultEnvelopeSplice.SQL_FQN` (`meta::relational::mapping::sql`) or `SQL_REMOVE_FORMATTING_FQN` whose first arg is NOT String-typed, with 1 arg or 2 args where arg 2 is a `TypedCInteger >= 0` (:734-751) | STV:722-763; FQNs `ResultEnvelopeSplice.java:514-516` |
| `findPlanProducer` | a `TypedNativeCall` to `PlatformTypes.EXECUTION_PLAN` (:958-959) | STV:948-972 |
| `lookThrough` / `lookThroughPlanProducer` | `UserCallInliner(specs).inlineBody(letPrefix + side)`, then `findPlanProducer` on the reduced last statement with the reduced prefix as scope; any RuntimeException → null | STV:926-946 (record `LookThrough` :923) |
| `hasTdgProducer` | a `TypedTestDataGen` with flavor != "plan" (:1292); a `TypedNewInstance` of `meta::relational::testDataGeneration::TestDataGenResult` (the folded carrier; constant :1090-1091, test :1298); a native call to `PlatformTypes.GENERATE_TEST_DATA` (:1304) | STV:1284-1318 |
| `frameMappingAndClass(resultArg, letPrefix, hook, specs)` → `FrameFacts(mapping, cls, facts, populationRead, query, mappingRef, context)` (record :1558-1564) | `Lets.bound(resultArg)` (:1569), strip `TypedFrom`s; if not a native call and `hook != null`: `hook.apply(VerdictQueries.activitiesRead(resultArg), Set.of())` and take the spliced `TypedPropertyAccess.source()` (:1573-1585); if `NativeFn.Handle.isExecute(callee)` with ≥2 args (:1591): mapping = `args[1].fullPath()`, lambda = `Lets.bound(args[0])`, `cls = rootClassFqn(lam)`, `context = BodyPlanner.boundContext(args[2], letPrefix, specs)` (≥3 args), `facts = AssertVerdicts.replayFacts(chain, letPrefix)`, `populationRead = AssertVerdicts.unpagedRead(VerdictQueries.fromWrapped(chain, mappingRef, context))` (:1592-1614); otherwise all-null facts (:1617-1618) | STV:1566-1619 |
| `readIndex(read)` | `sql($res, n)` → n, else 0 | STV:622-627 |
| `statementLets(query)` | the lambda's leading `TypedLet`s whose value type is `ClassType` or `RelationType` ("statements of the engine's plan") | STV:664-680 |
| `statementRoute(k, golden, fm)` → `StatementRoute(let \| null)` (record :634-636) | golden matches `tempTableForIn_<ident>` → main (:641); `k < #lets`: `k==0 && isPopulationGolden` → main, else `lets.get(k)`; `k == #lets` → main; else null (:650) | STV:638-651; `NAMED_IN_TEMP` :682-683 |
| `isPopulationGolden(golden)` | lower-cased golden starts with `select distinct` | STV:655-658 |
| `populationShape(golden, query)` → `PopulationShape(rowsRead, temps)` (record :602) | a many-valued leading let named by `tempTableForIn_<var>` → `TempTable("tempTableForIn_"+var, "population:integer"/"population:string", [])` (:705); else the first many-valued let with a population golden → `rowsRead = let.value()` (:711) | STV:690-718 |
| `inListTemps(golden, query, letPrefix, zone)` | exactly one `tempTableForIn_<digits>` name in the golden (:1186) AND exactly one `in(x, [literals])` (`Pure.IN__ANY_1__ANY_MANY`, :1200) collection in query+lets (:1209), literal kinds date/datetime/string/integer (dates spelled in the frame zone via `LiteralSpelling.isoTimestamp/inZone`; an unwitnessed kind → empty, :1232) → one `TempTable(name, kind, values)` (:1241) | STV:1178-1244; `NUMBERED_IN_TEMP` :1158-1159 |
| `frameZone(read)` | `ExecutionContext.froms(read).get(0).context().timeZone()` or null | STV:1173-1176 |
| `tdgHop(actualSide, letPrefix)` → `TdgHop(index, TypedTestDataGen)` (record :1095) | unwrap `sqlRemoveFormatting(x)`; require `at(receiver, TypedCInteger i)` (`Pure.AT__T_MANY__INTEGER_1`, :1118); the carrier let is a `TestDataGenResult` instance whose `sqls` equals the receiver or whose name equals `$v` in `$v.sqls` (:1140-1145), or a let whose value contains a non-plan generator (`generatorIn`, :1149-1151) | STV:1105-1156; `generatorIn` :1261-1272 |
| `stripChainedPlanWarning` | drops `"\nWarning: Results only shown for first relational query."` and everything after (engine toSQLString.pure:156,166) | STV:611-617 |
| `planReplaySql(golden)` | a bare `select`/`with`/`(select` golden is itself; a plan text with exactly ONE `sql = ... connection =` node yields that SQL; several/none → null | STV:893-910, `PLAN_SQL_NODE` :912-916 |
| `legContext(producer, env)` | `env.frame()` or `ExecutionContext.NONE`; for `TO_NON_EXECUTABLE_SQL_STRING` (:237) → `withPostProcessors(...withNonExecutable(true))` | STV:232-243 |
| `scalarString(TypedSide)` | `TypedSide.Scalar` holding a String, else null | STV:1629-1633 |
| `fetchTranscript(TestDataGenerator.Result)` | the generator's fetches as `SqlReplayReferee.FetchTranscript(sqls, hops)` | STV:1247-1258 |

### 1.3 Per-arm: inputs, evaluation, verdicts

**`tryArm` (toSQLString producer)** STV:69-229
- Inputs: `SqlTextInputs.of(producer, letPrefix)` (:98) → query/mapping/dialect/runtime; lambda chased via `Lets.bound(in.query(), letPrefix)` (:99-101); requires a `TypedLambda` with non-empty body and a `TypedPackageableRef` mapping else null (:102-107).
- Dialect: `TypedEnumValue` → `db.value()` (:114-115); else runtime = `Lets.bound(in.runtime())` inlined by `UserCallInliner(specs).inlineBody` (:117-119) and read by `ExecutionContext.reader().bind(v -> Lets.bound(v, letPrefix)).read(Optional.empty(), rt)` (:120-123); `databaseType() == null` → `dbType = "unresolved"` (:132); then `env = env.withFrame(frameCtx)` (:143) and `env.withTableReplace(env.postProcessors().tableReplace())` when non-empty (:144-147).
- Evaluates (under `StatementOrigin.REFEREE_OURS`, :152): `golden = scalarString(JudgePlans.evalSide(goldenSide, letPrefix, specs, env, false, true, hook))` (:153), `ours = scalarString(JudgePlans.evalSide(producerSide, ...))` (:155); either null → return null (:157-159). Our TEXT is the K-routine `EnginePlans.toSqlString` folded at plan time: `EnginePlans.nativeRoutines(specs, env)` is handed to the stager at BD:307 and `BodyPlanner.java:247`, keyed by `NativeFn.JavaRoutine.TO_SQL_STRING/_PRETTY/TO_NON_EXECUTABLE_SQL_STRING` (EP:212-234).
- `golden = stripChainedPlanWarning(golden)` (:161); `textEqual = golden.equals(ours)` (:165). `env.replayReferee() == null` → `NotImplementedException` wall (:166-172).
- `dbType != Databases.REPLAY_REFEREE.name()` (:174; = `H2`, `Databases.java:31`) → `declined(env, name, "foreign-dialect:"+dbType)` (:177) + `textVerdict` (text decides, :178-181).
- Rows leg: `lamPrefix = letPrefix + lambda's leading statements` (:189-190); `stmtLets = statementLets(lam)` (:191); `legCtx = legContext(producer, envF)` (:198). If `stmtLets` non-empty and the golden is not a population (:199) → rows over `let0.value()` wrapped `VerdictQueries.fromWrapped(let0.value(), mapping, legCtx)`, classFqn = let0's ClassType, `replayFacts(let0.value(), lamPrefix)`, `unpagedRead(fromWrapped(...))` (:200-213, call at :204). Else `query = lam.body().last` (:214) → rows over `fromWrapped(query, mapping, legCtx)`, `rootClassFqn(lam)` (:217-226). Both → `rowsLegAndVerdict(... replaySqlOrNull=null, mappingFqn=mapping.fullPath(), ..., query=lam)`.

**`tryArmSameSql`** STV:254-314
- `root.args().size() != 2` → null (:259). String-typed `resultArg` → `tryArm("assertSameSQL", true, root.args(), letPrefix, specs, env, hook)` (:262-276, call :275).
- Result-typed: `strip = VerdictQueries.sqlStripRead(resultArg, env.ctx())` (:278-279), null → WALL `NotImplementedException("sql-text assertSameSQL: Result actual is not a mintable frame read")` (:284-289).
- Evaluates golden and `strip` via `JudgePlans.evalSide` (:290-293); null → WALL "did not evaluate to a string" (:294-299). Our text = the envelope splice's fold of `sqlRemoveFormatting($frame)` to the frame's executed SQL (`ResultEnvelopeSplice.sqlProducerCall`, `ResultEnvelopeSplice.java:520-556`, `frames.relationalActivitySql(name, k)`).
- `textEqual` (:300); referee null → WALL (:301-307). `fm = frameMappingAndClass(resultArg, letPrefix, hook, specs)` (:308). → `rowsLegAndVerdict("assertSameSQL", golden, ours, textEqual, referee, VerdictQueries.valuesRead(resultArg), null, fm.mapping(), fm.cls(), fm.facts(), fm.populationRead(), letPrefix, specs, env, hook, fm.query())` (:309-313).

**`tryArmDualGolden` + `dualGoldenVerdict`** STV:326-454
- `args.size() != 3` → null (:331). goldenSide = `args[1]` (UPGRADED golden), actualSide = `args[2]` (:334-335); the legacy golden `args[0]` is never read.
- String-typed actual: `read = findSqlRead(actualSide, letPrefix)`, `readK = readIndex(read)` (:345-346). `read != null && readK > 0` (:347) → `oursExpr = VerdictQueries.firstStatementRead(read)` (:352), evaluate goldenN/oursN (:354-360), → `dualGoldenVerdict(goldenN, oursN, read.args[0], readK, ...)` (:361). `read == null` → `tryArmPlanText("assertEqualsH2Compatible", ...)` (:368) then `tryArmTdgSql("assertEqualsH2Compatible", ...)` (:377). Else `oursExpr = actualSide`, `resultArg = read.args[0]` (:381-382).
- Result-typed actual: `strip = sqlStripRead(actualSide, env.ctx())` (:384-385), null → WALL (:389-393); `oursExpr = strip` (:395).
- Evaluate (:398-401); null → WALL "did not evaluate to a string" (:402-407). → `dualGoldenVerdict(golden, ours, resultArg, readK, ...)` (:408).
- `dualGoldenVerdict` (:414-454): strip warning (:419), textEqual (:420), referee null → WALL (:421-427); `fm = frameMappingAndClass(resultArg)` (:428); `route = statementRoute(readK, golden, fm)` (:431), null → return null (:432-434) (AV:330-337 then adjudicates `assertEquals(upgraded, actual)` generically). `route.let() != null && fm.mappingRef() != null` (:435) → rows over `fromWrapped(route.let().value(), fm.mappingRef(), fm.context())` with that let's facts (:436-448, call :439); else rows over `valuesRead(resultArg)` with `fm` facts (:449-453).

**`tryArmExecRead`** STV:471-600
- TDG first: `tdg0/tdg1 = hasTdgProducer` (:482-483); exactly one (:484) → `tryArmTdgSql(name, goldenSide, tdgSide, ...)` (:485-489).
- `r0/r1 = findSqlRead` (:490-493). Both-or-neither (:494): neither: `q0/q1 = findPlanProducer` (:497-500); both null and `name == "assertEquals"` → `lookThroughPlanProducer` (:501-507); exactly one → `tryArmPlanText` (:509-513); else null (:518). Both → null.
- `resultArg = read.args[0]` (:523); `golden = evalSide(goldenSide)` (:524-525). `fm = frameMappingAndClass(resultArg)` (:537); `k = readIndex(read)` (:538); `route = statementRoute(k, golden, fm)` (:539), null → null (:540-542). `k > 0` → `producerSide = firstStatementRead(read)` (our ONE statement, :543-546). `letStatement = route.let()` (:547). `ours = evalSide(producerSide)` (:548-549); null → null (:550-552). Strip (:553), textEqual (:554), referee null → WALL (:555-561).
- Routing: `letStatement != null && fm.mappingRef() != null` (:562) → rows over the k-th statement let's value, `fromWrapped(letStatement.value(), fm.mappingRef(), fm.context())` (:563-574, call :566). Else `pop = populationShape(golden, fm.query())` (:581): `pop.rowsRead() != null && mappingRef` (:582) → rows over the population let's expression (:585-593). Else rows over `valuesRead(resultArg)` with `temps = pop == null ? [] : pop.temps()` (:594-599).

**`tryArmPlanText`** STV:782-887
- `producer = findPlanProducer(actualSide, letPrefix)` (:787); null and `assertEquals` → `lookThrough` gives producer + reduced `scope` (:797-803). No producer/args → null (:805-807). `lamArg = producer.args[0]` (:808) let-chased in `scope` (:809-816); not a lambda → null (:817-819). `mapping = args[1]` if a `TypedPackageableRef` else null (mapping-less `executionPlan(lambda, ext)`, :825-826).
- Evaluate golden and `actualSide` under REFEREE_OURS (:829-834); null → null (:835-837). Our text is `EnginePlans.planToString` folded as a K-routine (EP:236-388, wired EP:216-229). `textEqual` (:838); referee null → WALL (:839-845).
- `bindings = VerdictQueries.refereeBindings(lam)` (:846-848); null → `declined(env, name, "plan-params-unbindable")` (:850) + `textVerdict` (:851-855).
- `filled = golden` with each `${name}` replaced by `bindings.spellings()` (:856-859); `bound = scope + bindings.lets()` (:860-861) `+ lam's leading statements` (:869); `planCtx = BodyPlanner.boundContext(args[2], scope, specs)` when ≥3 args (:863-865); `replay = filled.contains("${") ? null : planReplaySql(filled)` (:877); `read = mapping == null ? last : fromWrapped(last, mapping, planCtx)` (:879-880).
- → `rowsLegAndVerdict(name, golden, ours, textEqual, referee, read, replay, mapping?.fullPath(), rootClassFqn(lam), replayFacts(last, bound), unpagedRead(read), bound, specs, env, hook, lam, goldenPlan = replay == null ? golden : null, planBindings = bindings.lists())` (:881-886). So a residual hole or a multi-node plan goes to `referee.verifyPlan`.

**`tryArmTdgRoot` / `tryArmTdgSql` / `tdgRowsNow`** STV:978-1084
- Root: 2 args (:983), exactly one `hasTdgProducer` (:986-990) → `tryArmTdgSql("assertSqlEquals", golden, tdg, ...)` (:991-994).
- `tryArmTdgSql`: `!hasTdgProducer(actualSide, letPrefix)` → null (:1010-1012). Evaluate both (:1013-1016; the actual side is the folded carrier's `sqls->at(i)` literal after the orchestrator's fold); null → null (:1017-1019). `textEqual` (:1020); referee null → WALL (:1021-1027). Batch active → `batch.defer(name, true, VerdictSql.textEquals(golden, ours), env.session(), () -> tdgRowsNow(...))`, return `ok()` (:1028-1037, defer :1034). `textEqual` → `ok()` (:1039-1042). Else `tdgRowsNow` (:1043).
- `tdgRowsNow`: `hop = tdgHop(actualSide, letPrefix)` (:1054). Under `REFEREE_GOLDEN` (:1056): `hop == null` → `referee.verifyFetchTexts(env.session(), golden, ours)`; else `referee.verifyFetchChain(env.session(), hop.index(), golden, ours, () -> fetchTranscript(TestDataGenerationNatives.transcript(hop.source(), env.ctx(), env.planSessions(), env.target(), env.dialect(), BodyDriver.viewSqlRenderer(specs, env))))` (:1057-1065; `TestDataGenerationNatives.java:186-210` re-runs `TestDataGenerator.generate` through the plan runner on the body's sessions). `refereed(env, name, rv)` (:1066). MATCH → ok (:1068); DIVERGED → `fail(...)` (:1071); FAULT → `fail(...)` (:1074); DECLINED → `declined(env, name, "oracle-declined")` (:1078), then `textEqual ? ok() : fail(...)` (:1079-1083). NOTE: in this arm OUR ROWS are NOT `evalSide`'d — the harness runs `ourSql` itself on the body's connection (RR:456-484 `tdgSqlReplay`: `RefereeVerify.rawRows(st, ourSql)` on the session or a second reader) and multiset-compares with the golden replayed on the mirror (`RefereeVerify.multisetCompare`, RV:1191-1208).

### 1.4 Verdict outcomes per path (all arms)

| Situation | Code | Outcome |
|---|---|---|
| no referee registered | STV:166-172, 301-307, 421-427, 555-561, 839-845, 1021-1027 | WALL `NotImplementedException("sql-text assert verdict needs a replay oracle and none is registered on this env ...")` |
| golden/ours not a string | tryArm :157-159, exec-read :550-552, plan-text :835-837, tdg :1017-1019 → null (generic path continues); sameSql :294-299 and dualGolden :402-407 → WALL |
| foreign dialect (`dbType != H2`) | STV:174-181 | `declined("foreign-dialect:<db>")`; `textVerdict`: textEqual → ok, else `AssertFailed("... text is the contract ...")` |
| plan params unbindable | STV:849-855 | `declined("plan-params-unbindable")`; textVerdict |
| text equal (no batch) | STV:1394-1401, 1039-1042 | `ok()` — rows leg NOT run |
| text differs, rows leg: our rows raise / null | STV:1433-1440, 1460-1464 | `declined("rows-underivable")`; textEqual ? ok : `AssertFailed` |
| referee MATCH | STV:1482-1485, 1068-1070 | `ok()` |
| referee DIVERGED | STV:1486-1488, 1071-1073 | `AssertFailed("... ROW verdict — golden rows vs ours diverged, whatever the text said ...")` |
| referee FAULT | STV:1489-1491, 1074-1076 | `AssertFailed("referee FAULT ...")` |
| referee DECLINED | STV:1492-1503, 1077-1083 | `declined("oracle-declined")`; textEqual ? ok : `AssertFailed` |
| under a verdict batch (database judge) | STV:1381-1393, 1028-1037, 1704-1714 | `batch.defer(name, true, VerdictSql.textEquals(golden, ours), env.session(), appeal)` (:1388, :1034, :1709); arm returns `ok()`; the appeal (`rowsLegNow`/`tdgRowsNow`/`fail`) runs at the flush only when the row failed (`VerdictBatch.defer(name, wantEqual, query, on, appeal)`, `core/src/main/java/com/legend/judge/VerdictBatch.java:233-245`; `VerdictSql.textEquals` builds a one-row NULL_SAFE_EQUAL over two string constants, `core/src/main/java/com/legend/lowering/VerdictSql.java:596-602`) |

`ok()` = `new TypedSide.Scalar(Boolean.TRUE, BOOLEAN)` (STV:1695-1698); `fail` throws `com.legend.error.AssertFailed` (STV:1716-1719); `textVerdict` (STV:1704-1714).

---

## 2. The rows leg

### 2.1 `rowsLegAndVerdict` — three overloads

| Overload | Lines | Params | Delegation |
|---|---|---|---|
| 16-param | STV:1327-1344 | `(name, golden, ours, textEqual, referee, rowsRead, replaySqlOrNull, mappingFqn, classFqn, facts, populationRead, letPrefix, specs, env, hook, query)` | → next with `goldenPlan = null`, `planBindings = Map.of()` (:1338-1343) |
| 18-param | STV:1346-1365 | `+ goldenPlan, planBindings` | → next with `temps = List.of()` (:1359-1364) |
| 19-param | STV:1367-1407 | `+ temps` | the policy (below) |

The policy (STV:1380-1406), quoted:
```
com.legend.judge.VerdictBatch batch = env.verdictBatch();                 // :1380
if (batch != null && batch.active()) {                                     // :1381
    // BLOCK-COMPILER RUNG 2a (2026-09-21): the text assert IS a verdict row of
    // the body's statement — ... and the referee is
    // its APPEAL, run at the flush only when the row failed ...
    batch.defer(name, true, com.legend.lowering.VerdictSql.textEquals(golden, ours),
            env.session(), () -> rowsLegNow(...));                         // :1388-1391
    return ok();                                                           // :1392
}
if (textEqual) {                                                           // :1394
    // THE HOST JUDGE, THE SAME RULE (user ruling 2026-09-21, option 1): a text
    // byte-equal to the golden IS the assert's verdict — the engine's own —
    // and rows are the APPEAL on a failed text only. ...
    return ok();                                                           // :1400
}
return rowsLegNow(...);                                                    // :1402-1405
```
**FINAL VERDICT RULE: a byte-equal text short-circuits — rows run only when the text differs** (STV:1394-1401; same in the TDG arm STV:1039-1042). When they run, rows decide: MATCH passes whatever the text said, DIVERGED/FAULT fail, DECLINED/underivable fall back to text (which on this path is unequal → FAIL; the only way `textEqual` is true inside `rowsLegNow` is the batch appeal, whose row already failed). Net: text equal → pass; text unequal → pass iff the referee says MATCH.

### 2.2 `rowsLegNow` STV:1409-1505 — what runs where

| Step | Lines | Where it runs |
|---|---|---|
| `provideStores(env, mappingFqn)` | :1426; :1652-1681 | `storesOf(env.ctx(), mappingFqn, ...)` walks the compiled mapping's `ClassBinding.Relational` → `RelationalSource.Table.database()` plus includes (:1663-1681); each store → `AssertListener.provideStore(store)` (the runner may seed a fixture into the body's session, `PureTestRunner.java:510-517`) |
| OUR ROWS | :1424-1443 | `priorSuspend = SqlTypeCensus.probeSuspended()` (:1424-1425), `probeSuspend(true)` (:1428), restore in `finally` (:1441-1443); under `StatementOrigin.REFEREE_OURS` (:1429): `rows = JudgePlans.evalSide(rowsRead, letPrefix, specs, env, false, true, hook)` (:1430-1431) → `BodyDriver.planHostSide` (BD:1184-1210: inline, fold a literal-only side to a constant with nothing sent, else `planStaged`) → `JudgePlans.sideResult(side, env)` runs the plan through the plan runner on `WrappedSide.session` (the route's session; BD:1143-1150; JP:382-395, 410-412). RuntimeException → `declined("rows-underivable")` (:1437) + text (:1433-1440) |
| population (paged chain) | :1444-1458 | `populationRead != null && rows != null` → `evalSide(populationRead)` → `facts = facts.withPopulation(population)` (:1454); no catch (a failure is OURS and fails loudly) |
| rows null | :1460-1464 | `declined("rows-underivable")` (:1461) + text |
| GOLDEN ROWS + compare | :1466-1479 | under `REFEREE_GOLDEN` (:1467): `rv = goldenPlan != null ? referee.verifyPlan(env.session(), goldenPlan, planBindings, rows, mappingFqn, classFqn, facts, env.ctx()) : referee.verify(env.session(), replaySqlOrNull != null ? replaySqlOrNull : golden, rows, mappingFqn, classFqn, facts, env.ctx(), temps.isEmpty() ? inListTemps(golden, query != null ? query : rowsRead, letPrefix, frameZone(hook == null ? rowsRead : hook.apply(rowsRead, Set.of()))) : temps)` (:1468-1478) |
| census | :1480 | `refereed(env, name, rv)` → `AssertListener.refereed(name, rv.outcome().name())` (STV:1687-1693) |
| verdict | :1481-1504 | see §1.4 |

Harness side of `referee.verify(...)` (RR): `verify` (8-arg, RR:605-615) → `verify1` (RR:618-662): `seeds = tempSeeds(temps)` (RR:760-790, H2 `CREATE LOCAL TEMPORARY TABLE ... (ColumnForStoringInCollection ...)` + inserts); a `population:*` temp is filled from the most recent `select distinct` golden in `ATTEMPT_SQL_GOLDENS` (RR:630-645), the current golden appended (RR:656); → `verifyArmed(connectionOf(session), goldenSql, ours, mappingFqn, rootClassFqn, ctx, seeds, facts)` (RR:794-852): enum decode maps per column from the mapping (`RefereeVerify.decodeOf`), then `verifyAuto(session, recorder.seeds(), extraSeeds, goldenSql, ours, enumDecode, enumProp, facts)` (RR:391-403): if `TestBackends.of(session) == TestBackends.referee()` (the H2 lane) the golden runs DIRECTLY on the body's connection (`verifyOnSession`, RR:409-421); else `verify(seeds, extraSeeds, ...)` (RR:339-382) → `onReferee(seeds, VERIFY_SESSION, st -> compareFrame(...))` (RR:265-309: the live family MIRROR with incremental ledger replay, or a fresh in-memory H2 replaying the whole ledger). `connectionOf` declines a session the test side did not make (the system database's) (RR:898-906).

Compare policy: `RefereeVerify.compareFrame(st, goldenSql, ours, enumDecode, graphEnumProp, facts)` (RV:340-402): population → page-membership; a paginated golden without a typed tail page → `Unverifiable` decline; a `now()`-projecting golden → decline; Graph → `goldenGraphCompare` (RV:443); else `goldenRowsCompare` (RV:689-): the §7 order rule — `facts.population()` → `pageMembership`; `facts.ordered() && keyIdx != null` → `orderedVerdict` (in order, ties grouped by `facts.sortKeys()`); otherwise both sides sorted and compared as a multiset (RV:785-815, `Cells.compareTo` RV:825-845). TDG texts: `RefereeVerify.multisetCompare(golden, ourRows)` — header equality (projection) then sorted-rows equality (RV:1191-1208). `Unverifiable` → `outcome(...)` → `RowVerdict.declined` or `.fault` (RR:670-679).

### 2.3 What is counted

| Counter | Where | Note |
|---|---|---|
| `AssertListener.declined(name, reason)` | STV:1638-1644 (`declined`); reasons `foreign-dialect:<db>` (:177), `plan-params-unbindable` (:850), `rows-underivable` (:1437, :1461), `oracle-declined` (:1078, :1495); `AssertListener.java:26` | runner keeps it on the upcoming verdict (`PureTestRunner.java:490-493`) |
| `AssertListener.refereed(name, outcome)` | STV:1687-1693, called :1066, :1480; `AssertListener.java:39` | MATCH = "differential witness" (`PureTestRunner.java:501-506`) |
| `CanonicalDivergence.sqlRoute(name, "sql-text")` | AV:382 — only when `tryArm` answered; the three ROOT arms (AV:300-324) are not routed-counted | `CanonicalDivergence.java:387-393` |
| `ReplayReferee.OUTCOMES` | RR:31-32, 580, 613, 701, 859 | harness display only |
| `SqlTextEmission` | STV:31 javadoc only | **no class of that name exists in the repo** (grep: the only hit is the javadoc) — "text match/diff is a census number" has no code form today |
| `SqlTypeCensus.probeSuspend` | STV:1428, 1442, 1449, 1457 | suspends the SQL-type probe around referee-class executions (`SqlTypeCensus.java:183-192`) |

---

## 3. The environment the arms need from `BodyDriver.ExecEnv`

`ExecEnv` is a record `(PlanEnv plan, Session session, AssertListener assertListener, SqlReplayReferee replayReferee, ExecuteOptions options, ExecutionTrace trace, VerdictBatch verdictBatch, EffectSink effectSink)` (BD:114-121). Every `env.` use in STV (from `grep -on "env\.[a-zA-Z]*("`):

| Accessor | STV lines | For | Definition |
|---|---|---|---|
| `env.replayReferee()` | 166, 301, 421, 555, 839, 1021 | the SPI; null → WALL | BD:117 (component) |
| `env.withFrame(frameCtx)` | 143 | install the producer's runtime context (post-processors, tz) read off a `toSQLStringPretty(..., runtime, ...)`/handle form | BD:190-192 → `plan.withFrame` |
| `env.postProcessors().tableReplace()` | 144 | the runtime's `replaceTables` renames for OUR rows | BD:161-163 |
| `env.withTableReplace(tr)` | 146 | same | BD:193-195 |
| `env.frame()` | 234-235 (`legContext`) | base bound context for the rows leg's `from` | BD:158-160 |
| `env.ctx()` | 279, 385 (`sqlStripRead` overload lookup), 1062 (TDG transcript), 1470, 1473 (`referee.verify*` enum decode), 1658 (`storesOf`) | model navigation | BD:137-139 |
| `env.verdictBatch()` | 1028, 1381, 1707 | database-judge deferral | BD:118 |
| `env.session()` | 1035, 1058-1059, 1389, 1469, 1471, 1710 | handed to `batch.defer(...)` and to every `referee.verify*` as "the executing connection" | BD:115 |
| `env.planSessions()` | 1063 | `PlanSessions.of(session, trace)` for the TDG transcript re-run | BD:214-216 |
| `env.target()` | 1063 | `JudgePlans.target(this)` for the TDG transcript | BD:219-221; JP:56-66 |
| `env.dialect()` | 1063 | the lane's dialect for the TDG transcript | BD:143-145 (`plan.writtenIn()`) |
| `env.assertListener()` | 1640 (`declined`), 1654 (`provideStores`), 1689 (`refereed`) | census + fixture-on-demand | BD:116 |
| whole `env` | every `JudgePlans.evalSide(..., env, ...)` call (:153, 155, 290, 292, 398, 400, 524, 548, 830, 832, 1013, 1015, 1430, 1452); `BodyDriver.viewSqlRenderer(specs, env)` :1064 (→ `env.plan()`, BD:1228-1232) | planning + running a side | — |

Not used by STV: `queryLets()`, `runtimeFqn()`, `options()`, `trace`, `effectSink`, `timeZone()` (the zone comes from the read's own `TypedFrom`, STV:1173-1176). `letPrefix` arrives as a parameter, not from env. The `hook` parameter is `AssertVerdicts.SpliceHook` (`BiFunction<TypedSpec, Set<String>, TypedSpec>`, AV:44-46), built by `BodyDriver.spliceHook(frames, letPrefix, specs, env)` (BD:590-625) over `ResultEnvelopeSplice.hook(Frames)`; the walk hands `frameReplaceEnv(stmt, execFrames, env, letPrefix, specs)` (BD:416-427) as the env. Thread-local marks: `StatementOrigin.enter(REFEREE_OURS)` at :152, :829, :1429, :1450; `REFEREE_GOLDEN` at :1056, :1467 (`StatementOrigin.java:34-36, 77-81`).

---

## 4. The harness side

**The SPI** (`SRR`): `interface SqlReplayReferee` (SRR:23) with `RefereeRows rows(String sql)` (:35), `RowVerdict verifyFetchTexts(Session, goldenSql, ourSql)` (:46), `RowVerdict verifyFetchChain(Session, int hopIndex, goldenSql, ourSql, Supplier<FetchTranscript>)` (:60), `RowVerdict verify(Session, goldenSql, TypedSide ours, mappingFqn, rootClassFqn, ReplayFacts, ModelContext)` (:102), default `verify(..., List<TempTable> temps)` ignoring temps (:159), default `verifyPlan(Session, goldenPlan, Map<String,List<String>> bindings, ours, mappingFqn, rootClassFqn, facts, ctx)` → declined (:175); records `FetchTranscript(sqls, hops)` (:68), `FetchHop(parentIndex, table, columns, rows)` (:75), `RefereeRows(labels, jdbcTypes, rows)` (:84), `ReplayFacts(extentSubset, ordered, sortKeys, population)` with `NONE` and `withPopulation` (:127-149), `TempTable(name, kind, values)` (:151), `RowVerdict(Outcome{MATCH,DIVERGED,DECLINED,FAULT}, detail)` (:185-207).

**Who implements it**: one class, `spec/src/test/java/com/legend/harness/ReplayReferee.java:29 public final class ReplayReferee implements com.legend.judge.SqlReplayReferee`. Nothing in `core/src/main` or `core/src/test` implements it (grep: core hits are the SPI, the ExecEnv/BodyDriver/BodyRouter/Execution carriage, `PureTestRunner`, `TestObserver`, and two ledger tests `JavaEvalLedgerTest.java:666,1401`, `JdbcSurfaceCensusTest.java:177`).

**Who registers it**: `TestObserver.referee(PureTests.TestCase)` (default null, `core/src/main/java/com/legend/test/TestObserver.java:50`); `MinimalCorpus` overrides it (`spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:617-619`) returning the referee built in `options(...)`: `recorder = new RawSqlBoundary.Recorder(shared ? seedLedger : List.of()); referee = new ReplayReferee(recorder);` (MinimalCorpus.java:600-604). `PureTestRunner` reads it per test (`PureTestRunner.java:434 SqlReplayReferee referee = observer.referee(t)`) and passes it to `Execution.runProgram(Compiler.queryResolved(ctx, resolved), runtimeFqn, Source.given(conn), new AssertListener(){...}, referee, options)` (PureTestRunner.java:476-521).

**"The run's recorded seed ledger"**: the `RawSqlBoundary.Recorder` the referee is constructed with (RR:35-41) — the session's seed prefix plus everything the setups and the body executed, appended by the executor through `ExecuteOptions` (MinimalCorpus.java:597-604; on the program road `BodyRouter.Moved.record` BR:280-284 and `seeded`/`effectRan` BR:258-272 do the appending). The referee reads `recorder.seeds()` on every verify (RR:191, 831-833, 866-868, 885-887, 908-910). Application: `onReferee(seeds, Session, Work)` (RR:265-309): a live family MIRROR — one H2 per package session, opened by `TestBackends.referee().openReferee(...)` and installed with `ReplayReferee.mirrorBegin(mirrorConn)` at `sessionBegan` (MinimalCorpus.java:566-576), detached at `sessionEnding` (:580-590), suspended for private workspaces (:594-596) — gets only the ledger statements not yet applied (`applyPendingSeeds`, RR:313-329; an H2-rejected seed POISONS the mirror); otherwise a fresh in-memory H2 replays the whole ledger. Per-verify extras (temp tables) never advance the cursor (RR:339-367). Attempt bracket for effectful bodies: `beginAttempt` (RR:111-117, clears `ATTEMPT_GOLDENS`/`ATTEMPT_SQL_GOLDENS`), `commitAttempt`/`rollbackAttempt` (RR:205-232), driven by `MinimalCorpus.bodyStarting/bodyPassed` (MinimalCorpus.java:622-640).

**How `Execution.runProgram`/`BodyRouter` carry it**: `Execution.runProgram(TypedQuery, runtimeFqn, Source sessions, AssertListener listener, SqlReplayReferee referee, ExecuteOptions, Answer)` (`core/src/main/java/com/legend/Execution.java:147-155`), the no-answer overload (:160-164), and the session overload (:182-191) → `BodyRouter.execute(typed, runtimeFqn, dialect, session, listener, referee, options, answer)` (BR:50-52). The router hands the referee to `BodyDriver.bodyEnv(typed, runtimeFqn, dialect, session, listener, referee, options)` for the planned-asserts env (BR:70-71; BD:82-94 → `ExecEnv.withListeners(assertListener, replayReferee)` BD:196-199) and to `BodyDriver.execute(..., referee, ...)` for the walk (BR:96). `HostAsserts.plan`/`DatabaseAsserts.plan` receive an env carrying it but never read `replayReferee()` (grep: no `replayReferee` in HostAsserts/HostJudge/DatabaseAsserts/TestDoor/PlanMaker/PlanRunner). `PlanRunner.program(plan, values, PlanSessions.Source, Judging, ProgramSink)` (`core/src/main/java/com/legend/exec/PlanRunner.java:208-209`) has no referee parameter; the `ProgramSink` has `verdict`, `fused`, `sides`, `sidesFailed`, `seeded`, `originOf`, `effectRan`, `effectFailed` (PlanRunner.java:147-196). `TestDoor.Recorded` keeps "the text the walk records (the referee's spelling)" beside effect/seed steps (`TestDoor.java:39-41, 55-58`).

---

## 5. Planning-time pieces that already exist on the program road

**`planner/SqlTextInputs`** (STI:36-67). `public record SqlTextInputs(TypedSpec query, TypedSpec mapping, TypedSpec dialect, TypedSpec runtime, boolean receiverForm)`; `public static @Nullable SqlTextInputs of(NativeDispatch.RoutineCall call, List<TypedSpec> letPrefix)`: the receiver form (`NativeFn.JavaRoutine.ofDerived(call.fqn())` present and 5 args; the receiver `Lets.bound(args[0])` must be a `PlatformTypes.TO_SQL` call with ≥3 args) → `(h.args0, h.args1, call.args1, h.args2, true)` (:44-56); the direct forms need ≥3 args → `(args0, args1, args2, args2, false)` (:58-66). Does NOT chase a let-bound query lambda (the caller does, STV:99-101) and does not resolve the dialect (the caller does, STV:114-141; EP:41-78).

**`planner/EnginePlans`** (EP). `private static @Nullable String toSqlString(NativeDispatch.RoutineCall call, List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env)` (EP:29-105): inputs from `SqlTextInputs.of`; dialect from the enum tail (`typedEnumTail`), from the handle's runtime (inlined, `ExecutionContext.reader().bind(...).read`, throws when unreadable), or from the `dbArg`'s context — with a C4 default to `EngineText.ENGINE_TEST_DATABASE` (H2) when unread (EP:70-78: a defaulting branch, flagged in its own comment); renderer `EngineText.engineText(Databases.named(db))` (EP:79); `engineSql(lam, mapping.fullPath(), specs, env, renderer)` (EP:92; public overloads EP:113, 134, 148; `record EngineSql(SqlQuery plan, String sql, List<TypedSpec> body)` EP:108-110); `SqlPostProcessors.apply(plan, env.tableReplace() + the call's own runtime renames)` (EP:93-99); `SqlPostProcessors.nonExecutable` for `TO_NON_EXECUTABLE_SQL_STRING` (EP:100-104). `private static @Nullable String planToString(RoutineCall call, List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env)` (EP:236-388): let-chases arg 0; a `TypedTestDataGen` → `env.generatorPlanText()`; else requires an `EXECUTION_PLAN` call (throws otherwise), unwraps `PREVAL`, mapping from arg 1 or the query's first `from()` / chain mappings, `BodyPlanner.boundContext(args[2], specs)` when a runtime is present, features, `sequencePlan` for parameters/multi-statement lambdas, `PlanText.singleRelationRoot` / `crossDbTdsPlan` / `PlanText.single`. Both are exposed only through `public static Map<String, NativeDispatch.Routine> nativeRoutines(SpecCompiler specs, PlanEnv env)` (EP:212-234), keyed by `NativeFn.JavaRoutine.{PLAN_TO_STRING, PLAN_TO_STRING_WITHOUT_FORMATTING, TO_SQL_STRING, TO_SQL_STRING_PRETTY, TO_NON_EXECUTABLE_SQL_STRING}.fqn()`, handed to the stager at `BodyPlanner.java:247`, `ProductStatements.java:129`, BD:307. They need a `PlanEnv` (ctx, tableReplace, features), the `SpecCompiler`, the let prefix; they never touch a session.

**`compiler/spec/VerdictQueries.fromWrapped`** (VQ:69-82). `public static TypedSpec fromWrapped(TypedSpec query, TypedPackageableRef mapping)` → the 3-arg with `ExecutionContext.NONE`; `public static TypedSpec fromWrapped(TypedSpec query, TypedPackageableRef mapping, ExecutionContext base)` → `new TypedFrom(query, base.withMapping(Optional.of(mapping)), query.info())`: the rows-leg read is the producer's query under its mapping, the runtime left to the executing env. Siblings used by the arms: `sqlStripRead(resultArg, ctx)` (VQ:90-108, mints the Result overload of `sqlRemoveFormatting`), `valuesRead(resultArg)` (VQ:111-114, `$result.values`), `firstStatementRead(read)` (VQ:46-49), `activitiesRead(frameVar)` (VQ:389-392), `assertEqualsOf(expected, actual, specs)` (VQ:56-67), `extentSubset(n)` (VQ:359-387), `refereeBindings(TypedLambda)` → `PlanBindings(lets, spellings, lists)` (VQ:126-230: fixed referee values per parameter type — String "A", Integer 22, Float/Number 1.0, Boolean true, Date/StrictDate 2015-10-16, DateTime 2015-10-16 00:00:00, String/Integer collections ["A","B"]/[22,23]; anything else → null).

**`AssertVerdicts.replayFacts`** (AV:1379-1386). `static SqlReplayReferee.ReplayFacts replayFacts(TypedSpec chain, List<TypedSpec> letPrefix)` → `new ReplayFacts(VerdictQueries.extentSubset(chain), ordered = OrderView.of(chain, letPrefix) == OrderView.SORTED, ordered ? sortKeys(chain, letPrefix, new HashSet<>()) : null)`. Typed-tree navigation only; these are the referee's compare gates (SRR:112-126).

**`AssertVerdicts.unpagedRead`** (AV:1395-1431). `static @Nullable TypedSpec unpagedRead(TypedSpec read)`: a tail `TypedLimit`/`TypedDrop`/`TypedSlice` is replaced by its source; the walk descends through order-preserving wrappers (`TypedFrom, TypedFilter, TypedProject, TypedSelect, TypedRename, TypedDistinct, TypedSort, TypedSortBy, TypedMap, TypedPropertyAccess, TypedCast, TypedNavigate`, and `OrderView.ORDER_PRESERVING` natives) rebuilding with `withChildren`; null when no page at the tail. Used as the `populationRead` of every rows leg (STV:209-211, 223-225, 443-445, 571-573, 590-592, 884, 1611-1613).

Also already on the program road and reused by the arms: `BodyPlanner.boundContext(runtimeArg, letPrefix, specs)` (`BodyPlanner.java:118-125`), `ExecutionContext.reader()/froms()` (`ExecutionContext.java:294, 236`), `Lets.bound/binding` (`Lets.java:22, 41`), `UserCallInliner.inlineBody`.

---

## 6. Size and shape summary

**Distinct arms**: 5 package-visible entries (`mayArm`, `tryArm`, `tryArmSameSql`, `tryArmDualGolden`, `tryArmTdgRoot`) and 5 private arms/tails (`tryArmExecRead`, `tryArmPlanText`, `tryArmTdgSql`, `dualGoldenVerdict`, `tdgRowsNow`), plus the shared tail `rowsLegAndVerdict` (3 overloads) + `rowsLegNow`. By what they compare:

| Arm | Kind | Our text | Our rows | Golden rows |
|---|---|---|---|---|
| `tryArm` (toSQLString) | golden text vs our text + rows leg | K-routine `EnginePlans.toSqlString` folded at plan time | producer's lambda `fromWrapped(mapping, legCtx)` via `evalSide` | `referee.verify(golden)` + `inListTemps` |
| `tryArmSameSql` | same | frame's executed SQL (`ResultEnvelopeSplice.sqlProducerCall`) | `$result.values` (spliced to the frame's chain) | `referee.verify` |
| `tryArmDualGolden` | same, UPGRADED golden only | as sameSql / exec-read / plan / TDG | per `statementRoute` | `referee.verify` |
| `tryArmExecRead` | same | frame's k-th statement SQL (ours always index 0) | the k-th statement let's value, a population let's expression, or `$res.values` (+ in-list / population temps) | `referee.verify(..., temps)` |
| `tryArmPlanText` | golden PLAN text (holes intact) vs our plan text + rows leg under referee bindings | K-routine `EnginePlans.planToString` | lambda's last statement under the minted parameter lets | `referee.verify(one filled SQL node)` or `referee.verifyPlan(plan, bindings)` |
| `tryArmTdgRoot`/`tryArmTdgSql` | fetch TEXT vs fetch TEXT; rows leg entirely inside the referee | the folded carrier's `sqls->at(i)` literal | the referee executes OUR text on the body's connection (RR:456-484); chained hop: the generator re-run transcript | `verifyFetchTexts` / `verifyFetchChain` |

Exceptions that are "something else": the foreign-dialect and plan-params-unbindable paths are TEXT verdicts (`textVerdict`, STV:1704-1714) with a counted decline; `tryArmSameSql`'s String overload is a re-entry into `tryArm` (STV:275); the dual-golden null → `assertEqualsOf` rewrite happens in AV:330-337, outside STV.

**Static state in STV**: constants only — `CHAINED_PLAN_WARNING` (STV:611-612), `TDG_RESULT_FQN` (:1090-1091), patterns `NAMED_IN_TEMP` (:682-683), `NUMBERED_IN_TEMP` (:1158-1159), `PLAN_SQL_NODE` (:912-916). No mutable statics.

**Mutations / side effects performed by STV**: `SqlTypeCensus.probeSuspend` on a process-global `AtomicBoolean` (STV:1428, 1442, 1449, 1457); `StatementOrigin.enter` thread-local marks (STV:152, 829, 1056, 1429, 1450, 1467); `AssertListener.declined/refereed/provideStore` (the last may SEED a fixture into the body's session before our rows run, STV:1426 + `PureTestRunner.java:510-517`); `VerdictBatch.defer` (STV:1034, 1388, 1709); the local `env` rebinding `withFrame`/`withTableReplace` (STV:143-147, immutable copies).

**Harness-side state the arms depend on across statements (ReplayReferee)**: `ATTEMPT_GOLDENS` (RR:120-121; filled by `verifyFetchChain0` RR:139-140 per hop index, read for ancestor temps RR:170-176: a chained hop's verdict needs the ancestor hops' asserts to have run EARLIER in the body); `ATTEMPT_SQL_GOLDENS` (RR:685-686; every verified golden appended at RR:656; a `population:*` temp is filled from the most recent `select distinct` golden, RR:630-645: the in-list golden(1) needs golden(0) verified earlier in the same attempt); the mirror cursor/poison (RR:58-65, 313-329).

**Statement-ORDER dependence inside the arms**: (a) every finder chases `letPrefix` (the lets BEFORE the assert, as the walk holds them); (b) `readIndex`/`statementRoute` pair the engine's n-th statement with the frame lambda's n-th statement-let, read off the frame's `execute()` through the splice hook (STV:537-546, 622-651) — the frame must exist as an `ExecFrame` (built by an earlier `let r = execute(...)`); (c) `populationShape`/`NAMED_IN_TEMP` (STV:690-718) and the referee's population temp (above) assume the population golden was asserted first; (d) `tdgHop` reads the hop index `i` of `$td.sqls->at(i)` (STV:1105-1156) and `verifyFetchChain` needs ancestor goldens asserted first; (e) the TDG actual side is the carrier FOLDED by the orchestrator at the let's own execution (`TypedNewInstance` of `TestDataGenResult`, STV:1090-1091, 1140-1145), so the arm's detection depends on the let having already run; (f) `frameMappingAndClass` via `hook.apply(activitiesRead(resultArg))` depends on `ResultEnvelopeSplice` frames registered by earlier statements (STV:1573-1585; BD:590-625).

**Walls (hard `NotImplementedException`) that keep a body off any judge**: no referee (6 sites, §1.4); a Result-typed `assertSameSQL`/`assertEqualsH2Compatible` actual that `sqlStripRead` cannot mint (STV:284-289, 389-393); a golden/actual that does not evaluate to one string in those two root arms (STV:294-299, 402-407).

**Not answerable from the code**: how many corpus asserts each arm claims today (the lanes' census output, not in the tree); whether `mayArm`'s superset over-refuses on the program road (e.g. a two-producer `assertEquals`, which `tryArm` would hand back to the generic path at STV:86-88) — the code refuses it (AV:747) regardless.
