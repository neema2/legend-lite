# Seam homework: the driver, the planner and the plan (code measurements)

> Evidence for `docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md`. Measured by an independent read-only agent on 2026-10-30 at HEAD `caa4bb8c` (step C). Line numbers are that commit's; the measurements, not the lines, are the facts.

Repository `/home/user/legend-lite`, branch `claude/wizardly-dirac-rw6tim`, HEAD `caa4bb8c`. Read-only: grep, sed, python3 over files; no bazel run, no file changed. Every path below is under `core/src/main/java/com/legend/` unless it says otherwise; `file:line` is the line in that file at this HEAD. Measurements only; no recommendations.

---

## 1. Call sites of TypedQuery's internals and PlanMaker's four statics

The seven internals are declared at `TypedQuery.java:45` (`specs`), `:51` (`resolved`), `:130` (`lower`, 4 args), `:139` (`record Resolved`) with `:140` (`root`), `:148` (`resolve`), `:165` (`lowerer`), `:189` (`writes`). Their javadoc at `TypedQuery.java:41-44` names them "the planner's and the driver's internals, public since step C (D73, 2026-10-30)".

### 1a. `specs()` — 2 callers
- **BodyDriver** `driver/BodyDriver.java:57` `SpecCompiler specs = typed.specs();` — the body driver takes the query's own SpecCompiler and hands it to `BodyCompiler.execute` (`:93-94`, `:97`), from where it rides every arm (`prepareValue`, `buildFrame`, `AssertVerdicts.tryAdjudicate`, the judges): the compiler the driver inlines and types helper bodies with, mid-run.
- **PlanMaker** `planner/PlanMaker.java:119` `StagedRows.refuseLetsTheWritesChange(body, query.specs())` — a write program's plan-time guard that no let the writes change is read after them.

### 1b. `resolved()` — 1 caller
- **BodyDriver** `driver/BodyDriver.java:59` `… resolved = typed.resolved();` → `:69-72`: when it is a parameterless `LambdaFunction`, `env.withProtocolBody(rlf.body())` — the source-shaped protocol statements the handle rows (`PlanAllocations`' lineage scan) are built from.

### 1c. `lower(runtime, streaming, slots, features)` (4 args) — 1 external caller
- **PlanMaker** `planner/PlanMaker.java:64` `Compiler.LoweredQuery l = query.lower(runtime, true, slots, features);` — the read-only product plan: one lowering at the streaming graph root, then `typed(...)` (`:90`) or `keyed(...)` (`:87`).
- Internal: `TypedQuery.java:124` (the 2-arg `lower` delegates to it; the 2-arg one is called only by `TypedQuery.plan` at `:229`, the legacy `QueryPlan` for `server/PureV1Api.java:390`).

### 1d. `Resolved` / `root()` — PlanMaker only
- `planner/PlanMaker.java:135` `TypedQuery.Resolved r = query.resolve(with(lets, rows), runtime);` then `:139` `r.root().info()` — a staged let's rows resolved with the lets before it; its root's columns name the temp table's.
- `planner/PlanMaker.java:146` `TypedQuery.Resolved r = query.resolve(with(lets, stmt), runtime);` then `:147` `Lets.bare(r.root())`, `:165` `typed(r.root(), q, dialect, target)` — each statement of a write program resolved with its let prefix; the last one's root is the plan's answer.
- Internal: `TypedQuery.java:132-133`.

### 1e. `resolve(statements, runtime)` — PlanMaker only
- `planner/PlanMaker.java:135` and `:146` (above). No driver caller: the driver builds its own `StoreResolver` (`driver/BodyDriver.java:577-587`) instead.

### 1f. `lowerer(temporal, streaming, slots, features)` — PlanMaker only (4 sites)
- `planner/PlanMaker.java:136` `query.lowerer(r.temporal(), false, slots, features)` → `:137` `lowerer.namedColumns(lowerer.lower(r.body()))` — a staged let's rows as a `CreateTempTableAs` (`:138-140`).
- `planner/PlanMaker.java:155` `query.lowerer(r.temporal(), false, slots, features).lowerStoreWrite(write)` — a store write's DML.
- `planner/PlanMaker.java:163` `query.lowerer(r.temporal(), last, slots, features).lower(r.body())` — a non-write statement (a `Check`, or the last one's `TdsResult`).
- `planner/PlanMaker.java:191` `query.lowerer(false, false, List.of(count.slot()), features).lowerPlanParam(ExecutionPlan.COUNT, type)` — the write count bound as a value.
- Internal: `TypedQuery.java:133`. No driver caller: the driver builds its own `Lowerer` (`driver/BodyDriver.java:2237-2255` and `:641-651`).

### 1g. `writes()` — 2 callers
- `planner/PlanMaker.java:57` `if (query.writes())` → `program(...)` (a `Sequence` plan) else the single lowering.
- `Execution.java:71` `if (typed.writes())` → the write plan on `planSessions` (`:72-75`) else the session the source opens (`:77-88`).

### 1h. The driver's actual reach into TypedQuery
`grep 'typed\.' driver/BodyDriver.java` finds exactly four member calls: `context()` `:56`, `specs()` `:57`, `body()` `:58`, `resolved()` `:59`. Nothing in `com.legend.driver` calls `lower`, `resolve`, `lowerer`, `writes` or names `TypedQuery.Resolved` (grep over `driver/*.java`: zero hits). The "driving of compiler phases through TypedQuery's internals" is, measured, the driver taking the SpecCompiler and the typed body off the query and then re-running G½/H/I/J with its own inliner, resolver, lowerer and renders (section 2).

### 1i. PlanMaker's four public statics
Declared at `planner/PlanMaker.java:49` (`plan`), `:252` (`keyed`), `:298` (`sql`), `:346` (`serversFor`).
- **`plan(TypedQuery, runtime, features, serverVersion)`** — 2 callers, both the door: `TypedQuery.java:215` (`executionPlan(runtime)`), `TypedQuery.java:225` (`executionPlan(runtime, features, serverVersion)`). No driver caller.
- **`keyed(lowered, pivot, schema, dialect, target)`** — 2 external callers, both the driver: `driver/JudgePlans.java:142` (`streamResult`: the body's last statement, a TABULAR side whose plan holds an unnamed dynamic pivot, streamed as a `Keyed` node), `driver/JudgePlans.java:489` (`keyed(...)`: a host-judge side over an unnamed dynamic pivot, run by `PlanRunner.rows` at `:492`). Internal: `PlanMaker.java:87`.
- **`sql(statement, dialect, target)`** — 3 external callers, all the driver: `driver/JudgePlans.java:104` (`rowsPlan`: a judge's or a side's typed-rows statement, `TypedRows.statement(statement)`), `driver/JudgePlans.java:422` (`objects`: a graph fetch side's one-cell statement), `driver/BodyDriver.java:2129` (`keyReader`: the static pivot's keys query as a `VALUE` `TdsResult`). Internal: the private overload at `:234-243` used by `:138`, `:156`, `:166`, `:194`, `:220`, `:227`, `:232`, `:294-295`, `:299`.
- **`serversFor(type, serverVersion)`** — 1 external caller: `driver/JudgePlans.java:71` (a judge's target: the servers narrowed to the body's session version). Internal: `PlanMaker.java:68`, `:112`.

**What this means:** of the seven internals the driver uses two (`specs`, `resolved`); the other five are PlanMaker's and the door's. Of PlanMaker's four statics, `plan` is the door's alone and `keyed`, `sql`, `serversFor` are the driver's alone (JudgePlans and one BodyDriver site).

---

## 2. Where the driver compiles or types during a run

### 2a. BodyCompiler's entry points and their callers
- `driver/BodyCompiler.java:55` `static TypedSide execute(stmts, letPrefix, specs, env0)` → `:57` delegates to
- `driver/BodyCompiler.java:64` `static TypedSide execute(stmts, letPrefix, specs, env0, answer)` — the segment walk (`:66-188`).
- Callers: `driver/BodyDriver.java:93-94` (inside `session().inTransaction` when the body stages rows or writes a store, `:80-96`), `driver/BodyDriver.java:97` (otherwise), `driver/BodyDriver.java:298` (`executeStatements`, `:291-299`, which `driver/AssertErrorNative.java:64` calls for an effectful function's body expanded as a statement sequence in a fresh frame).

### 2b. Every compiler-phase use inside `com.legend.driver` (measured by grep; no `Typer`, no `NameResolver`, no `new SpecCompiler` anywhere in the package — the only "Typer" text is a comment at `driver/AssertVerdicts.java:1494`)

**SpecCompiler (G) — the one from `typed.specs()`; the driver calls two of its methods:** `specs.ctx()` (`driver/AssertVerdicts.java:498`, `driver/DatabaseJudge.java:626`, `driver/HostJudge.java:936`) and `specs.viewRelation(fn)` (`driver/BodyDriver.java:2745`, a view's lifted function typed for the test-data generator). Everything else passes `specs` on to `UserCallInliner`, `StoreResolver`, `StatementEffects`, `ExecuteChainAssembly` (which type helper bodies on demand).

**UserCallInliner (G½) constructions — 14 sites:**
- `driver/BodyDriver.java:349-352` (`prepareValue`: the let prefix + the statement, with the splice hook) — per statement.
- `driver/BodyDriver.java:475-476` (`frameReplaceEnv`: a `TypedUserCall` reached by the rename scan) — per statement.
- `driver/BodyDriver.java:530-531` (`toSqlString`: a runtime argument), `:1214-1215`, `:1225-1226` (`boundContext`: a runtime argument that is a helper call), `:1500-1501` (`buildFrame`: the runtime argument) — per helper argument.
- `driver/BodyDriver.java:629-630` (`engineSql`: the engine-text lowering's body) — per plan-text call.
- `driver/BodyDriver.java:2330-2333` (`inlinedSide`: every assert side and every planned value) — per side.
- `driver/BodyDriver.java:2523` (`prelude`) — per planned statement.
- `driver/BodyCompiler.java:289-291` (an `executeInDb` let as an effect) — per such let.
- `driver/AssertVerdicts.java:665-666` (`UserCallInliner.forVerdictSource(specs, rawHook).reduceVerdictSource(source, letPrefix)`) — per quantified assert.
- `driver/SqlTextVerdicts.java:99-100`, `:912-913` — per SQL-text assert.

**StoreResolver (H) — one factory and 6 uses:**
- Factory `driver/BodyDriver.java:577-587` `resolver(specs, env)`: `new StoreResolver(env.ctx(), specs).withLetBindings(env.queryLets()).withPlanRows(env.planRows()).withHandleRegistrar(...)` — the resolver carries the body's accumulated lets and handle rows.
- Uses: `driver/BodyDriver.java:409-410` (`prepareValue`, Phase H), `:636-637` (`engineSql`, with a mapping and chain mappings), `:1477-1478` (`buildFrame`, a legendQuery chain), `:1530-1531` (`buildFrame`, an execute chain), `:2347` (`stagedSide`), `driver/BodyCompiler.java:292` (an `executeInDb` let). Every use goes through `resolvedToExecute` (`driver/BodyDriver.java:2350-2357`) which adds `CrossStoreGuard.check` (`:2355`).

**Lowerer (I) — 2 constructions:**
- `driver/BodyDriver.java:2231-2255` `lowererFor(body, env, ctx, identity)`: `new Lowerer(layouts, findClass, implementations).withEngineExistsJoinForm().withDbTimeZone(env.timeZone())`, `.withInstanceKeys(...)`, `.withInstanceIds(...)`/`.withObjectIdentity(...).withDeclaredEgress()`, `.withFeatures(featuresOf(env, body))`; used at `:2092-2094` (`lowerAndPrepare`: `lowerer.lower(SeedableLets.withSeedableLetPrefix(body, env.queryLets(), ctx))`) — per planned statement and per planned side.
- `driver/BodyDriver.java:641-651` (`engineSql`): `new Lowerer(...).withFeatures(...)`, `.withEngineExistsJoinForm()`, `lw.withEngineText().lower(body)` — the engine-text lowering for plan-text asserts; callers `driver/PlanAllocations.java:86`, `:233`, `driver/BodyDriver.java:562`, `:850`, `:865`, `:962`, `:987`, `:1111`, `:1302`, `:1457`.
- Also `Compiler.lowerResolved` at `driver/PlanAllocations.java:412` (lineage rows).

**Render (J) — dialect renders inside the driver:**
- `env.dialect().render(...)`: `driver/BodyDriver.java:2148` (a store write's DML), `:2173` (`CreateTempTableAs.staged`), `:2193` (`DropTable` of a staged table), `:2260` (`renderFor`), `:2753` (`viewSqlRenderer`), `:3019`, `:3061-3062`, `:3066-3067`, `:3108`, `:3118` (DDL effects, each also rendered by `REFEREE_DDL` for the ledger), `:3187`, `:3209`, `:3220` (`ENGINE_TEXT` DDL strings); `driver/PlanAllocations.java:118`, `:240`; `driver/PlanEnvelope.java:37`; `driver/CsvLoad.java:70`; `driver/DatabaseJudge.java:724`; `driver/JudgePlans.java:328` (`chars`: the statement's length for the batch).
- `renderer.render(plan)` (engine text): `driver/BodyDriver.java:571`, `:673`.
- `dialect.renderStatement` through `PlanMaker.sql` (`planner/PlanMaker.java:298-299`): `driver/JudgePlans.java:104`, `:422`, `driver/BodyDriver.java:2129`.
- `Render.resolveAllDeferredTds` (a lowering pass taking a probe): `driver/BodyDriver.java:2114-2118`.

### 2c. Per-statement, per-helper, per-body
- **Per body** (once): `driver/BodyDriver.java:56-59` (ctx, specs, the body typed once by `TypedQuery.body()` → `specs.typeQueryBody` at `TypedQuery.java:59`, the resolved AST), `:63-72` (env, listeners, protocol body), `:80-96` (the writes/staged check and the transaction bracket); `driver/BodyCompiler.java:66-70` (segments, `execFrames`, `effectMemo`, `staged`), `:230` (the verdict batch, re-made per segment at `:339`), `:179` (`dropStaged`).
- **Per statement** (the loop `driver/BodyCompiler.java:71-176`): `:72-73` `StatementEffects.containsEffect/containsTdgGenerator` (typing-side classification with `specs`); `:80-84` `TestDataGenerationNatives.foldCensus(...)` (reads the session before planning); `:85` `establishContexts` (seeds through `PlanRunner.setUp`, `driver/BodyDriver.java:1987-1990`); lets `:93-147`; an assert root `:152-163`; a value `:166-175` prepared now (`prepareValue` = G½ at `:349-352` + `NativeDispatch.stage` at `:356-358` + H at `:409-410`) and run at the segment's close (`:325-327` → `runValue` `driver/BodyDriver.java:439-463` → `executeTyped` `:2548-2585`: `prelude` + `LiteralFold` + `lowerAndPrepare` (I) + `wrapSide` + `JudgePlans.streamResult`/`statementResult` → `PlanMaker.sql` (J) → `PlanRunner` (K)).
- **Per helper** (lazy, inside a statement): every `UserCallInliner` site above (`specs` typing a helper body at its first inlining — the F/G separation of AGENTS.md invariant 6 means helper bodies are `ValueSpecification`s until inlined); `buildFrame` for each `execute(...)` / `legendQuery` let (`driver/BodyDriver.java:1394-1613`: `ExecuteChainAssembly.prepare`/`chain` at `:1512-1513`, `:1531-1533`, two resolvers, an eager `executeTyped` at `:1481-1483`, `:1536`); `engineSql` per plan-text call; `planHostSide` per assert side (`:2693-2716`).

### 2d. Why the driver needs the compiler mid-run — each reason with its lines
1. **Lets bound to earlier results.** The let prefix is carried as typed lets and spliced into each statement before inlining (`driver/BodyDriver.java:345-352`: `single = letPrefix + stmt`; `:2326-2333`); `queryLets` accumulate from each inlining (`:353` `env.queryLets().putAll(stmtInliner.queryLets())`, `:2334`) and feed the resolver (`:580`). A frame let's readers are rewritten to the frame's chain by the splice hook (`:1701-1776`, `ResultEnvelopeSplice.hook(...)`) — "the re-plan of a spliced chain is the architecture" (`:465-470`). A staged let is bound to its temp table by `StagedRows.boundTo` (`driver/BodyCompiler.java:114-115`). An alias let shares the frame (`:94-97`).
2. **Helper functions typed lazily.** Every `UserCallInliner` takes `specs` (section 2b); a helper that β-reduces to an assert root is re-classified after inlining (`driver/BodyDriver.java:377-390`); a plan-text assert inlines its producer (`driver/SqlTextVerdicts.java:912-913`).
3. **Deferred TDS text.** The lowering registers `DeferredTdsString`s (`lowering/Lowerer.java:117-122`, filled at `lowering/Render.java:85-92`, `:336-338`); only the driver resolves them, with a session probe (`driver/BodyDriver.java:2108-2118`), because the column list "exists only NOW, post-staticize" (`:2103-2107`); the renderer refuses an unresolved one (`sql/dialect/AnsiSqlRenderer.java:781-784`).
4. **The metamodel store route.** `routeSystemDatabase` (`driver/BodyDriver.java:2039-2062`) switches the env's session to the graph's system database (`withSystemRoute`, `:213-230`) per planned statement (`:2391`, in `prelude`), with renames; a body reading both the store and a user store is refused (`:2048-2052`).
5. **Dynamic shapes read from the session.** `DynamicPivot.staticize` (`:2097-2102`), `WireProbe.reconcile` (`:2808-2812`), `WireProbe.staticized` (`:2839-2841`; `driver/JudgePlans.java:127-128`, `:444-446`), `RawGridSchema.stamp` with the grid referee (`driver/BodyCompiler.java:136-138`, `driver/BodyDriver.java:2394-2395`).
6. **Provisioning before planning.** "a frame's reported wire types are read from the seeded tables" (`driver/BodyCompiler.java:30-34`) and "two facts are read from the live session at plan time (a frame's wire types after seeding, a raw read's schema …)" (`:50-54`); `establishContexts` runs before each statement is planned (`:85`; `driver/BodyDriver.java:1925-1991`).
7. **Effects between statements.** An effect segment is sent as one script before the next non-effect statement is planned (`driver/BodyCompiler.java:45-54`, `:87-92`, `:272-305`, `:308-319` → `driver/BodyDriver.java:2938-2972`); the engine-text arms (`executeInDb` `:2974-2996`, the DDL natives `:3014-3123`) send raw text.
8. **Engine-text plan asserts.** `engineSql` lowers with the engine-text dialect per call (`driver/BodyDriver.java:620-676`) for `SqlTextVerdicts`/`PlanAllocations`.

**What this means:** the driver re-runs G½, H, I and J itself on every statement and every assert side, with the SpecCompiler it took from the query, a resolver and lowerer of its own construction, and renders at 20+ sites; the reasons are state the plan path has no node for (frames, spliced reads, deferred TDS, the system route, effects) plus four facts read from the live session.

---

## 3. The four probes, and the product path beside them

### 3a. ColumnProbe (`driver/ColumnProbe.java:27-59`)
- Call site: `driver/BodyDriver.java:2114-2118` — `Render.resolveAllDeferredTds(plan, lowerer.deferredTds(), inner -> ColumnProbe.probe(inner, fDialect, fSessions, fTarget), WireProbe::pureOfSqlType)` (only when `!lowerer.deferredTds().isEmpty()`, `:2108`). The one caller (grep: `ColumnProbe.` outside the class → `BodyDriver.java:2116` and a test register at `core/src/test/java/com/legend/JavaEvalLedgerTest.java:1416`).
- Statement: `SELECT * FROM (<inner>) _pct_probe LIMIT 0` (`:37-38`), rendered by the dialect, sent as `PlanRunner.columns(ExecutionPlan.Sql.of(dialect.render(probe), target), sessions)` (`:39-40`) → `Statements.columns` — the statement RUN with no row read (`exec/PlanRunner.java:127-129`, `:137-145`).
- Returns: `PlanProbe(outs, typeNames)` (`:58`) — each reported column as an `OutputCol` typed DATE / TIMESTAMP / VARCHAR by `dialect.wireTypeName` (`:45-50`), and a typing name where the dialect's name says the type (`:53-56`).
- Re-entry into planning: a callback into lowering — `lowering/Render.java:391-414` rewrites every `SqlExpr.DeferredTdsString` into concrete projections (`resolveDeferredTds`, `:420+`), each column's Pure type from the registered schema's `pivotColumnType(name)` or else `sqlTypeToPure(p.typeName(name))` (`:404-410`). The rewritten plan then continues to `namedColumns`/render in the driver (`driver/BodyDriver.java:2120`).

### 3b. GridProbe (`driver/GridProbe.java:30-87`)
- Call site: `driver/BodyDriver.java:1369-1372` `gridReferee(env)` returns a `RawGridSchema.SchemaReferee` lambda `(sql, writtenFor) -> GridProbe.probeTypedColumns(sql, writtenFor, env.planSessions(), env.target(), env.dialect())`. The referee is used at `driver/BodyCompiler.java:136-138` (a let whose value contains a raw or catalog grid, stamped at the let) and `driver/BodyDriver.java:2394-2395` (`prelude`: every planned statement and side). `probeNames` (`:36`) has no caller; `probeColumns` (`:74`) is called only by the two methods in the class.
- Statement: `SELECT * FROM (<authored sql>) _p LIMIT 0` as `new SqlSource.RawSql(sql, "_p", List.of(), writtenFor)` (`:76-78`; the one chartered `RawSql` construction, `core/src/test/java/com/legend/RawSqlLedgerTest.java:122` pins it at 1), counted under `StatementOrigin.PROBE` (`:79`), run by `PlanRunner.columns` (`:81-82`).
- Returns: `List<Type.Column>` — compiler types: `dialect.typingWireName(c[1])` → `WireProbe.pureOfSqlTypeOrNull(wire)` (`:57-58`), else `Type.RelationType.trustedColumn(name)` (`:60-61`), multiplicity `[0..1]` (`:64`).
- Re-entry: `resolver/RawGridSchema.stamp(body, referee)` (`resolver/RawGridSchema.java:94`; the resolve/recurse passes at `:152`, `:232`, `:318`) — a Phase-H rewrite that stamps the late-bound relation type onto the typed tree; "the single-query rule of RawGridSchema.stamp decides" (`driver/BodyCompiler.java:133-134`).

### 3c. DynamicPivot (`driver/DynamicPivot.java:38-173`)
- Call site: `driver/BodyDriver.java:2097-2102` in `lowerAndPrepare`, after `SqlPostProcessors.applyRecorded`, with `keyReader(env)` (`:2126-2131`). Test: `core/src/test/java/com/legend/DynamicPivotKeyLiteralH2Test.java:74`, `:84`.
- Statements: (1) the keys query `SELECT DISTINCT <key> AS v FROM <pivot source> WHERE <key> IS NOT NULL ORDER BY v ASC` (`:79-86`), run as typed rows by the `KeyReader` — `driver/BodyDriver.java:2127-2131` builds `new ExecutionPlan.TdsResult(VALUE, columns, PlanMaker.sql(query, env.dialect(), JudgePlans.target(env)))` and runs it with `JudgePlans.run(..., PlanSessions.of(env.session(), env.trace())).rows()`; counted under PROBE (`:100`); (2) when the key column carries no type fact, a LIMIT-0 schema read `SELECT * FROM (<keys query>) _k LIMIT 0` through `PlanRunner.columns` (`:128-140`), refused by name when the database reports no vocabulary kind (`:134-138`).
- Returns: the plan with every `SqlSource.Pivot` whose `in()` is empty rewritten with the keys as typed literals (`:68-70`, the literal arms `:105-121`); untouched where `!dialect.needsStaticPivot()` (`:58-60`).
- Re-entry: a plan edit after lowering and before deferred-TDS resolution and render (`driver/BodyDriver.java:2097-2120`).

### 3d. WireProbe (`driver/WireProbe.java:37-202`)
- `reconcile(plan, shapeInfo, dialect, sessions, target, memo, frames)` (`:96-101` → `:103-157`): call site `driver/BodyDriver.java:2808-2812` (`planStaged`, when the side reads a store and is not the host side), memo `penv.session().state().wireTypes()`, frames the verdict batch's. Statement: the plan with its frame CTEs (`FrameCtes.attach`, `:99`) rendered and DESCRIBED — prepared, never run — through `PlanRunner.describe(ExecutionPlan.Sql.of(sql, target), sessions)` (`:189-200`; `exec/PlanRunner.java:133-135` → `Statements.describe`). Returns the plan with a bare store-column projection re-typed to the wire kind where the declared kind is wire-decided (`Type.wireDecided`: String, Number, Any — `compiler/element/type/Type.java:693-696`) and differs (`:122-156`), the censuses `WIRE_SLOT_SKEW`/`WIRE_RETYPED` (`:139`, `:142`). Re-entry: the retyped `OutputCol`s feed `BarePlan` (`driver/BodyDriver.java:2814-2815`) → `wrapSide` → `JudgePlans.rowsPlan`'s column facts (`driver/JudgePlans.java:91`).
- `staticized(plan, dialect, sessions, target, memo)` (`:69-83`): call sites `driver/BodyDriver.java:2839-2841` (`wrapSide`: a TABULAR side under a canon rider with no outputs and a null or late-bound schema), `driver/JudgePlans.java:127-128` (`streamResult`: the body's last statement when late-bound), `driver/JudgePlans.java:444-446` (`tabular`: a host-judge side when late-bound). Statement: the plan DESCRIBED (`:71-72`). Returns `SELECT * FROM (plan) side` with the reported columns as outputs, VARCHAR where the kind is outside the vocabulary (`:76-82`); the plan itself when nothing is reported (`:73-75`). Re-entry: `VerdictQueries.wireSchema(plan.outputs())` rebuilds the relation schema from the outputs (`driver/JudgePlans.java:129`, `:447`; `driver/BodyDriver.java:2842`) → `rowsPlan` / `CanonicalRenderSql.wrapTdsCanon`.
- `pureOfSqlType` (`:44`) — `driver/BodyDriver.java:2118`; `pureOfSqlTypeOrNull` (`:49`) — `driver/GridProbe.java:58`.
- All four probes reach the database only through `PlanRunner.columns`/`describe` (callers measured: `driver/ColumnProbe.java:39`, `driver/GridProbe.java:81`, `driver/DynamicPivot.java:131`, `driver/WireProbe.java:196`, plus `testdatagen/TestDataGenerator.java:485`, `:973`).

### 3e. The product path (PlanMaker / TypedQuery), fact by fact
- **Dynamic pivot — handled without a session, as a plan node.** `planner/PlanMaker.java:76-88`: `KeyedPivot.dynamic(lowered)` (`:77`); if the row schema has dynamic columns and the shape is TABULAR (`:81-83`) → `keyed(lowered, dynamic, rowSchema, dialect, target)` (`:86-87`). `keyed` (`:252-296`): `dialect.keysSnapshot()` first (`:257`, "a database with no probe plan (MongoDB) refuses by its own name"), `KeyedPivot.of(lowered, pivot, schema)` (`:258`), facts from `PlanColumns.of` (`:260-263`), outputs `Keyed.Fixed`/`Keyed.PerKey` (`:272-285`), snapshot (`:288-293`), `new ExecutionPlan.Keyed(sql(parts.keys()), keyFields, outputs, parts.perKeyAt(), sql(parts.body()), snapshot, dialect.resultColumnLimit())` (`:294-295`). The record: `executionplan/ExecutionPlan.java:210-284` (`Keyed(Sql keys, List<String> keyFields, List<Output> outputs, int perKeyAt, Sql body, Snapshot snapshot, Integer maxColumns)`; its compact constructor refuses a keys probe with a region, a body without one, two targets, a fixed output after a per-key one, no per-key output, a bad `perKeyAt`, a `keyFields` count ≠ per-key outputs + 1, `:211-250`; `Output` `:253`, `Fixed` `:260`, `PerKey` `:270`, `Snapshot` `:283`). The runner: `exec/PlanRunner.java:78` (`case Keyed keyed -> keyed(...)`), `keyed()` `:393-473`: null types checked (`:394-400`), key columns read as VARCHAR text (`:406-410`), `st.inSnapshot(keyed.snapshot(), ...)` (`:413`), the keys probe run as typed rows (`:420`), `maxColumns` refused by name (`:423-426`), the answered columns named by the keys' cells and read as `"_pk" + k + "_" + field` (`:427-443`), the relation's order by `perKeyAt` (`:446-456`), `expand(keyed.body(), keys, checked)` (`:457`; `:497-561`: each region once per key with `lead`/`separator`/`counters`, a `Binding.Key` slot bound to the key's text `:533-536`, `empty` written with no key and `NotImplementedException("a dynamic pivot with no fixed column found no key: its relation has no column")` when `r.empty() == null` `:519-524`), the body run and each cell placed (`:459-467`). Refused on the product path: a `Keyed` inside a `Sequence` — `exec/PlanRunner.java:294-295` and `:361-362` `throw new com.legend.error.NotImplementedException("a keyed result inside a plan of several steps is not run yet")`.
- **A raw SQL grid of unknown schema — no probe, no stamp, no refusal found.** The lowering emits `SqlSelect.starOf(new SqlSource.RawSql(raw.sql(), nextAlias(), gridOutputs(raw.info()), raw.writtenFor()))` (`lowering/Lowerer.java:2386-2388`; `gridOutputs` = `outputsOf(info, DERIVED)`, `:2405-2407`). A late-bound schema has no columns (`compiler/element/type/Type.java:516-519`: `columns().isEmpty() && dynamicColumns().size() == 1 && …`), so the outputs are empty. `RawGridSchema.stamp` has exactly two callers, both the driver (`driver/BodyCompiler.java:136`, `driver/BodyDriver.java:2394`); `GridProbe`/`WireProbe.staticized` have none on the planner side. `PlanMaker.typed` TABULAR builds `PlanColumns.of(TypedRows.schema(root), lowered, RELATION)` (`planner/PlanMaker.java:225-227`; `plan/PlanColumns.java:44-49` maps `schema.columns()` → zero columns for a late-bound schema; `executionplan/ExecutionPlan.java:174-181` refuses a non-RELATION of ≠1 column only). No refusal or default naming a late-bound root was found in `planner/`, `TypedQuery.java`, `Compiler.java` or `plan/PlanColumns.java` (grep `isLateBound|lateBound|trusted` → none). The only late-bound checks outside the driver are the lowering's canon wrap (`lowering/CanonicalRenderSql.java:392-393` declines "tds-canon: late-bound schema") and a fold (`lowering/Fold.java:1057-1059` trusts the column), both driver-side callers.
- **A deferred TDS — refused at render.** `PlanMaker` never calls `Render.resolveAllDeferredTds` (its one caller is `driver/BodyDriver.java:2114`); the registered placeholders reach the dialect, which throws: `sql/dialect/AnsiSqlRenderer.java:781-784` `case SqlExpr.DeferredTdsString d -> throw new IllegalStateException("deferred relation-toString reached the renderer — the execution boundary must resolve the dynamic column list first (DeferredTdsString id " + d.id() + ")")`; `sql/dialect/MongoDb.java:270-272` the same.
- **A wire-decided column type — the compiler's slot stands; neither refused nor probed.** `Type.wireDecided` (`compiler/element/type/Type.java:693-696`) is read by `driver/WireProbe.java:111`, `:124` and `compiler/spec/VerdictQueries.java:1108` only. `PlanMaker` takes each column's slot from the lowering (`ColumnFacts.slot(outs.get(0).type())` `planner/PlanMaker.java:218`, `:281`; `PlanColumns.of` → `ColumnFacts.columns`), with no `describe` and no reconcile (grep `describe|reconcile|wireDecided` in `planner/`, `TypedQuery.java`: none). There is no default branch either: the declared/lowered slot is the fact the plan carries.

**What this means:** the product path has a plan node for exactly one of the four probe shapes (the dynamic pivot, `Keyed`, with its runner); a deferred TDS is refused by the renderer; a late-bound raw grid is neither stamped nor refused (zero columns in the plan); a wire-decided kind is carried as the compiler's slot. The driver's four probes exist because of the three shapes the plan has no node for, plus the pivot's legacy two-phase form on `needsStaticPivot` dialects.

---

## 4. The ExecutionPlan vocabulary and what the runner does per kind

`executionplan/ExecutionPlan.java` (648 lines; 381 stripped):

| Line | Record / variant | One line | Runner (`exec/PlanRunner.java`) |
|---|---|---|---|
| 22 | `ExecutionPlan(List<Parameter> parameters, Node root)` | the plan: declared parameters and a root node; `answerShape()` `:32`, `answerSql()` `:42`, `answerColumns()` `:52` (null for a Keyed) | `stream` `:68-96` checks parameters (`PlanParameters.check`, `:70`) and switches on `root` |
| 89 | `Parameter(name, type, multiplicity, enumValues)` | a declared parameter's validation facts | `PlanParameters.check` (`:70`) |
| 99 | `Multiplicity(lower, upper)` | `[lower..upper]`, null upper is `*` | read by writers/decoders |
| 108 | `sealed interface Node permits Sequence, TdsResult, Keyed, Update, Check, Effect` | the six node kinds | — |
| 118 | `Sequence(List<Node> steps)` | steps in order on one session, one transaction if any changes; answer = its one result step | `:79-87` → `sequence()` `:270-328` (one target else `NotImplementedException` `:306-308`; `openToChange` if changes `:317`; `st.inTransaction` → `steps()` `:331-367`; held rows streamed after commit `:82-86`) |
| 129 | `COUNT = "#count"` | the name a write's count is bound by | `steps()` `:341-343` binds it |
| 138 | `Update(Sql sql, String count)` | a change (store write, staged table made/dropped); its count | as a root: refused `:88-89`; in a sequence `:277-284` / `:336-344` (`st.update`); standalone `update(Update, sessions)` `:103-108` (the body driver's one change) |
| 150 | `Effect(Sql sql)` | a statement sent for what it does (raw text, DDL, script, seed rows) | as a root: refused `:92-93`; in a sequence `:286-289` / `:350-354` (`st.execute`); standalone `effect(Effect, sessions)` `:112-119` (slots refused `:113-115`) |
| 158 | `Check(Sql sql)` | a query run for the error it may raise, rows dropped | as a root: refused `:90-91`; in a sequence `:285` / `:345-349` (`st.query(..., row -> {})`) |
| 173 | `TdsResult(Shape shape, List<Column> columns, Sql sql)` | typed rows: RELATION / VALUE / OBJECTS (`Shape` `:191`); non-RELATION must be one column `:178-181` | `:72-77` (`send` → `st.rows(statement, slots, bound, columns, streamed::row)`); in a sequence `:290-293` / `:355-360` (held) |
| 210 | `Keyed(Sql keys, List<String> keyFields, List<Output> outputs, int perKeyAt, Sql body, Snapshot snapshot, Integer maxColumns)` | a dynamic pivot's probe plan (keys first, body with per-key regions) | `:78` → `keyed()` `:393-473`; inside a sequence refused `:294-295`, `:361-362` |
| 253 / 260 / 270 | `Keyed.Output` / `Fixed(column, field)` / `PerKey(column, int name, field)` | a body output: fixed once, or once per key (named by the keys row's cell `name`) | `:427-443` |
| 283 | `Keyed.Snapshot {TRANSACTION, REPEATABLE_READ, SERIALIZABLE, NONE}` | how the two statements see one state | `st.inSnapshot(keyed.snapshot(), ...)` `:413` |
| 297 | `Column(name, type, multiplicity, slot, store, precision)` | a typed column's facts (Pure type, slot, store type, precision); `taggedSlotColumn` `:311` | handed to `Statements.rows` for decode; passed through to the sink |
| 321 | `enum Precision {WRITTEN, DAY, INSTANT, MINIMAL}` | how a temporal cell's written precision is known | read by the writers (`result/CellText`), not the runner |
| 351 | `Sql(statement, slots, target, tree, regions)`; `Sql.of(statement, target)` `:379` | one statement final for its target; `Region.check` and type-hole order checked `:357-371` | `send()` `:590-611`; `statement()` `:656-671` fills type holes; `columns`/`describe` `:127-145` (slots refused `:139-141`) |
| 394 | `Region(start, end, lead, separator, empty, firstSlot, slotCount, counters)` | a per-key run of text in a keyed body | `expand()` `:497-561` |
| 443 | `Slot(parameter, Binding binding)` | a placeholder and how it binds | `valuesOf`/`value` `:378-389`, `:644-653` (an undeclared parameter refused `:647-649`) |
| 451 / 456 / 464 / 472 | `Binding` / `One(nullType, hole)` / `Array(elementSqlType)` / `Key(cell)` | one value (with a type hole), a whole list as an array, a key's cell | `JdbcStatements.nullType` via `nullTypes()` `:369-376` and `send()` `:592-596`; `Key` bound in `expand()` `:533-536` |
| 486 | `TypeHole(at, Map<ValueKind, TypeSpelling> types, ValueKind absent)` | where the value's type is written into the text | `typeOf()` `:673-701`, `spelling()` `:703-711` |
| 501 | `enum ValueKind {INTEGER, DECIMAL, FLOATING, DATE, DATE_TIME, DATE_TIME_NANOS}` | a value-typed parameter's kinds | `kindOf()` `:726-736` |
| 519 / 534 | `TypeSpelling(name, Digits digits, fraction)` / `enum Digits` | a type's spelling for one kind | `typeOf()` `:685-690`, `dateTimeText()` `:707-713` |
| 545 | `Target(Database database, Servers servers, List<String> session, List<SetupStep> setup)` | where a statement runs, its server versions, the session statements, the setup steps | `opened()` `:622-642` (`st.check(target)`, session statements once per connection `:625-640`); `setUp()` `:164-190` |
| 555 / 562 / 575 | `Database` / `Declared(connection)` / `Platform(type)` | a declared connection's database or the platform engine | `st.check(target)` `:624`; `PlanSessions` opens by it |
| 583 / 586 / 590 | `Servers` / `Every()` / `Versions(prefixes)` | which server versions the statements are written for | `st.check(target)` `:624` |
| 601 / 604 / 620 | `SetupStep` / `Statement(sql)` / `Rows(stagingTable, createStaging, copy, dropStaging, rows)` | a setup statement, or rows for the bulk loader | `setUp()` `:167-186`: `Statement` → `effect(...)` `:168-174`; `Rows` → `st.load(r)` else refused `:175-184` |

**What this means:** the plan vocabulary has six node kinds and one answer shape per plan; the runner has a switch arm for each (the Keyed-in-Sequence arm is a named refusal), and three standalone entries the body driver uses for single statements (`update`, `effect`, `columns`/`describe`, `setUp`).

---

## 5. The body driver's statement loop

### 5a. From entry to the last statement (methods in order, with line ranges)
1. `Execution.runProgram(typed, runtimeFqn, sessions, listener, referee, options, answer)` `Execution.java:145-153` opens the session for `Compiler.executesOn(ctx, runtimeFqn)` (`:150`) → package-private `runProgram(..., session, ...)` `:180-187`: `dialectOf` (`:121-130`: `Sessions.check(session, decided.type())`, `Databases.dialect(type).forServer(version)`) then `BodyDriver.execute(typed, runtimeFqn, dialect, session, listener, referee, options, answer)` (`:186`).
2. `BodyDriver.execute` `driver/BodyDriver.java:49-98`: `:56-59` ctx/specs/typedBody/resolved off the query; `:63-66` `ExecEnv` with options and listeners; `:67-72` the protocol body; `:76-96` if any statement stages rows or writes a store → `refuseLetsTheWritesChange` and `session().inTransaction(() -> BodyCompiler.execute(...))`; else `:97` `BodyCompiler.execute(typedBody, new ArrayList<>(), specs, env, answer)`.
3. `BodyCompiler.execute` `driver/BodyCompiler.java:64-188`: `Segments` `:66` (`:206-342`), `execFrames` `:67`, `effectMemo` `:68`, `staged` `:70`; the loop `:71-176`:
   - `:72-73` effect/generator classification; `:74-77` a generator closes both segments first;
   - `:80-84` `foldCensus` (TDG, reads the session); `:85` `establishContexts` (seeds);
   - `:87-91` an effect statement → `closeVerdicts()`, `collectEffect(...)` (`:272-305`);
   - `:92` `closeEffects()` (the pending script sent, `:308-319` → `BodyDriver.sendScript` `:2938-2972`);
   - `:93-147` a non-last let: alias frame `:94-98`; staged let `:103-117` (`prepareValue(...).stagedInto(table)`, `StagedRows.boundTo` onto `letPrefix`); frame let `:118-128` (`buildFrame`, `execFrames.put`); raw-grid let `:135-143` (`RawGridSchema.stamp` with `gridReferee`); handle/value let `:145-147` (`PlanAllocations.registerHandlesIn`, `letPrefix.add(let)`);
   - `:152-163` a statement root claimed by `AssertVerdicts.tryAdjudicate(bare, letPrefix, specs, frameReplaceEnv(...), spliceHook(...))` → `seg.verdict(v)`;
   - `:166-175` otherwise a value statement: `prepareValue(stmt, bare, letPrefix, execFrames, specs, env)` (G½ + staging + H) → `seg.value(pv, last)` (host judge: run now `:258-260`; database judge: deferred to the segment close `:261-266`).
   - `:177-178` close both segments; `:179` `dropStaged`; `:180-187` a verdict as the last statement written as host rows (`JudgePlans.host`).
4. `Segments.closeVerdicts` `driver/BodyCompiler.java:324-341`: the prepared values run in order (`run` `:235-241` → `BodyDriver.answerValue` `:423-436` for the body's answer, else `BodyDriver.runValue` `:439-463` → `executeTyped` `:2548-2585`), then `AssertVerdicts.flush(batch, env())` (`:332` → `driver/AssertVerdicts.java:138-140` → `VerdictBatch.flush(listener)` with the runner from `JudgePlans.runner` `driver/JudgePlans.java:318-339`), then a fresh batch (`:339`).

### 5b. State carried across statements
- In `BodyCompiler.execute`: `letPrefix` (`List<TypedSpec>`, grown at `:114-115`, `:146`); `execFrames` (`Map<String, ExecFrame>`, `:67`, `:96`, `:124`); `effectMemo` (`:68`); `staged` (`:70`). In `Segments`: `batch` (`:213`), `values` (`:214`), `sink` (`EffectSink`, `:215`), `effectFrom`/`effectTo` (`:216-217`), `last` (`:218`), `pendingVerdict` (`:219`), `lastIsValue` (`:220`), `answers` (`:224`), `fragments` (`:209`).
- `ExecEnv` (`driver/BodyDriver.java:110-126`): `ctx`, `runtimeFqn`, `dialect`, `session`, `queryLets` (a mutable map filled by every inlining: `:353`, `:2334`), `tableReplace`, `instanceIds`, `assertListener`, `replayReferee`, `planRows` (mutable, the handle rows: `PlanAllocations`), `protocolBody`, `frame` (the bound `ExecutionContext`), `options`, `trace`, `verdictBatch`, `effectSink`; derived `planSessions()` `:200-202`, `target()` `:205-207` (= `JudgePlans.target(env)`), `withSystemRoute` `:213-230` (switches the session, adds renames).
- The session's own state: `SessionState.established(key)` / `establish` (`:1961-1963`, `:1991`: seeds once per session per seed-source key), `state().wireTypes()` memo (`:2811`, `:2840`), `markWriting()` (`driver/BodyCompiler.java:274`), the session-statements memo in the runner (`exec/PlanRunner.java:627-639`).

### 5c. Where verdicts happen
- Router: `driver/AssertVerdicts.java:51-99` `tryAdjudicate` → `batched` (`:102-136`, database judge: `batch.open/close/resolve/discard`) or `adjudicate` (`:161`, host judge, listener told at `:76`, `:91`).
- Host judge arms: `driver/HostJudge.java` (`eq` `:869`, `equals` `:1021`, `sameElements` `:948`, `size` `:675`, `contains` `:709`, `tolerance` `:727`, `condition` `:742`, `empty` `:780`, `instanceOf` `:897`, `is` `:925`, `quantified` `:992`, `cellPool` `:1006`, `jsonStringsEqual` `:1078`, `rendered` `:1128`, `tdsRowValuesVerdict` `:81`, `tdsEquivalent` `:644`); each side through `JudgePlans.evalSide` (`driver/JudgePlans.java:341-364`) → `BodyDriver.planHostSide` (`driver/BodyDriver.java:2693-2716`) → `JudgePlans.sideResult` (`:366`) → `tabular`/`keyed`/`values` (`:436-604`) → `PlanRunner.rows` via `JudgePlans.run` (`:308-315`).
- Database judge arms: `driver/DatabaseJudge.java` (`databaseVerdict` `:41`/`:52-210`, `runVerdict` `:211`, `verdictOf` `:227`, `planSide` `:260-305`, `databaseSize` `:317`, `databaseEmpty` `:337`, `databaseContains` `:351`, `databaseTdsEquivalent` `:376`, `databaseCondition` `:419`, `databaseTolerance` `:446`, `jsonStringsEqual` `:682`); the verdict statement is composed by `VerdictSql` and sent by the batch's runner (`driver/BodyDriver.java:304-308` `newVerdictBatch`; `driver/JudgePlans.java:318-339`).
- Both judges: `driver/AgreementArm.java:135-270` (`ExecuteOptions.JudgeMode.BOTH`); `driver/VerdictArm.java`.
- SQL-text asserts: `driver/SqlTextVerdicts.java:50` `tryArm`, `:235`, `:307`, `:959`.

### 5d. Where the probes and the compiler are called from within the loop
- From `prepareValue` (`driver/BodyDriver.java:343-420`, called at `driver/BodyCompiler.java:110`, `:166`, `:298`): G½ `:349-352`, staging `:356-358`, H `:409-410`.
- From `executeTyped` → `prelude` (`:2385-2535`, via `:2552`): `routeSystemDatabase` `:2391`, `RawGridSchema.stamp(body, gridReferee(env))` `:2394-2395` (**GridProbe**), the from's frame `:2399-2420`; then `lowerAndPrepare` (`:2089-2121`, via `:2568`): `lowererFor` + `lower` `:2092-2094`, **DynamicPivot.staticize** `:2097-2102`, **ColumnProbe** inside `Render.resolveAllDeferredTds` `:2108-2118`.
- From `planHostSide` → `planStaged` (`:2776-2818`): **WireProbe.reconcile** `:2808-2812`; `wrapSide` (`:2820-2875`): **WireProbe.staticized** `:2839-2841`.
- From `JudgePlans.streamResult` (`driver/JudgePlans.java:116-158`): **WireProbe.staticized** `:127`, `PlanMaker.keyed` `:142`, `rowsPlan` `:144`; from `JudgePlans.tabular` (`:436-482`): **WireProbe.staticized** `:444`, `keyed` `:455` → `PlanMaker.keyed` `:489`.
- From the let branch: `buildFrame` (`driver/BodyCompiler.java:125` → `driver/BodyDriver.java:1394-1613`) runs two resolvers and an eager `executeTyped`; `RawGridSchema.stamp` at the let (`driver/BodyCompiler.java:136-138`, **GridProbe**).

### 5e. Sizes (comment-stripped: block and line comments removed, blank lines removed; raw total in parentheses)
| Class | Stripped | Raw | Ledger pin |
|---|---|---|---|
| `driver/BodyDriver.java` | 2321 | 3297 | `core/src/test/java/com/legend/JavaEvalLedgerTest.java:798` pins 2321 |
| `driver/BodyCompiler.java` | 247 | 353 | — |
| `driver/JudgePlans.java` | 460 | 605 | — |
| `driver/HostJudge.java` | 804 | 1156 | `JavaEvalLedgerTest.java:570` pins 804 |
| `driver/DatabaseJudge.java` | 595 | 750 | `JavaEvalLedgerTest.java:571` pins 595 |
| `driver/AssertVerdicts.java` | 1259 | 1717 | `JavaEvalLedgerTest.java:574` pins 1259 |
| `driver/SqlTextVerdicts.java` | 1240 | 1701 | `JavaEvalLedgerTest.java:978` pins 1240 |
| `planner/PlanMaker.java` | 403 | 551 | — |
| `TypedQuery.java` | 152 | 238 | — |
| (for reference) `Execution.java` 96 / 188; `exec/PlanRunner.java` 559 / 737; `executionplan/ExecutionPlan.java` 381 / 647; `driver/ColumnProbe.java` 41 / 60; `driver/GridProbe.java` 52 / 87 (pinned `:246`); `driver/DynamicPivot.java` 118 / 173 (pinned `:217`); `driver/WireProbe.java` 140 / 202; `driver/PlanAllocations.java` 392 / 488; `driver/AgreementArm.java` 223 / 274 | | | |

**What this means:** one loop (`BodyCompiler.execute`, 247 stripped lines) drives a 2321-line BodyDriver plus ~4,350 stripped lines of judges; every statement and every assert side passes through `prepareValue`/`prelude`/`lowerAndPrepare`, where the four probes and the three compiler phases are called.

---

## 6. Visibility facts

### 6a. The five doors: public members that are machinery-only (callers measured by exact greps)
**`TypedQuery.java`**
- `context()` `:36` — `driver/BodyDriver.java:56`, `planner/PlanMaker.java:108`, `Execution.java:77`, `:149`, `:184`. No product (`server/PureV1Api.java`) caller.
- `specs()` `:45`, `resolved()` `:51`, `body()` `:56` (`driver/BodyDriver.java:58`, `planner/PlanMaker.java:118`), `lower` 4-arg `:130`, `Resolved` `:139`, `resolve` `:148`, `lowerer` `:165` — machinery only (section 1).
- `parameters()` `:78` — `planner/PlanMaker.java:51` only in main.
- `writes()` `:189` — `planner/PlanMaker.java:57`, `Execution.java:71`.
- `planStreaming` `:207` — zero callers (main and test).
- `executionPlan(runtime)` `:214` — `Execution.java:72`; test `core/src/test/java/com/legend/OutputMatrixTest.java:121`; `executionPlan(runtime, features, serverVersion)` `:223` — `Execution.java:79`; `pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java:39`.
- Product API (callers in `server/PureV1Api.java`): `target()` `:101` (`:388`, `:421`, `:452`, `:784`), `resultType()` `:69` (`:478`, `:567`, `:569`, `:580`), `plan(runtime)` `:202` (`:390`); `expression()` `:95` and `lower(runtime, streaming)` `:123` have test callers only (`core/src/test/java/com/legend/lowering/LowerRelationDuckDbTest.java:91` etc.; `integration/CompileLatency.java:183`, `integration/DeclaredQuotedTableH2Test.java:52`, `:126`).

**`Compiler.java`** (public members at `:37-902`)
- Machinery-only: `LoweredQuery` `:567` (`planner/PlanMaker.java:64`, `:417`; `TypedQuery.java:123-133`); `executesOn` `:626` (`planner/PlanMaker.java:66`, `:109`; `driver/JudgePlans.java:57`; `driver/BodyDriver.java:2060`; `Execution.java:78`, `:124`, `:150`; `TypedQuery.java:155`; `server/ConnectionResolver.java:52`); `queryResolved` `:599` (`test/PureTestRunner.java:356`, `:475`; `test/ServiceTestRunner.java:244`); `lowerResolved` `:799`/`:810` (`driver/PlanAllocations.java:412`); `resolveQuery` `:677`, `hasStatementEffects` `:721`, `programFacts` `:729` (the test runners); `Target` `:576` (`TypedQuery.java:101-117`; product `server/PureV1Api.java:388`, `:1035`).
- Test-only callers: `parseSources` `:151`/`:163`/`:172`, `buildModel` `:248`, `BuiltModule` `:509`, `buildModule` `:532`, `compileAllBodies` `:849`, `ModelSource` `:121`, `ParsedModule` `:134`.
- `dialectOf` `:604` is package-private (used by `TypedQuery.java:230`).

**`Execution.java`**: `runProgram` `:145`/`:158` — `test/PureTestRunner.java:356`, `:475`; `test/ServiceTestRunner.java:244`; `pct/.../channelb/ChannelB.java:281`; `core/src/test/java/com/legend/testing/Program.java:94`; `core/src/test/java/com/legend/JudgesOnPlansTest.java`. `Answer` `:170` — `driver/BodyCompiler.java`, `driver/JudgePlans.java`, `driver/BodyDriver.java`, `server/PureV1Api.java`, `queries/SavedQueries.java`. `Source` `:32` — implemented by `server/ConnectionResolver.java` and `Source.given` at the test runners above. Package-private: `dialectOf` `:121`, `runProgram(…, session, …)` `:180`.

**`ExecuteOptions.java`**: `withFeatures` `:54` — `driver/BodyDriver.java`; `withSystemConnections` `:60` — `driver/SystemDatabase.java`; `withJudgeMode` `:47` — `test/PureTestRunner.java`; `recording` `:69` — tests only; `JudgeMode` `:36` — `driver/AssertVerdicts.java`, `test/PureTestRunner.java`.

**`HeldModels.java`**: `with` `:49`, `size` `:64` — `server/PureV1Api.java`; `hold` `:36`, `release` `:41`, `withText` `:59` — tests only.

### 6b. `planner/` public members and callers
- `PlanMaker.plan` `:49` → `TypedQuery.java:215`, `:225`; `keyed` `:252` → `driver/JudgePlans.java:142`, `:489`; `sql` `:298` → `driver/JudgePlans.java:104`, `:422`, `driver/BodyDriver.java:2129`; `serversFor` `:346` → `driver/JudgePlans.java:71`.
- `CrossStoreGuard.check` `:29` → `TypedQuery.java:156`, `driver/BodyDriver.java:2355`.
- `MetamodelSeeds.rows` `:37` → `planner/PlanMaker.java:433`, `driver/BodyDriver.java:2061`, `planner/OpSeeds.java`.
- `QueryParameters.Declared` `:28` / `of` `:150` → `planner/PlanMaker.java`, `TypedQuery.java`, `driver/BodyDriver.java`, `driver/PlanAllocations.java`.
- `ProgramFacts` `:19`, `shape` `:29` → `Compiler.programFacts`, `test/PureTestRunner.java`, `judge/AssertListener.java`.
- `package-info.java:5-15`: "this package is theirs and the driver's… nothing here depends on com.legend.driver".

### 6c. `driver/` public members and callers
- `BodyDriver.execute` `:49` (public static) → `Execution.java:186` only (`ArchitectureTest.theDriverHasOneCaller`, `core/src/test/java/com/legend/ArchitectureTest.java:550-558`); `registeredEffectKeys` `:1803`, `registeredRoutineKeys` `:1809` → tests only.
- `ColumnProbe.probe` `:35` → `driver/BodyDriver.java:2116`. `GridProbe.probeNames` `:36` → none; `probeTypedColumns` `:50` → `driver/BodyDriver.java:1370`; `probeColumns` `:74` → in-class only. `DynamicPivot.KeyReader` `:47`, `staticize` `:55` → `driver/BodyDriver.java:2097`, `:2126`; test `DynamicPivotKeyLiteralH2Test.java:74`, `:84`. `WireProbe.pureOfSqlType` `:44` → `driver/BodyDriver.java:2118`; `pureOfSqlTypeOrNull` `:49` → `driver/GridProbe.java:58`, test `sql/dialect/WireTypeNamesTest.java:26`; `staticized` `:69` → `driver/JudgePlans.java:127`, `:444`, `driver/BodyDriver.java:2839`, test `WireProbeTest.java:91`; `reconcile` `:88`/`:96` → `driver/BodyDriver.java:2808`, test `WireProbeTest.java:65-95`.
- `SystemDatabase.Route` `:95`, `of` `:110`, `routeFor` `:125` → `driver/BodyDriver.java:213`, `:2059-2060`.
- `AgreementArm` public arms `:135-270` → `driver/AssertVerdicts.java`, `driver/HostJudge.java` only. `ConnectionLets.onlyConnectionReads` `:32` → `driver/BodyCompiler.java:283`.
- `JudgePlans` is a package-private class (`driver/JudgePlans.java:42` `final class JudgePlans`); `HostJudge`, `DatabaseJudge`, `AssertVerdicts`, `SqlTextVerdicts`, `PlanAllocations`, `BodyCompiler` members are package-private (section 5's method map; no `public` member besides the ones listed).

### 6d. Bazel targets and the layers file
- **`exec`** `core/BUILD.bazel:201-212`: `srcs = glob(["src/main/java/com/legend/exec/*.java"])`, `deps = ["//base", "//json", ":cache", ":diagnostics", ":error", ":execution_plan", ":model", ":result", ":sql", ":values"]`. `tools/deps/core-layers.txt:98`: `exec	base cache diagnostics error execution_plan json model result sql values`.
- **`judge`** `core/BUILD.bazel:215-219`: deps `//base, :compiler, :compiler_element_type, :diagnostics, :error, :exec, :lowering, :sql, :sql_dialect, :values`; layers `:99`.
- **`planner`** `core/BUILD.bazel:238-257`: `srcs = _PLANNER_DOORS (Compiler.java, HeldModels.java, TypedQuery.java) + glob(planner/*.java)`, `visibility = ["//datacube:__pkg__"]`, `deps = ["//base", ":builtin", ":cache", ":compiler", ":compiler_element_type", ":database", ":door", ":error", ":execution_plan", ":lineage", ":lowering", ":model", ":normalizer", ":parser", ":plan", ":platform", ":protocol", ":resolver", ":setup", ":sql", ":sql_dialect", ":validation", ":values"]`. Layers `:107`: `planner	base builtin cache compiler compiler_element_type database door error execution_plan lineage lowering model normalizer parser plan platform protocol resolver setup sql sql_dialect validation values`.
- **`driver`** `core/BUILD.bazel:259-271`: `srcs = glob(["src/main/java/com/legend/*.java", "src/main/java/com/legend/driver/*.java"], exclude = _PLANNER_DOORS)`, `deps = ["//base", "//json", ":builtin", ":cache", ":compiler", ":compiler_element_type", ":database", ":diagnostics", ":error", ":exec", ":execution_plan", ":judge", ":lineage", ":lowering", ":model", ":normalizer", ":parser", ":plan", ":planner", ":platform", ":protocol", ":resolver", ":result", ":setup", ":sql", ":sql_dialect", ":testdatagen", ":validation", ":values"]`. Layers `:111`: `driver	base builtin cache compiler compiler_element_type database diagnostics error exec execution_plan json judge lineage lowering model normalizer parser plan planner platform protocol resolver result setup sql sql_dialect testdatagen validation values`.
- `plan_side` `core/BUILD.bazel:326-332` exports `_PLAN_SIDE_TARGETS` (`:320-324`): everything up to `:planner` and `:pure_v1`, none of `:exec`, `:testdatagen`, `:driver`.
- Class-level rules: `ArchitectureTest.theDriverHasOneCaller` (`core/src/test/java/com/legend/ArchitectureTest.java:550-558`), `plannerIsExecutionFree` (`:567-577`: planner + the three doors depend on nothing in `com.legend.driver`, `com.legend.exec..`, `com.legend.judge`), `execReachesNoPlanning` (`:1008-1023`, `EXEC_MAY_REACH`), `theBodyDriverAndTheJudgesHoldNoJdbc` (`:1193-1208`), `theRunnerIsTheOneCallerOfTheSessionsStatements` (`:1224`), `hostVerdictIsReachableOnlyFromTheVerdictSeam` (`:1311-1345`), `stageFunnelOnlyTheDriverDrivesResolverLoweringExec` (`:593`). Closed registers: `JavaEvalLedgerTest.PLANNER_CLASSES` (`:1383`), `DRIVER_CLASSES` (`:1398`), checked at `:1470-1471`.

**What this means:** the driver's public surface that is not product API is nine members across BodyDriver, the four probes and SystemDatabase, each with one to three callers inside `driver/`; TypedQuery's seven internals have exactly the callers of section 1; the planner target carries no exec/driver/judge edge and the driver target carries every planning library plus exec and judge.

---

## 7. Tests that pin the driver's behaviour per statement kind, and their lanes

### 7a. Lanes (how each reaches the code)
- **core lanes** `core/BUILD.bazel:647-660`: `junit_test(name = "core_tests_" + p, select = ["--select-package=com.legend." + p], …)` for each `p` in `_CORE_TEST_PACKAGES` (`:597-630`: includes `driver`, `exec`, `integration`, `judge`, `planner`, `testing`, …); `core_tests_root` `:695-706` (`com.legend` minus every listed package: the root-package tests); `core_tests_integration_sqlite` / `_h2` `:726-742` (the LEAN twin's lanes, `-Dlegend.test.lane=`), suite `core_tests_lanes` `:744-747`. Gate `//gates:core` `gates/BUILD.bazel:19-65` runs `//core:core_tests` (`:24`) and `//core:core_tests_lanes` (`:26`).
- Integration tests run a BODY through `core/src/test/java/com/legend/testing/Program.java:94` (`Execution.runProgram(Compiler.queryResolved(ctx, resolved), runtime, Source.given(TestSessions.over(c)), listener, …)`) and a QUERY through the product route (`testing/Lean.java:25-35`: `Execution.run`, the plan runner, the JSON rows writer).
- **The corpus** `//gates:corpus_duckdb` (`gates/BUILD.bazel:131-132` → `//spec:corpus_duckdb`, `spec/BUILD.bazel:179`), `//gates:corpus_h2` (`:137-138` → `spec/BUILD.bazel:221`): `spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:345`, `:741` → `core/src/main/java/com/legend/test/PureTestRunner.java:356`, `:475` → `Execution.runProgram` (the body driver).
- **Channel B** `//gates:pct_channel_b` (`gates/BUILD.bazel:161-162` → `//pct:pct_channel_b`, `pct/BUILD.bazel:470`): `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:281` → `Execution.runProgram` (the body driver).
- **PCT main lanes** `//gates:pct_duckdb`, `pct_h2`, `pct_postgres` (`gates/BUILD.bazel:143-156` → `pct/BUILD.bazel:163`, `:176`, `:219`) and the heavy lanes (`gates/BUILD.bazel:355-362`: sqlite, mariadb, mysql, trino, clickhouse, databricks, snowflake, bigquery): the PRODUCT path, not the driver — `pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java:36-39` (`Compiler.query(...)`, `query.executionPlan(StorelessRuntime.RUNTIME, Set.of(CORRECT_SQL_SUBSTRING_INDEXING), PctBackend.serverVersion())`, then `PlanRunner.stream`), called from `PctExecuteNative.java:153`. No `runProgram`/`PureTestRunner` reference exists under `pct/src` outside `channelb/` and `PctPlanRoute` (grep).
- **Guards** (root lane, `core_tests_root`): `ArchitectureTest` (rules in 6d), `JavaEvalLedgerTest` (the per-file pins in 5e; the probe register at `:1416-1417`), `RawSqlLedgerTest:122` (GridProbe's one `RawSql`), `JdbcSurfaceCensusTest`, `DialectBoundaryTest:131`, `CodeShapeGuardrailTest:679` (tag `guardrail`: excluded from the lanes by `_CORE_TEST_EXCLUDED_TAGS` `core/BUILD.bazel:638-645`, run by `//core:guardrails` in `//gates:checks`, `gates/BUILD.bazel:113`).

### 7b. Per statement kind
- **A let (frame, alias, handle, effectful):** `core/src/test/java/com/legend/exec/ExecuteInDbDuckDbTest.java:168` `effectfulLetExecutesOnce`, `:182` `effectfulLetReadIsLoud`, `:197` `queryShapedLetReadsBack` (lane `core_tests_exec`); `exec/ExecuteInDbProbeCountTest.java:96` `undemandedGridSingleQuery`, `:114` `namedReadProbesOnce`, `:133` `columnNamesDemandsProbe` (exec).
- **A staged let (`stage()`):** `integration/StagedRowsTest.java:50` `aStagedLetHoldsItsRowsAcrossTheWritesAfterIt`, `:75` `aDeleteOfTheRowsAnotherRelationNames` (lanes `core_tests_integration`, `_sqlite`, `_h2`); the product path's twin `integration/WritePlansTest.java:56` `aProgramThatWritesIsPlannedAsItsSteps`, `integration/WritePlansH2Test.java`, `integration/CypherWritePlansTest.java`.
- **An effect (`executeInDb`, DDL natives, scripts):** `exec/ExecuteInDbDuckDbTest.java:65` `multiStatementBlobWithKeywordColumns`, `:85` `ownDatabaseTextRunsAsWritten`, `:108` `sqlArgumentEvaluatedThroughPipeline`, `:151` `effectfulSetupFunctionSequences`, `:208` `dropAndCreateFromStoreModel`, `:236` `brokenStatementFailsLoudly` (exec). No core test references `EffectSink`/`sendScript` directly (grep: `JavaEvalLedgerTest` only); the segment script's oracle is the corpus and channel B.
- **An assert (both judges, the batch, the arms):** `core/src/test/java/com/legend/JudgesOnPlansTest.java:102` `everyBodyHoldsUnderBothJudges_onThePlatform`, `:167` `aFailingAssertFailsUnderBothJudges_withTheHostsMessage` (root); `AssertVerdictsTest.java:73-153` (`assertEqualsVerdict`, `nativeAssertVerdict`, `assertInstanceOfVerdict`, `assertTdsEquivalentVerdict`, `enumUnderAnyDeclinesToHost`, `quantifiedVerdict`; root); `driver/AgreementArmTest.java:76-134` and `driver/ConstantPlanParityTest.java:55-83` (lane `core_tests_driver`); `judge/*Test.java` (`CanonicalFormTest`, `EqualityWorldsConformanceTest`, `PureAssertsTest`, `VerdictWorld2ConsistencyTest`, …; lane `core_tests_judge`); `VerdictChannelRegisterTest`, `SqlTextRatchetTest` (root); the corpus's `JudgeLedger` (`spec/src/test/java/com/legend/rcorpus/JudgeLedger.java`).
- **A dynamic pivot:** driver side — `core/src/test/java/com/legend/DynamicPivotKeyLiteralH2Test.java:94` `floatKeyRoundTrips`, `:108` `timestampKeyKeepsSeconds`, `:122` `subsecondKeyKeepsFraction` (root); `exec/ExecutorTest.java:119` `pivotDynamicColumnsInheritTemplateType` (exec); `integration/PivotCheckerTest.java:50-419`, `integration/PivotJoinColumnTest.java` (integration); product side — `planner/PlanMakerTest.java:332` `aDynamicPivotsPlanReadsItsKeysFirst_everyOutputEveryDatabase`, `:370` `aDynamicPivotsProbePlanIsMadeOnEverySqlDatabase`, `:389` `aDynamicPivotsProbePlanAnswersForTheKeysItsRunFinds`, `:421`, `:455` (lane `core_tests_planner`); `planner/TypedPlanTest.java:111`; `testcases/PlanRoundTrip.java:61-88` (the `Keyed` JSON round trip).
- **A raw grid (`executeInDb` read):** `exec/RawGridSchemaDuckDbTest.java:69-191` (`helperConnectionDeclaresItsDatabase`, `disagreeingOverloadsAreRefused`, `aTypeThatIsNotALiteralIsItsDatabase`, `theStatementFormReadsTheSameDatabase`, `anUnreadableTypeIsRefused`, `columnNamesComposes`, `rowsValueAutoMaps`, `rowMajorCellRead`, `foldColumnCollect`, `shadowedBinderIsNotResolved`), `exec/ExecuteInDbProbeCountTest.java:96-133`, `exec/ExecuteInDbDuckDbTest.java:244` `classQueryDerivedSqlResolves` (exec); `core/src/test/java/com/legend/WireProbeTest.java:65-95` (root; `staticized` at `:91`).
- **A deferred TDS:** no test under `core/src/test/java` references `DeferredTds`, `deferredTds` or `resolveAllDeferredTds` (grep: the only hit is a comment at `JavaEvalLedgerTest.java:581`). Its oracle is the corpus and channel B lanes alone.
- **The metamodel store route:** `integration/MetamodelStoreTest.java:61-119` (`classExtentIsNonEmpty`, `classExtentIsEmptyIsFalse`, `filterByNameOverCompiledModel`, `mapNameContainment`, `userClassWithoutContextStillWalls`, `extentFollowsTheActiveContext`, `mixedChainKeepsTheWall`), `integration/MetamodelMappingStoreTest.java`, `integration/MetamodelQueryFunctionsTest.java` (integration); product side — `planner/PlanMakerTest.java:632` `theMetamodelStoresTablesAreThePlansSetupInProcessAndTheSessionsOwnTablesOnAServer` (planner).

**What this means:** the driver's per-kind oracles are spread over five core lanes (root, driver, exec, integration with its two twin lanes, judge) plus the corpus and channel B; the PCT main lanes already run the product path; a deferred TDS has no core test at all; a dynamic pivot and the metamodel route have twins on both paths (`PlanMakerTest` and `PlanRunner`'s `Keyed`).
