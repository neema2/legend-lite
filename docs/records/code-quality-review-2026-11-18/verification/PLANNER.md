# PLANNER verification

## Verdict on the unit: agree (MIXED)

I re-checked every S1 and S2 citation against `4dfbb5b7`, and nearly all of them hold, line for line. The decomposition is sound: typed leaf records, exhaustive switches, one identity scheme, one target maker. The debt sits in the program road's orchestration, in the lowering sequences rebuilt at each site, and in the test-data machinery.

I would lower both S1s to S2:
- **PLANNER-2** (the two-pass program planning) is confined to `PlanMaker` and `ProgramState`. Restructuring it is a medium refactor of one method family, not a redesign of the unit.
- **PLANNER-1** (the allowlist) has a real cost, but the cost is bounded. The reviewer's main remedy also fails under this repository's constraints: Java visibility, JPMS and Bazel visibility cannot replace the allowlist, so it can be simplified but not deleted.

MIXED stays right with twelve S2s: sound design, notable quality debt.

## Findings

| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| PLANNER-1 | S1 | OVERSTATED | S2 | 61 entries, 33 public classes (out of 35 classes and 36 files), and the `Mode#ordinal()`/`values()` entries are all confirmed. Access control cannot replace the allowlist: the callers span 5 Java packages and 6 Bazel targets in 2 BUILD packages. Facades can shrink it and a type-level rule can simplify it, but some guard stays. See Details. |
| PLANNER-2 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Every line cited is exact (:107/:112, :416-492, :429-436, :443-445, :580/:615/:657, :655/:669/:673/:694). "Written twice" is wrong: it is one method with an early return. The waste is larger than stated: the probe also builds the whole target and then throws it away. |
| PLANNER-3 | S2 | CONFIRMED-WITH-CORRECTION | S2 | All six sites are confirmed. SnapCopy's self-check runs at **planning** time, not at run time. `OutputPlan.make` plans the pull twice but compares nothing. |
| PLANNER-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | `executesOn` has 8 call sites (4 in planner, 4 in testplan), not 9. The cycle is inside one Bazel target, and half of it is the door/facade pattern. The fixable half is real. |
| PLANNER-5 | S2 | CONFIRMED | S2 | `SqlSource.java:78-112` is as described. `TableSwaps.java:36` (not :38). Prefix checks `u.url().startsWith("data:")` appear in 10 dialects plus AnsiSqlRenderer and DataSources. |
| PLANNER-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The count is three refusal entry points plus one `instanceof` wrapper (:152, :166, :178 + :189), not four plus a wrapper. Two catch sites use the double negative (PlanAllocations.java:256, :509). |
| PLANNER-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The duplicates are confirmed. The literal and order "divergence" is mostly deliberate: the `plan*` helpers write the engine's displayed plan text, and TestDataPlanner writes the statement that is sent. `:testdatagen` does not depend on `:setup`. |
| PLANNER-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The Lowerer state and the six `unswapped` arms are confirmed. GeneratorSplice's placement in `compiler/spec` is forced by the zero-exception invariant-7 rule. Typed HIR holding `SqlQuery` predates the series (`TypedFrameRef`, 2026-09-20). |
| PLANNER-9 | S2 | CONFIRMED | S2 | `DataSources.java:64-77`, `:98-163`, `:235-256`, `:338-365` and the two scanners all match. `inPayloadOrder` runs unconditionally from `read` (:38), so nothing per database forces it into the planner. |
| PLANNER-10 | S2 | CONFIRMED | S2 | First call wins (ProgramState.java:231-236). The post-hoc check (PlanMaker.java:487-490) only catches the never-registered case, not a late registration, so the risk is if anything understated. |
| PLANNER-11 | S2 | CONFIRMED | S2 (low) | `SystemCatalog.java:188-220`, one call (:126). `Hash`/`Sha256` are portable and need no `java.security`. No host or fixture pins the key. A one-line fix. |
| PLANNER-12 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low) | Synthesize's site is at :122-125, not :112-115. LoadPlanner.java:369-372 is confirmed. |

## Details

**PLANNER-1 (OVERSTATED, S2).**

The facts are right:
- 33 public classes out of 35. The 36 files include `package-info`.
- 120 of the 125 `public static` lines are methods.
- `PLANNER_ENTRIES` has 61 pairs (ArchitectureTest.java:1129-1239).
- Of the 33 public classes, 32 are referenced from another package in main code. Only `PlanEnvelope` could be package-private today.

The analysis has three errors.

*(a) The rule does not police what the finding says.* It exempts every class in `planner`, `testplan` and `lowering` (`plannersPackage`, :1285-1287). So testplan's reach into about 20 planner internals (PlanEnv, BodyPlanner, FramePlanner, ...) is not policed at all. The 61 entries are what **outside** callers reach:

| Caller | Entries |
|---|---|
| `planner.Boundary` (//wasm:boundary) | 32 |
| `com.legend.Compiler` | 11 |
| `com.legend.ExecuteOptions` | 10 |
| `TypedQuery` | 3 |
| `TestExecution` | 2 |
| `server.TestDataRoutes` | 2 |
| `probe.Shadow` | 1 (shared with Compiler) |

The maintenance cost is therefore bounded to those signatures. Internal planner refactors never touch the table.

*(b) "The guard exists because Java visibility was given up" is false.* The callers sit in 5 Java packages:
- `com.legend`, split across three targets: `:planner`, `:execution`, `:test_execution`
- `com.legend.server` (`:server_lib`)
- `com.legend.probe` (`:probe`)
- `com.legend.testplan` (`:testplan`)
- a top-level package `planner` in a different BUILD package (`//wasm:boundary`)

The members they reach sit in 3 more packages and targets: `planner`, `testplan` and `lowering`. Each candidate mechanism fails:
- **Package-private.** Java has no friend packages, so package-private cannot span this.
- **JPMS qualified exports.** Blocked by the split `com.legend` package across `:planner`, `:execution` and `:test_execution`. Neither the TeaVM nor the native-image builds use modules.
- **Bazel visibility.** All core targets live in one BUILD package (`core/BUILD.bazel`, `default_visibility = private`), so they always see each other. Visibility is also per target, not per member or per calling class. Entries like `QueryDoor#refuseRawSql(TypedQuery)` (TestDataRoutes only) versus `refuseRawSql(List, SpecCompiler)` (Compiler only) cannot be expressed by any access-control mechanism. Target-level restriction already exists in `tools/deps/core-layers.txt`.

*(c) The expert alternative is internally inconsistent.* Facades in `planner.api`/`planner.spi` subpackages cannot call package-private members of `com.legend.planner`, because Java subpackages have no privileged access. The internals would stay public.

What would work (medium size):
- Facades in `com.legend.planner` itself. For the boundary's 32 entries, a single `HostPlans` with request records.
- Moving Compiler's 11 entries into the planner (PLANNER-4).
- Replacing the per-signature string table with a type-level rule, for example "outside callers reach only `@PlannerEntry` types". This removes the brittleness (javac's synthetic `Mode.values()`/`ordinal()` from `switch (mode)` at Boundary.java:508 appearing as entries) but keeps a guard.

The finding misses that `PlannerSurfaceTest` (1,859 lines, 226 `Map.entry` rows) lists the root doors' signatures a second time. For example `Compiler#lowerResolved(...)`, `Compiler.LoweredQuery#new(...)` and `TypedQuery#specs()` at :503-560. So a root-door signature change edits two string tables.

The design point stands: the planner's API is not visible in the code. But it is an S2 cost, not a structure that makes every planner change expensive.

**PLANNER-2 (CONFIRMED-WITH-CORRECTION, S2).**

*The claim that a writing query is planned twice is accurate.* `PlanMaker.plan` calls `programmed(... probe=true)` and then `programmed(... probe=false)` (:107, :112).

The probe does a lot of work:
- It runs `decide()`.
- It builds the full `target(...)` (:432-436 → :1232-1262). That includes `project(...)`, `LoadPlanner.declared` and two `Hash.ofUtf8` identities, all discarded. The reviewer did not mention this.
- It runs every let's `LetPlanner.kind`.
- It runs `ProductStatements.prepare` for every staged let and statement. Prepare is not memoised (ProductStatements.java:110-160): inlining, `NativeDispatch.stage`, StoreResolver, CrossStoreGuard, `FramePlanner.renames` and `BodyPlanner.prepare`.

So the front half (G½ + H + BodyPlanner.prepare) runs twice, and lowering and rendering run once.

*"Every new statement kind must be written twice" is wrong.* Each kind is written once, with a `state.probing()` early return. The real hazard is forgetting that return, and the `CatalogReads.RUNTIME` check (ProgramState.java:157-161) exists only to catch the two passes disagreeing.

*The alternative is feasible, and the reading path already shows it.* `plan()` prepares once and routes from `st.stores()` (:122-140). One caveat is that a prepared statement depends on the lets bound before it, and `stagedGridLet` binds dialect text (`gridRead(table, state.dialect())`, :582). That is harmless, because a grid let marks `readsAnotherSource`, so it never coexists with catalog routing.

Size: medium, confined to `PlanMaker` and `ProgramState`. That makes it an S2 (high), not an S1.

**PLANNER-3 (CONFIRMED-WITH-CORRECTION).** The six sequences are confirmed, and `plan()` does lack the `FrameCtes.attach` that `ProgramState.lowered` has (:123).

Two corrections:
- SnapCopy's `IllegalStateException` ("a copy is its pull's statement", SnapCopy.java:69-73) is raised while **planning**, not at run time.
- `OutputPlan.make` (:84-101) prepares and lowers the pull twice but has no comparison.

There is also a constraint the reviewer missed: the plan's stored tree names payloads by digest and "is never rendered again" (SqlSource.java:95-97), so `copyInto` cannot reuse the plan's tree. The reviewer's `PlannedStatement` (tree + sql), made before digesting, is the right fix and fits inside the planner package.

`LoweredQuery` is used as a local tuple across about 30 lines, not "destructured on the next line".

**PLANNER-4 (CONFIRMED-WITH-CORRECTION).**
- **Count.** `executesOn` has 8 call sites: PlanMaker:530, ProductStatements:151, ProgramState:195, SystemRoutes:50, TestPlanner:43 and BodyDriver:83/185/195.
- **The cycle.** It is a Java package cycle inside one Bazel target (`_PLANNER_DOORS` in `:planner`), not a build cycle. `TypedQuery → PlanMaker → TypedQuery` is the "doors in the root, machinery in a subpackage" shape AGENTS.md prescribes. So the "cannot be extracted" argument applies only to the movable half: `executesOn`, `lowerResolved`/`LoweredQuery` (stale javadoc confirmed, Compiler.java:716-719) and Compiler's calls into `ProgramFacts`, `CrossStoreGuard` and `QueryDoor`.
- **The two `DatabaseType` enums.** They are forced by `:exec` not depending on `:model` (BUILD deps), so the fix is a shared leaf enum, not "the plan's enum everywhere".
- **`lowerResolved`.** It really is a second lowering configuration. It has no `NativeDispatch.stage` and no `BodyPlanner.prepare`, and it builds its own Lowerer (Compiler.java:950-955). Its main-code callers are `PlanAllocations:500` and `tools/census/RenderCensus.java:85`.

**PLANNER-6 (CONFIRMED-WITH-CORRECTION).** The entry-point count is three, not four (see the table).

The expert remedy needs a qualifier. The test road deliberately lowers `TypedRawSqlRelation` on the shared program road (`stagedGridLet`). So "refuse in the product-mode Lowerer" means adding a product/test mode to the shared lowering, and `TestDataRoutes` refuses before it scans, not only before it lowers.

The cheapest real fix is small: make `RawSqlRefusal` stop extending `NotImplementedException`, so the two catches need no filter.

**PLANNER-7 (CONFIRMED-WITH-CORRECTION).** The duplicated condition, milestone-filter and literal code is real (TestDataPlanner.java:1673-1755, 1828 vs TestDataGenerator.java:640-725). But the reviewer reads the divergence as a bug, and it is mostly deliberate:
- TestDataGenerator's `plan*` helpers build the engine's **displayed plan text**. `planRootFetch` feeds `"  sql = " + sql` (:560). Their javadoc says "the PROCESSING dimension first (its bitemporal order)", the engine's plan order.
- TestDataPlanner builds the statement that is **sent** (business first, getMilestoningFilter). Its typed, checked literal is the injection fix of 2026-11-12, which display text does not need.

So the shared builder must take the order and the literal policy as parameters. The reviewer's `MilestoneOrder` does take the order; the literal policy would need the same.

`:testdatagen` does not depend on `:setup` (BUILD.bazel:248-253), so a shared `StoreLookup` needs a home both already see (model or compiler).

The 11-argument `new SqlSelect(` is a codebase idiom, not the series' (VerdictSql.java has 59).

**PLANNER-8 (CONFIRMED-WITH-CORRECTION).** The Lowerer's mutable `swaps`/`swappedScans` (Lowerer.java:600-616) and six `unswapped(...)` arms (:633, :639, :1145, :1159, :1160, :1164) are a real smell. The post-lowering rewrite alternative is feasible, since `TableSwaps.swapped` is already a rewrite and the MIR can show the refused source kinds.

The finding misses a forcing constraint. `ArchitectureTest.typedNodesAreMintedOnlyByCompilerLayers` (:3711-3793) says only `compiler`, `resolver`, `normalizer` and `lowering` may construct typed-HIR nodes. Its javadoc says it has zero exceptions by user directive (2026-08-21), and it names an SPI as the pattern to use. GeneratorSplice follows that pattern: its `Generator` is implemented in `testplan/GeneratedTestData`. So "keep the idiom recognition in testplan" is legal only if testplan emits plan nodes rather than rewriting typed HIR, which is a larger change than implied.

Typed HIR carrying `SqlQuery` predates the series (`TypedFrameRef`, added 2026-09-20). The listed files total about 3.3K lines (4.5K including all of `testdatagen`).

**PLANNER-12 (CONFIRMED-WITH-CORRECTION).** Only the line number is off (Synthesize.java:122-125). This is a low S2: two sites, a small fix.

## S3 and strengths spot-checks

- **PLANNER-13, inline FQNs.** My recount over the same 16,022 lines (non-import, non-comment lines, same regex family) gives 1,806. Per file:

  | File | Mine | Reviewer |
  |---|---|---|
  | EnginePlans | 329 | 229 |
  | Compiler | 277 | 184 |
  | LoadPlanner | 160 | 133 |
  | TestDataPlanner | 111 | 66 |
  | SystemCatalog | 99, 0 imports | 57 |

  The ranking and the pattern hold. The reviewer's 1,254 is an undercount, not an overclaim.
- **PLANNER-14, history markers.** ISO dates 242 and "audit" 83 match exactly. "until then" is 49 (case-insensitive) against 43, and "the user's ruling/go" is 31 against 34. Close enough.

  The cited examples are confirmed:
  - PlanMaker.java:892-894 is a comment about the deleted `everyReadStampBound`.
  - :506-507 is an orphaned javadoc stacked on another.
  - MetamodelSeeds.java:14-20 says `exec.SystemDatabase` writes the rows, while SystemDatabase's own javadoc says the plan's seed step has written them since 2026-11-03. Stale.
- **Strength: exhaustive switches without `default`.** `ConnectionPlans.specification`/`authentication` (:38-85), `DataSources.read(SqlDml)` (:235-256) and `PlanMaker.readsAnotherSource` (:1332-1345) all have no default. Confirmed. One caveat: `readsAnotherSource` accumulates into a one-element `boolean[]` holder, the same out-parameter pattern the reviewer criticises in `GeneratedTestData`.
- **Strength: credentials stay out of the plan.** `LoadSteps.held` creates a `Bind.Held(name, field)` (LoadSteps.java:104-107). `LoadPlanner.remoteMount` binds only held names plus the scope (:164-185). Confirmed.

## Missed (if any)

- **A second string table for the planner's surface.** `core/src/test/java/com/legend/PlannerSurfaceTest.java` (1,859 lines, 226 `Map.entry` rows) lists the root doors' and host doors' signatures with field kinds. Together with `PLANNER_ENTRIES`, a door signature change edits two tables in two files. This matters to PLANNER-1's cost; how much to weigh it is the guards unit's call.
- **More work done twice or thrown away:**
  - The probe builds the whole target, including the project data fill and load identities (PlanMaker.java:432-436 → :1232-1262), and discards it.
  - `SystemCatalog.seeds(...)` is computed for every routed statement (:477) even though only the first registration is kept (ProgramState.java:231-236).
- **Re-preparation per statement (suspected).** Each statement of a program is prepared together with all the lets before it (`with(state.lets(), stmt)`, PlanMaker.java:654, :575, :611). So preparation work grows with the square of program length. The probe doubles that.
