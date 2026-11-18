# GUARDS verification

## Verdict on the unit: agree (WEAK, at the boundary with MIXED)

The mechanisms the reviewer praises are real, and I checked them. They are `bites()`, the type-closure walk, the genquery comparisons and the import self-check. The cost is real too:
- **The cost is measured.** I re-ran the coupling measurement and sampled the coupled commits. About two in three product-plus-guard commits carry a forced bookkeeping edit: a pin re-measured, a register row removed with a dated tombstone, a ceiling moved, or a call-site text re-spelled. The rest add new guards.
- **The false assurance is real.** I confirmed the two vacuous rules, and found more of the same kind: a one-way allowlist with 15 stale entries, and a live false negative in the field regex.

This fits WEAK's wording, "will slow every change in the area": the area here is core and the hosts, whose changes the guard layer taxes. Three findings are lower than reported, though. GUARDS-2 and GUARDS-3 each fix locally, in a few guard files, and several of the suggested alternatives are blocked by constraints the reviewer missed. So the unit is a high WEAK, not a REWORK.

## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| GUARDS-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 | EVICT_SIZE: 28 rows (25 `core/src/main`, 3 `pct/src/test` harness files), PlanText 862 and AssertVerdicts 1528, and shrink fails (JavaEvalLedgerTest.java:2072-2104). UNKNOWN_CASTS: 101 in 40 files. The call-site text pins total 88 (DATABASE_SENDS 19 + WORKERS 4 + CONNECTS 17 + SUBMITS 1 + FETCHES 37 + RUNNER_CALLS 10). FixtureWriteCensusTest: 135 pins, ceiling 950. STATIC_STATE: 56. Funnel registers: 158 `.java` files in the 8 directories, 154 without package-info. **Corrections:** (a) `MUTABLE_FIELD_ALLOWLIST` has **203** entries, not 129. (b) The 195/556 coupling is not all forced cost (see Details). (c) The EVICT_SIZE register is mandated by AGENTS.md, and FixtureWriteCensusTest is a user ruling (B2c), so "delete" needs the owner's word. |
| GUARDS-2 | S1 | CONFIRMED-WITH-CORRECTION | S2 | The two rules are vacuous exactly as claimed, and a third is partly stale. Pointed at the right classes they would report 3 classes and 1 class (see Details). Severity is lower because Bazel layering holds the product-path half of the invariant, and the fix is small. |
| GUARDS-3 | S1 | CONFIRMED-WITH-CORRECTION | S2 | The regexes are as quoted (PlannerSurfaceTest.java:466-468, 1109, 1293-1294, 1465). `codeOf` has CCN 43 (lizard 1.26.1). The 60 s Windows timeout note is at PlannedSqlTest.java:464. The verbatim TS line is at :1041. There are at least 9 named strippers, and the two strip orders differ (JavaEvalLedgerTest.java:2085-2086 vs 2115-2117). **Corrections:** no Python or Java formatter is configured anywhere in the repository, so "black breaks the build" is hypothetical. The layout rule is *not* enforced, only assumed: I found a live false negative (Missed). Fixing it is local to about five guard files. |
| GUARDS-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | PLANNER_ENTRIES has 61 signatures, including `OutputPlan$Mode#ordinal()` and `#values()`, which come from an enum switch. ROOT_DOORS has 90 entries, QUERY_ROADS 39, exports 35+30=65 and BOUNDARY 38 doors; NAMES_SQL is at :790. The `planner.` key collision is at ArchitectureTest.java:1187, owner `planner.ConnectionPlans`, caller `planner.Boundary`. **Correction to the alternative:** every core library is in one Bazel package, so visibility cannot help (Details). |
| GUARDS-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 (weak) | EXEC_MAY_REACH matches the `exec` line of core-layers.txt (:110); its javadoc gives split-package jars as the reason (ArchitectureTest.java:1776-1789). **Corrections:** `theDoorsHoldNoLogic` is a complement, not a duplicate (Details). The two type walkers differ in more than leaf policy. core-layers.txt is the stand-in for visibility that Bazel cannot provide inside //core. |
| GUARDS-6 | S2 | CONFIRMED | S2 | The `*Cache`/`*Store` name rule is at ArchitectureTest.java:3374-3383. The deletion censuses hold 17 Deleted/Bounded rows with 124 symbols, plus HOST_SYMBOLS. No `new Pending(` exists anywhere, and `REMOVED_SO_FAR = Map.of()` (DataMechanismCensusTest.java:337). The count 12 is pinned at :480. `verdictFilesJudgeOnly`'s javadoc admits the primitive-arithmetic hole (JavaEvalLedgerTest.java:1834-1848). |
| GUARDS-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The line-order check is confirmed: `asks >= parses` on `getLineNumber()` (ArchitectureTest.java:1006-1019), and lambdas are found through reflection (:999-1003). **Corrections:** the doors' one-statement rule is design that AGENTS.md states, not checker convenience. The suggested permit cannot reach the lexer (Details). |
| GUARDS-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | One class with 122 `@Test`s in 4,407 lines. Three jar importers (:48-58, :832-842, :3179-3195). `assertMakes` (:4069) is unused by `everyLoadMakerOnTheListMakesOne` (:3982-3998). `Class.forName` initialises every class, and NoClassDefFoundError or ClassNotFoundException leads to `continue` (:3608-3614). **Corrections:** `coreAndTheHostsDoors()` has 7 call sites, not 8. Inline JDK, ArchUnit and JUnit FQNs number 780 in code once comments and strings are stripped, plus 155 inline `com.legend.` ones, against 6 imports; 838 is right in substance. |

## Details

**GUARDS-1.** The facts are right. The one correction that changes the picture is **MUTABLE_FIELD_ALLOWLIST**: it has 203 literals (CodeShapeGuardrailTest.java:52-421), not 129.

On cost, I re-ran the coupling measurement. I took the 936 non-merge commits in `770acc55^1..4dfbb5b7` and counted a commit as "product" when it touched `*/src/main/`, the TS `src/` trees or `python/legend_lite/`. With the 17 core guard files, 202 of 559 product commits (36%) also edit a guard. The reviewer's 195/556 is reproduced, and JavaEvalLedgerTest is touched by exactly 85 commits. Most of that coupling is forced bookkeeping, but not all:
- **The sample.** I drew 14 of those commits at random (seed 7). 9 carry a forced bookkeeping edit, for example:
  - `65c0eb09b`: FramePlanner pin 219 -> 212 because a catch was deleted
  - `3322efce2`: EffectPlanner 204 -> 222, and deletion-census rows revived
  - `d0eee7699`: ceiling 912 -> 911
  - `812acf89f`: two call-site texts re-spelled after a file move
  - `5592d2c86`: register rows replaced by tombstone comments
- **The rest is new work.** 5 commits are purely new guard work (`348f03c97`, `5bef81ac4`, `dc28a9070`, `53fba7ef8`, `e6ce8b9dd`).
- **So the forced tax is smaller.** It is roughly 20-25% of product commits, not 35%. That is still S1-level for a layer that taxes ordinary edits.
- **The move-commit table reproduces.** 8e59e9ec3 is 32 product files, 6 guard files, +40/-36. 8d2571404, 4f268584f and 3dea62938 match exactly. 5300ea203 touches 6 guard files on the unit list (+52/-50), and its own message says "Guards not yet updated".

**Constraints the alternative must respect.**
- **EVICT_SIZE.** AGENTS.md names "a NAMED, SHRINKING residue (the register in `JavaEvalLedgerTest`)" as the measurable form of the execution tenet. The expert alternative is therefore not "delete the ledger" but "keep the residue as a name register, exact both ways, with no line counts".
- **FixtureWriteCensusTest.** Its javadoc records the user's B2c ruling, "bounded by their guard, not converted" (FixtureWriteCensusTest.java:25-26). Deleting it is the owner's call.

**GUARDS-2: the core claim, checked precisely.**
- **`theCompatibilityJsonReaderIsTheJudgesAlone`** (ArchitectureTest.java:3419-3430). The target is `haveNameMatching("com\\.legend\\.driver\\.JudgeJson(\\$.*)?")`.
  - The package `com.legend.driver` does not exist anywhere: no `package com.legend.driver` in any `.java` file of the repository, test sources included.
  - JudgeJson has been `com.legend.testexecutionplan.JudgeJson` since 5300ea203, the commit that created the file there.
  - **Pointed at the real class, it would report one class**: `testexec.HostJudgments` (JudgeJson.read at HostJudgments.java:784 and :789). The only other dependent, `testplan.DatabaseJudge`, is exempt.
- **`hostVerdictIsReachableOnlyFromTheVerdictSeam`** (:3432-3481). The target is `com\\.legend\\.judge\\.(PureAsserts|CanonicalForm|CanonicalDivergence|Equality)`.
  - The package `com.legend.judge` does not exist; all four classes are public in `com.legend.testexec`.
  - **Pointed at them, it would report three classes:** `testexec.AssertRuns` (CanonicalDivergence, :264), `testexec.DatabaseVerdicts` (:46) and `testexec.HostJudgments` (Equality, PureAsserts and CanonicalDivergence, :89-133).
  - **This hides a real change.** All three were created in 5300ea203, where HostJudge's 839-line body moved out of the exempt `driver.HostJudge` into the non-exempt `HostJudgments`. That is exactly the "new dependent" the rule exists to flag ("register a new dependent consciously").
  - **Neither rule fails as empty.** ArchUnit's default `failOnEmptyShould` judges the `that()` set, not the target predicate, and no archunit.properties overrides it.
- **The stale targets survived several touches.** 5300ea203 and then 8e59e9ec3 rewrote these rules' *exemptions*, and 70 later commits touched the file.
- **`theBodyDriverAndTheJudgesHoldNoJdbc`** (:2733-2748) is stale for `driver.*` and `judge.*`. It is live only for `testdatagen.TestDataGenerationNatives` and `testdatagen.TestDataGenerator`, both of which exist. This one is harmless:
  - Repointed at testplan and testexec it would pass; those packages mention `java.sql` only in comments.
  - Its source twin `SESSION_ONLY_FILES` *was* updated to testplan and testexec (JavaEvalLedgerTest.java:1904-1918).
  - The funnel rule covers every class anyway.
- **The dead exemption is confirmed.** `lowering.Scalars` has 8 "dialect" mentions, all in comments, and imports no `sql.dialect` (ArchitectureTest.java:2627-2628, under a "ZERO exceptions" javadoc).

**Why S2 rather than S1.**
- **Bazel already holds the product-path half.** Only `test_execution` and `test` depend on `//core:testexec` (core-layers.txt:31, 37, 40), so no planner, exec or server class can reach the host lattice whatever these rules say. What went unguarded is which classes on the test side may reach it.
- **The rot is narrow.** My scan of every `com.legend.*` string in ArchitectureTest found no other dead class or package name. Most registers there are exact in both directions.
- **The fix is small.** Correct two regexes, or use class literals (all five targets are public), and add a meta-check that each target pattern matches at least one imported class.

**GUARDS-3.**
- **Formatting is hypothetical.** No `pyproject.toml`, `ruff.toml`, `.flake8`, `.editorconfig`, prettier, spotless or google-java-format config exists, so formatter breakage has not happened and is not imminent. (Black's quote normalisation would indeed empty the regex at PlannerSurfaceTest.java:1109 and fail the test.)
- **The stronger argument is false negatives, and the reviewer did not show one.** I did: see Missed 2. The CodeShape regex does not dictate layout; it silently misses code that is not in its layout.
- **The TypeScript constraint is real.** It is TypeScript 7's Go-binary API (HostSqlTest.java:25-27), as the reviewer says. The Java half could move to bytecode.
- **Error Prone is feasible,** with one caveat. `NULLAWAY_OPTS` disables all checks before re-enabling, so each check must be listed explicitly, as `LEGEND_JAVACOPTS` already does (tools/java/defs.bzl:15-38). Also, `FieldCanBeFinal` proves a field is mutated, not that the mutation is deliberate, so it does not fully replace an allowlist that wants a reason per field.
- **Severity: S2.** The cost falls mostly on guard upkeep, and the fix is local to the guard files.

**GUARDS-4.** The facts stand; the alternative needs correcting.
- **Visibility is unavailable inside //core.** All 57 `legend_java_library` targets live in one Bazel package, `core/BUILD.bazel`; there is no other BUILD file under `core/`. Bazel visibility does not restrict targets within a package, so "Bazel `visibility` on `//core:lowering`" cannot stop `//core:server_lib` from depending on it. That is the gap core-layers.txt fills. Using visibility would mean splitting //core into per-layer Bazel packages, a large build change.
- **JPMS is not used.** There is no `module-info.java` in the repository.
- **Package-private is already used** where the packages coincide (AGENTS.md: "package-private in `planner/` since 2026-11-13").
- **What is feasible:** auto-admit record constructors and accessors and enum synthetic members, which shrinks the 61 entries, plus a per-client facade. That is medium-sized.
- **The typed-request-record idea is sound** and consistent with PlanTypesAreDataTest.

**GUARDS-5.**
- **The doors pair is not duplication.** `theDoorsHoldNoLogic`'s own javadoc says it complements `theDoorsOnlyCompose`: bytecode pins which calls are made, and "a branch is invisible there" (JavaEvalLedgerTest.java:1941-1945). Its brace counting is fragile, but it is not a second encoding of the same fact.
- **The walkers differ in more than leaves.** `takesData` (ArchitectureTest.java:2023-2085) descends into wildcard bounds and array components, where PlanTypesAreDataTest flags a wildcard as a violation (PlanTypesAreDataTest.java:102). A shared walker needs a policy object, not just a leaf predicate.
- **The real duplicate is the JDBC rule,** whose bytecode copy went stale while its source copy was updated (GUARDS-2). EXEC_MAY_REACH's twin is justified in its own javadoc by the split-package jars.
- **Severity:** S2, at the low end.

**GUARDS-7.**
- **The doors' shape is stated design.** AGENTS.md itself states it: "Each door method is one planning call and one run call (`ArchitectureTest.theDoorsOnlyCompose`, `JavaEvalLedgerTest.theDoorsHoldNoLogic`)". The product does not give up ordinary Java there for the checker's sake. Only the depth-2 `;` counter is accidental.
- **The parse-door bans are checker convenience,** as claimed: no lambda, no reference, no nested class and the line-number ordering (ArchitectureTest.java:766-770).
- **The permit cannot reach the lexer.** "Every lexer, parser and lowering entry takes [a `PlanningPermit`]" collides with the Lexer's "JDK-only and stays that way" (AGENTS.md layer table) and with the parser depending on nothing above it (ArchitectureTest.java:761-764). `RunScope.planning(String)` is void (base/src/main/java/com/legend/base/RunScope.java:134).
- **A door-level permit works.** The door methods take a permit only RunScope can mint, and `PARSE_BY_THE_DOORS` stays. It is medium-sized, since every door caller changes.

**GUARDS-8.**
- **The call count is 7.** `coreAndTheHostsDoors()` is called at lines 658, 718, 739, 783, 1113, 1281 and 3174, against the 8 reported.
- **The skip has a stated reason, but it is still silent.** The `continue` carries a comment ("a class whose driver is not on the test JVM's classpath"). It is still silent for an unregistered field. A registered field of a skipped class would surface as "registered but no longer exists", which is misleading but not silent.

## S3 and strengths spot-checks
- **GUARDS-9 count holds.** ArchitectureTest has 75 `.as(` messages. 32 cite a process code, audit, round, slice, step or date by my regex (claimed: 33). Examples: "Invariant 4c: ... — audit 19; step C, D73", and "slice 5: the test's run is the test door's alone".
- **GUARDS-9 EVICT_SIZE block holds.** The block spans lines 82-1062: 981 lines, 952 of them pure `//` comments, and 28 entries (claimed: 993 and 959).
- **GUARDS-9 misattached javadoc holds.** There are three stacked javadocs before `extensionCodecsAreTheEdges` (:1804-1823), and the reflection-ban javadoc sits orphaned above `compileSideLayersAreDialectBlind` (:2591-2618). The class header's Strangler-Fig, `engine/` and "org.duckdb, org.sqlite" text is stale (:20-32).
- **GUARDS-10 holds.** `Map<String, Object[]> EVICT_NAMES` is read through `(Integer) e.getValue()[1]` (JavaEvalLedgerTest.java:1075, 2107). PlanTypesAreDataTest's queue holds `Object[]` pairs (:80-83).
- **Strength, `bites()`: confirmed.** It asserts "was violated", not just an AssertionError (ArchitectureTest.java:2836-2841), across 37 call lines. There are 38 `Planted*.java` files, 1,700 lines, against 37 and 1,674 claimed. Only rules held as `ArchRule` constants (27 of them) can be bitten; inline rules such as the two vacuous ones cannot, which supports GUARDS-2.
- **Strength, `theImportIsEveryProductLibrary`: confirmed.** It fails if a library is missing from the import and if test support is judged as product (:118-128).
- **Strength, PlanTypesAreDataTest: confirmed** as a reflective closure over ExecutionPlan and TestPlan (:75-166).
- **Strength, TEXT_WRITTEN_PORTABLY: confirmed,** with the TeaVM reason stated (:2694-2711).

## Missed
1. **`MUTABLE_FIELD_ALLOWLIST` is one-directional and has gone stale.** `mutableInstanceStateIsExplicit` (CodeShapeGuardrailTest.java:819-846) checks only `contains`; unlike STATIC_STATE, PLANNER_ENTRIES or the funnel registers, it is not exact. I ran the guard's own two regexes over core's main sources:
   - 188 fields match, all allowlisted.
   - **15 of 203 entries match nothing**, for example `PlanRunner.columns`, `PlanRunner.first`, `UserCallInliner.fresh`, `SessionState.dirty`, `Sessions.version`, `TsToPure.fresh` and `SyntheticHeads.filterPredDepth`.
   - So this register now lies by 7%.
2. **A live false negative in the field regex.** `NESTED_MUTABLE_FIELD` (CodeShapeGuardrailTest.java:451-453) requires an access modifier at 8-space indentation. `SqlTextVerdicts.Deferring` (core/src/main/java/com/legend/testplan/SqlTextVerdicts.java:76-81) holds three package-private mutable fields that the guard cannot see: `boolean readingOurs`, `boolean readingGolden` and `@Nullable String originInForce`. The allowlist names `SqlTextVerdicts.originInForce` (:151), which is useless because the regex never reaches it. This is the "guard reports green, guards nothing" pattern of GUARDS-2, in a regex guard.
3. **The deletion census scans raw text, comments included.** `aDeletedMechanismsSymbolsReadZero` uses `text.contains(symbol)` on the unstripped file (DataMechanismCensusTest.java:443-452), over core, spec, pct, native, testing, warehouse and wasm. A comment or javadoc that names a deleted spelling fails the build. That is one more reason the code is full of paraphrased tombstones.
4. **A constraint behind GUARDS-4 and GUARDS-5's alternative.** All of //core is one Bazel package, so intra-core visibility is not available without splitting `core/BUILD.bazel`. core-layers.txt is the project's substitute, and this partly justifies its existence alongside the BUILD deps.

Scripts and data: `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/review/verify/work/` contains `deps.py` (would-be violations), `stale.py` (dead names), `coupling.py` and `sample.txt` (commit coupling and the 14-commit sample).
