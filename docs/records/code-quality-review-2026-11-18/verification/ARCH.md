# ARCH verification

## Verdict on the unit: would set MIXED (low end), not WEAK

The reviewer's strengths hold up when checked: a closed plan algebra, the wall enforced by the Bazel graph, thin doors, 0.16% textual duplication, and an average CCN of 4.0. Of the six S1 findings, only ARCH-4 keeps S1. The other five lose their S1 weight for one of three reasons:
- **The shape is something the user ruled, or a goal requires.**
  - ARCH-2: the `exec_program` target (ruling 3a.1) and two plan types (ruling 2.1).
  - ARCH-3: RunScope on every planning seam (G4) and the round trip of every lane's plan (G11).
- **The host architecture mostly forces it** (ARCH-6).
- **A headline metric does not measure what it claims** (ARCH-1's "about 30 files per change", ARCH-3's "guards as large as the code they guard").

What remains is a sound design with broad but local debt: a type model full of nullables, comments that read as a process log, positional records, duplicated program-shape logic, and session routing spread across exec. That is MIXED by the rubric ("sound design, notable quality debt"). It falls short of WEAK ("problems that will slow every change in the area"). The comment noise and the guard friction keep it at the low end of MIXED.

## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| ARCH-1 | S1 | OVERSTATED | S2 | Codecs 4,620 lines; VERSION 51/21; regex pin; 25 bumps, median 33 files; `Sql.tree` read only by `PlanJson` and the test `PlanRoundTrip`: all verified. But the 30-file metric is the size of feature commits, `:sql` stays an exec dependency without `tree`, and tag-level compatibility conflicts with G7. |
| ARCH-2 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Four step types and two shape walkers verified. The divergences are mostly deliberate policies for each road. The target split is ruling 3a.1, and it does not cause the duplication: ProgramRunner already calls 18 package-private PlanRunner methods across it. |
| ARCH-3 | S1 | OVERSTATED | S2 | `PLANNER_ENTRIES` has 61 entries, not 76. The guards cover all of core (about 400K main lines), not the 25K of plan/exec. RunScope breadth is G4's ratified mechanism. The round trip is G11's stated proof and runs only on the test road. |
| ARCH-4 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Holes verified: any-Node root, nested sequences, branch steps, verdict nullables, two judge enums, the hand-made agreement verdict at `AgreementAsserts.java:76`, 101 and 62 `IllegalStateException`s. Corrections: load-root plans are a real plan kind; the nested-sequence refusal is at about 10 sites, not 11. |
| ARCH-5 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Two source hierarchies (5 and 8 variants), `boolean[] state`, four routers and deep `Target.equals` verified. `Execution` has 5 `run` overloads plus 2 `runOnProjectDatabases`, not "six run overloads". The one production caller is `ConnectionResolver:41`. The routing is confined to about 5 exec files. |
| ARCH-6 | S1 | OVERSTATED | S2 | Three runners and the host walls exist. `TabExports` has 36 `@JSExport`s, not 72. Native `Compiler` has 33 `@CEntryPoint`s, not 34. `e376eec90` (37 files) added two kinds plus a refactor of host operations. The shape is largely forced by hosts running their own DuckDB, and the user ruled host-side separation (C3-C5). |
| ARCH-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Lizard hotspots match exactly. The 14 `assertSameElements` files span 7 packages, not 6, and about 6 of them are comment-only mentions. |
| ARCH-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | "until then" 149; "audit" 306; 107 distinct docs cited; stale `SetupRunner` and `BodyRouter` verified. Corrections: "the user's ruling/go/question" counts 97, not 111; `docs/` holds 742 `.md` files (1,410 files in all), not 455; "one line in six" is true of comment lines (about 15%), not of all lines (about 1 in 29). |
| ARCH-9 | S2 | CONFIRMED | S2 | 15 values. The count is understated: 62 code references in 10 main files, not 39. Further support: the test plan carries an origin as a `String`, decoded with `StatementOrigin.valueOf` (`TestRunner.java:233-237`). |
| ARCH-10 | S2 | CONFIRMED | S2 | Component and constructor counts verified (`Sql` 7/4, `Verdict` 8/4, `Verdicts` 6/4, `Target` 6). 37 positional `new ExecutionPlan.Target(` calls in 20 test files; helpers are written once per test class, with no shared fixture. |
| ARCH-11 | S3 | CONFIRMED | S3 | Counts verified. Wording fix: 5,004 is all qualified forms of the Nullable annotation; the literal `com.legend.base.@Nullable` appears 0 times. |

## Details

**ARCH-1 (overstated).**
- **The facts are right:**
  - 5 codecs, 4,620 lines.
  - `PlanJson.java:151` `VERSION = 51`, under about 120 lines of changelog.
  - `plan-runner.ts:131`, `_runs.py:26`, and the regex pin `PlanVersionsAgreeTest.java:34`.
  - 25 bumps in the window, median 33 files.
  - `Sql.tree` is written at `PlanJson.java:672` and read back at `:1237`. Its only other readers are `core/src/test/.../PlanRoundTrip.java:99-126`, which re-renders the tree through the dialect.
- **Three corrections matter for the cost:**
  - **The change-cost metric conflates.** The 33-file median is the size of feature commits that also bumped the version. Two examples:
    - `fbb448ebf`, 77 files, is "Slice 5: two thin doors".
    - `5ee415fcf` is a pure bump in 6 files.

    In `fb15b1672` (23 files), 16 are tests that construct records positionally, which is ARCH-10's problem. Only 2 are codec files. A generated codec would save about 2-4 files of such a change, not 25.
  - **Dropping `tree` does not free exec from `:sql`.**
    - exec lists `:sql` itself (`core/BUILD.bazel:218`).
    - exec imports `SqlType` (`exec/WireTypes.java:7`), `SqlDdl.ColumnType` (`exec/JdbcDecode.java:346`) and `SqlTyping` (`exec/PlanParameters.java:183`).
    - The plan's `Column` carries `SqlDdl.ColumnType store` (`ExecutionPlan.java:662`).
  - **Tag-level compatibility conflicts with G7.** It would let an old host silently ignore a new field on a known tag, for example v49's `settings` or v50's `transaction` on a target. The TypeScript reader is loose (optional fields, a `_type: string` catch-all), so G7 ("no silent defaults") holds there only through the exact-version match. A version-free design needs strict unknown-field refusal in every host. Java does this only partly (`PlanJson.only`, about 10 uses).
- **What survives at S2:** a hand-kept symmetric codec, a dead MIR payload with a 1,146-line codec, and loose host types.

**ARCH-2 (correction; S2).**
- **The duplication is real.** `PlanRunner.java:377-474` and `ProgramRunner.java:355-485` both:
  - compute the one target, results and snapshot;
  - check that a count's name does not clash with a declared parameter;
  - wrap `inTransaction` in the same `UncheckedIOException` tunnel.
- **But "already disagree" misreads deliberate policies:**
  - The product answers exactly one result; a test body may end on an assert.
  - The program brackets only on a change, because a bracket would carry a survivable abort into later steps on Postgres (`ProgramRunner.java:477-481`).
  - The program treats an effect as committing itself.

  There is also a divergence the reviewer missed: `PlanRunner.sequence` always opens a transaction, even for a read-only sequence (`:455-474`), although its javadoc (`:371-375`) says only "where a step changes the database".
- **Is the target split a real constraint?** It is a user ruling, not a goal. The quote is from `docs/PLAN_AS_DATA_2026_11_07.md` §3a.1: "Yes on its own build target", so that product binaries do not carry the test walking.
  - **It does not cause the duplication.** ProgramRunner already calls 18 package-private `PlanRunner` primitives across the split (`stricter`, `nullTypes`, `systemStore`, `update`, `check`, `seed`, `keyedOn`, `opened`, and others). A shared `Shape` and bracket in `:exec` is feasible under the ruling.
  - **Merging conflicts with no goal.** Both walkers are exec code, so G4, the RunScope wall and the exec dependency wall (`execReachesNoPlanning`) are untouched. G1 even names "the ONE runner (`PlanRunner`)", so one walker is better aligned.
  - **The full alternative needs the user to re-rule** two shapes:
    - 3a.1, the walker's own target.
    - 2.1, two plan types: moving `Extension` steps into `executionplan` lets a product plan represent test steps.

**ARCH-3 (overstated; S2).**
- **Count errors:**
  - `PLANNER_ENTRIES` has 61 signature entries. The source itself records "70 entries -> 61" (`ArchitectureTest.java:1125`).
  - The GUARDS unit's 26.6K lines include build infrastructure (`gates/BUILD.bazel`, `jars_table.bzl`, `LocksTest`) and guards over all of core: dialect, lowering, carriers, identity. Core main is about 400K lines. The series added about 9.6K of these lines. "About the size of the code it guards" compares against the wrong denominator.
- **Shaping that a goal mandates, not guard overreach:**
  - **RunScope.** G4 says "Proven twice: at run time, by the wall (`RunScope`) ... RunScope on every planning seam (done: I1)". The checks are one line per planning-phase entry, in about 29 files. "Keep RunScope only at the callback seams" would reopen a ratified goal.
  - **The round trip.** G11's proof is "every plan of every lane round-trips through its JSON form" (`TestPlanner.java:159` cites it, `TestPlanJson.java:77-85`). The test road has no production caller: `ServiceTestRunner` and `PureTestRunner` are called only from tests and PCT. The real defect is an inconsistency, S3: the product's round trip is gated by `-Dlegend.plan.roundtrip` (`TypedQuery.java:249`, set for every JUnit lane in `tools/junit/defs.bzl:57`), while the test road's runs unconditionally.
- **What survives at S2:**
  - The execution id is set in a ThreadLocal and read back two lines later (`PlanRunner.java:88-97`).
  - `PlannedSql.fromPlan` is public, with provenance held only by an ArchUnit list.
  - Source-text guards: regex and brace counting at `JavaEvalLedgerTest.java:1947-1990`; `tools/guards/PlannedSqlTest.java`, 1,652 lines scanning TypeScript.
  - Allowlists that re-implement access control at method grain.
- **Feasibility of the alternatives:**
  - The "package-private `PlannedSql` factory" alternative is only partly feasible. `PlanRunner.expand` (exec) and the test plan's savepoints (`testexecutionplan`) mint `PlannedSql` too. Without JPMS, that means moving the expansion into `executionplan`.
  - The "10-15K lines could go" figure is unsupported.

**ARCH-4 (keep S1, with corrections).**
- **Load-root plans are a real plan kind.** `LoadPlanner.java:77,195,241,292,395` and `Synthesize.java:135` build `ExecutionPlan.writing(List.of(), load(...))`, which the hosts run with `runLoad` (`_runs.py:45-51`). So the proposed `Root` sum needs a `Load` variant, or a separate `LoadPlan` type. "Exclude them" is not enough.
- **The database half needs an "unjudged" case.** A verdict's `wrapper` is legitimately null under the database judge when the arm cannot be expressed (`TestPlan.java:300-305`), so `Database(Node wrapper…)` needs that case.
- **Count.** The nested-sequence refusal is at about 9 Java sites plus 1 TypeScript site, not 11.
- **Line.** The agreement verdict is built by hand at `AgreementAsserts.java:62-87`, specifically :76.
- The substance stands.

**ARCH-5 (S2).**
- **Verified:**
  - `SessionSource.java:25`: 5 variants.
  - `PlanSessions.java:27`: 8 variants, including the decorators.
  - `boolean[] state` at `PlanSessions.java:250`.
  - The decision tree in `ProductRun.java:73-120`, `Routes`/`UserBeside` (`ProgramRunner.java:572-640`) and `sessionsByTarget` (`TestRunner.java:130-134`).
  - `Execution.run` overloads are called only from core tests and PCT's test sources (`pct/src/test/.../PctPlanRoute.java`).
- **Why S2:** the routers serve genuinely different policies (a catalog lease for a system-only query, a system database beside the user's, the appeals' replay), and they are confined to exec's session layer.
- **The proposed normalisation is a large change.** Targets by id is a plan-format change in three languages, not a local one. The `SessionBinder` part is medium.

**ARCH-6 (overstated; S2).**
- **The constraint.** The tab runs DuckDB-WASM, and Python runs its in-process DuckDB.
- **The host walls were asked for.** The user ruled that the hosts' separation be "proven structurally" (C3), and PRs #101, #103 and #105 built exactly these host walls.
- **The duplicated runner code is small:** 810 lines of TypeScript and 335 of Python, each a refusing subset. The host walls are 58, 35 and 86 lines.
- **Option (a) is unproven.** Compiling a Java runner into TeaVM and native needs asynchronous callbacks into DuckDB-WASM from TeaVM, and ctypes callbacks from the native image. Both are unproven.
- **Option (b)** ("a runner of about 100 lines per host") is roughly today's design, with an optimistic line count.
- **What survives:** generating the protocol table (kinds in `_planner.py:17-40`, `planner-answer.ts`, `Boundary`, `TabExports`, native `Compiler`) is a fair S2.

**ARCH-7 / ARCH-8 corrections.**
- **ARCH-7.** These files mention `assertSameElements` only in comments: `AssertVerdicts.java:565`, `DatabaseJudge.java:150,228-229`, `TestPlan.java:295`, `CanonicalRenderSql.java:51`, `TestDataPlanner.java:1112`. The substantive set is about 8 files. G2 makes about 5 of them essential: the signature, the arm, the database SQL, and the host comparison and its judgment.
- **ARCH-8.** The counts are as corrected in the table. Further stale examples:
  - `ExecutionPlan.java:224` cites `PlanRunner.program`, which does not exist.
  - The header of `plan-runner.ts:2` says "PlanJson's text, version 22", but the runner reads version 51.

### The headline recommendation and its cost (4-6 engineer-weeks; deletes 4-5K lines; a plan change from 30 files to 5)

**The cost estimate is optimistic.** A realistic sum:
- A codec generator with hooks for bundles, hoisting and digests (`PlanJson.java:253-440, 961-1011`), and TypeScript and Python emitters: 2-3 weeks.
- One step IR and one walker: 1-2 weeks.
- A root and verdict sum, including a Load plan kind, across 3 hosts: about 1 week.
- Target normalisation and one binder across 3 hosts: about 2 weeks.
- Pin edits across the dated guard layer: about 1 week. ArchUnit entries, `PlannerSurfaceTest`, `JavaEvalLedgerTest`, `PlannedSqlTest`, `tools/deps/core-layers.txt` and `ts-layers.txt` all name the classes and files that move. ARCH-3 itself says this friction exists, and the estimate does not budget for it.
- Lane verification: about 1 week. That is PCT on DuckDB, H2 and channel B, the corpus on DuckDB and H2, the stress suites, and CI on every OS, because AGENTS.md requires a PR for a multi-change landing.

That totals 8-12 engineer-weeks.

**The deletion estimate is plausible gross, smaller net.**
- What goes: the codecs (4.6K, including MirJson's 1.1K), the step types and translations (about 150 lines) and `PlanRunner.sequence/steps` (about 150 lines).
- What comes back: the generator and its hooks (about 1-1.5K).
- Net: about 3-3.5K.

**The file count is overstated.** The "30 files" is the median size of feature commits. A pure plan-field change today is 6-23 files, mostly positional test constructions, which are ARCH-10's fix, not the IR's. A fairer claim is about 10-20 files down to about 5-8, once fixture factories are included.

**What it puts at risk:**
1. **User rulings.** It reverses two user rulings: two plan types (2.1) and the walker's own target (3a.1). If ARCH-3's "shrink RunScope" is folded in, it also reopens G4. Each is the user's call under AGENTS.md.
2. **G7.** Replacing the exact version with tag-level checks lets an old host silently ignore new fields unless every reader refuses unknown keys.
3. **The host contract.** The plan format is the contract with DataCube, Studio and Python. A big-bang format change breaks the version handshake across all three at once.
4. **Fidelity.** It touches the plan every PCT and corpus verdict runs on, so fidelity regressions would surface late.
5. **The exec dependency.** It does not, as suggested, free exec from `:sql`.

**Recommendation for the owner.** The pieces are worth doing separately, smallest first:
- fixture factories (ARCH-10);
- dropping `Sql.tree`;
- a shared `Shape` and bracket inside `:exec`, within ruling 3a.1;
- the verdict sum (ARCH-4);
- a generated codec, last.

## S3 and strengths spot-checks
- **S3 count: inline FQNs (ARCH-11).**
  - Re-counted, excluding comment and import lines: testplan 1,889, planner 1,040, testexec 801, exec 373, executionplan 127, testexecutionplan 108, result 106. That is about 4.4K, matching the report's about 4.3K.
  - `@com.legend.base.Nullable` in all its forms appears 5,004 times in core main, matching.
  - It is not forced: `ProgramRunner.java:5` imports `com.legend.base.Nullable` and uses `@Nullable`.
- **S3-style count: process history (ARCH-8).**
  - "until then": 149, matching.
  - "audit": 306, matching about 300.
  - Dated references: 1,154 in the 7 packages.
  - Lines carrying a process marker: 1,715 of 49,089, which is about 15% of the 11,223 comment lines.
- **Strength: the wall is in the build.** Confirmed:
  - `core/BUILD.bazel:210-219`: the `exec` deps hold no compiler, lowering, dialect or planner.
  - `:270-285`: the planner deps hold no exec.
  - `:237-241`: testexec does not depend on testplan.
  - `exec_program` (`:225-229`) depends only on base, error, exec, execution_plan and result.
- **Strength: per-database variation is contained.** Confirmed:
  - A grep for `DatabaseType.[A-Z_]{2,}` in exec, planner, testplan, testexec and lowering, outside the connectors, returns 0.
  - The only switch on the database type is `planner/EngineText.java:41-74`, an engine-text renderer registry at the edge.
- **Strength: the doors are thin.** Also confirmed: `Execution.java:43-70`, one plan call and one run call per method.

## Missed (if any)
- **`PlanRunner.sequence` brackets every sequence in a transaction**, read-only ones included (`PlanRunner.java:455-474`). Its javadoc (`:371-375`) and the program walker (`ProgramRunner.java:459-485`) bracket only on a change. A doc/code mismatch, and a real third divergence for ARCH-2.
- **A census label is carried as a `String` inside the test plan.** `HostJudgment.EngineText.oursOrigin` is read by `Verdicts.deferredTextOrigin` (`testexecutionplan/Verdicts.java:123-137`) and turned back into exec's enum with `StatementOrigin.valueOf` (`TestRunner.java:233-237`). The cause is that `StatementOrigin` stayed in `exec`, although design §2.9 moves the census into `diagnostics`. This is stringly-typed measurement data inside the plan, and it strengthens ARCH-9.
- **The G11 comment at `TestPlanner.java:159` is misplaced.** It sits over the appeals block; the round trip it describes is at `:167`.
- **The two round trips are gated differently.** The product's is flag-gated (`TypedQuery.java:249`); the test road's is unconditional (`TestPlanner.java:167`). One mechanism should serve both.
