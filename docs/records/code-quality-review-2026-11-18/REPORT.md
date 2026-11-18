# The plan/exec separation: a code-quality review (main @ 4dfbb5b7, 2026-11-18)

**Scope.** The plan/exec separation as it landed on `main`: PRs #92, #94, #95, #101, #103, #106/#110 (in #111) and
#105. That is about 1,020 commits and +118K/−33K lines, of which about 29K are docs. The review judged the code as it
stands at `4dfbb5b7`, by area. Correctness and test results were out of scope; the tests pass.

**Method.**
1. **The bar.** One rubric (`RUBRIC.md`) for every reviewer: "would a strong senior engineer who owns this code for
   years have written it this way, and could a capable new engineer be productive in it within a week?"
2. **The reviews.** Nine independent reviewers, all at extra-high effort. Eight each took one area; the ninth reviewed
   the architecture end to end. None was told the others' findings.
3. **The verification.** A second agent per report tried to refute every design (S1) and local (S2) finding. It
   checked the cited lines, re-counted the numbers, looked for a constraint that forces the shape (TeaVM, GraalVM,
   the portable plan, fidelity with legend-engine, the goals of record), and tested whether the proposed alternative
   is feasible.
4. **The measured baseline** (`METRICS.md`). The series' packages compared with the older ones.
5. **First-hand checks.** I re-checked four headline claims myself.

**Caveat.** This was a static review: no build, test or probe was run. Every count was reproduced by a second agent.
The behaviour findings in §5 come from reading the code and need a probe each to confirm.

---

## 1. Bottom line

**Your suspicion is partly right.** The code is not expert-written. It is also not a disaster, and nothing in it calls
for a redo from scratch.

- **The core design is sound.**
  - The plan is a closed set of immutable, validated records, read through exhaustive switches.
  - The separation itself is enforced cheaply and correctly by the Bazel graph: exec depends on no planning library.
  - The doors are thin.
  - Per-database variation is confined to the dialects and connectors.
  - Textual copy-paste is low (0–1.3%).
- **The debt is in the orchestration code around that core, where most day-to-day changes land.** It falls into seven
  systemic problems (§4): a contract defined many times over, types that admit invalid states, the same concept
  written two to five times, residue of the deleted design, a guard layer that pins incidental structure, a heavy
  readability tax, and god classes concentrated in test planning.
- **No area needed a redesign.** Three areas are weak; the rest carry notable debt on a sound design (§2).
- **"The tests pass" says almost nothing about quality here.** The guards measure where code lives and what it
  reaches; they say nothing about whether it is designed well. Two of them now guard nothing at all (§5).

**Verification held the findings up.** Of the 98 design and local findings, none was refuted:

| Verdict | Count |
|---|---|
| Confirmed | 34 |
| Confirmed with a correction | 48 |
| Overstated | 16 |
| Refuted | 0 |

Design-level findings went from 26 to 14, four of them borderline. Every finding with its verified severity is in
`ALL_FINDINGS.md`.

## 2. Verdict per area

The scale runs EXPERT > SOLID > MIXED > WEAK > REWORK.

| Area | Code | Reviewer | Verifier | In one line |
|---|---|---|---|---|
| Plan model and codecs | `executionplan/`, `testexecutionplan/` | MIXED (weaker end) | agree | The type core would pass a demanding review; the five hand-written codecs, strings inside the typed plan and the comments would not. |
| Exec runtime | `exec/`, `result/`, the doors | WEAK | agree (borderline with MIXED) | A right skeleton with careful failure paths, but three design problems at its centre: column types, the step runners, session acquisition. |
| Planner | `planner/`, `setup/`, `testdatagen/` | MIXED | agree | Good leaf types. The orchestration (`PlanMaker`) and the test-data machinery carry the debt; both design findings fell to local on verification. |
| Test planning | `testplan/` | WEAK | agree | The plan it produces is good. The code that produces it is an accretion: two classifiers, four body walks, the deleted evaluator re-run in recording mode. |
| Test execution and judges | `testexec/` | MIXED | agree | The interpreter and value core are near SOLID; judge routing is at the weak end. Two likely behaviour defects (§5). |
| Guards | `ArchitectureTest`, censuses, `tools/guards` | WEAK | agree (borderline) | Real strengths (planted violators, `PlanTypesAreDataTest`), but mostly exact inventories of incidental structure, regex source parsers, and silent rot. |
| Python host | `python/legend_lite` | MIXED | agree | Mostly idiomatic. The plan format is hand-ported as dicts, and the guards are out of proportion. |
| TypeScript host | `engine-client`, `tools/deps` | MIXED | agree | Good branded types and protocol unions. The plan reader works by casts, `Boundary.java` is a grab bag, and the commit paths diverge. |
| Architecture (whole) | end to end | WEAK | **MIXED (low end)** | A sound core close to legend-engine's own shape. The verifier found five of six design findings forced or overstated, and kept "the type model admits invalid states". |

## 3. What is genuinely good (keep it)

- **The plan as data.** Sealed, immutable records with constructor checks, defensive copies and exhaustive switches
  (`ExecutionPlan.java:186`, `TestPlan.java:74,388`, `HostJudgment.java:25`). A new node kind fails to compile at
  every reader.
- **The build wall.** `//core:exec` depends on no planning library, pinned by `tools/deps/core-layers.txt`. This is
  the cheap, correct mechanism, and most of G4's structure rests on it.
- **The doors.** `Execution` and `TestExecution` are one planning call plus one run call per method.
- **The connectors.** Each is a small class behind one exhaustive enum lookup, and there is one JDBC send site in
  try-with-resources (`JdbcStatements.java:596-629`).
- **Careful failure paths.** Rollback before restore, secondary errors kept as suppressed, undo on load failure.
- **Failure as data.** `Failure` carries a closed `Kind` list. Credentials are named in the plan but never held.
  Refusals name the table, the column and the value.
- **Branded statement types in TypeScript** (`planned-sql.ts`), with `@ts-expect-error` tests that prove misuse fails
  to compile. In Python, the run frame is built on `contextvars`, and redaction never chains the secret-bearing error.
- **Behavioural tests on real in-process databases** that record the wire, plus conformance fixtures shared by the
  Java, TypeScript and Python runners.
- **The best guards.** `PlanTypesAreDataTest` walks the plan's type closure, names almost no classes and costs
  nothing on refactor. `bites()` tests each rule against planted violators.
- **Low copy-paste:** 0–1.3% in the series' packages, against up to 3.8% in older ones.

## 4. The seven systemic problems

Each problem is found independently in several areas. The IDs point into `reviews/` and `verification/`.

### 4.1 The plan is a contract defined many times over, and typed facts travel as strings

**Findings:** PLANMODEL-1 S1, PLANMODEL-3, PLANMODEL-5, EXEC-1 S1, TYPESCRIPT-1 S1, PYTHON-2 S1, ARCH-1, TESTEXEC-6.

- **Hand copies of one schema.** The plan's shape is the Java records, plus five hand-written codecs (about 4,540
  lines in three tag styles and three ways of writing a missing value). It is written again as loose TypeScript types
  (about 29 casts and a `{ _type: string }` catch-all) and as Python dict access (82 string-keyed reads). The version
  (51, bumped 25 times in the window) is copied by hand into three places, and a regex keeps two of them equal.
- **Typed facts as strings.**
  - A column's SQL type is turned into text (`DECIMAL(p,s)`, `STRUCT<…>`) and parsed back by three decoders and three
    writers, partly per cell.
  - Render forms travel as `"CSVJOIN:"+sep`.
  - Literal carriers are Java class names matched as strings.
- **The highest-value single fix.** Every statement carries the planner's whole SQL tree, which no runtime reads in
  any host. It makes plans about 5.5 times larger and contradicts the project's own ratified design rule (P3 in
  `docs/PLAN_AND_RESULT_MODEL_2026_10_18.md`).
- **What is forced and what is not.** The no-reflection rule (TeaVM, GraalVM) does force codecs written out in code.
  It does not force five unrelated styles, a generator whose output was committed and then hand-edited, or casts in
  place of narrowing.

### 4.2 The type model admits states the code then rejects at run time

**Findings:** ARCH-4 S1 (the one design finding the architecture verifier kept), EXEC-8, PLANMODEL-4, TESTEXEC-1 S1,
TESTEXEC-2.

- **Roots and nesting.** A plan's root may be any node, so exec throws for Update, Check, Effect and Load roots
  (`PlanRunner.java:135`). Sequences can nest, and that is refused in 11 places.
- **Verdicts.** A `Verdict` is an 8-field record with 3 nullable fields, and `Verdicts` holds 7 more. Which
  combination belongs to which judge is written in prose. The runner works out a verdict's kind from which fields are
  set.
- **Sentinels and duplicates.** The model uses sentinel numbers (`NO_VERDICT=-1`), and two enums name the same
  concept (`JudgeMode`, `Judge`).
- **The cost.** Exec has 101 `IllegalStateException` throws and testexec 62, and many of them reject shapes the types
  could have excluded.

### 4.3 The same concept is implemented two to five times

This is structural duplication, not copy-paste.

**Findings:** EXEC-2 S1, EXEC-3 S1, EXEC-5, EXEC-10, TESTPLAN-1 S1, TESTPLAN-3 S1, TESTEXEC-5, TESTEXEC-7, ARCH-2,
ARCH-5, PYTHON-6, TYPESCRIPT-5.

- **Two runners for a list of steps on one session.** `PlanRunner.sequence` and `ProgramRunner` each work out the
  target, the result count, whether the plan writes, and the bracket. They already disagree in three ways:
  - one requires exactly one result step, the other at most one;
  - one counts an effect as a write, the other does not;
  - `PlanRunner.sequence` wraps even a read-only sequence in a transaction, against its own javadoc.
- **Sessions and transactions.** Getting a session goes through five overlapping abstractions, with process-global
  caches that nothing owns or closes. The JDBC transaction bracket is written twice.
- **Test planning.** There are two parallel assert classifiers, the host's and the database's. The database road runs
  the host classifier and then a third partial switch. The walk over a body's statements exists in four copies, with
  the `let` handling pasted.
- **Smaller repeats.**
  - Three decodes of a constant's text.
  - The listener reporting protocol, copied five times.
  - Two or three JSON readers.
  - Two planner clients for one protocol.
  - Three hand-written host runners. They already differ on a failed commit (§5).

### 4.4 Residue of the deleted design: the walk was deleted, its shape was not

**Findings:** TESTPLAN-2 S1, TESTPLAN-4 S1, TESTEXEC-4, EXEC-9, PLANMODEL-6, PYTHON-4, TYPESCRIPT-11.

- **The recording mode.** The database and engine-text planners are the deleted evaluator re-run in a "recording"
  mode. Arms return a throwaway `ok()`, the real plan is pushed into collectors as a side effect, deferred texts are
  sentinel strings compared with `==`, and exceptions double as planning outcomes.
- **`BodyClass`.** It is about 1,200 lines that predict what the planners can do, one rule per corpus test. It still
  matters as their precondition, so it must be dissolved into the components that own each check, not deleted.
- **References to the deleted walk.** "The walk" is cited 283 times across 19 files to explain current behaviour.
- **Leftovers that still run.** A measurement instrument for a deleted byte channel still runs inside production
  verdicts. There are public methods with no callers, about 110 lines of plan-bundle code that only tests use, and
  re-export shims in both hosts.

### 4.5 The guard layer pins incidental structure, and some of it has silently rotted

**Findings:** GUARDS-1 S1, GUARDS-2, GUARDS-3, GUARDS-4, GUARDS-6, GUARDS-7, PYTHON-1 S1, TYPESCRIPT-2, ARCH-3.

- **Exact inventories.**
  - Line counts of 25 product files, which fail on shrink as well as growth.
  - 61 planner entry points by full JVM signature, including an enum's `ordinal()`.
  - 88 TypeScript call sites by their literal text.
  - Per-file counts of `CREATE TABLE`.
- **The tax.** 36% of product-changing commits also edit a guard file. Sampling shows about 20–25% are forced
  bookkeeping, such as a line count re-measured or a row replaced by a tombstone.
- **Silent rot.**
  - Two `ArchitectureTest` rules target packages that no longer exist (`com.legend.driver`, `com.legend.judge`).
    ArchUnit passes a rule whose target matches nothing, so they have guarded nothing since a move. Pointed at the
    right classes, they would flag four classes.
  - 15 of the 203 mutable-field allowlist entries match nothing.
  - That guard's regex cannot see package-private fields in nested classes, which leaves three mutable fields
    unpoliced.
- **Hand-written source parsers.** Regexes over Java, Python and TypeScript source, with at least nine comment
  strippers that disagree. One guard spells `import` as `['im','port'].join('')` to dodge the others.
- **Not all of it is replaceable.** All 57 core libraries sit in one Bazel package, there is no JPMS, and Java has no
  friend packages. So the planner allowlist cannot simply become access control. The fix is consolidation (facades
  inside the package, type-level rules, name-only registers), not deletion.

### 4.6 A readability tax on every file

**Findings:** every area's S3 patterns, plus PLANMODEL-10, TESTPLAN-8 and ARCH-8.

- **Comments as a process log.** The series' packages carry 12–48 dated references per 1,000 code lines, against 2–20
  in the older packages. They also carry 2–14 doc-path citations and 3–9 mentions of "the user" per 1,000 lines.
  Javadoc tells when and why code moved, not what it does now. Each verifier found stale or wrong comments, such as
  links to methods that no longer exist, a "version 22" in a file that reads 51, and javadoc attached to the wrong
  member.
- **Inline fully qualified names.** About 4,300 of them instead of imports. `Boundary.java` has 0 imports and 246
  inline names. This habit runs codebase-wide but is heaviest in the series' packages.
- **Vocabulary sprawl.** "Arm", "side", "wall", "road", "door", "lane" and "rider" each mean several things, so a
  reader needs a glossary. Some test names come from process events (`AuditRound3Test`).

### 4.7 God classes and long parameter lists, concentrated in test planning

**Findings:** TESTPLAN-6, EXEC-4, PLANNER-2, TESTEXEC-8, TYPESCRIPT-6.

- **The worst methods, all in test planning:**

  | Method | Complexity | Size | Parameters |
  |---|---|---|---|
  | `DatabaseJudge::databaseVerdict` | 92 | | 13 |
  | `BodyClass::frameProgram` | 76 | | |
  | `AssertVerdicts::adjudicate` | 65 | 313 lines | |
  | `SqlTextVerdicts::rowsLegAndVerdict` | | three overloads | 15, 17 and 18 |

- **The other hot spots:**
  - `PlanRunner` is a 1,209-line bag of statics that passes results through one-element arrays.
  - `PlanMaker` runs on flags and plans a writing query twice: a probe pass that rebuilds the whole target, then the
    real one.
  - `TestRunner.Moved` is 356 lines with 14 fields.
- **Elsewhere the series is average.** Its average complexity matches the older packages.

## 5. Likely behaviour defects found along the way (confirm each with a probe)

These are beyond style. Each was found by reading the code and needs a probe or test to confirm. The first four I
re-checked first-hand as far as reading goes.

| # | What | Where | Status |
|---|---|---|---|
| B1 | Two `ArchitectureTest` rules guard nothing: their targets name deleted packages. Pointed right, they flag `HostJudgments`, `AssertRuns` and `DatabaseVerdicts`. | `ArchitectureTest.java:3427`, `:3476` | Confirmed by reading |
| B2 | The service-JSON comparison applies the 2-ULP float tolerance unconditionally, against your 2026-11-05 narrowing of S15/S132 (compatibility mode only). The other site gates it on the rule set (`:242`). No register row. | `testexec/Equality.java:539` | Confirmed by reading |
| B3 | TypeScript's `transacted` commits outside its `try`, so a failed COMMIT is not rolled back. `stream()` and Python's `run_write` do roll back, so the hosts diverge. | `engine-client/src/plan-runner.ts:240-258` | Confirmed by reading |
| B4 | A probable wrong pass. With a `Decimal` property mapped to an INT column, `assertSameElements([1,2], T.all()->map(x|$x.d))` fails the typed check, but the untyped message re-check overturns it, so the assert passes. The same inputs through `assertEquals` crash with "verdict/message divergence". | `testexec/HostJudgments.java:586-588`, `:36-48`, `:756-769` | Traced by the verifier; needs one `AssertBarTest` probe |
| B5 | `PlanRunner.sequence` wraps read-only sequences in a transaction, against its javadoc and against `ProgramRunner`. | `exec/PlanRunner.java:378-475` | Verifier |
| B6 | The host and database judges disagree on a body whose `let` raises at planning. The host plans the error at the next assert; the database judge refuses the whole body. | `testplan/HostAsserts`, `DatabaseAsserts` | Traced, not run |
| B7 | Planning catches `RuntimeException` at 18 sites, so a `NullPointerException` or `ClassCastException` becomes a "planned raise" reported as a test failure, which masks lite's own bugs. | `testplan/` (TESTPLAN-5) | Verifier |
| B8 | `containsOf`/`subsetOf` drop declared kinds (against S131), and grid `assertSameElements` ignores the declared column kinds that `assertEquals` uses. | `testexec/` (TESTEXEC-9) | Verifier |
| B9 | `RunScope`'s shared close token: a double close leaves depth at −1, and then a statement outside any run is allowed. The wall silently switches off. | `base/RunScope.java` (EXEC-12) | Verifier |
| B10 | `Boundary.catalogColumns` silently defaults `notNull`, precision and scale (invariant 4). | `wasm/src/main/java/planner/Boundary.java` | Verifier |
| B11 | `Equality.Unjudged` is thrown at two sites and caught nowhere. | `testexec/Equality.java:193`, `:361` | Verifier |
| B12 | Python: remote files work only on a connection `Frames` opened, so a user cannot have both their own SQL and remote files. | `python/legend_lite/engine.py:740-741` | Verifier |

## 6. What to do: options in tiers

Each tier is independent, and none needs a rewrite. The verifiers' advice on order was to land the work in small
pieces, smallest first.

### Tier 0: confirm and fix the behaviour defects (§5)

- **Size:** small, days.
- **Probes first:** B2, B4 and B6 each need one probe in `AssertBarTest` or the PCT lanes.
- **Small fixes:** B1, B3, B9, B10 and B11 are a few lines each.
- **Make the silent rot impossible:** every name-matching rule must match at least one class, and every allowlist is
  checked in both directions.

### Tier 1: mechanical sweeps

- **Size:** low risk, days, largely automatable.
- **Imports.** Replace inline names with imports (OpenRewrite or an IDE).
- **Comments.**
  - Move process history out of javadoc: git and `docs/records` already hold it.
  - Fix the stale comments the reports list.
  - Add one short glossary.
  - Watch out: the deletion census reads raw file text, comments included.
- **Dead code and residue.**
  - Unused `PlanRunner` methods, the plan-bundle API and the shims.
  - Move the deleted channel's measurement instrument into test sources. `CanonicalForm` must move, not be deleted:
    it is the oracle for `SqlCanonConformanceTest`.
- **Test fixtures.** Factories for the positional plan records, so adding a field stops rippling through 16 test files.

### Tier 2: targeted refactors

- **Size:** weeks in total, each landed separately.
- **The order:**
  1. **Take the SQL tree out of the wire plan** (P3 compliance, plans about 5.5× smaller). Small to medium.
  2. **One step-list core inside `:exec`.** It owns the shape analysis and the one transaction bracket, and both
     runners use it. This is feasible today: `ProgramRunner` already calls 18 package-private `PlanRunner` methods.
     Medium.
  3. **Typed column facts in place of the type-string grammar** (EXEC-1, PLANMODEL-3). Medium.
  4. **Sum types for plan roots and steps, and for verdicts** (ARCH-4, TESTEXEC-1/2, PLANMODEL-4). Medium to large.
  5. **Test planning: one classifier, one body walk, arms that return their plan.** Dissolve `BodyClass` into the
     owners of its checks (TESTPLAN-1 to 4). This is the largest single refactor.
  6. **One session binder in exec** (EXEC-3, ARCH-5). Medium.
  7. **A guard diet.**
     - Replace exact inventories with name-only registers that are exact both ways.
     - Replace signature strings with type-level rules.
     - Rewrite the regex guards on the compilers' own APIs, with one shared scanner, as `brand-casts.ts` already does.
  8. **Last and largest: one codec kit, or a build-time generated codec.** Generate the TypeScript and Python plan
     types and the version from the Java records (ARCH-1, PLANMODEL-1, TYPESCRIPT-1, PYTHON-2).
- **The architecture verifier's estimate** for the whole consolidation: 8–12 engineer-weeks, deleting about 3–3.5K
  lines net. That is double the reviewer's 4–6 weeks, once the guard pin edits, lane verification and CI on every OS
  are counted.

### Rulings this would need from you

Several shapes exist by your earlier rulings. The reviews do not overturn them, but some refactors need your word:

- **"Two plan types"** (`docs/PLAN_AS_DATA_2026_11_07.md` §2.1) and **`ProgramRunner` on its own build target**
  (§3a.1). A shared core inside `:exec` (Tier 2, step 2) needs neither. A full merge into one walker needs both.
- **The plan's own copy of the connection description** (your 2026-11-08 ruling). This bears on PLANMODEL-7.
- **"Guard files are load-bearing"** in `AGENTS.md`, and `FixtureWriteCensusTest` (ruling B2c). A guard diet moves
  pins and needs your word.
- **G4's `RunScope` on every planning seam.** Shrinking it would reopen G4. The verifiers advised fixing its defects
  (B9) and keeping its reach.

## 7. What this means for the edge review

The handoff for the edge program (`claude/total-wall-orch`, `docs/records/edge-program-handoff-2026-11-18.md`) names
causes that match this review's findings:
- workers ran at default effort;
- the census measured where code lives, not how it works;
- byte-for-byte moves were rewarded;
- the guards carried heavy weight.

When the edge PR is stable, the same method applies: one rubric, independent area reviewers and verifiers at
extra-high, and the same §4 lenses. Then decide salvage against redo from the evidence, not from a premise. On this
evidence, the landed work warrants consolidation, not a redo.

## 8. Files in this folder

| File | What |
|---|---|
| `REPORT.md` | This synthesis |
| `ALL_FINDINGS.md` | All 127 findings with the reviewer's severity, the verifier's verdict and the final severity |
| `reviews/<AREA>.md` | Each area reviewer's full report: summary, strengths, findings with `path:line`, the expert alternative, measurements |
| `verification/<AREA>.md` | Each verifier's check, finding by finding, with corrections and what the reviewer missed |
| `RUBRIC.md`, `VERIFY_BRIEF.md` | The bar and the instructions every agent worked to |
| `METRICS.md` | The measured baseline (comment content, inline names, nullables, complexity, copy-paste) |
