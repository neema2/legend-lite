# Independent review of the compatibility program's design and plan (2026-11-19)

Reviewer: an independent session on branch `claude/compat-review`, asked by the program's orchestrator
(`session_0195rQEoSpv1FxC3WndEk5ZH`) for the user. I wrote none of the program. Review only: no product code,
pin, ceiling, ledger or allowlist moved.

**Read at these heads:** `main` `b75d6c73a`; the architecture under test at
`origin/claude/refactor-code-quality-review-9fqhug`; H1-H4 at `origin/claude/compat-homework-h{1..4}`; the census and its
SQL-text probe at `origin/claude/census-b2`; the oracle at `origin/claude/presentation-r`; the plan (r5) and log at
`origin/claude/compat-program` `933ee1f1`.

## 1. My own view (written from the evidence and the code, before I read the plan)

**The right architecture.** The architecture under test is right in its shape: one meaning in the core, compatibility
as translation at the two boundaries. The evidence supports it better than I expected. H2's count is the strongest
single fact in the homework: upstream programs essentially never spell a value as text and then compute with it (14
feeding sites in 1,012 upstream spelling sites; 0 over Float, Decimal, Date, DateTime or Boolean). So the feared R1
"print the way legend-pure does, inside SQL" family has almost no real demand; the presenter covers PCT's print tests.
What remains hard is not text but MEANING: substring, unchecked operations, numeric literal typing, null order, scan
order. Those are where the design will be tested.

What I would insist on:
1. **The translator works on resolved names, not on spellings.** A rewrite keyed on `substring` the token is the JSON
   rename H3 used, and H3 found it untyped and refusing by the wrong name. A rewrite keyed on the resolved FQN and the
   chosen overload needs name resolution (phase D) and, for overloads, typing (phase G). So the translator either
   reimplements part of the frontend (a second compiler, by the back door) or the core exposes a narrow entry that
   lets the edge run D, substitute FQNs, then continue. Neither is free, and H3 did not prototype it ("the model-level
   ENGINE adapter ... Not prototyped (time box)"). This is the single largest unknown in the design.
2. **Compatibility capabilities, if any, are named by what they compute, never by whose they are.** A native called
   `pureFloatText` is upstream meaning in the core under a new name; the census's M2 lens would (rightly) flag it.
3. **The presenter reads three inputs (typed result, plan, typed query facts), and the plan must already carry what
   it reads.** H3 (c) showed this. It means CP-6-style typed facts are product plan data, and their cost to the lean
   plan JSON is real.
4. **The judges: compatibility judging host-only is right, but it removes the database judge from the two biggest
   suites (PCT, the corpus).** G2 parity then rests on whatever still runs under LITE's rules. That has to be planned,
   not discovered.

**What I would do first.** De-risk the riskiest leg before any deletion: a two-day spike of the MODEL-level, typed,
target-aware ENGINE translator over the 15 corpus tests H3 (a) broke, through the public doors only. If it needs a new
core entry, the user should see that before the plan commits 64 session-days to an architecture whose input half is
unproven at the scale that matters. In parallel, the deletions that need nothing (the engine-text printers, rule 4)
are cheap and measured (B2's probe), and can go early.

**What I expected to go wrong.**
- The translator needs an entry into the core that does not exist today, or grows into a second frontend.
- The plan's sizes are optimistic for the judges' and translator's slices, and the lane times (corpus host passes,
  PCT, stress) dominate the calendar, not session-days.
- Deleting the edge's SQL shapes moves H2 row order for unordered results, and the corpus's positional asserts catch
  it before the multiset judge lands.
- "No special functions" plus "no old database versions" quietly moves the ENGINE audience's answers away from
  legend-engine in more places than counted, and the per-audience "move only toward the oracle" check cannot pass as
  worded.

## 2. Verdict

**Sound with changes.**

- **The architecture is right.** Translator in, formatters out, no modes in the core, compatibility judging host-only
  and the appeal kept: the shape is correct, and the homework supports it.
- **The plan is not yet right**, in five ways:
  - its critical path and calendar are wrong;
  - one slice would undo a ratified ruling (plan as data, for the appeal's artifacts);
  - one slice removes a goal's guard (G2 on PCT) with no ratified replacement;
  - its riskiest leg (the model-level translator) is first tested around day 10-16;
  - its central criterion, when legend-engine is "wrong" (not copied) versus "different" (rewritten), is undefined, and
    CP-3's own words contradict slices 3.3 and 3.4.
- **None of these needs a different architecture.** Each needs a specific change, listed with its finding in §3.

## 3. Findings, most severe first

Each finding gives its evidence and the change I recommend. Paths are under `core/src/main/java/com/legend/` unless
they start with a top-level directory.

### F1. The translator is unproven at the scale that matters, and the plan tests it late

**Evidence.**
- H3 never built the model-level adapter. H3 §7: "The model-level ENGINE adapter, and the corpus run through it. Not
  done (time box)."
- Slice 3.2 says "typed call retargeting over the parsed model". A parsed model has no types. Which overload a bare
  call means (`indexOf` on a String or on a collection) is decided at G. Before G, only H3's pattern is sound: rename
  to a twin that overloads every upstream overload.
- The rewrite is target-aware (H3: target-blind 17 of 25 on DuckDB, target-aware 24 of 25). CP-5 forbids a native
  the dialect could spell per database. So one model's bodies (library functions, mapping property functions, derived
  properties: H3's `propertyfunc` tests) must be rewritten into a different lite program per database. That means a
  model compiled once per (audience, target). This touches:
  - the server's pure/v1 projects with more than one connection;
  - `HeldModels`' caches;
  - the corpus's two lanes.

  None of it is measured.
- The second checkpoint ("after the translator's first real rewrite (3.3)") comes after phase 0, 3.1 and 3.2. By F4's
  arithmetic that is around day 10-16, with phases 1 and 2 already running against the architecture.

**What exists today.** The entries a translator needs already exist, with no new hook:
- `Compiler.parseSources` (`Compiler.java:287`) and `Compiler.buildModule(ParsedModel)` (`:697`) for models;
- `Compiler.resolveQuery` (`:868`) and `Compiler.queryResolved` (`:768`) for queries;
- `com.legend.compiler.NameResolver`, which the edge already calls (`edge/TestDataRoutes.java:401`).

So the spike can be done without touching the core.

**Change.**
- Spike it in phase 0, in about 2 days on a scratch branch.
- Scope: H3's 15 newly failing corpus tests, through a model-level, resolved-name rewrite using only the entries above,
  on both corpus lanes (H2 and DuckDB).
- Measure two things: whether anything needs a new core entry, and the memory and time of per-target compilation.
- Move checkpoint 2 to the spike's end, so the user confirms phases 1-3 knowing whether the input half holds.

### F2. Slice 2.2 would undo the 2026-11-08 plan-as-data ruling for the appeal's artifacts

**What the plan says.**
- Slice 2.2 and log F8 say the appeal needs "planning-time artifacts ... each seed's referee copy, built from the CSV
  load's MIR ... which the plan does not carry".
- Slice 2.2 then deletes `testexecutionplan` `Edge` and `NoEdge`, and `ExecuteOptions.recording`.

**What main does.** The plan does carry them.
- `testplan/TestDoor.java:142-149`:
  - `byStep.put(i, edge.effect(...))` and `byStep.put(i, edge.seed(referee.steps(), referee.content()))`;
  - these are typed `Extension` records in `TestPlan.extensionsByStep` (`testexecutionplan/TestPlan.java:44`);
  - they are serialized by the edge's `ExtensionCodec`.
- `testexecutionplan/Extension.java:7-12` cites the ruling: "the user's ruling of 2026-11-08: the edge plans, under the
  compatibility rules, into a typed extension point".
- The corpus lane runs every body from the test plan read back from its JSON (`spec/BUILD.bazel`, `corpus_duckdb`,
  `-Dlegend.diagnostics=decoded-plan`). That guard therefore covers the appeal's artifacts today.

**The risk.** If the "one edge judge object with a planning interface" keeps those artifacts in memory between
planning and run:
- compatibility test plans stop being data (G4, G11);
- the decoded-plan guard stops covering the appeal;
- a ruled design is reversed without the user being asked.

**Change.**
- Keep the typed extension point and its codec.
- 2.2 deletes only the per-decision runner seams: `EdgeHearing`, `HostWords`, `HostForms`, and `CompatEdge`'s ledger
  plumbing where it is not plan data.
- The edge judge's planning interface returns `Extension`s that the test door plans, as `Edge.effect` and `Edge.seed`
  do now.

### F3. Slice 2.2 removes G2's guard from PCT, and nothing ratified replaces it

**Evidence.**
- G2's guard (`docs/GOALS_OF_RECORD_2026_11_07.md` §1): "the agreement judge (`BOTH`) on every lane; the gained and
  lost registers empty."
- The corpus already left BOTH on 2026-11-17 (`spec/BUILD.bazel:179-183`). The database judge agreed with the host on
  61% of the corpus.
- After 2.2, PCT's compatibility judging is host-only. Parity then rests on the 36 shapes 2.1 ports.
- §12 I5 proposes running PCT under the LITE judges, but only "as an addition to 2.1, for the user's word".
- Under LITE's exact rules, PCT will fail wherever today's leniencies carry it: S15 and S132 (2 ULP), S131 (multiset),
  S144 (json-simple). Nobody has counted those failures.
- Running PCT under the LITE rules is not a flag away: channel B builds its options with `ExecuteOptions.recording(...)`
  (`pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:387`).

**Change.**
- In phase 0, run one suite (DuckDB essential) under the LITE rules with BOTH, and count what fails and why.
- Make a LITE-PCT lane, with its own known-failure ledger, part of 2.1 and a precondition of 2.2's deletions.
- Or ask the user to amend G2's guard explicitly. Either way the user decides it; it should not fall out of a slice.

### F4. The critical path is not the translator, and it is 21 to 30.5 session-days, not 18

**The method.** I recomputed the path from the plan's own slice sizes and "Depends on" lines
(the script is in §6; it runs in seconds).

**Variant A: the stated dependencies, with unlimited sessions.**
- Slice 4.1 ends at day **21.0**, along 2.1 (2) -> 2.2 (7.5) -> 1.4 (3) -> 2.3 (5) -> 4.1 (3.5).
- The translator's longest lane (3.4) ends at day **8**. The translator is not on the critical path.
- Phase 0 ends at day 4.5 (0.2 -> 0.4 -> 0.6), against the plan's "about 2.5 days".

**Variant B: three realistic additions.**
- the phase-0 checkpoint gate on phases 1-3 (§5: "phases 1-3 are confirmed at the phase-0 checkpoint");
- the hidden dependencies:
  - 0.4 (j) needs 0.3's ENGINE lane;
  - CP-5 waits for 0.4 (j);
  - 0.6 needs CP-5;
  - 3.3 and 3.4 wait for 0.6 ("the table is put to the user before the translator's first rewrite lands");
- one session per track, as §8 assigns them. B1 does 0.2, 1.1, 1.2, then 1.3, then 1.4, in that order.

With these, 4.1 ends at day **30.5**, and 2.3 at day 27.

**Also not counted:**
- lane latency (the corpus host passes alone are 10.5 minutes for the two lanes);
- about 12 PRs, each with an xhigh review and the user's word;
- the plan's sizes sum to 66.5, not "about 64".

A realistic calendar is 7 to 9 weeks, not 5 to 6.

**Change.**
- Split 2.2:
  - 2.2a deletes the compatibility database judge's half and moves PCT's compatibility judging to the host. This is
    what unblocks 1.3 and 1.4.
  - 2.2b moves the appeal.
  - Run them on two sessions.
- Give 1.3 and 1.4 to two sessions.
- Restate the calendar from the dependency graph, not from the translator lane.

### F5. "Wrong" versus "different" is undefined, and CP-3's own words contradict slices 3.3 and 3.4

**What the rulings and the plan say.**
- CP-3 (E-R2): "where legend-engine answers wrongly, lite answers correctly".
- Architecture rule 5: "Upstream's bugs are never copied."
- §12 I11: "The product follows Pure, not legend-engine, from now on."

**What the slices do.**
- 3.3 rewrites the ENGINE audience to legend-engine's uncorrected `substring`. legend-engine itself treats that as a
  correction kept off for old queries: "correcting substring unconditionally would change the results of existing
  queries" (`platform/Feature.java`, `CORRECT_SQL_SUBSTRING_INDEXING`). H3's target-aware library reproduces each
  database's window quirks.
- 3.4 rewrites to:
  - `isNumeric` as `lower(s) = upper(s)`;
  - `parseDecimal` as `DECIMAL(5,2)`;
  - `parseJson` with its `}{` tolerance.
- Each of these differs from Pure's own definition, so by CP-3's text it is "wrong".
- Meanwhile S22 (`count` is 1 over a missing association) and legend-engine's `joinStrings` separator (B2's F3) are
  classed as defects and not copied.

**The gap.** No stated rule separates the two classes. Each R0 row will be argued case by case in 0.6.

**Change.** Before 0.6, put one rule to the user. For example: "rewrite only where an existing legend-engine client's
answers would change and the rewrite is exact; otherwise Pure's answer and a register row." Under the strict reading
(legend-engine is wrong wherever it departs from Pure's definition), most of 3.3 and 3.4 become register rows. That
is a much smaller phase 3; see §4.

### F6. A mode the strand map misses: upstream's feature flags in the product door and the lowering

**Evidence.**
- The product carries them at four points:
  - `Execution.run(..., Set<com.legend.platform.Feature> features, ...)` (`Execution.java:62-69`);
  - `TypedQuery.executionPlan(runtime, features)`;
  - `ExecuteOptions.withFeatures` (`ExecuteOptions.java:62`);
  - `Lowerer.withFeatures` (`lowering/Lowerer.java:229-235`).
- `Feature` mirrors `meta::pure::executionPlan::features::Feature` "member for member" (`platform/Feature.java`).
- `LEGACY_SQL_NULL_UNSAFE_EQUALS` switches the core's equality to plain `=` (`Lowerer.java:216-235`; `FeatureRules`
  lines 29-46). That is upstream meaning, selected by an upstream flag, inside the lowering.
- The plan moves only `CORRECT_SQL_SUBSTRING_INDEXING` (3.2: "S130's flag first").
- Neither the strand map nor done-when 1 names `Feature`, `FeatureRules` or `withFeatures`. The census's TSV has no
  hit for them, and H1 mentions the flag only for Legend SQL (S44).
- So "no mode in the core" could be declared done with this one still in place.

**Change.**
- Add the flags to the strand map and to done-when 1.
- The translator consumes every flag.
- `LEGACY_SQL_NULL_UNSAFE_EQUALS` becomes a rewrite if ordinary functions express it. Under CP-5, otherwise, it becomes
  Pure's answer with a register row.
- `Execution.run`'s feature parameter goes.

### F7. R4 reaches further into lite's own frontends than slice 3.1 covers

**What 3.1 covers.** The Cypher translator and the `###Relational` dynaFunction (H3 items 5 and 6).

**What it does not cover.**
- **The #SQL (Postgres) island.** Its javadoc says it emits Pure `substring` as "SQL's substring(start, length)"
  (`protocol/sql/PostgresFunctions.java:447-458`).
- **The #IQ island.** About 13 `substring` and `indexOf` emissions with 1-based starts:
  - `protocol/sql/iq/IqFunctions.java:667`, `:910`, `:1512-1580`, `:1998`, `:2194`, `:4593`;
  - `IqAggregates.java:1489`.
- **Others.** `grep` finds `"substring"` in 9 product main files outside the edge, including `normalizer/RelOpTranslator`
  and `builtin/DynaFn`.

**Why it matters.** H3's core run reported no failure in either island, though `IqFunctionsTest` and
`PostgresToPureTest` cover substring. Either those paths are not executed by `//core:core_tests`, or they are
meaning-neutral at those sites. Either way, R4's effect there is unmeasured. A silent change in the #IQ island's
answers is the worst outcome, because that island is "measured against SAP IQ" and has no upstream oracle here.

**Change.**
- 3.1 lists every product emitter of `substring` and `indexOf`.
- Each gets a test that executes the translated Pure on a database.
- Resize 3.1, which today is 1.5 days for two emitters.

### F8. Measurement 0.4 (j), CP-5's cost, leaves out the corpus and has a hidden dependency

**Evidence.**
- S20's bare-DECIMAL literal is a property of the edge's dialects: `floatLiteralKind` in `edge/PureText.java` at 350
  (DuckDB), 406 (H2), 438 (Postgres), 484 (Databricks) and 649 (Trino).
- So it shapes every edge plan, including both corpus lanes' executed SQL.
- 0.4 (j) runs only "PCT channel A as ENGINE on H2 and DuckDB, plus pure/v1".
- 0.4 (j) needs 0.3's ENGINE PCT lane, but the plan says 0.4 depends on 0.2 only.

**Change.**
- Add both corpus lanes to 0.4 (j).
- Make 0.4 (j) depend on 0.3, and CP-5's ruling on 0.4 (j), as F4's variant B does.

### F9. Slice 1.4's "plan FORMAT presenter" is phase 5's export, and it sits against CP-4

**Evidence.**
- generatePlan's `SingleExecutionPlan`, written from a lite plan, is a structural translation of lite's plan into
  legend-engine's node tree. It is not a presentation of typed rows.
- 5.1's `PlanExport` (2.5 days) does the same thing, and "is reached from the ENGINE presenter's plan format (1.4)".
- The plan-text family today is about 3,524 lines:

  | File | Lines |
  |---|---|
  | `EnginePlans` | 1,011 |
  | `PlanText` | 1,003 |
  | `PlanEnumForm` | 295 |
  | `AggAwareActivities` | 276 |
  | `InProtocol` | 240 |
  | `EngineParams` | 193 |
  | `EngineAllocations` | 179 |
  | `EnginePlanRows` | 146 |
  | `PlanSupportFunctions` | 140 |
  | `PlanVariables` | 41 |

- The plan never splits that code into "format" and "SQL spelling". 1.4 is sized at 3 days for all envelopes, the
  plan format, the graph keys and the trace cells.
- The architecture's rule 4 says upstream's "internal shapes are never promised", and the plan recommends exactly that
  for the six `testSupportStreamFlag*` plan-structure tests. Yet 1.4 keeps legend-engine's plan structure as a
  promised presenter.

**Change.** Put one ruling to the user: is legend-engine's plan structure (generatePlan, `planToString`) promised?
- **No:** 1.4 drops the format item, `planToString` goldens join the "not promised" class, and phase 5 becomes
  optional.
- **Yes:** merge 1.4's format item into 5.1, size it from a measured split, and move it off the formatter track.

### F10. CP-8's single blanket register row makes the oracle lane impossible to attribute

**Evidence.**
- Done-when 5 says: "every move is toward the oracle. Each move away is a listed, ruled register row."
- With version-caused differences under one blanket row, no test can tell a version effect from a new regression.
  Examples: O5's 8 digits, S141's division scale, legend-engine's H2 UDFs (H1 risk 5).

**Change.** The oracle's pinned-differences file classes each line:
- version (CP-8);
- ruled divergence (a register row);
- upstream defect;
- open.

A new line that is not classed fails the lane. This keeps CP-8's "not chased" ruling without hiding regressions.

### F11. The referee must own its settings and dialect before 3.2 and 4.1 delete the edge's

**Evidence.**
- §12 I4 says the referee "keeps whatever settings replaying legend-engine's SQL needs".
- But 3.2 deletes `DatabaseSettingsForm`, 4.1 deletes `PureText` and its 12 dialect subclasses, and 0.4 (h) measures
  only "the corpus's H2 lane run under H2's regular mode".
- The referee's seed copies are built "in the referee's dialect" (`testplan/EffectPlanner.java`, per the plan's 2.2).

**Change.**
- In 2.2b, give the referee its own dialect and settings owner inside the edge judge.
- 0.4 (h) measures the executed lane and the referee separately.

### F12. Other consequences of the rulings the plan has not seen

**E-R2 and the corpus.**
- Under E-R2, the corpus tests whose expected outcome is legend-engine's planning refusal will now get an answer.
  Examples: an `assertError` or expected-failure test over a construct legend-engine refuses.
- 3.5 checks only PCT's manifests.
- **Change:** count the corpus's expected-refusal tests in 0.4, and list them as their own class.

**CP-4 and `toSQLString` for databases lite has no dialect for.**
- Under CP-4, "lite answers its own SQL" has no meaning for DB2, Composite, Sybase, Oracle, SqlServer and the rest.
  Today:
  - `EngineStyleDB2` serves DB2 and Composite;
  - plan text refuses the rest (`edge/PureText.java:530-535`).
- B2 counted 23 DB2 and Composite goldens among the declined appeals.
- **Change:** 2.3 states that these are refusals by name and counts them.

**The appeal kept, and the translator.**
- Under the architecture, the appeal's "our rows" must come from the translated program, or they judge the wrong
  meaning.
- That is a dependency between 2.2b (the appeal moved into the edge judge) and 3.2 (the translator) that no slice
  names.
- **Change:** 3.2's skeleton includes the appeal's derivation of "our rows" among its doors.

**R4 and saved artifacts outside the repository.**
- §12 I1 lists the emitters and saved artifacts "it can find". Deployed query and cube stores cannot be scanned from
  here.
- **Change:** saved artifacts carry the meaning version, so an old `substring` is detected, not silently re-read.

### F13. The strand map's numbers disagree with each other and with main in places

| Claim | At main `b75d6c73a` |
|---|---|
| `LoweringForms` "15 members (`:45-107`)" (§3) and "17 members" (§0, §11) | **15** (`lowering/LoweringForms.java`) |
| `raw` at `edge/PureText.java:326-330` | **359** (DuckDB) and **447** (Postgres) |
| `refusesDecimalArithmetic` at SQLite `:535` | **567** |
| `floatLiteralKind` at DuckDB `:318` | **350**. H2 `:406` holds |
| the `DatabaseJudge` pin is 851 | **852** (`core/src/test/java/com/legend/JavaEvalLedgerTest.java:592`, raised in PR #118's fix round) |
| `Appeals` is 354 lines | **401** |
| `RelationalRootForm.primaryKeyColumns`' product users | also `resolver/StoreResolver.java:3636` and `resolver/Substitution.java:1837` (`stripQ`) |
| phases total "about 64" | **66.5** by the plan's own sizes |

None of these changes a decision. They do say that 0.4 (a)'s ratchet must be generated by a script from the code, not
confirmed by hand.

## 4. Answers to the brief's questions

### Q1. Is the architecture right? Where does it leak, and what would break it?

It is right. It leaks in six places, each acceptable if it is named and guarded.

1. **The presenter reads the plan, not only the typed result** (H3 (c): `activities[].sql`, `relationalType`). This is
   fine while the plan is data. The rule's text should say so.
2. **Envelope fields that are a function of upstream's SQL**, such as the `COUNT(*)` label (O3) and the database's
   error text over upstream's statement. These are register rows, as the plan says.
3. **"No hooks in the core" is not literally true.** The edge judge is called back from `TestDoor` and `TestRunner`
   with the live session (`testexec/TestRunner.java:126-132`; `appeals.decide(session, ...)`). That is a documented
   hook. Guard it:
   - pin the two call sites;
   - pin the interfaces' method sets, shrink-only;
   - the hook never receives a planning object at run (F2).
4. **Print positions inside returned structures** (1.3) need a typed flow analysis in the translator, which decides
   whether a print is carried or read. That is part of a compiler. Guard it: the analysis reads types and computes
   nothing, and its operation list is pinned.
5. **Target-aware rewriting without special functions means per-target compilation** (F1).
6. **CP-6's typed facts are product plan data that exist because upstream needs them** (stored or computed, the
   written precision). Calling them lite's own meaning is defensible. Their size should be counted in the lean plan
   JSON baseline, as 1.1 already says.

**What would break it:**
- a translator that needs a new core entry (F1);
- an unavoidable R1 demand from a real client, such as exact-decimal Float arithmetic;
- "wrong versus different" left undefined (F5), so the edge's scope is re-argued on every row.

### Q2. Is anything wrong, missing, mis-ordered, mis-sized or unsafe? Hidden dependencies

**Wrong:**
- the critical path (F4);
- "the plan does not carry" the appeal's artifacts (F2);
- the numbers in F13.

**Missing:**
- the feature flags (F6);
- the #SQL and #IQ islands under R4 (F7);
- the corpus in 0.4 (j) (F8);
- the corpus's expected refusals (F12);
- `toSQLString` for databases lite has no dialect for (F12).

**Mis-ordered:**
- The translator's first real test comes after weeks of other work (F1).
- 2.3 bundles the text printers, which are measured and safe (B2's probe), with the executed shapes, which need 2.2's
  multiset compare. The printers can go early as 2.3a. The probe already replaced them with lite's own H2 text, with
  152 tests moved and 3 answer differences.

**Mis-sized:**
- 2.2 at 7.5 days on one session (F4);
- 1.4 at 3 days (F9);
- 3.1 at 1.5 days (F7);
- 3.2 at 3.5 days, for a model-level, typed, target-aware translator with six doors, which H3 did not prototype;
- 0.4 at 2 days for ten measurements, (a) to (j), each "reproducible from a committed probe". B2's single SQL-text
  probe alone was a sizeable piece of work.

**Hidden dependencies:**
- 0.4 (j) on 0.3;
- CP-5 on 0.4 (j);
- 3.3 and 3.4 on 0.6;
- 2.2b and 3.2, through the appeal's "our rows" (F12);
- the referee on `DatabaseSettingsForm` and `PureText` (F11);
- 2.2 on the previous orchestrator's A5, which the plan names in §9.

**Unsafe:**
- F2 (it reverses a ruling silently);
- F3 (it drops a goal's guard);
- F7 (a silent meaning change in a product island with no oracle).

### Q3. The claims checked

§5 has them: 35 claims, 24 hold, 11 do not or hold only in part.

### Q4. Could a smaller plan get most of the value?

Yes. About 20 to 25 session-days can be cut or deferred without giving up done-when 1 to 3.

**Defer:**
- **phase 5** (6.5 days): export and import are a new capability and remove no upstream meaning from the core;
- **the LITE-host parts of 1.5 and 1.6** (about 4 days): the store adapters' bytes are lite's own `{kind, message}`
  (HOSTS-9), and Python's lite `query/run` writer is lite drift, not upstream.

**Keep:**
- 1.6's CP-7 part (lite's sentence leaves the plan);
- 1.7 (DataCube's ENGINE workarounds, real (a) hits).

**Shrink:**
- **phase 3**, under the strict reading of "wrong" (F5), where it collapses mostly to register rows. Keep only the
  rewrites with a measured client cost (0.4 (j), corpus included);
- **1.4's format item**, if plan structure is not promised (F9).

**What I would do first, in this order:**
1. the translator spike (F1, about 2 days);
2. the PCT-under-LITE measurement (F3, one lane run);
3. the rulings F5 and F9;
4. 2.3a, the engine-text printers deleted on their own (about 3,000 lines, measured);
5. 2.2a, the compatibility database judge's half;
6. then the presenters.

### Q5. What will go wrong first, and the earliest cheap test

**What will go wrong first.**
- Slice 2.2: the largest single slice, on one session, in the most guarded code. It shares the judges with A5 and
  moves several ratified pins (each needing the user's word).
- Its first symptom will be the schedule. Phases 1.3, 1.4, 2.3 and 4.1 all wait on it (F4).
- The first symptom against the architecture will be the model-level translator needing something the public entries
  do not give (F1).

**The earliest cheap tests, in order:**
1. **The dependency script** in §6 (done here; seconds).
2. **The emitter grep for R4** (done here; F7).
3. **One PCT suite under LITE rules with BOTH** (one lane run; F3).
4. **The 2-day translator spike over H3's 15 corpus tests** (F1).

### Q6. Consequences of the user's rulings the plan has not seen

I do not argue the rulings' merits. These are consequences the plan has not taken into account:

| Ruling | Consequence | Finding |
|---|---|---|
| R4, Pure substring | the #SQL and #IQ islands emit the old meaning; saved artifacts in deployed stores | F7, F12 |
| E-R2 | CP-3's "wrongly" contradicts the rewrites; the corpus's expected refusals | F5, F12 |
| SQL text never promised | `toSQLString` for databases lite has no dialect for; plan structure, promised or not (it decides 1.4 and phase 5) | F12, F9 |
| No old database versions | one blanket row cannot be checked by the oracle lane; the referee must keep its own settings while the executed dialects lose theirs | F10, F11 |
| No special functions (provisional) | with target-aware rewriting, it implies per-target compilation of whole models; its cost measurement omits the corpus | F1, F8 |
| The appeal kept | its artifacts must stay plan data under the 2026-11-08 ruling; its "our rows" must go through the translator | F2, F12 |
| Rule 9, compatibility judging host-only (the user's amendment) | G2's guard leaves PCT, after already leaving the corpus | F3 |

## 5. The claims checked against `main` `b75d6c73a`

| # | The plan's claim | Checked | Holds? |
|---|---|---|---|
| 1 | `PlanStyle` has 10 methods, 173 lines | `database/PlanStyle.java`, read | yes |
| 2 | named by 43 product files outside the edge | `grep -rln PlanStyle ... \| grep -v /edge/ \| wc -l` = 43 | yes |
| 3 | 5 facet kinds | `testplan/PlanStyleFacets.java`, `KINDS` | yes |
| 4 | 16 two-form hooks | `PlanStyleFacets.TWO_FORMS`, counted | yes |
| 5 | `LoweringForms`: 15 members (§3); 17 members (§0, §11) | `lowering/LoweringForms.java`: 15 | §3 yes; §0 and §11 **no** |
| 6 | the edge is 93 files, 21,988 lines | `wc -l edge/*.java` | yes |
| 7 | `EngineStyleH2` 2,507, `EngineStyleDB2` 365, `EngineStyleComposite` 79 (about 2,950) | `wc -l` | yes |
| 8 | executed edge `relations`, `keyedCounts`, `computedPruning` equal the product's (`EdgeLoweringForms.java:104/111/118`, `ProductLoweringForms.java:58/64/70`) | read both | yes |
| 9 | executed edge `view`, `wireCast` equal the product's (`:44/49`, `:31/36`) | read both | yes |
| 10 | `EngineShape` at `:186`; `engineSql` at `:156`, called only at `EnginePlans.java:174` | `grep engineSql(` | yes |
| 11 | the compatibility database judge writes through the text facets (`VerdictSql.java:2651-2657`; `DatabaseJudge.java:301`, `:952`) | read | yes (`:2657`, `:301`, `:952`) |
| 12 | `RelationalRootForm.apply` at `:51`, called at `EnginePlans.java:164` alone; `primaryKeyColumns`' product users listed | `grep` | apply: yes. The users: **in part** (`StoreResolver.java:3636`, `Substitution.java:1837` missing) |
| 13 | `raw` at `edge/PureText.java:326-330` | read | **no** (359, 447) |
| 14 | `refusesDecimalArithmetic` at SQLite `:535` | read | **no** (567) |
| 15 | `floatLiteralKind` at H2 `:406`, DuckDB `:318` | read | **in part**: H2 yes, DuckDB 350 |
| 16 | `traceCell` at `planner/PlanEnv.java:173-177` | read | yes |
| 17 | the graph keys at `planner/BodyPlanner.java:72` | read | yes |
| 18 | the appeal's planning artifacts at `testplan/TestDoor.java:144`, `:148` | read | yes |
| 19 | a planned `Failure.message` at `testplan/AssertErrorNative.java:81` | read | yes |
| 20 | the test plan's words at `TestPlan.java:45-48`; `SideShape.java:37` | read | yes |
| 21 | `PureMessages.compile`'s splice at `edge/PureMessages.java:358-366` | read | yes |
| 22 | the deletion lists' sizes: `StableScanOrder` 598, `RelationTextLowering` 791, `SqlTextVerdicts` 1,675, `EdgePureOnlyTexts` 136, `PurePrintForms` 184, `EngineResultEnvelopes` 92, `EngineExecutedValues` 44, `UpstreamVerdictWords` 198, `CompiledPureVerdictNumbers` 54, `EngineTextGates` 110, `SiteWordsRecorder` 124, `PureListTexts` 49, `PureDateTimePrint` 43, `EdgeDateTimeTexts` 28 | `wc -l` | yes |
| 23 | `Appeals` is 354 lines; the appeal stack is about 3,360 | `wc -l` | **no**: 401; the stack is 3,411 |
| 24 | the residue pins: `DatabaseJudge` 851, `HostJudgments` 679, `BodyDriver` 545, `EngineTextPlanning` 151 | `JavaEvalLedgerTest.java:592, 847, 1027, 1053` | **in part**: `DatabaseJudge` is 852 |
| 25 | `ExecutionPlan.Target.settings`, "which exec already verifies" | `ExecutionPlan.java:1023`, `exec/JdbcStatements.java:240-253` | yes |
| 26 | `NativeCatalogGovernanceTest`'s two guards exist | `everyLiteInternalNativeIsNamedAndVerified`, `theVerifiedSetsOnlyShrink` | yes |
| 27 | `FeatureRules`' flag-keyed substring choice | `lowering/FeatureRules.java:59-78` | yes |
| 28 | DataCube's help example is in Pure's meaning (`datacube/src/calc.ts:88`) | read | yes |
| 29 | the Cypher translator writes legend-engine's 1-based `substring` | `protocol/cypher/CypherToPure.java:11656-11677` | yes |
| 30 | the referee's seed copies are artifacts "the plan does not carry" (2.2, log F8) | `TestDoor.java:142-149`, `Extension.java` | **no** (F2) |
| 31 | the critical path is about 18 session-days, through the translator | §6 | **no** (21 to 30.5, through 2.2 and 2.3; F4) |
| 32 | phase 0 takes about 2.5 days, its slices in parallel | §6 | **no** (4.5 by its own dependencies) |
| 33 | about 64 session-days in all | summed | **in part**: 66.5 |
| 34 | today every PCT test is judged by both judges (§12 I5) | `pct/BUILD.bazel:654-657` (BOTH plans the fused program; HOST passes are separate lanes) | yes |
| 35 | `toDDL`'s texts at `edge/EnginePlans.java:958-995` | read | yes (955-1002) |

## 6. The critical-path script

The slice sizes are the plan's; the dependencies are its "Depends on" lines. Variant B adds the phase-0 checkpoint
gate, the hidden dependencies of F4 and F8, and one session per track as §8 assigns them. Run it with `python3 -I`.

```python
S = {'0.2':1,'0.3':1.5,'0.4':2,'0.4j':0.5,'0.6':1.5,'1.1':2,'1.2':4.5,'1.3':5,'1.4':3,'1.5':2.5,'1.6':3,'1.7':2,
     '2.1':2,'2.2':7.5,'2.3':5,'3.1':1.5,'3.2':3.5,'3.3':2.5,'3.4':4.5,'3.5':0.5,'3.6':1.5,'3.7':0.5,'4.1':3.5}
stated = {'0.3':['0.2'],'0.4':['0.2'],'0.4j':['0.2'],'0.6':['0.3','0.4'],'1.2':['1.1'],'1.3':['1.1','2.2','0.4'],
          '1.4':['1.1','2.2'],'1.5':['1.1'],'1.6':['1.2','1.5'],'2.2':['2.1'],'2.3':['0.4','1.4','2.2'],
          '3.3':['3.1','3.2'],'3.4':['3.2'],'3.5':['0.3'],'3.6':['0.4j','1.3','3.5'],'3.7':['3.2','1.3'],
          '4.1':['1.2','1.3','1.4','2.2','2.3','3.3','3.4','3.5','3.6','3.7']}
def run(deps, tracks=None, gate=None):
    end, free = {}, {}
    for s in ['0.2','0.3','0.4','0.4j','0.6','2.1','1.1','3.1','3.2','1.2','1.5','1.7','2.2','3.3','3.4','3.5','1.3',
              '1.4','1.6','2.3','3.6','3.7','4.1']:
        t = max([end[d] for d in deps.get(s, [])] + [0])
        if gate and s[0] in '123': t = max(t, gate(end))
        tr = tracks.get(s) if tracks else None
        if tr: t = max(t, free.get(tr, 0))
        end[s] = t + S[s]
        if tr: free[tr] = end[s]
    return end
a = run(stated)
hidden = {k: list(v) for k, v in stated.items()}
hidden['0.4j'] = ['0.3']; hidden['0.6'] = ['0.3', '0.4', '0.4j']; hidden['3.3'] += ['0.6']; hidden['3.4'] += ['0.6']
tracks = {s: 'B1' for s in ['0.2','1.1','1.2','1.3','1.4']}
tracks.update({s: 'J' for s in ['2.1','2.2']}); tracks.update({s: 'A' for s in ['3.1','3.2','3.3','3.4','3.5','3.6','3.7']})
tracks.update({'0.4': 'B2', '2.3': 'B2', '4.1': 'B2', '1.5': 'H', '1.6': 'H', '1.7': 'H', '0.3': 'R', '0.6': 'A'})
b = run(hidden, tracks, gate=lambda e: e['0.6'])
```

**The output:**
- **Variant A:** 4.1 ends at day 21.0. 2.2 ends at 9.5, 1.4 at 12.5, 2.3 at 17.5 and 3.4 at 8.0. Phase 0 ends at 4.5.
- **Variant B:** 4.1 ends at day 30.5. 2.2 ends at 14.0, 1.4 at 22.0, 2.3 at 27.0 and 3.4 at 16.5. Phase 0 ends at
  4.5.

## 7. Method and limits

**What I read.**
- I read the binding documents, the architecture brief, H1 to H4, the census and its SQL-text probe, and the oracle
  record.
- I read the code at `main` named in the brief, and more where a claim led.
- I wrote §1 before opening the plan.

**What I did not do.**
- I ran no lane and no Bazel target. Every count here is a file read, a `grep` or the §6 script.
- I did not re-run the homework's probes. Where a finding rests on a homework number (H2's counts, H3's batteries,
  B2's 143), the number is the homework's.
- I did not read the upstream trees myself. No finding rests on upstream behaviour I did not take from a cited record.
- F7's "unmeasured" is literal: I did not determine why H3's core run left the two islands green.
