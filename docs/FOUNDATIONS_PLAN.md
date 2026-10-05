# Foundations Plan — the burndown pause

> **Read `docs/TENET_AUDIT_2026_08_16.md` first.** Every task here cites a finding in it.
> This document is the executable form; the audit is the evidence.
>
> **Decision (2026-08-16):** corpus burn-down is **paused as a goal**. It continues to run
> **every cycle as a referee**. We fix foundations, guards, and duplication first, then resume
> burn-down on a baseline that means what it says.
>
> **Status update (2026-08-18):** the 2026-08-16 pause ENDED on 2026-08-17 by user
> directive (burn-to-zero resumed; net corpus 2347→2332 with the interpreter-deletion
> declines recorded in `BURNDOWN_EXPLANATIONS.md` §0.4) — this file was not told at the
> time, a §5 honesty finding in `ADVERSARIAL_TENET_AUDIT_2026_08_18.md`. On 2026-08-18
> the user re-prioritized foundations ("not in a rush on the burn-down — lay the right
> foundations"): burn-down is again PAUSED AS A GOAL and continues as the referee, while
> the audit's remediation (Tier 1 live bugs, Tier 3 honesty, guard coverage
> self-assertion, SQL-string ratchet) and the relation-typed `fetchDb` deletion leg land.

---

## 0. The deal — read this before touching anything

### 0.1 What "paused" means, precisely

| | Status during the pause |
|---|---|
| Converting non-passing corpus rows to PASS | **STOP.** No conversion work. Not even easy ones. |
| Running the full corpus sweep every cycle | **CONTINUE — mandatory.** It is the only instrument that catches mass diversion (the `hostChannel` predicate has collapsed it twice: 2096→408 and 2091→2013). Losing it during the riskiest surgery in the project's history would be the single worst call available. |
| Recording family deltas each cycle | **CONTINUE.** |
| Writing burn-down verdicts / `BURNDOWN_EXPLANATIONS.md` | **CONTINUE where a task produces one.** Several tasks below convert a false PASS into an honest *explained* row; write the verdict at that moment, not later. |

**The sweep changes role, not frequency.** It stops being the thing we optimise and becomes the
thing that tells us we broke something. Treat any unexplained family delta as a stop-the-line
event.

### 0.2 Success criteria — and the pass count is not one of them

**The pass count will go DOWN. That is the plan working, not the plan failing.** Declare this
before starting so nobody panics at cycle 3.

Track these five instead:

| Metric | Now | Target at end of pause |
|---|---|---|
| **Unexplained rows** (non-passing without a verdict, *plus* passing-with-unverified-elements) | unknown — the scoreboard cannot express the second term | **0**, and mechanically gated |
| **Duplicate implementations** (JSON readers/writers, substitution, multiplicity, ASOR, quoting, renderers) | 5 readers, 4 writers, 2 substitution, 2 multiplicity, 2 ASOR, 3 quoting, 5 renderers | **one owner each** |
| **Unguarded tenet surface** (`java.sql` reachable from any package; harness unconstrained) | total — every ArchUnit allowlist admits `java..` | **funnelled + positively constrained** |
| **Falsified self-claims** | 9 | **0** |
| **Uncounted concealment channels** | 2 of 3 (only `H2Verify` instruments its declines) | **0** |

`passed + explained = 2793` stays the burn-down's definition of done. This pause exists to make
the **left** term trustworthy before we drive the right term to zero.

### 0.3 Operating rules for the implementing session

0. **The S1 endgame is platform-executed tests — do not polish what that deletes.** The
   harness's architecture (name-match a test-body construct, rewrite it before compilation,
   reimplement its semantics in Java — audit §3 S1) has a charted destination this plan must
   not fight: the platform executes the whole test function, asserts included, and the
   harness only reports — the model PCT already lives in (audit §4.3.1) and the direction
   `docs/DEFERRED_TEST_EXECUTION.md` already chartered for mapping/service suites when the
   invented engine-lite runners were deleted rather than ported. Practical consequences now:
   (a) Phase 4/6 work SHRINKS harness arms, never generalizes them; (b) standing rule for
   the burn-down when it resumes: **prefer teaching the platform to execute the assert over
   adding a harness arm** — the F0.1 audit delta documents that the paused burn regrew
   S1-shaped surface in exactly this way (AssertLoopForm, envelope peels, splice arms).

1. **One task per commit.** Task ID in the subject line (`F1.2: …`). No task bundling.
2. **Run the referee after every task**, not every phase. Record the four numbers
   (pass / fail / error / shape) plus family deltas in the commit body.
3. **Expected-red is declared per task below.** If a task turns red *more* rows than declared,
   or reddens a family it shouldn't touch — **stop, revert, report.** Do not patch forward.
   That divergence is information; burying it is how we got here.
4. **No discretionary scope.** If you find something new, append it to §9 Backlog and keep
   going. The failure mode for a foundations pass is sprawl.
5. **Every deletion must be justified by the deletion test** (audit §6): delete the site, let
   the platform answer. Same result → it was compensation. Loud error → a real gap that must be
   **named** in `docs/OUTSTANDING.md`, not patched.
6. **Never widen a tolerance or add a fallback to make a task green.** That is the exact
   mechanism this pause exists to reverse.
7. **Before any task that touches `core/`, `mvn -pl core install -DskipTests`** — downstream
   modules resolve from `~/.m2`, not the reactor (`AGENTS.md` common mistake #11).

### 0.4 Expected-red and the referee baseline (policy)

The corpus runner **fails the build** when a family drops below the baseline committed in
`docs/RELATIONAL_CORPUS.md`, and the PCT exclusion ledger fails when pins go stale in either
direction. Expected-red tasks therefore need an explicit blessing mechanism or the line stops
at the first one (F6.1's 71 reds would make G4 permanently red):

1. **A red-landing task commits its re-frozen scoreboard in the same commit.** The sweep
   rewrites `docs/RELATIONAL_CORPUS.md`; the commit body lists every family delta and matches
   it row-for-row against the task's declared expected-red. A delta the declaration does not
   cover = revert (rule 3 of §0.3), never a baseline bless.
2. **The referee's DOWN-only ratchets (advisory ceiling, decline buckets) move only with a
   dated justification comment at the constant**, naming the task ID. Same discipline the
   burn-down used.
3. **PCT reds enter the exclusion ledger as adjudicated pins in the same commit** — each pin
   carries the task ID and the platform-gap verdict. The ledger's stale-pin assert stays; a
   pin that starts passing later fails the build exactly as today.
4. **`BURNDOWN_EXPLANATIONS.md` verdicts are written in the same commit** (per §0.1) — the
   scoreboard row moves from false-PASS to explained in one atomic step, never two.

### 0.5 Gate commands

```bash
# referee (run after EVERY task)
tools/allgates.sh                       # reads GATES_LOG; check G<n>_EXIT by eye — it always exits 0

# focused
mvn -pl core install -DskipTests
mvn -pl core test -Dtest=RelationalCorpusRunner -Dlegend.engine.root=$HOME/legend/legend-engine
mvn -pl pct -am test
mvn -pl parser-equivalence -am test
```

**Sanity tells for a wrong engine checkout** (`docs/GATES.md`): `census: 2759` instead of
`2798`; `h2-exec 0 verified`; ~320s instead of ~90s. Any one means the flag is wrong — fix it
before reading any number.

---

## 1. Phase 0 — Baseline (do this first, it is not optional)

### F0.1 — Capture the honest baseline

**First, two corrections this plan needs at execution time (2026-08-16):**

- **Re-verify every line anchor on contact.** This plan and the audit were written against
  `f6a50a7d`; main has moved past it (the burn's closing batches extracted `MetamodelSteps.java`
  and `PlanAllocations.java` out of `StatementExecutor`, reworked `SubselectPrune`, and touched
  `UnionSynthesis`/`JoinChainEmission`). Cited line numbers are hints, not addresses — the
  audit's own "do not cite round 1's line numbers" warning applies to round 2 already.
- **The baseline is captured at CURRENT HEAD**, not at the audit's HEAD — otherwise every
  expected-red is measured against numbers that no longer exist.

**Do:** run the full referee plus PCT and record, in a new `docs/FOUNDATIONS_BASELINE.md`:

- corpus: total / runnable / pass / fail / error / shape / sqldiff-pass, **per family**
- PCT: run / failures / errors / skipped, per suite, plus the 36 pin list
- parser-equivalence: every ratchet constant's live value
- `H2Verify`: verified / unverifiable / diverged, and the decline buckets
- the four `@Disabled("GAP:")` counts by file

**Why:** every "expected red" below is measured against this. Without it, task 3 is unfalsifiable.

**Also do (audit delta):** append to `FOUNDATIONS_BASELINE.md` an audit-delta section covering
the commits landed after the audit's HEAD (`f6a50a7d..HEAD` — the burn's closing batches).
The audit has not seen them; some added S1-shaped harness surface (assert-loop lifting,
envelope peels, splice arms, identity-hook recognition), some are clean typed-fact reasoning,
and one (the metamodel-walk unification with its honest-failure sentinel) partially addresses
a §9 note. One paragraph per commit: which audit shape it matches, or "clean", so the ledger
stays complete without re-running the eleven auditors.

**Acceptance:** the file exists, committed, and every number is dated and reproducible by a
named command; the audit-delta section covers every commit in `f6a50a7d..HEAD`.

**Expected red:** none.

### F0.2 — Write the tenet charter (the audit's two missing clauses, plus the positive definition)

**Files:** new `docs/TENET_CHARTER.md`; referenced from `AGENTS.md`

**Why:** audit §8 says "Do V0 first: correct the nine falsified self-claims **and write the
two missing charter clauses**" — round 1's V0.2 (model-space vs data-space) and V0.6 (the
host channel's charter). This plan's first draft silently dropped the charter half; without
it, every adjudication below is judgment-by-vibes, and A9-class findings stay "adjudicable
only by hand." The funnel's exemption list (F1.3) is itself a charter decision and should be
derived from this document, not improvised in an ArchUnit rule.

**Content, at minimum:**
1. **Orchestration (Java MAY):** JDBC transport and connection/transaction management; typed
   value CARRIAGE (moving a value without computing from it); emitting envelopes/headers from
   COMPILE-TIME plan facts (column names/types/multiplicities — "types drive construction");
   control flow over statements; byte transport of DB-rendered artifacts.
2. **Execution (Java MAY NOT):** computing a derived value from a ResultSet-crossed value;
   deciding a TYPE from a value's magnitude or text; reordering/filtering/aggregating/
   deduplicating result rows; fabricating values the platform never computed; rendering a
   value's print form when a representation RULE exists (see clause 4).
3. **Provenance, not arms (V0.6):** no ResultSet-derived value may reach `HostEval.eval()`.
   The audit proved a ~6-line dispatch edge reclassified all 47 arms at once — the charter
   forbids the provenance; F1.5 is its enforcement.
4. **The literal exception, once (the LiteralFold rule):** Java may answer without the DB
   only for values that are (a) syntactically verbatim in the typed AST and (b)
   representation-trivial (no coercion/format rule to duplicate). `LiteralFold.ADMITTED =
   {String, Boolean}` is the canonical instance, pinned by `ConstantPlanParityTest` —
   "admitting a kind is a green differential, not an argument." The SAME admission rule and
   the SAME differential-pinning mechanism govern any Java-side literal RENDERING (Phase 4)
   and any future fold. One rule, N applications, zero new judgment calls.

**Acceptance:** the charter exists; F1.3's exemption list and F1.5's invariant each cite the
clause they enforce; Phase 4's Java residue cites clause 4.

**Expected red:** none.

### F0.3 — HostEval arm census (classify the interpreter before pinning it)

**Files:** read-only census over `exec/HostEval.java`'s 47 arms + its two consumers
(`StatementExecutor`, `harness/EngineTestExecutor`); output appended to
`docs/FOUNDATIONS_BASELINE.md`

**Why:** F1.5 PINS the interpreter (provenance invariant) but the plan otherwise leaves an
894-line, 47-arm evaluator on a production path. The tenet's strict form says most of those
arms should not exist in production. Before Phase 1 locks the walls in place, measure: which
arms are reachable from production `StatementExecutor` paths vs only from the harness? The
harness-only set moves out with F1.2's spirit (follow-up task, sized by this census); the
production-reachable set gets chartered per F0.2 clause 3-4 or ledgered as gaps.

**Acceptance:** every arm classified {production-reachable, harness-only, dead}; the
harness-only and dead counts become a follow-up task in §9 Backlog with the census as its
work list.

**Expected red:** none (read-only).

---

## 2. Phase 1 — Guards (FIRST; everything after is non-regressing)

> **Rationale for going first:** every finding in the audit was produced by burn-down-shaped
> activity — "make this failing test pass." Guards before repair means the repair work cannot
> quietly re-create what it is removing.

### F1.1 — Extract `H2Verify.SETTINGS` out of the harness

**Files:** `core/src/main/java/com/legend/harness/H2Verify.java`,
`pct/src/test/java/org/finos/legend/lite/pct/extension/ExecuteLegendLiteQuery.java:157`

**Change:** move the `SETTINGS` constant to a non-harness home
(`com.legend.exec.H2Settings` or similar); repoint both readers.

**Why:** this is the **only** thing outside `com.legend.harness` that depends on it
(verified: `grep -rn "com\.legend\.harness" pct/src/` returns exactly this one line). It is
the sole blocker to F1.2.

**Acceptance:** `grep -rn "com\.legend\.harness" pct/ core/src/main --include=*.java` returns
only files inside `core/src/main/java/com/legend/harness/` itself, plus comments.

**Expected red:** none. **Gate:** full referee + `mvn -pl pct -am test`.

### F1.2 — Move `com.legend.harness` to `src/test/java`

**Files:** all 16 files under `core/src/main/java/com/legend/harness/` → `core/src/test/java/com/legend/harness/`. `core/pom.xml` may need a `test-jar` for `pct`/`rcorpus`.

**Change:** relocate. Delete the four `com.legend.harness` exemption clauses in
`core/src/test/java/com/legend/ArchitectureTest.java` (`:133`, `:141`, `:158`, `:504`) and the
exemption in `ParserBoundaryArchTest:46-53`.

**Why:** 7,019 lines currently ship in the production jar with **zero production consumers**,
and the package appears in `ArchitectureTest` *only* in exemptions — so no rule constrains what
the harness may do, while tenet #2 calls harness compensation the cardinal sin. This makes the
cardinal sin structurally unreachable rather than merely discouraged. Audit §7 T2.

**Acceptance:** the four exemptions are gone and `ArchitectureTest` still passes; production jar
no longer contains `com/legend/harness/`.

**Expected red:** compile errors in `rcorpus`/`pct` until the `test-jar` wiring is right. No
corpus row should move. **Revert if:** any corpus family delta is non-zero.

### F1.3 — The `java.sql` funnel rule ★ **the keystone guard**

**Files:** `core/src/test/java/com/legend/ArchitectureTest.java`

**Change:** add a rule that only
`{com.legend.exec, com.legend.server, com.legend (root), com.legend.testdatagen}`
may depend on `java.sql..`. After F1.2, `harness` is out of `src/main` and needs no exemption.

**Why:** **this is the single rule that turns the tenet from cultural into mechanical.** Every
existing allowlist in `ArchitectureTest` ends in `"java.."` (7 occurrences), which *includes*
`java.sql` — so `com.legend.lowering` could import `ResultSet` tomorrow and the build stays
green. The proof it matters: the audit rubric's own worked calibration example
(`TestDataGenerator`'s `hashString` over `rs.getString`) survived **691 commits** unfixed
precisely because nothing forbade it. Audit §7 T1.

**Acceptance:** the rule exists, passes, and costs ≤5 exemptions; deliberately adding
`import java.sql.ResultSet;` to `com/legend/lowering/Scalars.java` **fails the build** (verify
this, then revert the probe).

**Known limitation the rule must name (F1.3b, same task):** the funnel licenses the
`com.legend` ROOT package — which contains `StatementExecutor`, the audit's own S1 dispatcher
("21 name-matched dispatch arms before anything reaches the Lowerer"). The funnel constrains
the harness and the compiler but leaves the biggest offender structurally free. Mitigate now,
fix later: (a) pin root's `java.sql` usage to an **enumerated, shrink-only class list**
(seeded at the current consumers) so a NEW root class touching `java.sql` fails the build;
(b) record in §9 Backlog the real fix — split root into orchestration (unlicensed) and a
named exec seam (licensed). Do not attempt the split during this pause; the class-list pin is
the guard.

**Expected red:** none. **Gate:** full referee.

### F1.4 — A positive rule on the harness

**Files:** `ArchitectureTest` (or a new `HarnessDisciplineTest`)

**Change:** the harness may not `Collections.sort` / `stream().sorted()` / `.distinct()` over
an `ExecutionResult` outside a site gated on `sortedChain()`.

**Why:** `EngineTestExecutor.compare:2775-2806` **already applies this discipline to itself** —
it gates every unordered comparison on `ordered && actual.sortedChain()`, and `sortedChain()` is
a *compile-time fact about the query*, not about the data. The discipline exists; it is simply
not required of anyone. Audit §7.3.

**Acceptance:** rule passes with an explicit, enumerated allowlist of currently-gated sites.
Every entry in that allowlist has a comment naming why.

**Expected red:** none in the corpus; the rule itself may need several allowlist entries — that
list *is* the finding, record its size in the commit body.

### F1.5 — Pin the `hostChannel` dispatch predicate

**Files:** new `core/src/test/java/com/legend/exec/HostChannelPredicateTest.java`

**Change:** assert that no `ResultSet`-derived value can reach `HostEval.eval()`. Pin
`wantsHostEval`'s selection on a fixed set of shapes, including the `containsFetchDb`
containment arm.

**Why:** the predicate gates 894 lines of interpreter, has **collapsed the sweep twice by two
different mechanisms**, and `grep wantsHostEval core/src/test` returns **zero hits**. Nothing
pins it. Audit §7 T3, §8.

**Important — write the invariant the right way.** Round 1 assumed the risk was the channel
*growing arms*; it did not (+24/−7 over 691 commits). The real mechanism is that **one dispatch
edge silently reclassified all 47 existing arms**: 18 of them (`fold`, `map`, `concatenate`,
`at`, `size`, `eq`, `in`, `slice`, `filter`…) are *dual-use* — model-space or data-space
depending on what flows through. A ~6-line commit wired a `ResultSet` into the bottom of the
chain and moved an entire interpreter from compilation into execution. **A charter enumerating
forbidden arms would not have caught it. Forbid the provenance, not the arm.**

**Acceptance:** the test fails if `executeInDb`'s READ path is re-wired into `chainBottom`.

### F1.6 — Make the R0 rule real

**Files:** `ArchitectureTest`; `core/src/main/java/com/legend/exec/RawSqlBoundary.java`

**Change:** assert `RawSqlBoundary` is called only with corpus-authored text. Until F7.4 lands,
encode the current call sites as an explicit shrink-only ledger.

**Why:** its contract says *"corpus-authored statements only … never against platform-GENERATED
SQL."* At HEAD **4 of 5 call sites violate it, 3 exclusively Java-generated**, one of them in
the harness. `docs/RUNNABILITY_PLAN.md:65` has listed R0 as a plan item without a mechanism.
Audit §3 S4.

**Acceptance:** ledger exists, shrink-only, and F7.4 can drive it to the single legitimate entry.

### F1.7 — Ban `default ->` fallbacks that lose a type

**Files:** `CodeShapeGuardrailTest`

**Change:** (a) fix the `SIG` regex to scan nested-class methods — it anchors on exactly 4
spaces today, so nested methods are unscanned (the 8-space variant currently finds **0**
offenders, so this is free); (b) add a shrink-only ratchet on `default -> "String"` /
`default -> <literal>` in type-mapping switches.

**Why:** `default -> "String"` is the exact defect audit 15 removed from `Executor.pureOfSqlType`
and that PCT reintroduced. Audit §4.2 P2.

### F1.8 — Detect dead private methods

**Files:** `CodeShapeGuardrailTest`

**Change:** flag unreferenced private methods (shrink-only ratchet seeded at the current count).

**Why:** ~89 dead lines in `EngineTestExecutor` alone (`csvText`, `constantStrings`,
`jsonDeepEquals`), plus `inlineFunctionLiterals` in PCT firing 0 of 2,473 times. Nothing catches
this.

### F1.10 — The tenet ratchet (a direct metric, not a proxy)

**Files:** `ArchitectureTest` or a new `TenetRatchetTest`

**Change:** a shrink-only count of ResultSet-cell CONSUMPTION sites outside the seam the
F0.2 charter licenses (egress transport, the LiteralFold-admitted arm). Seeded at the
current count; every decrease is recorded, every increase fails the build with the charter
clause it violates.

**Why:** the §0.2 metrics are all proxies (funnel coverage, duplicate counts, claim counts).
None of them measures the tenet itself. "Java orchestrates, the DB executes" needs a number
that must go down — otherwise progress is inferred from guard coverage rather than measured.

**Acceptance:** the ratchet exists, is seeded, and its seed value is recorded in
`FOUNDATIONS_BASELINE.md`.

### F1.9 — Wire the orphans into the gate

**Files:** `tools/allgates.sh:176`, `docs/GATES.md`

**Change:** add the **11 parser-equivalence classes** that have real assertions but run in no
gate (including all four census tests, whose ratchets already exist). Either make
`CorpusDifferentialTest` a real gate step (run `differential.py` in `allgates.sh`) **or delete
its claim** at `:16-19` to be "the third assertion mechanism."

**Why:** the strongest verifiers in the tree are dark in every build. `CorpusDifferentialTest`
`Assumptions`-skips because `core/target/diff` never exists. Audit §7 T15, T18. **Done 2026-10-05 (Bazel workplan
P3-18): `//core:corpus_differential_test` runs it on every build over `//scripts/corpus:gen_differential`'s data.**

**Acceptance:** `allgates.sh`'s ran-verification loop names every added class; a rename goes red.

**Expected red:** possible — these ratchets have never run in CI. **Any red here is a
pre-existing defect, not a regression.** Record it, adjudicate it, do not weaken the ratchet.

---

## 3. Phase 2 — Measurement honesty

### F2.1 — Render the soft-pass columns ★

**Files:** `core/src/test/java/com/legend/rcorpus/Runner.java:2365-2382` (`writeScoreboard`),
`RelationalCorpusRunner.java`

**Change:** emit, per family and in the totals: `advisory` (the dropped `r.advisory()` counter),
`0-asserts` (PASSes whose detail starts `"0 asserts"`), `vacuous`, and `rescued` (see F2.2).

**Why:** `writeScoreboard` currently prints **only non-passing rows**, so every soft pass is
structurally unmeasurable. `score()` has a four-rung softening gradient — real failure → FAIL,
SQL-only divergence → FAIL, advisory-only → SHAPE, **assert-free-but-executed → PASS** — and
renders none of it. The data already exists in `Outcome.detail` and is discarded at render
time. This is round 1's C0.2 finished properly. Audit §5.1.

**Acceptance:** the scoreboard carries all four columns; the totals row reconciles;
`2301 = <genuinely verified> + advisory + 0-asserts + vacuous + rescued`.

**Expected red:** none — **this is a render change with no semantic change.** The numbers it
reveals will be uncomfortable; that is the point.

### F2.2 — Count the SQL-text rescue

**Files:** `core/src/main/java/com/legend/harness/EngineTestExecutor.java:1007-1013`
(`sqlTextVerify`)

**Change:** increment a counter on the divergent-text-but-rows-match branch; surface it in F2.1.

**Why:** when our SQL text diverges from the engine golden, `h2Upgrade` runs and a row match
returns `null` → scored **verified**, and the divergence is **never recorded in `sqlDiffs`**.
So the committed `244` sqldiff-passes count only the divergences the H2 oracle *failed* to
rescue. The true rate is `244 + <this counter>`. Audit §5.1.

### F2.3 — Count the uncounted concealment channel

**Files:** `core/src/main/java/com/legend/harness/ExecCallFinder.java:151-156`

**Change:** route its `catch (RuntimeException | SQLException) → null` through the same
decline/bucket machinery `H2Verify.decline:121-126` + `bucketOf:131-147` already use, so it is
counted, bucketed, and **the build fails when a bucket grows**.

**Why:** this gates the **entire golden-SQL channel (1,220 sites)** and nothing counts it — a
genuine renderer crash and "this test has no SQL side" are the same outcome, printed only under
`LL_SQLTEXT_DEBUG`. `H2Verify`'s machinery is excellent and exists in exactly one place;
generalise it. Audit §3 S3.

**Acceptance:** decline counts appear in the scoreboard; buckets are shrink-only.

**Expected red:** the initial bucket seed may be large. Seed it, don't fix it here.

### F2.4 — Complete the leniency census

**Files:** `EngineTestExecutor.java` — wire `ordLeniency:3003-3007` into the four uninstrumented
paths: `compare:2915-2936`, `compare:2883-2914`, `csvJoinedEquals:3063-3077`,
`tdsStringEquals:3297-3301`. Add `LL_TOL_COUNT` to `H2Verify`'s float tolerance (`:562`).

**Why:** the instrument that exists to make leniency countable is wired into **2 of 6** paths,
so every number it produces is a floor, not a count. `H2Verify` never counts a comparison that
passed only because of its 10-digit rounding. Audit §5.1.

### F2.5 — Correct the nine falsified self-claims (V0)

**Files:** `exec/Executor.java:319-321`, `harness/LineageForm.java:44-46`,
`testdatagen/TestDataGenerator.java:43-45`, `pct/.../ExecuteLegendLiteQuery.java:73`,
`exec/Column.java:5-9`, `rcorpus/Corpus.java` javadoc, `exec/Ddl.java:321`, plus round 1's five
in `docs/ENGINEERING_LOG.md:123-125`, `RawSqlBoundary.java:20-21`, `sql/Json.java:11`.

**Change:** each header becomes a statement of what is **true**, plus a pointer to the task here
that would make the original claim true again.

**Why:** these headers are load-bearing for reviewers — an auditor who trusts them stops
looking. Nine falsified claims across two audit rounds, **every one flattering**. One commit,
no code change. Audit §1.2.

**Acceptance:** no header asserts a discipline the file does not implement.

### F2.6 — Surface the `@Disabled("GAP:")` rows

**Files:** the 20 `@Disabled("GAP: …")` sites (mostly `RelationalMappingIntegrationTest`);
`docs/OUTSTANDING.md`

**Change:** emit them into `OUTSTANDING.md`. **Re-check each — at least two are stale**:
`"GAP: XStore not in grammar"` and `"GAP: AggregationAware not in grammar"` are both implemented
at HEAD, and `aggregationAware/test/rewrite` scores **13/13 pass**.

**Why:** 20 declared platform gaps are invisible to every scoreboard. Audit §7 T21.

---

## 4. Phase 3 — De-duplication ("all the duplicate stuff")

> Each of these is one behaviour with two or more owners, and in every case **the platform's
> version is the disciplined one**. The rule for this phase: **delete the copy, keep the owner.**
> Never merge them into a third thing.

### F3.1 — JSON: one reader, one writer ★

**Current state — five readers, four writers, three escape tables:**

| Reader | Lines | Policy |
|---|---|---|
| `server/Json.java:708-907` | 908 | strict RFC-8259; **decimal → `double`** ← the bug |
| `sql/Json.java:33-180` | 181 | very lenient: `t`/`f`/`n` **prefix** match with blind index advance; decimal → `BigDecimal` |
| `compiler/spec/TdsChecker.java:229-371` | ~143 | validator only, **inside the type checker**; accepts leading zeros and `1.` |
| `parser/section/MongoDBSectionGrammar.java:154-218` | ~65 | token-level, no escape decoding, no floats |
| `exec/Executor.java:400-430` | 31 | JSON-string decoder; unknown escape **keeps** the backslash (opposite of `sql/Json`, which drops it) |

**F3.1a — fix `server/Json`'s decimal path first (highest severity, smallest change).**
`server/Json.parseNumber:874` returns `Num.ofDouble(Double.parseDouble(num))`. `sql/Json.java:167-170`
documents the exact bug: *"Decimal tokens parse as BigDecimal, NOT double (audit 18): two
distinct Decimals beyond 17 significant digits round to the SAME double, so a wrong Decimal wire
value would compare equal."* **The audit-18 fix was applied to one of two parsers with the same
name**, and `server/Json` reads every HTTP body and LSP message. Audit §5 A15.

**F3.1b — collapse `TdsChecker`'s validator into the platform reader.**
Replace `:212-371` with `try { Json.parse(v); return VARIANT; } catch (…) { return STRING; }`.
**Why it matters beyond duplication: the parser that decides the TYPE is not the parser that
reads the VALUE**, their acceptance sets differ, and the disagreement surfaces as a runtime cast
failure on a type decision Java already committed to. A single parser cannot disagree with
itself. Ledger the residual (`Json` is still lenient relative to DuckDB).

**F3.1c — one writer.** `server/Json.Writer`, `exec/ResultJson`, `protocol/ProtocolEmitter.str`
(uppercase `\u%04X` for Jackson parity), `server/serial/JsonSerializer`. Keep **one** escape
table. `ProtocolEmitter`'s byte-parity requirement is a legitimate constraint — make it a
parameter of the one writer, not a second writer.

**F3.1d — reconcile the two escape policies** in `exec/Executor.java:426` (keeps the backslash)
vs `sql/Json.str:150` (drops it). One of them is wrong; determine which against RFC-8259 and
delete the other.

**F3.1e — `MongoDBSectionGrammar`**: leave it. It is token-level, not char-level, and loud on
floats. Record the exemption with a reason.

**Acceptance:** `sql/Json.java:11`'s "one for the platform" claim becomes **true**, or the claim
is deleted. A single grep for hand-rolled scanning (`charAt` + `skipWs` + `parseValue`) finds
one owner plus documented exemptions.

**Expected red:** possible in `server/` tests if any pinned a `double`-rounded value.
**That red is the bug.**

> **LANDED ENDPOINT (2026-08-16, second pass — the Phase-3 deep audit caught c/d/e
> unlanded after F3.7's commit prematurely declared the phase complete):**
> **c)** the WRITE table lives once in `protocol/Escapes.jsonEscape(out, s, upperHex)` —
> the three spellings (server/Json.escapeTo, ProtocolEmitter.str, ResultJson.writeString)
> differed ONLY in control-escape hex case, so Jackson's uppercase is the one parameter;
> all three delegate (JsonSerializer already rode ResultJson). Byte parity pre-flighted
> (CorpusSweepTest green) before the chain.
> **d)** the READ table for string bodies is `sql/Json.unescapeString` (drop-backslash —
> the platform reader's and the Pure unescape family's shared terminal rule);
> `Executor.jsonUnescape`'s keep-the-backslash twin lost the adjudication and delegates.
> **e)** the MongoDB exemption is recorded AT THE SITE (token-level, no escape decoding,
> loud on floats). `sql/Json`'s header claim is now TRUE with two documented exemptions
> (server/Json strict HTTP reader; MongoDB). The five-readers table above is history.

### F3.2 — Substitution: delete the harness copy

**Files:** delete `core/src/main/java/com/legend/harness/HarnessSubstitution.java`; repoint
callers to `compiler/spec/SourceSubst.java`.

**Why:** two β-substitution engines, and **they disagree semantically**. The harness resolves a
`let`'s RHS **at the use site** (`:69-70`) — dynamic scoping — where `SourceSubst:50` substitutes
**at binding time** (lexical). `let a = $x; let x = 5; …$a…` yields `5` in the harness and the
old `$x` in the platform. The harness copy also lacks the self-referential cycle guard its own
sibling `ExecCallFinder:60-66` explicitly added, so a `let result = $result` shape
**StackOverflows**. `SourceSubst`'s javadoc already calls itself *"the compiler-side sibling of
the harness's inliner"* — **the fork is documented and nothing binds the halves.**
Audit §5 A8.

**Do first — the decisive cheap experiment:** run both over every corpus `[let*, final]` body and
diff. An empty diff retires the urgency; a non-empty diff names the bug and its blast radius.

**Acceptance:** one substitution engine. The quoted-code fold at `HarnessSubstitution:71-87`
(which hard-codes `Dialect.LEGEND_ENGINE`, something `SourceSubst:66-70` **explicitly refuses**
with a stated reason) is deleted, not ported.

> **RE-SCOPED after a reverted first attempt (2026-08-16).** The full swap+delete
> was tried and REVERTED under §0.3 rule 3: red exceeded the declaration —
> tds/tests fell 253→237 and M1 text-matches 325→291, because
> HarnessSubstitution is TWO things fused: the substitution ENGINE (duplicated,
> killable — F3.2a already killed its dynamic scoping at the binding sites) and
> a FOLD PASS (pair `.first/.second` projections + the late quote-fold) that
> ~16 tds tests and ~34 golden-text extractions genuinely consume. The plan's
> "deleted, not ported" underestimated the folds' blast radius. New sequence:
> LANDED ENDPOINT (same day): a fourth fused concern surfaced during
> extraction — ElqSplice.keyAlias inside the ColSpec arm, a src/test type
> the src/main owner structurally cannot call — so the folds cannot become
> a clean post-pass without platformizing them first. The honest landing:
> (1) SourceSubst GAINED the lambda-local-let shadow-stop the harness copy
> had right and the owner lacked (pinned in SourceSubstTest); (2)
> SubstitutionParityTest BINDS the two engines equal on the shared
> substitution semantics — A8's "nothing binds the halves" is answered
> mechanically; (3) HarnessSubstitution carries an explicit charter naming
> its three extras and their retirement owners (quote-fold → platform
> quote folding; pair fold → typed-level StaticFold; keyAlias →
> harness-coupled), with "new semantics go in SourceSubst, never here."
> Full deletion is BLOCKED on platformizing the extras — recorded, not
> forced.

### F3.3 — Multiplicity: delete the second engine

**Files:** `core/src/main/java/com/legend/harness/ReflectAsserts.java:89-204`

**Why:** `:120-122`/`:187-189` is **character-for-character** `compiler/spec/Typer.java:2635-2636`,
hand-maintained twice, and the harness copy runs over the **untyped** spec. It asks the value a
question the type already answers — the typed-fact tie-breaker, failed outright. Audit §5, §7.3.

**Acceptance:** the multiplicity walk is gone; `expressionSequenceReturnsAtLeastToOneDataType`
routes through the typed pipeline or becomes a named, ledgered gap.

### F3.4 — ASOR: delete the harness decode

**Files:** `core/src/main/java/com/legend/harness/ObjectRefs.java:88-198`, `:330-344`

**Why:** the platform already decodes ASOR references **inside SQL**
(`resolver/Substitution.java:1517`, `:2534`), and derives the prefix from the model
(`GraphEmission.asorPrefix:3324`). The harness re-implements both in Java, remapping positional
`pk$_i` keys to column names **by index** (consults magnitude → fails §6-Q5(b)), returns `null`
on six unrecognized shapes instead of throwing, and **hard-codes an H2 connection literal**
(`"type":"H2"`, `"timeZone":"GMT"`) so a golden that should expose a connection-shape difference
cannot. Audit §5 A11.

### F3.5 — Identifier quoting: one rule

**Files:** `exec/Ddl.java:37-54` (`createTable`, quotes everything),
`exec/Ddl.java:79-104` (`createTableStatementText`, quotes only H2 reserved words),
`exec/RawSqlBoundary.java:210-234` (`quoteCreateColumns`, whitespace-delimited head)

**Why:** **three coexisting rules; whichever runs last wins.** Traced at HEAD: a real corpus
column `"Previous Fiscal Week Year"` (`datePeriods.pure:699`) emits bare from
`createTableStatementText`, then `quoteCreateColumns` mangles it to
`"Previous" Fiscal Week Year`. **Java rewrites rather than failing.** Audit §5 A16.

**Change:** `createTableStatementText:89-91` quotes unconditionally, matching `createTable:50`.
Keep `H2_RESERVED` only if a golden genuinely pins unquoted non-reserved names.

### F3.6 — Escape tables: delete `Protocol.unescapeSegment`

**Files:** `core/src/main/java/com/legend/protocol/Protocol.java:3005-3031`

**Why:** its javadoc claims *"same rules as the parser's canonical
`TokenStreamCursor.unescapeBody`"*. **It is not** — `unescapeJavaLike:1039-1077` decodes octal
and `\uXXXX`; `unescapeSegment` has neither, both falling to `default -> sb.append(esc)`. So
`Class a::'xAy'` names the element `x` `u0041` `y` on the model side while the same escape
in a property name yields `xAy`. `TokenStreamCursor:665-668` records that refusing octal/`\u`
"was an invented divergence (adversarial-audit fuzz)" — **that fix landed in one copy only.**
Audit §5.

**Change:** delete the copy; route `splitFqn` → `unquoteSegments` through the owner. It wants no
error channel — give it a non-throwing wrapper rather than a second table.

**Acceptance:** add three `AdversarialParityTest` rows (`'xAy'`, `'x\101y'`, and the same
escape in property position) and compare against the reference engine.

### F3.7 — Rebuild the pardon ledgers on the one good shape

**Files:** `parser-equivalence/.../model-refuse-allowlist.tsv` (71 rows, no stale-row check, ≥1
provably dead row), `refusal-allowlist.tsv`, `version-skew-claims.tsv`,
`MutationFuzzTest.ADJUDICATED`, `OwnCorpusConformanceTest` per-class pins.

**Change:** give every ledger the shape `FixtureCorpusParityTest` already has (`:128-133`) — a
**stale-row assert** and **total accounting**.

**Why:** 196 named pardon rows are live, plus two *file-granular* allowlists with unbounded
capacity (`OwnDialectCensusTest:37-67` — `ElementParserTest.java`, 3,483 lines, is in both), plus
an unbounded lenient-fixture population under a 21-**kind** ceiling. Audit §7.5.

**Also:** delete `sibling-corpus/parity-quarantine.tsv` (8 rows, read by no Java file — dead data)
or wire it.

---

## 5. Phase 4 — The `RENDER` phase ★ the keystone

> **One missing pipeline phase generates the largest class of violations in the tree.** Do this
> after Phases 1–3 so the guards and the single JSON writer are already in place.
>
> **DESIGN DECISION (2026-08-16, supersedes the first draft): rendering EXECUTES IN THE
> DATABASE.** The first draft's `com/legend/exec/render/` Java renderer would have
> consolidated five print-form copies into two and then needed a permanent differential test
> to bind them — recreating the §2 disease at smaller scale. The DB-side design consolidates
> to ONE. The precedent is already certified in this tree: audit §4.3.6 — "*Graph results
> pass through untouched — the JSON is built by the database*" (the M2M/graph-fetch design:
> the Lowerer emits SQL that CONSTRUCTS the serialized artifact; Java carries bytes). This
> phase generalizes that to CSV/TDS/Pure-print. It also dissolves A15's class outright: a
> DECIMAL(38,10) rendered by `to_json` in the DB cannot be destroyed by `Double.toString`,
> because Java never touches the value.

### F4.1 — RENDER = render lowerings + a plan-wrapping step (DB executes)

**Files:** render rules in `lowering/` (beside `Scalars.floatRepr`, where the print forms
already live); a plan-wrapping step at the orchestration seam; `error/LegendCompileException.java:27`

**Change:** `RENDER` is a PLAN-CONSTRUCTION phase: when a format is requested (HTTP param,
assert spelling, `toCSV` in query position), the orchestrator wraps the compiled plan with a
render projection — row-wise `concat_ws` for CSV lines (streaming-friendly: one VARCHAR
column, `Executor.stream` finally matters), `to_json`/`json_object` shapes for JSON (the
graph-fetch machinery, generalized), the existing `floatRepr`/`DateFmt` emissions for the
Pure print form. Add `RENDER` to `Phase` as a plan phase.

**The Java residue, chartered (F0.2 clauses 1 and 4):**
1. **Envelope + headers from typed plan facts** — CSV header row, JSON envelope brackets,
   TDS schema line: emitted from the compiled plan's column names/types (model-space,
   "types drive construction"), never from values.
2. **The LiteralFold-admitted identity render** — Java may render ONLY literal kinds with no
   representation rule (`ADMITTED = {String, Boolean}` today). Integer/Float/Decimal/Date
   render in the DB even as literals, because their representation rules live in the SQL
   path — the exact reason `LiteralFold` rejects them ("folding those would duplicate a
   coercion rule in a second place"). Widening = a green differential in the
   `ConstantPlanParityTest` pattern, never an argument.
3. **Byte transport.**

**Reuse, do not invent:** `Scalars.floatRepr:3170-3209` and `DateFmt.ISO_PURE_UTC` are the
correct print forms and already exist — in SQL. `Lowerer.java:2674-2688` already flattens the
relation shape; `Scalars.java:2645-2660` builds a nested-`CONCAT` render; graph fetch already
builds JSON DB-side end-to-end.

**Why (unchanged from the audit):** `Phase { PARSE, RESOLVE, NORMALIZE, MODEL, TYPE, MAPPING,
LOWER, EXECUTE }` has no `RENDER`, so the Pure print form of a value is implemented five
times by parties that do not know about each other; a `DECIMAL(38,10)` survives CSV and is
destroyed by JSON; the platform's float printer is untested by all 1,109 PCT tests. Audit §2.

**Sequencing note:** this design is MORE disruptive up front than a Java renderer — every
egress consumer changes its consumption model in one arc rather than swapping a formatter
behind an interface. That is why F4.2b (compare-and-log) is mandatory before F4.3, and why
Phase 5 (type fidelity) stays a hard prerequisite: the render wrapper renders BY the plan's
typed columns, so they must be right first.

### F4.2 — Register `toCSV` and relation `toString` as platform lowerings

**Why:** `toCSV` has **no owner anywhere in `com.legend`** — it exists only inside the harness,
which self-describes at `:3248` as *"RFC4180 cell rendering (the engine's toCSV)"* — yet
**165 of 2,070 corpus test functions (8.0%) pass over it.** Audit §2, §4.

**Also narrow the `toString` overload.** `builtin/Pure.java:2054`'s `toString(any: Any[1])`
still accepts a `RelationType` because `Any` is unchecked in both kernel halves
(`InferenceKernel.java:75`, `:961`, `compatibleRebind:1287-1289`). The mis-lowering is already
fixed — `Scalars.pureToString:2790-2791` **throws** — but the fix fires in phase LOWER, not TYPE.
Narrow the overload so it fails at TYPE. **Do not "fix" `pureToString` by adding a cast; that
throw is the single best fix since round 1.**

### F4.2b — The CSV differential probe ★ run this before F4.3, it de-risks the whole phase

**Files:** a temporary probe in `core/src/test/java/com/legend/harness/` (delete after use);
no production change.

**Change:** with `toCSV` now registered (F4.2) but the strip **still in place**, run both paths
over every corpus `toCSV` assertion and diff them:

- **side A** — what the harness renders today: `csvCell`/`csvJoinedEquals`' view of the `Tabular`
- **side B** — what the platform now produces: execute the *unstripped* expression through
  `Compiler.execute` and take the returned string

Emit one row per assertion: test FQN, equal/differs, and on a difference the first divergent
line with both spellings.

**Why:** this is the same de-risking trick as F5.3 Stage A — **compare-and-log before
compare-and-fail.** It converts F4.3 from "delete and see what burns" into "delete against a
known work list." Three specific unknowns it settles:

1. **The true size of F4.3's red.** The declared "up to 165 test functions" is a worst case
   assuming the platform's render disagrees everywhere. The probe replaces that guess with a count.
2. **`Scalars.floatRepr`'s first real workout.** It is currently exercised by **neither** the
   corpus (the harness renders instead) **nor** PCT (which uses Java's `BigDecimal.valueOf`,
   a *more correct* path). 246 corpus `toCSV` occurrences across 32 files are about to become
   its first genuine test. **Expect divergences here and budget for them** — they are fixable
   bugs in one function, and finding them is the point.
3. **Which of the four leniencies are actually load-bearing.** Run the probe twice — once
   comparing exact, once comparing as a line multiset — and the delta is exactly the set of
   assertions that genuinely depend on row-order tolerance.

**Acceptance:** a committed work list (`docs/CSV_DIFFERENTIAL.md`) with a row per assertion and
a total. **F4.3 must not start until this exists.**

**Expected red:** **none — the probe is read-only and the strip is untouched.**

**Gate:** full referee (should be a no-op delta).

### F4.3 — Delete the harness renderer and the strip

**Files:** `EngineTestExecutor.java:2579-2602`, `:2603-2616` (the strip),
`:3136-3302` (`tdsStringEquals`, `csvText`), `:3248` (RFC4180 cell)

**Why:** the strip intercepts `toCSV`/`toString` tails, executes only the receiver, and renders
in Java — which is what keeps the platform gap invisible. It also **downgrades an exact string
equality to a tolerant structural grid comparison**: what the corpus wrote as
`assertEquals('<literal csv>', …)` becomes header-pinned token match + unordered row multiset +
`Double.parseDouble` on **both** sides + 1e-11 relative tolerance, so `'007'` equals `'7'` and
`'1e3'` equals `'1000'`. **Four leniencies replacing one exact test.**

**Keep the comparison policy; delete the renderer.** That is the conflation to avoid.

**Expected red:** **exactly the differing rows F4.2b listed — no more.** The worst case is 165
test functions (concentrated in `calendarAggregation`, 41 functions currently reported 92/92
PASS), but that figure assumes the platform's render disagrees everywhere; **F4.2b replaces it
with a count before you start.** Budget a full cycle.

**Revert if:** red exceeds F4.2b's list, or reaches a family with no `toCSV`/`toString`.
Either means the strip was load-bearing for something nobody knew about.

**What to keep, and what to drop** — the strip bundles four leniencies and they do not share a
fate:

| Leniency | Fate | Why |
|---|---|---|
| Row order → line multiset | **KEEP as comparison policy** | The corpus genuinely wrote a string-equality assert against an unordered `groupBy`. That is a property of the test, not a platform gap. `endsInSort(orderView(…))` already distinguishes ordered chains and carries over untouched |
| Float tolerance | **KEEP, bounded and counted** | Non-associative double summation is real (`testPwaValue`). But it **shrinks**: today it absorbs any float-print divergence; afterwards printing is `floatRepr`'s job, so it need only cover genuine drift. `LL_TOL_COUNT` already measures it |
| Header pinning | **DELETE** | The platform emits the header now. A divergence is a finding |
| Cross-kind collapse (`'007'` == `'7'`, `'1e3'` == `'1000'`) | **DELETE** | Justified by nothing in the comments — a side effect of `Double.parseDouble` on both sides, which `wireEquals:3325-3335` refuses 200 lines away |

**The principle: keeping a structural comparison does not require keeping a renderer.** That is
the conflation to avoid, and it is why the 165 are recoverable rather than lost.

### F4.4 — Route the other renderers through the owner

`pct/.../ExecuteLegendLiteQuery.formatValue:678-707` (fixed 3-digit subseconds where
`DateFmt.SUBSEC_MIN` is minimal; a hard-coded `"+0000"` on a zone-less `LocalDateTime`);
`exec/ResultJson.java:87`; `server/serial/CsvSerializer.java:78-83`.

Under the DB-side design these are DELETIONS, not reroutes: PCT's `formatValue` consumes the
DB-rendered text directly (so PCT finally exercises `floatRepr` with no adapter between);
the server serializers become envelope-plus-byte-transport around DB-rendered columns. The
permanent differential test shrinks to covering only the LiteralFold-admitted identity arm.

**Acceptance:** ONE implementation of every value print form, and it runs in the database.
PCT exercises `floatRepr`.

> **ATTEMPTED AND REVERTED (2026-08-17), design lesson recorded:** a post-hoc
> plan rewrite (`RenderOption` ThreadLocal + `Render.pctPrintCells` over the
> FINAL SqlQuery at the orchestrator seam) was built and iterated through ~10
> shape classes — empty-projection `starOf` selects, star/`StarExcept`
> expansion, VALUES-backed selects (the dialect COLLAPSES a Subselect over
> VALUES), DuckDB lateral projection aliasing (extend chains), order-scope
> hoists — each fix surfacing the next structural interaction. The measured
> conclusion: the final plan's construction discipline cannot be safely
> rewritten FROM OUTSIDE. The correct integration is a LOWERER ROOT MODE
> (the `withStreamingGraphRoot` precedent): the print projection emitted at
> root construction where aliases/scopes/order are still owned. Also
> measured on the way: the PCT wire's DateTime spelling is fixed-3-millis
> `+0000` (the upstream deephaven parser's accepted form — minimal
> subseconds demote the column to STRING), and an abstract-Date slot needs
> `typeof()`-style column reflection because OutputCol's slot claim is
> unreliable (the mechanism-3 deviation). F4.4 proceeds as its OWN leg with
> the Lowerer-mode design; Phases 6-8 do not depend on it.

---

## 6. Phase 5 — The result bridge (type fidelity)

> Evidence gathered 2026-08-16: `Type.Column(name, type, multiplicity)` — the compiler carries
> multiplicity, **required and non-null**. `exec.Column(name, sqlType, pureType)` — the bridge
> **drops it**. That is the gap.

### F5.1 — PCT: `sqlType()` → `pureType()` (isolated, do first)

**Files:** `pct/.../ExecuteLegendLiteQuery.java:635`, `:645`; delete `pureTypeName:657-676`

**Why:** `sqlType()` has **exactly two consumers in the entire tree, both in PCT.**
`pureType()` already exists, is already populated, is already read by `H2Verify` (4 sites), and
is already pinned by `ExecutorTest` (`assertEquals(Type.Primitive.STRING/INTEGER, …)`). This is
a pure call-site swap with **no platform change**.

**Hypothesis to test here:** the one concealed defect we caught — declared `Float[0..1]`, wire
said `Decimal[1]` — arrived through `pureTypeName(col.sqlType())`, i.e. the SQL-type sniff
(DuckDB reports `DECIMAL`, the table maps it to `"Decimal"`). **If `col.pureType()` says `Float`,
this task alone dissolves that case.** A good fraction of the "concealment inventory" may be
self-inflicted rather than a platform defect. Cheap and decisive.

**Expected red:** none, ideally — and if some, they are the inventory arriving early.

### F5.2 — Carry multiplicity across the bridge

**Files:** `core/src/main/java/com/legend/exec/Column.java`; its population site in `Executor`

**Change:** add `Multiplicity multiplicity` to `exec.Column`, populated from
`Type.RelationType.columns()`.

**Why:** it is computed and then discarded. **Additive and low-risk:** nothing reads multiplicity
today because it does not exist, so existing corpus behaviour cannot change.

**Expected red:** none. **Revert if:** any corpus family delta is non-zero.

### F5.3 — PCT: delete the null-scan and the header overlay ★

**Files:** `pct/.../ExecuteLegendLiteQuery.java:628-636`;
`pct/src/main/resources/core_legend_lite_pct/pct_adapter.pure:88-128`, `:285-294`

**Run this in two stages.**

**Stage A (read-only, do it before the burn-down baseline is ever re-frozen):** change
`pct_adapter.pure:286-294` to **compare** the declared header against the wire header and
**log** the mismatch instead of overwriting. Run the suite. **The mismatch list is the exact
concealment inventory** — every wrong column type, multiplicity, and name PCT is currently
unable to see.

**Stage B:** delete the overlay and the null-scan; take type and multiplicity from
`col.pureType()` / `col.multiplicity()`; adjudicate each red as a real gap, exactly as
`SqlPostProcessors.java:64-73` did for the 7 cteExtraction tests. **Do not fix them by
re-widening the overlay.**

**Why:** the adapter replaces the first line of the DB-produced TDS with a header built from the
test's own declared return type. The discarded header crossed a `ResultSet`; the replacement is
the **expectation**. **405 results overlaid; 322 of 405 have their multiplicity rewritten.**
PCT cannot currently detect a wrong column type, multiplicity, or name. Audit §4.1.

**Expected red:** unknown until Stage A. **That is why Stage A exists.**

### F5.4 — Split `DATE` from `DATE_TIME`, kill the midnight heuristic

**Files:** `lowering/PureSql.java:70`; `exec/Executor.java:337-350`

**Why:** `case DATE_TIME, DATE, LATEST_DATE -> SqlType.Scalar.TIMESTAMP` erases the kind, which
is why `Executor` needs a heuristic that asks whether the time-of-day is midnight — reading the
cell's **magnitude**, failing §6-Q5(b). **A genuine `DateTime` at exactly `00:00:00` under a
`Date`-typed root is silently returned as a `StrictDate`.** Carry the kind as a typed fact.
Audit §5 A10.

### F5.5 — PCT: make the permissive fallbacks throw

`:572-573` (`return coreInstances.get(0)` — comment: *"this shouldn't happen … but return first
as fallback"*), `:576`, `:674`, `:541-550` (date classifier reading text precision). Scope the
five unconditional null-drops (`:258-261`, `:244-246`, `:312-314`, `:326-329`, `:766`) **by
channel**, following `d0f3a356`'s precedent — the unscoped version was already proven wrong in
the corpus harness (`tds/tests 248→235`).

### F5.6 — PCT: the free deletions (can be done any time, zero risk)

Delete `inlineFunctionLiterals` (`:147`, `:1110-1123` — fires **0 of 2,473 executions**, a live
`DOTALL` regex over every query with no test coverage), the dead `removeDuplicates::cmp` body
(`:109-112`), and `createClassInstance:726-793` (68 lines, `LIKELY` unreachable, whose
`default -> "Pair"` fabricates a type for any unknown struct — prove reachability or delete).

### F5.7 — PCT: replace the hardcoded support functions

**Files:** `pct/.../ExecuteLegendLiteQuery.java:77-113`

**Change:** add `extractFunctionDefinitions`, mirroring the `extractClassMetadata:848-897` /
`extractEnumDefinitions:806-846` mechanism **already in the same file**.

**Why:** five verbatim copies of legend-engine PCT support-function bodies keyed by exact FQN —
an **unversioned fork of third-party test source**. If upstream changes `filterValues`, we
silently keep testing the old definition and stay green.

---

## 7. Phase 6 — Compensation removal (the baseline movers)

> Each of these converts a **false PASS** into an **honest `explained` row**. In burn-down's own
> scheme (`2793 = passed + explained`) that is the same distance from done — but true. Write the
> `BURNDOWN_EXPLANATIONS.md` verdict **in the same commit**.

### F6.1 — Stop fabricating execution activities ★

**Files:** `StatementExecutor.java:2592-2600`, `:2622-2670` (esp. `:2649-2651`)

**Why:** `$r.activities…comment` returns a Java-manufactured string containing
`java.util.UUID.randomUUID()`; `$r.activities` folds to `[]` and a `filter` over it folds to
empty **without evaluating the predicate**. The code names the asymmetry itself: absence-asserts
**pass for the wrong reason** while presence-asserts fail. **71 occurrences / 24 corpus files.**
Audit §5 A1.

**Change:** delete both; let the reads fail with
`NotImplementedException("execution activities are not recorded")`.

**Expected red:** up to 71 reads. Adjudicate as blocked-on-feature.

**LANDED ENDPOINT (2026-08-17):** both fabrications deleted — the `[]` fold (and its
filter-over-activities variant) and the UUID trace comment; those reads now wall with
`NotImplementedException("execution activities are not recorded")`. The aggregationAware
`rewrittenQuery` arm SURVIVES: it is a derived read (routed print recomputed from the frame's
actual chain via `AggAwareActivities`), not fabrication — which is why the realized red is
**13**, not 71 (the derived arm still answers the rewrite-print asserts). Deltas, all the wall:
`aggregationAware/test/rewrite` 13→9, `…/rewrite/NOP` 15→7, `functions/tests` 238→237
(`testSQLComments`). Verdicts in docs/BURNDOWN_EXPLANATIONS.md (blocked-on-feature); corpus
total 2347→2334, scoreboard re-frozen. Un-red path = recording REAL activities (§9 backlog).

### F6.2 — Delete the `u_map__` null strip

**Files:** `EngineTestExecutor.java:2507-2521`

**Why:** filters NULL rows out of a `ResultSet`-crossed collection when the column name starts
with the platform's own synthetic map-binder prefix (hardcoded as a literal rather than imported
from `SqlSelect.SYNTH_MAP_COL`). `assertEquals([1,2], …)` passes when the DB returned
`[1, NULL, 2]` — **arity repaired before comparison.** Audit §5 A2.

**Fix belongs upstream:** the map-binder lowering should not emit a row where Pure semantics
demands absence (a `WHERE col IS NOT NULL` or a nullability-aware projection).

**Expected red:** ≥16 (its own comment records `tds/tests -13, tree -3` for a different scoping).

### F6.3 — Remove `coerceTemporal`'s side-agnostic coercion

**Files:** `EngineTestExecutor.java:2501-2506` → `H2Verify.java:301-323`

**Why:** `wireEquals:3368-3376` **explicitly refuses** to bridge "an ACTUAL that comes back as a
string where the engine returns a Date … a TYPING BUG this compare must catch, never bridge."
`coerceTemporal` runs one layer above, is **side-agnostic**, and converts that String to a
`Timestamp` before the refusal can fire. **The harness's own best discipline, defeated from
above.** Audit §5 A3.

**Probe first:** instrument `H2Verify.java:315` to print the declared type, value, and side. Any
hit on a side that came from an `execute()` binding is a masked typing bug — likely fixed by F5.4.

**LANDED ENDPOINT (2026-08-17):** the probe ADJUDICATED, then the coercion was RESCOPED, not
deleted. Firings: DuckDB full sweep **0** (nothing masked on the scoreboard backend); h2 full
sweep **71**, every one the **JSON collection carrier** — H2 has no native list type, so
collection reads ride `JSON_ARRAYAGG`, and JSON has no temporal types (verified by SQL dump:
`SELECT (SELECT JSON_ARRAYAGG(t1."from" …))` on testMilestoningColumnProjectionForRoot). That
decode is a carrier convention, not a masked typing bug. Fix: the side-agnostic
`coerceTemporal` wrappers in `Eval.values()` are GONE; the decode now lives ONLY in
`Eval.flatten`'s `byte[]` JSON-carrier branch (the one arrival whose provenance proves the
carrier typeless), so a String-where-Date on ANY other path now reaches `wireEquals`'s
typing-bug refusal — the audit's demand, honored without breaking the carrier. Referees: DuckDB
scoreboard byte-identical, h2 sweep 2282/2575 (identical to pre-change), full chain GREEN.

### F6.4 — Fix `hostEquals`'s numeric arm (one line)

**Files:** `exec/HostEval.java:244-247`

**Why:** compares two `Number`s via `longValue()` when neither is a `Double`, so
**`hostEquals(1.5, 1)` is `true`** — on the live assert path (`EngineTestExecutor:3316`). Use
`BigDecimal.compareTo` or exact comparison. Audit §5 A4.

### F6.5 — Fix `csvRowEquals`'s cross-kind collapse — and A7, assigned here

> **A7 assignment (2026-08-16, deferral review):** JsonAssertCanon's lexical
> row sort was in the Phase-8 tail with no owner; it is the same
> comparator-quality class and dies with this task. Its F1.4 allowlist row
> deletes with the fix.

**Files:** `EngineTestExecutor.java:3093-3095`; also `H2Verify.java:568`, `TdsEquivalence.java:71`

**Why:** three independent comparators fall back to `Double.parseDouble` or `String.valueOf` on
**both** sides, so VARCHAR `'007'` equals INTEGER `7` and VARCHAR `'1'` equals INTEGER `1` —
a collapse the primary `wireEquals:3325-3335` **explicitly refuses**. Whether an assert gets the
strict comparator or a permissive one is decided by **which spelling the test happened to use.**

**LANDED ENDPOINT (2026-08-17):** three sites adjudicated, two fixed, one already dead:
`csvRowEquals` was DELETED by F4.3b's render cutover (nothing left to fix);
`TdsEquivalence` cell fallback `String.valueOf(x)==String.valueOf(y)` → `Objects.equals`
(same kind, same value — no cross-kind bridge); **A7** `JsonAssertCanon.sortByKey` lexical
sort → pure `sortBy` comparator semantics (numbers numerically, strings lexically, mixed
kinds WALL). The A7 sort SITE stays allow-listed — it re-creates the TEST'S OWN
`^JSONArray(values=…->sortBy(getValue('K')))` canonicalization (the JSON metamodel never
executes through the SQL pipeline), so the site is the test's, not harness compensation;
the discipline ledger's "LEDGERED VIOLATION" label is retired to RESOLVED.
`H2Verify.norm` was inspected and STAYS: it is the cross-ENGINE oracle channel (two live
engines, different SQL texts — database-level VALUE equality is its documented contract;
the audit's `Double.parseDouble` was already replaced by the exact-integral/10-sig-digit
BigDecimal arms in H2_BACKEND §12). Referees: DuckDB scoreboard byte-identical, full
chain GREEN.

### F6.6 — Re-site the `executeInDb` READ path

**Files:** `exec/HostEval.java:376-382`, `exec/DbMetaData.java:89-99`, `:104-127`

**Why:** answers from a **fresh throwaway H2** reconstructed by replaying recorded statements —
CSV seeds and generator inserts are **absent** from that shadow DB, and rejected statements are
**skipped** (`:114-123`), so it can silently answer from a partially-populated database. It
**replaced a loud refusal** to gain **one test**. Audit §5 A9.

**Change:** route reads to the ambient connection (`Executor.executeRaw` already owns raw
statements) or restore the refusal and ledger the one test. Make the replay skip **throw**.

**LANDED ENDPOINT (2026-08-17):** reads re-sited to the AMBIENT session (`HostEval.Ambient`
record bound by the full entry; both StatementExecutor call sites pass it; the read refuses
loudly without one). `DbMetaData.query` now executes on the supplied connection — the shadow
replay for reads is DELETED; the metadata-channel replay that survives THROWS on a rejected
statement (the old skip printed and answered from a partial shadow). The raw boundary gained
ONE naming rule: an unaliased `count(*)` projection item aliases to H2's observable
`"COUNT(*)"` (witness: ddl::dropAndCreateTable reads `.value('COUNT(*)')`; DuckDB names the
column `count_star()`) — naming is raw-H2 observable behavior, squarely the translator's
charter. Referee: +1 GAIN (`H2Test` — its read ERRORED only because the shadow H2 rejected a
boolean-vs-varchar comparison the ambient session executes; declared in
BURNDOWN_EXPLANATIONS), corpus 2334→2335; h2 sweep 2282/2575 unchanged; the two old
`h2-meta-replay skip` lines on DuckDB sweeps are gone (those replays no longer happen).

### F6.7 — Fix the H2 extension wiring gap

**Files:** `harness/H2Verify.java:273` (`aliases()` is called in the *fresh-replay* branch only)

**Why:** `Runner.java:1855` makes the incremental **mirror** the default path for DuckDB sweeps,
and the mirror never registers the aliases — so the C1 fix this class exists for is not in effect
on the default path.

**LANDED ENDPOINT (2026-08-17):** `H2Verify.mirrorBegin` now registers
`H2ExtensionFunctions.aliases()` on the mirror connection before attaching it — the mirror and
the fresh-replay branch install identically (a failed registration is a loud
IllegalStateException, never a silent partial mirror). Measured delta today: ZERO (both sweeps
carry no `legend_h2_extension_*` declines — h2-exec 320+632/0/155 identical before and after),
so this is wiring parity, not a rescue: the gap would have surfaced the moment an
extension-calling golden hit the default mirror path. Full chain GREEN.

### F6.8 — Fix the emptiness-guard ordering hole

**Files:** `EngineTestExecutor.java:1944-1953`

**Why:** `assertSize` checks the carrier arm **before** the `emptinessUnverifiable` guard, so a
non-TDS result with failed seeds yields `av.size() == 0` against an expectation of 0 — a
**hollow PASS** bypassing the guard the file installs everywhere else. Move the guard above
`:1944`. (17 corpus asserts of shape `assertSize(…, 0)`; ≥4 over an exec-chain variable.)

### F6.9 — Fix the FQN-spelled assert polarity

**Files:** `EngineTestExecutor.java:1809`, `:1859`

**Why:** dispatch is by `simpleName` but the polarity decision reads the **full** name, so an
FQN-spelled `assert` demands its predicate be **false** and an FQN-spelled `assertNotEquals` is
evaluated as `assertEquals`. Blast radius today is **0** (the corpus spells all asserts bare) —
but `:3410-3412` already fixed exactly this class elsewhere and documented it. Two sites missed.

---

## 8. Phase 7 — Ingress, atomicity, SQL text

### F7.1 — Transactions ★ the highest-leverage deletion in the tree

**Files:** the seed paths — `exec/CsvSeed.java`, `exec/Ddl.java`,
`testdatagen/TestDataGenerator.loadSide`, `rcorpus/Runner.java`

**Why:** `grep -riE "setautocommit|\.commit\(\)|\.rollback\(\)|savepoint"` over the whole repo
returns **one hit, and it is a reserved-word string list.** Every seed runs autocommit, so a
mid-seed failure leaves the DB half-populated — which is precisely why
`rawSqlFailureSink`, per-statement tolerate-and-continue, `RawSqlBoundary.unrecordLast()`,
`Runner.failedSeeds`, and `emptinessUnverifiable` had to be invented. **That apparatus exists to
downgrade a red assertion to advisory.** Audit §5.2.

**Change:** wrap each seed unit in `setAutoCommit(false)` … `commit()`/`rollback()`. Then
**attempt to delete** the entire apparatus. Whatever refuses to delete is a real gap that must be
**named**.

**Expected direction: GREEN.** This converts unverifiable → verifiable. It is the one task in the
plan likely to *raise* the honest pass count.

**LANDED ENDPOINT (2026-08-17):** adjudicated in two halves.
*Transactions:* already BUILT AND MEASURED by the perf program (task #14, recorded at
`Runner.openSession`): DML txn-batching saved zero (DuckDB's per-statement cost is
parse+plan+JNI, not commit), and the DDL-bearing variant hit a **DuckDB 1.1.3 native abort**
(`DuckTransaction::Commit → std::terminate` through JNI — kills the JVM). Seed atomicity via
transactions is NOT available on the primary backend at this engine version; revisit on DuckDB
upgrade (§9). A failed CREATE is per-statement atomic already; the half-populated-DB window the
audit feared is otherwise covered by fail-loud below.
*The apparatus:* measured, then deleted to its one honest residue. The `rawSqlFailureSink`
per-statement tolerance channel fired ZERO times on both full sweeps — DELETED end to end
(Compiler overload, ExecEnv field, executeInDb + runtime-setups + print-effectful arms, the two
harness sink args): a failed raw statement now THROWS; the runner's per-SETUP-UNIT catch still
records into `failedSeeds`, so `emptinessUnverifiable` keeps its (stricter-grained) feed. The
module-DDL loop's catch NARROWED to exactly one named gap: `already exists` — a same-named
table from another database's DDL already living in the family session (6 on the h2 sweep, 0 on
DuckDB; all six spelled in the sweep log via the new seed-fail detail print) — anything else is
a loud `IllegalStateException`. Referees: DuckDB scoreboard byte-identical, failed seeds 0;
h2 2282/2575 with the same 6 named rows. The plan's GREEN prediction did not materialize as new
passes (the unverifiables were not seed-gated on the referee backend); the honest gain is the
deleted tolerance surface.

### F7.2 — CSV values: copy the fix that already exists

**Files:** `exec/CsvSeed.java:93-107`; `harness/EngineTestExecutor.java:2732`

**Why:** types each CSV literal from **a regex on the token's text** (`:101`) while holding the
resolved column type in scope (`:61-79`) — the typed-fact test, failed. The DDL side was fixed
(`ddlType:135-136` now throws); **the value side was not.** Worse:
`EngineTestExecutor.java:2732` calls `CsvSeed.sqls(csv, null, ctx)`, so `ddlType` is **never
called** and the T3.1 loudness is bypassed entirely.

**The correct policy is 1,000 lines away, by the same project, under the same remediation ID:**
`TestDataGenerator.loadSide:1194-1226` — *"literals and the DATABASE casts them to the model's
column types (uniform policy — no host-side type dispatch)"* — with `duckType:1272-1300`'s
explicit-per-variant switch. Copy it, or switch to a typed `PreparedStatement`.

**Expected red:** real, where the regex was papering over a type mismatch. **Those reds are the
finding.**

**LANDED ENDPOINT (2026-08-17):** the value-side regex is gone — every CSV value now rides as
a QUOTED literal and the database casts it to the model's column type (the exact
`TestDataGenerator.loadSide` policy, copied). Realized red: ZERO on both full sweeps (DuckDB
scoreboard byte-identical at 2335, h2 2282/2575) — the regex was live host-side type dispatch
but was not currently papering over any mismatch. The `:2732` `sqls(csv, null, ctx)` complaint
was adjudicated STALE: null dbFqn deliberately selects the DELETE+INSERT branch — the inline
`testDataSetupCsv` override seeds over family-DDL tables that already exist, so `ddlType`'s
loudness is not bypassed, it is out of scope by design (documented at the call site). Full
chain GREEN.

### F7.3 — JSON ingress through the database

**Files:** `resolver/JsonSourceFrame.java:75`, `:109-119`

**Why:** parses the `data:application/json` payload in Java, then `String.valueOf(v)` **erases
every JSON type back to text**, which `Scalars.tdsCell` re-parses (`Long.parseLong`,
`new BigDecimal`, `Boolean.parseBoolean`). Concrete bugs: a JSON string `"null"` becomes SQL
`NULL`; a nested object becomes Java's `{a=1}`, not JSON.

**The DB path exists and is fully wired end-to-end** and is simply not used:
`SourceUrlChecker.java:38` → `Lowerer.java:323-324` → `SqlSource.SourceUrl` →
`DuckDb.java:171-185` (`unnest(CAST(… AS JSON[]))`). 27 occurrences / 5 files. Audit §5 A12.

**LANDED ENDPOINT (2026-08-17):** both concrete bugs fixed AT THE PRODUCER; the full SourceUrl
re-platform is scoped and filed (§9). `JsonSourceFrame.cellText` now types every cell by the
DECLARED column: a **Variant** property IS a column (the frame no longer skips it with the
class-typed ones) and its value re-emits as REAL JSON via `ResultJson.jsonText` (quote-wrapped
for `Scalars.tdsCell`'s variant arm — nested objects never ride `Map.toString` again); a
structured value under a SCALAR column WALLS; a JSON string spelling the grid's null tokens
(`"null"`, empty, `---null---`) WALLS instead of silently becoming SQL NULL — the
`List<List<String>>` grid genuinely cannot carry it, which is the honest boundary of this
carrier. Four pins in `JsonSourceFrameCellTest`. Residue → §9: realize the frame OVER
`SqlSource.SourceUrl` + typed variant extraction (`variant::navigation::get` +
`variant::convert::to` exist; the missing piece is the FRAME_ORDINAL row-identity channel in
the sourceUrl spelling). Referees: DuckDB scoreboard byte-identical, full chain GREEN.

### F7.4 — End the self-inflicted SQL rewrite loop

**Files:** `exec/Ddl.java:323` (`Float_ -> "FLOAT"`), `exec/RawSqlBoundary.java:245-249`
(`mapColumnTypes`) and its three `Pattern` fields (`:111-115`);
`StatementExecutor.java:3352`, `:3354`; `rcorpus/Runner.java:1942`

**Why:** `Ddl.spell` emits H2 `FLOAT` **on purpose, with a comment saying so**, *so that*
`RawSqlBoundary` can regex it to `DOUBLE`. Java renders SQL from a typed model, throws the type
away into text, then pattern-matches the text to recover it. The root sentence is
`Ddl.java:15-18`: *"ONE adaptation path for hand-written and model-derived DDL alike."*
**That sentence is the bug** — hand-written text needs adaptation because its origin is another
dialect; model-derived text should be spelled correctly the first time. Audit §3 S4.

**Change, in order:** (a) `Ddl.spell` emits `DOUBLE`/`BOOLEAN` directly; delete `mapColumnTypes`
and the comment that justified it. (b) Route Java-generated DDL to `Executor.executeRaw`
**directly**, exactly as `dropAndCreateSchemaInDb:3316` already does — proving sites 3–4 never
had to route through the boundary. (c) Record on the H2 mirror via `recordMeta`
(`RawSqlBoundary:66-77`), the side channel **built for exactly this**, whose javadoc already
names the downgrade risk. (d) Retire `RawSql.splitStatements`, which mis-splits on `--`, `/* */`,
and `"`-quoted identifiers containing `;`.

**Acceptance:** `RawSqlBoundary`'s stated contract becomes **true for the first time**, and
F1.6's ledger drops to its one legitimate entry.

**LANDED ENDPOINT (2026-08-17):** the loop is dead. `Ddl.createTable(def, schema, duckTarget)`
spells the EXECUTION form for its target from the TYPE (`duckSpell`: FLOAT→DOUBLE, BIT→BOOLEAN
— exactly where H2's type semantics differ from its names); the H2 flavor survives as the
mirror-replay/h2-backend spelling. Both model-derived channels route DIRECTLY to execution
(`dropAndCreateTableInDb` via `Executor.executeRaw`; Runner's module-DDL loop executes the
pre-spelled unit), the H2 advisory mirror gets its stream by SPELLING THE MODEL A SECOND TIME
— recorded only after the session executed, so the recording still mirrors executed reality —
and `splitStatements` left the model-derived path (each unit IS one statement). Adjudications
against the plan text: `mapColumnTypes` STAYS — its witness (`relationalSetUp` testTable's
literal `float` column) is HAND-WRITTEN corpus text, the translator's actual charter; the
loop-justifying comments died instead. `recordMeta` was NOT the right channel for CREATEs —
the row-replay mirror needs the tables (only constraints ride the meta channel, as before).
F1.6's ledger: Runner LEFT — now `{StatementExecutor, HostEval}`, both corpus-authored text
(the F6.6 read direction joined after the plan was written); the boundary's stated contract is
true at last. Referees: DuckDB scoreboard byte-identical, h2-exec 320+632/0/155 identical, h2
sweep 2282/2575 identical, full chain GREEN.

### F7.5 — Batch the seeds

`CsvSeed.java:85-108`, `Ddl.setUpDataSqlsText:157-162`, `TestDataGenerator.loadSide:1212-1226`
(twice per table), `Runner.java:1934-1956`. Multi-row `INSERT … VALUES` or `executeBatch()`.
Also fold the effectful `map` arm (`StatementExecutor.java:3174-3185`), which materialises a
collection, calls `executeTyped` per element, and **keeps only the last result**.

**LANDED ENDPOINT (2026-08-17):** batched where the platform OWNS the text, then measured —
and the measurement closes the item. `CsvSeed` emits ONE multi-row `INSERT … VALUES` per
block; `TestDataGenerator.loadSide` likewise (both H2-mirror-replayable and DuckDB-valid).
`Ddl.setUpDataSqlsText` is EXEMPT — it is the golden-asserted engine TEXT surface, not an
execution stream. Runner's module DDL was already one-statement units after F7.4. Measured on
the full sweep: raw jdbc statements 980,068 → 979,780 (−288, 0.03%) — the seed stream is
overwhelmingly CORPUS-AUTHORED executeInDb text, whose per-statement execution IS the fidelity
contract (task #14's finding again: the cost is the statement stream itself). The effectful-map
fold was left alone: its per-element `executeTyped` is the same corpus-text channel and its
"last result" is semantically discarded setup output. The real speed lever remains family
sharding (recorded, out of scope). Scoreboard byte-identical; full chain GREEN.

### F7.6 — `DynamicPivot`: add typed arms, make `default` throw

**Files:** `exec/DynamicPivot.java:86-93`

**Why:** `default -> new SqlExpr.StringLit(String.valueOf(v))` silently turns a `DATE`/`DECIMAL`/
`TIMESTAMP` pivot key into a **string literal** in the regenerated `IN` list. `SqlExpr` already
has `DateLit`/`DecimalLit`/`TimestampLit`. **Do not regress this class's placement** — its
two-phase run at the execution seam, rewriting IR via `SqlRewriter`, is the target design.

**LANDED ENDPOINT (2026-08-17):** every JDBC pivot-key kind has its TYPED literal arm
(BigDecimal→DecimalLit, Double/Float→FloatLit, Date/LocalDate→DateLit,
Timestamp/LocalDateTime→TimestampLit, String→StringLit); an unmapped kind THROWS
(`NotImplementedException` naming the JDBC class). Placement untouched. Scoreboard
byte-identical, full chain GREEN.

### F7.7 — `planWalk`: exact FQN dispatch, loud default

**Files:** `StatementExecutor.java:1293`, `:1430`, `:1520`, `:1753`; the null returns at
`:1416-1418`, `:1421`, `:1243`, `:1268`, `:1650`, `:1763`, `:1812`, `:1908`

**Why:** ~750 lines dispatching on `fn.substring(fn.lastIndexOf(':') + 1)` — in a file that
states the opposite rule at `:2057-2058` (*"EXACT FQN (never suffix matching)"*) and already has
`AT_FQN`/`FIRST_FQN`/`SIZE_FQNS` constants at `:2059-2065` as the model. Terminal defaults should
throw, not return null. Delete the `LL_TMP_DEBUG` breadcrumbs that exist only because the silence
was undiagnosable.

**LANDED ENDPOINT (2026-08-17):** the walk vocabulary now dispatches on EXACT FQN.
`MetamodelSteps.metamodelStep`/`mappingNav` take the full qualified name; every case label is
the real engine FQN — censused LIVE over the full sweep (`[F77-PROBE]`, 19 distinct FQNs
observed) and the unfired residue (`filter`/`cast`/`map`/`toOneMany`/`schema`/`table`/
`superMapping`) verified against `Pure.java` registered signatures and the real legend-pure
sources (`meta::pure::mapping::superMapping` at functions_PropertyMappingsImplementation.pure:19).
A user function sharing a simple name can no longer shadow the metamodel vocabulary. The three
planWalk `LL_TMP_DEBUG` breadcrumbs died. ADJUDICATED, not changed: planWalk's terminal `null`
is a ROUTING signal — the statement falls through to the execute-frame / SQL pipeline channels
(`:312`), so throwing there would break the architecture, and `WALK_UNRECOGNIZED` already
separates "not my vocabulary" from Pure-empty; the `constructOp`/`constructNode` simple-name
switches sit INSIDE the four-package `startsWith` namespace guard (`:1160-1168` — platform-owned
namespaces), so no user shape reaches them. Scoreboard byte-identical, full chain GREEN.

### F7.8 — The `normalizer/`'s 31 silent defaults

**Files:** 31 `orElse(null)` sites in `normalizer/`, 11 in `resolver/`. Start with
`MappingNormalizer.isBitemporalClass:1507-1510`.

**Why:** an unresolvable class silently answers "not bitemporal", **omitting milestoning
predicates from the plan** — wrong rows, no error. Contradicts AGENTS.md common-mistake #10.

**Probe:** make it throw and run the referee. **Green → it was never reachable and the
defensiveness should be deleted (all 31). Red → each red names a real gap.** Either outcome is
a result.

**LANDED ENDPOINT (2026-08-17):** probed ALL 33 `findClass(...).orElse(null)` sites in
`normalizer/` (the audit's 31 had grown) with a per-site funnel over the full sweep. Verdict
split: **23 sites never fired → `orElseThrow`** (the named `isBitemporalClass:1507` among them —
the feared silent milestoning omission is now loud); **10 sites fire legitimately** and funnel
through the documented `MissProbe.knownMiss`: UnionSynthesis's metamodel probe (20,808 misses,
every key an engine metamodel/protocol class FQN — the miss IS the answer) and nine sites whose
keys are BARE super-class simple names — a REAL name-resolution gap, filed in the §9 backlog
(no corpus case bites today). The `resolver/` 51 `orElse(null)` are post-audit code written
under the null-policy decision procedure and were not part of this census. Referees: DuckDB
scoreboard byte-identical, h2 sweep 2282/2575 identical.

---

## 9. Phase 8 — Long tail (after the referee is trustworthy)

`A5`/`A6` testdatagen expression channel (give `Fetched` per-column `SqlExpr` instead of `String`
— **one capability, and both the SHA-256 breach and the lossy CSV scrub dissolve without touching
the temp-table machinery, which must be preserved**); `A13` host-side UNNEST; `A17`–`A22`;
`server/LegendHttpServer`'s double-encoded `data` payload and its unreachable streaming path;
`nlq`'s syntax-only validation (`Compiler.compileQuery` exists and is never called) and its
tautological sort-coverage metric; the F1.9 orphan-test reds; `Executor`'s decode cluster
(`V1.7`–`V1.9`); the three zero-assertion "tests"
(`DuckDBVariantLoadTest`, `DuckDBUnnestSyntaxTest`, `ProbeWireShapes`) — convert to
`experiments/backend-probes/` shape or give them assertions.

**Phase-8 progress ledger (2026-08-17):** `A17` retired by F7.6, `A18` by F7.8. **`A19`
LANDED:** `ConnectionSpecification.EmbeddedH2(databaseName, directory, autoServerMode)` —
carried WHOLE (the old fold to `LocalH2(null,null,null)` discarded all three fields and every
embedded connection shared one fixed `jdbc:h2:mem:testdb`); the resolver gives each
`databaseName` a DISTINCT in-memory db (the engine's directory-backed isolation without disk
side effects — deliberate divergence, spelled at the resolver). Referee zero-delta, chain
GREEN.
**Zero-assertion trio LANDED:** `DuckDBUnnestSyntaxTest`/`DuckDBVariantLoadTest` moved to
`experiments/backend-probes/duckdb-syntax/` as named PROBES with a README (a test that cannot
fail is not a test; as recorded probes they keep their evidentiary value); `ProbeWireShapes`
was already gone — only comment references remain. Chain GREEN.
**Executor decode cluster (V1.7–V1.9) LANDED:** V1.9's prefix matching is dead — the
parameter suffix strips once (`DECIMAL(38,9)` → `DECIMAL`), then the table is EXACT-match with
the loud default (NUMERIC added). V1.7 ADJUDICATED resolved-by-design: the `endsWith("D")`
parse is the mixed-identity carrier's decode (the print form IS the wire contract for
NUMBER-rooted mixed collections — the burn-down chose it over the audit's typed-sibling
column); documented at the site. V1.8 ADJUDICATED: the string arm already delegates to the one
unescape table (F3.1d); the number arm must NOT delegate to `sql/Json.num` — an Any-rooted
decimal JSON number is a pure Float (`Double`), while the strict JSON bridge deliberately reads
`BigDecimal` (audit 18); same grammar, different target kinds by design, documented at the
site. Referee zero-delta, chain GREEN.
**A20 (double-encode half) LANDED:** `/engine/execute` and `/engine/sql` responses carry
`data` as a REAL JSON array node (`Json.parse` of the ResultJson text into the response tree) —
the old put of the `toJsonArray` STRING double-encoded the payload; the integration test now
PINS `"data":[` and refutes `"data":"[` (the substring check that passed identically for both
forms is superseded). The STREAMING half — `Executor.stream` is correct and no HTTP route uses
it — is product-surface wiring, filed in the §9 backlog. Chain GREEN.
**A22 LANDED:** nlq validation is COMPILE, not parse — `NlqService` now REQUIRES the model
source (4-arg constructor, all nine sites migrated) and the generation retry loop calls
`Compiler.compileQuery` (type check against the model); the generator prompt demands fully
qualified class names. On its FIRST run the compile validator caught a real fixture bug — a
mock query reading `VaRResult.var95`, a property that never existed (the audit's exact
scenario). `NlqEvalMetrics`: `scoreQueryAccuracy` compiles when a model source is supplied;
the sort-coverage tautology is dead (the key must appear IN the sort section — the old
`contains("sort")` clause scored 1.0 for any sorted query); judge ERRORS count as UNJUDGED and
the report prints `judged M of N; K judge error(s)` (the old `overall() > 0` filter silently
shrank the denominator). Also un-rotted: a dead `com.gs.legend.model.m3` import had kept the
whole nlq test module from COMPILING outside the gates — 223 module tests green again. Chain
GREEN (nlq is not yet a gate — its module tests are the referee here).
**A21 (collectResults half) LANDED:** the three copy-pasted `collectResults` helpers are ONE
`CheckerResults.collect` — a duplicate key THROWS (the old `HashMap.put` silently kept the last
row, so a query wrongly returning two rows for one group key passed), insertion order kept
(LinkedHashMap). 68 call sites migrated, 137 checker tests green — no query returns duplicate
keys today, and now none can silently. The broader `rows().stream()` sorted/filter idiom sweep
(48 of 80 sites) is test-idiom hygiene of the same class, left to the ongoing correctness lane.
**A13 ADJUDICATED merged into the §9 TDS-to-many leg:** the host-side row explosion at
`Executor.shapeRow` is the EGRESS half of the same design (`OutputCol`/slot reconciliation —
emit the engine's union-subselect/LEFT-join explosion in SQL); the host path is loud-guarded
meanwhile (a second many-valued column throws; empty streams keep the LEFT-join row). **A5/A6**
(testdatagen `Fetched` per-column `SqlExpr`) remains the one substantial open Phase-8 build.
**A5/A6 LANDED (2026-08-17):** `hashStrings` and the CSV scrub are SQL. The dump's SELECT
spells the engine's hashString per text column (`repeat(substr(sha256(c),1,5), len//5) ||
right(…, len%5)` — first-5-hex tiled, engine testDataGeneration.pure:656) and the
quote/comma/newline scrub as nested `replace`; text columns learn from the union's SCHEMA (a
LIMIT-0 metadata read). The Java `MessageDigest` hashString and the display-string replace
chain are DELETED — the tenet's oldest open breach (survived 691 commits) is closed. Row
values cross into Java as display strings only. The full `Fetched` per-column-SqlExpr
re-plumbing proved unnecessary: the projection layer was the one seam that needed typing.
Referee zero-delta, chain GREEN. **Phase 8's build items are done**; residue = §9 backlog
(F4.4 Lowerer root mode, TDS-to-many + A13, HostEval re-platforming, SourceUrl frame,
streaming route, CorpusDifferentialTest gate wiring, reflection retirements, bare-super-name
resolution, DuckDB-upgrade txn revisit).

**Backlog (append new findings here, do not act on them mid-plan):**
- **HTTP streaming route (A20 residue):** `Executor.stream` (JSON straight
  to the wire, no materialization) is correct and unreachable — no HTTP
  endpoint uses it. Wire `/engine/execute` (or a streaming variant) through
  it when the server surface gets its product pass.
- **Normalizer bare super-name resolution (F7.8 census find):** nine
  `findClass` sites receive BARE super-class simple names ("Person",
  "Firm" — `TypeExpression.NameRef` never import-resolved), so
  superclass traversal silently contributes nothing at those sites (a
  temporal superclass referenced by bare name would drop its
  milestoning). No corpus case bites today; the sites are enumerable
  via `normalizer/MissProbe.knownMiss`. Fix = resolve super names
  through the import scope before the normalizer walks them.
- **TDS project to-many column contract (F4.3 probe find):** a to-many
  project lambda types a TO-ONE TDS column but LOWERS to a list-valued
  slot (OutputCol VARCHAR, wire array) that the Executor unwraps at
  egress — the one CSV-render residue (`testConcatenateWithFilter`,
  docs/CSV_DIFFERENTIAL.md mechanism 3). Reconcile OutputCol with the
  emitted slot (or emit the to-one coercion in SQL).
- `scripts/outstanding.py:14` / `walldepth.py:7` hardcode `/Users/neema/...` — another account's
  checkout. `docs/RUNNABILITY_PLAN.md:129-133,184` derives its re-forecast from their artifacts,
  so **those numbers cannot be reproduced from this checkout.**
- `tools/scoreboard.py` points at the deleted `engine/` module; with `--record` it appends **a
  row of all zeros** to `docs/SCOREBOARD.md` as a legitimate run.
- We wire **5 of 7** upstream PCT scopes — "1109/1109" is 1,109 of **1,203** (Variant: 88 tests,
  Quant: 6).
- `EngineSectionRosterTest:74` / `EngineElementRosterTest:58` use `>=`, so a roster that **grows**
  passes in exactly the silence the header says it should not.
- **F1.3b's real fix:** split `com.legend` root into orchestration (no `java.sql`) and a named
  exec seam (licensed), so `StatementExecutor`'s dispatcher role is structurally constrained
  rather than class-list-pinned. Deferred — not pause work.
- **The 56 counted sql-text-side gaps need an OWNER (frozen leniency,
  found by the user's deferral review):** F2.3's registry ceiling counts
  56 real renderer/recognizer gaps in the golden-SQL side channel
  (EngineStyleH2 list/array encodings: LIST_GET/LIST_CONCAT/STRING_AGG/
  array literals; toSQLString argument shapes; banker's ROUND;
  object-space TypedFilter substitution). Ceiling'd but assigned to no
  phase. Owner: the renderer-encoding buckets ride with Phase 4's
  render-lowering work (same dialect surface); the toSQLString/
  recognizer buckets are burn-resume fuel. Burn the ceiling down with
  dated justifications as each bucket closes.
- **Phase-1/2 deep-audit residue (2026-08-16):** (a) F1.7's ratchet misses
  `default -> CONSTANT` and old-style `default: return "x"` spellings — a
  tripwire, not a wall; (b) the float-ROUNDING leniency count (norm's
  10-significant-digit fold) needs a norm-free recheck; (c) ord-leniency
  should graduate from LL_ORD_COUNT-gated stderr lines to ALWAYS-ON
  census counters beside M1_RESCUED; (d) the `sql-text: ` message-prefix
  protocol should become a typed outcome channel; (e) the per-family
  regression gate compares pass COUNTS — a PASS→FAIL/FAIL→PASS swap is
  invisible to the GATE (mitigated: the non-passing list is committed,
  so swaps surface in the doc diff and §0.4 demands row-for-row
  justification); (f) TreeMap/TreeSet implicit sorting is censused clean
  in the harness but not pattern-guarded; (g) F2.3's decline total varied
  51→56 between an exclusive sweep and the chain's G4 — ceiling held
  since; if it flaps, diagnose the nondeterminism, never raise blind.
- **Reflection removal (F1.11 residue):** the bytecode ban found three
  pre-existing production reflection sites the source census missed —
  DbMetaData's java.sql.Types field iteration (replace with a literal
  type-name map), ScanColumns:311's reflective record-tree walker
  (replace with the TypedSpec children() spine or per-record accessors),
  server/Json's generic Array.get serialization (replace with typed
  array arms). Frozen shrink-only in ArchitectureTest; no new
  reflection compiles.
- **CorpusDifferentialTest gate wiring (F1.9 residue): DONE 2026-10-05 (Bazel workplan P3-18).** Was: run
  `scripts/corpus/differential.py` as a gate step so the Assumptions-skip
  stops firing — a python + oracle-checkout moving part deliberately not
  added mid-pause; its javadoc now states the truth (dark in every build).
  Also: the plan's "11 orphaned classes with real assertions" did not
  reproduce at HEAD — the measured set was TWO (GrammarCoverageCensusTest,
  GenerativeDualParseTest), both now gated; the other orphans are
  zero-assertion report/census printers (ProbeWireShapes et al. are the
  Phase-8 zero-assert item).
- **HostEval RE-PLATFORMING (F0.3 census result; reframed 2026-08-16, user directive):** the
  channel's demand is 100% harness (all four admission gates serve corpus vocabulary; no
  production entry point routes host today). The goal is NOT to relocate the interpreter — it
  is to make its vocabularies USE THE PLATFORM instead of reimplementing evaluation, the same
  move F3.2 (substitution → SourceSubst), F3.3 (multiplicity walk → Typer), and F3.4 (ASOR →
  AsorRef) made for their harness twins. Run it like a Phase-3 leg:
  1. **Instrument first:** one full referee cycle of per-arm demand data
     (`LL_HOST_ARM_COUNT` pattern) — the gates admit whole chains, so per-arm deadness is
     not statically decidable.
  2. **Convert arm-FAMILIES onto real platform capabilities**, deleting arms as they empty,
     corpus as referee per step:
     - `executeInDb` grid chains → compile the Pure chain, lower over the grid as a
       relation (grid → VALUES source); exec already owns the SQL round trip;
     - `schema()`/`table()` store navigation → compile-time metamodel constants answered
       by `ModelContext` (the F3.3 move, applied to reflection);
     - the curated typeInference construction set (`^DynaFunction`/`Literal`/`Alias`/…
       + property reads) → the landed STRUCT-values design (typed construction,
       DB-lowered values);
     - `containsFetchDb` JDBC-metadata grids → `information_schema` queries lowered like
       any relation.
  3. **Endgame, not goal:** whatever residue remains moves behind a harness-installed seam
     so production `StatementExecutor` carries no interpreter — the charter-3 invariant
     becomes vacuously true in production. Comparison POLICY stays harness-owned
     (orchestration, not evaluation — it must not migrate into the platform).
  Rides only after F1.5's pin exists (it does) and with the counter data; the admission
  predicate has collapsed the sweep twice, so every family conversion is its own gated
  task. Each conversion doubles as a product capability (relation-from-grid, compile-time
  reflection, struct values). Census: `FOUNDATIONS_BASELINE.md` §7.

---

## 10. Dependency order

```
F0.1  baseline  →  F0.2 CHARTER ★ (F1.3/F1.5/F1.10 and Phase 4 cite it)  →  F0.3 HostEval census
  │
  ├─ Phase 1 GUARDS ────────────────────────────────────  do first, in order
  │    F1.1 → F1.2 → F1.3 ★     (extract → move → funnel; exemptions from F0.2)
  │    F1.4, F1.5, F1.6, F1.7, F1.8, F1.9, F1.10   (parallel)
  │
  ├─ Phase 2 MEASUREMENT ───────────────────────────────  needs nothing; do early
  │    F2.1 ★, F2.2, F2.3, F2.4, F2.5, F2.6   (parallel)
  │
  ├─ Phase 3 DE-DUPLICATION ────────────────────────────  needs F1.2 for F3.2–F3.4
  │    F3.1 ★ (a→b→c→d), F3.2, F3.3, F3.4, F3.5, F3.6, F3.7
  │
  ├─ Phase 5 BRIDGE ────────────────────────────────────  F5.1, F5.6 need nothing
  │    F5.6 (free) ─ F5.1 ─ F5.2 ─ F5.3 Stage A ★ ─ Stage B
  │    F5.4, F5.5
  │
  ├─ Phase 4 RENDER ★ ──────────────────────────────────  needs F3.1c, F5.2
  │    F4.1 → F4.2 → F4.2b PROBE (read-only) → F4.3 (largest red) → F4.4
  │                       └─ F4.3 MUST NOT start until this work list exists
  │
  ├─ Phase 6 COMPENSATION ──────────────────────────────  F6.3 after F5.4
  │    F6.4 (1 line) ─ F6.8, F6.9 ─ F6.2 ─ F6.1 ★ ─ F6.3 ─ F6.5 ─ F6.6, F6.7
  │
  ├─ Phase 7 INGRESS ───────────────────────────────────  F7.4 needs F1.6, F3.5
  │    F7.1 ★ (likely GREEN) ─ F7.2 ─ F7.3 ─ F7.4 ─ F7.5 ─ F7.6, F7.7, F7.8
  │
  └─ Phase 8 tail
```

**If you can only do three things:** `F1.3` (the funnel — makes the tenet mechanical),
`F2.1` (soft-pass columns — makes the burn-down's left term visible), `F7.1` (transactions —
deletes the downgrade apparatus and probably raises the honest pass count). If you can do
four: `F0.2` first — the other three each cite it.

**The two read-only probes — `F4.2b` and `F5.3 Stage A` — are the highest value-per-risk tasks
in the plan.** Both are compare-and-*log* rather than compare-and-fail; neither can redden a row;
and each converts the largest unknown in its phase into a committed work list before any
destructive step begins. Run `F5.3 Stage A` early regardless of phase order — discovering a
type-propagation defect *after* re-freezing the burn-down baseline is the bad case. Run `F4.2b`
as soon as `F4.2` lands.

**Generalise the pattern:** any task in this plan whose expected-red is stated as a range rather
than a number deserves a probe first. That list is currently `F4.3`, `F5.3`, `F6.1`, `F6.2`,
`F7.2`, `F7.8`.

---

## 11. Definition of done for the pause

1. All five §0.2 metrics at target.
2. Every falsified self-claim corrected; no header asserts a discipline its file does not implement.
3. One owner for: JSON reading, JSON writing, substitution, multiplicity, ASOR decode, identifier
   quoting, result rendering.
4. `java.sql` funnelled and the funnel **proven** by a deliberate violation that fails the build.
5. `RawSqlBoundary`'s contract true; `sql/Json.java:11`'s claim true or deleted;
   `Column.java:5-9`'s claim true.
6. The referee (full corpus sweep) green in the sense that **every non-passing row has a verdict
   and every passing row is verified** — the scoreboard can now express both.
7. `docs/FOUNDATIONS_BASELINE.md` updated with the closing numbers and a written explanation of
   every family that moved.

**Then resume burn-down**, against a scoreboard where `passed` means passed.
