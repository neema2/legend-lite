# End-to-end plan: finish the harness, burn the roster to its honest floor (2026-09-08, v2 after the harness audit)

> **v2 (2026-09-08).** The adversarial harness audit — docs/HARNESS_AUDIT_2026_09_07.md and its
> evidence base docs/harness-audit-2026-09-07/ (referee.md, ambient-state.md, guards.md,
> roster-and-floor.md, main-residue.md, strength.md, driver.md) — was verified against source
> and is MERGED into this plan. Where the audit and v1 disagree, the audit wins unless a
> correction is stated in §V2 below. Read the audit's §1–§4 and §10 before anything else.

## V2. What the audit changed (verified 2026-09-08 against source; corrections to v1)

Verified TRUE, v1 corrected:
- **Row order stopped being a contract in batch 115** (audit §2): the old runner set
  `H2Verify.ORDERED_QUERY`/`SORT_KEYS` per test (`EngineTestExecutor:1656`); batch 115 deleted the
  writer and left the readers. v1 called the positional compare "already dead" — it was KILLED,
  silently, in the pass-manufacturing direction (95 passes rest on order leniency, 11 with the
  referee as the only judge). Decision: RESTORE the ordered compare (wire it from the verdict arm's
  own order derivation, `AssertVerdicts.orderView`), never "declare unordered".
- `NullSemantics.FILTER_POS` is WRITE-ONLY (declaration, save, set, restore, a comment). v1's
  Appendix A listed a reader that does not exist. It is deleted, not relocated.
- The 128 H2 `Column "tN.X" not found` failures are ONE core renderer bug: derived-column aliases
  emitted quoted, references to them unquoted; DuckDB's case-insensitive resolution hides it, every
  other target does not. Plus 11 `union needs at least two branches`. Both are core portability
  defects, fixed BEFORE the compiler legs.
- The paginate case is not "counted nondeterminism": `H2Verify:419-429` converts a COMPUTED
  divergence into a decline when the golden is paginated, and `SqlTextVerdicts` turns a decline
  into a pass on byte-equal text — a rescue path. Fixed in Phase 0 (referee faults/rescues).
- `H2Verify.norm` timestamp regex `\.?0+$` splits `00:00:10` from `00:00:10.0` (false divergences).
- 21 of 121 roster entries carry no diagnostic (`MinimalCorpusTest` prints the first message
  line only). The order-dependence example v1 named (`relationalResultSourcingOfListExecutionPlan`)
  fails in BOTH modes — not a leak; §6c's census stays, the example is withdrawn.
- v1's Appendix B set-difference compared name+message (a drifted message reads as a
  regression): compare NAMES.
- The 121 re-categorized by the audit (roster-and-floor.md §4) supersede v1's bucket counts:
  **REAL-DEFECT 37** (6 referee-confirmed divergences, 8 more dug from blank messages, 5
  simple-name resolution, 12 compiler/resolver walls, 5 typer gaps, 1 STRING_AGG dialect gap, the
  md5 digest encoding ×2, a Date-vs-DateTime rendering) — not v1's 7; ENGINE-MACHINERY 36;
  TEXT-ONLY 29 (9 of them unadjudicated, NOT proven equivalent); OTHER-STORE 10; CODE-AS-DATA 7;
  UNKNOWN 2. v1's "step 5 ≈ 45" exceeded the measured pool (43); §7 is restated below.
- PCT reads the two censuses as COUNTERS in the same JVM through 11 print-or-assert members: the
  refactor is two observer interfaces (~25 LOC) on the existing `AssertListener`/`ExecEnv`
  injection seam (ambient-state.md's `FactLedger`) — cheaper than v1's option C; ~500 lines of
  the V7 block are provably dead.
- The gate has ONE assertion (`pass.size() >= floor`): a count, monotone upward, skipped under
  `-Drcorpus.test`. Everything else prints. Phase 0 replaces it with a SET pin per lane.

USER DECISION 2026-09-08 (closes v1 §6d): **the H2 lane is KEPT, no matter what.** It is a
second oracle for everything we do; H2's missing functions are added as JAVA FUNCTIONS WRAPPED IN
H2 SQL (`H2ExtensionFunctions`, the engine's own H2 extension pattern), and 100% parity with DuckDB
is not required. Consequences: the H2 lane gets its own committed roster pin (a SET, like DuckDB);
the two renderer bugs (139 tests) are fixed first; the 283 declared `DialectCapability` gaps are
worked as legs where a Java-in-H2 function closes them, and named where they are genuine engine
limits; the lane prints `oracle=same-session` (its golden runs on the same connection — it is a
portability check, not an independent oracle; the DuckDB lane with the H2 mirror is the
independent one).

Not adopted as-is: the strength-budget NUMBERS become gates only after OUR harness prints the
census and reproduces them (Phase 0.7); the semantic-parity suite (`equal(1,1.0)` decided once)
is its own small leg, not a prerequisite.

## THE ORDER (v2)

**Phase 0 — measurement integrity (first; everything after is judged by it). COMPLETE 2026-09-08, batches 126–134 (docs/GATES.md): 0.1 = 126, 0.2 = 127, 0.3 = 128, 0.4 = 129, 0.5 = 130 (+ 0.5b page-membership verdict = 131), 0.6 = 132, 0.7 = 133, 0.8 = 134. Every criterion is a passing test in `MinimalCorpusTest` / `DanglingStateGuardTest` / `JavaEvalLedgerTest`.**
0.1 Roster pinned as a SET per lane against committed roster files (docs/parked), with a
    CEILING (a pass-count jump must be explained); the pin runs under `-Drcorpus.test` too
    (scoped subset ⊆ roster). Set-difference by NAME.
0.2 The failure message printed whole (≥3 lines); setup failure fatal; `INERT_SETUP` counted.
0.3 Zero-assertion passes EXCLUDED from the count (`Compiler.callsVerdict` descends into user
    function bodies; a test adjudicating zero verdicts is `SKIPPED (no assertion reachable)`) —
    the floor drops by ~32 (27 `mayExecuteAlloyTest` shells whose lambda never runs, 5 with
    asserts commented out, 2 vacuous placeholders) and becomes honest.
0.4 The dangling-state guard: for every static `ThreadLocal`/`Atomic*`/`LongAdder`/`volatile`
    across both roots, reads > 0 ⟺ writes > 0 (would have caught batch 115); no guard comment
    names a symbol absent from the tree (`HarnessDisciplineTest:96`).
0.5 The ordered compare RESTORED (from `AssertVerdicts.orderView`); `[ord]` firings 0 or in a
    committed register; the timestamp normalizer fixed; the paginate rescue removed (a
    computed divergence is a divergence).
0.6 No uncounted declines (`SqlTextVerdicts:1258-1273`, `:151-160` record a named decline
    before `textEqual ? ok() : fail`); DECLINED split into modeled GAP vs FAULT (an
    SQLException on our own seeding, a RuntimeException in the compare, a missing extension
    function) — FAULT never falls back to text; zero `catch (RuntimeException)` in the referee;
    the mirror's seed cursor keyed to the ledger entry, not a bare index; every leniency (2-ULP,
    CSV cell tolerance, `MathContext(10)`, µs flooring, fanout collapse, stitch-key drop) has a
    counter and a committed ceiling.
0.7 The strength census printed per lane per test (audit §3's ladder: differential / literal /
    cardinality / spelling / none) and, once reproduced, pinned monotone: `differential >= 1511`,
    `textOnly <= 39`, `zeroAssert → 0`, `weakOnly <= 25`.
0.8 The denominator re-derived (`discovered/excluded/declared` asserted against a corpus scan);
    `EVICT_SIZE` rows tightened to measured and failing on shrink (751 lines of slack today).

**Phase 1 — the two H2 renderer bugs. DONE in batch 135 (2026-09-08): the `SourceSpelling` pass (a reference spells as its source declared the name) and the one-branch unions — H2 1847 → 1961 pass, 128 `Column not found` → 1, 11 `union` → 0. USER RULING 2026-09-08: no more time on H2 beyond this quick win — the missing functions (STRING_SPLIT / REGEXP_EXTRACT as Java-in-H2), the tree-print verdict SQL per dialect and the declared list/variant gaps move to the OPTIONAL PHASE 7 below.**

**Phase 2 — the thread-local sweep** (v1 §1, corrected): 1a modes (NullSemantics.VERBATIM_EQ →
an argument of the equality lowering; FILTER_POS deleted; EngineTextBoundary/TextGoldens → the
renderer/lowering instance — note `CastPolicy:50` deletes a cast from the MIR under that flag, so
it is a LOWERING option, not a render option); 1b ledgers (RawSqlBoundary recorder object,
ExecutionTrace comment returned with the result); 1c the FactLedger observer interfaces replacing
`SqlTypeCensus`/`CanonicalDivergence` statics (PCT gate and Channel-B read the ledger; the dead V7
block deleted); 1d guards.

**Phase 3 — the 37 real defects** (roster-and-floor.md §4 is the list of record; v1 §2's homework
for exists-with-subtype and the inheritance plan still applies), three fix cycles per leg then a
named wall. Includes the md5 digest encoding (2), Date-vs-DateTime rendering, STRING_AGG, the 5
simple-name resolutions, the 5 typer gaps.
STATUS: leg 1 (splitPart → semantic `SqlFn.PURE_SPLIT_PART`, three dialect spellings) LANDED batch 139 —
DuckDB 140 → 139 fail; the md5 digests ×2 and the Date-vs-DateTime render were TRACED to golden
disagreements with the engine's own Pure spec → REVISIT rows (ledger §11), i.e. Phase 4 decisions, not
code. Leg 2 (exists-with-subtype) LANDED batch 140 — not a design leg: the implicit `Inheritance` op for an
unmapped routed target now covers routed class-typed properties (`ImplicitInheritance.implicitOpsForRoutedTargets`);
DuckDB 139 → 138, H2 600 → 599. Homework (ledger §12): joinStrings ×2 + firstDayOfWeek → REVISIT (engine golden
defects, USER decisions); 3 of the 5 "simple-name" failures are engine-core test-model files the corpus does not
load (USER scope decision), 1 is a bare enum reference in value position (real, one test). NEXT leg: the 18
guard-assert-gap shells (value-position helper asserts hoisted into the verdict channel — one design batch).
Leg 3 LANDED batch 141 — NOT the hoisting leg: the shells' helper (`createTableRowIdentifiers`) is a platform NATIVE
(the TDG carrier reads the call as syntax); two passes had two owners (the typer opened the Pure body, the statement
inliner did not), so the runner failed 18 tests for an assert nobody ran. `isPlatformOwnedFunction` names the three TDG
spellings; the 18 are SKIPPED `no assertion reachable` on both lanes (DuckDB 120 fail / 32 SKIPPED; H2 581 / 32).
OWED (Phase 5): the TDG carrier consumes VALUES, the helpers become programs, their guards become verdicts.
REASSESSMENT after batch 141 (ledger §13): of the DuckDB fail roster's 120, the referee's own classes are
ENGINE-MACHINERY ~38, TEXT-ONLY ~42, CODE-AS-DATA/OTHER-STORE ~19, REVISIT 6, SCOPE 5 (engine-core test-model files),
REAL/OPEN ~8 (single-test legs: testSortQuotes enumValues-filter fold, testPksWithImportDataFlow, testMixedMappingWithFilterInProject,
testDateTimeInclusiveRangeQuery (likely REVISIT: H2 literal precision), testRelationStoreAccessorOnView (view accessor + text
assert), testExecutionPlanGeneration (over typing), nested ModelJoin (parked), TDG view slice). The under-100 path is now
DECISIONS (REVISIT 6 + SCOPE 5) and Phase 4's TEXT leg (42), not Phase 3 code.
Batch 142 (USER: "fix the tests!") SUPERSEDES 141's classification: the TDG natives are deleted, the helpers are
programs, the carrier reads VALUES, the guard is a verdict — 18 shells PASS on both lanes (DuckDB 2441 pass);
schema()/table() are system-metamodel Pure accessors and `db->schema('S')->table('T')` is a store-element IDENTITY
(`StoreElementIdentity`, one owner); the H2 membership rewrite sees through the list carriers → +9 exists tests on H2
(1989 pass). Rule (memory `phase3-fix-not-reclassify`): never reclassify when a 1–2 batch code leg exists.
PHASE 3 CLOSE-OUT (batch 143, ledger §15): 21 DuckDB passes landed (+9 H2); the code pool left is THREE single
tests (importDataFlow option, mixed-mapping union key demand, TDG view slice — each a batch); the rest of the 120 is
REVISIT 10 + data nondeterminism 1 (USER), SCOPE 5 (USER), TEXT contracts ~27, ENGINE-MACHINERY ~38, CODE-AS-DATA/
OTHER-STORE ~19. The 'under 100' path is the two USER decisions. Mechanism landed: the quantified verdict's source
reduces with the literal arms on (`UserCallInliner.forVerdictSource`).
REVISIT DECIDED (batch 144, USER agreed to all 11 after a one-by-one review): ACCEPTED is a fourth status — the
test runs, its failure must carry the register's witness (the golden's own wrong value), the census counts it by
bucket; a changed divergence FAILs, a vanished one is GAINED. Correction: the filter-in-project row is an ENGINE BUG
(the filter lambda bound to the outer row), not nondeterminism. DuckDB 2441 / 109 / 14 / 11; H2 1989 / 566 / 14 / 6.
NEXT: SCOPE (batch 145) = the four engine-core fixture files as NAMED library sources (declarations in, tests not
discovered; the stdlib-namespace guard refuses functions only), then the three owed code legs.
SCOPE DECIDED (batch 145): the four engine-core fixture files are NAMED library sources (`Corpus.LIBRARY_FILES` +
`Corpus.CORE_PURE`; the guard refuses stdlib FUNCTIONS only; Prelude regenerated +5 spec classes). The five tests
resolve their names and reach their true walls — CODE-AS-DATA (2), ENGINE-MACHINERY (3); no pass change. USER lesson:
a Pure import is a name shorthand, not a dependency. NEXT: importDataFlow (146), mixed-mapping union key, TDG view slice.
importDataFlow ATTEMPT REVERTED (2026-09-08, six cycles > the three-cycle rule; USER: clean sheet): the design is
`docs/IMPORT_DATA_FLOW_DESIGN_2026_09_08.md` — implement §3 in order as batch 146.
importDataFlow LANDED (batch 146, 2026-09-08): implemented from the design §3 in order, one fix cycle (the typer arm the
design named). `testPksWithImportDataFlow` PASSES on both lanes: DuckDB 2442 / 108 / 14 / 11, H2 1990 / 565 / 14 / 6.
Four facts, four owners (ledger §19): union key threads recorded by the synthesis (`ModelContext.unionKeyThreads`), the
option read literal-only, the columns derived once (bound context + the call's output type), the append beside
DriverPkAppend. Deviation: the union binding's `primaryKeyColumns` untouched (eleven physical-key consumers). NEXT (USER 2026-09-08, after batch 146): straight to Phase 5 — code and metamodel as data (docs/CODE_AS_DATA_HOMEWORK_2026_09_05.md, sized first, smallest witnessed slice); the two remaining real code legs (the mixed-mapping union key demand `testMixedMappingWithFilterInProject`, the TDG view slice `testAlloyTestDatGenWithQuotedColumnsForViews`) are DEFERRED, not dropped.

PHASE 5–6 HOMEWORK (USER 2026-09-08, after batch 146): READ `docs/TWO_DESIGN_LEGS_2026_09_07.md` FIRST (from the
harness-audit branch, now on main): leg 2 code+metamodel as data is FIVE demands (D1 metamodel relations + D2 closures
LANDED; D3 plan rows partly; D4 the user program as relations NOT STARTED; D5 = re-hosting the engine's own compiler —
24 of the 45 candidate tests, named OUT of scope); realistic phase 1 = 8 tests; NO recursive CTEs (closure tables are
the policy; H2 has no cycle protection, DuckDB #13974 under-returns silently) — reconcile with ledger §15's
CODE-AS-DATA 19 / ENGINE-MACHINERY 38 as the first sizing step. Leg 1 single-shot: 1,998/2,575 already one statement;
sequence P-25 (resolvedDefinition census) and P-19 (NULL verdict) first, then the SqlUnion set-operator enum, then the
~700-test mapping/projection pilot. USER ORDER: Phase 5 next (the two remaining code legs deferred).

PHASE 5 SIZED (2026-09-08, after 146): `docs/PHASE5_SIZING_2026_09_08.md` — the 108 by owner NAMED (TEXT 33 /
ENGINE-MACHINERY 44 / CODE-AS-DATA 16 / other 15); D1+D2 landed, D4 STARTED (FunctionBodyRows + VS_KINDS rows), D5 out of
scope; slices: 1 extension registry by need (5, designed) → 2 program as m3 data both directions (2 + the func column) →
3 protocol types (2) → 4 class instances in value position (5, H4). Two user decisions owed before batch 147.

BATCH 150 LANDED (2026-09-08, burn to zero): 36 → 3 typing failures (docs/SPEC_BODY_CENSUS_2026_09_08.md §9) — units whole (Measure/Unit shapes, unit literal, `M~u`), packages as values, the PCT harness natives + generated shapes, the receiver-owned `_this` routing past bare special forms, eval run-time multiplicity, no-branch match as the raise. No pass change. NEXT: the generator emits DERIVED bodies via a protocol-to-Pure printer (the last 3 rows; Pair/List toString ride it), then §9.4 / §9.5.

BATCH 149 LANDED (2026-09-08, the census work list): 174 → 36 typing failures (docs/SPEC_BODY_CENSUS_2026_09_08.md §8) — fifteen hand shapes and 36 natives spec-exact, supertype instantiation in the kernel (`asSuper`), the enclosing function's type parameters as a rigid frame; the engine's SQL post-processing machinery WALLED by name (USER: design session separate) with an unroll budget in the inliner. No pass change. NEXT: burn the 36 to zero (units and PCT harness included), then the generator's derived bodies (§9.3), the lowering-list pin (§9.4), diagnostics (§9.5).

BATCH 148 LANDED (2026-09-08, the system prelude): design `docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md` (WORLD_MAP §8) — the prelude is system Pure GENERATED from the spec (shapes, derived bodies, spec function bodies); the five connection tests are ENGINE-MACHINERY; function-references/per-property/hand-off WITHDRAWN. `SpecBodyCensusTest` types every platform body: 481/643 → 950/174 once the kernel binds the enclosing function's type parameters per expression (kernel-class failures 470 → 1). No pass change; LEDGER §21. NEXT: the census work list in order (17 metamodel properties, 5 overload spellings, special-form routing, parser items, the IndexOutOfBounds typer bug), then generated derived bodies (Pair/List toString first).

BATCH 147 LANDED (2026-09-08, strict first): the extension-registry chain walked program by program — 19 ledger rows
(LEDGER_GRANULAR §20), the mechanism and vocabulary landed, the typing-surface-native → engine-program hand-off BUILT
and SWITCHED OFF (`UserCallInliner.HAND_OFF_ON`) until the chain compiles end to end; no pass-count change, rosters
byte-identical. USER: no regex band-aids (the engine function id is now GENERATED, FunctionDescriptor verbatim); the
row-18 chase reverted ("are we starting to hack?"). NEXT (batch 148): DESIGN function references as VALUES (row 18), then
the switch, then rows 19–23 of the chain (CODE_AS_DATA_HOMEWORK §2).

**Phase 4 — the TEXT referee leg** (v1 §3; the 9 unadjudicated TEXT rows are adjudicated, not
assumed) and REVISIT decisions (v1 §4). **Phase 5 — code and metamodel as data** over the
measured pool (ENGINE-MACHINERY 36 + CODE-AS-DATA 7; the engine's OWN compiler/router under test
never passes and is named as such — size first, smallest witnessed slice). **Phase 6 — item 4 with
referee.md's 30-site table as the spec** (classes (a) decline rules stay, (b) become SQL,
(c) become counted leniencies or die), then the graph leg, the residue census (v1 §6b),
scoped-equals-full (v1 §6c), then single-shot. The semantic-parity suite is a small leg at any
point after Phase 0.

**Phase 7 (OPTIONAL, user 2026-09-08) — H2 as compatible as DuckDB:** the H2 fail roster at that time is the input. **CENSUSED 2026-09-16 — [H2_PARITY_CENSUS_2026_09_16.md](H2_PARITY_CENSUS_2026_09_16.md)**: the roster this phase asked for, measured at HEAD and attributed row by row (PCT gap 246 of 1249 — all five suites, a number gate 7 never measured; corpus gap 332, failure sets nest exactly). Three findings change this phase's shape: H2 2.4.240 **has a full native ARRAY type** (jagged nesting and `CARDINALITY` both correct — `BACKEND_PORTABILITY` §3's contrary measurements are Postgres's), so `CarrierStrategies.Caps.H2.nativeLists=false` is simply wrong and is what raises most `LIST_*`/`UNNEST` walls; correlated explosion **is** reachable without `LATERAL` via a cardinality-bounded `SYSTEM_RANGE` ordinal join, empty collections preserved (probed); and no Java UDF buys correlation — a `ResultSet` alias is as un-correlatable as `UNNEST`. The `sessionSetup` seam is also a **live production defect**, not just an enabler: `H2.splitPartCall` already emits a UDF only the test harness registers. Its input list below is superseded by the census's tiering: Java-in-H2 functions for the H2-missing spellings (STRING_SPLIT 49, REGEXP_EXTRACT 26 today; the dialect's `sessionSetup` hook is the product-side seam — supersedes the July "never the CREATE ALIAS route" note in `Spellings`), the tree-print verdict SQL (`LineageTreeVerdicts.TREE_ROWS`, hand-written DuckDB text) built through the IR so every dialect renders it, the list/variant carrier `DialectCapability` gaps (variant 145, LIST_MIN 46, UNNEST 32, LIST_GET 29) as emulation legs or named limits. Not in the floor arithmetic.

**DONE (v2) = the audit's fifteen criteria (§10) + v1's §6f seven, merged**; every criterion is
decided by a test, not a reading. §7's floor is restated from roster-and-floor.md: 121 − 37 real
− (Phase 4 adjudications) → what remains is ENGINE-MACHINERY (36, named), TEXT decisions (≤29),
OTHER-STORE (10), CODE-AS-DATA (7 → Phase 5), UNKNOWN (2 → named) ≈ 60–80 named before Phase 5
and ≈ 35–45 after — restated from the ledger at each phase, never from memory.

---

# v1 (2026-09-08, kept as written; superseded where §V2 says so)


This is THE plan. A new session reads this first, then the documents it points to. Nothing
here is to be re-derived: every number below was measured, every wall was probed, every
design was read against the engine's own Pure. Where a decision is the user's, it says so.

Standing rules that bind every step (user rulings, in force):
- Always `-Dlegend.engine.root=/Users/neemsandv/legend/legend-engine -Dlegend.pure.root=/Users/neemsandv/legend/legend-pure`.
  Corpus roots are `-D` properties; `$HOME/legend/legend-engine` is a stale tag.
- Main session only, no subagents. One batch per gate cycle: probe → engine spec as homework →
  implement → both corpus lanes with a set-difference against the last roster → guardrails →
  full chain (`tools/allgates.sh`, tree FROZEN while it runs, never two heavy runs at once) →
  records (docs/GATES.md entry, docs/BURN_BREAKDOWN_2026_09_05.md status line, docs/SESSION_HANDOFF_2026_09_02.md
  §0 paragraph, memory) → commit NAMED files (never `-A`) → push.
- No hacks: no Java naming an engine program or test, no text surgery, no static sinks, no
  test-shaped arms, no shape sniffing, no string matching on identities, no caching without a
  root cause, no pin moved without a written reason. Fix the mechanism, never reshape a body.
- Rows are the verdict, always. A platform verdict brings the golden to ROWS by a referee and
  compares rows; never normalize-then-compare text.
- State the rule before the roster: after two or three roster-diff fixes with no stated model,
  STOP, write the finding, restate the rule. "While hot" only with a hard stop: three fix
  cycles per leg, then a named wall in the ledger and move on.
- Sequence by verdict impact, never by size. Every batch moves the ratchet or closes a named
  risk; mechanism-only legs are not batches.
- Size a deletion or a change across ALL modules (core, pct, server) — the pct module reads
  core statics (batch 123's lesson).
- Scripts that patch many files: write them to a file, make them idempotent (`git checkout`
  the touched files first), and never write any file before every assertion has passed.
- zsh: an unquoted `$R` holding two `-D` flags is ONE argument; the corpus test then assumes
  no checkout and SKIPS (Tests run 1, Skipped 1, 0.5s). Pass the flags literally.
- The chain keeps gate 4/5 outputs after GREEN runs now (`$TMPDIR/gates-neema.g4.out`,
  `.g5.out`); the exact roster of record is read there.

## 0. Where we are (main = facab6426, 2026-09-08)

| Measure | Value |
|---|---|
| DuckDB lane (gate 4) | batch 144 (REVISIT decided): **2441 pass / 109 fail / 14 SKIPPED / 11 ACCEPTED** (a fourth status: decided divergences with a witness, `rcorpus/duckdb-accepted-roster.txt`); batch 142 (Phase 3 leg 3, the real fix — the 18 alloy shells PASS with the guard as a verdict): **2441 pass / 120 fail / 14 SKIPPED**; batch 141 (the superseded classification): **2423 pass / 120 fail / 32 SKIPPED**; batch 140 (Phase 3 leg 2, exists-with-subtype): **2423 pass / 138 fail / 14 SKIPPED**; batch 139 (Phase 3 leg 1, splitPart): **2422 pass / 139 fail / 14 SKIPPED** of 2575; batch 132 (Phase 0.6): **2421 pass / 140 fail / 14 SKIPPED** of 2575 (+1 real defect the fixed referee surfaced: splitPart missing part, ledger §8); batch 131 (Phase 0.5b): 2422 / 139 / 14 (the page-membership verdict brought batch 130's 9 back); batch 130: 2413 / 148 / 14; batch 128 (Phase 0.3): 2422 / 139 / 14, pinned as SETS (`rcorpus/duckdb-fail-roster.txt`, `duckdb-skipped-roster.txt`). Was 2454 / 121 through batch 127: the −32 is the audit's zero-assertion census — 14 skipped + 18 named FAILs (`verdict-gap:guard-assert-in-expression-helper`, ledger §6). |
| H2 lane (gate 5) | batch 144: **1989 pass / 566 fail / 14 SKIPPED / 6 ACCEPTED**; batch 142: **1989 pass / 572 fail / 14 SKIPPED** (18 shells + 9 exists tests via the EXISTS membership rewrite); batch 141: **1962 pass / 581 fail / 32 SKIPPED**; batch 140: **1962 pass / 599 fail / 14 SKIPPED** (a Phase 3 leg gained on both lanes); batch 135: **1961 pass / 600 fail / 14 SKIPPED** (the renderer quick win; no further H2 legs until Phase 7); batch 132: 1847 / 714 / 14 (the seed-ledger fault fixed: +13); batch 131: 1834 / 727 / 14; batch 130: 1825 / 736 / 14 (sets: `h2-fail-roster.txt`, `h2-skipped-roster.txt`); was 1866 / 709 |
| Strength of the passes (batch 133, Phase 0.7) | DuckDB: DIFFERENTIAL 1512 (1259 with a literal beside it + 253 alone) / LITERAL 838 / CARDINALITY 22 / SPELLING 49 / NONE 0 — pinned monotone (≥ 1512, ≤ 49, ≤ 22); H2: 1198 (977 + 221) / 575 / 18 / 56 (≥ 1198, ≤ 56, ≤ 18). |
| Referee outcomes (DuckDB lane) | batch 142: verify8 1595 MATCH / 6 DIVERGED / 18 DECLINED / 0 FAULT; strength LITERAL 838 → 856; batch 141: verify8 1595 MATCH / 6 DIVERGED / 18 DECLINED / 0 FAULT; batch 140: verify8 1595 MATCH / 6 DIVERGED / 18 DECLINED / 0 FAULT; batch 139: verify8 1594 MATCH / 6 DIVERGED / 18 DECLINED / 0 FAULT; batch 132: verify8 1593 MATCH / 7 DIVERGED / 18 DECLINED / 0 FAULT (45 page-membership); text-decided tests by reason: rows-underivable 29, oracle-declined 22, plan-params-unbindable 6, foreign-dialect DB2 30 / Composite 7 — all pinned as ceilings. Batch 131: 1592 / 6 / 20. Batch 130: 1572 / 6 / 39. Before: verify 1591–1592 MATCH / 6 DIVERGED / 20–21 DECLINED; fetch-chain 49; fetch-texts 23; plan 28 / 4 DECLINED. The ±1 is `query::paginate::testPaginatedByVendor` (a page over a sort with ties; the two databases order ties differently — data nondeterminism, counted). |
| Harness | `core/src/test/java/com/legend/rcorpus/MinimalCorpus(Test).java` (~700 lines): discovery by stereotype, engine suite order, platform-namespace guard, setups derived once, session choice from `ProgramFacts`. The old 13.6k-line harness is deleted (batch 115). |
| Thread-locals in main | **3** after batch 138 (Phase 2d: RelationReads.DERIVED_DEPTH a parameter; TestResources deleted, the resolver rides ExecuteOptions.resources): SqlTypeCensus CONTEXT + WIRE_WATCH, StampCensus.CONTEXT — census attribution, die with 2c (the fact ledger, ~2 batches, recommended after Phase 3 and before item 4). Static accumulators in main: SqlTypeCensus 30 + CanonicalDivergence 17 (die with 2c) + SystemDatabase.IDS (an id mint, stays). 5 after batch 137 (Phase 2b: RawSqlBoundary RECORDER/META_RECORDER and ExecutionTrace.LAST gone — the recorder rides ExecuteOptions, the trace rides ExecEnv): SqlTypeCensus CONTEXT + WIRE_WATCH, StampCensus.CONTEXT (2c), RelationReads.DERIVED_DEPTH, TestResources.RESOLVER (2d). 8 after batch 136 (Phase 2a: NullSemantics.FILTER_POS / VERBATIM_EQ, EngineTextBoundary, TextGoldens gone): RawSqlBoundary RECORDER + META_RECORDER, ExecutionTrace.LAST (2b), SqlTypeCensus CONTEXT + WIRE_WATCH, StampCensus.CONTEXT (2c), RelationReads.DERIVED_DEPTH, TestResources.RESOLVER (2d). The execution-option family was zero since batch 122. |
| Fail roster of record | `core/src/test/resources/rcorpus/duckdb-fail-roster.txt` (121 names) + `h2-fail-roster.txt` (709) — the gate's SET pin since batch 126; `docs/parked/duckdb-fail-roster-batch119.txt` is the dated snapshot with messages. Regenerable in 60s. |

The five "harness endgame" items (memory `harness-rebuild-audit`): 1 single resolution pass
DONE (116); 2 corpus-library.pure into the platform DONE (117); 3 harness body scans → platform
facts DONE (118); 4 referee judges in the database — DESIGNED + MEASURED + coded, PARKED (this
plan puts it back at step 6); 5 censuses — DONE under option A (batch 123: the four unread
counters deleted; `SqlTypeCensus` + `CanonicalDivergence` STAY because the pct module pins on
them — see step 1c).

Batches 113–125 are recorded in docs/GATES.md with what each did and why. Read batch 120's
record for THE RULE that governs frame facts: the from is the only carrier of an execution
frame's facts (post-processors, time zone, options); `ExecEnv.frame` is a cache of the outermost
executed from, never a source; verdict arms build their reads with the producer's bound context;
every reader call under a statement binds the statement's let chase, with lambda-parameter
scoping (`ContextReading.scope`).

## 1. Step 1 — the thread-local sweep (main scope, 12 fields, three batches)

Measured 2026-09-08 (`grep -rn "ThreadLocal<" core/src/main/java`): 12 fields in 9 files.
core test has 6 (all `H2Verify`, they die with step 6); pct has none. This is the whole project.

**1a. Modes that change what the compiler emits (4 fields) — FIRST, verdict-relevant.**
- `lowering/NullSemantics.FILTER_POS`, `NullSemantics.VERBATIM_EQ` — lowering flags set by a
  caller around a run; the lowering reads them to pick the null policy of a comparison.
  Principled home: a parameter of the lowering context (the Lowerer already takes builder
  options: `withEngineExistsJoinForm`, `withDbTimeZone`, `withInstanceKeys`); the filter
  position is a property of the node being lowered (the predicate position), not of the thread.
- `lowering/EngineTextBoundary.ACTIVE`, `sql/dialect/TextGoldens.ACTIVE` — "we are rendering
  for an engine-text verdict" flags read by the renderer. Principled home: the render request
  (the dialect/renderer instance the text surface constructs — `EngineStyleH2` etc. are already
  distinct renderer classes; the flag becomes a constructor argument or a renderer subclass),
  exactly as the PCT render flag became `ExecuteOptions` (batch 122).
  Acceptance: rosters exact both lanes; `PlatformNamesGuardrailTest` and `CodeShapeGuardrailTest`
  ratchets hold; no new statics.

**1b. Ledgers that record what ran (3 fields).**
- `sql/dialect/RawSqlBoundary.RECORDER` + `META_RECORDER` — the raw-SQL ledger (every executed
  raw statement with its kind, `Raw(sql, query)`), read by the referee to seed the H2 mirror
  (`recordedSql()`). It is per-test state the HARNESS owns: the recorder should be an object the
  harness creates per test and hands to the executor through `ExecuteOptions` (or the
  environment), and reads back — not a thread slot the executor writes into.
- `exec/ExecutionTrace.LAST` — the `executionTraceID` comment stamped on each executed SQL,
  read by `PlanAllocations.registerActivityRows` (`lastComment()`) to name an activity. It is an
  identity of ONE execution: return it with the result (`ExecutionResult` already carries the
  plan's outputs) or pass it forward explicitly from `Executor.execute` to the activity register.

**1c. Attribution + census (3 fields + the two counter classes) — the item-5 remainder.**
- `SqlTypeCensus.CONTEXT`, `SqlTypeCensus.WIRE_WATCH`, `StampCensus.CONTEXT`, and the static
  adders of `SqlTypeCensus` / `CanonicalDivergence`.
- USER DECISION (recorded in handoff §0 batch 123): (A) keep as is — item 5 closed at 5a;
  (B) delete with their pins — loses the PCT lane's eight typed-IR zero-pins
  (`pct/…/PctCensusGate`) and the dual-verdict alarm (`ChannelB*Test`: `sqlDisagreeCount()==0`,
  the DB byte verdict and the host referee never disagreed; its corpus-side twin already died
  with the old runner in batch 115); (C) the principled form — the executor returns a per-run
  FACT LEDGER (label lies, wire divergences, dual-verdict disagreements) with the result, the
  PCT gate and Channel-B read that ledger, then the statics die. Recommendation: (C) as batch 1c
  now that 1a/1b make the pattern routine; never (B).

**1d. Guards (2 fields).**
- `normalizer/RelationReads.DERIVED_DEPTH` — a recursion depth: becomes a parameter.
- `exec/TestResources.RESOLVER` — a test-resource resolver injection: becomes a constructor /
  option argument (the harness passes it).

Each of 1a–1d is one batch with the batch protocol; 1c may be two. Static accumulators that are
NOT sinks (e.g. `SystemDatabase.IDS`, an id counter) stay.

## 2. Step 2 — the five real compiler legs (IMPL = 7, of which 5 are real)

The granular ledger (docs/LEDGER_GRANULAR_2026_09_06.md) + docs/BURN_BREAKDOWN_2026_09_05.md hold
the per-test rows. The current 121 by bucket (measured 2026-09-07, after the 6 unclassified were
classified): IMPL 7 (5 real + parked nested ModelJoin + the post-processor transform lambda,
which is code-as-data), IMPL-parked 2, REVISIT 5, TEXT 41 + 10 reclassified = 51, ENGINE 31,
OTHER STORES 8, NAMED 11 (incl. 5 connection-equality = code-as-data parked).

Order (most tractable first), each leg = one batch, three fix cycles then a named wall:

**2a. `projection::exists::testExistsAsNullWithSubType` — route selection by subtype cast.**
Homework COMPLETE (ledger row): the wall is `Substitution.assocLeaf` (leafBinding == null under a
NESTED target) via `rewriteExists → rewriteLambda → rewritePath`. The head `fnScope` IS
registered in the exists scope (`CorrelatedSubselects.nestedAssocMaterials` →
`AssociationJoins.aggJoinMaterial` → `sources.get(mapping, FunctionScope)`), but its target's
bindings have no `stc_<Public>___id`: `ClassSources` synthesizes stc pseudo-bindings only for
subclasses mapped over the SAME root table (`sameRootTable`, ClassSources ~L845–940); here
FunctionScope has no mapping, Private[map2] is on privateFn, Public[map3] on publicFn, and the
property is routed PER TARGET SET (`fnScope[map2]: @privateFnJoin`, `fnScope[map3]:
@publicFnJoin`). The engine (pureToSQLQuery.pure L10047: `subType_Any_m__T_1__T_m_` →
`processNoOp`) treats the cast as a NO-OP at SQL level — the router's set selection does the
work — and its golden lowers `$f.fnScope->subType(@Public).id->isNotEmpty()` inside the exists
subselect as ONE left join of the map3 route with `"publicfn_0".id is not null`. THE LEG: a
class-typed property with per-target-set Join PMs navigated through `subType(@X)` resolves to
the PM route whose target set is X's set (join that set's table, read the leaf), at the top
level and in nested scopes — the nested `AssocSub` for such a head carries per-route targets
(the `MixedRoute`/`targetSetId` machinery at ClassSources L405–460 is the top-level precedent;
`ctx.routedTargetSetOf(mapping, prop)` names the set). Model/mapping: engine
`relational/functions/tests/projection/testExists.pure` L23–60, mapping
`mappingForMultipleSubTypes` L257. Golden SQL at L122.

**2b. `executionPlan::tests::inheritance` — the plan printer over an Operation set (two walls).**
Wall 1: `ScanRelations.rootImpl` finds no Relational class mapping for `RoadVehicle` — the
mapping (`relational/tests/mapping/inheritance/testInheritanceRelational.pure` L401,
`inheritanceMappingDB`) maps RoadVehicle/Vehicle/Gasoline as `Operation` sets
(`inheritance_OperationSetImplementation`) whose members are the mapped subtypes. The golden's
type block is `Class[impls=(Bicycle | inheritanceMain.map2),(Car | inheritanceMain.map1)]` —
`rootImpl` must resolve an Operation set to its member impls (order = the golden's) and
`PlanText` L277 must render several impls. Wall 2: the plan's SQL is the union base
(`unionBase`, `u_type`, `pk_0_0/pk_0_1`); the plan printer's alias-to-table resolution walls on a
Subselect (`PlanText` L1089 "alias not resolvable to a table") — the same wall as
`testEnumFilterWithUnionMappingPlanGeneration`. Do wall 1 as its own batch (mechanical,
Java over model metadata — a text surface); wall 2 is the "plan printer over union bases" leg
and unlocks 2 tests.

**2c. `m2m2r::planGraphFetchWithNestedDerivedProperty`** — "class query under TypedGraphFetch is
not resolvable yet (HN vocabulary)" (ledger L13). Graph-fetch plan through a model-to-model
chain with a nested derived property. Not probed beyond the wall; probe first.

**2d. The union many-column pair** (`testUnionTwoRelationMappings_ManyColumnProject` ×2) is a
TRACED DIVERGENCE marked `revisit` (12-column distinct over a union of two relation mappings):
NOT a leg — step 4.

**2e. `testNestedModelJoinCompoundInnerCondition`** — PARKED with three walls (handoff note,
batch 111). Code-as-data adjacent; step 5.

## 3. Step 3 — the TEXT referee leg (two batches) + naming the rest

User ruling 2026-09-08: referee as many text goldens as makes sense. Measured against the
breakdown's per-test notes (docs/BURN_BREAKDOWN_2026_09_05.md §2):

- **3a. Referee bindings on BOTH sides for parameterized / open-variable plans (2–4 tests):**
  `testGroupByWithOpenVariableInAgg`, `testGroupByWithTwoOpenVariablesInAggAndFilter`,
  `testTemporalDateVariableInFunctionExpressionWithPropagation`, `m2m2r::testProp3`. The plan
  replay already binds referee values for the golden (`VerdictQueries` parameter bindings,
  `SqlReplayOracle.verifyPlan(…, bindings, …)`); our side must derive rows under the SAME
  bindings (today "rows underivable (parameterized)").
- **3b. Postgres goldens replayed on the DuckDB session (1–2 tests):** DuckDB speaks a
  Postgres-compatible dialect; rule: the session that speaks the golden's dialect is its oracle
  (no mirror). `tds::postgres::testSortQuotes` (+ its own "no scalar lowering" wall first).
- **Fall to the compiler legs, not to referee work (5–6):** plan-text goldens blocked by our own
  walls — inheritance (2b), withPlatform (DialectCapability STRING_AGG wall), the cross-db
  runtime-only plan (mapping-argument wall), viewToTDS (unknown function), window `over`
  overload, the two-mappings-one-runtime pair. The existing plan replay judges them once the
  wall falls.
- **Named decisions, never burned (~30):** the 11 substring asserts
  (`assert($sql->contains('union_gen_source_pk_0'))` — no golden SQL exists to run; their rows
  asserts already pass), ~9 DB2/SQL-Server goldens (no engine to execute them), 2 unformatted
  plan texts (whitespace stripped; parse nowhere), 7 plan-as-data (step 5). The 10 "TEXT
  (reclassified)" ledger rows are to be re-read one by one and split the same way.
- Acceptance: each refereed test's verdict is rows; the ledger names every remaining TEXT test
  with its reason; the honest burn target is restated (≈30 decisions + step-5 families).

## 4. Step 4 — REVISIT decisions (5) — one short session

`revisit:` rows are traced golden disagreements, decided at the end by the user, never
"resolved" by code. The five (ledger `## REVISIT (5)`), plus the union many-column pair (2d).
For each: the trace is in the ledger row; the decision is engine-defect / our-defect /
accepted-divergence. Output: five ledger rows closed with a decision and, where ours, a leg.

## 5. Step 5 — code and metamodel as data (the big one, ~45 tests) — SIZE FIRST

Homework that exists (never re-derive): docs/CODE_AS_DATA_HOMEWORK_2026_09_05.md (the 5
connection-equality tests fail in both channels; the mechanism is on branch
`wip/72c-extension-registry-read`; the right leg is the expression tree as m3 instances),
docs/METAMODEL_AS_RELATIONS_HOMEWORK_2026_09_02.md (read-only system DB per graph,
`relational_elements` table, boot layer, batch 5 real-name switch; MISSING feature = match/cast
over a discriminated row), docs/WORLD_MAP.md + Charter C6 (three kinds of Pure), memory
`metamodel-as-relations-state`, `code-as-data-leg-parked`, `metamodel-in-database-ruling`
(NO Java metamodel/lineage/plan facts in the verdict path; end-state = metamodel AS RELATIONS).

Families it unlocks: ENGINE 31 (routeFunction, planSqlStatement, relOpToString,
sqlQueryToString — Pure over the relational metamodel), plan-as-data 7, connection equality 5,
the post-processor transform lambda (1), nested ModelJoin (parked), and the referee's enum
decode (step 6 joins the system DB's enumeration-mapping table instead of a Java-built VALUES
list). Recursion = recursive CTE after code-as-data (user 09-06); dynamic = staged compile.

Rule for this step: pick the SMALLEST slice with a witness test, land it end to end (chain
green, the witness flips), then widen. Candidate first slice: connection equality (`match` over
extension-contributed arms = scalar match over m3 instances) — the mechanism branch exists.
Write the sizing doc before the first batch; it names the slices, their witnesses, and the
order.

## 6. Step 6 — the referee judges in the database (item 4), then single-shot

Design + census + implementation plan: docs/REFEREE_IN_DATABASE_DESIGN_2026_09_07.md
(§Implementation plan). Code parked: docs/parked/InDbVerdict.java (golden rows → a typed DuckDB
table via `DuckDBAppender` by JDBC type; verdict = count of two-way EXCEPT ALL; enum decode by
LEFT JOIN to a VALUES relation → after step 5, a join to the system DB). Three batches:
(i) the executor's prepare/run split (`prepareTyped` → Planned | Answered; `renderValue`) — no
behaviour change, roster exact; (ii) `verifyInDb` wired for row results, outcomes preserved or
every change named (the Java two-ulp float tolerance goes; ordered goldens judged as multisets,
counted — the positional compare was already dead: `ORDERED_QUERY` is never set); (iii) the
row-compare policy deleted (~900 of 1,256 lines of `H2Verify`; the graph compare, the mirror
machinery, `decodeOf` and the decline roster stay — the graph compare gets its own leg after).
Why here and not earlier: it changes no verdict today (outcomes stable across every batch) but
single-shot NEEDS the verdict to be a SQL query, and step 5 makes the enum join principled.

Then **single-shot**: each test as one statement — WITH seeds bound in the IR + one verdict
SELECT with asserts as boolean columns; DDL goldens, executeInDb reads and mid-body reseeds are
extra shots (handoff §0 Phase 1b step C). Owed before it: the NavPath/Hop path-model leg + the
core-wide `startsWith` audit with a shrink-only ratchet (user ruling 2026-09-06, memory
`string-hacking-audit-navigation-paths`).

## 6b. Step 6b — the verdict residue and the graph leg (added 2026-09-08, "close-out" gaps)

**Measure the host-judge residue NOW, then burn it.** `AssertVerdicts.finish` takes the
database's byte verdict as the verdict of record (`byteHeld`) and falls back to the HOST lattice
(`PureAsserts`, `TdsCompare`, `JsonCompare`) when the canon rider DECLINED the side
(`CanonRider.decline(reason)`). Nobody counts how many verdicts Java judges today. Batch: a
per-lane count of verdicts by channel (byte / host-fallback / text) printed by the harness like
the referee outcomes, then the decline reasons become legs (each reason is a shape the canon
render cannot ride yet). Single-shot's acceptance is "host judged ZERO", not "the rider was
deleted".

**Graph / JSON verdicts in the database.** Item 4 leaves `H2Verify.goldenGraphCompare` (+
`bookkeepingAlias`, the pk-collapse) in Java. The leg: our graph result is a JSON document the
database built; the golden is rows with the engine's bookkeeping columns. Judge in the database
by flattening our JSON with path extraction to the golden's row shape (or assembling the
golden's rows into the tree with the same grouping the engine's graph fetch uses) — one query,
no Java tree walk. Schedule: right after item 4 (iii).

## 6c. Step 6c — order independence as a gate (added 2026-09-08)

At least one test passes in the full run and fails scoped
(`resultSourcing::relationalResultSourcingOfListExecutionPlan`), and the H2 lane's
`testFullOuterJoinSimple` swaps with another test between runs: state leaks between tests
(session temp tables, the read-only system database per graph, package setups). Census: run
every test SCOPED (`-Drcorpus.test=<fqn>`, one JVM each or one JVM with the session reset) and
diff against the full-run roster; every difference is a leak to name and close. Then a gate:
the scoped roster equals the full roster. Without it the 121 is honest in ONE order only.

## 6d. Step 6d — the H2 lane's end (USER DECISION, added 2026-09-08)

709 failures, floor 1866, "advisory" (memory `h2-backend-and-sqlglot-vision`). Either (A) it
stays advisory with a floor — then say so and stop reading its number as progress; or (B) it
gets its own dialect-capability burn (the emulation rules in `CarrierStrategies` are its legs:
list carriers, FULL OUTER over carrier wrappers, …). Not decided; not in the floor arithmetic.

## 6e. Step 6e — the name-check ratchet to zero (added 2026-09-08)

`PlatformNamesGuardrailTest` holds literal `equals("meta::…")` checks outside `PlatformTypes` at
72, shrink-only; the stated goal is ZERO (every platform identity dispatched through the catalog
/ `PlatformTypes`, never a bare FQN compare in an arm). Batches by file (the audit of
2026-09-06 listed 76 checks in 32 files). Also the audit's finding 6: the harness's vacuous-body
check (`body == true`) becomes a `ProgramFacts.vacuous` fact (trivial; ride any batch).

## 6f. Definition of DONE (added 2026-09-08)

The core_relational harness is closed out when ALL of these hold and are gated:
1. The driver does four things only: discover tests by stereotype, establish setups through
   the platform, run ONE statement per test (single-shot), read one boolean row. It interprets
   no Pure (no body scans, no keyword tests, no vacuous check — facts come from `ProgramFacts`).
2. The referee TRANSLATES only: it makes the engine's goldens executable (mirror, seeds, temp
   tables, plan replay) and hands rows to the session; the DATABASE judges every row verdict
   (item 4), every graph verdict (6b), and every assert (single-shot). No Java compares values:
   `PureAsserts`, `TdsCompare`, `JsonCompare`, the canon rider/render and `H2Verify`'s compare
   policy are DELETED, not bypassed.
3. Zero thread-locals and zero static accumulators in main that a verdict or a test can read
   (step 1 incl. 1c option C); the PCT pins read a per-run fact ledger.
4. Every test passes scoped exactly as in the full run (6c gate).
5. Every failing test is NAMED with a reason in the granular ledger (TEXT decision / engine
   machinery / other store / revisit decision / code-as-data slice), and the floor number in §7
   is restated from the ledger, not from memory.
6. The name-check ratchet is at zero; `JavaEvalLedgerTest`'s verdict-class pins are at the
   sizes the single-shot verdict leaves (the K-arm dies with it).
7. One final state document replaces the running handoff (§0 of this plan rewritten as
   "closed out on <date>: what remains, why").

Adjacent, NOT in scope: the PCT lane's own expected-failure list (docs/PCT_EXPECTED_FAILURES.md
— pure-function conformance, 1 fail + 22 err expected) and the NavPath path-model cleanup
(owed before single-shot, memory `string-hacking-audit-navigation-paths`).

## 7. The honest floor

(Restate from the ledger at each step; 6d's H2 decision is outside this arithmetic.) 121 today. Real legs 5 (step 2) + referee leg 4–6 (step 3a/3b) + walls that also carry text
goldens 5–6 → ≈ 105. Step 5 families ≈ 45 → ≈ 60. REVISIT 5 + parked 2 decided → ≈ 55. What
remains is TEXT decisions (~30), OTHER STORES (8), ENGINE machinery outside code-as-data, and
NAMED receipts — named, with reasons, in the ledger. "Zero" means zero UNNAMED failures.

## 8. Lessons that must not be re-learned (2026-09-06 → 09-08)

- Five roster-diff fixes without a stated rule = probing; the user stopped it; the cause was
  then measured in ONE instrumented run. Instrument, don't list suspects.
- The corpus test SKIPS silently when the engine root is mangled (zsh `$R`); a green-looking
  0.5s run is a skip.
- The chain's failure copies (`gates-neema.g1.out` etc.) persist from OLD failures; read the
  log's EXIT lines for the run you launched; a background waiter's `pgrep` must not match itself
  (`pgrep -f "[t]ools/allgates"`).
- A display sort in the harness counts against `HarnessDisciplineTest`'s sort-site pin (use a
  TreeMap); a new JDBC-touching file in test roots must be registered with
  `JdbcSurfaceCensusTest`; raw `new SqlExpr.Column` outside the stamped doors trips
  `CodeShapeGuardrailTest`; `com.legend.sql` may not reference `com.legend.error`.
- `JavaEvalLedgerTest` pins verdict-class line counts (SqlTextVerdicts 1071 after batch 120):
  context plumbing moved out of a static sink is a justified bump; evaluation is not.
- Item 4 was parked by MY recommendation (it changes no verdict today) — the user accepted
  "burn first"; it returns at step 6 because single-shot needs it.

## Appendix A — step 1's fields: who sets, who reads (measured 2026-09-08)

| Field | Set by | Read by | Principled home |
|---|---|---|---|
| `NullSemantics.FILTER_POS` | `Lowerer` L1470, L1926 (`enterFilter()` scope around lowering a filter predicate) | `NullSemantics` (the null-arm choice of comparisons) | a parameter of the lowering call for the predicate node (the Lowerer knows it is lowering a filter) |
| `NullSemantics.VERBATIM_EQ` | `Lowerer` L1472/L1932 (`enterVerbatimEquality()` / `keep()`) | `NullSemantics.equalNullArms` via `Scalars` L156 | same: an argument of the equality lowering, not a thread flag |
| `EngineTextBoundary.ACTIVE` | `StatementExecutor` L619 (`enter()` around the engine-text render) | `CastPolicy` L50 (`c.wire() && active()`) | the render request: the engine-style renderer instance / a lowering option, like `withDbTimeZone` |
| `TextGoldens.ACTIVE` | `StatementExecutor` L620, L640 (`enter()`) | `EngineStyleH2` L1031 | the renderer instance (EngineStyle* are already distinct classes; make the mode a constructor argument) |
| `RawSqlBoundary.RECORDER` / `META_RECORDER` | `MinimalCorpus` L442 (`record(recording)` per test); `StatementExecutor` L1948/L2563/L2593/L2643/L2644 (`recordExecuted`), L2594/L2662/L2665 (`recordMeta`) | `ReplayOracle` L198/L859/L897 (`recordedSql()`) | a recorder OBJECT the harness creates per test and passes through `ExecuteOptions`; the executor appends to it; the referee reads it from the harness |
| `ExecutionTrace.LAST` | `Executor` L238 (`stamp(sql)` per executed query) | `StatementExecutor` L1484, L1532 (`lastComment()` → `PlanAllocations.registerActivityRows`) | the executed statement's comment returned with its result (or passed forward from `Executor.execute` to the register) |
| `SqlTypeCensus.CONTEXT` / `WIRE_WATCH`, `StampCensus.CONTEXT` | `pct/…/ChannelB.runOne` L191–192 (per test); the census itself | the census reports; `PctCensusGate` | step 1c (option C: per-run fact ledger) |
| `RelationReads.DERIVED_DEPTH` | itself (recursion guard inside `RelationReads`) | itself | a depth parameter of the recursive method |
| `TestResources.RESOLVER` | `MinimalCorpus` L443 (`register(path -> …)`) | `CsvLoad` L43 (`TestResources.read(path)`) | a resolver argument of the CSV load (through `ExecuteOptions` / the environment) |

## Appendix B — the commands (copy these; do not retype the flags)

```bash
# DuckDB lane (gate 4), exact roster + set difference against the roster of record
mvn -pl core test -Dtest=MinimalCorpusTest -Dsurefire.excludedGroups= \
  -Dlegend.engine.root=/Users/neemsandv/legend/legend-engine -Dlegend.pure.root=/Users/neemsandv/legend/legend-pure > $T/lane-duck.out 2>&1
grep -a "\[corpus2\] pass=\|\[corpus2\] roster\|referee-outcome\|LOST\|GAINED" $T/lane-duck.out
# batch 126 (Phase 0.1): the test itself pins the fail roster as a SET of NAMES per lane against
# core/src/test/resources/rcorpus/{duckdb,h2}-fail-roster.txt (LOST / GAINED in the assertion message,
# also under -Drcorpus.test). By hand, by NAME (never name+message — one drifted message read as a regression):
grep -a "\[corpus2\] FAIL" $T/lane-duck.out | sed 's/.*FAIL //' | cut -d' ' -f1 | LC_ALL=C sort > $T/fail-now.txt
comm -13 core/src/test/resources/rcorpus/duckdb-fail-roster.txt $T/fail-now.txt   # LOST (must be empty)
comm -23 core/src/test/resources/rcorpus/duckdb-fail-roster.txt $T/fail-now.txt   # GAINED (explain, then update the file)

# H2 lane (gate 5): add -Drcorpus.backend=h2 (floor 1866)
# one test, with stacks and the resolved dump:
LEGEND_LITE_STACKS=1 LL_DUMP_RESOLVED=1 mvn -q -pl core test -Dtest=MinimalCorpusTest -Dsurefire.excludedGroups= \
  -Drcorpus.test=<fqn> -Dlegend.engine.root=… -Dlegend.pure.root=…   # output: core/target/surefire-reports/…-output.txt or the redirected file
# SQL of what ran: LEGEND_LITE_DUMP_SQL=1 (or LL_TMP_SQL=1 for the exec-sql lines)

# full chain (tree FROZEN until ALLGATES_DONE); outputs kept in $TMPDIR/gates-neema.{log,g4.out,g5.out}
LEGEND_ENGINE_ROOT=/Users/neemsandv/legend/legend-engine LEGEND_PURE_ROOT=/Users/neemsandv/legend/legend-pure \
  nohup caffeinate -dims tools/allgates.sh > $T/allgates.log 2>&1 &
# waiter (background): the pgrep pattern must not match itself
until grep -q ALLGATES_DONE $L && [ "$(stat -f %m $L)" -ge "$start" ] && ! pgrep -f "[t]ools/allgates" >/dev/null; do sleep 15; done

# downstream modules compile against core's INSTALLED jar: after a core change touching pct,
mvn -q -pl core install -DskipTests && mvn -q -pl pct test-compile
```

## Appendix C — the guardrails and what each pins (they WILL fire; that is their job)

| Test | Pins |
|---|---|
| `PlatformNamesGuardrailTest` | literal `equals("meta::…")` checks outside `PlatformTypes` — 72, shrink-only; no runtime-shape walker outside `ExecutionContext.Reader` |
| `JavaEvalLedgerTest` | ROOT_CLASSES / file lists of Java-evaluating classes (register new root classes consciously, e.g. `ExecuteOptions.java`); verdict-class stripped-line pins (SqlTextVerdicts 1071 — bump only with a written justification) |
| `HarnessDisciplineTest` | per-file sort/distinct site counts (display sorts count: use a TreeMap) |
| `CodeShapeGuardrailTest` | raw `new SqlExpr.Column(…)` sites (7): new references go through `Column.of(...)` / `derived` / `physical` |
| `ArchitectureTest` | static-sink registry (every static accumulator named), verdict classes reachable only from the verdict seam, invariant 6a: `com.legend.sql` depends only on itself and the JDK (no `com.legend.error` from a dialect — throw `DialectCapability`) |
| `JdbcSurfaceCensusTest` | every file touching `java.sql` in test roots is registered (InDbVerdict will need this) |
| `ObservabilityGuardrailTest` | main-scope `System.err` print sites, asserted EXACTLY at 34 (unchanged by batch 123's final cut; the stamp census print in `StampCensus.fire` is one of them and goes with step 1c) |
| `ErrorShapeGuardrailTest` | broad-catch sites per file |
| `DanglingStateGuardTest` (batch 129) | every static ThreadLocal/Atomic*/LongAdder/volatile across all modules has readers ⟺ writers (register at ZERO since batch 130); guards pin only files in the tree and cite only live mechanisms |
| `MinimalCorpusTest` | per lane, as SETS: the fail roster (`rcorpus/*-fail-roster.txt`), the SKIPPED roster (0.3), the `ordered-keys-unmappable` register (0.5) + the denominator 2575 + ceilings on inert setups (5) and arrival-order leniency tests (DuckDB 108 / H2 11); LOST and GAINED both fail; holds on the scoped subset too |
| gate 7 (`PCT`) | `PctCensusGate` ceilings per suite; Channel-B dual-verdict assertions (see step 1c) |

## Appendix D — document map (read in this order for any step)

- docs/END_TO_END_PLAN_2026_09_08.md — this plan.
- docs/GATES.md — one entry per batch (113–125 are this week's); the record of what changed and why.
- docs/SESSION_HANDOFF_2026_09_02.md §0 — per-batch paragraphs incl. the batch-123 item-5 finding (options A/B/C) and the batch-120 rule.
- docs/BURN_BREAKDOWN_2026_09_05.md — the 168-fail breakdown by leg (IMPL L1–L16, TEXT T1–T5, ENGINE, OTHER, NAMED) + status line.
- docs/LEDGER_GRANULAR_2026_09_06.md — per-test rows with walls; the exists-with-subtype probe; the 6 classified.
- docs/parked/ — `duckdb-fail-roster-batch119.txt` (roster of record), `InDbVerdict.java` (item 4), the batch-120 patch scripts.
- docs/REFEREE_IN_DATABASE_DESIGN_2026_09_07.md — item 4 design, census, three-batch implementation plan.
- docs/BATCH_120_FRAME_FACTS_HANDOFF_2026_09_07.md — the from-is-the-carrier rule, the reader call-site inventory.
- docs/AUDIT_BATCHES_116_118_2026_09_07.md — the audit that found the option statics.
- docs/CODE_AS_DATA_HOMEWORK_2026_09_05.md, docs/METAMODEL_AS_RELATIONS_HOMEWORK_2026_09_02.md, docs/WORLD_MAP.md — step 5.
- Memory: `end-to-end-plan-2026-09-08` (pointer), `harness-rebuild-audit` (history), `stop-when-probing-blind`, `sequence-by-importance-not-size`, `code-as-data-leg-parked`, `metamodel-as-relations-state`, `string-hacking-audit-navigation-paths` (owed before single-shot).
