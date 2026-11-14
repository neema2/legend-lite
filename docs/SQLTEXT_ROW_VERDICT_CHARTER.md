# SQL-TEXT → ROW-VERDICT CHARTER (2026-09-01, user-ratified design)

**READ WHOLE BEFORE CODE. This charter was ratified line-by-line in
conversation (2026-09-01); do not re-derive the design — verify your
homework against it.** Parent program: harness deletion item 1
(WHOLETEST_COMPILATION_CHARTER.md). Bound by
[[metamodel-in-database-ruling]] (memory): nothing here consumes
Java-computed metamodel facts — the producers below are the emitters
and the executor, both already platform.

## 0. The one-sentence rule

**A SQL-text assert's verdict is ALWAYS rows, never text — including
when the text matches byte-for-byte.** Golden text is H2-flavored;
we execute a DuckDB translation. Identical text proves the emitter's
spelling, not the answer. Text match/diff is a CENSUS number
(shrink-only ratchet for emission work), never a verdict.

> **Corrected 2026-11-02 (the test-is-the-product plan's M10b, recording the user's ruling of 2026-09-21, option
> 1, which the code has carried since):** a text byte-equal to the golden IS the assert's verdict -- the engine's
> own -- and the rows leg is the APPEAL on a failed text alone (MATCH holds whatever the text said, DIVERGED and
> FAULT fail, a decline keeps the text as the contract, counted). The sentence above describes the design before
> that ruling; §3-§7 (the derivations, the referee, the normalization inventory, the order rule) stand as the
> appeal's.

## 1. The lane (measured 2026-09-01)

1,545 tests fall back on text-policy; plus ~15 plan/SQL-text
platform-fail rows. Walk-side machinery already row-verifies most of
it: per sweep ~463 text-matched + ~1,001 text-divergent all
row-verified, 0 row divergences, ~21 counted replay declines, 44
"text-only". The clean-sheet work is RE-HOMING triggers and ownership
— replay itself exists and is proven.

The 44 "text-only" decompose (measured): 23 plan-text (17
plan-literal + 6 plan-let) + ~21 sqlstring renders. Both are
runnable — §4 and §5.

## 2. Platform vs testing — the ruled split

- **Platform** (product-legitimate, mostly exists): SQL/plan text
  emission (toSQLString/planToString JAVA_ROUTINEs), executing SQL on
  a connection, the H2 dialect as a real backend, and the
  assert-verdict layer.
- **Testing**: the goldens, the seed ledger + H2 mirror, the
  comparison policy, the divergence census, the plan replayer.
- **The bridge — an SPI, the AssertListener precedent**: the platform
  defines a tiny interface (`SqlReplayOracle`: rows for a SQL text;
  plan replay entry) carried on ExecEnv; the harness registers the H2
  mirror implementation per run. Production registers nothing and a
  SQL assert WALLS loudly (correct: no goldens outside tests). The
  platform never learns what a golden is.

## 3. The pipeline, step by step (the ratified walkthrough)

1. Whole test body compiles as one unit (existing).
2. Query-level lets substitute forward (existing) — after this the
   assert's typed argument tree CONTAINS the producer chain inline.
3. Statement-root asserts reach the verdict layer before evaluation
   (existing).
4. **Detection = navigation of the typed tree by node kind + exact
   callee FQN** (never text sniffing): a TypedNativeCall to
   toSQLString/planToString/executionPlan in an argument tree, or the
   assertSameSQL-family form itself. The producer node's CHILDREN are
   the structured inputs: query lambda, mapping ref, dialect,
   runtime.
5. Derive four artifacts:
   a. OUR TEXT — the platform emitter (the same routine staging
      calls).
   b. GOLDEN TEXT — ordinary evaluation of the literal side.
   c. OUR ROWS — the query lambda from the producer's own children,
      run through the ordinary pipeline on DuckDB (the referee
      executes it even when the test never did — this dissolves most
      of "text-only": the pure query is IN the assert's arguments).
   d. GOLDEN ROWS — the oracle SPI replays the golden text on the H2
      mirror.
6. VERDICT = row compare under §6's ratified policy (order-sensitive
   per §7). Text result → census.
7. Outcomes: rows= & text= → clean pass. rows= & text≠ → pass +
   emission-census row. rows≠ → FAIL (real bug, whatever the text
   said). leg underivable → counted decline, advisory, visible.

## 4. sqlstring "text-only" — dual-derivation

`assertSameSQL(golden, toSQLString({|query}, mapping, dialect))`: the
pure query is arg 0 of the producer. Referee executes it (5c) and
replays the golden (5d). Irreducible residue, named:
- FOREIGN-DIALECT renders (Sybase/Snowflake/…): no oracle database —
  text stays the contract, census-listed; the H2 member of each
  family still row-verifies.
- testNonExecutableSQLString (by design), both-ours compares (both
  sides OUR renders → execute both queries, compare rows to each
  other — self-consistency beats string compare).

## 5. Plan-text — the plan replayer (test-side)

A plan is a PROGRAM, not a statement: Sequence / Allocation (bind
name = evaluate expr) / FreeMarkerConditional (>50 elements → temp
table, else inline literals) / CreateAndPopulateTempTable /
Relational(sql-with-${holes}). Tests generate plans for PARAMETERIZED
queries and supply no values — nothing executes as-is.

The replayer (harness-side, a few hundred lines over that closed node
vocabulary): obtain parameter values (harvest from sibling executions
where they exist, referee-chosen otherwise — coverage is a
measure-first item), run allocations, take the conditional **BOTH
WAYS** (small collection AND >50 to force the temp-table branch),
fill the template, run on the oracle, compare rows per branch against
our pipeline with the same values. Branch-forcing is the point:
character-matching can never prove the temp-table path works. The
plan PRINT joins the text census like any emission.

## 6. THE RATIFIED NORMALIZATION INVENTORY (2026-09-01, per-row
## verdicts — the compare policy IS this table; receipts H2Verify.java)

| # | rule | verdict |
|---|---|---|
| 1 | null → `<null>` token | REAL; harden to non-printable sentinel (string-collision edge) |
| 2 | integers exact | REAL |
| 3 | floats @10 significant digits | HACK-ish → REVERIFY: upgrade to ULP-based compare (unify with canon ulp-policy lane); probe the float witnesses under ULP-1 first |
| 4 | timestamps floored to micros | REDUCIBLE — PROVEN 2026-09-01: duckdb_jdbc 1.5 TIMESTAMP_NS round-trips .123456789 (plain TIMESTAMP truncates). A real leg: DDL/seed translation, casts, wire-census types |
| 5 | trailing-zero trim on ts strings | REAL (canonicalization) |
| 6 | temporal carriers → instant | REAL (engine 9-digit parity, receipt in-file) |
| 7 | JSON-carrier ISO→temporal, type-driven only | REAL |
| 8 | per-column enum code→name decode | REAL (underivable = decline) |
| 9 | array/JSON carrier unwrap | REAL (JDBC mechanics) |
| 10 | unconditional row sort | **HACK → FIX**: order-sensitivity decided STATICALLY from our typed query (pipeline ends in sort?) — ordered compares in order, unordered as multisets. Measure blast radius first via the existing LL_ORD_COUNT counter |
| 11 | column-arity mismatch → decline | DECLINE-OK but REDUCIBLE: the extra columns are OURS (driver PKs/order keys) with provenance — project them away instead of declining |
| 12 | golden-side all-NULL single-col row drop (value frames) | REAL, USER-RATIFIED as the observable-boundary policy (engine's own asserts are the receipts) |
| 13 | duplication skew → decline | **ADJUDICATED + BURNED 2026-09-01** (§6.1): no lowering bug — decline arm DELETED; instance frames verdict via the EXTENT_SUBSET golden-side pk-collapse, value/tabular duplication differences diverge loudly |
| 14 | graph compare by label, sorted-key tupling | REAL |
| 15 | bookkeeping-column exclusion (pk_N, u_type, from_z/thru_z/in_z/out_z, k_businessDate/k_processingDate) | REAL but pattern-based: engine's own generated spellings; keep list PINNED shrink-only |
| 16 | graph nesting/key-skew → decline | REAL |
| 17 | assertEqualsH2Compatible dual goldens | REAL (engine's own H2-version variance) |
| 18 | sqlRemoveFormatting folds | REAL (engine's own helper) |
| 19 | (breadcrumb strip) | GONE — reverted with the metamodel ruling |
| 20 | session settings + extension fns + seed ledger | REAL — this IS the oracle |

### 6.1 The dedup adjudication (slice 0 — CLOSED 2026-09-01)

**VERDICT: no lowering bug.** Homework receipts (full sweep census +
seed hand-count + SQL dump + pure spec):
- Census: EXACTLY ONE skew decline corpus-wide
  (testQualifierQueryWithOr); the pass side is skew-free by
  construction (the compare is a sorted-list MULTISET compare —
  duplicates count — so every row-verified pass already matched
  duplication exactly).
- The premise "our filter lowering dedups" was IMPRECISE: our SQL for
  the witness contains ZERO DISTINCT (LEGEND_LITE_DUMP_SQL receipt).
  The 1-vs-7 difference is join SHAPE — we emit the engine's OWN
  forced BuildCorrelatedSubQuery pred-in-leg subselect form
  (testForcedStructure.pure:60 golden, byte-similar); the engine
  default leaves fan legs UNFILTERED with preds in WHERE, so the
  disjunction lets the other leg roam: 4+4-1 = 7x (1,'Firm X'),
  hand-counted from relationalSetUp.pure:1264-1270 seeds.
- Real pure: filter is SUBSET semantics (filter.pure PCT receipts) and
  Class.all() yields each instance once → 1x Firm X = OUR answer. The
  engine's own asserts pin nothing (assertSize(values->at(0),1) sizes
  one element), and the engine disagrees WITH ITSELF across isolation
  strategies (default 1 row vs forced 4 rows on the sibling tests).
- Dedup-site census (9 sites): all user-commanded semantics
  (distinct/removeDuplicates/uniqueValueOnly), the row-preserving
  ExistsJoinForm DISTINCT-keys EXISTS rewrite, pivot header
  discovery, or compile-time name dedup. filter() lowers to
  WHERE/HAVING/QUALIFY only.

**THE FIX (landed with this record):** the skew decline arm is
DELETED. Instance frames (graph compare) resolve duplication skew via
a GOLDEN-SIDE-ONLY collapse of full-row duplicates (pk identity
included), gated on the STATIC extent-subset fact of our typed query
chain (getAll root through subset-preserving ops — the §7 doctrine
applied to multiplicity; EngineTestExecutor.extentSubset →
H2Verify.EXTENT_SUBSET). Direction-safe: OUR side never collapses, so
an over-duplicating pipeline still diverges. Value/tabular frames get
NO tolerance — pure preserves duplicates there, a count difference is
a REAL divergence. Verdict roster: golden-fanout-collapsed. Pins
moved with the burn: exec-passing 1527→1528, unable-to-exec 21→20,
text-rescued ceiling 900→901 (the witness's pass trades sqldiff 13→12
+ advisory 15→14 softness for the row-verified rescue flag).

## 7. Order-sensitivity rule (ratified; LANDED 2026-09-01)

Compare IN ORDER exactly when the compared query is ordered — decided
statically from OUR TYPED QUERY (the pipeline ends in
sort/sortBy/asOfJoin-ordered shape), never by grepping SQL text.
Unordered queries compare as multisets. Migration: run LL_ORD_COUNT
over a full sweep first; every pass that depended on order-leniency
under an ordered query is a pre-existing defect to burn before the
flip, pinned like any census.

**LANDING RECORD (slice 2, 2026-09-01).** Blast radius measured:
of ~404 order-leniency events per sweep, 104 were h2-oracle; the
static classification (walk endsInSort threaded as
H2Verify.ORDERED_QUERY, the EXTENT_SUBSET twin) found exactly **2
under ordered queries — both the SAME defect: ASC null placement.**
The engine's relational sort rides its H2 backend's NULLS-LOW default
(receipt: testPropertyProjectionQueryWithInnerJoinEmbeddedMappingTable
asserts ['null','1 the street','5 Park Ave'] ascending) while our
platform had pinned DuckDB's NULLS-LAST and made H2 conform to IT
(5d0e1ccf — a self-consistency pin between our two executions, never
an engine-semantics adjudication). BURNED by emission — the THREE-LAYER null-placement design:
(1) PURE-lane sorts (TypedSort.pureNullOrder=true — the colspec
relation API + value-collection sorts, the PCT surface) stamp
null-largest EXPLICITLY in lowering, now BOTH directions
(Fold.sortNulls ASC→NULLS LAST joined the existing DESC→NULLS FIRST;
the ASC leg had ridden DuckDB's un-clause default, which coincided —
PCT 1115 + ChannelB Relation 355 are the referees); (2) ENGINE-lane
sorts (legacy TDS string-key shapes, pureNullOrder=false) stay bare
in the IR and the EXECUTION dialects render bare keys with the
engine's observed placement (ASC NULLS FIRST / DESC NULLS LAST —
AnsiSqlRenderer.sortKey; the H2 conform-to-DuckDB override died);
(3) the engine-TEXT channel spells NO NULLS clause ever (goldens
never do). Executed SQL always carries explicit placement; no
backend default ever decides an answer. Corpus +1:
query::sort::testSortByLambdaAndGraphFetchDeep FAIL→PASS (the defect
class, clean pass).

**TIES (the flip's one design completion).** sortBy(a)->sortBy(b)
emits `order by b` ALONE (engine last-sort-wins;
testSortByLambdaMultiple's golden) and two Johns tie on firstName —
both backends order the tie arbitrarily, both correct. The in-order
verdict therefore runs with TIE GROUPS: sort-key columns derive
STATICALLY from the typed chain (EngineTestExecutor.sortKeyCols —
sort('col')/ColSpecs, sortBy(p|$p.prop), #/Path/prop#; computed
expressions underivable); key SEQUENCES compare positionally, full
rows as multisets WITHIN each equal-key run. Ordered rows whose keys
are underivable or not in the compared output (sort-then-rename,
non-projected keys) KEEP the multiset verdict, counted under
LL_ORD_COUNT as `ordered-keys-unmappable` (measured: 7/sweep — the
named residual burn); verification is never traded for a decline.
Unordered leniency measured 105/sweep — incidental backend order,
legitimate forever.

## 8. Slices (each: sweep between mechanisms, ratchet WITH the burn,
## one gate chain per batch, push green; regressions → 2-bisection
## stop rule)

0. **Dedup adjudication** (§6.1) — CLOSED 2026-09-01, no bug; skew
   arm burned, pk-collapse landed (pins moved with it).
1. Oracle service extraction — LANDED 2026-09-01, census
   byte-identical: harness/ReplayOracle owns mirror lifecycle +
   ledger application + the verify entry points (the 4x duplicated
   mirror-or-fresh session acquisition folded into ONE onOracle with
   failure spellings preserved as named Session policies — decline
   reasons are census keys); H2Verify keeps comparison policy only.
   Platform SPI exec/SqlReplayOracle (rows for a SQL text; the plan
   entry joins with slice 4, derived from the replayer's real
   consumption) rides ExecEnv beside AssertListener; the flip path
   and FlipProbe register ReplayOracle.INSTANCE; production carries
   null. Conscious registrations: JDBC census (both files), exec
   funnel register, StatementExecutor eval-ledger 2566→2571.
2. Order-sensitivity fix (§7) — LANDED 2026-09-01, see the §7
   landing record (nulls-low emission burn + tie-grouped in-order
   verdict; corpus 2342→2343, lane pins byte-stable).
3. Verdict arms for the sqlstring family incl. dual-derivation (§4);
   text census wired; flip the lane's simple shapes.
   **3a LANDED 2026-09-01 (the tosqlstring-simple cohort).** Platform
   arm SqlTextVerdicts (com.legend, ledgered + registered in all
   three guardrails; the top-level package became a CLOSED register
   the same day — user directive — so a new file here now fails the
   gate at birth until registered): detection = typed-node + exact
   FQN with LET-AWARE chasing (the platform keeps lets as lets; §3.2's
   substitution is walk-side only — measured the hard way: 44 tests
   flipped and the arm missed all of them until the chase landed).
   Four artifacts per §3.5; OUR ROWS = the producer's lambda wrapped
   in from(mapping) through evalValue (the one router); GOLDEN ROWS +
   §6/§7 compare via the SPI RowVerdict entry (harness ReplayOracle
   implements; declines COUNT through the one funnel, probe-isolated).
   Flip: 44 tests (ratchet 2040/533 → 1996/577), corpus +5
   (2343→2348: transform/fromPure +4, tds +1 — old text-strict
   failures whose rows agree). Emission census live: 18 text-matched /
   7 text-diverged / 19 text-verdict (15 DB2 + 2 Composite foreign
   dialect = the permanent §4 residue; 1 split-part replay gap; 1
   catalog$schema store shape). Dual-channel: walk-text vs arm-rows is
   DESIGNED divergence — probe re-buckets arm-fired asserts to their
   own counted census (`sqltext-arm :: host=X rows=Y`), disagree
   stays EXACT ZERO.
   **3a's SURFACED DEFECTS — BOTH RESOLVED same day:**
   - firstDayOf* DATE-CARRIER gap: BURNED, DIALECT-OWNED (user
     ruling: the lowering states pure's semantics only —
     firstDayOf*(Date):Date; whether a cast is needed to honor it is
     each backend's idiom). AnsiSqlRenderer's DATE_TRUNC arm casts
     day-grained truncations to DATE (this backend's date_trunc
     returns TIMESTAMP); DATE_TRUNC left the Spellings data map
     (CODED register) so the arm owns it; EngineStyleH2 keeps its own
     verbatim double-cast spelling — NO channel conditional anywhere
     (the first cut's EngineTextBoundary gate inside a lowering rule
     was the placement smell, caught by the user). The three affected
     arm rows flipped to rows=pass.
   - firstDayOfWeek "week-start bug": ADJUDICATED NOT OURS — pure's
     own definition is mostRecentDayOfWeek(Monday) (dateExtension
     .pure:204) and the engine's own DuckDB extension emits the
     explicit Monday formula; only engine-on-H2's date_trunc('week')
     yields Sunday. Our Monday answer matches the language spec; the
     golden's Sunday rows are an H2 artifact (the fan-out's sibling).
     Stays the ONE counted `sqltext-arm rows=fail` census row, this
     record as its receipt.
   NEXT: 3b assert-form cohort (~750), 3c exec-sql-read (~700).
   **3b LANDED 2026-09-01 (the assertsamesql-simple cohort — the
   big migration, +308 tests in one flip).** Root arm tryArmSameSql:
   `assertSameSQL` reaches the verdict layer PRE-inline as the
   statement root; OUR TEXT/ROWS minted by VerdictQueries (the
   Result-overload sqlRemoveFormatting selects on param type;
   valuesRead splices the frame chain via ResultEnvelopeSplice —
   invariant-7 clean, no TypedFrom born in the verdict layer).
   Admission: `assertsamesql-simple` (2-arg, foldString golden,
   Variable-or-containsExecute actual) joined allSimple; message-arg
   variants fold tolerantly (first foldable = golden). Movement, all
   attributed in-pin: ratchet 1996/577 → EXACT 1688/885; exec-passing
   lane 1528 → EXACT 1208; clean passes 1401 → 1627; text-rescued
   901 → 681; corpus byte-stable 2348. Emission census: 111 matched /
   237 diverged / 28 text-verdict. Remaining assert-form (~440) is
   DESIGN-gated, not compile-gated: H2Compatible dual-golden,
   assertSqlEquals TDG, computed goldens, mixed bodies — next chunks.
   **THE WOBBLE BURN (user directive: fix, don't envelope).** The
   run-to-run ±1 on the canon census was row-ORDER drift, not float
   arithmetic: unordered chains (no ORDER BY) have undefined arrival
   order, and the byte channel compared grid text order-strictly.
   Receipts: 72-statement SQL dump byte-identical across JVMs for
   testProjectThroughAsso (compiler deterministic; the order moves in
   the DB, legitimately). Fix: probeGridText now takes the SAME
   compile-time sortedness fact the verdict already uses — unordered
   grids judge as two-sided row multisets (sorted chains in wrong
   order stay REAL disagrees); CSVJOIN one-line grids normalize rows
   to lines first (that spelling had been hiding order drift as
   cell-diff@line0 — 3 stable rows were misclassified all along).
   Attribution needed a reserved DISAGREE_SAMPLES witness buffer: the
   wobbling rows sat past the shared 200-cap behind decline rows
   (the SQL_DISAGREE_SAMPLES lesson re-learned). Result: paired
   sweeps BYTE-IDENTICAL — canon disagree EXACT 23 (the 21-row
   cross-engine float class + 2 assertEquals), rescue census printed
   (row-order-canon=17, arrival-order-dependent by definition so
   diagnostic-only), ratchet/lane pins EXACT (the earlier "admission
   wobble" never reproduced on a frozen tree across 6 identical
   rosters — it straddled mid-cascade harness edits). Every envelope
   pin reverted to exact.
   **3c LANDED 2026-09-01 (the execsqlread-simple cohort — 541 tests,
   the single biggest flip of the program).** Arm tryArmExecRead: the
   test's OWN sql($res)/sqlRemoveFormatting($res) read (let-aware,
   exact splice FQNs, FIRST-STATEMENT forms only — sql($res, n>0)
   names the n-th activity and pairing it against result rows would
   judge the wrong statement, so those stay counted) chases to the
   frame; OUR TEXT = the actual side evaluated as written; OUR ROWS =
   valuesRead(frame); the SHARED rows-leg tail judges. Admission
   MIRRORS arm preconditions (2-arg assertEquals, foldable golden,
   1-arg read over an executed frame). Movement, all attributed
   in-pin: ratchet 1688/885 -> EXACT 1147/1426 (text-policy 1067 ->
   386); exec-passing lane 597; M1 floors 373->134 verified,
   777->405 rescued — CONSERVATION receipts: walk-lane row-verifies
   1150->539 (-611), arm row-verifies 348->957 (+609), the 5
   platform-fails demoted with counted reasons; emission 346 matched
   / 611 diverged / 37 text-verdict (16 DB2 + 2 Composite foreign
   dialect permanent, 12 enum-decoded post-transform, 2
   forced-isolation, singletons with receipts). Quality gates held in
   the same sweep: sql-verdict disagree=0, M1 diverged=0, corpus
   byte-stable 2348.
   **ADJUDICATION (the 9 "value divergences" that were not):** 3c's
   flips surfaced 9 canon byte-channel value-list rows (e<11.0>
   a<25.0>, e<Anthony> a<Peter>) that read as VALUE bugs until the
   diagnosis payload landed (byteEqual now appends firstCanonDiff;
   the canon key's NUL separator had been silently breaking grep and
   truncating every console print — escaped in the roster). All 9
   were positional drift on OrderView.INCIDENTAL chains the host had
   lawfully disregarded; probeEqual gained the SAME compile-time
   gate as probeGridText (two-sided sorted compare, gated on the
   caller's static order view) — and the payloads exonerated the 2
   OLD assertEquals pinned rows as the same class. Canon disagree
   EXACT 21: pure calendarAggregations float-print, every row named.
   Paired sweeps byte-identical (rosters and counts; only the
   diagnostic rescue counter varies, by definition).
   NEXT: the text-policy remainder (386) = 3b dual-golden
   (assertEqualsH2Compatible) + computed goldens + mixed bodies + 3c
   declined forms (multi-statement reads, non-literal goldens); then
   the compiler-wall buckets (~600: HN vocabulary, scalar-lowering
   overloads, unported natives) as named feature legs.
   **3d LANDED 2026-09-01 (the h2compat-simple cohort — the
   dual-golden family, +199 flips, ratchet EXACT 948/1625).** Arm
   tryArmH2Compat: the engine's own body (h2Extension.pure:29) picks
   ONE golden by H2 version — legacy on 1.4.200, upgraded otherwise;
   our oracle IS the upgraded H2 (W10 4.138.2 pin), so the arm
   replays the UPGRADED golden, the same choice the engine's own
   dispatch makes on this oracle. Both actual spellings owned: bare
   Result (minted strip) and the test's own
   sqlRemoveFormatting($result) String (the findSqlRead chase) — the
   first cut missed the String spelling and 200 admitted tests walled
   on "store resolution left user call uninlined" (the platform
   cannot inline getH2Versions' store read; counted, then owned).
   Lane moves attributed in-pin: exec-passing 597 -> 389, M1 floors
   134 -> 85 / 405 -> 246. Canon disagree UNCHANGED at EXACT 21 —
   zero new divergences from the whole cohort; sql-verdict disagree=0
   held; paired sweeps byte-identical. text-policy 386 -> 118.
   Residual walls surfaced by flip attempts, counted where they
   belong: 43 join-condition-whole-variable + 6 HN-vocabulary
   (compiler gaps, feature-leg lanes), 1 store-resolution residue.
   **3e LANDED 2026-09-01 (conformance hardening, zero census
   movement).** The root arms now WALL COUNTED on
   recognized-but-underivable SQL shapes (a Result-typed actual on a
   sql-assert form whose leg cannot derive) instead of falling
   through to the generic path's TEXT verdict — the charter's one
   forbidden outcome is now mechanically unreachable. Admission
   dropped the foldable-golden requirement (the arms EVALUATE
   goldens; computed goldens are ordinary string code). Net flips:
   ZERO — the remaining 118 text-policy adjudicated by ROSTER as:
   ~35 executionPlan PLAN-TEXT tests (the §5 plan-replayer cohort,
   slice 4 BY DESIGN — no executed frame exists), the TDG
   assertSqlEquals family (TDG-refereed, excluded by design),
   ~24 other-producer + ~20 multi-statement/mixed residue (named,
   counted). The sql-text EXECUTED-FRAME migration is COMPLETE:
   every assert over an executed frame's SQL judges on rows.
   **§5 FIRST CUT LANDED 2026-09-01 (the plan-text arm — the
   single-statement class, pushed 69c7dec3).** tryArmPlanText: the
   actual side navigates a generated plan to a SQL text with template
   holes; REFEREE BINDINGS (VerdictQueries.refereeBindings — fixed
   scalar values per parameter type: String 'A', Integer 22, Float
   1.0, Boolean true, Date 2015-10-16, DateTime 2015-10-16 00:00:00)
   bind the plan lambda's parameters as MINTED LETS (parameters
   resolve exactly like test-body lets — no substitution walk); OUR
   ROWS = the lambda body in from(mapping) with those lets; GOLDEN
   ROWS = the golden with every hole filled with the SAME value's
   spelling, replayed via the oracle (rowsLegAndVerdict gained the
   replaySql channel — the census still compares the RAW golden).
   Movement: +3 flips (ratchet 945/1628), text-only lane 43 -> 40.
   COUNTED residue, the cut's named boundary: whole-plan planToString
   compares with FREEMARKER-OPERATION holes (7 demoted — the full
   program replayer: run allocations, force the >50-element
   temp-table conditional BOTH WAYS) + unbindable params. Canon exact
   21 held; paired sweeps byte-identical.
   REMAINING TO ZERO (the burn map, measured 2026-09-01, updated
   after the effectful cutover — 878 fallbacks): ~110 text-policy
   (~25 plan-program + 36 TDG scoring-flip + ~50 named residue) +
   ~420 compiler walls (~200 metamodel-query classes -> the
   metamodel-as-relations leg; 51 scalar lowerings + ~60 unported
   natives; 43 join-condition-whole-variable; 21 lineage; misc) +
   ~15 platform-fail singletons. The effectful bucket is GONE
   (transactional cutover, record below); the 31 generateTestData
   unclassified-Variable rows BURNED TO ZERO (TDG let-adoption
   2026-09-01, ratchet 876/1697: resolveStructuralArgs adopts the TDG
   data-constructor shapes DEEP — SourceSubst.tdgCtorShape — since
   inner args can be let-bound too; +2 flips, remainder re-bucketed
   TRUE: 15 scalar-lowering [the known lane, now 66], 10 plan-execute
   values-binding [the chartered referee-binding cut — now a named
   10-test leg], 3 REAL platform divergences [testConstant,
   testSimpleSingleTableWithNoDataToInsert + 1 — verdict-level
   assert failures, the genuine burn list], 1 store-resolution).
   Endgame: fallbacks 0-or-named = the walk's scoring machinery
   deletes.
   **THE EFFECTFUL CUTOVER — LANDED 2026-09-01 (ratchet 878/1695,
   the gate deleted).** The static verb classification (effectKind's
   verb scan + collectExecInDb, ~90 harness lines — a statement-shape
   recognizer, the class this program deletes) is REPLACED by the
   database's own atomicity: every effect-bearing body executes
   inside a TRANSACTION on the session connection — commit only
   after the verdict stream passes; on any failure exit rollback +
   ledger truncate-to-mark (RawSqlBoundary.LedgerMark — unrecordLast,
   range edition) + mirror-detach-if-ahead (a mid-body verify can
   advance the mirror cursor past the rolled-back mark; H2 cannot
   roll back, so the family's remaining verifies ride fresh-replay —
   failure-path only). Census (82 -> 0): +50 flips (42 modelJoin +
   8 TDG/misc); 28 generateTestData unclassified-Variable + named
   singletons re-bucketed TRUE; 4 wall-type rows re-spelled
   wall-exec (the gate's eager typeQueryBody is gone — same
   failures, surfaced at execution). CONSERVATION (audited after a
   first WRONG receipt — the emission census is the TEXT channel and
   `sql-verdict` in the canon line is the dual-verdict alarm, neither
   proves row verification): the 44 migrated sql-asserts row-verify
   via the arm rows leg calling the SAME machinery
   (ReplayOracle.verify -> H2Verify.compareFrame) through the SPI;
   receipt = the text-verdict decline census byte-identical (102 =
   102) and the 3e rule that a declined rows leg cannot flip; each
   test's data is also platform-judged by its own value asserts.
   Corpus total 2349 STABLE, clean passes 2101 -> 2143 (the 42
   text-rescued flags cleared — the chartered §0 accounting, as in
   3b-3d). Rescue ceiling ratcheted 901 -> 165 (measured); M1 floors
   85/246 -> 83/204; exec-passing 389 -> 345 (emission census +44).
   Group F burn (2026-09-02, harness burn-down batch 7): exec-passing
   345 -> 344 and M1 verified 83 -> 82 — testSubTypeMappingValidWhen
   MappedExplicitly flipped (its _classMappingByClass is a Pure body over
   the metamodel store); its assertSameSQL row-verifies via the platform
   arm — a lane move, not lost verification. Ratchet 841/1732 -> 820/1753.
   TXN-VISIBILITY note: tdgSqlReplay's our-side read runs on the
   SESSION connection when it carries an open transaction (a
   duplicate is a separate snapshot — the attempt's writes are
   invisible there); outside a transaction the duplicate stays (an
   ambient read would break the walk's open streaming result). Two
   correctness constraints, keyed on the connection's own state; the
   duplicate branch dies with the walk. Instruments: rollbacks /
   mirror-detaches / rollback-failures in the flip census header
   (measuring sweep: detaches 0, rollback-failures 0; rollbacks
   fired and state-restored — the 28 mid-body TDG walls left corpus
   totals byte-stable).
   PLACEMENT (user probe "right place if it survives?"): the
   protocol is ONE owned invariant — ReplayOracle.beginAttempt/
   commitAttempt/rollbackAttempt (txn + ledger mark + mirror repair;
   the service that already owns family-session world state);
   WholeTestFlip only DRIVES it. Txn policy stays CALLER-side by
   design — the platform executes on the connection state it is
   handed, never owns transaction semantics. At cutover the call
   site moves to the runner; the protocol survives in place as
   family-session failure hygiene (a failed body must not leak
   partial writes into later tests' shared session).
   **THE "ANY-PROPERTY CHECKER" LEG — DISSOLVED BY CENSUS 2026-09-01
   (ratchet 928/1645).** Measure-first killed the design: all 46
   walls were ONE shape ($cv.first in createTableRowIdentifiers,
   receiver typed Any), and the Any came from OUR OWN CATALOG — three
   widened native spellings shadowing the corpus's real declarations
   (classDef is native-first): RowIdentifier.columnValuePairs Any[*]
   -> Pair<String,Any>[*] (testDataGeneration.pure:46), Table.schema
   Any[0..1] -> Schema[1] (relational.pure:94; the layout-cycle fear
   behind the widening is obsolete — LayoutTypes walk-guard rides a
   revisited class as JSON), Schema.name String[0..1] -> String[1]
   (relational.pure:38). The engine never "type-checks dynamically"
   here; its declarations are precise, ours were weak — the
   [[verify-signatures-against-real-legend-pure]] doctrine, class
   edition. NO dynamic Any-property checker exists or is wanted
   (that would have been conform-by-weakening). Movement, attributed
   in-pin: 15 alloy-TDG flips; corpus 2348 -> 2349
   (testAlloyTestDatGenForNestedViews, assert-free body, counted
   under the 0-asserts ceiling 29 -> 30); the other 31 re-bucketed
   to TRUE walls — 25 effectful, 3 generateTestData
   unclassified-Variable (milestoning dates through a let), 3
   planTestDataGeneration Pair-arg typing. Lane pins byte-stable
   (exec-passing 389, text-only 40, unable 20, M1 85/246); canon
   disagree EXACT 21 held; sql-verdict disagree 0 held; text-policy
   65 unchanged (these were wall-type, never text policy). The 9
   debugPrint host-channel ERRORs kept their outcome; their census
   message gained a [root=...] payload (histogram re-aggregated).
   **TDG SCORING FLIP, FIRST SLICE — LANDED 2026-09-01.** The
   fetch-text verdict joined the SPI (verifyFetchTexts: ours executes
   on the calling session's DuckDB, the golden replays on the mirror,
   multiset compare — the walk's tdgSqlReplay semantics behind the
   oracle interface; ordered/chained texts DECLINE with their walk
   names). Arms: assertSqlEquals ROOT + the plain-assertEquals
   spelling + the H2Compatible/assertSameSQL TDG routes (a text
   compare of generated SQL can never judge as text). CLASSIFIER FIX
   with receipts: the census's actual-side pick took the LAST
   non-foldable parameter, so assertSize($tdg.sqls, 3) read the
   literal 3 as its actual and whole TDG bodies misclassified
   other-producer — the sql-tainted parameter is the actual now.
   Movement: ratchet 943/1630; text-policy 118 -> 65 — the TDG
   cohort's remainder re-bucketed to its TRUE walls, named:
   execute/3-spelling reached through corpus helper bodies
   (setUpDataSQLs chain) and Any-property reads
   (createTableRowIdentifiers' .first) — compiler-gap legs, not
   policy. Canon exact 21 held; paired sweeps byte-identical.
   **PLAN-EXECUTE NATIVE LANDED 2026-09-01 (8c531339, two walls
   peeled).** meta::pure::executionPlan::execute(plan, values, ext)
   joined the catalog (REAL signature executionPlan_execution.pure:20;
   golden-line diff in native-catalog.txt) and NORMALIZES in
   StatementExecutor.buildFrame: the plan argument peels let-aware to
   its executionPlan(...) build — same positional shape as
   router::execute — so the ordinary frame machinery serves it whole;
   plan TEXT is engine-text (EngineStyleH2) and NEVER executes on the
   session connection (single-compiler tenet). Non-traceable plans and
   non-empty parametersValues wall counted (values-binding = the
   referee-binding cut, future). TWO REAL PLATFORM BUGS flushed by the
   attempt: Env.resolveAlias had no cycle guard (a helper parameter
   named like its caller's let — loadAndTestExecution's runtime <-
   $runtime — self-aliased and hung the sweep; jstack receipts,
   guarded) and the execute/3 wall (30 tests) peeled to its NEXT named
   walls: Any-property dynamic reads (+28, now 46 total — the next
   checker leg) and effectful (+4). Zero net flips; honest movement.
   **FOUNDATION PROBE LANDED 2026-09-01 (ratchet 876/1697 ->
   871/1702, corpus 2349 -> 2350; the full record is
   docs/PLATFORM_FAIL_ADJUDICATION_2026_09_01.md).** Every platform-fail
   block of one debug sweep read against ENGINE SOURCE. Verdict: no
   silent filter/null-out corruption anywhere. The null-vs-value trio
   (testMixedMappingWithFilterInProject cohort) is the ENGINE's own
   column-resolution defect pinned by its golden (its corpus carries a
   `test.ToFix` sibling admitting "AGE column fails to resolve against
   inner relation"); ours is the Pure answer — ADJUDICATED NOT OURS,
   stays counted (the firstDayOfWeek class). Dup-rows = a join-chain
   TERMINAL-binding divergence with a corpus-wide blast radius of ONE
   mapping (165 chain refs censused) + the fan-out instance-carrier
   class; empty-vs-null = our TDSNull encoding differing between the
   instance-literal and row carriers; union padding = the engine's csv
   carrier conflating '' with null. TWO BURNS: (1) the MAPPING-SEAM
   WINDOW RULE — Fold.containsWindow's doc named the resolver's
   "windowed ~func" isolation but nothing built it; ClassSources stamps
   `extentBoundary` on every window in a class extent's pipeline and
   the lowerer isolates it (plain relation composition keeps the
   engine's WHERE-under-window fold, PCT testExtendFilterOutNull) —
   testMappingWithWindowColumn flips. (2) ONE TEST CLOCK — the engine
   pins -Duser.timezone=GMT for every test JVM; our H2 replay oracle
   had been reading now() in the LOCAL zone against DuckDB's UTC, so
   the five dateDiff(..., now()) goldens diverged by the zone offset
   and the DAYS one only after 21:00 local — the ±1 evening wobble
   that read HEAD at 877/1696 against its own pin. Root pom pins GMT.
   Under GMT they still RACED at unit boundaries (two instants: the
   oracle replays later than we executed), so H2Verify.compareFrame
   declines BY NAME "datediff-to-now golden" (a datediff spelling the
   instant, in the SELECT list — predicate uses and instant-invariant
   projections keep their row verdict, both over-broad cuts measured
   and rejected); §3.7 text contract + byte-identical spelling = +5
   DETERMINISTIC flips, ROW-verdict diverged bucket 10 -> 5, emission
   census 396/812/102 -> 392/812/110. Named residue added
   to the burn map: join-chain terminal binding (1), instance-carrier
   fan-out cardinality, TDSNull cross-carrier encoding, connection
   timeZone at execution (2), inline MapperPostProcessor (1), identity
   leak on multi-pk map (1).
   **LEGACY TDS JOIN, LET-BOUND JoinType — LANDED 2026-09-01 (ratchet
   871/1702 -> 848/1725).** The census had filed 25 tdsJoin tests
   under "unported native getInteger"; the getters existed — the
   tests parameterize the kind (`let type = JoinType.LEFT_OUTER; ...
   ->join(tds, $type, {a,b|$a.getInteger(..) == $b.getInteger(..)})`)
   and JoinChecker's legacy desugars matched a literal EnumValue only.
   JoinChecker.resolveLetBoundArgs chases the kind through
   Env.resolveAlias (the ONE alias channel); +23 flips, oracle roster
   byte-identical, unit test in RelationApiIntegrationTest. Residue 2
   (testJoinLambdaAsVariable, testJoinWithLiteralColumn): a let-bound
   `{a:TDSRow[1], b:TDSRow[1]|...}` condition walls at its OWN let —
   a nominal TDSRow has no columns to read; it only types against the
   consuming join's rows — a deferred-kind candidate for the bind-once
   charter (family A), NOT an alias chase.
   **TDG ARM REACH — LANDED 2026-09-01 (zero net flips, honest
   movement; ratchet EXACT 848/1725).** The handoff's STEP 4 ("register
   the TDG carrier in execFrames — frame-path work") was a
   misdiagnosis: the generator carrier IS executed and FOLDED to a
   TestDataGenResult instance literal per statement
   (TestDataGenerationNatives.foldCensus, S2); the verdict layer's
   hasTdgProducer recognized only the unfolded carrier, and the
   corpus's assertSqlEquals inlines to assertEquals over
   sqlRemoveFormatting on both sides, so findSqlRead claimed the
   actual side as an exec-read (a Result frame it never was) and the
   TDG route was unreachable. SqlTextVerdicts now recognizes the
   folded literal and routes TDG FIRST in tryArmExecRead. Movement:
   "no scalar lowering (assertEquals)" 66 -> 51; the 3 dialect
   TOP/LIMIT platform-fails (testConstant cohort) dissolve; 12 rows
   decline BY NAME "chained fetch — generator temp tables not
   replayable" (the walk's own compile-time decline surfaced through
   the SPI; §3.7 text contract, text differs by dialect → counted
   fail); 7 rows join plan-execute values-binding (10 -> 17).
   mirror-detaches 0 -> 1 deterministic (arm replays on the mirror,
   attempt fails on text, rollback detaches — failure hygiene by
   design). NEXT for this cohort: replay the generator's temp tables
   on the oracle (the chained-fetch decline) — a real oracle leg.
4. Plan replayer (§5) + plan-text flips; branch-forcing.
5. Inventory upgrades as their own commits: #3 ULP probe/upgrade,
   #4 TIMESTAMP_NS leg, #11 projection, #1 sentinel.
6. Emission byte-parity: the text-diff census becomes the shrink-only
   ratchet; dialect spelling work retires rows class-by-class —
   NEVER a verdict, never a blocker.

## 9. Standing constraints

No metamodel-fact consumption ([[metamodel-in-database-ruling]]);
declines counted never silent; no statement-shape recognizers on the
platform side (detection is typed-node + exact FQN only); the walk's
sql-text recognizer dies only as flips make it unreachable; foreign-
dialect/by-design-unrunnable residue stays a NAMED census forever.

## 10. Open items ratified as measure-first (no guessing)

toSQLString overload coverage per test; runtime pairing for render
tests whose module runtime may not bind the producer's mapping;
parameter-value harvesting coverage for plan replay; LL_ORD_COUNT
blast radius; ULP-1 float survivor set.

---

## APPENDIX A — the worked example (the ratified walkthrough, verbatim)

```
let query = {|Person.all()->filter(p | $p.firm.legalName == 'X')};
let sql   = toSQLString($query, simpleRelationalMapping, DatabaseType.H2, ext);
assertEquals('select "root".NAME as "name" from personTable as "root" ...', $sql);
```
After whole-body compile + let substitution, the assert's typed
argument tree literally contains
`assertEquals('select…', toSQLString({|Person.all()->filter(…)},
simpleRelationalMapping, H2, ext))` — the producer node's children
ARE the query lambda / mapping ref / dialect. The verdict arm reads
them off the node; OUR ROWS come from executing that lambda through
the ordinary pipeline with the mapping from the producer and the
runtime from ExecEnv (runtime pairing = §10 measure-first item).

## APPENDIX B — the plan-text example (real corpus shape)

```
Sequence(
  FunctionParametersValidationNode(y: String[1])
  Allocation( name=z, value = $y->split(',') )
  FreeMarkerConditional(
    if collectionSize(z) > 50:
       CreateAndPopulateTempTable(tempTableForIn_z) … select from temp table …
    else: inline the literals )
  Relational( sql = select "root".LEGALNAME … where LEGALNAME in (${inFilterClause_z}) ))
```
`${…}` holes fill at runtime from parameter values the test never
supplies. Replayer building blocks that already exist:
PlanSupportFunctions.relationalPlanSupportFunctions (freemarker
support fns + enum-map template functions, used by plan emission) and
the TDG temp-table machinery.

## APPENDIX C — homework receipts (all verified 2026-09-01)

**The oracle machinery (test-side today):**
- Seed recording: `sql/dialect/RawSqlBoundary.java` — RECORDER
  thread-local installed per test (Runner.java:934); every corpus
  statement is H2-flavored BY DEFINITION and records verbatim;
  `unrecordLast()` keeps the ledger matching executed reality;
  META_RECORDER is a SEPARATE metadata-only channel (PK constraints,
  schema creates DuckDB skips) consumed ONLY by fetchDb* metadata
  replay — kept out of the row-replay stream on purpose.
- Mirror: ONE live in-memory H2 per family session
  (Runner.java:~1552, `jdbc:h2:mem:famMirror<N>` + H2Settings),
  INCREMENTAL (applies only unmirrored ledger entries — the fresh
  full-history replay per verify was O(n²)); a rejected statement
  POISONS the family (identical decline set to fresh-replay);
  `mirrorSuspend` for private-session tests; engine H2 extension
  functions registered on the mirror too (F6.7 — they were
  fresh-path-only once and the default path declined).
- Session settings: `exec/H2Settings.SETTINGS` =
  `;NON_KEYWORDS=ANY,…,OVER;MODE=LEGACY` — engine's 2.1.214 server
  verbatim; the old DATABASE_TO_UPPER leniency is dead.
- Compare: `H2Verify.goldenRowsCompare` (:757-841; unconditional
  sort at :826-827 — the §7 fix target), `goldenGraphCompare`
  (:566-750), `norm` (:1314-1394), `coerceTemporal` (:405),
  `carrierList` (:115), `bookkeepingAlias` (:552),
  `divergenceOrSkew` (:843). TDG replay variants exist too:
  `tdgSqlReplay` (:1023) / `tdgChainedReplay` (:1165).
- Instruments: per-test verdict roster → target/h2-verdicts.txt
  every sweep; UNVERIFIABLE_CENSUS per-reason (declared-gap registry
  asserts growth); GOLDEN_NANOS/MIRROR_NANOS wall-clock.

**Lane classification:** `EngineTestExecutor.v7DualChannel`
(:3440-3505) — SQL_TEXT_OUTCOME thread-local set by the verify exits;
buckets: exec-passing / text-only (…" nothing executed anywhere") /
unable-to-exec::<sub>. "Our side ran" detection = execVars refs +
containsExecute + TDG `.sqls` reads (S3 user catch: a TDG sqls read
means our side EXECUTED). Text-only composition measured: plan-literal
17 + plan-let 6 + match-noreplay 3 + no-generator-noreplay 9 +
no-root-exec-variable 7 + bare 2.

**NanoProbe (2026-09-01, duckdb_jdbc 1.5.0.0):** `TIMESTAMP_NS`
column: getObject → `2015-08-26 15:22:23.123456789`,
getTimestamp().getNanos() = 123456789; plain `TIMESTAMP` truncates to
`.123456`. Receipt for inventory row 4.

**Dedup skew witnesses seen this session (slice 0 starts here, then
censuses properly):** testQualifierQueryWithOr ("row-cardinality skew
(distinct rows agree)"; the in-file example: 7× Firm X where the
engine builds one object per joined row and the test's
assertSize(values->at(0),1) pins nothing);
advanced::forced::structure::testQualifierWithOperation /
testTwoQualifiersWithOperation decline as "forced-isolation golden
over a VALUE frame" (adjacent, engine debug-mechanism pin). Engine
receipt: RelationalResult.java has zero distinct/dedup/pk sites
(verified 2026-08-28).

## APPENDIX D — operational wiring the slices MUST touch

- **The flip-side gate**: WholeTestFlip's per-assert TEXT-TAINT
  prescan (:~140-165) routes any test whose assert reads a
  sql-producer (taint flows let-to-let) to `fallback("text-policy")`
  BEFORE anything runs. Migrating the lane = replacing this gate with
  the verdict arms; flips retire it incrementally.
- **Exact pins that WILL trip on every lane slice** (move them WITH
  the burn + update V7 charter §8.0 in the same commit — the
  lane-guard doctrine): RelationalCorpusRunner exec-passing
  assertEquals(1527) at ~:852, text-only assertEquals(44) at ~:893,
  unable-to-exec assertEquals(21) at ~:935; M1_VERIFIED floor ≥455;
  h2-verdicts roster; the migration ratchet (2040/533 at this
  charter's writing).
- **Both-ours compares** (two of OUR renders vs each other): execute
  both queries, compare rows to each other — self-consistency.
- **Foreign-dialect residue**: the H2 member of each AllDBs family
  still row-verifies; the non-H2 renders are the permanent named
  text census.
