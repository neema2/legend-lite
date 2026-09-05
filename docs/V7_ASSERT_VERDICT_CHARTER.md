# V7 — corpus asserts become SQL verdicts (charter, 2026-08-28)

**Mission.** Retire the corpus harness's private assert-comparison
lattice — the THIRD implementation of pure equality semantics — by
routing corpus assert statements through the production verdict path
(`StatementExecutor` → `AssertVerdicts`), i.e. the PCT lane's
RATIFIED dual-verdict architecture EXACTLY AS IT EXISTS
(`AssertVerdicts:125-140`): each side executes with the canonical
render RIDING THE SIDE QUERY ITSELF (`wrapWithCanon` — the DATABASE
computes the canonical bytes; sameElements arrives ORDER BY canon
text in the same execution); the verdict of record is byte equality
of DB-computed canonical renders (Java's remaining role is a
semantics-free `String.equals`); the host lattice stays as the
PERMANENT parallel referee with the disagreement alarm; boolean
`assert`/`assertFalse` K-arm conditions adjudicate fully in-DB.
What dies is the corpus harness's SEMANTIC compare code
(`goldenEqualScalar`, temporal decode, numeric-by-value) — the
semantics move into the one canon owner's SQL. TRUE single-query
fusion (both sides in one round trip, one verdict row out) is V12,
deliberately NOT this leg. Standing ruling 2026-08-24 (PROGRAM_MAP
longer-arc §3): one leg, no incremental drift, no half-migrated
referee; unblocked by PCT completion 2026-08-28. Phase-0 census:
[V7_ASSERT_VERDICT_CENSUS.md](V7_ASSERT_VERDICT_CENSUS.md).

---

## 1. The facts this charter stands on (all verified 2026-08-28)

- **The seam is ONE dispatch arm.** `EngineTestExecutor` already runs
  non-assert statements through `Compiler.executeResolved` (the
  production path); ONLY statements matching
  `simpleName(...).startsWith("assert")` divert to `checkAssert`
  (`EngineTestExecutor:753-769`) — the host lattice
  (`goldenEqualScalar` + golden temporal-decode arms + the grid/CSV
  conventions). The temporal-decode arms' own comment already says
  "these arms delete wholesale" with a render cutover.
- **The production adjudicator covers 11/12 forms.** `AssertVerdicts`
  (PCT-battle-tested: K-arm verdicts, canon riders, dual-verdict
  alarm) recognizes assertEquals/NotEquals, assertSameElements,
  assertSize, assertEq, assertEqWithinTolerance, assert/assertFalse,
  assertInstanceOf, assertIs, assertEmpty/NotEmpty,
  assertTdsEquivalent — vs the corpus census (~1,880 data sites) this
  is total coverage except **assertJsonStringsEqual (167 sites)**,
  the one NEW verdict form (the graph lane; production `JsonCompare`
  + the byte-canon channel are its design anchors — Channel B's
  graph verdicts are the precedent).
- **Performance is a non-issue, measured**: 24,529 queries / 6.5 s
  full-sweep (0.26 ms each). Per-assert round trips are UNCHANGED by
  this leg (both sides already execute today; the canon rides those
  same executions as appended columns — wire grows by the canon
  column, shrinks by nothing yet). The round-trip HALVING (fused
  sides, one verdict row) is V12's payoff, measured then.
- **The dual-referee plumbing already exists**: the corpus lane
  prints `sql-verdict agree/disagree/declined` counters (all zero,
  unexercised).
- **The softness flags mostly do not migrate**: text-rescued (614),
  sqldiff (258), adv-pass (304) annotate the GOLDEN-SQL/H2-replay
  advisory channel — plan text and cross-engine rows, host/oracle by
  design. Only two obligations cross: 0-assert passes (27) must stay
  visibly zero-assert (a vacuous verdict must not hide them), and
  per-test softness attribution survives the re-route.

## 2. Scope partition (the honest claim)

MIGRATES (~1,880 data-assert sites): the §1 form table.
STAYS HOST BY DESIGN (named, not debt):
- `assertSameSQL` (229) + `assertEquals(...sqlRemoveFormatting())`
  (341) — PLAN-TEXT comparisons.
- The TDG arms (`generateSeedDataString` CSV compares, plan-text
  compares — `EngineTestExecutor:1842-1896`) — host artifacts.
- The golden-SQL advisory / H2-replay oracle channel, wholesale.

## 3. Design decisions

**D1 — one owner, no fourth implementation.** Verdict queries are
constructed by the production `AssertVerdicts` ONLY. The harness's
contribution shrinks to what it already owns: statement sequencing,
the execute-handle SPLICE (assert args referencing `$result` reads
wrap exactly like the non-assert statements it already routes:
`LambdaFunction(execStmts + spliced)`), and outcome accounting. Any
corpus-only comparison rule found during the burn moves INTO
`AssertVerdicts` (or its canon layer) with a witness — never into a
harness arm.

**D2 — the dual phase is a referee, never a mode** (no-adapter-hedges
doctrine). Host verdict stays the verdict OF RECORD while the SQL
verdict runs beside it; the existing `sql-verdict` counters carry the
per-test disagreement census; DECLINED is a named per-form census,
never a silent skip. The cutover deletes `checkAssert`'s comparison
lattice in the same slice that flips the verdict of record.

**D3 — order keys are explicit.** `assertSameElements` verdicts sort
both sides canonically; `assertEquals` over rows carries the
row_number order key (PROGRAM_MAP §3's recorded acceptance). The
grid render conventions (TDSNull sentinel, engine text-compare) move
into verdict-query construction.

**D4 — assertJsonStringsEqual lands INSIDE the leg** (a data assert
cannot stay host-side past cutover without violating the no-half-
migration ruling). Design anchor: the graph lane's byte-canon; if the
form proves un-verdictable for a subshape, that subshape gets a
NAMED, ceiling-pinned residue adjudicated with the user BEFORE
cutover, not after.

## 4. Sequencing (~3 gated batches inside the one leg)

1. **Wire the dual channel** (no behavior change): the assert
   dispatch arm (`EngineTestExecutor:753`, gated by
   `harnessVocabName`) additionally routes each assert through the
   production path; host verdict remains of record; counters
   populate. **The wiring shortcut — do NOT hand-plumb**: the harness
   ALREADY calls `Compiler.executeResolved(...)` for setup statements
   (`:793-809`, with the `LambdaFunction(execStmts + spliced)` wrap
   and `NameResolver.resolveQuery(wrapped, imports,
   ctx.elementFqns())`) — and `executeResolved` →
   `StatementExecutor.executeStatements` ALREADY dispatches
   statement-root assert-family calls to `AssertVerdicts
   .tryAdjudicate`. Batch 1 = the same call for assert statements
   with the outcome captured instead of thrown away; never construct
   `ExecEnv`/call `AssertVerdicts` directly from the harness. The
   counters' owner is `com.legend.exec.CanonicalDivergence`
   (`probeSqlVerdict`/`sqlDisagreeCount`/`sqlDeclinedCount` — the
   [canon]/sql-verdict lines the runner already prints). Instrument:
   per-form agree/disagree/declined + a rows-fetched-per-assert
   histogram (the golden-size fact §5-1 of the census). Scoreboard
   byte-identical BY CONSTRUCTION — full chain green.
2. **Burn the census to zero**: fix verdict-construction gaps
   per-form (order keys, TDSNull, temporal spellings, the JSON form);
   every fix is a production-side change with a witness. DECLINED
   shrinks to the named §2 partition. Scoreboard still untouched.
3. **The cutover** (one slice): SQL verdict becomes the verdict of
   record; `checkAssert`'s comparison lattice + `goldenEqualScalar` +
   the golden temporal-decode arms DELETE (shrink pins move with
   dated justifications); 0-assert accounting and softness
   attribution re-anchored; the dual-verdict alarm stays armed
   permanently (PCT precedent). Acceptance: scoreboard IDENTICAL
   (2,334 + the family table), disagreement 0, declines = the §2
   partition only, full chain green, push.

## 4L. Batch-1 LANDING RECORD (2026-08-28)

**EXECUTED — the dual channel is live; scoreboard byte-identical.**

- **Call-site census**: `checkAssert` has exactly TWO direct call
  sites — the main dispatch arm (`EngineTestExecutor` statement loop)
  and `runPerDriverLoop`. `AssertLoopForm` and `RuntimeIfForm` re-enter
  by pushing statements onto the `work` deque, so their asserts land at
  the main arm — both direct sites carry the dual channel
  (`v7DualChannel`), so coverage is total.
- **The wiring is the §4.1 shortcut verbatim**: `v7DualChannel` calls
  `evalSpliced(subst(spelledAssert, lets), execStmts, …)` — the
  existing setup-statement pattern; `AssertVerdicts` is never touched
  from the harness. Two enabling facts discovered en route:
  1. Real Pure AUTO-IMPORTS `meta::pure::functions::asserts`
     (m3.pure:202's system imports) — that is why corpus tests call
     the family bare. Our implicit tier is the native registry, which
     owns only assert/fail/assertEqWithinTolerance/assertError/
     assertTdsEquivalent. So (a) the corpus global model now loads the
     REAL legend-pure assert sources (12 files, `Corpus.PURE_ASSERTS`)
     as library sources — parsed native twins drop at the global
     compile's library-scoped prune (the ChannelB idiom; registry is
     the definition) — and (b) the splice FQN-spells bare assert names
     the registry does not own (`v7Spell`). Qualified and
     registry-owned spellings pass through untouched.
  2. **Probe isolation**: the duplicate executions must not feed the
     primary lane's pinned compiler censuses —
     `SqlTypeCensus.probeSuspend` brackets the probe (first sweep
     tripped four ceiling pins purely by double-counting).
- `testExtension.pure` (`assertJsonStringsEqual` — same asserts
  package) deliberately does NOT load in batch 1: its unported
  `meta::pure::functions::test` siblings add wall rows to the
  scoreboard doc. It loads with D4 in batch 2.
- **Census (full DuckDB sweep 2026-08-28, the batch-2 work list)**:
  `dual-channel agree=141 disagree=0 declined=5100`. ZERO
  disagreements — every pair both adjudicators judged, they agreed
  (per-form agrees: assertSize 79, assertEquals 44, assertSameElements
  10, assert 4, assertEq 2, assertFalse 1, assertNotEmpty 1). Inner
  referee on those pairs: `sql-verdict agree=41 disagree=0
  declined=15`; [canon] pin 27 held exactly. Declines by class
  (console `[v7]` lines are the authority):
  | class | ~sites |
  |---|---|
  | lowering gap (TypedPropertyAccess/TypedVariable under verdict sides) | 1,644 |
  | exec-envelope reads (`$result.values`/`.activities` — "no row scope") | 1,407 |
  | resolver: class query under wrapper (TypedUserCall/TypedMap/if) | 703 |
  | host partition: sql/plan-text forms (§2, BY DESIGN) | 375 |
  | unknown function (assertJsonStringsEqual et al — D4) | 180 |
  | resolver: getAll shape unresolved | 174 |
  | no scalar lowering for overload (assert/2 under assertContains etc.) | 89 |
  | unbound variable (TDG lets) + host-unsupported + grid sides + tail | ~200 |
- **§5-1 answered**: side-rows histogram `0:24 1:2069 2-3:666 4-7:455
  8-15:87 16-31:21 32-63:3` — 92% of sides are ≤3 elements, max
  bucket 32–63. VALUES-literal cost for V12 is a non-issue.
- Guardrail registers moved with justification: ErrorShape
  EngineTestExecutor 3→4 (the decline tunnel), HarnessDiscipline
  CanonicalDivergence 4→6 (report display sort), JavaEvalLedger
  AssertVerdicts 834→840 (histogram hook), ArchitectureTest statics
  (V7_FORMS/V7_DECLINES/V7_SAMPLES). Witness:
  `V7DualChannelCensusTest`.

**Batch-2 reading of the census**: with disagreement already ZERO, the
burn is the DECLINE table — chiefly the exec-envelope read lane
(`$result.values` splice into the verdict side path) and the
verdict-side lowering/resolver gaps; the host-partition rows (375) are
the named §2 residue and stay.

## 4M. Batch-2 slice 1 LANDING RECORD (2026-08-28)

**The envelope splice reaches the verdict lane; two byte-channel gaps
the alarm caught are fixed. Census: agree 141→2,023, declined
5,100→3,128, disagree 0→90 (named, the next slices' work list).**

- **The splice**: `evalValue` built its `UserCallInliner` WITHOUT the
  statement loop's `spliceHook` — verdict sides compiled
  `$result.values` as raw variable reads and walled. The hook now
  threads `executeStatements` → `tryAdjudicate` → every side
  evaluation (`SpliceHook`); pin: `AssertVerdictSpliceTest` (adjudicate
  + polarity + condition/size lanes). `.activities` reads hit F6.1's
  loud wall — honest declines.
- **Alarm catch #1 (enum-under-Any)**: the literal channel spells an
  Any-carried enum as a quoted string while the enum canon spells the
  bare name — the byte verdict FAILED an assert the engine holds.
  Fixed as a NAMED decline (`enum kind has no literal channel`);
  witness `AssertVerdictsTest.enumUnderAnyDeclinesToHost`.
- **Alarm catch #2 (TDSNull sentinel)**: expected `'TDSNull'` vs an
  actual NULL cell holds in the lattice by the DECLARED sentinel
  policy (PureAsserts, expected-direction only) but byte-differs by
  construction. New declared-policy arm on the byte channel
  (`sqlTdsNullPolicy`, the 2-ULP shape) — hold BY POLICY, counted.
- **Alarm witnesses got a RESERVED buffer** (`sqlDisagreeSamples`,
  printed as `[canon] ALARM`) after the one alarm row was crowded out
  of the 200-cap shared sample buffer.
- Full sweep GREEN: inner `sql-verdict disagree=0` (1,464 agree, 478
  declines), scoreboard byte-identical, soft ceilings exact.
- **Remaining outer census (next slices)**: disagree 90 = the
  grid-text render family (CSV floats/TDSNull-in-joined-strings/`~`
  joins — D3's `GridCompare.renderedText` arm), arrival-order rows
  (D3 order key), decimal/temporal golden spellings, 3 forAll-contains
  shapes. Declined 3,128 = resolver class-query-under 924 (largely
  sql-text family), Tabular sides 654 + Graph sides 214 (the D3 grid /
  D4 graph verdict arms), §2 partition 375, unknown-function 180 (D4),
  getAll shapes 177, assertContains-overload 89, TDG unbound 68, tail.

## 4N. Batch-2 slice 2 LANDING RECORD (2026-08-28)

**D3 executed — the grid/order conventions live in verdict
construction. Census: disagree 90 → 8, agree 2,023 → 2,105.**

- **ORDER VIEW** (`AssertVerdicts.orderView`): SORTED (ends in sort
  through the audited order-preserving tails, moved verbatim from the
  harness) / INCIDENTAL (bottoms at a store source or an execution
  frame — SQL arrival order; engine goldens encode H2's) / DEFINED
  (pure values). An INCIDENTAL assertEquals fetches CANONICAL-order
  riders on both sides and judges order-insensitively (exactly the
  assertSameElements shape); SORTED/DEFINED stay strict. Witness:
  `AssertVerdictSpliceTest.incidentalOrderPolicy` (reversed golden
  holds unsorted, FAILS under sort()).
- **RENDERED-TEXT arm**: toCSV/toString-over-relation/
  toCSV→replace('\n',sep)/sep-join-over-incidental spellings route to
  `GridCompare.renderedText` — the one policy owner, R1b-probed;
  assertSameElements gets the token-multiset view. Burned the
  calendar CSVJOIN family and the joined-string rows.
- **GRID-PAIR arm**: both sides statically relation-stamped →
  `GridCompare.grids`. Witness `gridPairVerdict` (#TDS golden vs
  project). Non-tabular execution under a relation stamp is a LOUD
  wall.
- **forAll-contains subset fold** moved from the harness's fc arm:
  both sides DB-computed, membership judged by the lattice.
- **DriverPk parity**: the verdict side lane now applies
  `DriverPkAppend` exactly like the generic statement path — the
  option is EXECUTION ENV (the validation family's 14 disagreements
  were a missing ID column; all burned).
- **R1 probe isolation** (`r1Suspend`): the dual channel's duplicate
  executions no longer double-feed the [canon] disagree≤27 pin.
- **The remaining 8 disagreement rows are WIRE-FIDELITY findings,
  not verdict-construction gaps** — the census doing its job:
  1. Decimal SCALE drift (×4, testDataTypeMapping): literal `1.234d`
     vs the column-scaled wire cell — X2's own doctrine ("fixed at
     emission, never re-blurred"); an emission-scale work item.
  2. Temporal nine-digit convention (×2, graphFetch dates): the wire
     decode must carry the engine's fromSQLTimestamp nine-digit
     subseconds for the lattice's exact compare.
  3. Sort-TIE order (×1, testTDSConcatenate): golden encodes one tie
     order; a re-plan legally produces another (the charter's phantom
     class).
  4. Milestoning TDSNull row-strings (×1): null-cell string-concat
     spelling across lanes.
  Cutover (batch 3) requires these adjudicated: emission fixes or
  NAMED ceiling-pinned residue agreed with the user (D4's mechanism).

## 4O. Batch-2 slice 3a LANDING RECORD (2026-08-28)

**Census: agree 2,105 → 2,635, declined 3,128 → 2,598, disagree
steady at the 8 named §4N rows.**

- **§2 partition BY FORM at the splice**: assertSameSQL/
  assertSameSQLs/assertEqualsH2Compatible/assertSqlEquals classify as
  `host-partition-sqltext` WITHOUT routing — they compare the PLAN by
  design; routing them only produced noise walls.
- **assertSize learns the result kinds** (cluster 34 moved to the
  owner): grid ROWS, graph array length, values otherwise; the
  ONE-CARRIER envelope rule ($r.values of a relation execute = one
  TDS) reads from the MODEL (`ExecutionResult.envelopeCarriers`),
  keyed by the same read shape as the harness arm
  (`envelopeValuesRead`).
- **assertContains arm**: both sides DB-computed, lattice membership.
- Witnesses: `AssertVerdictSpliceTest.sizeEnvelopeAndContains`.
- Remaining declines (~2,598): §2 partition ~690, class-query-under
  ~610, assertEquals tabular/flat-cells ~340, D4 JSON 175 + graph
  sides, getAll shapes ~175, TDG unbound ~68, tail. Remaining
  slices: flat-cells/tabular assertEquals conventions, D4
  (testExtension + JSON verdict arm), resolver gaps adjudication.

**Process note (recorded twice now)**: no repo writes while a chain
runs — two chains were stopped mid-run after edits started; the
certification chain must launch AFTER the slice's last write.

## 4P. Batch-2 slice 3b LANDING RECORD (2026-08-28)

**D4 partially landed; the §2 partition is fully NAMED. Census:
agree 2,637, declined 2,596 (of which the honest partition:
sqltext 961 + TDG 123), disagree 8 (§4N).**

- `testExtension.pure` loads (the 7 unported-sibling wall rows join
  the scoreboard doc — this slice's adjudicated change);
  `assertJsonStringsEqual` now RESOLVES and the verdict ARM exists
  (JsonCompare, the V3 tree owner; the [x]≡x root bridge moved).
- Graph sides DECODE (the DB-built JSON array's elements — the
  harness Eval convention moved to the owner).
- TDG/plan-text asserts classify `host-partition-tdg` at the splice
  (§2's TDG arms), joining the sqltext partition.
- **D4 remainder (typing/registry legs, the arm is ready)**: the JSON
  form's sites stay declined on (a) `Result<T|m>` — our
  `execute().values` types `[*]` where the engine refines to the
  inner query's `String[1]` (multiplicity failure BEFORE any arm),
  and (b) the `equalJsonStrings`/`parseJSON` natives the loaded
  bodies reference. Both are engine-parity platform work, not
  verdict-construction.
- **Remaining non-partition declines (~1,512)**: resolver
  class-query-under (~610) + getAll shapes (~175) — resolver legs;
  assertEquals tabular/flat-cells conventions (~340); D4 typing
  (above); host-unsupported 30; tail.

## 4Q. TENET CORRECTION (2026-08-28, user catch) — the pure-source
## dependency is EVICTED

**The violation:** slices 1–3b made corpus assert resolution work by
LOADING real legend-pure assert sources (and engine's
testExtension.pure) into the corpus model as library files. That made
the reference implementation a RUNTIME COMPONENT of our platform —
against the project's core premise (legend-lite REPLACES pure/engine;
checkouts are spec and test input only). The error conflated
oracle-as-test-input (corpus/PCT sources — legitimate) with
oracle-as-platform-machinery (the stdlib our model resolves against —
never).

**The correction (this slice):** the assert family is now 47
PLATFORM-OWNED registry natives in Pure.java — every real overload of
assert/assertFalse/assertEquals/assertNotEquals/assertSameElements/
assertSize/assertEq/assertEmpty/assertNotEmpty/assertInstanceOf/
assertIs/assertContains/assertEqWithinTolerance +
assertJsonStringsEqual, signatures copied VERBATIM from the real
`.pure` files and verified (spec by verification, never by loading);
`PlatformTypes.ASSERT_FAMILY_OWNED` suppresses parsed twins loudly
(the existing platform-owned mechanism — the twin-shadowing fear that
justified the library route was already solved by the house design).
DELETED: Corpus.PURE_ROOT/PURE_ASSERTS/TEST_EXTENSION + their loads,
the library-scoped native prune, v7Spell. GUARD:
`Runner.registerLibrarySource` refuses `meta::pure::functions::`
elements outright (`LibraryPlatformNamespaceGuardTest`); tenet
recorded as an INVARIANT in AGENTS.md.

**Verification:** full sweep GREEN with the census IMPROVED (agree
2,637 → 2,650, declined 2,583, disagree 8 unchanged); the scoreboard
change is exactly the 7 testExtension wall rows REVERTING; soft
ceilings exact; inner alarm 0.

## 4S. LEG 0 + LEG 1 LANDING RECORD (2026-08-28)

**LEG 0 EXECUTED** (90be6b6c): lane-classification guard — sqltext 961
/ tdg 123 pin EXACTLY in the corpus runner; h2-exec rescued ≥ 632
floor + unverifiable ≤ 145 shrink-only ceiling (the leg-7 ratchet).

**LEG 1 EXECUTED — the grid canon, the ratified end-state-shaped
design (NOT a host-only arm). Census: agree 2,650 → 3,002 (+352),
declined 2,583 → 2,230 (the FULL 353-row flat-cells wall), disagree
8 → 9 (the +1 adjudicated below). Inner sql-verdict alarm 0,
ulp-policy 7. Scoreboard byte-identical; witnesses 10/10; full chain
GREEN.**

- **The mechanism**: `wrapGridCanon` (CanonicalRenderSql) appends one
  per-ROW canonical text to any TABULAR plan — per-cell PURE-LITERAL
  spellings (LiteralSpelling.literal, the ONE grammar both the grid
  cells and the value peer's literal channel spell), GRID_CELL_SEP
  () joined, NULL cells spelling bare `TDSNull` (disjoint from
  a quoted string by the grammar itself); a string cell carrying the
  reserved separator POISONS its row canon to NULL (counted decline,
  never a mis-split). The executor routes by the DECLARED result
  shape (static, pre-execution); the tabular decode strips + harvests
  the canon column row-aligned. The peer's row canons FRAME from its
  literal-channel element canons chunked by the grid's width —
  framing writes separators only. Judgment: ordered list equality,
  or sorted-list equality under the INCIDENTAL view (cross-row
  shuffles fail — audit 9); host cell lattice (rowTupleMultiset /
  loose pool for sameElements) = the parallel referee; message,
  verdict, and probe from ONE decision point (the 28-row phantom is
  structurally impossible).
- **Fetch rule discovered**: a grid pair fetches BOTH sides in
  DEFINITION order (canonicalOrder=false) — the canonical-order rider
  re-sorts a literal side's VALUES and destroys the row chunking (the
  first witness run caught it; grid-ness is static, so the rule is
  compile-time).
- **Alarm catches, all fixed same-slice** (19 inner disagreements →
  0): (1) ENUM cells decline the byte channel at wrap ("grid-canon:
  enum cell has no literal channel" — the §4M scalar precedent);
  (2) the abstract DATE stamp missed the pure-literal % prefix in
  LiteralSpelling.literal — a real gap in the shared grammar, fixed
  (leaf already claimed DATE); (3) the declared 2-ULP
  dialect-arithmetic policy gets its grid arm (positional cell gate,
  counted sqlUlpPolicy — 7 rows).
- **The +1 outer disagreement is ADJUDICATED as a grid-form member of
  the NAMED temporal subsecond wire-decode class** (§4N row 2;
  witness `[%2014-12-05T21:00:00.000000, 5]` — the same value row the
  flat-cells attempt census carried). Burns with leg 5's decode fix.
- Registers moved with justification: JavaEvalLedger AssertVerdicts
  1240→1525 + StatementExecutor 2472→2482; TenetRatchet 13→14 (the
  grid harvest's ONE getString — text carriage); CodeShape allowlist
  CanonRider.gridWidth.
- **Traps recorded**: the battery shorthand "CodeShape" names NO test
  class — the guard is `CodeShapeGuardrailTest`, and a -Dtest name
  that matches nothing passes SILENTLY (two chain iterations lost);
  measure the ledger AFTER the slice's last edit (a post-measure
  3-line edit tripped a chain); allgates REQUIRES the
  LEGEND_ENGINE_ROOT/LEGEND_PURE_ROOT env (bare launch = stale-root
  phantom failures across 5 gates).

## 4T. LEGS 2+3 LANDING RECORD (2026-08-28) — Result<T|m> ENGINE-
## VERBATIM + the JSON natives; a misdiagnosis caught by the user

**Census: agree 3,002 → 3,161 (+159), declined 2,230 → 2,071 (the
JSON family adjudicates), disagree 9 unchanged, inner alarm 0,
h2-exec counters IDENTICAL (320/632/0/145). Full chain GREEN.**

- **Leg 2 — the engine's `Result<T|m>` (result.pure:17), spelled
  VERBATIM and solved by the GENERAL machinery**: `execute<T|m>
  (f:Function<{->T[m]}>…):Result<T|m>[1]` (router_entry.pure's own
  shape). New capability, general: class-level MULTIPLICITY
  parameters — GenericType carries multiplicity arguments
  (TypeClassifier captures the `|m`/`|1` spellings the parser always
  preserved), the kernel resolves them through the same bindings the
  if/let machinery fills, and parameterized-receiver property access
  instantiates `values:T[m]` positionally (mults-omitted receivers
  keep the pre-leg [*]; over-supply throws). A serialize execute's
  `.values` types `String[1]` and the strict JSON signature accepts.
- **Leg 3**: `parseJSON`/`equalJsonStrings`/`JSONElement` registered
  verbatim from core_functions_json/json.pure (§4Q pattern); the
  JSON-assert side reader accepts a GRAPH result (the DB-built
  envelope IS the String[1] document). JSON tail now 4 rows (2
  strict-parse, 2 toPrettyJSONString) — leg 6.
- **THE PROCESS RECORD (user catch — "we need to support the correct
  signatures")**: the first signature attempt was retreated to a
  call-site fix-up on a MISDIAGNOSIS: the sweep's per-test FAIL lines
  were read as regressions without checking the COMMITTED scoreboard
  — all 7 "regressed plan goldens" were pre-existing FAIL rows that
  print on every sweep. The real attempt-1 damage was the then-
  incomplete instantiation arm THROWING on 1-arg Result receivers.
  With the machinery completed, the engine spelling passes every
  gate with ZERO h2/plan movement; the fix-up seam is DELETED.
  **RULE: before attributing a sweep FAIL row to the change under
  test, grep the committed scoreboard for it.**

## 4U. LEG 4 CENSUS SPLIT LANDING RECORD (2026-08-28)

**The 513-row "class query under TypedUserCall" bucket was ENTIRELY
sql-text family.** Two diagnostics: the resolver wall names its
wrapper CALLEE, and v7DualChannel classifies by CONTENT (an assert
whose args pull the sqlQueryToString-family vocabulary — the
harness's own containsSqlText — is a plan-text compare whatever its
form name; §2 by definition). Result: sqltext partition 961 → 1,529
(+568), agree/disagree/declined-total EXACTLY unchanged (3,161/9/
2,071 — the movement was entirely within the declined column; a
split, not a burn). Lane-guard pin moved with this table. Leg 4's
REAL remainder: getAll shapes ~175, TypedMap wrappers 63, pkOfFunc
FunctionDefinition-vs-Function typing 43 (CLOSED §4V — now the
expressionSequence reflection wall), metamodel-fn lowering gaps
~50, tail.

## 4V. LAMBDA/REFERENCE CLASSIFIER LANDING RECORD (2026-08-28)

**Function values now classify m3-true.** A lambda literal's stamp is
`LambdaFunction<ft>` (normalized in the TypedLambda CONSTRUCTOR — the
node owns its classifier, every mint site inherits it); an eta-expanded
reference to a body-bearing user function stamps
`ConcreteFunctionDefinition<ft>` (passed explicitly; the ctor keeps a
non-bare info); an overloaded/unresolvable mangled ref keeps
`Function<Any>`. Structural `FunctionType` survives only as the
SIGNATURE under the carrier; one reader (`TypedLambda.functionType()`
strict / `PlatformTypes.functionTypeOf` tolerant) replaced ~35
ad-hoc casts and instanceof reads. Kernel: PAIRWISE carrier
unwrapping (a formal that keeps its carrier nominal —
`FunctionDefinition<Any>`, argument not a FunctionType — sees the
actual's carrier; everything else unwraps both sides as before) +
raw-class lattice subtyping in BOTH unify and paramTypeScore generic
arms (the two halves agree). Deferred lambda literals against a
nominal carrier formal self-type like the TypeVar slot
(`nominalFunctionCarrier`). Demangle fix: the mangled tail's return
name compares EXACTLY against the raw simple name
(`rawSimpleName`), never `endsWith` on the parameterized typeName —
`pkTestBare__Relation_1_` (returns `Relation<Any>[1]`) now resolves
and eta-expands.

RESULT: pkOfFunc 43 rows advanced from the TYPE wall
("expected FunctionDefinition<Any>, got Function<…>") to the honest
REFLECTION wall ("FunctionDefinition has no property
'expressionSequence'" — the metamodel growth-by-witness arc's next
leg). Census EXACTLY stable 3,161/9/2,071, sqltext 1,529, h2 lanes
unchanged; scoreboard diff = diagnostic-dump spellings only (dumped
lambda infos now show the carrier) + one SHAPE wall message became
more precise. Witnesses: CompileFunctionTest ×4 (classifier stamp;
lambda literal and concrete-ref acceptance into
FunctionDefinition<Any>; Function<{…}>-typed variable REJECTED — the
lattice direction that keeps native refs out).

TWO TRAPS: (1) five formerly-TOLERANT instanceof readers (Lowerer
map-mapper ×2, Fold.isManyScalarCol, AssociationJoins paramClass/res)
were first converted to the STRICT accessor and threw
"non-function classifier" across validation/milestoning — constraint
desugar paths mint TypedLambda with BODY-typED infos (Integer/String;
pure-false, recorded debt below); converted-from-CAST sites keep the
strict accessor (they threw before too). (2) A failed lane guard used
to HIDE the census — the [canon]/[v7] prints now run BEFORE the
guard asserts (diagnostics before verdicts).

DEBT (named, not fixed here): resolver/desugar mint sites that stamp
TypedLambda with a non-function info (constraint validation,
milestoning frames). The ctor normalization passes them through; the
tolerant readers preserve behavior. The proper fix is minting real
`{row->Boolean}`-shaped FunctionTypes at those sites.

## 4W. LAMBDA-SLICE AUDIT + FIX-SLICE LANDING RECORD (2026-08-28)

User-directed adversarial audit of §4V BEFORE the sql-producer leg —
full findings + receipts in docs/V7_LAMBDA_AUDIT_2026_08_28.md.
Verdict: core architectural (ctor-owned classifier, one reader,
kernel lattice); three burns executed same-day (F1 honest
DriverPkAppend mint + ALL readers strict, falsifier-verified sole
producer; F2 engine-verbatim carrier signatures — router execute,
preval, concatenateTemporalTdsQueries — + kernel nominal-gate-before-
structural-unwrap, witnessed both directions; F3 identity-argument
generalization governance pin). NEW FINDING R8:
meta::pure::mapping::execute is an INVENTED FQN (nowhere in either
checkout) — kept as a legacy alias spelled identically to router's
verbatim signature; deletion = named future leg. Deferred with the
reflection leg: eta value-identity (R6/R7). Census exactly
3,161/9/2,071 through every iteration.

## 4X. SQL-PRODUCER LEG, SLICES 1+2 LANDING RECORD (2026-08-28)

**Slice 1 — the activity log answers from the compiler.** The whole
sql/sqlRemoveFormatting/assertSameSQL family (helperFunctions.pure:
38-60, testAssert.pure:18-25) telescopes to ONE fact: the SQL an
execute() generated — the COMPILER's own output. ExecFrame retains
its source execute call; Frames.relationalActivitySql renders it
through the SAME engineSql pipeline as toSQLString (EngineStyleH2).
Two splice arms answer the reads by exact FQN: the INLINED activities
chain, and the producer CALL pre-inline (the inliner's hook rewrites
a bare frame-var argument into its chain, erasing frame identity —
the call folds first; the verbatim corpus bodies are the SPEC,
mirrored exactly incl. the \n/\t strip). Witnesses pin the exact
render both ways. +2 agree (3,161→3,163); two bitemporal-union rows
advanced ERROR→honest FAIL (real SQL divergence now visible).

**Slice 2 — classification by resolution; ALL sql name-sniffing
DELETED.** isSqlText (endsWith), containsSqlText, the simple-name
form set (which carried 'assertSameSQLs' — defined NOWHERE in the
engine, R8's smaller sibling, dropped), and ExecCallFinder's
simple-name terminal/through sets are GONE. One classifier:
resolvesTo(af, ctx, fqns) — explicit FQN, resolver-recorded import
candidates, model lookup, then an exact simple-name REGISTER lookup
(the set's own FQNs) for bare native spellings. Registers:
SQL_PRODUCER_FQNS {mapping::sql, mapping::sqlRemoveFormatting,
sqlstring::toSQLString, ::toSQLStringPretty} (shared with the splice)
and SQL_ASSERT_FORM_FQNS (asserts::assertSameSQL,
h2::assertEqualsH2Compatible, tests::assertSqlEquals). FALSIFIER:
the sqltext partition re-measured EXACTLY 1529 — mechanism moved,
zero rows moved; the §8.0 pin's provisional marker retired.

REMAINING DISTANCE (named): harnessVocabName survives in its
UNRELATED assert-form dispatch role (planToString wrappers,
runLegendTest — not sql classification); the D3 bare-name hijack
exposure is unchanged from the old vocab gate and dies at the
typed-tree cutover; slice 3 = the host-produced String[1] short
circuit + routing these asserts through actual evaluation with the
advisory/h2-replay POLICY preserved at the verdict layer.

## 4Y. HARNESS-DISPATCH IDENTITY CUTOVER (2026-08-28)

**harnessVocabName is DELETED — all nine sites.** The user's catch:
a hard-coded name list deciding what the harness recognizes is the
same disease as the sql sniffing, and `startsWith("assert")` was a
name-PREFIX match. Every recognition site now resolves IDENTITY
through the one `resolvesTo` path (explicit FQN → resolver import
candidates → model lookup → exact register lookup for bare
spellings):

| site | old gate | new register |
|---|---|---|
| assert dispatch (main + per-driver loop) | vocab + startsWith("assert") | ASSERT_FORM_FQNS — DERIVED from the platform registry (every native in meta::pure::functions::asserts) + 5 corpus assert FQNs |
| test wrappers (runLegendTest/runTest/runGraphFetchTest/mayExecute*) | vocab + simple-name set | TEST_WRAPPER_FQNS (7 exact corpus FQNs) |
| print/println statements | vocab + simple names | PRINT_FQNS (io::print, io::println) |
| compileLegendGrammar splice | vocab + simple name | COMPILE_LEGEND_GRAMMAR_FQNS |
| per-driver map loop | vocab + "map" | MAP_FQNS (collection::map) |
| executeLegendQuery splice (ElqSplice) | vocab + simple name | meta::legend::executeLegendQuery |
| isExecuteCall | vocab + bare-or-::execute suffix | ExecCallFinder.EXECUTE_FQNS (router, post-R8) |

WHAT REMAINS in the harness, honestly classified: checkAssert — the
HOST LATTICE, the V7 program's own referee (dual-channel, counted
every sweep, D3-pinned for wholesale deletion at cutover) — plus
pure ORCHESTRATION (unwrap wrappers, unroll loops, splice sources)
whose evaluation all happens through the platform. Recognition is
now identity everywhere; evaluation-in-harness = the referee only.

**Census headline honesty**: v7Summary now prints
`agree / disagree / sqltext / tdg / declined` — the two BY-DESIGN
text partitions (sql renders 1,529; test-data-generation CSV 123)
are their own columns; `declined` finally means real migration
backlog only: **417** (the leg-4 remainder estimate ~419, confirmed
by measurement). FALSIFIER: both partitions re-measured EXACTLY
under identity dispatch — zero rows moved.

## 4Z. SQL-TEXT OUTCOME BUCKETS — user-ratified, measured, pinned
## (2026-08-28)

The run/gen split was scaffolding; the user's directive replaced it:
"sql-run should ONLY count if we are 100% confident we ran the H2
dual and compared equal; whatever is left that is NOT 100% verified
must be transparent." Mechanism: the verify machinery records a
PER-ASSERT OUTCOME at every exit (SQL_TEXT_OUTCOME); the census
consumes outcome + shape — it reads what HAPPENED, never a guess.

MEASURED (sum 1,652 = old sqltext 1,529 + tdg 123, exact):

| bucket | count | meaning |
|---|---|---|
| assert-sql-text-with-exec-passing | **989** | golden EXECUTED on H2, rows compared EQUAL to our DuckDB rows (both text-match and text-differs paths; row divergence stays HARD-GATED 0). The ONLY comfort bucket. |
| assert-sql-text-only | **44** | nothing executed in the assert's sides — text IS the contract (plan-literal 17, plan-let 6 pulled OUT of the old tdg bucket which conflated them, render tails) | — **27 as of batch 18 (2026-09-03): 44→43 (slice 3a) →40 (§5 first cut) →35 (batch 15 paginate SQL-text asserts) →27 (batch 18 plan nodes as rows: the plan-text asserts of the optional-parameter and datetime plans flipped)**
| assert-sql-text-UNABLE-TO-EXEC | **492** (was 502) | transparent residue, per named sub-reason: **diff-noreplay 321** (text DIFFERS and replay impossible — the WEAKEST class, previously invisible: no counter incremented, soft-ceilinged only), match-noreplay 142, no-generator-noreplay 20, **predicate-diverged 6** (slice 3: predicates now EVALUATE for real — 10 of the old 16 became verified dual-channel passes; these 6 evaluate FALSE against our dialect's SQL and are recorded divergences, the assertSameSQL-mismatch policy applied uniformly), both-ours 3 |
| assert-test-data-csv | **117** | TDG CSV compares (pass/fail definitive; was 123 with the 6 plan-let rows wrongly inside) |

FINDINGS the instrumentation forced: (1) the old "145 unverifiable"
was only the match-noreplay slice — the 321 diff-noreplay class is
2.2× larger and WEAKER (text disagrees AND rows never checked);
both are now shrink-only burndown scope. (2) the old tdg 123
carried 6 plan-text rows. (3) the "394 own-arm" estimate dissolved:
h2compat rows replay-verify inside exec-passing (989 > the old 952
estimate). Pins: 989 grows-only-by-burndown / 44 / 502 shrink-only
/ 117 — all EXACT lane guards.

## 4AA. SLICE 3 LANDING RECORD — REAL EVALUATION, TASK #13 CORE
## COMPLETE (2026-08-28, pushed 518f32d8 + 62a09657)

**Predicates (518f32d8):** the 16 fragment-check predicates evaluate
for real (activity-log strings) — 10 verified dual-channel passes, 6
predicate-diverged (engine-internal naming markers our correct SQL
spells differently; recorded via the sql-text: channel, never a hard
fail — the dialect-text policy uniform at the verdict layer).

**Equality half (62a09657):** evalSideText = the assert's OWN side
evaluated (outcome-driven: as written; a non-string value — the raw
Result envelope — re-evaluates through the corpus body's definition
sqlRemoveFormatting($result), testAssert.pure:20, exact FQN).
ExecCallFinder.sideSqlText terminal surgery DELETED; finder retired
from the JDBC register. THE FIND: toSQLString dispatched at
STATEMENT ROOT only — nested calls leaked their lambda to resolver
walls, masked by the surgery for months; it now folds
POSITION-INDEPENDENTLY at the splice (Frames.renderSqlText → the
same toSqlString K-arm). Side effect: **94 backlog rows burned** —
census agree 3173→3269, backlog 417→**323**, exec-passing 989→990;
verified/rescued/diverged 321/632/0; zero test regressions.

Registers adjudicated with receipts: evaluator ledger 2513→2520;
sql-text-side gap 57→66 (RECATEGORIZATION — every acquisition
failure now counted with its cause where old surgery failed
silently; verification equal-or-better in the same sweep); four
typed-IR tolerance pins scaled with ~1000 new plans over the SAME
registered seams (all EQUALITY-0 gates held); h2 capability walls
945→946. NAMED FOLLOW-UP: tolerance pins should key by DISTINCT
SEAM KIND (mapping × column × type-pair), not slot count — plan
volume must never masquerade as model change again.

CURRENT CENSUS (the §4Z bucket table's exec-passing is now 990;
backlog 323): agree 3269 / disagree 9 | exec-passing 990 /
text-only 44 / unable-to-exec 492 / csv 117 | backlog 323.
BURNDOWN INVENTORY: 9 disagreements (leg 5) + 323 backlog (leg 4:
getAll ~175 partially burned, TypedMap, expressionSequence 43,
tail) + 492 unable-to-exec (diff-noreplay 321 FIRST, by replay
root cause).

## 4AB. DIFF-NOREPLAY BURNDOWN, SLICE 1 — GRAPH FRAMES REPLAY
## (2026-08-28)

**Census-first (the h2-unverifiable causes now RIDE the outcome):**
H2Verify.decline stashes its canonical bucket per thread
(LAST_DECLINE); the diff-noreplay exit names its replay-decline cause
(`diff-noreplay :: <cause>`) — the §4Z transparency rule one level
down. Measured composition of the 321 (never guessed): **non-tabular
result frame 295** (Graph 314 / Collection 41 / Scalar 6 across the
noreplay classes), engine-golden duplicate-alias 11, enum-decode 7,
tempTableForIn 4, arity 2 (+7 more under text-only).

**The fix (one root cause = 93%):** class-mapped queries produce a
GRAPH frame — the instance array the DATABASE built, flat json
objects keyed by mapped property name — and those keys are EXACTLY
the golden's data aliases. `H2Verify.goldenGraphCompare`: golden rows
vs json objects as order-insensitive multisets over the golden's data
aliases; `pk_$i`/`k_businessDate`/`k_processingDate` are the engine's
graph-assembly bookkeeping (excluded by the engine's own spelling);
temporal json text decodes TYPE-driven from the golden's JDBC column
type. Every structural surprise is a COUNTED decline: nesting, key
skew, enum-typed property (frame carries decoded names, golden raw
codes — same rule as the tabular enum decline, predicate supplied by
the harness from ctx.findProperty). Rider fixes: **microsecond
temporal floor** in norm() (H2 nanos vs DuckDB's microsecond STORAGE
— the float 10-digit rule's temporal twin; witness testLessThan seed
.123456789 vs .123456) and divergence messages print the DIFFERING
rows (diffRows), not two identical-looking heads.

**ONE adjudication the referee refuses (named, counted, never a
verdict) — CORRECTED after user challenge + code homework (see the
§4AC receipts):** the engine's relational execution is a consistent
ROW ALGEBRA — one object per result-set row, NO pk dedup anywhere
(RelationalResult.java: zero distinct/dedup/pk sites; the qualifier
test's assertSize(values->at(0),1) sizes one element and pins
nothing — the earlier "engine pins both conventions" reading was a
misread). OUR compilation is set-shaped (filters dedup via
subselect/exists; string-plus keeps pure's plus([...]) reducer
semantics — plus.pure: joinStrings over the flattened collection).
Witnesses: testQualifierQueryWithOr golden 7 identical rows vs our
1; testQualifierWithOperation golden 1 row vs our 4 ('Test' from
concat over the empty qualifier — invisible to the test's own at(0)
asserts). BOTH the cardinality skew and the parked Collection/Scalar
lane are the SAME finding: our relational lane diverges from the
engine's row semantics. Direction per the relation-lane tenet:
conform BY EMISSION (qualifier/filter lowering fans out rows like
the engine; string-plus over an optional column null-propagates so
the empty row vanishes) — a design leg to schedule, not a referee
patch.

**MOVEMENT (all lanes else byte-stable, zero test regressions):**
exec-passing 990 → **1276** (+286 REAL golden-vs-ours row compares),
UNABLE-TO-EXEC 492 → **206**: diff-noreplay 321 → **160**
(graph-keys-mismatch 69, collection/scalar 45, enum-decode 22,
engine-golden dup-alias 17, tempTableForIn 4, arity 2, skew 1),
match-noreplay 142 → **27**, no-generator 8, predicate-diverged 6,
both-ours 5. M1 verified 320-floor → 436, rescued 632-floor → 791,
unverifiable 145 → 30, diverged 0. Registers adjudicated in the same
commit: dup-alias registry 11 → 17 (same engine-golden gap, reached
by 6 more rows now that goldens actually execute), advisory sql
diffs 318 → 157 (down-ratchet: diffs moved to the counted rescue
channel), rescued-passes ceiling 614 → 751 (flag moved channels on
the same passing tests; no exact pass demoted).

**NEXT (by size):** graph-keys-mismatch 69 (measure shapes; likely
association/nested-property alias classes) → collection/scalar 45
(BLOCKED on the plus adjudication) → enum-decode 22 (derive the
decode for class frames like enumDecodeFor does for tabular) →
residue ~24 (engine-golden defects + machinery gaps, permanent named
transparency candidates).

## 4AC. SLICES 2–4 + THE HOMEWORK CORRECTIONS (2026-08-28, user
## challenge answered with code receipts)

**Slice 2 (keys-mismatch 69 → 6):** three measured classes, all
engine-generation conventions — union bookkeeping (pk_$i_$j member
pks, u_type discriminator) and milestoning period columns
(from_z/thru_z/in_z/out_z, union-suffixed) joined bookkeepingAlias;
the frame-side twin: an instance's reserved businessDate/
processingDate keys with NO same-named golden alias are the query's
temporal context echoed back (golden spells them k_*), excluded;
an EMPTY frame verifies by golden row count (data rows only —
all-NULL golden rows are the engine's client-side SQLNull drop,
relationalMappingExecution.pure:480). +84 exec-passing.

**Slice 3 (enum 22 → 7):** per-key enum decode for class frames —
mappingFqnOf + decodeOf extracted from enumDecodeFor; the golden's
raw source codes decode to the frame's names through the SAME
EnumerationMapping; underivable keeps the counted decline. +14.

**Slice 4 (dup-col 17 → 0) — REGISTRY STORY FALSIFIED BY PROBE:**
stock h2-2.1.214 accepts the "duplicate column" goldens on the
ENGINE's session (H2Defaults: case-SENSITIVE; its patched jar
replaces only Mode/TypeInfo — NO duplicate leniency exists). The
collisions ("city" vs CITY in one subselect,
testColumnCollisionInSubselect) were created by OUR
CASE_INSENSITIVE_IDENTIFIERS session (added for DuckDB seed-replay
parity). Fix: H2Settings.ENGINE_CASED (H2Defaults verbatim, incl.
OVER in NON_KEYWORDS which ours lacks); the oracle retries
case-collision goldens on it. Registry row ratcheted 17 → 0; the 6
seeds that cannot replay case-sensitively surface under their own
'seed replay' cause. modelJoin family: 44/44 row-verified.

**Q2 correction (the §4AB "both conventions" claim was WRONG):**
assertSize($result.values->at(0), 1) pins nothing (sizes a single
element); RelationalResult.java builds one object per row with no
dedup. Engine row algebra is consistent; the skew is OURS. §4AB's
adjudication list corrected in place.

**NAMED FOLLOW-UP (user challenge on the slice-2 exclusions):** the
bookkeeping exclusion verifies every mapped property value row-wise
but NOT instance identity (pk_$i_$j) or union-member attribution
(u_type) — the frame's json never carries them, though OUR executed
SQL computes them before the json assembly discards them. The
strictly stronger fix: capture the pre-assembly raw rows alongside
the Graph frame (executor graph-egress change) and compare the
golden's COMPLETE column list, no exclusions but k_* constants.
Queue after the set-vs-row adjudication or before — user's call.

**CENSUS after slices 2–4:** exec-passing **1385** / text-only 44 /
unable-to-exec **97** / csv 117; diff-noreplay 321 → **71**
(collection-scalar 45 PARKED + enum-underivable 7 + case-sensitive
seed replay 6 + keys tail 6 + tempTableForIn 4 + arity 2 + skew 1),
match-noreplay **8**; M1 455 matched + 880 rescued / 0 diverged /
**11** unverifiable. agree 3269 / disagree 9 / backlog 323
byte-stable throughout.

## 4AD. RELATIONAL-CONFORMANCE LEG — RATIFIED DESIGN (2026-08-29,
## user sign-off in session)

**THE RULE: class-query navigations compile to the ENGINE'S ROW
ALGEBRA — left-outer-join fan-out, conditions in the join/WHERE, one
result row per surviving joined row. No dedup, no correlated scalar
subqueries.** Empty propagation falls out of the shape: a
non-matching root's row has NULL in the joined columns, the WHERE
fails, the row vanishes — no synthesized IS-NOT-NULL, no
null-handling arm.

**THE BOUNDARY (user question answered + ratified): PROVENANCE.**
Values born in pure-land — literals, parameters, computed scalars,
collection ops over them — keep pure semantics untouched (plus is a
reducer, list carriers unchanged). Values born from a MAPPED
NAVIGATION are a join while inside the query, and the engine defines
what a join means. The resolver already knows which side every
expression sits on. Precedent: Legend itself — in-memory vs
relational execution genuinely differ in these corners, and the
corpus is the RELATIONAL suite. (Naming trap: the parked oracle
lane "Collection/Scalar" names RESULT-FRAME shapes of relational
queries, not pure collections.)

**Ratified decisions:**
1. Per-row operations over navigations, not per-object reduction:
   map(f|$f.emps.firstName + 'Test') with two matches = TWO rows
   ('JohnTest','PeterTest'), engine-exact. Explicit aggregation
   (sum, joinStrings...) is where reduction lives. USER flagged and
   accepted: this changes observable class-lane results.
2. No dedup on the class lane: filter over a fanned-out qualifier
   keeps duplicates (testQualifierQueryWithOr: 7 rows, ours today 1).
3. Placement follows engine SEMANTICS (rows match); alias/text
   differences stay recorded dialect divergences. Row equality is
   the contract, text advisory.

**Execution (each slice a gated batch, battery-then-chain):**
census of navigation-arm firings (blast radius as a NAMED list,
never an estimate — **DONE 2026-08-29**: NAV_ARM_CENSUS_4AD.md;
**routing design NAV_ROUTING_DESIGN_4AD_SLICE1.md (2026-08-29,
awaiting sign-off)** — syntax-directed two-form routing (fanned join /
grouped join), one owner (the lift pre-pass), deletion list incl.
filteredNavLeafRead + aggScan's implicit-plus arms; supersedes every
batch-1 gate;
exists-material 946 / correlated-count 234 / exists-join-dedup 109 /
correlated-agg 2; 1,017 distinct tests, committed witness dump
nav-arm-census-4AD.txt, instrument lowering/NavArmCensus prints
per-sweep) → slice 1 map/select navigations (**BATCH 1 LANDED
2026-08-29, ALLGATES GREEN**: scalar filtered-nav reads in VALUE
position lift to the #fN fan-out join — the correlated-scalar arm
loses the shape; infix plus is row-wise over navigations, never an
aggregate demand (arity gate, the and/or precedent); position-scoped
— filter predicates untouched pending the slice-2 dedup plan (user
directive). The scope gate spends MAPPING TOPOLOGY: a CHAINED head —
a navigate step whose oriented predicate reads other slot aliases
(the mid-hop shape materializeRoot detects the same way) — keeps the
correlated arm, because its shared mid-hop join cross-fans under a
per-occurrence filter (testProjectMerge 3→10). An interim
pred-COUNT proxy gate was landed and REPLACED same-day by this
semantic gate after user review ("why did you hack a fix") — the
topology gate strictly dominated it (+3 more verified rows).
Receipts: exec-passing 1,385→1,390 (incl. testQualifierWithIsolation,
a baseline ERROR flipped to PASS on both lanes), M1 matched 455→457
(2 upgraded rescued→byte-match), tests/advanced 62→64, zero
regressions. NAMED residue on the correlated arm, each measured:
MILESTONED heads (lift emits unbound-alias SQL — pre-existing bug,
witness testTemporalDateVariable...), CHAINED mid-slot heads (burn =
per-occurrence mid-hop materialization), filter-position reads
(slot-prefix collision; slice-2 scope)) → slice 2
filter/qualifier predicates (dedup removal) → slice 3 unpark the
oracle Collection/Scalar lane (45 verify; the cardinality-skew
decline RETIRES — its reason for existing is gone). Acceptance per
slice: zero DuckDB-lane pass regressions, oracle conversions
grow-only, pins + charter same commit. Expected: unable-to-exec
97 → ~50, both known semantic divergences become verified
agreements.

## 4AE. MAPPING-REFLECTION FOLD LEG — SLICE PLAN (2026-08-30,
## census-first; the no-scalar-lowering cluster, 72 declines)

**Census (measured — baseline sweep at a6115d56 reproduces the
handoff scoreboard exactly: agree=3381 disagree=9 declined=323).**
The 72 `no scalar lowering registered for resolved overload` rows
split into THREE shapes, not one:

| shape | rows | witnesses (probed, scoped runs) |
|---|---|---|
| A. mapping/store reflection, scalar-ending chains | 46 | classMappingById 21 (tests/mapping/extends — testExtendsForMainTable: 4 assertEquals over mainTable() HANDLES + testSuperSetIdsAreCollected `.id` strings; testExtendsForPrimaryKey: 14 assertEquals + 2 assertSameElements, all ending `.column.name` STRINGS); rootClassMappingByClass 13 + view 6 + inferRelationalType 5 (meta::relational::tests::typeInference::* in tests/testRelationalExtension.pure — EVERY assert compares a STRING against dataTypeToSqlText); _classMappingByClass 1 (testSubtypeMapping.pure:70, assertSize over a filtered set) |
| B. toPostgresModel::newState | 18 | sqlDialectTranslation family (tests.pure assertConversion helpers): `assertEquals(^Node-literal, convertElement($input, newState()))` — NESTED PROTOCOL-NODE structural compares. The handoff's caution CONFIRMED: state-threading for dialect translation, a different shape than mapping reflection — its own slice, same seam |
| C. relationalExtensions | 8 | plan-text compares (`assertEquals($expectedPlan, $plan->planToStringWithoutFormatting(relationalExtensions()))` in testUnion/testRelationalResultSourcing + one `assert($result2->sql()->contains(...))`) — NOT reflection at all; the probe crashes at the extensions arg before the sql-text partition can claim the assert. A LANE-CLASSIFICATION row |

**Mechanism (probed, not guessed).** The HOST channel passes every
one of these tests today via the ratified read-only metamodel
vocabulary (MetamodelSteps/MetamodelWalk — planWalk claims
statement roots). The decline is DUAL-CHANNEL-ONLY: a statement-root
assert routes to AssertVerdicts (verdict-in-DB), its args lower
through Scalars.lower, and reflection natives have — correctly — no
scalar rule. The typing/checker half of the TDG template ALREADY
EXISTS: every FQN here is a registered Pure.java native validated
against the real legend-pure/engine .pure sources (CLASS_MAPPING_BY_ID
et al; specs re-read this session: functions_Mapping.pure:28-79,
platform_store_relational/functions.pure:254, relationalExtension.pure
:120-137, toPostgresModel.pure:31-48, extensions/extension.pure:62).
The MISSING half is the orchestration-time FOLD.

**RESOLUTION — QUARANTINE (user ruling 2026-08-30).** Three
designs were drafted and reviewed in session (per-FQN orchestrator
fold; platform pure-source library; one-router/one-evaluator, then
its metamodel-in-the-database correction). The tractability
homework showed the classMappingById family is probably tractable
under the metamodel-store frame but 66 of the rows
(inferRelationalType, pkOfFunc, newState) have no honest design
yet. RULING: **quarantine every metamodel-only decline into a
named, exact-pinned census partition and burn everything else to
zero first** (declined / mismatch / sql-exec); return to this
program afterwards. The full record — census, specs read, homework
done, homework open, per-bucket tractability verdicts, and the
standing one-entry-point architecture ruling — lives in
**docs/PROGRAM_MAP.md § "DEFERRED PROGRAM — METAMODEL AS DATA"**;
do not re-derive it here.

**STEP-0 CENSUS EXECUTED FIRST (user directive, same day):**
docs/FULL_RESIDUE_CENSUS_2026_08_30.md — every one of the 417
residue rows bucketed 1-by-1 via the new per-row decline-witness
instrument; docs/METAMODEL_MACHINERY_CENSUS.md — the machinery
lay-of-the-land. The census MOVED the quarantine boundary to
**144 rows** (ONE-STAMP 17 + SQLNull 1 are toPostgresModel-family
co-located; bare-lambda 10 + InstanceValue 2 are expression-tree
reflection; 2 expressionSequence walls hid in host-unsupported;
routerExtensions 5 proposed in) and collapsed getAll-76 to ONE
design leg (lambda-under-native splice resolution). The census doc
§6 carries the proposed burn order; its numbers supersede this
section's earlier 46/18/43 sketch.

**The quarantine slice (the only §4AE code change):**
- A `metamodel-reflection :: <detail>` classification bucket in
  the dual-channel census — a PARTITION, never a test exclusion
  (the tests keep running and passing on host). Composition: the
  census doc §4a's 144 rows (verified per test). Out:
  relationalExtensions 8 (plan-text lane rows — they burn with
  the classification work) and TypedMap 65 + TypedGraphFetch 2
  (h2-lane).
- Exact pin on the bucket total + per-FQN sub-reasons preserved;
  `declined` returns to meaning REAL BACKLOG. Scope table §8.0,
  the pins, and this charter move in the same commit.
- Classification is exact (resolver-produced FQNs / the metamodel
  property name), never message-sniffing beyond the production
  system's own refusal vocabulary; the chosen detection site is
  recorded at landing.

**Then the burn order for everything else (agreed in session):**
ONE-STAMP 17 (live invariant firing — a bug leg, outranks ports) →
getAll 76 (census-first, ~20 tests, expect shape families) →
relationalExtensions 8 + routerExtensions 5 classification →
bare-lambda 10 → host-unsupported 26 + tail 9; in parallel lanes:
sql-exec unable-to-exec 50 (TDG §S5 + emission-anatomy 7) and
text-only 44 re-examination after S5. Mismatch stays frozen at the
user-ruled 9.

**Do-NOTs (unchanged):** disagree-9 pin untouched (user ruling
only); TypedMap-65 rows untouched (h2-lane); TDG §S5 execution
untouched until its turn; NO new per-FQN pure entry points
anywhere ([[one-router-one-evaluator]] — the standing architecture
ruling survives the deferral); walls stay loud.

## 4AF. THE BURN PROGRAM (user go 2026-08-30: "start burning
## everything down") — slices, each gated, pins with attribution

Inputs: FULL_RESIDUE_CENSUS (rows + §6 order, §7 deepened walls),
the quarantine ruling, the wall-deepening landing (0845eeba).

**Slice Q — the quarantine partition (first, so `declined` becomes
the honest ACTIVE number).** A REGISTERED quarantine reason
vocabulary (the CanonDeclines register pattern — exact refusal
spellings the PRODUCTION system itself emits, never test-name
matching): the 7 no-scalar reflection FQNs' overload refusals,
the FunctionDefinition.expressionSequence property refusal, the
InstanceValue unknown-type refusal, the routerExtensions auto-map
refusal, the SQLNull-layout refusal, and the ONE-STAMP refusal
(census receipt: ALL 17 witnesses inside toPostgresModel chains;
the per-row decline witnesses keep repromotion observable — an
outside witness shows in the census immediately). v7Summary prints
`metamodel-quarantined=N` split OUT of `declined`; N EXACT-pinned;
scoreboard + census updated same commit. Partition, never test
exclusion.

**Slice B1 — plan-producer classification (the census's biggest
single unmasking).** PLAN-PRODUCING chains can never lower — a
plan is not a DB value — so an assert whose args pull
executionPlan / planToString / planToStringWithoutFormatting
(exact FQNs, resolution-backed — the SQL_PRODUCER_FQNS seam)
classifies into the sql-text partition BEFORE the dual-eval probe,
with a plan sub-reason. SQL-CONTENT predicates (sql() reads over
frames) keep the dual-eval path (slice-3 rule unchanged). Expected
movement measured-then-pinned: the getAll-76's executionPlan bulk
+ relationalExtensions 8 leave `declined` for text-only/plan rows;
TypedMap-65 rows that are plan-bearing may move too (measure,
attribute every row; h2-lane ownership transfers only with
receipts). declined shrinks, text-only grows, charter §8.0 scope
table + pins same commit. The BARE-2 sub-reason gap (census §3)
fixes here too (the plan path sets its outcome).

**Lineage-tree row channel, batch 59 (2026-09-04):** a new counted
verdict channel, `lineage-rows agree/disagree`, for `assertEquals(<tree
print>, $tree->relationTreeAsString())`: golden and ours both become
rows through one database query (LineageTreeVerdicts.TREE_ROWS — the
lineage referee) and compare as rows; a disagreement fails the assert.
The engine's decorated SQL aliases in join labels resolve to the tree's
own node names at parse (the engine's `alias = false` label form).
Census after landing: agree 66, disagree 0.

**§8.0 scope-table receipt, batch 58 (2026-09-04, the H2VERSION
decision):** assert-sql-text-only 24 → 17: the seven flipped
H2-compatible tests (three TDG alloy milestoning, two sqlstring
adjust-date, two businessdate) row-verify through the oracle SPI — the
H2 version probe answers the referee's own jar (2.1.214), the helper's
if-with-assert-branches adjudicates as a verdict. Lane move by
migration; sql-verdict disagree 0; dual-channel disagree 0.

**§8.0 scope-table receipt, batch 69 — THE DELETION (2026-09-05):**
unable-to-exec 8 → 9 — the walk's three text-only "verified" returns are
advisory (a byte-equal golden the referee could not replay, at two sites;
a contains predicate that held over our own generated text): one such
assert now counts in this lane's decline census instead of the walk's
verified total. exec-passing 9, text-only 15 unchanged; ratchet 179/2394
unchanged; no family baseline moved; disagree 0 both channels. Every
verified assert the walk reports is a row or value verdict.

**§8.0 scope-table receipt, batch 69c (2026-09-05):** exec-passing 10 → 9,
text-only 16 → 15, unable-to-exec 9 → 8 — one test, the datePeriods
testGroupByWithFilterFunction_noDatePath, left all three walk lanes at once:
its CSV value assert (exec-passing beside the declines), its index-less
toSQLString assert over the chained plan (text-only: statement 0 + the
engine's chained-plan warning, now stripped and routed to the calendar let's
rows), and its `sqlRemoveFormatting($res, 0)` assert ("column arity differs"
— now the let statement's own rows). Disagree 0 both channels. fetchDb
primary keys flipped without a lane move (a lowering wall, never a sql-text
row).

**§8.0 scope-table receipt, batch 72b (2026-09-05):** exec-passing 7,
text-only 15, unable-to-exec 9 unchanged; ratchet 176/2397 → 168/2405 (the
eight objectReferenceIn tests are platform JSON row verdicts; the walk's
ObjectRefs.java deleted); disagree 0 both channels. The SQL_TEXT_OUTCOME
channel is reset per assert (a stale plan-literal had moved one
DB2 sql-text assert into text-only with the run order).

**§8.0 scope-table receipt, batch 72a (2026-09-05):** exec-passing 9 → 7
(M1 rescued floor 9 → 7 in lockstep) — testBusinessDateInjectionFromVarReference's
two assertSameSQL asserts left the walk's lane: its statement-root map over the
two execute bindings unrolls (LiteralMapUnroll) and the whole test flipped, so
both asserts are platform-arm row verdicts now. text-only 15, unable-to-exec 9
unchanged; ratchet 179/2394 → 176/2397; disagree 0 both channels; the two
malformed `]"` graphFetch goldens are named engine-golden-defect rows
(malformed-json-golden), not divergences.

**§8.0 scope-table receipt, batch 69a (2026-09-05):** exec-passing 12 → 10 —
the forced-isolation pair (testQualifierWithOperation,
testTwoQualifiersWithOperation) left the walk's exec-passing lane: the
value-frame guard is gone and the forced golden's rows ('PeterTest' + three
'Test' — LEFT join of the isolated filtered subselect onto firmTable; pure's
plus over an empty operand) row-diverge from our 1-row INNER-joined frame —
an honest divergence of OURS (the walk fails them too; tests/advanced
re-baselined 66 → 64 by hand). M1 rescued floor 11 → 9: the union
sqlQueryMerging pair's text-divergent rescues cleared (both flipped —
^TDSNull() on the variant carrier). unable-to-exec 9 and text-only 16
unchanged; disagree 0 both channels. New ledger names:
`engine-golden-defect:alloy-adjust-widening` (columnValueDifferenceWithoutPrevalTest,
AlloyOnly; interpreter sibling prints the date) and
`referee-cannot-replay:no-fixture` (testProp3).

**§8.0 scope-table receipt, batch 68 (2026-09-05):** unable-to-exec 11 → 9 —
the two "graph keys mismatch golden aliases" declines
(testQueryOfMilestonedTypeWithFilterInMapping,
testQueryOfMilestonedTypeUsingLatestWithFilterInMapping) are ROW verdicts: the
bare-root serialize envelope projects the set's OWN property mappings
(ClassBinding.DeclaredKeys.ownProperties, stamped before the implicit
inheritance merge), so StockProduct's frame keys are the golden's
[id, name, type] + the pk/k_businessDate coordinates; the implicitly inherited
stockProductName/classificationType bindings stay demand-readable. exec-passing
12 and text-only 16 unchanged; disagree 0 both channels.

**§8.0 receipt, batch 67b — the `engine-golden-defect` bucket (2026-09-05, USER
RULING "quarantine/bucket those as engine bugs"):** four asserts whose GOLDEN
carries the engine's own departure from Pure's semantics; ours follows Pure;
the rows verdict truthfully FAILS and the ledger bucket names why. Register =
AssertLedger.ENGINE_GOLDEN_DEFECTS, EXACT test FQN → defect, consulted only when
the platform produced rows that differ (walls stay walls; passes never reach the
ledger). (1) `joinStrings-rendering` — engine relational rendering of
`joinStrings([a, b], sep)` is `concat(a, b, sep)` on EVERY dialect: the
separator trails instead of joining ('PeterSmith|' where Pure gives
'Peter|Smith'); the digest goldens are md5 OF THAT STRING
(ee0af362d8c1e4fa8c805dfeadd1aa37 = md5('PeterSmith|')) — so even the value
expectations encode the bug. Tests: testToSQLStringForTDSStringJoin,
testExtendDigest_Relational, testJoinWithExtendWithDigestOnColumnsOnBothQueries.
(2) `h2-week-start` — the engine renders firstDayOfWeek as
`date_trunc('week', x)`; H2 starts the week on SUNDAY under that call, Pure's own
dateExtension tests (dateExtension.pure:18-19) and DuckDB say MONDAY, as ours
does; the engine's H2 dialect extension does not compensate — an H2-dialect
defect of the engine, one database. Test: testToSqlGenerationFirstDayOfWeek.
testHashFunctions (traced 2026-09-05, batch 67c): its plus-column agrees, but its
`tds_digest` column is `joinStrings([...], '|')->hash(MD5)` and the golden renders
it `rawtohex(hash('MD5', concat(FIRSTNAME, LASTNAME, '|')))` — golden-only row
Anthony Allen: aceae941… = md5('AnthonyAllen|'), ours 0a8c4f1f… =
md5('Anthony|Allen'); the other five columns agree cell for cell, lowercase hex
both sides. Registered under `joinStrings-rendering`.

**§8.0 scope-table receipt, batch 67 (2026-09-05):** exec-passing
12 (14 → 12): the engine's two-statement in-list plan as ROW verdicts —
golden(0), the population statement of `let v = <to-many expr>` inside
the query lambda, verifies against that let's value (the rows leg
evaluates the let expression wrapped in the frame's mapping);
golden(1), reading `tempTableForIn_<v>`, replays with the temp filled
from golden(0)'s rows (the oracle's attempt-remembered population
golden, `SqlReplayOracle.TempTable` kind "population"); the exec-read
arm owns `sqlRemoveFormatting($res, n>0)` only for this shape. An
assert-free body with statements runs through the platform (a clean run
= zero-assert pass, the engine's own contract). +3 flips
(testInExecutionWithTempTableAndQueryChaining ×2, twoDBRenameColumns);
disagree 0. Ratchet 190/2383 → 187/2386. MEASURED, NOT BURNED: the
forced-isolation value-frame guard was lifted and put back — the
forced golden yields 'PeterTest' + three 'Test' (H2's concat treats a
NULL operand as ''), not droppable NULL rows, while the engine's own
value assert runs the default strategy; testToSqlGenerationFirstDayOfWeek
is H2's Sunday week start vs DuckDB's and Pure's own Monday
(dateExtension.pure:18-19) — a named dialect divergence, our answer is
Pure's; a driver argument that is neither an enum literal nor a runtime
is never assumed H2 any more (foreign-dialect residue).

**§8.0 scope-table receipt, batch 66 (2026-09-05):** exec-passing
14 (17 → 14), text-only 16 (17 → 16): the golden PLAN replayed node by
node — `SqlReplayOracle.verifyPlan` (harness PlanReplay behind the
SPI) runs a plan text's nodes in order: an Allocation's Constant
literal or Relational rows bind the later holes; the engine's own
template helpers evaluate by their published bodies
(relationalMappingExecution.pure: collectionSize, renderCollection,
varPlaceHolderToString, optionalVarPlaceHolderOperationSelector,
GMTtoTZ — PlanDateParameter's GMT→zone move; the `?replace` builtin);
the final node's filled SQL replays for rows. The arm binds collection
parameters (two referee elements) and scopes a plan lambda's leading
lets over our rows leg; the chained-TDG hop finder sees through
`sqlRemoveFormatting(String)`. +3 flips (testMapWithOpenVariable,
testExecutionPlanForQueryWithVariableRundateWithinLambda,
testQualifier); ROW verdicts, disagree 0. Ratchet 193/2380 →
190/2383; M1 rescued 14 → 11. Lane move by migration. NAMED residue:
testGroupByWithOpenVariableInAgg ×2 are FIXTURE-LESS plans (the engine
never executes them — SALES_GCS exists on neither side; text is the
contract); testIsEmptyOnCollection's golden is
planToStringWithoutFormatting (its SQL lost its spaces — not a
statement); testPlanForDateTimeVariableESTTimeZone's plan assert now
ROW-verifies (host=pass rows=pass) but its template-function-list
assert keeps the test on the walk; enumMap_* and
renderCollectionWithTz template operations are not modeled (named
declines).

**§8.0 scope-table receipt, batch 65 (2026-09-04):** exec-passing
17 (21 → 17): the four inline in-list temp-table tests
(testInExecutionWithTempTableFor{DateTimes,Dates,Numbers,Strings}) left
the walk's lane — the engine's `tempTableForIn_N` holds the query's
`in([...])` literal; the platform arm reads the literal off the frame's
typed query (SqlTextVerdicts.inListTemps: one inline in-collection, one
numbered temp in the golden) and hands the oracle a `TempTable` spec in
Pure terms; the oracle materializes the H2 temp (ReplayOracle.tempSeeds
— the walk's literalTempSeeds) as per-verify statements before the
replay. ROW verdicts (disagree 0). Ratchet 197/2376 → 193/2380; M1
rescued 18 → 14. Lane move by migration. NOT burned, by design: the
population-golden temp (`tempTableForIn_<var>`, 2 tests) is the
engine's two-statement plan against our one-statement plan — golden(0)
has no counterpart statement; the forced-isolation VALUE-frame goldens
(2) pin an engine debug strategy whose rows differ from the observable
value (H2Verify FORCED_MECHANISM: 4 rows incl. NULL-minted vs 1); the
graph-keys mismatch (1) is our frame fetching properties the golden
never selects — our bug, kept loud.

**§8.0 scope-table receipt, batch 64 (2026-09-04):** exec-passing
21 (55 → 21): the ten chained testDataGeneration tests left the walk's
lane — the platform arm's chained-fetch verdict
(`SqlReplayOracle.verifyFetchChain`: the hop addressed by its
`$testData.sqls->at(i)` index and the let-bound generator node; the
oracle remembers each hop's golden for the attempt, materializes the
ancestor `testDataGen_Temp_<T>` temps from those goldens root-first,
runs the hop's golden and multiset-compares the hop's transcript rows,
the generator re-run under a byte-exact text receipt) replaced the
walk's tdgChainedVerify for them: 34 fetch-text asserts are ROW
verdicts (sql-verdict disagree 0; dual-channel disagree 0). Ratchet
207/2366 → 197/2376. Lane move by migration; the walk's own chained
arm is now reachable only by testQualifier (its hop-0 golden spelled
`sqlRemoveFormatting('literal')`).

**§8.0 scope-table receipt, batch 57 (2026-09-04):** exec-passing
55 (57 → 55): the two flipped hybrid-milestoning union tests (the
`repeat` native) left the walk's lane; their sql-asserts row-verify
through the oracle SPI (sql-verdict agree +4, disagree 0; M1 rescued
54 → 52). Lane move by migration.

**§8.0 scope-table receipt, batch 56 (2026-09-04):** exec-passing
58 → 57 and the walk's M1 text-match lane 1 → 0 (RETIRED, pinned
exactly empty): the last walk-lane test, testLessThanFilterAsVariable
(a let-bound lambda in filter position), flipped to the platform arm
and its sql-assert row-verifies through the oracle SPI (sql-verdict
agree +1, disagree 0; dual-channel disagree 0). Lane move by
migration, never lost verification.

**FIRST BURN ROW LANDED — the withMapping fix (2026-08-30, rode
this batch):** `->withMapping(M)[->cast]->from(runtime)` now slots M
as THE from-mapping (FromChecker strip beside the
withChainedMappings idiom; real spec mappingExtension.pure:386) and
the runner's mapping DISCOVERY learned the spelling (withMapping's
param 1 is a mapping ref — without it the module compiled unseeded
and the try-run wall said "table does not exist", masking the real
gap). RECEIPTS: testFromWithMapping +
testFromWithMappingAndIntermediateFuncCall SHAPE → PASS; corpus
2,356 → 2,358; agree 3,381 → 3,383; exec-passing 1,495 → 1,497
(golden SQL executed + row-verified); h2 advisory registry 67 → 69
(the two tests' sql() side rows join their siblings'
TypedUserCall[mapping::sql] vocabulary class — verification gained,
advisory gap merely gained its members). Zero regressions in every
other family. POLISH ROW noted: the try-run lane's wall for a store
query with no discovered mapping should say so, not "table does not
exist".

**B6 FIRST HALF LANDED (2026-08-31):** TDG chained-fetch live-session
refereeing — the whole TDG unable residue (29, incl. the "26+2")
executes and row-verifies; exec-passing 1497 → 1526, unable 50 → 21.
Full landing record: TDG charter §S5-L (platform Fetch transcript +
ancestor-golden mirror synthesis + the concatenate root-sort platform
fix + the sqlRemoveFormatting golden fold). Emission-anatomy byte
parity remains §S5's future leg.

**Then, in census-§6 order, each its own gated slice:**
B2 getAll RESIDUE (rows still declined after B1 = the true
resolver leg: parameterized non-plan shapes — legacyNullUnsafe,
m2m2r, singles; census the survivors first); B3 host-unsupported
registration gaps (~12 mechanical: plan-node/metamodel property
rows + JSONArray json-node family from §7); B4 small ports/bugs
(col() overload gap, withMapping lowering, resolver defects 2,
trailing-JSON 2); B5 plan walls 7 + mapping walls 4 (family
census first); B6 sql-exec lane (TDG §S5 26+2, then
emission-anatomy 7). Frozen: quarantine (rides Slice Q), h2-lane
walls, disagree-9.

## 8. PLAN OF ATTACK — the batch-2 remainder → cutover (handoff,
## 2026-08-28)

**State on entry**: batches 1 + 2.1–2.3b + the §4Q eviction are
EXECUTED and pushed (..ec9f6fe8). Census: agree 2,650 / disagree 8 /
declined 2,583 (partition NAMED: sqltext 961 + tdg 123). Scoreboard
byte-identical throughout. The census console is the work list:

```
mvn -pl core test -Dtest=RelationalCorpusRunner \
  -Dlegend.engine.root=/Users/neemsandv/legend/legend-engine \
  -Dlegend.pure.root=/Users/neemsandv/legend/legend-pure
# read the [v7] lines; scoped probes: -Drcorpus.only=<family>
# (scoped runs never write the scoreboard)
```

## 8.0 SCOPE TABLE — the ratified denominator (2026-08-28, user
## sign-off; measured from the baseline full sweep at 9958c040)

**§4AF UPDATE (2026-08-30, Slice Q landed; B1 built, measured and
REVERTED):** live lane numbers: exec-passing 1,495 / text-only 44 /
unable-to-exec 50 / csv 0 / **metamodel-quarantined 142** (Slice Q,
exact-pinned, the deferred metamodel-as-data program's rows) /
declined 181. B1 (plan-producer classification) was reverted on a
user catch — its flat `plan-producer` sub-reason flattened 141
reason-diverse rows into one coarse label, WORSE bucketing than the
walls it replaced. Its MEASUREMENT is permanent knowledge
(FULL_RESIDUE_CENSUS §8): of the 181 active declines, 141 are
PLAN-BEARING asserts (all 76 getAll walls, the 8
relationalExtensions, 55 of the TypedMap-65, the 2 in-plan resolver
messages) whose burn belongs to the plan-text LANE with per-shape
sub-reasons, landed WITH that lane's fix — classification rides the
fix, never precedes it again. True non-plan decline residue: 40
(TypedMap 10, host-unsupported 26, JSONArray 2, trailing-JSON 2).

**§4AF UPDATE (2026-08-31, SLICE 1 LANDED — census §10h/§10h-addendum,
all 8 gates green):** job-1 threading fixes (5, main tree) killed the
runtime-candidate fallback estate-wide (90 firings → 0); the Runner
lost its guessing layers (−704 lines: name-scan, try-run/SHAPE gate,
module DDL, conflict router, crossRefs, preflight + setup-universe
module); per-package workspaces are the ONLY topology (G4 63s, was
~255s). Lane numbers: exec-passing **1526** / text-only 44 / unable 21
(2026-08-31 chained-fetch live-session refereeing — census §10o leg 1,
TDG charter §S5 landing record: the TDG 29 all row-verify)
/ csv 0 / declined 181 / **metamodel-quarantined 107 witness rows +
20 wall tests** (CHANNEL MOVE, same partition: with the try-run lane
deleted, the toPostgresModel family fails at the TEST level before
per-assert adjudication; counted through the same vocabulary via
CanonicalDivergence.noteWall; h2 floor 1347→1329 and walls 983→993
carry the same family's host-adjudicated h2 "passes" — worktree
receipt in §10h-addendum; DuckDB family baseline sqlDialectTranslation
21→1 pass / 20 error, same move).

**§4AF UPDATE (2026-08-31, PHASE-B LANE 1 CLOSED — getAll-76):**
user go: "Let's burn it all down to zero." The lane's ONE design leg:
the executionPlan/preval call is an OPAQUE PLAN HANDLE — Phase-H
store resolution neither descends into it nor demands a bound chain
(StoreEscapees skips the subtree; the plan lane compiles the lambda
at consumption under the call's own mapping and plan parameters), and
plan-text consumption works in ANY expression position
(StatementExecutor.planTextRewrite: planToString/
planToStringWithoutFormatting chase let-bound plan handles and splice
their computed text as literals; shapes the plan lane cannot print
KEEP their current classification — the rewrite is monotone). ONE
process bug caught by the referee mid-slice (immutable inlineBody
list under replaceAll — executionPlan family 74→0→74). Estate:
**agree 3383→3469 (+86), declined 181→95 (−86)**, every other lane
pin byte-identical, `store resolution left getAll` = 0 estate-wide.
Remaining declines: TypedMap-65, host-unsupported 26, plan-literal
17, plan-let 6 (+ sql-text sub-lanes unchanged).

**§4AF UPDATE (2026-09-01, PLAN-CHAIN STAGING — wall-exec burn):**
NativeDispatch grew the plan-chain arm: a SCALAR-typed chain over the
opaque plan handle (`$plan.rootExecutionNode->allNodes(..)->filter(..)
->cast(@X).prop`) evaluates through the executor's planWalk where it
stands and re-enters as a compiler-minted literal — the planToString
rule generalized from one whole-call spelling to every walk-ownable
chain, wired at BOTH staging sites (the statement loop and evalValue,
the assert-side pipeline). A walk refusal — null OR a thrown refusal
(open-variable predicates under walkFilter) — is a DECLINE: the chain
keeps its ordinary path and that path's walls (planWalkDecline; the
throwing first cut regressed 4 open-variable executionPlan tests,
caught by the scoreboard guard and fixed same-slice). Lane counts
byte-stable (text-only 44 unchanged); metamodel quarantine 172→116
(56 quarantined plan-node reads now evaluate through the chartered
staging seam — no new walk vocabulary, unlike the reverted side-door
attempt). Flip census: TypedMap-42 burns to zero — 32 tests flip, 10
land on attributed next walls (stamp-invariant, mapping-resolution,
plan-walk vocabulary, plan-text platform-fails).

**§4AF UPDATE (2026-09-01, ROW-13 ADJUDICATION BURN — SQLTEXT charter
§6.1 slice 0):** the "row-cardinality skew (distinct rows agree)"
decline was adjudicated (no lowering bug — our SQL carries zero
DISTINCT; the engine's default emission fans unfiltered join legs,
7x the same pk-stamped instance for testQualifierQueryWithOr, unpinned
by its own asserts) and the arm DELETED. Instance frames verdict via
the graph compare's EXTENT_SUBSET golden-side pk-collapse
(direction-safe: our side never collapses); value/tabular duplication
differences now diverge loudly (pure preserves duplicates there).
Lane numbers: exec-passing **1527 → 1528**, unable-to-exec **21 → 20**
(diff-noreplay loses its skew row), text-only 44 unchanged; verdict
roster gains golden-fanout-collapsed x1; soft-pass flags move WITH the
test (text-rescued 900 → 901, sqldiff 13 → 12, advisory 15 → 14 — the
same one pass re-attributed).

Plain reading: an "assert execution" is one assert statement judged
once during a full corpus sweep. Of **5,241** total:

| bucket | count | plan |
|---|---|---|
| sql/plan-text compares (assertSameSQL family + CONTENT-classified sqlQueryToString-family args, §4U census split; §4Y run/gen split: **run 1,491 / gen 38**) | 1,529 | **OUT of the migration by design, permanently.** Already end-state: text match → H2 row-check (320, 0 diverged); text differs → engine's golden SQL executes on H2, rows vs our rows (632 verified, 0 diverged); unverifiable → advisory, counted by reason (145 — LEG 8 burns these); 394 run-backed asserts on their own compare arms (h2Compatible 322 + mixed) — reconciliation = named follow-up. **§4AB-§4AC update (2026-08-28): exec-passing 1,385 / text-only 44 / unable-to-exec 97 (diff-noreplay 71, match-noreplay 8); M1 455 matched + 880 rescued, 0 diverged, 11 unverifiable. §4AD slice-1 batch-1 update (2026-08-29, topology-gated preview — that mechanism was DELETED; its numbers were reference points): exec-passing 1,390. §4AD BATCH 5 update (2026-08-29, THE ROUTER FLIP, landed): exec-passing 1,396 (+9 over the a618c5d2 floor 1,387 — testQualifierWithIsolation, a baseline ERROR, lifts and row-verifies; the filter-mapping overlap pair; six wrapper/hop-rich projection qualifiers via first()/head() unwrap + per-occurrence bundling), M1 457 matched, dual-channel agree 3,310 (+39) / disagree 9 (=). fnlr's TOP-LEVEL value dispatch DELETED (a loud wall guards route totality); surviving fnlr = nested instance-scoped + filter-position reads (batch 7 / the nested-material leg, census-attributed)** |
| TDG/test-data-gen text compares | 123 | OUT by design, permanently (host artifacts) |
| host-unsupported forms | 28 | name-by-name adjudication (leg 6) |
| **adjudicating through the production verdict path today** | **2,658** | 2,650 agree + 8 named wire-fidelity disagreements (leg 5) |
| resolver: class query under wrapper | 63 (was 614 — §4U: 513 were sql-text content, 33+30 TypedMap remain) | leg 4 |
| resolver: getAll/call shapes | ~175 | leg 4 |
| flat-cells / tabular sides | 353 | leg 1 (grid canon — fusion-spike F2 proved the SQL) |
| JSON family typing | 163 | legs 2–3 (arm exists; Result<T\|m> typing + 2 natives) |
| lowering gaps + tail | 166 | leg 6 |

Cutover acceptance re-stated against this denominator: disagree 0 and
declines == exactly the two BY-DESIGN rows (961 + 123) + adjudicated
residue. RATIFIED DESIGN for every remaining leg (fusion spike,
[V12_FUSION_SPIKE_2026_08_28.md](V12_FUSION_SPIKE_2026_08_28.md), user
sign-off across four rounds): comparison policy chosen at COMPILE TIME
from static types; host-executed today, emittable tomorrow as ONE
statement per test body (lets = `WITH ... AS MATERIALIZED`, asserts =
plain verdict columns, evidence side-tagged in the same result set;
first-failure = diagnostics not semantics — split rung is the
error-diagnosis fallback only; JSON rides the byte channel via
canonical sorted-key EMISSION; literals always inline — no
unspellable class, stringLit splices chr(0)).

**Leg order (each leg: witness → implement → full sweep → guardrail
battery → allgates → push):**

0. **Lane-classification guard (with the scope table, this slice).**
   The sqltext/tdg partition counts pin EXACTLY in the corpus runner
   (shrink-or-justify), and the h2-exec `diverged` counter pins 0 —
   an assert can never silently change lanes, and a replay divergence
   can never pass silently.

1. **Flat-cells compare (353 declines) — DESIGN SUPERSEDED
   2026-08-28 (user ratification after the fusion spike): lands as
   the GRID-CANON EXTENSION of `wrapWithCanon`, NOT a host-only arm.**
   `wrapWithCanon`'s 1-column decline is the whole blocker (spike F2);
   the extension: per-cell leaf spelling (LiteralSpelling) joined by a
   separator no cell can contain (chr(31)) into a per-ROW canon text;
   the row-canon multiset (sorted-list equality) is the byte verdict;
   NULL cells spell the golden's sentinel convention at the canon
   (COALESCE → 'TDSNull', direction preserved at compile time — spike
   R2-3); an explicit `->sort()` side orders by PURE'S TOTAL ORDER
   (kind-rank + typed value), never canon text (spike R2-1). The host
   cell lattice (`GridCompare.rowTupleMultiset`, engine semantics:
   column names OUT, cells row-wise, cross-row shuffles FAIL — audit
   9) stays as the PARALLEL REFEREE, exactly the scalar channel's
   dual-verdict shape. The failure message derives from the SAME
   judgment that failed — the reverted attempt printed the
   byte-verdict text for judgments the byte channel never made (its
   28-row phantom: judgment and message from different lattices with
   the probe unfired; the committed tail couples message⇔probe, the
   arm must too).
   Historical findings (the reverted attempt, still true):
   **ATTEMPTED AND REVERTED 2026-08-28 — findings for the retry
   (an attempt was measured then rolled back at ec9f6fe8; nothing
   half-understood was pushed):**
   - The TYPER COLLAPSES the trailing `TDSRow.values`: the typed side
     for `$r.values.rows.values` is a `rows` PropertyAccess over the
     values read — detect the flat-cells shape as a `rows` read at the
     side root (both property-access and call spellings), NOT as
     values-over-rows.
   - The CANON-RIDER execution changes the RESULT KIND (a rider-free
     probe returned Tabular; the ridered fetch did not) — the arm must
     fetch rider-free and skip the byte channel entirely (decoded
     cells have no canon; count a named decline).
   - A working shape: dedicated arm before the order-view path —
     rider-free evalValue both sides, flatten Tabular to cells (the
     harness Eval convention), ordered exact then rowTupleMultiset
     under INCIDENTAL view; witness pinned ordered/row-swap/cross-row-
     shuffle (shuffle must FAIL; its failure message is PureAsserts'
     "expected:" text, not the word "assert").
   - Measured outcome: declines 2,583 → 2,230 (−353), agree → 2,959,
     sweep GREEN — BUT disagreements 8 → 52. The +44: ~15 TDSNull
     null-cell spelling rows, ~9 order/cohesion variants, and a
     28-row class of "byte-verdict: canonical renders differ (host
     lattice agreed)" whose accounting was NOT understood — full
     witness list preserved in V7_FLATCELLS_ATTEMPT_CENSUS.md (the
     sql-verdict disagree counter stayed 0 while the message claims a
     divergence — reconcile the arm's `equal`-vs-message-lattice flow
     before trusting any of it). DIAGNOSE THE 28 FIRST; do not push
     the leg with an unexplained class.
   Watch: PCT G7/G9 ledger movement and the chB canon residue counts.
2. **Result typing (~175, unlocks the JSON family).** The engine's
   `Result<T|m>`: `execute(q,...).values` types as q's element type
   and multiplicity (a serialize query → `String[1]`); ours stamps
   `[*]`, so strict signatures (assertJsonStringsEqual) reject BEFORE
   any arm. Fix in the TYPER where the execute call's return/values
   read is stamped — engine-parity typing, not verdict work.
3. **Two JSON natives (small; pairs with #2).** `parseJSON` +
   `equalJsonStrings` registry natives (real signatures verbatim —
   engine-core corefunctions; the eviction's §4Q pattern). The
   assertJsonStringsEqual verdict ARM already exists (JsonCompare).
4. **Resolver legs (census-first; the deep one).** class-query-under
   ~610 + getAll shapes ~175. FIRST: group the sweep's decline
   details by wrapper node and split out the sql-text-family rows
   hiding in the 610 (they belong to the §2 partition). THEN
   per-shape StoreResolver arms, biggest bucket first. Large enough
   to charter its own slices.
5. **Wire-fidelity fixes = the 8 disagreements (§4N).** Decimal SCALE
   at emission (X2's doctrine — never re-blur the judge); temporal
   NINE-DIGIT decode (the engine fromSQLTimestamp convention, at the
   wire temporal seam); then the two phantoms (sort-tie order policy,
   TDSNull row-string spelling) — adjudicate with the user if a
   mechanical fix doesn't fall out.
6. **host-unsupported 28 + tail**: name-by-name adjudication (§2 rows
   or feature rows).
7. **H2-replay unverifiable burndown (145)** — user-ratified as its
   own leg 2026-08-28 ("so we actually do that"). The sql-text
   family's row-verification oracle currently declines 145 asserts
   (census by reason: non-tabular result frames 452-class dominant,
   no-root-exec-variable, array-literal dialect gaps, non-lambda
   toSQLString shapes, enum-decoded columns). Each fix converts an
   advisory pass into a ROW-VERIFIED pass; target: unverifiable → 0
   or a named, user-adjudicated residue. Independent of the cutover
   (advisory channel) — schedulable any time.
   **D3 PIN (arch-audit 2026-08-28, user-ratified): the cutover's
   DELETION LIST is an acceptance criterion, not an intention.** The
   dual-referee period's deliberate harness mirror dies with batch 3,
   enumerated: `checkAssert`'s comparison lattice, `goldenEqualScalar`
   + the golden temporal-decode arms, the harness `compare()`/`Eval`
   leniencies, the harness `endsInSort`/`orderView` duplicate, the
   harness rendered-form recognition (its `renderForm` twin), and
   `isFlatCellsRead` — each with its shrink pins moved (dated
   justifications). A batch-3 slice that flips the verdict of record
   WITHOUT this list deleted does not merge.

8. **BATCH 3 — the cutover (one slice, only at disagree 0 and
   declines == §2 partition):** SQL verdict becomes the verdict of
   record; DELETE `checkAssert`'s comparison lattice,
   `goldenEqualScalar`, the golden temporal-decode arms, `compare()`/
   `Eval`'s leniencies (shrink pins move with dated justifications);
   re-anchor 0-assert accounting (27) + softness attribution; the
   dual-verdict alarm stays armed permanently. Acceptance: scoreboard
   IDENTICAL, full chain green, push. Then V12 (fusion) and V13 per
   PROGRAM_MAP; indexOf/substring stays parked behind V13.

**Standing traps for the next session** (all bitten this session):
run the GUARDRAIL BATTERY (JavaEvalLedger, JdbcSurfaceCensus,
CodeShape, ErrorShape, HarnessDiscipline, ArchitectureTest,
NativeFunctionTest's golden catalog) BEFORE launching a chain — six
register/golden bumps tripped chains; ZERO repo writes while a chain
runs (launch only after the slice's last write); roots are -D
properties at the /Users/neemsandv checkouts; the reference-checkout
INVARIANT (AGENTS.md): checkouts feed the thing UNDER test, never the
thing judging — `registerLibrarySource` enforces it.

## 5. Witnesses (before behavior, where possible)

1. Per-form verdict unit witnesses beside AssertVerdictsTest for each
   corpus form it newly exercises (sameElements order key, TDS
   sentinel rows, tolerance, JSON canonical).
2. The dual-channel census itself is the leg's primary witness: batch
   1's disagreement list IS the spec of batch 2.
3. Regression: the 27 zero-assert tests still report 0-asserts; a
   deliberately-broken assert still fails (polarity witness — the
   verdict lane must never rescue a truly failing test).
4. `ExtendCheckerTest`-style pins for the splice: an assert whose
   args read an execute handle adjudicates identically pre/post.

## 6. Traps (recorded now)

- `checkAssert` has MORE THAN ONE call site (`runPerDriverLoop`,
  `AssertLoopForm`, `RuntimeIfForm` re-entries) — batch 1 starts with
  a call-site census; the dual channel must cover every one.
- The G8 fixture scanner adjudicates test-tree string literals —
  no Pure-shaped assert messages in new tests (metamodel-leg trap).
- Full `tools/allgates.sh` per batch, caffeinated, tree FROZEN;
  12-min P0 ceiling; `mvn -o -pl core install` before hand-run pct
  lanes; corpus doc regeneration rides G4 — commit it with the batch.
- The pct/corpus ROOTS are `-D` system properties (env vars IGNORED)
  on every hand-run.
- H2 advisory channel consumes our DuckDB rows — verify the re-route
  leaves its feed intact (it reads results, not assert outcomes).
- **Dual-phase double execution**: batch 1 runs each assert's sides
  TWICE (host path + verdict path; ~+1 s at 0.26 ms/query — fine).
  Consequence: any stateful or nondeterministic assert side (sequence
  reads, unordered limits) surfaces as a PHANTOM disagreement — that
  is the census working, not a verdict-construction bug; adjudicate
  such rows as nondeterminism (order-key or setup fix), don't chase
  the verdict SQL.
- Batches 1-2 must not touch production files' behavior for
  NON-corpus lanes: AssertVerdicts changes ride behind witnesses and
  the PCT suites (G6/G7) pin the existing dual-verdict behavior —
  any [canon]/sql-verdict census movement on the PCT lanes is a
  regression, not progress.

## 7. Out of scope (name them if tempted)

V12 round-trip fusion and V13 whole-function/let-IS-WITH fusion
(sequenced AFTER this leg; V13 reuses this leg's verdict semantics
wholesale). The sql-text/TDG host partition (§2). The
indexOf/substring seam (parked behind V13,
INDEXOF_SUBSTRING_LANE_CENSUS.md §7). Prepared statements (LAST,
standing ruling).
