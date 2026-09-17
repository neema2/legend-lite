# THE PROGRAM MAP

One document holding the WHOLE plan — the four ratified buckets, the
work they didn't name, the audit intake, and the longer arc — so "what
is the plan" is a thirty-second read here, never a chat archaeology.
Ratified 2026-08-23 (user review). Maintenance rule: this map names
programs and points at their charter docs; per-item state lives in
[OPEN_REGISTER.md](OPEN_REGISTER.md) — a program that closes updates
BOTH in the same commit.

Standing invariants that govern everything below: tenet #1 (Java
orchestrates, the DATABASE executes), single compiler with dialect
strategies, engine pure as the semantic spec (pinned to the
INTERPRETED lane — see audit intake #A5), asserts are verdicts always,
no exclusions — only named ledgers.

---

## BUCKET 1 — F10: the kind-faithful carrier (finish 100% first)

Charter: [F10_CARRIER_DESIGN.md](F10_CARRIER_DESIGN.md). Spelling-as-tag:
pure's literal grammar IS the kind tag; the LITERAL wire label splits
the carrier from genuine Variant's raw-JSON contract; one grammar owner
(`lowering/LiteralSpelling` SQL-side, `values/LiteralText` host-side).

| Slice | State | Content |
|---|---|---|
| 1 — one grammar owner | **SHIPPED** 079ebfda | three divergent copies collapsed, byte-identical |
| 2 — mixed collections | **SHIPPED** 13a3308a | LITERAL label lands; mixedSort FLIPS TO PASS; declines 1→0, ceilings pinned 0 |
| 2b — selections + sniffer death | **SHIPPED** d3fd1a88 | min/max/least/greatest/mode marked; `latticeKind` DELETED BY CENSUS (zero firings, both lanes) |
| 3 — Any positions migrate | **CLOSED — M4 RE-LAND EXECUTED 2026-08-25** (M4_PRELAND_CHARTER §4R landing record) | 3a 2026-08-23 (b1cfdd97): temporal grammar joined the carrier, #A1's subsecond strip DIED. 3b 2026-08-24 (bc8ba85a→3b251fc9): total two-carrier readers, lane flag, unspell-one-side equality, assumption audit (decodeAny gains NO carrier arm — spelling decode is LABEL-driven only, the LITERAL arm in unwrap). The hetero-LITERAL claim re-landed 2026-08-25 with ZERO of its four park compensations (each became a typed-IR capability: LambdaBinding conventions, stored facts, COMPARATOR_NATIVES, uniform() ERROR skip); the three residual view rows PASS via elementText/printForm; post-landing adversarial audit §4A done. `wip/slice3-claim-on-untyped-ir` is a superseded park artifact |
| 4 — consumer decay | **CLOSED 2026-08-27** (charter [ADAPTER_NECESSITY_CENSUS.md](ADAPTER_NECESSITY_CENSUS.md), four gated batches bc9ab622→) | Re-scoped mid-slice by user directive: derive the MINIMUM adapter from the typed surfaces, trust no audit (§5b). Cargo: nine Java arms dead by measurement, parseDate conforms by emission (the one narrowing witness cured platform-side). R2: four pure arms dead, fallbacks loud, Essential floor 295→297. R1: discovery = the pure-side M3 walk, all five regexes deleted by differential (which also caught the shadow-Pair injection bug + two collector bugs pre-landing). Fold-ins: D91 (equality.Key armed on every lane) + D94 (diamond layout ≡ findProperty). Admissibility row: already retired (77af0f37→3c353706, receipt in census §4); computed-mixed = the ceiling-0 mixedNumericKinds ledger row. Residue named in census §5 (keyless value-lane equality; Gap A transport). THE TRUTHFULNESS BURN (census §5c, user-directed 2026-08-27) then closed the purist list: scaffold dead (bare storeless execution + connection-derived dialect), shape decisions platform-typed (Java builds real List/Map instances), error PROVENANCE (U+001F sentinel + ONE RaisedErrors owner — the adapter arm AND the platform's broad strip both deleted; native errors surface whole, the 4 BigNumber pins now byte-match upstream's manifest), verbatim injection (decoration regexes dead), documented conventions + loud walls throughout, and the three-file split (PctExecuteNative/ModelPacker/ValueBridge). P7 is the ONE surviving declared-type read — chartered to T4 with the mechanism sized. |

## BUCKET 2 — PCT burndown (after F10)

Current: **1,107 / 1,118** (honest universe 1,117 — audit #A9's testGet
double-count). Relation 355/355, Standard 204/204, Unclassified 95/95,
Essential 316/327, **Grammar 137/137 — the FULL grammar lane**. THE
WINNABLE SET IS BURNED (2026-08-27, eight gated batches
183aeb33→1d71da33; landing record in
[CHANNELB_BURNDOWN_HANDOFF.md](CHANNELB_BURNDOWN_HANDOFF.md) §0):
every remaining Essential row is ledger (below).
testPlusInIterate LANDED 2026-08-28 (fold-strategy closure — the
cross-tree-binding bug; Grammar 136/137). getAll::testBasic LANDED
2026-08-28 — the METAMODEL STORE leg EXECUTED
([METAMODEL_STORE_HANDOFF.md](METAMODEL_STORE_HANDOFF.md) §10 landing
record: the metamodel lives IN the database — Class.all() = SQL over
the seeded metamodel.classes table through the ordinary store lane;
BOTH channels pass, the channel A ledger entry removed by its own
loud-fail design).

**The ledger boundary (not burnable):**
- 5 indexOf/substring rows — user-adjudicated IRREDUCIBLE (register A1:
  1-based indexing is real core_relational pure semantics; a reverted
  draft proves the trap).
- 4 adjustBy*BigNumber rows — **FORMALLY ADJUDICATED 2026-08-27 (leg
  8)**: dates beyond DuckDB's physical range; the reference DuckDB
  target fails IDENTICALLY, and after the truthfulness burn's B7
  provenance work our four pinned error texts BYTE-MATCH the reference
  manifest's own enveloped texts — the divergence is the backend's
  physical range, not our pipeline. Ledger.
- 2 sort rows (testSimpleSortWithKey, testSimpleSortWithFunctionVariables)
  — **ADJUDICATED, user sign-off 2026-08-27 (ledger 9→11)**: independently
  re-diagnosed by the burn session AND the dossier branch
  (docs/channelb-dossiers leg5) as derived members of the register-A1
  substring family — the key/comparator machinery is correct (our
  actual IS ascending-by-1-based-substring-key); interpreted substring
  is Java 0-based. Not an ordering bug; not winnable while A1 stands.

**The winnable 25, by leg:**
| Leg | Rows | Notes |
|---|---|---|
| assertError source positions | 6 | DB errors carry no line/column; error-shape channel threads positions |
| higher-order match / function-value params | 6 | match-with-functions ×4 + comparator fn-ref resolution ×2 |
| toString over instances | 3 | + one overload-ambiguity row |
| sort-with-key / removeDuplicates order | 3 | comparator-with-key ordering; first-occurrence order over mixed |
| fold | 2 | **F17 diagnosed**: `+=` parsed but DROPPED at NewChecker.checkCopy (desugar to concatenate(receiver.prop, v)); unset to-many fields emit untyped NULL |
| grammar residue + parseDate | 5 | |

Plus the audit's SILENT singleton bugs (outrank several rows above —
wrong VALUES, not errors): `['ACTIVE']->contains('TIV')` → true
(substring match), `['abc']->indexOf('b')` → 2; and Part-1 residuals
(filter grows a set, `[]->map` stamp trip, `1/0` → Infinity, `times()`
→ Float, `^new` missing required [1] → null, `Box<Integer>` match arm,
partial cast()).

## BUCKET 3 — the adapter shrink (formally F10 slice 4)

The question ratified: is ExecuteLegendLiteQuery + pct-adapter the
MINIMUM set to run reference PCT without compensating for the platform?
Method: the necessity-proof census (the same audit form that killed the
host-logic arms — every arm proves itself against our tenets or dies).
Register F16 already predicts the decay ("adapter receives kind-faithful
values and ONLY boxes; declared-type consult arms decay as F10 lands").
Clause 2b doctrine governs what moves INTO the platform. Audit finding
#A7 (F15 shadow-parser regexes are Channel A's alone) bounds the scope.
Charter: [ADAPTER_NECESSITY_CENSUS.md](ADAPTER_NECESSITY_CENSUS.md)
(opened 2026-08-27 — the per-arm verdict table; records that most of
PCT_AUDIT §7's "free wins" were already executed by the F5.x program).

## BUCKET 4 — single-shot asserts (V12/V13, the thesis validator)

Notes NOT lost — register §1:
- **V12** one round trip per assert: side-tagged UNION ALL, per-side
  typed value columns NULL-padded (no promotion erasure — the LITERAL
  carrier is the prerequisite, now real), literals INLINE, tunnel rung
  fused→split→bare→fold. GO/NO-GO: TimingLedger query.exec share.
- **V13** whole-function fusion: *let IS WITH* (materialized CTE =
  evaluate-once; dissolves within-test F13 identity), asserts as the
  verdict OVERLAY (the graphFetch→serialize species), verdict table
  out, typed list() evidence columns. HAZARD recorded: eager CTE vs
  pure first-failure sequencing → fusion-gradient tunnel. Sequenced
  after V7 (perturbs the golden-text lane, like prepared statements).

---

## THE WORK THE FOUR BUCKETS DIDN'T NAME

| Program | Where | Note |
|---|---|---|
| **T4 nullability** | register 2b | **CLOSED 2026-08-26** (charter §4bZ-V E + D): the 6,472 pad rows machine-counted 100% literal NullLit (N0), slot truth declared at construction (N1: a projected literal NULL is nullable — reconcileLabels, one owner; the wholesale-flip framing retired), EQUALITY-0 pinned on all four lanes (N2). The VALUE-level decode tripwire landed (D1) and CAUGHT a real bug on sweep one: SqlUnion's ctor dropped the mapping-seam tag — cured by union-label reconciliation; int-or-null settled to proven-empty (56/219 ceiling-pinned), wire-unknown EQUALITY-0 |
| **The builder leg** (label-at-construction) | register T3/T4 | HUGEINT adopt-pending 108/130 (contract widens per testLargePlus; labels derive from the RELATION SCHEMA, needs builders not expr-sniffing) + Number-erasure wire residue (~73 PCT / ~179 corpus) |
| **In-SQL equal() third lane** | audit #A2 | the X1–X4 treatment or three-lane pinning (see audit intake) |
| **V8/X6 2-ULP retirement** | register | `ulp-policy=0` across the whole corpus = the evidence it waited for; same-arithmetic H2 referee design briefed |
| **V7 corpus-lane cutover** | **CHARTERED 2026-08-28** — [V7_ASSERT_VERDICT_CHARTER.md](V7_ASSERT_VERDICT_CHARTER.md) (census: [V7_ASSERT_VERDICT_CENSUS.md](V7_ASSERT_VERDICT_CENSUS.md)) | THROUGH §4AD (2026-08-29): dual-channel live (agree 3269/disagree 9), sql-oracle burn slices 1-4 EXECUTED — exec-passing 990→1385, unable-to-exec 492→97; REMAINING: (a) burn the 97 to a named floor (~10-15: both-ours 5 text-is-the-contract, predicate-diverged 6 pinned, tempTableForIn 4 attempt-emulation), (b) backlog 323 (getAll ~175, TypedMap, expressionSequence 43) + disagree 9, (c) the two legs below |
| **Relational-conformance leg (row algebra)** | **RATIFIED 2026-08-29** — charter §4AD | class-query navigations compile to the ENGINE's row algebra (left-join fan-out, no dedup, no scalar subqueries); boundary = PROVENANCE (pure-born values keep pure semantics); census-first, then map/select → filter/qualifier → unpark the oracle's 45 |
| **Verify-arm consolidation** | [VERIFY_ARM_AUDIT_2026_08.md](VERIFY_ARM_AUDIT_2026_08.md) | 34 arms censused (8 COMP + 1 SNIFF); Seam A = referee compares PRE-ASSEMBLY rows (full columns incl pk/u_type — closes the identity gap; deletes 7 compensation arms); Cure C = enum decode rides the result envelope |
| **H2 session convergence** | **LANDED de1a58f6 (2026-08-29)** | ONE session = engine H2Defaults VERBATIM; leniency flags + sniff-retry DELETED; identifier ORIGIN (physical/derived) stamped at construction and spent by the H2 renderer (SQL-IR agnosticism slice 1 delivered as a side effect); all four consumers probed green pre-flip; residue: 2 named union-wrap rows (h2 floor 1367) |
| **SQL IR backend-agnosticism** | slice 1 (identifier ORIGIN) LANDED with the convergence; **slice 2 (outputs-from-projections) LANDED + FINISHED 2026-08-29** (pad truth on Join.outputs, padJoinOutputs deleted, all join frames source-derived, outputsOf/withOutputs guard-pinned shrink-only, positional pairing deleted outright — pair-native builders, SqlSelect.paired gone) — landing record in [ORIGIN_ARCHITECTURE_AUDIT_2026_08.md](ORIGIN_ARCHITECTURE_AUDIT_2026_08.md) | slice 2 deleted the audit's 5 compensations (stampJoinOrigins, name lookup, kind fallback, union blanket stamp, renderer positional pairing + reconcileLabels' star-tail shift): a select's outputs BUILD from its projection list at construction. h2 floor 1367 -> 1372 (milestoning residue + 3 label rows healed); the chained-joins residue RE-DIAGNOSED as a DEMAND/PRUNING divergence (undemanded union-extent projection, prune blocked by the star over the union frame) — a separate leg. Next: §4AD conformance census; later slices purify the ANSI base of DuckDB idioms (int-divide `//`, EXCLUDE, interval spellings, carrier caps) |
| **H2 capability parity via Java UDFs** | **CENSUSED 2026-09-16** — [H2_PARITY_CENSUS_2026_09_16.md](H2_PARITY_CENSUS_2026_09_16.md), the census for [END_TO_END_PLAN_2026_09_08.md](END_TO_END_PLAN_2026_09_08.md) Phase 7 (NEW 2026-08-29, user) | measured at HEAD: PCT 1001/1249 on H2 vs 1247/1249 on DuckDB (**gap 246**, and 219 of them sit in no gate — G7 runs Relation only); corpus 2151 vs 2477 (**gap 332**, sets nest exactly, 0 the other way). The lane is DIVERGING: H2 +3 tests since 2026-08-01 while DuckDB gained ~300, so 98.5% of DuckDB became 86.8%. Tiering: ~23 rows are a **spelling** (H2 has `BITAND`/`LSHIFT` and every collection-reduction aggregate natively), ~104 are **scalar Java UDFs** (9 of them already written in `H2ExtensionFunctions`, test-scope), ~151 are **array UDFs over `Integer[]`** (proven signature: array in, array out), ~64 are **codec** fixes with no SQL at all, and ~109 need the carrier flip plus the ordinal-join explode (79) and the lambda lowering (30). Original framing — the ~946 h2 capability walls burn by H2 Java functions (CREATE ALIAS) behind the SQL shim — the engine's own architecture (its H2 extension IS a UDF set; our oracle already registers legend_h2_extension_*); DELIBERATELY reverses the H2 dialect's UDF ban (recorded tension in H2.java header) |
| **Parser strict flip** | A2/A3, DEEP_AUDIT_HANDOFF | invention census (53 skew + 42 crash) then flip |
| **Foundations Phase 3 dedup** | A4, FOUNDATIONS_PLAN §4 | |
| **Prepared statements** | A7 merge | deliberately LAST (text-lane perturbation) |
| **Nullability-inference leg** | [TYPE_E2E_AUDIT_2026_08.md](TYPE_E2E_AUDIT_2026_08.md) findings 2–3 | TypeFact carries no nullability, so expression re-reads under-declare — converse census measured 925 corpus / 49 pct breaches (engine-correct VALUES, under-declared labels; ceiling-pinned both lanes). Design with the flag's first real consumer: TypeFact dimension vs from-tree reconcile vs per-SqlFn NULL-propagation rules |
| **Corpus burn resume** | burn-to-zero memory | paused at 2,347/2,575; largely superseded by the longer arc below |

---

## AUDIT INTAKE — VERDICT_AND_PCT_AUDIT_2026_08_23

(read from `worktree-e2e-deep-diagnosis`; audited at 1a4b0d12 —
PRE-dates the F10 slices, so several findings were fixed the same day)

**Already fixed by the F10 slices (same day, independently):**
- The three declines of §3.5 — letFn (the host-only-PASS "rescue"
  instance), map (binder-error wrap → real cause was the missing
  property-nav flatten, test now PASSES), mixedSort (now PASSES on the
  carrier). Recommendation 9 is structural now: decline ceilings pinned
  **0** — any decline fails the suite.
- §3.4's worst category inversion (mixedSort "wrong answer laundered as
  engine parity") — we now return the RIGHT answer.

**Open, adopted, in priority order:**
| # | Finding | Disposition |
|---|---|---|
| A1 | temporalCanon strips subsecond zeros — contradicts CANONICAL_FORM_SPEC three ways; tests-can-pass-that-should-fail | fix INSIDE F10 slice 3's temporal arm (precision-faithful spelling) |
| A2 | in-SQL `equal()` carries every deleted grant; `assert(equal(...))` bypasses the verdict program | the X1–X4 treatment or three-lane pinning; own slice after F10 |
| A3 | InstanceEquality classifier-mismatch arm sits BELOW the hasIdentityField early return — product lane disagrees with assert lane (one expression, two answers) | small arm reorder + witness test; near-term |
| A4 | host canon has NO LocalDateTime arm (the 26 residue rows); DB canon lacks the non-finite wall the spec promises | slice 3 companion |
| A5 | "engine-exact" under-specified: the engine has THREE lanes (interpreted/compiled/relational) and our interpreted-lane pin is unrecorded | RECORDED here + spec revision |
| A6 | frontier manifest excuses by NAME; `expectedError` never read; split "lite errored" from "lite answered wrongly" | ChannelBDiff fix with the A-ABSENT work |
| A7 | A-ABSENT bucket: 9 B-only rows claim agreement with a channel that never ran them | adopt (register already proposed it) |
| A8 | relation denominator explanation wrong: VERSION SKEW (pom pins 4.133.0 = 348 tests) not "qualifier filters" | register correction |
| A9 | testGet FQN double-count — honest universe 1,117 | prefix-qualify the discovery key |
| A10 | CanonicalRenderSql (491 lines) carries no eval-ledger pin | add pin |
| A11 | CANONICAL_FORM_SPEC §2/§3 stale vs shipped code (scale-normalized vs scale-preserving) on a doc requiring witnesses | spec revision |
| A12 | 2-ULP did zero work | fold into V8 retirement |

**Part-1 silent-value bugs adopted into Bucket 2** (contains substring
match, indexOf +1, and the residual list).

---

## THE LONGER ARC (after the buckets)

1. **test-corpus branch**: parse+compile-ONLY census FIRST (cheap; the
   wall inventory before committing to the execution harness), then
   execution. The bet: hours on legend-engine → minutes here (current
   full corpus sweeps ~3 min for 2,575 tests).
2. **compileAll mode vs demand-driven**: §8 global compile already
   landed for the corpus (one model + per-test overlays) — this is
   mostly discovery/harness work, and it forces `###Data` user-test
   execution, whose charter already exists
   ([DEFERRED_TEST_EXECUTION.md](DEFERRED_TEST_EXECUTION.md)). Bonus
   differential: compile-order bugs.
3. **core_relational burndown** to near zero. FIRST SLICE of this leg
   (user ruling 2026-08-24): **corpus asserts migrate host-side →
   SQL-verdict lane.** The rcorpus harness adjudicates asserts
   HOST-SIDE (EngineTestExecutor eval + compare — the third-impl
   problem) while PCT's Channel B verdicts run in the database (K-arm,
   asserts-are-verdicts-ALWAYS). Ruling: NO incremental drift before
   this slice — a half-migrated referee is a fourth implementation;
   corpus stays host-side until PCT is 100%, then this migration runs
   as one leg with its own acceptance (the grid-extraction render
   convention — TDSNull sentinel, engine text-compare — moves into the
   verdict queries with an explicit row_number order key).
4. **All test.Test functions** (the plain-test universe the parser wall
   burn opened).
5. **Byte-identical PlanGen drop-in** — flagged honestly: a
   parser-parity-SIZED program (plan OBJECT-MODEL parity, not SQL-text
   parity). Same playbook when we get there: oracle-first census,
   byte-exact ratchets, family-by-family.

## DEFERRED PROGRAM — METAMODEL AS DATA (quarantined 2026-08-30,
## user ruling; homework preserved here, do not re-derive)

**The decision (user, 2026-08-30):** quarantine every METAMODEL-ONLY
decline into a named, exact-pinned census partition and burn
everything else to zero first (declined / mismatch / sql-exec); come
back to this program afterwards. Quarantine is a partition, NEVER a
test exclusion — the tests keep running and passing on host; their
dual-channel rows carry a `metamodel-reflection` classification.

**CENSUS UPDATE (same day, step-0 row-by-row census —
docs/FULL_RESIDUE_CENSUS_2026_08_30.md is now the authority on
composition):** the quarantine set is **144 rows**, not 107 — the
1-by-1 pass moved the boundary: ONE-STAMP 17 + SQLNull-layout 1
join the toPostgresModel family (all their witnesses are inside
it; the stamp bucket is NOT an independent bug leg — repromote
only on a non-quarantine witness); bare-lambda 10 + InstanceValue
2 are tesIsToOne* expression-tree reflection (pkOfFunc's class);
2 expressionSequence walls hid under host-unsupported;
routerExtensions 5 is extension-lambda eval over constructed
connections (proposed in, user to confirm). The original
scoped-probe table below stands for the no-scalar cluster detail.

**Quarantined rows (census 2026-08-30, baseline a6115d56 —
attribution measured by scoped probes, not guessed):**
- classMappingById 21 — tests/mapping/extends (mainTable handle
  compares ×4 + `.column.name`/`.id` string compares; witness
  testExtendsForMainTable.pure:79).
- rootClassMappingByClass 13 + view 6 + inferRelationalType 5 —
  meta::relational::tests::typeInference::* (testRelationalExtension
  .pure); every assert compares a STRING against dataTypeToSqlText.
- _classMappingByClass 1 — testSubtypeMapping.pure:70 (assertSize).
- toPostgresModel::newState 18 — sqlDialectTranslation family;
  assertConversion compares CONSTRUCTED protocol-node trees
  (a different shape: runtime-constructed values, NOT authored
  facts — flagged below as the bucket that fits NO current frame).
- pkOfFunc 43 — pkInferenceTests.pure: one private helper reads
  `$func.expressionSequence` (function BODIES reified as metamodel
  data) and runs engine PK auto-inference over the tree; all 43
  funnel through the one helper (single-design bucket).
- routerExtensions 5 (metamodel::execute tests) — probe before
  adding: may be this program's shape, may be plan-text.
NOT quarantined: relationalExtensions 8 (plan-text compares that
crash before lane classification — burn with the plan-text/
classification work); TypedMap 65 (h2-lane owned).

**The architecture ruling that stands over this program
([[one-router-one-evaluator]] memory, ratified in session):** ONE
entry point for ALL pure code; no bespoke per-FQN entry points ever
again (the TestDataGenerationNatives fold is named an instance of the wrong
pattern and owes a spelled-out rename when it migrates); no pure
source carried by the platform. Two candidate resolutions were
explored and neither is ratified:
1. A host-side evaluator for store-free clusters — REJECTED as
   drafted: it is a parallel metamodel interpreter, and we already
   have one (MetamodelWalk 1307 + MetamodelSteps 196 stripped
   lines, ledger-pinned) — that debt should shrink, not
   consolidate.
2. **Metamodel in the database (the speculative TODO this section
   exists to preserve):** extend SystemMetamodel — the ratified
   precedent where `Class.all()` is a real SELECT over
   metamodel.classes seeded from the compiler registry, "zero
   special cases in dispatch" — so reflection queries LOWER like
   everything else. Grow-by-witness: mappings / class_mappings
   (fqn key, id, root, class, superSetImplementationId) /
   mapping_includes; schemas / tables / views / columns /
   view_column_mappings. Include-chain recursion: seed the
   TRANSITIVE CLOSURE at extent-render time (seedStatements
   already renders the extent) rather than recursive CTEs.
   Identity: FQN/path primary key (SystemMetamodel D2 rule).
   MetamodelWalk arms then DELETE as store lowerings claim their
   FQNs (ledger to zero).

**Homework DONE (do not redo):** exact census + witnesses (above);
engine .pure specs read from the real checkouts
(functions_Mapping.pure:28-79, platform_store_relational/
functions.pure:254, relationalExtension.pure:120-137,
toPostgresModel.pure:31-48, extensions/extension.pure:62,
pkInferenceTests.pure:25-29); decline mechanism verified (dual
channel → AssertVerdicts verdict-in-DB → Scalars.lower, no rule);
SystemMetamodel v1 scope verified (ONE table, name property only —
the trivial fragment; nothing multi-table has ever been proven).

**Homework OPEN (the tractability gate — run these BEFORE
chartering execution):**
1. Compile-time-fact vs derived-on-the-fly: read
   MetamodelWalk.mainTable/resolvePrimaryKey/infer +
   MappingNormalizer; if the compiled model already HOLDS the
   resolved facts (extends-chain main table, groupBy/distinct PK,
   view column types), seeding is a dump and lowerings are simple
   SELECTs; if the walk DERIVES them, the derivation must move to
   seed time — a design decision, not a detail.
2. Function-shaped navigation over mapped metamodel rows
   (`$x->mainTable()` is a function, not a property): mapped
   association vs per-native compiler-synthesized query — design
   not done; nobody has demonstrated cast->map(fn) chains or
   assertEquals over row-backed metamodel instances in the store
   lane.
3. Seed cost at corpus scale (compile-once sweep ~185s; metamodel
   extent per test/connection unmeasured).
4. inferRelationalType (5) and pkOfFunc (43): expression TREES as
   data — the hard end (trees-as-rows or trees-as-structs); no
   design.
5. newState (18): runtime-CONSTRUCTED protocol values compared
   structurally — fits neither seeded-facts nor the evaluator
   frame; needs its own design (struct-values canonical layouts
   are the one lead: constructed instances already lower as
   structs when the class declares stored properties).
6. Tractability prototype: ONE witness end-to-end
   (testMainTableForB1 — seed a class_mappings+tables fragment,
   hand-register one lowering, watch the verdict land in-DB)
   before designing the other 45.

**Per-bucket tractability verdict (honest, as of 2026-08-30):**
classMappingById/mainTable/view family (~40 rows) — probably
tractable under the store frame (ordinary mapped navigation over
facts the compiler demonstrably has). inferRelationalType +
pkOfFunc + newState (66 rows) — NOT honestly tractable under any
design discussed so far.


## METAMODEL AS DATA — leg 1 chosen (2026-08-31): PLAN-NODES-AS-ROWS
The plan node model is legend-lite's OWN closed artifact (no engine
metamodel baggage) — the cheapest proving ground for
metamodel-in-the-database. Acceptance test: the quarantined
TypedMap-65 plan-walk tests' filter lambdas lower to SQL over
plan-node rows. Quarantine partition at 172 witness rows + 20 wall
tests (pins in RelationalCorpusRunner; vocabulary in
CanonicalDivergence.METAMODEL_QUARANTINE).

## SQL-TEXT LANE — next design slices (2026-08-31)
1. LIVE-SESSION REFEREEING (chained-fetch 26): referee our side
   against the live DuckDB session where generator temp tables exist.
2. FRAME-IDENTITY THREADING (no-root-exec / no-generator rows): tie
   asserted SQL to the executing frame's session identity.
