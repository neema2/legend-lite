# The stress corpus through legend-lite — program record (2026-09-16)

**Decision (user, 2026-09-16):** bring the `test-corpus` branch fully onto main and
build everything needed to run the tests it holds through legend-lite. Measure before
gating: how long the corpus actually takes on legend-lite is a fact to establish first.

This file is the running record. `docs/RUNNING_THE_CORPUS.md` (from the branch) says
how the corpus runs against legend-engine; `docs/DEFERRED_TEST_EXECUTION.md` is the
charter this program executes; `docs/TEST_CORPUS_MASTER_PLAN.md` §6 is the estimate it
was built against.

## 1. What the branch is

| body | what | how it was checked on the branch | where |
| --- | --- | --- | --- |
| the corpus | 4,742 services (4,729 with test suites) over a 20-domain financial model, seeded by one `###Data` element, every expectation computed by an independent Python oracle | executed against legend-engine 4.138 via `tools/engine-runner` (`perf.TestableMain`): 0 unexpected, 28 engine defects quarantined | `core/src/test/resources/stress/` (202 files, 13 MB) |
| the project graph | 56 Legend projects with declared dependencies; eleven of them are LINKED into the corpus and must load first | compiled only, via `scripts/projects/check.py` on legend-engine | `projects/` |
| grammar fixtures | 215 negatives + 51 positives generated from the engine grammars | already ADOPTED on main as `parser-equivalence/.../sibling-corpus` + `FixtureCorpusParityTest` (main's version kept at merge) | `scripts/parser/` |

On the branch the legend-lite side only LOWERED each service to SQL text and checked it
was non-blank (`StressDomainTest`). Nothing executed a suite or judged an answer through
legend-lite, and that side was stale: it did not load the linked projects.

## 2. The merge (LEG 0)

`origin/test-corpus` merged into main with `--no-ff`. One conflict
(`FixtureCorpusParityTest`, add/add): main's stricter ratchet kept. Everything else was
additive.

Making the whole corpus LOAD in legend-lite took four parser fixes and one flipped
assertion:

| blocker | fix |
| --- | --- |
| `ExtractSubQueriesAsCTEsPostProcessor` in a connection's `postProcessors` | parsed as the bare-keyword flavor; wire `{"_type":"ExtractSubQueriesAsCTEsPostProcessor"}` (the engine's record has no field beyond its span) |
| `coalesce([[db]T.A, [db]T.B])` — an array argument of column refs | `PRelLiteralList` now holds ANY function-operation argument (the engine's `functionOperationArgumentArray`); elements emit as `{"_type":"literal","value":<element, untyped>}` because `Literal.value` is an `Object` field. `[` decides between array and `[db]` pointer by content, as ANTLR does |
| compact suite test `id : PURE_TDSOBJECT => ...` | told apart from a base-data resolver `path: Kind #{` by the `=>` after the identifier |
| `LegendLiteGapTest.booleanColumnType` asserted BOOLEAN accepted | main rejects BOOLEAN like the engine (F3 CLOSED); the corpus already uses BIT; assertion flipped |
| linked projects absent | `StressCorpus.LINKED_PROJECTS`, dependencies before dependents, each in model / store / mapping order |

Then the model-build census (probe: exclude the failing file, retry, until the model
builds). Four real gaps, five files:

| file | gap |
| --- | --- |
| 29-money.pure, 55-canonical-store.pure | Measure/Unit: `stress::Money~USD` never registers as a resolvable type |
| 70-surface-store2.pure | precise primitives (`meta::pure::precisePrimitives::Varchar(200)`) |
| 71-mapping-surface2.pure | M2M explosion `part*:` refused by the normalizer |
| 75-surface-gaps.pure | M2M local property `+localTag` colliding with a declared property refused |

Each has an executable case in `LegendLiteGapTest`, and `StressExclusions.EXCLUDED`
carries the reason. Removing one is a deliberate act that the gap test forces.

**Timing, lowering only** (`StressDomainTest`, 228 files, 13.2 MB, 7,608 elements):
parse + build 4.9 s; lowering all 4,735 services to SQL 2.4 s (1.7 s in the lowerer).
2,753 lower cleanly, 1,979 fail — 1,798 of them one shape: navigating a join-mapped
property inherited by a subtype set (`dataquality::UniquenessRule.book`, where `book`
is mapped on the base set and the subtype set `extends` it). The bare projection over
the same subtype passes; only navigation through the inherited edge fails. One
normalizer fix, many rows. Remaining lowering buckets: `orElse` unported (73),
associations declared across linked projects not mapped (80), `combo::ComboRT` binding
(15), typing of derived properties (9), grouped-subselect aggregates (3).

## 3. The pieces (LEG 1 + LEG 2)

**Typed suites in the model.** `ServiceDefinition.testSuites` is the typed
`Protocol.PServiceTestSuite` list and `test` the typed legacy block; the `"<suites>"`
placeholder is gone. `###Data` is a `DataDefinition` (the protocol body, indexed by
name, `ModelContext.findData`); the opaque-carrier exemption for `ElementParser` in
`ArchitectureTest` is retired. `RuntimeDefinition.connectionIds` keeps the
`store: [ id: conn ]` id, which is how a suite addresses its provisioning.

**`com.legend.test.ServiceTestRunner`** (product): the engine's testable rules from
its sources —

- provisioning: `data: [ connections: [ id: ... ] ]` → the runtime's store for that id;
  compact resolvers → the store; `Reference` → the `###Data` body; `Relational` CSV
  → `CsvSeed` DDL + INSERT typed from the parsed store. Other kinds SKIP loudly;
- sessions: one seeded session per distinct provisioning, shared by every test whose
  program has no statement effects (the corpus's 4,729 suites all reference
  `stress::TestData`, so the 5,159-row seed loads ONCE); an effectful body gets a
  private freshly seeded session;
- execution: parameters as `let`-bound variables ahead of the body, then the ONE
  production entry (`Compiler.executeResolved`) against the service's runtime;
- serialization: `PURE_TDSOBJECT`/`RAW` = one object per row keyed by column; a graph
  result is the `{"builder":{"_type":"json"},"values":[...]}` envelope; DEFAULT on a
  tabular result SKIPS (not in the corpus);
- judgment: `EqualToJson` through `com.legend.test.TestAssertions` — the engine's
  `JsonNodeComparator.NULL_MISSING_EQUIVALENT_AND_UNORDERED_ARRAYS` with exact
  decimals: null ≡ missing, arrays unordered at every level, `1 == 1.0`.

`CsvSeed` learned two things the corpus needed: RFC 4180 quoted cells (a value with a
comma), and reserved-word identifiers quoted the way the query renderers quote them
(a column named `LIMIT` was created unquoted and the DDL failed to parse).

**`StressServiceSuitesTest`** (core, integration): runs every `stress::` suite through
the runner on DuckDB, writes `target/stress-suites-{pass,fail,skipped}.txt`, prints the
per-phase times, the failure buckets and the slowest tests. Measurement first: no
count is pinned until the numbers are known.

## 4. Measurements

First smoke, one service (`F30_TradeEverything`, 16 columns, 3-hop chains, qualified
properties): model parse + build 6.4 s; seed 5,159 rows once ≈ 1.4 s; PASS against the
oracle.

**Full run, 2026-09-16 (first):**

| | |
| --- | --- |
| tests | 4,736 (4,729 suites; a few suites carry two tests) |
| PASS (equal to the oracle) | **2,702** |
| FAIL | 2,028 |
| SKIPPED | 6 |
| model parse + build | 6.3 s |
| execution of every test | **8.5 s** |
| wall, whole test | **20.7 s** |
| sessions opened (distinct provisioning) | 19 |
| slowest test | 1.36 s (the one that seeds the shared session) |

The same corpus takes about an hour against legend-engine (`docs/RUNNING_THE_CORPUS.md`:
0.6 s per service plus 9 s per JVM). Legend-lite runs it in the time the engine spends
starting two JVMs. The cost that matters is the 6 s model build, paid once.

Both counts are pinned as ratchets in core's suite: `StressServiceSuitesTest.MIN_PASS =
2702` and `StressDomainTest.MIN_LOWERED = 2753`. Raise, never lower.

## 4b. Decisions taken with the user (2026-09-16, afternoon)

**The runner executes no SQL.** The first cut seeded through JDBC in the runner; that
was wrong. The engine's design is: a suite runs on a TEST RUNTIME whose connection
carries the suite's data, loaded when the connection is established. Legend-lite had
that seam (`StatementExecutor.establishContexts`) but only for Pure-INSTANCE runtimes;
a connection declared as an element with `testDataSetupCSV` was parsed and never
seeded — a platform gap, now closed. The runner builds the test runtime through the
existing execution overlay (`PureModelContext.withExecutionOverlay`, what the corpus
harness uses) and hands sessions to the platform, like `PureTestRunner`. Registered in
the JDBC census with that argument.

**Seeding: baseline the engine's way FIRST, optimize second.** `ServiceTestRunner.Sessions`:
`FRESH_PER_TEST` (a fresh session per test, seeded on establishment — the engine's
fresh database per run) is the apples-to-apples baseline; `SHARED` (one seeded session
per distinct provisioning for read-only tests; an effectful body gets its own) is the
optimization, measured after. The platform-side "established once per session, re-seed
after a write" memo is in place and is a no-op under FRESH_PER_TEST.

**Nothing "smarter than the engine" is hidden.** The judge implements the engine's own
JSON rules (null ≡ missing, unordered arrays, exact decimals); rendering is ours in the
engine's shapes; graph JSON is built by the database, not Java (the standing tenet);
DEFAULT tabular serialization, EqualTo/Relation assertions, multi-execution services,
ExternalFormat/ModelStore data are SKIPPED loudly, never faked; parameters bind as
`let` variables; ratchets list every failure by name; where the engine is wrong and the
oracle right (F6/F50/F51/F54) lite is expected to PASS.

**The parser ledger, re-measured on real divergence.** `FixtureAdjudicationTest` fed
Java text blocks with their SOURCE indentation, so every `###` header sat 16 columns in
and the engine refused 270 ordinary fixtures ("Unexpected token"); it also never asked
legend-lite. Fixed: runtime text, both parsers, a row is a DISAGREEMENT. Real ledger:
749 fixtures, 722 agree, 23 leniencies in 6 kinds — the clean-sheet mapping language
(18), two legend-pure forms the engine subsets away (3), the SQLite backend (2) — and 4
over-strict rows, two of them lite INTERNAL errors on one-/three-end associations (a
defect). `InMemory` is gone since 2026-08-10. Left as-is for now by the user's decision,
every kind named.

## 5. Ledger — the fail rows, by bucket (first run)

| rows | bucket | where it fails | decision |
| --- | --- | --- | --- |
| 1,798 | `property 'book' of class 'X' is not mapped in mapping 'stress::AllMapping'` — navigation through a join-mapped property INHERITED by a subtype set (`X[sub] extends [base]`; `book[positions_Book]: [store::DB]@Join` mapped on the base) | resolver (ClassSources / Substitution) | FIX, first: one normalizer/resolver rule, 1,798 rows |
| 80 | `association 'A' is not mapped in mapping` — associations declared across linked projects (`reporting::BookHasRollup`, `middleoffice::TradeLifecycle`, `brokerage::TradeBrokerage`, …) | resolver | FIX, second |
| 73 | `unknown function 'orElse'` — unported platform function (F7) | typing | FIX: port `orElse` from the spec |
| 15 | `runtime 'combo::ComboRT' has 0 mappings binding class` — the combination matrix's mapping fails to normalize | normalizer | census the normalize failure |
| 9 | typing of derived properties (`expected hier::Profile, got String`; `expected Float, got Number` under `trustOne`) | typing | census |
| 3 | `aggregate over navigation requires equi-join parent keys` | lowering | census |
| 2 | stores bound to DIFFERENT connections under one runtime (`external::EntityDB` + `store::DB`) — XStore | lowering | XStore leg |
| ~45 | `matchesOracle: … has no equal element in the actual array` — a genuine ANSWER difference (numbers, dates, nulls, ordering of nested arrays) | judgment | read each: engine-quarantined rows where lite is RIGHT vs real lite defects |
| 6 SKIPPED | multi-execution services (2), `ExternalFormat` data (1), runtimes in excluded files (3) | runner | multi-execution binding + ExternalFormat provisioning later |

Ledger order follows the rows: the 1,798 first.

## 5b. Fix ledger — findings of the afternoon (2026-09-16), each with its judge

| # | finding | fix | judge | state |
| --- | --- | --- | --- | --- |
| F-A | Multi-store suites (10: the `hier` family, the external-entity pair) SKIP because the test runtime binds one connection | one test connection per provisioned store in the overlay (the engine: one test connection per store) | those 10 rows PASS/FAIL, no SKIP | open |
| F-B | Routed set names resolved in the DEFINING mapping's closure (1,798 + 80 rows) | LANDED in two parts. (1) PINS: a pin is a NAME; `UnionSynthesis.resolvePin` resolves it in the defining closure first, else MODEL-WIDE by effective set id (the same set any including mapping binds; the engine's compiler accepted these pins — the corpus compiled there); the route names the set's function under ITS defining mapping and the queried mapping binds it at query time. An ambiguous id (several mappings define it) POISONS that one property with the owners named; NO re-synthesis. Declared divergence: a query under a mapping that cannot see the set still navigates here, where the engine fails. (2) ASSOCIATION ENDS: `AssociationSynthesis.endAnchors` — an end class the closure cannot see anchors on the table the single-hop join names for that end (`reporting::BookHasRollup`: the association in the rollup mapping, `positions::Book` in the positions mapping). | DuckDB 108 / H2 440 EXACT; stress H2 1,693 → 2,710 pass (ratchet 2,702 GREEN on H2); the 1,798 rows moved to F-M | fixed |
| F-M | The former 1,798 sat on ONE resolver seam (1,436 + 360 rows, all `book.desk.businessUnit.legalEntity.jurisdiction` over the 359 `book`-pinned classes): a ROUTED navigation head (`book`, a class-typed Join PM slot) followed by ASSOCIATION hops (`desk`, `businessUnit`, `legalEntity` are all association ends, not slots). The materializer's assoc-sub rule took ONE extra hop past a slot head and the association join materialized only SLOT tails of its target, so the read walk died at the second association. | LANDED as recursion in the existing owners, not a new pass-through: (1) `NavMaterializer.demandUnboundTail`/`foldAssocSubs` — a deeper tail past an association end rides that association join as its nav tails, and the SubNav carries the join's own sub-tree (`composeSubNavPrefixes`); (2) `AssociationJoins.associationJoin` — a tail whose head is an ASSOCIATION end of the target (no binding) joins the nested-association widening (`widenNestedAssocs`, the navigate() rule) with its deeper tails, the widened SubNavs ride the join's target sub-tree, and the nested join carries the DOTTED chain key (an explicit hop date keys by it — corpus testDerivedPropertyOnNonTemporalClassWithMilestonedChain) under the same temporal gate the one-hop rule applies. A chain-side slot pass-through was tried first and reverted: it duplicated the head's own assoc-sub columns. | stress DuckDB shared 2,765 → 4,203 (MIN_PASS), H2 fresh 2,710 → 4,148; corpus lanes 108/440 EXACT | fixed |
| F-O | 360 rows: the same family on the graph-fetch side (DSTree*: `book { desk { businessUnit { legalEntity { leId }}}}`). The graph node builder fixed its row type from the node's relation BEFORE emitting children; a ROUTED node (a union source) projects only what its own leaves demanded, so the `Book_Desk` condition's parent-side read (`DESK_ID`) found no column on the `book` child's row and the lowering walled ("unresolvable even after isolation") | `GraphEmission.buildGraphNode0` widens the node's relation for every ASSOCIATION child's parent-side key reads BEFORE the row type is fixed — `StackBuilder.demandForCondition`, the same demand seam an association join applies to its target (`AssociationJoins.associationParentSide` exposes the column-space condition and which param is the parent; property-space conditions substitute through bindings and need nothing). Rows that already carry the keys pass through untouched. `ServiceTestRunner` prints a failure's stack under `LEGEND_LITE_STACKS` (the resolver walls' switch). | stress DuckDB shared 4,203 → 4,564 (MIN_PASS), H2 fresh 4,148 → 4,509; corpus lanes 108/440 EXACT, no churn | fixed |
| F-R | Chain budget (user, 2026-09-16: the parallel chain's budget is 4 MINUTES; today's chains ran 309–387 s): G1 grew ~150 s when the two stress classes joined the core suite (`StressServiceSuitesTest` 108 s + `StressDomainTest` 37 s inside the parallel chain), and G6/G8 inflated under its contention | (1) the seed memo (F-Q); (2) `StressDomainTest` DELETED — it only checked that services lower to SQL, which the suites test's row verdict implies (4,564 pass ≥ its 2,753 floor) and whose failures the suites test names by reason; its two reason lists (unbindable runtimes, one unresolvable service) are the suites test's fail reasons; (3) GATE 10 = the stress corpus in its own parallel stream D (`tools/allgates.sh`, CI lane "gate 10 stress corpus"), G1 runs without it; (4) the H2 lane has its own floor (`MIN_PASS_H2` 4,509) | chain wall + per-gate times before/after in docs/GATES.md | landed; measure |
| F-P | What remains after F-O (DuckDB 156 rows / H2 211) — HOMEWORK DONE: docs/STRESS_CORPUS_BURNDOWN_HOMEWORK_2026_09_16.md reads every row with the engine's behaviour beside it, in leg order: `orElse` 73 (the prelude GENERATOR misses `langExtension.pure`); OR/range navigation 14 (silent-wrong counts: `parentKeysLenient` mines OR branches as conjunctive equi keys); timestamp `+0000` text 14; `isAlphaNumeric` 15; five small resolver/lowering rows; H2 `EPOCH_MS`/`REVERSE`/`firstDayOfWeek`; the numeric-conformance DESIGN leg (doubles at the envelope, not the read — 11 rows); `dateDiff` HOURS 4 = ENGINE-QUARANTINE (Pure's Java is elapsed-truncated, the engine's H2 SQL counts boundaries); `Binding` property mapping wall 2; H2 `CHAR(n)` padding 9 to VERIFY (the engine seeds through the same `setUpDataSQLs`, declared types, so its `CHAR(6)` exists too — its read path must trim) | the homework's order; MIN_PASS/MIN_PASS_H2 rise per leg | stress both lanes; corpus lanes exact | homework done; legs open |
| F-Q | The seed-once memo keyed by the RENDERED seed text (per statement regeneration: CSV parsed, rows typed, DDL + INSERTs rendered) | key = a VALUE RECORD of the seed's sources (`StatementExecutor.SeedSources`); generate on a miss only; two wrong shapes caught by measurement (model identity; a joined string) | DuckDB shared 51 → 26 s wall, identical fail set; H2 identical; lanes exact; G1 alone 107 → 93 s | fixed (71a407160) |
| F-S | `orElse` (73 rows) was CORPUS AUTHORING, not a lite gap: measured across the engine checkout, `orElse` is engine-internal Pure (121 calls in implementation code; ZERO in the engine's user-shaped relational test corpus, which uses `coalesce` 120 times). Our eight generated sites reached for it because `[0..1] + [0..1]` refuses to compile | the eight sites rewritten to `coalesce(...)` (what a Legend user writes; the engine inlines `orElse` to exactly that); notes updated (06-trading, UPSTREAM_FINDINGS F7). 62 rows green; the other 11 are last-digit floats and join the numeric-conformance family (now 22 rows). The platform-library scope question (engine `corefunctions` into the prelude: dry run +185 bodies, ZERO closure) stands on its own merits, no corpus pressure | DuckDB shared 4,564 → 4,626 (MIN_PASS), H2 fresh 4,509 → 4,571 | fixed |
| F-T | Aggregates / emptiness over a navigation whose join is OR or a range (15 rows: AA_*, BK*, LE2/LE6, SP_def*, CV3/CV6/CV7): (1) the parent-copy grouped subselect grouped by columns MINED from the condition — `parentKeysLenient` descends `or`, and grouping by non-key parent columns multiplies the count by every parent sharing them (8 for 2); (2) `COUNT(*)` over the LEFT-joined parent copy counted the unmatched parent as 1; (3) a `{target}` self-join mapped for BOTH ends of one association was oriented by the ASSOCIATION's property order while the predicate is built from the mapping's FIRST property mapping (CV6 longer→shorter, CV7 shorter→longer) | (1) parent-copy shapes correlate by the parent's declared `~primaryKey` (the exploding subselect's own rule; condition keys stay the fallback for PK-less parents); (2) a row count over the joined shape counts a TARGET column (its first key column) — NULL on the unmatched row; (3) `AssociationBinding.forwardProperty` (the mapping's first PM's property walks the join as written, the other end backwards) — a DECLARED divergence from the engine, which returns the forward set for both ends (corpus finding F52) | DuckDB shared 4,626 → 4,641; corpus lanes 108/440 EXACT, ResolveNavigationTest green | fixed |
| F-U | Timestamp text (23 rows: Q*, BO0, CB_*, GG_* trees): the engine's JSON spelling of a DateTime is seconds + NINE fractional digits + `+0000` whatever precision was written; the runner printed the literal body, the graph envelope nanos without the zone | `PureDateLiteral.toEngineJson` (one owner; the runner's cell uses it); the GRAPH envelope keeps ISO_NANO: it is the engine's execute→JSON channel (the corpus lanes' graphFetch goldens carry no zone) — adding `+0000` there lost 35 corpus rows, and DuckDB's `strftime` with `%n` followed by literal text emits NUL bytes (probed: `...000000+0000\u0000\u0000\u0000`). The service-test channel's `+0000` on graph DateTime leaves (6 GG_* rows) is a per-channel spelling the RUNNER must own — OPEN (F-W) | DuckDB shared 4,641 → 4,654 (TDS rows); H2 fresh 4,571 → 4,602 | fixed for TDS; graph open |
| F-V | The corpus's 16 ENGINE-QUARANTINED services (scripts/corpus/quarantine.py: expected rows = the author's corrected semantics, the ENGINE fails them — F6 count over empty, F13 Otherwise, F14 groupBy enum, F32/35/37/38/39/41/50/51/52, F27 Binding, F10/F12 enum through union/chain, F15 XStore) ALL PASS in lite as of this row — lite is right where the engine is wrong on every one | measured 2026-09-16 after F-T/F-U | — | recorded |
| F-W | Graph-fetch DateTime leaves under the SERVICE-TEST channel spell `…000000000+0000` (the engine's ServiceTestRunner serializes the objects with its date transformer); the execute→JSON channel (corpus goldens) spells no zone; lite's envelope is built once in SQL | a channel-selected spelling: the runner names the serialization channel and the envelope's DateTime format follows it (never a string rewrite of the JSON tree; never the execute channel's spelling changed). DuckDB caveat: `%n` + trailing literal text is broken in strftime — spell the suffix by concatenation | 6 GG_* rows | open |
| F-X | `isAlphaNumeric` in a mapping expression (15 `combo::` rows): the dynafunction table knew the NAME (generated from the engine's dialect extensions) but its resolution was UNSUPPORTED — no membership row, no signature, no lowering; the wall fired honestly | a platform function we OWN (pure's body is the isDigit/isLetter walk over the string, stringExtension.pure:85): one membership row (`IS_ALPHA_NUMERIC__STRING_1`, signature GENERATED by `-Dnatives.generate=1`), the dynafunction row flipped to PURE (resolution is OURS; the generator keeps it), one lowering rule — `REGEXP_FULL_MATCH(x, '[a-zA-Z0-9]+')`, each dialect owning the anchoring spelling (DuckDB `regexp_full_match`, H2 `REGEXP_LIKE('^(?:…)$')`) | the 15 rows; 4 turned green outright, 11 uncovered F-Y/F-Z behind them | landed in tree |
| F-Y | `firstHourOfDay(ts)` in a mapping prints a DATE on DuckDB (`2024-04-04` for `…T00:00:00.000000000+0000`, 8 rows): DuckDB's `date_trunc('day', ts)` RETURNS A DATE at day grain and coarser (TIMESTAMP only for hour and finer — probed `typeof`); the engine's H2 keeps the TIMESTAMP. The lowering was right (semantics only: DATE_TRUNC 'day'); the backend's return type was the divergence | the DuckDB dialect casts the day-grain truncation back to TIMESTAMP (the dialect owns the idiom; the coarser parts are the Date-typed firstDayOf* heads and stay) | the 8 fhday rows + corpus lanes | landed in tree |
| F-Z | `splitPart(col, '-', 1)` in a mapping returned the SECOND token (`'S'` for `'R'`, 3 rows): the engine's dynafunction is SQL's `split_part` verbatim in every dialect extension (parts count from 1); pure's `splitPart` counts from 0 (splitPart.pure: `'Hello World'->splitPart(' ', 0)` is `'Hello'`) and lite's lowering adds one for SQL — the mapping-side translation passed the 1-based index into the 0-based function | conform by emission in the translation arm: the dynafunction's part becomes `cast(part) - 1` before pure's splitPart. DECLARED residual divergence: SQL `split_part` keeps empty tokens, pure's drops them (`'a--b'` part 2) — no corpus row reaches it | the 3 split rows; PCT splitPart unchanged (the pure-side rule is untouched) | landed in tree |
| F-AA | H2 FLOOR CORRECTION: `MIN_PASS_H2 = 4602` (written at a4c4a883d) was never true of that commit — the H2 lane measured 4,596 there (re-measured 2026-09-17 with this leg's product edits stashed). The 4,602 was measured while the graph-envelope `+0000` spelling (F-W's first attempt) was in the tree; it was reverted for DuckDB's NUL bytes and the H2 lane was not re-run — the H2 lane is not in the local chain (gate 10 is the DuckDB shared lane), so nothing caught it | the floor is set to the MEASURED count with this leg: 4,600 (4,596 + isAlphaNumeric 2 + splitPart 2 on H2; the fhday fix is DuckDB-only). DECISION OWED (user): the H2 lane in CI's lane matrix (2 min; the local chain's 4-min budget cannot carry it) so an H2 floor is enforced somewhere | the H2 lane twice (baseline 4,596; with the leg 4,600; identical fail sets but the 4 rows) | recorded; floor corrected in this commit |
| F-AB | `highestRate` over the aggregated view `CURVE_SUMMARY` (4 rows: AA_CurvesYieldCurveCounts, CV9_CurveSummaries, SP_defCurveSummaryGraph/Group): "expected Float, got Number" — the class function's ctor check. A TABLE column's kind resolves from its declared type (DECIMAL → Decimal → the Decimal→Float coercion); a VIEW column's kind resolved only THROUGH a plain column reference, so `HIGHEST_RATE: max(RATE_CURVE_POINT.ZERO_RATE)` had no kind, no coercion fired, and the pure `max` overload typed the value Number | the view column's kind is its expression's inferred SQL type (`RelationalTypeInference`, the engine's inferRelationalType — the same rule the metamodel store stamps on relational-operation rows): `KnowledgeLayer.columnKind` falls to it for computed view columns; `DeclaredCoercions.coerceColumnToDeclared` asks `columnKind` (view-through) instead of the table-only column lookup | the 4 rows; corpus lanes; PCT | landed in tree |
| F-AC | View-backed to-one child in a graph tree (GG_BookTree, DSTree_CapitalGain): the correlated child's predicate over the GROUPED view resolved EVERY variable against the group's projections — the parent row's key hit the view's own `BOOK_ID` and the correlation became `HAVING t3.BOOK_ID = t3.BOOK_ID` (every book's rollup; "more than one row returned by a subquery") | post-aggregation predicate resolution scopes by VARIABLE: the lambda's own row reads are the grouped select's projections; any other variable is a free read of an ENCLOSING scope and resolves there (`Lowerer.scopedResolver(select, ownVar, postAggregation)`; the grouped branch of `tryPredicate` used to ignore the variable). FIRST ATTEMPT (resolving the other variable through the ENCLOSING scopes) put the correlation into the group's HAVING with the outer alias; DuckDB binds that, H2 does not ("Column t0.BOOK_ID not found": the grouped view is a DERIVED TABLE inside the scalar subquery, and H2 cannot correlate a derived table) — and the H2 corpus lane lost testGraphFetchWithGroupByViewAtChild. A second attempt (ISOLATE every correlation over a group) inlined the group's aggregates into an outer WHERE and broke PCT testExistsOnGroupedRelation on both backends. LANDED: over a grouped select the OTHER variable is simply UNFOLDABLE, so the filter isolates the group and correlates from the wrapper's WHERE — the exact path the exists-over-group PCT rows already take, valid on both backends | the 2 trees on BOTH lanes; corpus lanes (H2 testGraphFetchWithGroupByViewAtChild back); PCT | landed in tree |
| F-AD | Routed head + nested association (D_PaymentDense): `paidTrade[paymentBase, otcBase]` materializes the routed target as `SELECT …, OTC_ID AS __route0_0` (the key lives under the route slot, the physical column is dropped); the deeper tail `optionTerms.optionTrade.currency` joins the sub-target `ON t2.OTC_ID = …` and the binder finds no such column. (The homework named `widenNestedAssocs`; the join is built by `NavMaterializer.foldAssocSubs` — verified with a diagnostic, then removed) | the sub-join demands its condition's left-side keys on the routed pipe before binding (`StackBuilder.demandForCondition(pipe, cond, 0)` — the F-O seam; `demandOnArm` re-reads the physical column from the projection's source) | the 1 row; corpus lanes | landed in tree |
| F-AE | CI gate 10 (the DuckDB shared lane) FAILS on the x86_64 runners (linux, windows) and passes on macOS (arm64): 4,662 vs the arm64 floor. RESOLVED 2026-09-17 by the uploaded ledger (ffb314e24 added `core/target/stress-suites-*.txt` to the gate-10 artifact): the ten extra rows are ALL `cbrt` cells of the combination battery (CB_C0/1/2/3/4/8/12/13/14/15: `6.600030608979562` vs `…561`, `4.641588833612779` vs `…778`, `7.130215735712044` vs `…045`) — a last-ULP difference in the C math library's cube root between arm64 and x86_64 (DuckDB's `cbrt` calls libm; the engine's H2 uses Java's fdlibm `Math.cbrt`, identical everywhere). NOT the numeric envelope | DECISION OWED (user): (1) the stress judge adopts the corpus referee's DECLARED 2-ULP policy for Float cells computed by libm functions (PureAsserts already declares it for World 1), or (2) per-architecture expectations. Recommended: (1) — it is an existing declared policy, the divergence is the platform's, and the rows stay the verdict everywhere else | the linux + windows ledgers (13 rows named, all cbrt) | CLOSED 2026-09-17: the stress judge (`TestAssertions`) adopts the corpus referee's declared 2-ULP policy for finite doubles after an exact compare — the 10 linux + 3 windows `cbrt` rows (all one ULP) pass; the ~50-ULP double-arithmetic rows stay red everywhere until Rule 1 lands. CI watched to green on all three platforms before any further push. |
| F-AF | NUMERIC ENVELOPE — TRIED AND PARKED (homework docs/NUMERIC_ENVELOPE_CENSUS_2026_09_17.md §1–§7). The engine's codified rules were read from the pinned checkouts: literals bare, the database's own arithmetic, cells read by JDBC kind with identity numeric transformers, the ONE conversion to the declared Pure kind at the PCT adapter (`toFloat` by declared return type), service tests compared by decimal value. Lite's read-time double decisions (Float literal `CAST AS DOUBLE`, Decimal→Float read-time cast) are lite inventions; division's operand cast is NOT (pure's `divide` returns Float; H2 rounds NUMERIC division inside the query — probed) | the attempt: bare literals + type-assertion coercion + a declared-kind DOUBLE cast at three envelopes (Render TDS cell, JSON leaf, value root). Stress: DuckDB 41 → 20, H2 4,607 → 4,619, zero new rows. THE CHAIN WENT RED: G4 lost 14 (sub-aggregations, sqlFunction projections — the referee's expected side carries the engine SQL's DECIMAL kind, `19.75D`, ours the envelope's DOUBLE), G5 lost 1 (a Float cell inside makeString printed `52` — `Fold.cellText` has no declared-kind rule), G6 PCT wire census +25…37 columns and G9 Channel B 26 disagreements (both pin lite's own policy 'a Float-declared column's WIRE is DOUBLE'), G1 four pins (value-egress arm keyed on the SQL label DOUBLE fired on Number- and Decimal-declared roots: `[42]` → `[42.0]`, `353791.470` → `353791.47`; a precision-exact Float literal `123456789123456789.99` is decimal-carried BY DESIGN and the DOUBLE cast printed `1.2345678912345678E+17`). Reverted. THE REAL LEG: the three verdict lanes (AssertVerdicts numeric kinds, SqlTypeCensus wire policy, Channel B dual verdict) and the precision-exact-Float design must adopt the engine's rules FIRST — compare by value, kind by declared type at the boundary, DECIMAL wires allowed for Float-declared columns; only then the product flips land | every lane (census §4, attempt §7) | STEP 2 LANDED (the judges: Rule 3 equality by declared kind, Rule 2 wire delivery, cellText Float form — chain green, every lane unchanged at today's emission); STEP 3 (Rule 1 emission: bare literals, no read-time Decimal→Float cast) is the next leg |
| F-N | Lane shapes (user ruling): H2 = the engine-faithful reference, a fresh freshly-seeded session per test (1.9 min for 4,736 tests; the engine ~60 min: ~32x); DuckDB = the fast gate, one seeded session per distinct provisioning SHARED across its tests, isolation by write detection (a session that ran an effectful statement is re-seeded before the next test). Apples-to-apples DuckDB fresh-per-test: 2,765 pass in 27 min; shared gives the same count in ~30 s (read-only corpus). `-Dstress.sessions=fresh|shared` overrides either lane | `StressServiceSuitesTest` defaults by backend; MIN_PASS 2,702 → 2,765 | stress run both lanes | done |

| F-C | H2 reserved words `VALUE`, `YEAR` missing from the H2 lexicon (DDL and queries over such columns fail on H2) | added to `Lexicon.H2` (witnessed) | H2 lane EXACT; stress on H2 | landed in tree |
| F-D | Seed DDL typed from the PURE property type (`DECIMAL(18,4)` → `DECIMAL(38, 9)`): 940 H2 rows answered decimal division at the wrong scale; DuckDB hid it | the seed creates the store's DECLARED types through the one DDL producer | the 940 rows on H2; DuckDB unchanged | in progress (F-E's shape) |
| F-E | DDL spelled by a `Flavor` enum + a `constraints` boolean + `rawH2IsNative()` ternaries at call sites — a target decided outside its dialect, three times over (USER stopped three patches) | DDL as IR: `CreateTable`/`DropTable` nodes rendered by the dialect with its own identifier and type rules; `Ddl` = model→node; keys/nullability ride the node as DECLARED; delete Flavor, the boolean, the duplicate identifier/type tables | DuckDB 108 / H2 444 EXACT; engine-text goldens byte-equal; stress both backends | in progress (task #6) |
| F-F | Nothing stops the next flavor: no test pins target-name decisions outside the dialect package | `DialectBoundaryTest`: `rawH2IsNative()` callers and `DatabaseType` checks outside `com.legend.sql.dialect` pinned to the two seams (dialect resolution, the raw-SQL boundary), shrink-only | the test itself; a new ternary fails the build | open — DO NOT FORGET (this row) |
| F-G | Two over-strict parser rows are lite INTERNAL errors (one-/three-end Association → IllegalStateException, not a parse refusal) | refuse at parse with a positioned message | `FixtureAdjudicationTest` lite-internal list empty | open |
| F-H | Service query-by-reference leniency and the clean-sheet/SQLite spellings | user decision: left as-is for now, every kind named | — | decided |
| F-I | Rendering DDL from the declared store (keys included) exposed a metamodel DATA defect: `DatabaseDefinition.views()`/`tables()` are flat mirrors of EVERY schema, and the metamodel walks paired them with `default` — each named-schema view was registered twice, two same-named views in two schemas (scanRelations DB2 `E.AltID_View` / `ViewSchema.AltID_View`) collapsed onto ONE id; the tables/columns walks hid the same shape behind a put-then-remove-by-name compensation | the model derives `defaultSchemaTables()`/`defaultSchemaViews()` ONCE (flat minus named, by identity); every walk (MetamodelSeeds tables/columns/views/schemas, OpSeeds types/view columns) reads it; the compensations died | corpus lanes 108/444 EXACT with keys declared | fixed |
| F-J | The stress H2 session refused `VALUE`/`YEAR` column names because the test opener used a bare `jdbc:h2:mem:` — the engine's H2 test connection carries `NON_KEYWORDS` (H2Settings) that make them legal; my first patch added them to the H2 lexicon, which quotes a lowercase `value` and breaks against the uppercase table (23 corpus rows) | reverted the lexicon; the stress opener carries `H2Settings.SETTINGS` like the corpus lane. OPEN: the product's own H2 URLs (`ConnectionResolver`, EmbeddedH2/default arms) also open without the engine's settings — a LocalH2 user connection is not the engine's session | `ConnectionResolver` H2 arms append H2Settings; a test on a `value` column through a user connection | open (opener fixed; product arms open) |
| F-K | Declared keys exposed a second physical-order reliance: `Mapping.enumerationMappings` is an ORDERED Pure collection, its metamodel table carried no declaration ordinal, and `first()` below a navigation hop lowered to a bare `LIMIT 1` — insertion order on both engines until H2 scanned the keyed table in key order (corpus testEnumTheSame: Foo by declaration, Active by key) | `enumeration_mappings.ordinal` (declaration order, seeded); the limit family (first/take/drop/slice) over any metamodel row carrying the ordinal sorts by it first (`FlattenOps.byDeclarationOrder`, both fold sites), read through the hop join's column prefix. CENSUS still owed: `enum_value_mappings`, `enum_value_sources`, `class_mappings`, `group_by_mappings`, `view_column_mappings` are ordered in Pure and carry no ordinal — a positional read over them still follows scan order | corpus lanes 108/444 EXACT (DuckDB 54 s, H2 21 s) | fixed for the witness; census open |
| F-L | The H2-only stress gap was ONE bucket (936 rows): Pure `divide(Number, Number)` IS a Float, but the renderer only promoted with `1.0 *`, so H2 divided two DECIMAL(18,4) columns into an exact 36-digit NUMERIC (DuckDB divides decimals into a DOUBLE natively); the engine's expected values are doubles | divide renders as a DOUBLE division — both operands cast BEFORE the division (a cast after keeps the operands' arithmetic: integers truncate, decimals round from an exact quotient); the `1.0 *` trick is retired | corpus lanes DuckDB 108 EXACT / H2 440 (four wavg/round rows retired from the roster); stress H2 1,693 → 2,642 pass (DuckDB 2,702) | fixed |

**Measured today (engine's shape, fresh session per test, all 4,736):** DuckDB 23.2 min at 290 ms/test; H2 95 s at 17 ms/test. Same fixture replay: DuckDB 247 ms, H2 32 ms. Loading levers measured (200k rows): Arrow 8 ms ingest + 52 ms vector build, Appender 102 ms, read_csv 82 ms + file, text 2,654 ms, prepared batch 20 s; 393 tiny tables: Appender 90, text 137, Arrow 256, DDL floor 23 ms; ADBC from Java: the JNI driver (0.21.0) has no executeUpdate — ingest impossible today. Seeding policy decisions (clone vs share) deferred until the load path is right.

### F-AG — numeric step 1 landed (2026-09-17)

The kind decided once, in SQL, at the root (docs/JUDGING_TWO_MODES_2026_09_17.md, step 1;
docs/NUMERIC_CHARTER_2026_09_17.md). DuckDB shared lane 4,689 → 4,700 (20 fail); H2 lane
4,612 → 4,622 (98 fail); zero new rows. The 20 DuckDB residue unchanged from F-AE/F-AF:
headroomToLimit ×2 (DuckDB decimal division), graph DateTime +0000 ×6 (F-W), dateDiff hours
×5 (ruling owed), the five engine-quarantined services (F-V wrong, correction owed), Binding
PM ×2. Corpus rosters: DuckDB 108 EXACT; H2 440 → 430 (12 gained, 2 lost to H2 rendering).
