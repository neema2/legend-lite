# Standing gates — every change cycle runs ALL of these, sequentially

Established 2026-08-02 after the engine-suite audit: 23 tests had been
failing for months because only the corpus runner was gated. The FULL
suite is the acceptance scoreboard — a runner-only cycle is not a gate.

**2026-08-11: the engine module is DELETED.** Its behavioral suite lives in
core (`com.legend.integration`), the corpus runner in `com.legend.rcorpus`,
the server shell in `com.legend.server`. Gate 3 (engine suite) folded into
gate 1; gates 4/5 run `-pl core`. Gate numbers stay stable in
`tools/allgates.sh` so logs remain comparable.

**Numbers below are refreshed 2026-08-06.** Prefer regenerating a report to
quoting one; the ratchet constants in the test sources are the authority, and
they move.

**2026-08-22: GATE 9 added — the ChannelB dual-verdict suites** (all five:
Standard/Essential/Grammar/Unclassified/Relation; discovery pins 287/137,
sql-verdict disagree=0, decline ceilings). Added because the X-slice pushed
with a ChannelB pin unvalidated: the suites were in NO gate, and their
discovery pins depend on `-Dlegend.pure.root`/`-Dlegend.engine.root` SYSTEM
properties (env-only hand-runs silently referee the stale `$HOME` checkout
and fake a 280!=287 "regression" — same trap class as the corpus root).

---

## The root flag is a SYSTEM PROPERTY, and the fallback is silent

`rcorpus/Corpus.java:47` reads `-Dlegend.engine.root`, defaulting to
`$HOME/legend/legend-engine`. It does **not** read the `LEGEND_ENGINE_ROOT`
environment variable — that name exists only for `tools/allgates.sh`, which
converts it into the `-D` flag for you (`allgates.sh:17-20`). Export the env
var and run `mvn` BY HAND and you get the default checkout with no warning.

On this machine that default is a stale July tag with 2,759 test functions
against the real checkout's 2,798, so a hand-run sweep reports a plausible
seven-family "regression" that does not exist. It cost an hour and a false
"main is red" report on 2026-08-08.

**The tells, in the order they appear:** `census: 2759` instead of `2798`;
`h2-exec 0 verified` (the goldens do not match, so nothing verifies); and a
~320s runtime instead of ~90s. Any one of them means the wrong checkout —
check the flag before reading the scoreboard. Prefer `tools/allgates.sh`,
which cannot make this mistake.

## Read this before trusting a green

Three ways this chain reports success without having checked anything:

1. **CI enforces gates 1, 2 and 4 only — and gate 4 skips.**
   `.github/workflows/gate.yml` runs `core clean test`, `core install`, and
   `engine test -Dtest=RelationalCorpusRunner`. It does **not** check out
   legend-engine, so `Corpus.available()` is false,
   `RelationalCorpusRunner.java:55` skips via `Assumptions`, and JUnit reports
   success. **Gates 3, 5, 6, 7 and 8 never run in CI at all.** A green CI badge
   means the core suite passed.
2. **`tools/allgates.sh` has no `set -e` and always exits 0.** It echoes
   `G<n>_EXIT=` lines into `$GATES_LOG` (default `/tmp/gates.log`). Pass/fail
   must be read by eye — the script's own exit code tells you nothing.
3. **Missing upstream checkouts skip rather than fail.** Gates 4, 5 and 8 all
   need `~/legend/legend-engine` (and gate 8 also `~/legend/legend-pure`).
   Without them the tests `Assumptions`-skip, which is **not** a pass. The
   corpus baseline reader is worse: `readBaseline` prints "gate SKIPPED" and
   goes green if `docs/RELATIONAL_CORPUS.md` is unreadable.

**Core must be INSTALLED before any downstream run.** `mvn -pl <module> test`
resolves `legend-lite-core` from `~/.m2`, **not** the reactor — so after
touching core it silently A/Bs the previously installed jar. Use `-am`, or run
gate 2 first. This has already produced a phantom regression report
(2026-08-06: four DIFFs and a collapsed column count that did not exist).

Sequential, never parallel — concurrent heavy JVMs get killed on this machine.
And do not BUILD while a chain runs: a `mvn install` underneath a running gate
swaps the jar it loaded and produces a fake failure (2026-08-08: G8 reported
MATCH 25,142 mid-chain; re-run clean it was 25,472, the baseline exactly).

## Budget decision, 2026-08-10 — gate 8 grew by ~100s

**Four `parser-equivalence` test classes were in no gate and no workflow**, including the
two that pin the programme's flagship claims. All four now run in gate 8.

| class | time | pins |
|---|---:|---|
| `ViewFilterParityTest` | 0.8s | view-filter shapes |
| `CorpusSweepTest` | ~40s | THE consolidated sweep (2026-08-12): whole-document parity + SPI seam + dialect quarantine + leniency classification — absorbs the deleted `PmcdEquivalenceTest`/`StrictDialectParityTest`/`LeniencyCatalogTest` |

> **A measurement warning, learned the hard way.** My first timing put
> `StrictDialectParityTest` at **722s** and I nearly recorded it as unaffordable. It was a
> slept/preempted run — precisely the failure mode this file documents below. Re-measured
> under `caffeinate -dims` it is **34s, 21× faster**. **Never time a gate on this machine
> without `caffeinate`, and treat any outlier as suspect before treating it as data.**

The chain moves **324s → ~424s (7.1 min)**, over the 330s ceiling. Per this file's own
rule that is recorded, not absorbed. Three ways to settle it, all explicit human decisions:

1. **Raise the ceiling to ~430s.** These four gate claims that were previously enforced by
   nothing automated — `DEEP_AUDIT_HANDOFF.md` calls `PmcdEquivalenceTest` "the audit's
   strongest regression net", and it ran in no gate at all.
2. **Take the cut this file already nominates** — gate 5 (41s, the same sweep as gate 4
   against a second backend, scoreboard not written) → ~383s.
3. **Split the chain**: the fast seven on every push, the four heavy parity tests
   pre-push/nightly. Riskier — a gate that runs less often is a gate that catches less.

## Budget decision, 2026-08-12 — the sweep collapse: gate 8 143s -> 50s, chain 5m22s

The user's challenge ("time should have gone DOWN — did the parser regress?")
forced the full decomposition:

- **Lite's parser did NOT regress**: `parseDocument` covers the ENTIRE corpus in
  ~0.5s, and an A/B against the pre-flip commit measured the strict flip
  marginally FASTER (477ms vs 514ms avg).
- The growth was (a) four tests ADDED by the simplification plan (+~35s,
  RefusalSymmetryTest dominating) and (b) the `OracleParses` evict-after-2
  policy silently re-running the whole engine oracle on sweeps 3 and 5
  (~24s each) once five tests consumed it.
- The REAL fix was the plan's own end state, previously skipped: ONE sweep
  (`CorpusSweepTest`, ~39s) replacing six classes and the cache entirely —
  one oracle parse per source, every claim a column, all assertions
  collected. Two slack ratchets surfaced immediately and tightened
  (strict census 258 -> 187, JSON-asymmetry 10 -> 9).

Measured 2026-08-12, full chain GREEN: G1 29s, G2 8s, G4 92s, G5 43s,
G6 76s, G7 24s, G8 50s — **5m22s total, back under the 5.5m ceiling**.
Standing rule reaffirmed: time a full chain after every harness-shape
change; a budget breach is an entry here, never an absorbed drift.

## Budget decision, 2026-08-14 — gate 8 +13s for three new standing gates

An in-chain reading of 380s (6m20s) triggered an audit; most of the
delta was same-day cache/thermal contention (three chains back to
back). Isolated re-measure: G8 63s (was 50) — +6.5s is the actual
test time of THREE new members (`FixtureCorpusParityTest`, 266
vendored sibling sources; `MutationFuzzTest`, 950 live differential
mutants; protocol-check inside the sweep) and ~6s is compiling the
larger core; the sweep itself is unchanged at ~39s. G4 97s / G5 ~50s
(+5-7s each — the Phase-1/2 validation walks now run inside corpus
parsing). Honest chain estimate ≈ **5m45s**. Decision: the ceiling
moves to 6m — 950 mutants + the fixture ratchet + engine-side
protocol validation are the cheapest coverage per second in the whole
chain, and the alternative (sampling them) reintroduces the silent
blind spots they exist to close.

2026-08-14: `GrammarCoverageCensusTest` (the bulletproof-and-total
program's completeness instrument — corpus coverage of the engine's
own grammars, ratcheted; see GRAMMAR_COVERAGE_CENSUS.md) is
TRIGGERED, NOT SCHEDULED: its inputs are both pinned (corpus manifest
SHA + oracle jar version), so its output is a constant between pin
changes and re-measuring a constant every chain is pure cost (~40s).
Run it — ratchets enforced — on exactly three triggers: corpus
manifest change, oracle-pin bump (it is a step of the bump procedure),
or edits to the census itself:
  mvn -pl parser-equivalence -am test -Dtest=GrammarCoverageCensusTest \
      -Dlegend.engine.root=... -Dlegend.pure.root=...
The chain ceiling stays 6m.

2026-08-15 re-pin (post literal-fold, 0e527998): measured chain
5m03-5m06s — G1 28-29, G2 8-9, G4 72-73, G5 35-37, G6 76, G7 24,
G8 59 — back under the 2026-08-08 5m22s best. The fold took G4
89->72 and G5 44->35; ceiling stays 6m as headroom against this
machine's +/-20-30% wobble. Per-mutant oracle instances were
already hoisted (FixtureCorpusParityTest 2.4s -> 0.5s); the next real
lever, if the budget ever binds, is sharing one surefire JVM across
gates 4/5 (the family-sharding speed leg), not thinning coverage.

## Budget BREACH, 2026-09-02 — group F landed at 12m54s; the fix is batch 8

The group F burn (eaf025c9) landed GREEN at **776s = 12m54s**: G1 114s,
G4 173s, G5 196s, G6 158s (parser-only G8/G9 flat). Two causes, both
per-execution or per-compile re-derivation of facts that are constant:

1. **Normalizing the injected system metamodel per model compile** —
   2.3ms -> 28.2ms per compile; ~3,000 compiles in G1. Profiled: 40% was
   `UnionSynthesis.mergedScan` PRINTING syntax trees to compare them
   (quadratic in the 21-member if-chain), 45% an unindexed subclass
   search over the whole class universe per inheritance op. Both FIXED
   in batch 8 (record equality; a direct-subclass index per model +
   native catalog): 28.2ms -> 8.0ms. The residual 5.7ms is normalizer
   re-derivation the boot-layer leg removes.
2. **Seeding ~20 metamodel tables of a corpus-sized graph on EVERY
   store-reading execution** (the four op-tree tables each re-walked the
   whole graph). Batch 8: THE SYSTEM DATABASE (user ruling) — one
   in-memory database per graph per engine, separate from every user
   connection, written ONCE (exec/SystemDatabase, ModelContext.derived);
   the executor ROUTES store-reading bodies to it. DuckDB lane 173s ->
   66s; H2 lane 196s -> 159s.

**Named residue (batch 9):** the H2 lane's remaining 110s is TEN
typeInference tests (9–18s each — the per-test `slowest` ledger names
them): their queries join the `RelationalOperationElement` extent, a
UNION ALL over five store tables (tables/columns/views/table_aliases/
relational_ops), which H2 cannot index (rescan per outer row; DuckDB
hash-joins it in ~1ms). Fix = the store's own idiom: ONE table for the
hierarchy (`kind` column, `id` PK — as data_types/relational_ops already
are) so the extent is an indexed filtered scan, plus the plain-`id` key
read for merged members. Then the boot layer (system elements compiled
once per process). The 5.5-minute ceiling is re-armed when both land.

**Batch 9 + 10 (same day): landed.** Single table (dea642c4) + union
lowering: H2 lane 137s → 41s (standalone; the ten tests <1s each),
DuckDB 61s, G1 ~55s. Remaining over the 5.5-minute line: G6 (PCT)
~105s vs ~90s pre-group-F and G1 ~55s vs 33s — both the per-compile
normalizer residual (5.7ms) the boot layer + per-mapping index legs
remove. Ceiling re-arm stays pending on those two legs.

**Gate-shape decision, 2026-09-02 — Channel B runs ONCE (G9).** Homework,
not script-reading: G6's exact command (`cd pct && mvn clean test` with
both root properties) executed the five Channel B suites alongside the
five PCT suites (1115 tests), and G9 executed the same five classes on
the same two properties (5 tests). The discovery / disagree-zero /
decline-ceiling assertions live INSIDE those classes, so the two gates
asserted the same facts on the same inputs — ~13s of duplicate test
time per chain (ChannelBRelation 4.8s, Essential 3.4s, Standard 2.3s,
Unclassified 1.2s, Grammar 1.2s). Cut: G6 excludes `ChannelB*`
(`-Dtest='!ChannelB*'`) and is purely the PCT suites; G9 stays the one
Channel B run with the roots pinned at the gate and its own log line
(user choice: the dedicated gate is the cleaner home). Measured: G6 86s
→ 82s (1115 → 1110 tests), G9 18s; chain 5m49s — the module's build and
JVM startup dominate G6, so the wall saving is ~4s of the 13s of test
time; the cut is kept for its shape (no fact asserted twice).

**Batch 52 (post-processors as compiler passes: the nonExecutable IR pass; the text verdict arm takes the toSQLString runtime overload with the runtime's table replacements on the rows leg, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 279/2294 → 277/2296 (+2, 0 lost); lane move text-only 25 → 24 (disagree 0); SqlTextVerdicts ledger 669 → 690 (justified). G1 40s, G2 9s, G4 58s, G5 39s, G6 82s, G7 25s, G9 18s, G8 71s.

**Batch 53 (THE COMPILER COMPARES, THE DATABASE COMPUTES — tier-1 unroll with residuals, debugPrint 9 with zero Java value computation; the world map: docs/WORLD_MAP.md + TENET_CHARTER Clause 6, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 277/2296 → 267/2306 (+10, 0 lost; disagree 0); lane move exec-passing 59 → 58 (disagree 0); JavaEvalLedger AssertVerdicts 1511 → 1529 (justified: per-class nested key projection; SQL-canon follow-up named); LiteralUnrollLedgerTest pins the compare-only fold set; StoreNav's host construction set DELETED. G1 40s, G2 9s, G4 55s, G5 38s, G6 83s, G7 25s, G9 19s, G8 71s.

**Batch 72b (objectReferenceIn as a platform program, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9), 6m02s (G1 45, G2 8, G4 67, G5 45, G6 80, G7 26, G9 19, G8 72)** — ratchet 176/2397 → **168/2405** (+8, 0 lost; disagree 0 both channels; exec-passing 7, text-only 15, unable-to-exec 9 unchanged). All eight objectReferenceIn tests flip; the walk's ObjectRefs.java (the Java reference builder + decoder) is DELETED and AssertLedger's `decision:objectReferenceIn` rule with it. MECHANISMS: (1) the engine's generators `generateObjectReferences[ForGivenSetId]` are registered natives whose value is never materialized — Substitution.objectReferenceInRewrite reads the SPELLED pk maps (`pair(k,v)->newMap()`, collections of maps, collections of generator calls) straight off the typed call and emits the same pk-membership predicate (ObjectReferenceArms.generatorPkMaps); the harness JSON-array carrier arm is gone. (2) A LET-BOUND graph tree literal CLOSES over the lets in scope (GraphFetchChecker: `biTemporalClassification($processingDate, $businessDate)` serializes as `biTemporalClassification(2017-06-10, 2017-06-11)` — real pure evaluates the literal at its let); a tree spelled INSIDE the query lambda keeps its variable spellings (`classification($bd)` — the plan's open variables). A first attempt resolving the variable at EMISSION time broke seven graphFetch tests (testSnapshotMilestoning, testInScopeVariableBoolean, testMultiLevelMilestoning ×2, testBiTemporalProperty, CrossStoreGraphFetchWithRelationalMilestoned ×3) — the distinction is WHERE the tree was spelled, measured and reverted. (3) `decodeObjectReferencesAndGetPkMap` is a DATABASE expression: ObjectReferenceDecode (resolver pass after resolveNode) finds the serialized frame the map reads, takes the DEFINING mapping its reference prefix names (AsorRef.prefixMapping) and spells that mapping's include-closure `setId -> [pk columns]` (declared ~primaryKey, else the set's table key through the typed reader RelationalRootForm.primaryKeyColumns — never the raw AST: ArchitectureTest 6c' caught the first draft) as a literal argument of the lite reader `asorDecodePkMap(ref, table)`; AsorReaders lowers it as base64 decode + the framing regex + a CASE over the spelled set ids + json_object. (4) References computed AT RUNTIME (UsingResultReferences: a column of an earlier serialize result, `->take(3)`) = `in(pk, refs->map(r | asorPkValue(r, 0)))`, the reader typed as the pk column so the SQL casts the decoded text; the earlier result reads as a closed from() inside the predicate — SubQueryLift.resolveClosed runs on filter predicates before the row substitution, and the nested resolution keeps the OUTER serialize's envelope state (StoreResolver.resolveNested — the inner query's objectReference channel had leaked into the outer envelope: `got [objectReference, value]`). (5) DECODE_BASE64 renders `decode(from_base64(..))` — a CAST of the blob to VARCHAR ESCAPES quotes (`\x22`) and the JSON cast refused it (probed on the DuckDB jar). (6) H2 has NO base64 functions (probed 2.1.214: BASE64/FROM_BASE64/BASE64DECODE all absent) — these JSON graphFetch tests verdict on DuckDB only, as before. TRAP found and fixed: the walk's SQL_TEXT_OUTCOME thread-local is per ASSERT now (reset at checkAssert entry) — an earlier assert whose classification returned early (plan-let) lent its "plan-literal" to the next test with the run order (testGroupByWithJoinDB2 moved lanes in the full run only). Guardrails: INTERNAL_DESUGAR 14 → 16 (asorPkValue, asorDecodePkMap — the reference readers), native catalog golden regenerated (+5 rows, deliberate), Substitution/StoreResolver/Scalars kept under 3500 by extraction (ObjectReferenceArms, PipelineWalks, AsorReaders). Pre-existing fidelity gap stated, not widened: our reference prefix embeds a CONSTANT test-H2 connection JSON (AsorRef.CANONICAL_H2_CONNECTION), not the runtime's. NEXT: leg B connection equality (5 tests): admit the engine's storeContract.pure as a LIBRARY file (WORLD_MAP rule 5 — the relational arm, compareObjectsWithPossiblyNoProperties, postProcessorsMatch are programs there), fold `relationalExtensions()->routerExtensions()` to the spelled store contract, prepend a folded non-empty dynamic arm prefix as spelled arms in the inliner, and answer hierarchicalProperties from the system store's classes/properties rows (both tables exist; the Class metaclass mapping lacks a `properties` navigation).

**Batch 72a (the four small walk-only legs — malformed goldens named, the self-alias let, the statement-root map unroll, positional relation concatenate, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9), 5m05s (G1 43, G2 9, G4 64, G5 48, G6 84, G7 26, G9 19, G8 72)** — ratchet 179/2394 → **176/2397** (+3, 0 lost; disagree 0 both channels); lane moves exec-passing 9 → 7 and M1 rescued floor 9 → 7 (testBusinessDateInjectionFromVarReference's two assertSameSQL rescues became platform-arm verdicts when the test flipped); JavaEvalLedger AssertVerdicts 1599 → 1605 (a message wrapper, no evaluation). Homework first: docs/WALK_ONLY_PLAN_2026_09_05.md researched every "walk-only" test one by one (engine source, walk mechanism, exact wall) and CORRECTED the batch-71 count — 16 walk-only, not 33 (17 fail in both channels; the tmp census mislabeled them); its §4 audit then re-verified every claim by receipt (two temporary debug prints, reverted). LEGS: (F) both testMilestonedRootAndMilestonedProperty goldens end in `]"` — the engine passes because json-simple returns after the first complete value (probed on the 1.1.1 jar); our rows are byte-identical up to that tail (probed); AssertVerdicts names the golden side ("golden JSON does not parse") and AssertLedger registers `engine-golden-defect:malformed-json-golden` ×2. (D) the two-round test-data generation walled because the inlined `loadAndTestExecution($query, [], $mapping, …)` helper re-bound `let query = $query` under the caller's let name and every later structural consumer saw a Variable (traced with a temporary print: the alias pointed at line 1020 col 25 — the helper call's argument); SpecCompiler.typeQueryBody re-binds a statement-level SELF-alias to the outer alias — lambda-local `let v = $v` shadows (the plan printer's injected Allocation lets) are untouched; a first attempt in Env.withLet broke 18 plan-text goldens and was reverted. (C) `[$result, $result2]->map(r | let orders = …; assertEquals(…); assertEquals(…);)` at statement root unrolls to its element statements with per-element let names (LiteralMapUnroll at the query front door, beside the validate desugar) so the asserts reach the statement-root verdict channel. (E) USER RULING: relation concatenate types POSITIONALLY like the engine's relational lowering (processConcatenate, pureToSQLQuery.pure:2709 — same arity, position-wise compatible types, the LEFT operand's names; a right operand with other names is spelled as a TypedRename onto the left's) — the in-memory tds.pure:483-487 name assert is that implementation's own runtime check; the rule lives in ConcatenateChecker, never in the generic T-binding (InferenceKernel wall stays for every other signature). TRAP found and fixed: the checker's first draft synthesized its first argument twice (once itself, once through checkGeneric) and re-registered typer state (TDS literals rendered as bare VALUES lists, plan parameters unbound) — Typer.checkGenericTyped now takes ALREADY-TYPED arguments so a checker that must read a type before choosing its rule synthesizes each argument exactly once. TRAP found: a corpus run without `-Dlegend.engine.root=/Users/neemsandv/…` scores 2536 tests instead of 2575 against the stale $HOME checkout and shows phantom regressions — recorded in memory. NEXT: leg A (objectReferenceIn as a platform program, 8 tests, plan §1.A) then leg B (connection equality, 5 tests, plan §1.B) — after those the walk-only set is empty.

**Batch 69 (THE DELETION: the walk's text-only "verified" passes are advisory, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 179/2394; disagree 0 both channels; NO family baseline moved; lane unable-to-exec 8 → 9. USER DIRECTION (2026-09-04): "delete the harness code that does the platform's job … if the harness was cheating or hiding results it's probably okay to regress to the truth", ordered AFTER fixing everything fixable (batches 69a-c, 70, 71 — the SQL-text lane residue is fully named: 6 contains, 4 engine defects, 2 forced-isolation decision, 1 no-fixture). THE THREE RETURNS (EngineTestExecutor): (1) a byte-equal golden the referee could not replay returned null = "verified" ("match-noreplay", sqlTextVerify) — now ADVISORY_MARKER; (2) the same in the H2-match catch (Unverifiable) — now advisory; (3) `assert($sql->contains(...))` evaluated over OUR OWN generated text and returned null = "REAL verified pass" — now advisory ("predicate-held"). An advisory assert is neither pass nor fail: a test keeps PASS on its row asserts and drops to SHAPE only when text agreement was its sole verification — measured: no such test moved a family count (the contains tests carry row asserts; the byte-equal-no-replay cohort was burned to row verdicts in batches 64-69c; testProp3's two plan-text asserts ride the H2Compatible route, unaffected). The walk's "verified" counter is truthful now: every verified assert is a row or value verdict.

**Batch 71 (fetchDb primary keys the ENGINE's way: constraints on the native, a live-catalog key grid, the model fact-walk deleted, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 179/2394; 0 failed seeds; disagree 0 both channels; the four fetchDb tests pass. USER Q: "why do we need to join across metamodel data and user data?" → we don't, and the metamodel store was the wrong place: legend-pure's fetchDbPrimaryKeysMetaData IS DatabaseMetaData.getPrimaryKeys over the live database, and the engine's tests create their tables through dropAndCreateTableInDb with applyConstraints=true (extensionDefaults.pure:611 emits NULL/NOT NULL and PRIMARY KEY(<metamodel names, raw>)), so the physical key exists there; the engine's compiler builds Table.primaryKey from the DECLARED key columns only (HelperRelationalBuilder:449 — no milestoning columns added, answering the user's in_z/out_z question). OURS stripped every constraint from model-derived DDL — a harness decision made for the ambient CsvSeed (multi-version milestoning rows) and applied to the native too. FIX: Ddl.createTable(def, schema, flavor, constraints) — the dropAndCreateTableInDb native (and its H2 mirror) emits the engine's constraints; the key list spells each column as its definition was spelled in the flavor (the ONE seed failure the experiment surfaced: datePeriods' PRIMARY KEY("date","calendar name") — the engine's metamodel name carries the quotes, our model unquotes and stamps quoted()); CsvSeed stays unconstrained (no engine counterpart). CatalogGrids.PRIMARY_KEYS = information_schema key_column_usage ⋈ table_constraints (constraint_type = 'PRIMARY KEY'), dialect-neutral like the three sibling grids — probed identical on DuckDB 1.4.4 and H2 2.1.214 (ordinal positions, quoted names). DELETED: pkSql (the VALUES splice), pkFacts, collectPks, tablePks, findDbRef, and the batch-69c typer let-channel lookup (CatalogGrids.sql(nc) needs no context). Shadow-SQL register CatalogGrids 9 → 10 (the live key query). USER RULINGS recorded: fetchDb* = physical introspection (drift detection / schema import — "things NOT defined through our platform"), the live catalog is their truth; the store answers what the MODEL declares; the system database is separate by ruling (no statement reads both); Runtime/ConnectionStore rows = purity for its own sake (values, not elements). Batch 69 (the deletion) measured before this: ratchet unchanged, lane unable-to-exec 8 → 9 only — NEXT.

**Batch 70 (the isolation family's join rule, and the correction that reshaped it, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 179/2394; disagree 0 both channels; no pin moved. THE RULE (user-ratified after reading the engine's isolation code and pure's plus): in value position the join kind follows the mapper body's per-parent multiplicity — a body that IS the flattened navigation keeps the row-dropping INNER join (pure's flattening drops non-matching parents; the 4 tests::map shapes, byte-identical); a body that REDUCES a BARE many-valued read to one value per parent (`$o.children->filter(..).name->joinStrings(',') + 'T'`) joins LEFT with its predicate in-target, one value per parent ('T' for a childless org) — SyntheticHeads.liftValueMapFilter/liftValueRead, witness ValueMapPlacementTest.bareManyReduceKeepsParents (1 LEFT OUTER JOIN, [BetaT, T, T]). THE CORRECTION (measured: the broad rule broke four placement pins, [BetaT] → [BetaT, T, T]): the structure qualifier `employeesByCityOrManagerAndLastName` ends in `->toOne()` (Person[1]) — for a firm with no matching employee pure's `[]->toOne()` is a RUNTIME ERROR, so pure has NO answer and my earlier "4 rows is pure's answer" was wrong; the engine never raises it relationally and its two strategies are two conventions for the undefined case: the DEFAULT drops the parent (1 row — the structure goldens, row-identical to ours and to every measured cell, the six placement pins), the FORCED debug strategy (RelationalDebugContext.forcedIsolation = BuildCorrelatedSubQuery) keeps it with a NULL (4 rows). So a toOne-NARROWED read stays INNER; the forced pair are a DECISION row (`decision:empty-toOne-forced-isolation`, AssertLedger register — a registered "decision:" bucket is used verbatim), and the two default goldens are NOT defects (the batch-69a receipt calling our 1-row frame "wrong" is superseded). SPEC GAP noted: real pure's `String[*] + String[1]` (plus(strings:String[*]), the corpus's `.firstName + 'Test'` over a many read) does not type in our typer (binary [1] string plus only) — the witness uses joinStrings; a typer leg. STILL OPEN in this family: isolationTest's depth-3 correlated predicate (a loud wall since 69b).

**Batch 69c (the two remaining named legs fixed: fetchDb primary keys, the datePeriods chained group-by, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet 181/2392 → 179/2394 (+2: testFetchDbPrimaryKeysMetaData, testGroupByWithFilterFunction_noDatePath; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G1 44s, G2 8s, G4 65s, G5 45s, G6 79s, G7 25s, G9 18s, G8 71s. USER: "Can we actually fix the remaining three or not?" → "Yes do it, but also the probe for 3 and the fix for 2 and then this". FETCHDB PRIMARY KEYS — two causes, both named by the probe: (1) the constructed `^Runtime(connectionStores=...)` lowered as a struct whose canonical layout demanded a SQL slot for `Runtime.preprocessFunction`, a Function-typed property — ClassLayouts now skips FUNCTION-typed stored properties (code, not data; no SQL carrier, no slot); (2) the PK grid composes the store's key facts at TYPING and must find the database inside the connection argument — for `let connection = runtime().connectionByElement(db)->cast(..)` the walk met a variable with an EMPTY let table; CatalogGrids.sql/pkFacts/findDbRef now take a lookup function and the Typer hands them its let channel (exprAlias → synth on demand), so the walk reaches `db`; the call types as the catalog relation like its three siblings. DATEPERIODS — the agg helper's `$reportEndDate.day` (a let bound to `FiscalCalendarDate.all()->filter(d | $d.date == $endDate)->toOne()`) reached substitution unlifted: SubQueryLift lifts a LET-BOUND instance read as the same uncorrelated scalar subquery a written-out chain gets; the lift now runs on the execute()/driver route as well (only the from() arm lifted) and STOPS at a TypedFrom (a from() carries its own mapping context — the driver-route statement `toCSV(from(...))` lifted the calendar read under no mapping and dispatched to a 0-mapping runtime); our SQL reads the calendar values as scalar subselects where the engine inlined the constant 37 — row-equivalent. Its toSQLString assert over the chained plan: the engine's index-less rendering prints statement 0 + "Warning: Results only shown for first relational query…" (relationalMappingExecution.pure) — the referee strips the warning (a spec-text shape, like the population statement's) and the toSQLString arm accepts MULTI-statement lambdas: golden(0) = statement let 0's own rows, the lambda's lets scope the rows leg. PINS: lane exec-passing 10 → 9, text-only 16 → 15, unable-to-exec 9 → 8 (the one test left three walk lanes), ledger SqlTextVerdicts 1011 → 1035. USER Q (the metamodel store): today's PK grid is Java collecting store facts into literal rows — the end-state under the metamodel-as-relations ruling is a key fact on the Column rows (or a table_keys relation), Runtime/ConnectionStore rows, and ONE SQL from the connection's store to its keys existence-filtered against information_schema; recorded as that program's next leg.

**Batch 69b (isolationTest: a wrong answer becomes a named wall, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 181/2392 (isolationTest was already a fallback; its ledger row moves from `divergence` — 9 rows for 5 — to `wall:resolver`); disagree 0 both channels; no pin moved. TRACED (temporary prints, removed): the projection column `$x.employees.group.children->filter(c | $c.coveredProduct.name == $x.employees.product.name).name->toOne()` parks its predicate as a CORRELATED pred on `children#f` (it reads the outer row); the predicate's own outer read `$x.employees.product.name` registers the nav path employees.product.name FIRST and demands the `employees` slot; the column's chain employees.group.children#f then reaches the reroute trigger with its parent alias already demanded, so the #69 parent-copy reroute (the only route that applies a tail-hop correlated pred) is skipped and the chain continues onto the slot spine — where NavMaterializer never parks a sub-hop correlated pred in-target. The predicate vanished and the join answered with every child. FIX (mechanism): StoreResolver.unappliedCorrelatedWall — any tail-hop correlated predicate the reroute did not take walls LOUDLY (depth and chain named), before the demanded-alias skip and inside the reroute for hops deeper than the first tail hop (the tail loop's reach). The leg proper (apply a correlated predicate at depth ≥ 2 of a chain, and reroute a chain whose parent alias a plain path already claimed) is named in the handoff.

**Batch 69a (fix-before-delete: the walk-passing SQL-lane residue, one by one, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet 185/2388 → 181/2392 (+4: testSQLQueryMergingForInnerJoins, testSQLQueryMergingForInnerJoins2, testPlanForDateTimeVariableESTTimeZone, otherwiseTestQualifierPropertyConstantExpression; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G1 43s, G2 8s, G4 63s, G5 46s, G6 78s, G7 25s, G9 19s, G8 73s. USER ORDER: "We need to fix all the other 9 first before we delete — referee replay, rows differ, odd rows". FIXED: (1) the union sqlQueryMerging pair — the expected `^TDSNull()` literal and our null cell now meet on ONE spelling: the Typer's many-stamped rows.get desugar yields the ^TDSNull() INSTANCE (not a 'TDSNull' string), and MixedEncoding.lubCase spells a [1]-stamped NULL branch of a mixed if as the JSON null VALUE (the variant lane's value law), so the cell survives list_filter and equals the literal — tds 255/266 and union 123/127 family sweeps unchanged; (2) the zoned plan's processingTemplateFunctions — ConnectionFlags.timeZoneOf resolves the corpus helper's TWO lets and its PARAMETER let (PlanAllocations passes the let prefix): the GMTtoTZ/renderCollectionWithTz pair is emitted for 'US/Arizona'; (3) an assert-free body whose let RUNS a store query (`let r = execute(...); true;`) is the engine's countable work — WholeTestFlip counts it, the platform runs the embedded-otherwise filter and passes. TRUTH RESTORED: the H2Verify FORCED value-frame guard is DELETED — re-measured with the fixture read, the forced golden for testQualifierWithOperation LEFT-joins the isolated filtered employee subselect onto firmTable and yields 'PeterTest' + three 'Test' (pure's plus(String[*]) ignores an empty operand: one value per firm) — that IS pure's answer and our one-row INNER-joined frame is WRONG; the pair are honest `divergence` rows now (exec-passing lane 12 → 10), and since the walk's referee shares the guard the walk fails them too: tests/advanced 66 → 64 RE-BASELINED BY HAND (the regression is the truth surfacing). datePeriods testGroupByWithFilterFunction_noDatePath: the engine's plan is ONE STATEMENT PER STORE-BACKED LET of the query lambda — SqlTextVerdicts.statementRoute/statementLets route golden(k) to let k's own rows (shared by the exec-read and H2Compatible arms; the H2Compatible verdict tail extracted so an n-th read routes there): golden(0) = the calendar instance select VERIFIES; golden(1) now reaches the resolver and walls loudly ("filtered-navigation read 'day' reached substitution unlifted") — a named platform leg. RECEIPTS: columnValueDifferenceWithoutPrevalTest → `engine-golden-defect:alloy-adjust-widening` (it is <<test.AlloyOnly>>; its interpreter sibling columnValueDifferenceTest asserts the SAME relational rows with the date-only spelling — the executor's H2 dateadd comes back a TIMESTAMP); testProp3 → `referee-cannot-replay:no-fixture` (its m2m2r schema has no setUp anywhere in the engine — plan-text by construction). PINS: ledger SqlTextVerdicts 919 → 1011 (routing), M1 rescued floor 11 → 9 (the union pair's walk rescues cleared), exec-passing 12 → 10. OPEN (named, next): the ISOLATION FAMILY design — SyntheticHeads.liftValueRead parks a value-position filtered navigation as an INNER join to match the engine's DEFAULT strategy, which drops parents; pure and the engine's FORCED strategy say LEFT + isolate whenever the mapper body reduces to one value per parent — a user decision (it moves the non-forced goldens to engine-golden-defect); isolationTest's correlated predicate is dropped silently on the corrPreds path (must wall loudly at minimum); testFetchDbPrimaryKeysMetaData = a Function-typed value at the lowering boundary.

**Batch 68 (the instance OVER-FETCH fixed by the engine rule, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet 187/2386 → 185/2388 (+2: testQueryOfMilestonedTypeWithFilterInMapping, testQueryOfMilestonedTypeUsingLatestWithFilterInMapping; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G1 44s, G2 8s, G4 63s, G5 48s, G6 83s, G7 26s, G9 19s, G8 74s. USER (2026-09-05): "for graph, is it feasible to fix our over fetch instead of compensate?" — FIXED, no compensation. THE ENGINE RULE: a set's instance select projects the set's OWN property mappings; a property the child does not map is served on access through the declaring ancestor's set (per-property routing), never fetched with the instance. OURS: the implicit same-extent inheritance pre-pass (ImplicitInheritance.apply) merges the ancestor's unqualified mappings into the child set so demand reads work — and the bare-root serialize envelope (GraphEmission.synthesizeScalarTree) projected EVERY binding, so StockProduct over milestoningmap fetched the Product set's stockProductName (a two-hop join) and classificationType (an enum join) beside its own id/name/type; the referee declined "graph keys mismatch golden aliases: golden [id, name, type] vs frame [...]". MECHANISM (fact lifetime = the compile artifact): SetKeyFacts stamps the set's OWN property names into ClassBinding.DeclaredKeys.ownProperties BEFORE the merge (beside the own key text it already captured); ClassSources.ownPropertiesOf answers it for an extends-less relational binding (an explicit `extends` set carries its parent's mappings by contract — no restriction; function-form/unknown bindings declare none); the envelope skips a binding not in the set's own names — the merged binding stays for `$sp.stockProductName` on demand. Lane pin unable-to-exec 11 → 9 (the two graph-keys declines are row verdicts). NOT over-fetch (measured, stay): testGraphFetchWithTableMapperPostProcessor (post-processor table mapper: golden employees=[] vs ours 4 — the mapper is not applied), testCheckedWithCircularConstraints (checked defects: constraint evaluation).

**Batch 67c (testHashFunctions traced — the same joinStrings defect, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 187/2386. USER: "now we need to look at hash again maybe we can burn that one down too with some debug/looking at engine code". PROBE (LEGEND_LITE_DUMP_SQL=1, scoped): the golden's own text renders the test's `joinStrings([$firstName, $lastName], '|')->hash(MD5)` column as `rawtohex(hash('MD5', concat("root".FIRSTNAME, "root".LASTNAME, '|')))` — the trailing separator again; H2 and DuckDB both return 7 rows and lowercase hex; the golden-only row is Anthony Allen whose cells are md5('Anthony') = 20f1aeb7…, md5('AnthonyAllen') = 581ffb57… twice (plain concat and joinStrings with '' separator agree), and `tds_digest` aceae941… = md5('AnthonyAllen|') where ours is 0a8c4f1f… = md5('Anthony|Allen'). Nothing else differs — NOT hex case, NOT the SHA spellings. So the test is registered under `joinStrings-rendering` (now ×4 + the digest pair). The 67b record's "NOT registered" line is superseded by this trace.

**Batch 67b (the `engine-golden-defect` ledger bucket, 2026-09-05): chain GREEN (gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 187/2386 (a ledger NAME, no verdict moved; the four tests still FAIL on rows); G1 ~43s, G4 61s, G5 42s, G6 85s, G7 25s, G9 19s, G8 73s. USER RULING (2026-09-05): joinStrings and firstDayOfWeek "are both just broken on H2 basically? … quarantine/bucket those as engine bugs" — they are engine bugs of two different kinds and the bucket records which: `joinStrings-rendering` = the engine renders `joinStrings([a, b], sep)` as `concat(a, b, sep)` on EVERY dialect (separator trails: 'PeterSmith|'; the digest goldens are md5 of that string) — testToSQLStringForTDSStringJoin, testExtendDigest_Relational, testJoinWithExtendWithDigestOnColumnsOnBothQueries; `h2-week-start` = under the engine's `date_trunc('week')` H2 starts the week on Sunday while Pure's own dateExtension tests and DuckDB say Monday — the engine's H2 dialect fails to normalize; testToSqlGenerationFirstDayOfWeek. MECHANISM: AssertLedger.ENGINE_GOLDEN_DEFECTS, keyed by EXACT test FQN, consulted only when the platform produced rows that differ (a wall stays a wall; a pass never reaches the ledger) — a register row can hide nothing, and each carries its receipt in the charter §8.0. NOT registered: testHashFunctions — it hashes `firstName + lastName` (plus, not joinStrings); its 7-row divergence is unexplained and stays `divergence` until traced (our H2 spelling is the engine's `rawtohex(hash('MD5', x))` — the next probe).

**Batch 67 (one by one through the remaining rows: the two-statement in-list plan as rows, assert-free bodies on the platform, 2026-09-05): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 190/2383 → 187/2386 (+3: testInExecutionWithTempTableAndQueryChaining, …OnIntegerColumn, twoDBRenameColumns; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G1 43s, G2 8s, G4 64s, G5 45s, G6 83s, G7 25s, G9 18s, G8 72s; chain ~6m. USER (2026-09-05): "can we do golden(0) through the same replay mechanism to compare rows instead of shapes?" — YES: golden(0) is the population statement of `let v = <to-many expr>` inside the query lambda, so its rows ARE that let's value — the exec-read arm evaluates the let's expression (wrapped in the frame's mapping, through the one router) as the rows leg and the oracle replays golden(0) against it; golden(1) reads `tempTableForIn_<v>`, which the oracle fills from the attempt's remembered population golden (SqlReplayOracle.TempTable kind "population" — `INSERT INTO tempTableForIn_<v> <golden(0)>`); the arm owns `sqlRemoveFormatting($res, n>0)` only for this shape (VerdictQueries.firstStatementRead mints the index-0 read — Invariant 7 caught the arm minting it). ASSERT-FREE: a body WITH statements (prints included) runs through the platform; a clean run is the engine's own "N statements executed" pass (WholeTestFlip reports statements.size(), never SHAPE — G4's family baseline caught the SHAPE scoring); only a body with nothing to execute stays a named zero-assert row. MEASURED, NOT BURNED: the forced-isolation value-frame guard was lifted and restored — the forced golden yields 'PeterTest' + three 'Test' (H2's concat treats a NULL operand as ''), not droppable NULL rows, while the engine's own value assert runs the default strategy; a toSQLString driver that is neither an enum literal nor a runtime is never assumed H2 (foreign-dialect residue). Lane pins moved as migration: exec-passing 14 → 12, unable-to-exec 13 → 11 (the two statement-pairing arity rows burned), ledger SqlTextVerdicts 842 → 919; SqlTextRatchetTest registers the arm's ONE `select distinct` recognizer (a referee's read of the golden's shape). NAMED for the next commit (user ruling 2026-09-05, "quarantine/bucket those as engine bugs"): an `engine-golden-defect` ledger bucket with receipts — joinStrings rendered as `concat(a, b, sep)` on every dialect (the digest golden's md5 is of 'PeterSmith|'), and H2's Sunday week start under the engine's `date_trunc('week')` where Pure's own tests say Monday.

**Batch 66 (the golden PLAN replayed node by node; the eleventh chained TDG test, 2026-09-05): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 193/2380 → 190/2383 (+3: testMapWithOpenVariable, testExecutionPlanForQueryWithVariableRundateWithinLambda, testQualifier; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G1 43s, G2 9s, G4 64s, G5 44s, G6 80s, G7 24s, G9 18s, G8 72s; chain ~6m. USER (2026-09-05): "fix the rest of the ones we can first before deletions" and "for plan text we have a way to extract the sql and run rows". THE PLAN REPLAY: `SqlReplayOracle.verifyPlan` (harness PlanReplay behind the SPI) runs a golden plan text's nodes in order — an `Allocation` binds its name to a `Constant`'s literal values or to a `Relational` node's rows fetched on the oracle; the later nodes' `${...}` holes fill from the bindings (a name, a `name.column` read of a bound row, the freemarker `?replace` builtin, or the engine's own template helpers evaluated by their published bodies in relationalMappingExecution.pure: collectionSize, renderCollection, varPlaceHolderToString, optionalVarPlaceHolderOperationSelector, GMTtoTZ = PlanDateParameter's GMT→zone move printed in the input's pattern); the final Relational node's filled SQL replays for rows. A hole ends at ITS closing brace (map arguments nest); nested calls parse by balanced parentheses; the one-line plan spelling separates `connection =` by spaces. The arm: collection parameters bind two referee elements (VerdictQueries.refereeBindings lists); a plan lambda's leading lets scope our rows leg (they were out of scope — "rows underivable"); the chained-TDG hop finder sees through `sqlRemoveFormatting(String)` and the oracle's transcript receipt compares flattened text (testQualifier). Lane pins moved as migration: exec-passing 17 → 14, text-only 17 → 16, rescued floor 14 → 11, ledger SqlTextVerdicts 830 → 842. NAMED residue (charter receipt): fixture-less plans (testGroupByWithOpenVariableInAgg ×2 — the engine never executes them, SALES_GCS exists nowhere), planToStringWithoutFormatting goldens (SQL without spaces), enumMap_* / renderCollectionWithTz template operations, testPlanForDateTimeVariableESTTimeZone's template-function-list assert (its plan assert now row-verifies). G1 caught a JDBC-surface drift (PlanReplay named java.sql.Timestamp; the oracle owns JDBC — removed).

**Batch 65 (the inline in-list temp table as a ROW verdict, 2026-09-05): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 197/2376 → 193/2380 (+4: testInExecutionWithTempTableFor{DateTimes,Dates,Numbers,Strings}; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G2 8s, G4 67s, G5 46s, G6 85s, G7 26s, G9 18s, G8 72s; chain ~6m. The engine's `tempTableForIn_N` (numbered by plan node) holds the query's `in([...])` literal; the golden reads it and the oracle's H2 never had it ("Table TEMPTABLEFORIN_4 not found" — the walk synthesized it as extraSeeds, the platform arm did not). Now: the platform arm reads the literal off the frame's typed query (SqlTextVerdicts.inListTemps — the frame's execute call resolved through the splice hook rides FrameFacts.query to the rows leg; exactly one inline in-collection, one numbered temp in the golden; kinds date/datetime/string/integer with the literal's Pure spelling) and hands the oracle a `SqlReplayOracle.TempTable` spec; the oracle (ReplayOracle.tempSeeds — the walk's literalTempSeeds) spells the H2 temp as per-verify statements (drop-first, `ColumnForStoringInCollection`, DATE/TIMESTAMP/VARCHAR/BIGINT) through verifyAuto's extraSeeds — never the mirror's cursor. A first cut searched the rows leg (`$result.values`) and found nothing: the query sits behind the frame variable. Lane pins moved as migration: exec-passing 21 → 17, rescued floor 18 → 14, ledger SqlTextVerdicts 765 → 830. NOT burned, by design (charter receipt): the population-golden temp (2 tests) is a two-statement engine plan vs our one statement; forced-isolation VALUE frames (2) pin an engine debug strategy; the graph-keys mismatch (1) is our frame over-fetching — our bug, kept loud.

**Batch 64 (the chained generator fetch as a ROW verdict — the walk's mechanism behind the oracle SPI, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 207/2366 → 197/2376 (+10: the chained testDataGeneration tests testSimpleTwoTable, testSimpleTwoTableMultipleStartRows, testSelfJoin, testUnion, testUnionToUnion, testInheritanceMultipleTableJoin, testTableToTDSMultipleJoins, testTableToTdsWithJoinAndOLAPGroupBy, testTableToTdsWithJoinAndUnion, testTableToTdsWithJoinToSameTable; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); G2 8s, G4 63s, G5 46s, G6 81s, G7 25s, G9 19s, G8 72s; chain ~6m. USER DIRECTION (2026-09-04, after the one-by-one review of how the walk scores its passes): delete the harness code that does the platform's job; the golden-SQL asserts the referee CAN replay become row verdicts at the platform seam first. THIS BATCH: the walk row-verified chained generator fetches (tdgChainedVerify) but the platform arm declined them ("chained fetch — generator temp tables not replayable") — a plumbing gap, not a semantics gap. `SqlReplayOracle.verifyFetchChain(session, hopIndex, golden, ours, transcript)`: the platform arm (SqlTextVerdicts.tryArmTdgSql) addresses a hop by its `$testData.sqls->at(i)` index and the let-bound generator node (the fold runs at the let's own execution, so the let still holds the call); the oracle remembers each hop's golden for the attempt (cleared in beginAttempt), materializes every ancestor `testDataGen_Temp_<T>` from that ancestor's golden root-first (the engine fills each temp with the parent fetch's rows), runs the hop's golden and multiset-compares the hop's transcript rows — the generator re-run (TestDataGenerationNatives.transcript, deterministic reads over static seeds) under a byte-exact text receipt. The transcript crosses the SPI in exec's own terms (FetchTranscript/FetchHop — an exec → testdatagen dependency was an Invariant-4 cycle, caught by G1). Lane pins moved as migration: exec-passing 55 → 21, M1 rescued floor 52 → 18, evaluator ledger SqlTextVerdicts 690 → 765 (routing, no evaluation). testQualifier (the eleventh) spells its hop-0 golden as `sqlRemoveFormatting('literal')` and stays on the walk for now.

**Batch 63 (the joined table's scan order, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 210/2363 → 207/2366 (+3: testProjectWithIfWhereBothSidesUseTheSameEnumMapping, testProjectWithIfWhereOneSideIsEnumLiteral, testProjectionWithEnumThroughAssociation; 0 lost; sql-verdict disagree 0; dual-channel disagree 0; lineage-rows agree=66); G1 44s, G2 8s, G4 66s, G5 45s, G6 82s, G7 25s, G9 19s, G8 73s; chain 6m02s. THE RULE: the engine-corpus-compat scan-order key (ScanOrder; StableScanOrder is flag-gated, host channel only — the platform default stays order-honest) is LEXICOGRAPHIC over the join tree's base-table scans in join order (driving rowid, then each joined base table's rowid; frames contribute no key) and now covers plain-table joins. H2's nested-loop join emits the driving scan's order and, within one driving row, the joined table's scan order; DuckDB's hash join does not (Product ⋈ Product_Synonym, synonyms 11→P1, 12→P2, 13→P1: H2 reads (P1,11),(P1,13),(P2,12) — the three enum tests' rows->at(i)). Measured: no other test's rows moved (dual-channel disagree 0; row-order-canon 16 → 15). H2 RECEIPT (jshell, h2-2.1.214, the enum fixture rebuilt: Product 1,2; Product_Synonym 11→1, 12→2, 13→1): `select p.prod_desc, sy.id from Product p left outer join Product_Synonym sy on sy.product_id = p.id` returns (My Product, 11), (My Product, 13), (My Product 2, 12) — the key is a measurement of H2, not an inference from the golden.

**Batch 62 (the join chain's terminal column is read at the chain end, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 211/2362 → 210/2363 (+1 testIsolatioWhereNoConstaintsAndInnerJoin; 0 lost; sql-verdict disagree 0; dual-channel disagree 0; lineage-rows agree=66); G1 44s, G2 8s, G4 64s, G5 44s, G6 78s, G7 24s, G9 18s, G8 70s; chain 5m50s. THE RULE (pureToSQLQuery.pure resolveJoinElement): a property mapping's `@J > @J | table.COL` terminal is re-resolved in the JOINED cursor — `reprocessAliases(OldAliasToNewAlias(tac.alias -> op.alias))` for a plain column, the extracted columns for a DynaFunction — so the spelled table is grammar. The golden reads `"persontable_0".ADDRESSID` (exported from the isolated bridge⋈person subselect) for a mapping that spells `| firmTable.ADDRESSID`; ours resolved the spelled name against the ROOT row and lost the chain's fan-out (4 rows for 7). RelOpTranslator.joinNavigation now rebases terminal column refs that the chain end DECLARES to the chain end (Pipeline records each slot's target column names at hoist time, table or view); a column the target does not declare stays where spelled — TestMappingWithViewJoins' `| firmTable.LEGALNAME` after a hop onto a view without LEGALNAME reads the root (the engine's extracted-column projection finds it there); the unguarded first cut regressed testView + testViewWithJoinsAndDistinct exactly there. KNOWN GAP (not a burn): a chain's `(INNER)` hop still emits LEFT OUTER (the engine isolates `LEFT OUTER JOIN (bridge INNER JOIN person)`); rows agree on the fixture. USER RULING recorded this batch: batch 61's PCT trade (two domain-error tests as expected failures, the engine's relational-adapter precedent) is KEPT — "you did the right thing"; such trades are surfaced before committing from now on.

**Batch 61 (acos/asin = the engine's bare spec cell; the PCT precedent for its domain error, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 213/2360 → 211/2362 (+2: testFilterUsingArcCosFunction, testFilterUsingArcSinFunction; 0 lost; sql-verdict disagree 0; dual-channel disagree 0; lineage-rows agree=66 disagree=0); G1 45s, G2 9s, G4 65s, G5 46s, G6 84s, G7 25s, G9 18s, G8 72s; chain 6m04s. THE RULE: the engine's relational spec cell for acos/asin is the bare function (extensionDefaults.pure `dynaFnToSql('acos', … 'acos(%s)')`); out of domain H2 yields NaN, every comparison is false and the row DROPS — the corpus contract ([9, 10] survive the filter). Our Scalars rule had raised "Unable to compute acos of 1.1" in SQL — the interpreter's error, invented beyond the relational spec; it is now the plain family (Scalars trig map), and the DuckDB dialect's existing domain guard (DuckDb.call: `CASE WHEN x BETWEEN -1 AND 1 THEN acos(x) ELSE 'NaN'::DOUBLE END`, goal #18) reaches H2's NaN on a backend that raises. THE PCT PRECEDENT: the Pure tests testArcCosineError/testArcSineError expect the interpreter's error; every engine relational PCT adapter ledgers them as expected failures (relational-h2 EssentialFunctions_manifest: "No error was thrown"; duckdb/postgres: the database's own error). Ours does the same in both PCT lanes: Test_LegendLite_EssentialFunctions_PCT rows ("Infinite or NaN" — the NaN cell cannot be read back as a Float) and the channel B essential floor 316 → 314 (DELIBERATE −2, "AssertFailed: No error was thrown" — the engine's exact H2 status). RECEIPT (not a burn): testDateTimeInclusiveRangeQuery's golden contradicts H2 itself — the engine's literal keeps all nine sub-second digits (legend-pure DateFormat appends the whole subsecond for `SSSSSS`; the client serializes `$d->toString()`), the relation fixture stores `.123` (relationMappingSetup.pure:1342), and H2 2.1.214 excludes that row for both the nine- and six-digit literal (jshell receipt in the handoff); only a MILLIS literal includes it. Stays a divergence row; the six-digit literal probe was reverted.

**Batch 60 (THE ASSERT LEDGER — truthful per-assert accounting, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet unchanged 213/2360 (a report change, no verdict moved); G1 43s, G2 9s, G4 66s, G5 47s, G6 81s, G7 25s, G9 18s, G8 74s; chain 6m03s. USER DESIGN (2026-09-04): a clean platform pass counts at the TEST level; every partial or failing test gets one row PER ASSERT in docs/RELATIONAL_CORPUS.md ("### assert ledger"): pass, or the truthful bucket naming why the platform could not verify it, plus one row for the asserts never reached — never "decline" as a euphemism. Buckets (harness/AssertLedger, classified from the UNMASKED wall/failure message): `pass`; `zero-assert`; `sql-text-assert` (the subject is emitted SQL TEXT — a contains/equality on the engine's spelling); `referee-cannot-replay` (golden SQL the H2 referee cannot execute); `decision:<name>` (tdg-chained-fetch 12, objectReferenceIn 7, routeFunction 5, protocol-transform 2, dynamic-compilation 2, recursion 2); `wall:<owner>` (typer 49, resolver 29, lowering 16, exec 14); `divergence` (rows produced and wrong, 49); `not-reached` 62. First census: 213 tests in the ledger, 83 asserts pass inside them, 7 sql-text-assert, 16 referee-cannot-replay, 3 zero-assert. The flip attempt records the listener's per-assert verdicts and REFINES the failing assert's bucket from the attempt's reason (a declined referee, a text subject, a decision) instead of adding a second row; early walls (assert-free, resolve/type) are one test-level row. The old soft-pass reconciliation line stays as the test-level view.

**Batch 59 (the lineage-tree ROW verdict, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 234/2339 → 213/2360 (+21: the whole lineage/scanRelations tree family; 0 lost; sql-verdict disagree 0; dual-channel disagree 0; lineage-rows agree=66 disagree=0); G1 45s (4,403), G2 8s, G4 66s, G5 44s, G6 80s, G7 26s, G9 19s, G8 73s; chain 6m01s. WHAT THE 21 WERE: text asserts on a scanRelations tree print whose only divergence was the engine's decorated SQL ALIASES inside join labels (`buildUniqueName(alias = true)`: `_d#N`/`_dy<i>`/`_m<N>`/`_l`/`_r`/`_md`/duplicate counters — pureToSQLQuery.pure buildNodeId), an artifact of its SQL generation the row charter retired; the harness walk had passed them under a regex strip (LineageRelationsForm.stripAliasBreadcrumbs, handoff §12). THE VERDICT NOW (user-directed: "the right way, before commit"): `LineageTreeVerdicts`, the scanRelations sibling of `SqlTextVerdicts` at the verdict seam — BOTH prints (the golden literal, inline/let/concatenated; and the database's own print of our LineageRows) become rows through ONE query the database runs (preorder, indent, kind, name, join label with every decorated alias resolved to a node name the tree itself declares — longest name first, ordered by the database — and columns), and the two row lists compare; every verdict counted in the summary (`lineage-rows agree/disagree`), registered as the lineage REFEREE in the shadow-SQL ratchet (a referee's parse of a spec cell, never an emission), the V3 seam rule, and the evaluator ledger (dispatch only). A first cut as a string canon of the expected literal (LineageTreeCanon) was built, measured at 21, and REPLACED before commit for exactly the reason the SQL-text charter names: normalize-then-byte-compare is not a rows verdict. Left in the family: testTableToTdsWithCrossJoin (a lowering gap, no SQL type for a function-typed value) and testTableToTdsWithConcatenate (TDS concatenate of unequal schemas).

**Batch 58 (the H2VERSION decision, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 241/2332 → 234/2339 (+7: three TDG alloy milestoning, testSqlGenerationForAdjustStrictDateUsageIn{Filters,Projection}ForH2, testBusinessDatePropagationInColFunction_asQueryParam, testDateFunctionInMilestonedProperty; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); lane move assert-sql-text-only 24 → 17 (charter §8.0 receipt); G1 44s (4,402), G2 8s, G4 66s, G5 46s, G6 83s, G7 26s, G9 19s, G8 75s; chain 6m07s. DECISION (user-ratified in session): `SELECT H2VERSION()` on an H2-typed connection — the engine's assertEqualsH2Compatible / createDbExtensionForH2 version probe — answers the H2 dialect LEVEL the raw-SQL boundary translates from, `RawSqlBoundary.H2_DIALECT_VERSION = 2.1.214`, the referee's own jar (H2VersionPinTest ties the literal to org.h2.engine.Constants.VERSION; registered in the JDBC census as a constant read). Behind it, each a small mechanism: toOne over a list-producing call on the relation lane is its checked element (a raw grid's rows.values); an if whose BRANCHES are asserts adjudicates its condition as a value query and the taken branch as the verdict (AssertVerdicts, evaluator ledger +19 with the justification: dispatch only); a primitive literal compared against an Any/JSON cell enters the channel (VariantShapes.alignLiteralToJson — a bare VARCHAR was parsed as JSON); a lambda literal cast to a function carrier is the lambda (CastChecker identity). Left behind: testDateFunctionInMilestonedPropertyWithMilestonedEntity now reaches a REAL row divergence (golden 0 rows on H2, ours 2 — a milestoned-property date-function semantics probe), the two TDG non-alloy variants sit on the inliner's self-aliasing helper parameter.

**Batch 57 (the mechanical type walls, one by one, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 243/2330 → 241/2332 (+2: testPlanHybridMilestoningUnionOperationWith{Non,}TemporalRootWithPropagation via the `repeat` native; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); lane move exec-passing 57 → 55 and M1 rescued 54 → 52 (the two flips left the walk's lane; charter §8.0 receipt); metamodel quarantine 5 → 0 (the routerExtensions refusal is DEAD, spelling retired); hand native classes 76 → 78; native catalog +4 (eval/4-6, repeat); G1 44s (4,400), G2 9s, G4 66s, G5 47s, G6 80s, G7 26s, G9 19s, G8 74s; chain 6m05s. LANDED, each verified against the spec: `eval` arities 4-6 (legend-pure eval.pure verbatim); `repeat` (collection/repeat.pure) as the semantic node `SqlFn.REPEAT_VALUE` the dialects spell (DuckDB list_transform over range; carrier-purity kept); m3 `Package` (m3.pure:1469) and `Testable` (m3.pure:3295) hand shapes with `PackageableElement.package` and the root-package literal `::` typed as a Package; `Mapping.includes` as DIRECT include rows (`mapping_includes` seed + MappingInclude view — the closure stays the visibility relation); the Service metamodel generated (core_service root; the generator now accepts m3's un-annotated bootstrap headers); a lambda IS an Any (cast(lambda, @FunctionDefinition<Any>) — shape gate + self-typed slot + multiplicity unify); pure's DOT auto-map over a many-valued receiver (`$exts.routerExtensions()`, best-effort receiver probe); a mapping element read as its system-store row; the static fold's map unroll expands a function-valued helper over the element and folds inside reified accessor lambdas (the digest inliner's escaped binder — three tests now run end to end); a TDSRow getter over the column lambda's row lowers as the column read (lowering/RowGetters). Extractions for the size guardrails: compiler/spec/CallShapes, lowering/ListRules, lowering/RowGetters. WHERE EACH PROBED WALL WENT (all honest, none a pass): eval → post-processor lambdas over SelectSQLQuery (compiler passes, design); connection-equality ×5 → the lowering's match over extension-contributed arms (the extension VALUE leg); cast-lambda ×1 and TDG ×1 → H2VERSION (decision bucket); extractDBs → `resolveStore` (store substitution rows); Service ×2 → `evaluate` + class query under a property access; applyMilestoningFilters → overload tie over a RelationalOperationElement row (the match/dispatch recursion family); TDG non-Alloy → a helper parameter self-aliasing its caller's let (inliner α-capture, design); digest ×3 → the MD5 INPUT spelling diverges from the engine (a value probe with the engine's SQL), and the in-memory variant → `zip` in relation position. NOT taken (not mechanical after all, in the handoff): createTempTable (a K-arm evaluating a DDL-producing lambda), toSQLString/8 (Format + DebugContext classes + post-processors), loadCsvToDbTable/4 (no spec declaration in the checkout), createDbConfig's DbConfig return (a corpus class), asserts inside map/forAll over a spelled list (statement unroll).

**Batch 56 (no-decision singles: a let-bound lambda in a core construct's argument position; a mapping element read as a metamodel value, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 245/2328 → 243/2330 (+2: testLessThanFilterAsVariable, testEnumTheSame; 0 lost; sql-verdict disagree 0; dual-channel disagree 0); lane move exec-passing 58 → 57 and the walk's M1 text-match lane 1 → 0 — RETIRED, pinned exactly empty (the last walk-lane test flipped to the platform arm; charter §8.0 receipt); 0-assert passes 29; G1 45s (4,396), G2 8s, G4 66s, G5 48s, G6 84s, G7 26s, G9 20s, G8 72s; chain 6m10s. Mechanism: `Typer.expandLetBoundLambdaArgs` at the CORE-construct entry (applyCore) — a let-bound lambda literal is its literal where the checker types a literal against the signature (Args.lambda); generic and user calls (execute's query carrier) keep the function VALUE (a first cut at the generic entry expanded 900+ execute($query) sites and was withdrawn — measured, not reasoned); `Typer.metamodelElementClass` — `<mapping>.enumerationMappings` is a property access over the element's system-store row exactly like `<db>.schemas` (one rule for both element kinds). Guardrail: the inline α-rename counter moved into its own owner (`compiler/spec/AlphaRename`, allowlist entry moved with it). Probed and NOT taken (design legs, in the handoff): objectReferenceIn 7 (generateObjectReferences = protocol transform + reflective eval + base64 — decision), Date `add(Date, Duration)` (a platform-namespace spec PROGRAM: needs a platform Pure-text library owner), relation accessor on a VIEW (ViewRelation's expansion needs a mapping-less owner), dynamic mapping compilation (getNoArgFlattenMapping — decision), FunctionExpression reflection (decision).

**Batch 55d (toPostgresModel slice B, the POSITIONAL pick over a to-many navigation, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 246/2327 → 245/2328 (+1 testConvertTableAliasColumn, 0 lost; sql-verdict disagree 0; dual-channel disagree 0); family sqlDialectTranslation 18/21 → 19/21 (sql-verdict agree=33 disagree=0 declined=0); sqltypes untyped=0; G1 44s (4,396), G2 9s, G4 67s, G5 46s, G6 81s, G7 26s, G9 19s, G8 72s; chain 6m04s. Mechanism: `$t.columns->at(k)[->cast(@C)].name` lifts into a synthetic to-one head `columns#pN` (SyntheticHeads.POSITIONAL) whose join target is the navigation's physical row with `ordinal == k` (the store's ORDER column, SystemMetamodel.ORDINAL_COLUMN; relational_elements seeds each column's declaration ordinal) — a LEFT JOIN step like every other navigation, never a subquery per read; the lift walk descends into a constructed instance's fields (the map-over-row body); MetamodelMapping maps `Table.columns[col]: @TableToColumns`; a navigate slot named after a relation accessor (`columns`/`rows`) mints clear of it; `classTypedTargetIfMapped` accepts an abstraction whose subclasses are mapped. Guardrail: liftArms' descent switch extracted (`descend`). Left in the family: testConvertJoinTreeNode / testConvertSelectSQLQuery (row-backed join-tree recursion — the design note in the handoff; LAST per the agreed order).

**Batch 55c (toPostgresModel slice B, the STORE-ROW leg + F10 proper's construction-site canon, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 251/2322 → 246/2327 (+5: testConvertAlias, Table, TabularFunction, SelectSQLQueryWithCTE, Union; 0 lost; dual-channel disagree 0; 0-assert passes 29); sqltypes untyped=0; G1 44s (4,396), G2 8s, G4 66s, G5 46s, G6 85s, G7 27s, G9 18s, G8 76s; channel B unchanged (316/137/204/355/95). Family 13/21 → 18/21, every verdict real (sql-verdict agree=32 disagree=0 declined=0 — the 17 instance-key-shape declines gone; debugPrint's 9 likewise now real verdicts). Mechanism: (1) a constructed instance over ONE toOne-wrapped element chain — by STRUCTURE, pure being referentially transparent — is the map of that chain's row (`resolver/ConstructedRowForm`; the row's navigations are its join steps, never a subquery per read — the user's ruling; a per-read scalar-subquery form was built, measured and reverted); (2) `MetamodelMapping` maps `Schema.tables` / `Table.schema` (SchemaToTables); (3) a navigate-slot hop threads downstream depth into its nested target (`NavProvenance.nestedTarget` / `registerHopHeads`, `FlattenOps.tailsThrough` — the association route's depth leg, for slots); (4) slot prefixes mint clear of the left row's composed names (`Pipelines.slotPrefix`, one rule at three sites); (5) a many-valued LIST value's map stays a list map in the substitution (`Substitution.listValueMap`; pure's map over one value is application — a broader arm perturbed calendar-aggregation float order, caught by the dual-channel pin); (6) the map-binder channel's VALUE is its cell for the canon (`ResultShape.valueInfo`) and a struct slot reads the lowered value's own element type; (7) F10 proper: `ClassLayouts.SYNTHETIC_CANON` — a constructed instance's canonical key text computed at its construction site from its own fields (`lowering/ConstructionCanon`: the struct bound once via `list_transform([s], s -> struct_insert(s, __canon := …))[1]` so every child is spelled once and recursive polymorphic shapes stay linear — a static `__type` dispatch was built first and overflowed: it is infinite for `Expression`-typed keys), carried on the wire as JSON; `CanonicalRenderSql` reads it at the root and in JSON/struct slots (`constructionCanon`/`jsonSlotCanon`; a JSON object without one is its identity); `SqlFn.STRUCT_INSERT` + typing + DuckDB rendering. Guardrails honoured by refactor, not by pin: Invariant 6h/6d (the lowering reads the lowered value's fact, exec keeps its own rule), carrier purity (one `emptyArray()` owner), file/method limits (ConstructedRowForm, ConstructionCanon, NavProvenance/FlattenOps extractions, Substitution predicates). Left in the family: TableAliasColumn (`Table.columns` to-many self-join + column ordinal), JoinTreeNode/SelectSQLQuery (§7 row-backed recursion).

**Batch 55b (toPostgresModel slice B, the compiler side — a system-store row dispatches over the relation's kinds; list-shape folds, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 252/2321 → 251/2322 (+1 testConvertJoinStrings, 0 lost; disagree 0); exec-passing 58 unchanged; sqltypes untyped=0; G1 45s (4,396), G2 8s, G4 63s, G5 47s, G6 82s, G7 26s, G9 18s, G8 74s; channel B unchanged (316/137/204/355/95, disagree 0). The previous session's nine uncommitted files were AUDITED against the real Pure sources: kept the declaration-only arm scan (`UserCallInliner.declaredSubtype`) and the lexicographic recursion measure (literal size, then a store argument of a class no enclosing activation holds); reverted the legend-pure `functions.pure` library admission (12 effect natives; the platform already owns that file's views in `SystemMetamodel`), the native-span text blanking, the native duplicate key, `orElse`, `string::plus(String[*])` + the Typer catch + the NameResolver multi-candidate, the widening-cast type, and the static first-arm dispatch (never fired) — each of their receipts was a wall inside an arm that must be DEAD for a Table input (reached through the already-admitted pureToSQLQuery library). New: `children()`/`childByJoinName()` as SystemMetamodel views (functions.pure:288-296); a runtime match over a SYSTEM-STORE row (a navigation rooted at an element reference) keeps only the arms some class bound in the system mapping beneath the declared class reaches (Table's rows are Table/View, never ViewSelectSQLQuery); a primitive input keeps only its lattice's arms; folds: spelled scalar `cast` to its primitive, `cast` over the empty spelled collection, native `concatenate` (and its empty-side identity), `zip`, `init` (LiteralUnrollLedger +concatenate/zip/init); `SqlTypeCensus` locators name a struct's blind field and a call's blind argument. Family 12/21 → 13/21; six of the eight left sit at the store-row leg ("class query under TypedNewInstance"), two are the §7 row-backed-recursion residue. Hang root cause: the library file's natives entering the model (never thread-dumped; removed by the audit).

**Batch 55a (the Java port of toPostgresModel and the host metamodel walk are DELETED, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 255/2318 → 252/2321 (+3, 0 lost; disagree 0); exec-passing 58 unchanged; G1 42s (4,396), G2 9s, G4 56s, G5 43s, G6 79s, G7 25s, G9 19s, G8 72s; channel B unchanged (316/137/204/355/95). Deleted: `exec/MetamodelWalk.java` (905 lines), `MetamodelSteps.java` (156), the executor's planWalk/constructNode/constructOp/nodeValue/walkProp/walkFilter/walkResult arms (583 lines; StatementExecutor 3,494 → 2,911), the harness's `instanceOfAssert` NodeH string-match arm; JavaEvalLedger register rows for both files removed, executor EVICT pin 40 → 5, AssertVerdicts 1568 → 1576 (justified: assertInstanceOf reads the wire's `__type` up the model's subtype relation). The three tests the walk still scored (measured by the nowalk probe: ratchet unmoved, family scoreboard −3) now ride the platform: SQLExecutionNode.connection and its LocalH2 datasource specification are plan ROWS (`plan_connections` / `plan_connection_sqls`, PlanRows.connectionRows, mapped as the engine's connection classes under inheritance operations; the cast raise beside a to-many leaf is stamped per joined row), a property-less class constructor is the identity struct (`ClassLayouts.syntheticOnlyLayout`), `assertInstanceOf` over a conforming literal folds (LiteralUnrollLedger +assertInstanceOf). Prelude +1 generated class (LocalH2DatasourceSpecification, demanded by the system store).

**Batch 54 (OPTION S — the prelude's library shapes are GENERATED from the spec; toPostgresModel slice A, 2026-09-04): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 267/2306 → 255/2318 (+12, 0 lost; disagree 0); G1 44s (4,396), G2 8s, G4 61s, G5 44s, G6 78s, G7 26s, G9 18s, G8 72s; channel B essential 316 / grammar 137 / standard 204 / relation 355 / unclassified 95, disagree 0 everywhere. Chain catch on the way: the core-import tier resolves a bare `equality`/`temporal`/`PCT` profile to its m3 FQN, and three consumers matched the BARE spelling — identity layouts silently replaced equality keys (channel B head/first/contains/in/equal over `<<equality.Key>>` classes); `PlatformTypes.isProfile` (exact FQN, or the bare spelling of a model that does not declare the profile) is now the one rule (ClassCompiler, FunctionCompiler, MilestoningStrategy). Also: `tail` over a spelled list and a `cast` over a spelled collection fold (the untyped FoldCall root in toPostgresModel's binary-expression chain is gone; sqltypes untyped=0); exec-passing 58 unchanged; NativeFunctionTest hand-class pin 255 → 76 (217 hand copies of spec shapes deleted — the generated `Prelude.java` (PreludeGeneratorTest, `-Dprelude.generate=1`, verify mode in the chain) carries 230 classes / 10 enums with their equality keys and defaults; hand = m3 bootstrap (tools/m3shape.py receipts), primitives, carriers, 13 Java-referenced definitions and 6 SYSTEM-STORE-COUPLED shapes); hand-enum pin 19 → 6; LiteralUnrollLedger fold set + size/contains/keyValues/get/defaultIfEmpty/assert/enumValues/dynamicNew/isTrue/greaterThan/lessThan/greaterThanEqual/lessThanEqual/pair (all compare-only); native catalog +6 signatures (eval/3, elementToPath(Type), collection groupBy/2, keyValues, defaultIfEmpty, dynamicNew ×2, isTrue). Receipts: docs/DECLARATIONS_HOMEWORK_2026_09_04.md; NameResolver.CORE_IMPORTS (real pure's implicit import group).

**Batch 51 (an Any-typed struct field decodes as its value at the wire, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 280/2293 → 279/2294 (+1, 0 lost); lanes unchanged (exec-passing 59, M1 rescued 54, disagree 0). G1 42s, G2 8s, G4 59s, G5 39s, G6 84s, G7 26s, G9 19s, G8 75s.

**Batch 50 (the engine-style H2 referee spells the MMMyyyy month-abbreviation parse, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 281/2292 → 280/2293 (+1, 0 lost); lane move exec-passing 60 → 59 (disagree 0). G1 40s, G2 8s, G4 58s, G5 41s, G6 84s, G7 27s, G9 19s, G8 74s.

**Batch 49 (a let-bound legacy aggregate value defers to the groupBy that consumes it, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 282/2291 → 281/2292 (+1, 0 lost); lanes unchanged (exec-passing 60, M1 rescued 54, disagree 0). G1 42s, G2 8s, G4 57s, G5 38s, G6 81s, G7 26s, G9 18s, G8 72s.

**Batch 48 (enumeration mappings as system-store rows; enumerationMappingByName and toDomainValue as Pure bodies over them, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 284/2289 → 282/2291 (+2, 0 lost); lanes unchanged (exec-passing 60, M1 rescued 54, disagree 0); native class pin 255 → 256. G1 40s, G2 8s, G4 57s, G5 38s, G6 79s, G7 25s, G9 18s, G8 72s.

**Batch 47 (parseDate is a semantic SQL node the dialects spell; the engine-style H2 text carries the engine's parsedatetime idiom, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 285/2288 → 284/2289 (+1, 0 lost); lanes unchanged (exec-passing 60, M1 rescued 54, disagree 0). G1 40s, G2 8s, G4 55s, G5 35s, G6 82s, G7 25s, G9 18s, G8 71s.

**Batch 46 (relation-rooted plan text: a table accessor / tableToTDS single node with precisePrimitives accessor columns; a map over a scalar read composes the mapper over the read, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 287/2286 → 285/2288 (+2, 0 lost); lane moves exec-passing 61 → 60, M1 rescued 55 → 54 (disagree 0). G1 40s, G2 9s, G4 54s, G5 38s, G6 77s, G7 26s, G9 19s, G8 73s.

**Batch 45 (if() over a class query decides on literal emptiness; a TDSNull-typed collection root egresses as the TDSNull value, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 291/2282 → 287/2286 (+4, 0 lost); lane moves exec-passing 63 → 61, M1 rescued 57 → 55 (disagree 0). G1 40s, G2 9s, G4 55s, G5 38s, G6 77s, G7 26s, G9 18s, G8 73s.

**Batch 44 (no-decision singles: zip is the positional list_zip pairing, the envelope splice erases cast/rows after splicing their source, meta::pure::tds::extend dispatches to the extend checker, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 297/2276 → 291/2282 (+6, 0 lost); lane moves exec-passing 68 → 63, M1 rescued 62 → 57 (disagree 0). G1 39s, G2 9s, G4 55s, G5 39s, G6 82s, G7 26s, G9 18s, G8 72s.

**Batch 43 (the referee render runs the H2 carrier strategies: a whole relation collected as a list then exploded becomes rows, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 303/2270 → 297/2276 (+6, 0 lost); lane moves exec-passing 75 → 68, M1 rescued 62 → 62
(passes 2379, disagree 0). G1 40s, G2 8s, G4 62s, G5 42s, G6 80s, G7 26s, G9 19s, G8 72s.

**Batch 42 (the static extent-subset fact from the typed chain arms the oracle's pk-collapse in the verdict-arm lane, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 304/2269 → 303/2270 (+1, 0 lost); lane moves exec-passing 76 → 75, M1 rescued 63 → 62
(passes 2379, disagree 0).

**Batch 41 (let-bound column arguments bind at project; the TDG no-seed Error plan, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 308/2265 → 304/2269 (+4, 0 lost); lane moves M1 verified 4 → 1, exec-passing 79 → 76,
text-only 26 → 25 (passes 2379, disagree 0).

**Batch 40 (the TDG plan as a platform value: plan-flavored TypedTestDataGen + planToString printer, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 310/2263 → 308/2265 (+2, 0 lost); lane move text-only 27 → 26 (passes 2378, disagree 0).

**Batch 39 (lateral explode → decorrelated UNION on the H2 family; engine-style render runs its passes; plan-text goldens replay their sql node, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 314/2259 → 310/2263 (+4, 0 lost); lane move exec-passing 82 → 79; text-verdict asserts 156 → 147
(passes 2378, disagree 0).

**Batch 38 (no-decision burn from the sqltext homework: frame mapping to the oracle's enum decode (includes, identity), let-bound join lambdas + declared TDSRow, TDSRow getters, assertSameSQL(String) general arm, paginated-golden rule, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 330/2243 → 314/2259 (+16, 0 lost); lane moves M1 verified 9 → 4, M1 rescued 75 → 63,
exec-passing 99 → 82, unable-to-exec 14 → 13; text-verdict asserts 170 → 156 (passes 2377, disagree 0).

**Batch 37 (the "text-policy" pre-decline gate DELETED; every sql-assert shape attempted; per-test text-verdict roster, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 366/2207 → 330/2243 (+36, 0 lost); lane moves M1 verified 12 → 9, M1 rescued 108 → 75,
exec-passing 135 → 99, unable-to-exec 20 → 14 (passes 2374 → 2375, disagree 0). Dossier: docs/SQLTEXT_HOMEWORK_2026_09_03.md.

**Batch 36 (percentile = one semantic reducer with a within-group order; DuckDB encodings as the QuantileOrder MIR pass, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 369/2204 → 366/2207 (+3, 0 lost); lane moves exec-passing 140 → 135,
M1 rescued 109 → 108 (passes 2374 stable, disagree 0).

**Batch 35 (referee render: literal-collection reductions, firstNotNull, round in the engine-style H2 dialect, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 379/2194 → 369/2204 (+10, 0 lost); lane move exec-passing 149 → 140
(passes 2374 stable, disagree 0).

**Batch 34 (assertSameSQL(String, String) takes the exec-read rows verdict, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9)** — ratchet 394/2179 → 379/2194 (+15, 0 lost); lane moves M1 verified 20 → 12,
M1 rescued 119 → 109, exec-passing 167 → 149 (passes 2374 stable, disagree 0).

**Batch 33 (runtime connections THROUGH lets — JSON source / chain mappings, 2026-09-03): chain GREEN
(gates 1,2,4,5,6,7,8,9; per-gate timings not captured this run)** — ratchet 416/2157 → 394/2179 (+22, 0 lost);
M1 rescued floor 127 → 119 (lane move: passes 2367 → 2374, disagree 0); other pins unchanged.

**Batch 32 (plan-execute FRAMES — the let-chase, rows/cast erase, TDS roots, 2026-09-03): chain 6m00s** —
G1 40s, G2 8s, G4 62s, G5 47s, G6 85s, G7 26s, G9 18s, G8 74s. Ratchet
430/2143 → 416/2157 (+14, 0 lost); exec-passing declines 170 → 167; other pins
unchanged.

**Batch 31 (the query FRONT DOOR — validate desugar in the platform path, 2026-09-03): chain 6m00s** —
G1 40s, G2 8s, G4 62s, G5 47s, G6 85s, G7 26s, G9 18s, G8 74s. Ratchet
446/2127 → 430/2143 (+16, 0 lost); exec-passing declines 171 → 170; other pins
unchanged.

**Batch 30 (effectful helper VALUES + generic multiplicity arguments, 2026-09-03): chain 6m00s** —
G1 40s, G2 8s, G4 62s, G5 47s, G6 85s, G7 26s, G9 18s, G8 74s. Ratchet
451/2122 → 446/2127 (+5, 0 lost); metamodel quarantine rows 22 → 5 (the multiplicity
arguments type reflection chains that walled); exec-passing declines 180 → 171; ledger
StatementExecutor 2692 → 2696 (justified); other pins unchanged.

**Batch 29 (SQL post-processors — CTE extraction, let-bound replaceTables, 2026-09-03): chain 6m00s** —
G1 40s, G2 8s, G4 62s, G5 47s, G6 85s, G7 26s, G9 18s, G8 74s. Ratchet
463/2110 → 451/2122 (+12, 0 lost); M1 verified floor 22 → 20, rescued 128 → 127
(lane moves); other pins unchanged.

**Batch 28 (INLINE handles on demand + the unrolled quantified verdict, 2026-09-03): chain 6m00s** —
G1 40s, G2 8s, G4 62s, G5 47s, G6 85s, G7 26s, G9 18s, G8 74s. Ratchet
487/2086 → 463/2110 (+24, 0 lost); AssertVerdicts ledger pin 1459 → 1511 (a verdict
shape, justified in the ledger); other pins unchanged.

**Batch 27 (referee render COVERAGE — chain mapping, H2 in-lists, 2026-09-03): chain 6m00s** —
G1 40s, G2 8s, G4 62s, G5 47s, G6 85s, G7 26s, G9 18s, G8 74s. Ratchet
505/2068 → 487/2086 (+18, 0 lost); exec-passing declines 198 → 180 (lane move);
other pins unchanged. G6 is creeping (78 → 85s): the first slice to shard if the
chain nears the budget.

**Batch 26 (the referee's render is the FRAME's chain — milestoning leg, 2026-09-03): chain 5m53s** —
G1 41s, G2 8s, G4 62s, G5 43s, G6 80s, G7 26s, G9 19s, G8 74s. Ratchet
581/1992 → 505/2068 (+76, 0 lost); lane pins moved as lane moves (M1 verified
54 → 22, M1 rescued 164 → 128, exec-passing declines 275 → 198); other pins
unchanged.

**Batch 25 (aggregation-aware ROUTING done right, 2026-09-03): chain 5m50s** —
G1 41s, G2 8s, G4 64s, G5 39s, G6 83s, G7 26s, G9 18s, G8 71s. Ratchet
unchanged 581/1992 (0 lost; the five nonGroupBy rewrittenQuery reads now flip
through rows, the Java fold is deleted); all pins unchanged. One failed run on
the way (the error-shape guardrail: the routing walk's unmatched kinds must
throw, not yield a placeholder path).

**Batch 24 (execution ACTIVITIES as rows, 2026-09-03): chain 5m54s** —
G1 40s, G2 9s, G4 65s, G5 44s, G6 78s, G7 26s, G9 18s, G8 74s. Ratchet
653/1920 → 581/1992 (+72, 0 lost); lane pins moved as lane moves: M1 verified
floor 82 → 54, M1 rescued floor 204 → 164, exec-passing declines 344 → 275
(receipt: corpus passes 2355 → 2367, clean 2151 → 2201, text-rescued 165 → 127,
oracle disagreements 0); other pins unchanged. Two failed chain runs on the way
(the rescued floor, then a real NOP-family regression when the rewrittenQuery
fold was deleted — restored).

**Batch 23 (consolidation — handle class from the native signature, shape-free
let registration, one resolver factory, 2026-09-03): chain 5m54s** —
G1 40s, G2 8s, G4 65s, G5 42s, G6 82s, G7 26s, G9 18s, G8 73s. Ratchet
unchanged 653/1920 (0 lost, 0 gained); all pins unchanged.

**Batch 22 (group H — the expression TREE as rows, 2026-09-03): chain 5m49s** —
G1 41s, G2 8s, G4 64s, G5 41s, G6 78s, G7 24s, G9 19s, G8 74s. Ratchet
656/1917 → 653/1920 (+3, 0 lost); native classes 249 → 255 (Multiplicity,
MultiplicityValue, InstanceValue, VariableExpression, FunctionExpression,
SimpleFunctionExpression); metamodel quarantine rows 34 → 22 (the m3 classes
type reflection chains that walled as unknown types); Java arm ReflectAsserts
deleted; other pins unchanged.

**Batch 21 (group I — column lineage AS ROWS, 2026-09-03): chain 5m56s** —
G1 40s, G2 9s, G4 65s, G5 42s, G6 82s, G7 26s, G9 19s, G8 73s. Ratchet
661/1912 → 656/1917 (+5, 0 lost); native classes 245 → 249 (PropertyPathNode,
Res, PropertyPathTree, ColumnWithContext); other pins unchanged.

**Batch 20 (group E — lineage trees AS ROWS, 2026-09-03): chain 5m42s** —
G1 38s, G2 9s, G4 61s, G5 40s, G6 78s, G7 25s, G9 18s, G8 73s. Ratchet
686/1887 → 661/1912 (+25, 0 lost); native classes 244 → 245 (RelationTree);
other pins unchanged.

**Batch 19 (group A — function bodies AS ROWS, 2026-09-03): chain 5m49s** —
G1 40s, G2 8s, G4 62s, G5 42s, G6 80s, G7 26s, G9 19s, G8 72s. Ratchet
729/1844 → 686/1887 (+43, 0 lost); metamodel quarantine rows 77 → 34
(walls 9); proven-empty int-or-null ceiling 67 → 87 (the temporal-TDS
concatenation tests' expressionSequence reads now type and their attempts
execute — same three witnesses, more probes); other pins unchanged.

**Batch 18 (group Q — plan nodes AS ROWS, 2026-09-03): chain 5m48s** —
G1 38s, G2 9s, G4 62s, G5 41s, G6 81s, G7 26s, G9 19s, G8 72s. Ratchet
778/1795 → 729/1844 (+49, 0 lost); walk text-only asserts 35 → 27 (the
plan-text asserts joined the flip cohort); metamodel quarantine rows 125
→ 77 (the plan-read refusals are dead; walls 9 unchanged); required-over-
nullable ceiling 533 → 534 (SQLExecutionNode.sqlQuery over the single-
table plan_nodes); exec-passing 344.

**Batch 17 (group Q opener — executionPlan signature verbatim,
2026-09-03): chain 5m56s** — G1 38s, G2 9s, G4 63s, G5 44s, G6 84s, G7
26s, G9 19s, G8 73s. Ratchet 780/1793 → 778/1795 (+2); other pins unchanged.

**Batch 16 (group D remainder — let-bound runtimes and CSV seeds,
2026-09-03): chain 5m56s** — G1 39s, G2 9s, G4 63s, G5 43s, G6 84s, G7
26s, G9 19s, G8 73s. Ratchet 782/1791 → 780/1793 (+2); other pins unchanged.

**Batch 15 (group D leg 2 — the meta::json tree on the variant lane,
2026-09-03): chain 5m56s** — G1 38s, G2 9s, G4 64s, G5 43s, G6 83s, G7
26s, G9 20s, G8 73s. Ratchet 791/1782 → 782/1791 (+9); exec-passing 344,
h2-exec 82, quarantine 125/9 unchanged; walk text-only asserts 40 → 35
(the paginate helpers' SQL-text asserts joined the flip cohort).

**Batch 14 (group D leg 1 — the router's string entry, 2026-09-03):
chain 5m49s** — G1 39s, G2 9s, G4 62s, G5 42s, G6 80s, G7 25s, G9 19s,
G8 73s. Ratchet 820/1753 → 791/1782 (+29); exec-passing 344, h2-exec 82,
quarantine 125/9 unchanged.

**Batches 12–13 (refs by id 5m44s; inline relations 5m57s with a 55s G5
outlier — two standalone H2 reruns measured 42s/42s, ledgers identical to
batch 12's; watch the next chain).** Channel B once (G9): 5m49s.

**Batch 11 (boot layer, same day): chain 5m51s** — G1 38s (clean build;
29s warm), G2 9s, G4 62s, G5 40s, G6 86s, G7 25s, G9 19s, G8 72s. A model
compile is 0.5ms (8.0ms at the breach, 2.3ms before group F). The 21s
over the 5.5-minute line is G4 at the top of its old range and G6 —
the per-mapping normalizer index leg is the named next slice; the
ceiling re-arms when the chain measures under 330s.

## The time budget: ~6m40s measured 2026-08-11 — re-pin pending

The 5.5-minute lock (measured 2026-08-08) was already exceeded BEFORE the
engine-module deletion (6m32s with the module still present), and the
deletion itself was time-neutral (6m41s after — gate 3's removal offset the
suite growth in gate 1). Suspected growth since the 08-08 pin: gate 8's
strengthening (whole-document parity + the four previously ungated tests)
and the clean NullAway compile absorbing the server shell. `allgates.sh`
now stamps per-gate wall time into the log (`GN_EXIT=0 (took Ns)`) — re-pin
this table from the next run's stamps instead of guessing.

Measured per-gate 2026-08-11 (the runner now stamps these into the log):

| # | gate | 08-08 | 08-11 |
|---|------|-------|-------|
| 1 | core suite (clean; 4,046 tests — engine's suite folded in) | 13s | 29s |
| 2 | core install | 1s | 8s |
| 3 | (folded into gate 1 — engine module deleted) | 21s | — |
| 4 | DuckDB corpus sweep | 92s | 93s |
| 5 | h2 corpus sweep | 41s | 43s |
| 6 | PCT full | 73s | 78s |
| 7 | PCT h2modern guard | 24s | 24s |
| 8 | parser parity | ~65s | **123s** → 103s after the oracle-parse dedupe |
| | **total** | **~330s** | **398s (6m38)** → ~6m15 |

The minute went to GATE 8: it roughly doubled when the whole-document PMCD
parity test (5,259 sources) joined the element-level sweep (26,168 verdicts)
— both layers re-parse largely the same source text, and the recorded
"harness dedupe" follow-up (PMCD-parity notes) is the lever to claw much of
it back: parse each distinct source once, feed both verdict layers from the
same parse. Everything else moved by seconds.

Previous table (2026-08-08 measurements) for reference:

| # | gate | time |
|---|------|------|
| 1 | core suite (clean, ~4,000 tests — engine's behavioral suite folded in) | ~35s |
| 2 | core install | 1s |
| 3 | (folded into gate 1 — engine module deleted) | — |
| 4 | **DuckDB corpus sweep** | **92s** |
| 5 | h2 corpus sweep | 41s |
| 6 | **PCT full (1,109)** | **73s** |
| 7 | PCT h2modern guard | 24s |
| 8 | **parser equivalence** | **59s** |
| | **total** | **324s — 5.4 min** |

**The whole chain must stay at or under 5.5 minutes (330s).** Adding work that
breaks that ceiling is an explicit decision to be argued and recorded HERE, not
absorbed silently — a chain that creeps toward ten minutes stops being run, and
a gate nobody runs is not a gate.

Two things this table settles. G1 is 13 seconds, not the minute-plus it is
usually assumed to be, so `clean` costs almost nothing and stays. And the
33-grammar oracle added to G8 on 2026-08-08 cost about 20s (it was ~40s with
three jars) — that is most of the current headroom, spent deliberately: three
jars was what let 2,270 corpus files leave the denominator unnoticed.

The cheapest cut available, if the ceiling is ever breached, is gate 5: it is
the SAME sweep as gate 4 against a second backend, it does not write the
scoreboard, and it is portability coverage rather than correctness. It is kept
on every run by explicit decision (2026-08-08), not by inertia.

---

| # | Gate | Command (from repo root) | Expectation |
|---|------|--------------------------|-------------|
| 1 | Core suite | `mvn -pl core **clean** test` | 0 failures. **`clean` is load-bearing** — NullAway runs only on `default-compile`, so a warm `target/` silently no-ops the null gate. |
| 2 | Core install | `mvn -pl core install -DskipTests` | — (required before 3–8) |
| 3 | Engine suite (corpus excluded — gate 4 owns it) | `mvn -pl engine test '-Dtest=!RelationalCorpusRunner'` | 0 failures (~21s). Note `engine/pom.xml` excludes the `heavy` group, so this is the default suite, not everything. |
| 4 | DuckDB corpus sweep | `mvn -pl engine test -Dtest=RelationalCorpusRunner -Dlegend.engine.root=<engine checkout>` | scoreboard vs `docs/RELATIONAL_CORPUS.md`; `M1_VERIFIED` floor (~115s) |
| 5 | h2 corpus sweep | `mvn -pl engine test -Dtest=RelationalCorpusRunner -Drcorpus.backend=h2 -Dlegend.engine.root=<engine checkout>` | portability sweep; scoreboard not written (~45s) |
| 6 | PCT full (DuckDB) | `cd pct && mvn -o test` | 1,109 run, 0 failures, 36 ledgered expected failures, nothing skipped (~30–80s) |
| 7 | PCT h2modern Relation guard | `cd pct && LEGENDLITE_PCT_BACKEND=h2 mvn -o test -Dtest=Test_LegendLite_RelationFunctions_PCT -Dh2.version=2.4.240` | see the warning below (~25s) |
| 8 | Parser equivalence | `mvn -pl parser-equivalence **-am** clean test -Dtest='CorpusSweepTest,RejectionParityTest,SectionParseSentinelTest,FixtureAdjudicationTest,EngineSectionRosterTest,EngineElementRosterTest,ViewFilterParityTest,ComparatorSelfTest,QuotedImportParityTest,CorpusManifestTest,OffsetCompositionParityTest' -Dsurefire.failIfNoSpecifiedTests=false -Dlegend.engine.root=<engine checkout> -Dlegend.pure.root=<legend-pure checkout>` — the authority is `tools/allgates.sh` (this row is a mirror) | the ratchets below (~60s) |

> **Gate 7 is one-directional and goes RED on improvement.** `allgates.sh:53`
> judges it with `grep -qE "Tests run: 348, Failures: 1, Errors: 22"` — a
> literal string. **Fixing any one of those 22 errors turns the gate red.**
> Fix the script before fixing the tests.

### Live ratchet constants (the authority is the SOURCE — this table is regenerated, not trusted)

Regenerated 2026-08-12 (the previous table was 100% dead: every row cited a
class deleted in the 08-12 sweep consolidation — deep-audit §6).

| Constant | Value | Source |
|---|---:|---|
| `MIN_PINS` | 424 | `RejectionParityTest.java` |
| `MIN_LINE_AGREEMENT` | 417 of 423 | `RejectionParityTest.java` |
| `MIN_COLUMN_EXACT` | 337 | `RejectionParityTest.java` |
| `MIN_DOCS_MATCHED` | 6489 (100%) | `CorpusSweepTest.java` |
| `MAX_SEAM_LENIENT_ACCEPTS` | 22 | `CorpusSweepTest.java` |
| `MAX_ENGINE_JSON_ASYMMETRY` | 9 | `CorpusSweepTest.java` |
| `MAX_PARSER_LENIENT_ACCEPTS` | 181 | `CorpusSweepTest.java` |
| `MIN_BEHAVIOUR_MATCHED` | 2093 | `SectionParseSentinelTest.java` |
| `MAX_DROP_IN_DEFECTS` | 0 | `SectionParseSentinelTest.java` |
| `MAX_LENIENT` | 17 | `SectionParseSentinelTest.java` |
| `MAX_UNJUSTIFIED_LENIENCY` | 0 | `SectionParseSentinelTest.java` |

> This table is re-checked against source whenever a floor moves (deep audit
> #2 found it wrong in 6 of 12 rows — the SOURCE constants are authority,
> this table is a courtesy). `SurfaceCensusTest` and `MessageParityTest` are
> gate-8 members since 2026-08-14; `AdversarialParityTest`'s class filter in
> `tools/allgates.sh` is the authoritative list, not the one quoted above.
| `MAX_LENIENCY_KINDS` | 21 | `FixtureAdjudicationTest.java` (distinct kinds, not fixtures) |
| `MAX_OVER_STRICTNESS` | 6 | `FixtureAdjudicationTest.java` |
| `MIN_SECTIONS` | 25 | `EngineSectionRosterTest.java` — DENOMINATOR: sections engine can parse |
| `MIN_ELEMENTS` | 41 | `EngineElementRosterTest.java` — DENOMINATOR: element types engine can produce |

Deleted classes previously cited here (`CorpusEquivalenceTest`,
`SpiSeamProofTest`, `PmcdEquivalenceTest`, `StrictDialectParityTest`,
`LeniencyCatalogTest`, `MappingEquivalenceTest`) are consolidated into
`CorpusSweepTest`; when a row and its source disagree, fix THIS table.

> **`FixtureAdjudicationTest` is the only tier pointed at OUR OWN fixtures.**
> Every other tier reads legend-engine's and legend-pure's files, and a
> corpus sweep structurally cannot find a disagreement about a form the
> corpus never contains — which is how three leniencies survived for months
> pinned by our own tests. It costs ~1s. Its two ratchets are debt ceilings,
> not targets, and its Javadoc clusters the 268 by the reference parser's own
> message so the list is actionable rather than a number.

Corpus ledger (`docs/RELATIONAL_CORPUS.md`, regenerated by gate 4):
**2,575 run / 2,318 pass**, of 2,798 total `<<test.Test>>` functions.
`docs/RELATIONAL_CORPUS_ALL.md` is the same sweep in 100% mode
(`-Drcorpus.includeExcluded`): 2,798 / 2,398.

> **`MAX_LENIENT_ACCEPTS` bounds the SPI bridge, not the parser.** The bridge
> is a site scanner that ignores tokens it does not recognise, so this number
> can be lowered by adding a scan guard rather than fixing a defect — which is
> how 182 → 170 happened. `MAX_PARSER_LENIENT_ACCEPTS` (742) is the honest
> parser-side figure. Lower that one.

---

`tools/allgates.sh` runs the whole chain (env: `LEGEND_ENGINE_ROOT`,
`LEGEND_PURE_ROOT`, optional `MVN_SETTINGS`; log at `$GATES_LOG`, default
`/tmp/gates.log`). It omits `clean` on gate 1 and `-am` on gate 8 — both
worth fixing.

**`tools/diagnostics.sh` — the measurement battery, OUT of the chain**
(user ruling 2026-08-26, reviving the 08-14 "triggered, not scheduled"
cadence): the parse-speed benchmark + six census/sizing classes (five
assertless printers; GrammarCoverage's ratchets bind PINNED inputs — a
constant between pin changes). Run it on its three triggers — corpus
manifest change, oracle-pin bump, parser/protocol/census-code change —
never per chain. It carries its own rename-goes-red roster, so the
"every class in some roster" discipline holds across both scripts.

**When each gate is required:** 1–5 whenever core is touched; 6–7 additionally
whenever a dialect (H2/H2Modern/shared renderer) or the lowering changes; 8
whenever the lexer, parser, protocol or emitter changes, and after any upstream
checkout pull.

Budget: the WHOLE chain measured END-TO-END at 284s (2026-08-03, machine held
awake): build+install 4s, core 8s, engine 22s, DuckDB corpus 110s (seed 47s +
h2-mirror 21s), h2 corpus 43s, PCT full 73s, PCT h2modern 25s.

**THE one failure mode that matters:** any gate showing ~900s wall with
near-zero CPU means THE MACHINE SLEPT mid-run (pmset log: 900–946s Maintenance
Sleep cycles with 45s DarkWakes; this box sleeps after 1 idle minute). Run long
chains under `caffeinate` (plain `-i` is NOT enough if the machine is already
in its sleep cycle) or `sudo pmset -a sleep 0` for the session — and re-run
before diagnosing any ~900s outlier. `mvn -o` on pct stays as hygiene (skips
remote metadata checks) but was NOT the cause of the historic 10–16 min runs;
those were sleep.

Scoped corpus runs (`-Drcorpus.only=…`) never write the scoreboard and their
universe differs from the full sweep — they are probes, not gates.

**After ANY upstream checkout pull, run gate 8's `SectionParseSentinelTest`
FIRST** (~1s). It parses every corpus file containing
`###Mapping`/`###Relational`/`###Connection`/`###Runtime` sections through the
real pipeline entry and fails if the parsing count drops — the named-failure
version of the 2026-08-04 `~src` pull that silently collapsed gate 4 to
2/2567. A new message bucket in `target/section-sentinel-report.txt` IS the
drift.
