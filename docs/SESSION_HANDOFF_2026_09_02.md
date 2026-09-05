# Harness-Deletion Burn — Session Handoff (for the session after 2026-09-01)

Supersedes `docs/SESSION_HANDOFF_2026_09_01.md` (kept for its audit trail).
Read this top to bottom before touching the tree; every number below is
a measured receipt from the sweeps that landed HEAD.

## 0. Where things stand — RIGHT NOW
- **Repo** `/Users/neema/legend/legend-lite`, branch `main`, **tree clean, all pushed.**
- **HEAD `5b63838c`** — three gated batches landed 2026-09-01 (evening):
  `ac99dcf5` foundation probe (mapping-seam window rule + one test clock),
  `302365b8` legacy TDS join let-bound JoinType (+23),
  `5b63838c` TDG arm reach (zero net flips, honest re-bucketing).
- **Ratchet 848 fallbacks / 1725 flipped** (EXACT pins,
  `RelationalCorpusRunner.java` ~line 1120). Corpus **2350** pass (2144 clean).
- **Standing pins, all green:** sql-verdict disagree EXACT 0; canon
  disagree EXACT 21 (calendarAggregations float class); M1 h2-exec
  verified 83 / rescued 204 / unverifiable ≤ 11; exec-passing 345;
  emission census 392 text-matched / 812 diverged / 110 text-verdict
  (cosmetic, shrink-only, never a verdict); fallback census header
  `rollbacks=48 mirror-detaches=1 rollback-failures=0` (the 1 is
  deterministic failure hygiene — see 5b63838c).
- **Full 8-gate chain** `LEGEND_ENGINE_ROOT=/Users/neemsandv/legend/legend-engine LEGEND_PURE_ROOT=/Users/neemsandv/legend/legend-pure caffeinate -dims tools/allgates.sh` ≈ 5.5 min, green at HEAD.
- **Foundation verdict:** sound. `docs/PLATFORM_FAIL_ADJUDICATION_2026_09_01.md`
  is the record: no silent filter/null-out corruption; the null-vs-value
  trio is the ENGINE's own defect (its corpus admits it in a
  `test.ToFix` sibling); the rest of the "9" are conventions or
  one-mapping bindings, all loud, all named.

## 1. THE DECISION THE NEXT SESSION OPENS WITH

The 848 fallbacks split (census of `target/wholetest-flip-fallbacks.txt`
at HEAD, families grouped by message class):

| family | rows | nature |
|---|---:|---|
| **METAMODEL family** (as-relations + metaprogramming bodies + harness vocabulary) | **~355** | ONE architectural program (§3) |
| platform-fail (9 adjudicated + cosmetic plan/sql-text + named singletons) | 77 | mostly named residue, not burnable by mechanism |
| text-policy (plan-program replayer cohort + by-design) | 65 | §5 replayer design (charter) |
| join-condition reads a whole variable | 43 | one resolver design leg |
| dialect capability (array literal 9, UNNEST 5, LIST_GET 5, banker's ROUND 4, …) | 31 | execution-dialect encodings |
| mapping resolution: class not mapped (cross-store graph-fetch, chains) | 27 | resolver legs |
| filter-predicate isolation (unresolvable after isolation) | 25 | one resolver/lowering leg |
| multiplicity stamp/compat | 23 | stamp program |
| plan-execute parametersValues binding | 17 | the chartered referee-binding cut (charter §5) |
| execution activities not recorded | 14 | harness/platform activities channel |
| post-processors (sqlQueryPostProcessorsConnectionAware 8, replaceTables 4, inline MapperPostProcessor 1) | 13 | recognizer legs |
| dialect-loop asserts (`$expected->map(p\|… assertEquals …)`) | 13 | verdict-layer map-over-literal unroll |
| TDG chained fetch (generator temp tables not replayable) | 12 | oracle sequence replay |
| executeInDb result reads | 7 | named wall by design |
| true singletons / small named tails | ~140 | burn incidentally |

**Option A — metamodel design first (RECOMMENDED).** It is the single
largest lever (~355 rows plus the dissolution of every per-construct
let-chase the bind-once leg had to add), it is the user-ratified
end-state ([[metamodel-in-database-ruling]]), and its precondition —
"the foundation is sound" — is now a receipt. The non-metamodel legs
are each bounded, gated, and independent of the metamodel design, so
they serve as gate-cycle fillers whenever a metamodel step is blocked
on a decision. Nothing in the non-metamodel list unblocks the
metamodel work; serializing ~490 rows of mechanism legs in front of it
only delays the hard design.

**Option B — burn to "only metamodel left".** ~490 rows across ~14
mechanism legs plus a ~140-row tail: several sessions. Every leg
below carries its diagnosis and entry points, so it can be executed in
the listed order without re-deriving. Choose B only if the user wants
the ratchet visibly moving while the metamodel design is being
thought about elsewhere.

Under either option the standing rules are unchanged: one gate chain
per batch, push after green, pins move only WITH their burn and a
written justification, paired same-tree sweeps byte-identical on all
three rosters before any pin move, no envelopes.

## 2. THE NON-METAMODEL BURN MAP — per leg, in execution order

Each entry: witness, diagnosis (receipted), design sketch, entry points,
expected movement. "Size" is an honest estimate of one batch or more.

1. **Dialect-loop asserts (13)** — `testToSQLString.pure` (testCbrt et
   al.): `$expected->map(p| let driver = $p.first; … assertEquals($expectedSql, $result, …))->distinct() == [true]`.
   The verdict layer sees statement-root asserts only; asserts inside
   the map lambda reach the scalar lowerer ("no scalar lowering for
   assertEquals/4"). Design: a map over a LITERAL collection (a
   `PureCollection` of `pair(...)` literals) whose lambda body ends in
   an assert β-unrolls into per-element statement sequences with the
   lambda parameter substituted (the bind-once machinery —
   `UserCallInliner`/`SourceSubst` — owns substitution; add the
   unroll where the whole-test body is compiled, `WholeTestFlip` →
   `Compiler`/`StatementExecutor` statement fold). NOT a bespoke arm:
   it is a general β-reduction of `map` over a literal. Size: one batch.
   Gate: the 13 flip; watch the `->distinct() == [true]` tail (fold to
   a trivially-true verdict after unroll).
2. **Plan-execute values-binding (17)** — charter §5 "referee bindings
   as MINTED LETS": `$plan->execute(parametersValues, ext)` with
   non-empty values walls counted (`StatementExecutor.buildFrame`,
   EXECUTION_PLAN_EXECUTE branch, ~line 1902). Bind each
   `pair(name, value)` as a let over the plan's query lambda (the
   normalization already peels the plan to its `executionPlan(...)`
   build). Size: one batch; the 7 TDG rows that joined this bucket in
   5b63838c come with it.
3. **TDG chained-fetch sequence replay (12)** — `ReplayOracle.tdgSqlReplay`
   line ~384 declines any fetch text touching `tdg_N_*` (the generator
   drops its temp tables in its finally). Design: replay the fetch
   SEQUENCE on both sides — for fetch k, first materialize fetches
   0..k-1 into their `tdg_*` temp tables (the generator's own naming),
   then execute k; multiset compare; drop. Oracle-side (testing) work;
   the platform side already produces the folded `TestDataGenResult`
   literal with the ordered `sqls`. Size: one batch. Note the dialect
   TOP/LIMIT text of these goldens is why they currently fail on the
   text contract — rows are the verdict, so a replay flips them.
4. **Join-condition whole-variable (43)** — witness
   testGroupByAndMilestoning: "join condition reads a whole variable —
   only column reads can correlate sides". A synthesized join whose
   condition references the row VARIABLE (e.g. passes `$r` to a
   function) rather than columns. Design: the resolver's correlation
   channel (`TypedFilter.Stamp.CORRELATION`, `NullSemantics.enterVerbatimEquality`)
   needs a row-struct carrier for the whole-row read, or the condition
   inlines the callee to column reads first (UserCallInliner before
   correlation). Read `docs/EMBEDDED_UNION_NAV_HANDOFF_2026_08_31.md`
   §7 first — it names this leg. Size: 1–2 batches.
5. **Filter-predicate isolation (25)** — "filter predicate references
   column '_', unresolvable even after isolation" (params `_rN`,
   `ms_row`): predicates over columns the isolated select no longer
   carries (milestoning `ms_row` 12, `_rN` 13). Design: the isolate
   must project the predicate's demanded columns (a demand pass before
   `Lowerer.isolate`), or the resolver keeps the columns on the
   milestoned frame. Size: one batch per witness class.
6. **Dialect capability (31)** — array literal 9 / UNNEST placement 5 /
   LIST_GET 5 / banker's ROUND 4 / others: the ENGINE-text dialects
   lack list encodings (`CarrierStrategies`, `EngineStyleH2`). These
   are the same walls the h2 lane multiplies ×863 (Layer 4 below);
   decide with the h2-lane decision, not before.
7. **Mapping resolution: class not mapped (27)** — cross-store
   graph-fetch (`meta::pure::graphFetch::tests::…` 8) and chain
   mappings (`graphFetch::tests::chai…` 4, nested union cross-store
   4): the class's set lives in an included/other-store mapping the
   resolver does not reach. Read `ClassSources.findBinding` +
   `classBindingsWithIncludes`. Size: one batch per witness class.
8. **Multiplicity stamp/compat (23)** — ONE-STAMP/LIST-SHAPE invariant
   (toPostgresModel::testConvertAlias 11, quarantine-adjacent) and
   `[*]` vs `[N]` argument compat (validateAllConstraints 11):
   `docs/STAMP_DISCIPLINE_PROGRAM.md`. The 11 stamp rows sit inside the
   toPostgresModel family (metamodel-adjacent — PROGRAM_MAP says
   "repromote only on a non-quarantine witness").
9. **Execution activities not recorded (14)** — `RelationalActivity`
   reads (`$result.activities`); the platform records one activity per
   execute (`spliceHook.relationalActivitySql`, activity 0 only).
   Design: an activities channel on the executed frame. One batch.
10. **Post-processors (13)** — connection-aware hook shapes
    (`extractSubqueriesAsCTEs` 8), `replaceTables` pair side not a
    schema()/table() navigation (4), inline `^MapperPostProcessor`
    on a connection (1, silent in production — named in the
    adjudication record). Recognizer legs on `RelationalMapperRenames`.
11. **executeInDb result reads (7)** — by design (opaque handle);
    stays named unless a witness reads real data.
12. **Text-policy (65)** — the §5 plan-program replayer cohort (~25:
    allocations + both-ways temp-table conditional) + TDG
    assertSqlEquals-by-design + mixed bodies. Charter §5 is the design.
13. **Platform-fail (77)** — composition (adjudication record): 9
    adjudicated (engine defect / conventions / one-mapping binding),
    ~12 plan-text formatting, ~4 auto-generated lambda names, 12 TDG
    text-contract fails (leg 3 above), 5 ROW-verdict diverged
    (testQualifierQueryWithOr-class isolation shape, hash-function
    spellings, TDS join-strings), 8 datediff-to-now named declines,
    dialect enum-decoded declines, and named singletons. Burnable rows
    here are the ones with a leg above; the rest are receipts.
14. **Also named, silent-in-production (adjudication record):**
    connection timeZone at execution (2 tests, testIn.pure — the
    execution dialect ignores the runtime's timeZone; only the
    engine-text renderer knows it), join-chain terminal binding (1
    mapping corpus-wide), instance-carrier fan-out cardinality,
    TDSNull cross-carrier encoding (instance literal JSON null vs row
    `'TDSNull'`), identity leak on multi-pk `map` (1).
15. **Let-bound TDSRow-typed join lambda (2)** — `let jc = {a:TDSRow[1], b:TDSRow[1]|…}`
    walls at its own let (a nominal TDSRow has no columns; it only
    types against the consuming join's rows). Bind-once charter
    family A: add "lambda with declared TDSRow parameters" to the
    deferred-kind closed list (`Env.withDeferred`, `Typer.deferredLetRhs`)
    and resolve at the consuming join. A design decision on a closed
    list — do NOT chase it through the alias channel (measured and
    removed in 302365b8).

Layer 4 stands: the **h2 lane (G5) has its own 1,618 fallbacks, 863 =
UNNEST placement walls**; walk deletion needs that decision (grow the
H2 execution dialect vs redefine the advisory lane). No plan yet.

## 3. THE METAMODEL PROGRAM — what the design session must produce

**Composition of the ~355 (LL_TMP_DEBUG sweep at HEAD, by shape):**

| shape | rows | witness |
|---|---:|---|
| `FunctionDefinition.expressionSequence` reads (metaprogramming callee bodies: pkOfFunc 43, scanRelations helpers, TDG helpers) | 70 | pkInferenceTests, testGraphFetch |
| class query under `TypedMap` (plan-walk over metamodel values) — "HN vocabulary" | 65 | testSQLCommentsInPlan |
| class query under `meta::relational::mapping::sql` (SQL-node metamodel) | 45 | testRewriteCanAggregateGroupByOnLiteralWithMultipleAgg |
| `meta::legend::executeLegendQuery` / `compileLegendValueSpecification` / `compileLegendGrammar` (harness vocabulary: compile-and-run pure TEXT) | 42 | testDropWithVariables |
| lineage `scanRelations` call typing | 21 | testSameRelationsAtSameLevel |
| mapping-metamodel query functions: `rootClassMappingByClass` 11, `classMappingById` 6, `view` 6, `inferRelationalType` 3, `_classMappingByClass` 1 | 27 | testDynaAndOrInference, testMainTableForB1 |
| `toPostgresModel::newState` (runtime-constructed protocol values compared structurally) | 10 | testConvertColumnName |
| `generateObjectReferences` 7, `routeFunction` 4, `InstanceValue` construction 4, `LambdaFunction` property reads 6, `repeat` 2, `toDomainValue` 2, `resolveStore` 1 | 26 | testObjectReferenceInEmbeddedMapping |
| stamp/compat rows inside toPostgresModel (see leg 8) | ~11 | testConvertAlias |

**The ruling that stands** ([[metamodel-in-database-ruling]], commit
47206a73): NO Java-computed metamodel/lineage/plan fact enters the
verdict path; the end-state is metamodel AS RELATIONS in the database.
Model layer = classes/properties/mappings/tables/joins/columns as seed
rows (INFORMATION_SCHEMA precedent; `SystemMetamodel` is the one-table
v1 where `Class.all()` is a real SELECT). Analysis layer = lineage/plan
trees as adjacency lists computed as a RESOLVER SIDE-OUTPUT (rows, not
text), traversed with recursive CTEs, printed by an owned recursive
query + tiny egress formatter. Metamodel classes get relational
MAPPINGS onto the metamodel tables so pure-over-metamodel lowers
through the one router.

**Homework already DONE (PROGRAM_MAP.md §"DEFERRED PROGRAM", do not
redo):** exact census + witnesses; engine .pure specs read from the
real checkouts (functions_Mapping.pure:28-79, platform_store_relational/
functions.pure:254, relationalExtension.pure:120-137,
toPostgresModel.pure:31-48, extension.pure:62, pkInferenceTests.pure:25-29);
decline mechanism verified; SystemMetamodel v1 scope verified (ONE
table, name only). Leg 1 chosen 2026-08-31: PLAN-NODES-AS-ROWS
(acceptance: the TypedMap-65 plan-walk filter lambdas lower to SQL over
plan-node rows; quarantine partition 172 witness rows + 20 wall tests,
pins in RelationalCorpusRunner, vocabulary
`CanonicalDivergence.METAMODEL_QUARANTINE`).

**Homework OPEN — the design session's deliverables, in order:**
1. **Compile-time fact vs derived-on-the-fly** (PROGRAM_MAP open item 1):
   read `MetamodelWalk.mainTable/resolvePrimaryKey/infer` +
   `MappingNormalizer`; if the compiled model already holds the facts
   (extends-chain main table, groupBy/distinct PK, view column types),
   seeding is a dump and the lowerings are plain SELECTs. Decide.
2. **The seed schema** (grow-by-witness): `metamodel.classes`,
   `properties`, `mappings`, `class_mappings` (fqn key, id, root,
   class, superSetImplementationId), `mapping_includes` (transitive
   closure seeded at extent-render time, not recursive CTEs),
   `schemas/tables/views/columns/view_column_mappings`, `joins`.
   Identity = FQN/path primary key (SystemMetamodel D2 rule). Seed
   cost at corpus scale must be MEASURED (compile-once sweep ~50s
   now; metamodel extent per test/connection unmeasured).
3. **Function-shaped navigation over mapped metamodel rows**
   (`$x->mainTable()` is a function, not a property): mapped
   association vs compiler-synthesized query per native — pick one,
   demonstrate `cast->map(fn)` chains and `assertEquals` over
   row-backed metamodel instances in the store lane.
4. **Trees as data** (expressionSequence 70 + inferRelationalType +
   pkOfFunc): trees-as-rows (adjacency list, resolver side-output)
   vs trees-as-structs. This is the hard end and the biggest row
   count — design it explicitly, with the recursive-CTE print path.
5. **newState (10) + constructed protocol values**: struct-values
   canonical layouts are the one lead (constructed instances already
   lower as structs when the class declares stored properties).
6. **Harness vocabulary (42: executeLegendQuery / compileLegend*)**:
   these compile-and-run pure TEXT inside a test — decide whether
   they are walk-by-design forever (the harness's own vocabulary) or
   platform (a compile-from-string entry through the one router).
   The user's one-router ruling suggests the latter only if it is
   the SAME entry point, never a second evaluator.
7. **Tractability prototype BEFORE chartering the rest**: ONE witness
   end-to-end — testMainTableForB1: seed a class_mappings+tables
   fragment, register one lowering, watch the verdict land in-DB.
   Then leg 1 (plan-nodes-as-rows) as the first shippable batch.
8. **Acceptance + pins plan**: the quarantine partition (172/20)
   shrinks as buckets migrate; each migration lands with its witness
   test, the ledger rows (`JavaEvalLedgerTest`: MetamodelWalk 1307 +
   MetamodelSteps 196 stripped lines) shrink to zero as store
   lowerings claim FQNs; the flip ratchet moves WITH the burn.

**Doctrine reminders for that session:** one router, one evaluator (no
bespoke per-FQN entry points — `TestDataGenerationNatives` is a named
instance of the wrong pattern owing a rename when it migrates); engine
source is oracle material, never runtime; verify every signature
against real legend-pure; measure before claiming; design with
conviction, not menus.

## 4. OPERATIONAL NOTES (all learned the hard way this session)
- **Clock:** the test JVM runs under `-Duser.timezone=GMT` (root pom
  surefire) — engine parity. Do not remove; do not "fix" a datetime
  test by editing the zone. A projected `datediff(..., now())` golden
  declines BY NAME (H2Verify.compareFrame `instantInSelectList`).
- **A frozen-tree ±1 can still be the clock** — check what the oracle
  and the execution each call `now()` before hunting nondeterminism.
- **G8 runs `-am clean` and wipes `core/target/`** — save
  `wholetest-flipped.txt`, `wholetest-flip-fallbacks.txt`,
  `h2-verdicts.txt` to job tmp BEFORE a chain; the chain's own G4
  rosters are gone by the time it reports.
- **There is NO `-Dlegend.corpus.containing` property** (the 09-01
  handoff was wrong); a full sweep is ~50s: `mvn -o -q -pl core test -Dtest=RelationalCorpusRunner -Dlegend.engine.root=… -Dlegend.pure.root=…`.
  `LL_TMP_DEBUG=1` unmasks `[flip-wall-debug]`/`[flip-fail-debug]`
  lines (the folded TDG literal was sitting in that log — read the
  block before designing a mechanism). Sweep logs contain NUL bytes:
  python/awk, never grep.
- **Paired sweeps:** two same-tree sweeps must be byte-identical on
  ALL THREE rosters before a pin moves. Two shells started in the
  wrong cwd this session — every mvn command starts with
  `cd /Users/neema/legend/legend-lite;`.
- **Governance you will trip:** `CodeShapeGuardrailTest` (Lowerer ≤ 3500
  lines — it is at 3499), `JdbcSurfaceCensusTest` (every test file that
  opens JDBC registers with a tenet argument), `JavaEvalLedgerTest`
  (per-file stripped-line pins; SqlTextVerdicts now 592 — a bump needs
  a written justification, routing/recognition only),
  `OwnCorpusConformanceTest` (our own test Pure must parse on the
  4.138.2 oracle: spell a Relation mapping's function as the
  DESCRIPTOR `~func f():meta::pure::metamodel::relation::Relation<Any>[1]`,
  never the mangled `f__Relation_1_`).
- **QueryService/Compiler.execute take a single EXPRESSION** — unit
  tests spell multi-statement bodies as `{| let …; expr; }`.
- **Own-corpus mapping tests:** `Relation { ~func … }` class mappings
  parse in our parser; the seam witness lives in
  `RelationMappingWindowSeamTest` (registered in the JDBC census).
- **Memory files** (`~/.claude/projects/-Users-neema-legend/memory/`)
  `harness-deletion-program`, `sqltext-row-verdict-charter`, `MEMORY.md`
  reflect HEAD; `metamodel-in-database-ruling` is the standing ruling.

## 5. FIRST STEPS FOR THE METAMODEL SESSION — exact entry points (added 2026-09-02, after the homework)

Read first: `docs/METAMODEL_AS_RELATIONS_HOMEWORK_2026_09_02.md` (§7 decisions,
§8 open questions, §9 prototype order, §11 worries). The census scripts and
their outputs are in `tools/metamodel-census/` (`build.py <sweep.log>` then
`scan2.py`, `scan3.py`, `closure.py`, `props.py`; inputs = an
`LL_TMP_DEBUG=1` sweep log + `target/wholetest-flipped.txt`).

**Step 1 — census dump (half a session).** `WholeTestFlip.java:60-110` holds
`BUCKETS` (bucket → count) and `WITNESSES` (bucket → one test). Add a
`bucket → all test names` map written to `target/wholetest-flip-buckets.txt`
at the same shutdown hook. That names the ~88 HN-vocabulary tests the
homework could only count by bucket. Harness-only; no pin moves.

**Step 2 — run-time branch choice on a row's type column (one session).**
Today: `MatchChecker.java:18-70` selects a `match` arm STATICALLY by the
input's compile-time type; when an arm is a strict subtype of the input
type it keeps all arms in a `TypedMatchRuntime` "for the host channel";
`lowering/MatchFold.java` folds that node statically and says verbatim
"a genuinely polymorphic input (class hierarchies) stays a loud wall";
`CollectionLanes.java:199-200` refuses both node kinds; `instanceOf` folds
only when statically decided (`Scalars.instanceOfFold`, :2508). Rows from
inheritance/union mappings already carry a subtype column
(`ClassMapping.isSubTypeColumn` :62, `UnionSynthesis`, `subType(@X)` reads
in ClassSources). Build: lower a `TypedMatchRuntime` whose input row has a
discriminator into `CASE <kind> WHEN … THEN <arm body> …` for VALUE arms,
with each arm's parameter bound to the narrowed row (the subtype's columns);
`instanceOf` → `kind = '…'`; `cast(@Sub)` → the narrowed row (a wall if the
kind does not match at run time is acceptable for v1, documented). Witness:
a NEW unit test on an ordinary user inheritance mapping (no corpus test in
tests/mapping/inheritance or extends uses `->match` — grep confirmed), plus
the metamodel navigations themselves (`mainTable` = match Table/View). Row-
returning arms (UNION of arms) are step 4's problem, not this one's.

**Step 2 LANDED (2026-09-02).** Placement differs from the sketch above,
on evidence: the dispatch happens in the RESOLVER's class-lambda
substitution (`Substitution.typeDispatchArms`), not the Lowerer — the
union/inheritance row already carries the discriminator as the
`$member` membership witness (`ClassMapping.memberWitness`, NULL in
non-conforming threads) and per-subtype `stc_` columns registered under
`SUBTYPE_KEY`; `MatchFold`/`instanceOfFold` never see a head-variable
dispatch any more. Forms: `$p->instanceOf(Sub)` → `isNotEmpty(witness)`;
`$p->match([s:Sub[1]|v,…])` → nested `if` over witnesses, catch-all arm
(`Any` / the input's class / a TOTAL-membership subtype) as the ELSE,
otherwise ELSE = `fail('Match failure …')`; `$p->cast(@Sub).prop` →
`if(witness, stc read, fail('Cast exception …'))`. `fail` gained its
scalar lowering (`ERROR(...)`, cast to the position's carrier — no
dialect inference). A subtype the row carries no columns for is LOUD.
Witness: `RuntimeTypeDispatchTest` (8 cases, rows are the verdict, an
ordinary user inheritance mapping). Corpus: byte-identical rosters,
848/1725 unchanged (no corpus test dispatches on a union row).
`collectSubTypeFqns` now also demands match-arm / instanceOf / cast
targets. Residue named: row-returning arms (step 4), navigation through
an arm parameter (`$c.mechanic.name`), a match `extra` argument,
supertype arms other than `Any`/the input's class.

**Step 3 — prototype 1, testMainTableForB1 (one session).** Witness:
`tests/mapping/extend/testExtendsForMainTable.pure` (`B1Mapping->classMappingById('b1')->cast(@RootRelationalInstanceSetImplementation)->map(x|$x->mainTable())` equals the super mapping's). Pieces:
- Seed tables, grown from `SystemMetamodel.java` (its `SOURCE` is the Pure
  text of the store + mapping; `seedStatements` renders DDL+INSERT from the
  active `ModelContext`; injected at `Compiler.java:232/267`; resolver hook
  `StoreResolver.java:1244`): add `metamodel.mappings(fqn PK)`,
  `metamodel.class_mappings(mapping_fqn, id, class_fqn, root, super_set_id,
  main_db, main_schema, main_table)`, `metamodel.mapping_includes_closure
  (mapping_fqn, included_fqn)`. Source of the rows: `MappingDefinition
  .ClassBinding` (:89, Relational carries `RelationalSource.Table`),
  `classBindingsWithIncludes`, `MappingNormalizer.mainTableDefOf`.
- Map `meta::pure::mapping::Mapping` and `RootRelationalInstanceSetImplementation`
  in the system mapping (inheritance mapping with a `kind` column for the
  set-implementation subtypes; `~primaryKey` = mapping_fqn + id).
- Natives implemented as queries (spec = the engine bodies cited in the
  homework §2b): `classMappingById` (closure over includes),
  `rootClassMappingByClass`, `mainTable` (Table vs View arm). Register in
  the native catalog (`Pure.java`; the catalog golden line diff is the
  conscious registration; verify signatures against
  `legend-pure/…/platform_dsl_mapping/functions_Mapping.pure:61/74` and
  `platform_store_relational/functions.pure:277`).
- Verdict: `assertEquals` of two Table rows → row equality in the DB.
- Pins that move: `CanonicalDivergence.METAMODEL_QUARANTINE` (:513) and
  the runner's quarantine counts (172 witness rows / 20 wall tests,
  `RelationalCorpusRunner.java` ~:1219/:1223) shrink; the flip ratchet
  moves +tests; Java-eval ledger rows for `MetamodelWalk` (1307) /
  `MetamodelSteps` (196) must NOT grow — the whole point.
- Acceptance = the verdict lands with zero test-specific Java; then
  `testMainTableForB2..` and the 5 extends tests follow for free.

**Step 3 LANDED as a PARTIAL prototype (2026-09-02) — mechanisms proven,
the witness's verdict blocked on ONE named resolver gap.** Landed (all
platform code, zero test-specific Java; witness
`MetamodelMappingStoreTest`, 9 cases, rows are the verdict):
- **Seeds** (`SystemMetamodel` schema + `MetamodelSeeds` rows, seeded per
  execution like `classes`): `mappings`, `class_mappings` (one row per
  RELATIONAL class mapping; the compiler's stamped, extends-resolved main
  table — P4 receipt: `B[b1] extends [a] {}` carries `ABC`),
  `mapping_includes_closure` (reflexive-transitive; a ROW ENTITY
  `meta::lite::metamodel::MappingVisibility` with associations
  `viewer`/`visibility` and `visible`/`visibleFrom` and `visibleSets`),
  `table_aliases` (the set's main-table alias as its own relation, keyed
  like the set — never a self-join), `tables`. Null seed cells render as
  NULL (`Ddl.metamodelSeed`).
- **Metaclasses mapped**: `Mapping[mapping]` (+ real m3 `classMappings`),
  `SetImplementation` as an INHERITANCE op whose member is
  `RootRelationalInstanceSetImplementation[rootRel]` (`id`,
  `superSetImplementationId`, `parent`, `mainTableAlias`),
  `TableAlias[alias]` (`name`, `relationalElement[tbl]`),
  `RelationalOperationElement` as an inheritance op, `Table[tbl]`. Native
  class growth, all real-m3 spellings: `PackageableElement` (new),
  `Mapping extends PackageableElement` + `classMappings`,
  `SetImplementation.parent: Mapping[1]`,
  `RelationalMappingSpecification.mainTableAlias: TableAlias[1]`
  (NativeFunctionTest surface pins moved; class count 211→212).
- **Natives as Pure bodies** (the one router inlines them; the engine
  bodies are the SPEC, ours read our rows): `meta::lite::metamodel::classMappingById` =
  `Root.all()->filter(cm | $cm.id == $id && $cm.visibilityOf.viewer->exists(v | $v->elementToPath() == $_this->elementToPath()))->first()`;
  `meta::lite::metamodel::mainTable` = `$_this.mainTableAlias.relationalElement->cast(@Table)`.
  They carry LITE names for now: taking the real FQNs away from the
  natives turned 6 extends tests that the LEGACY WALK scores through
  those natives into walls (scoreboard `tests/mapping/extends` 23→17 —
  the corpus-regression gate caught it, measured and reverted same
  session). The switch-over is one rename, owed to the navigation-depth
  leg that flips the witnesses. New native
  `elementToPath(PackageableElement[1])` (real elementToPath.pure:44):
  over a REFERENCE it is the path literal; over a metamodel ROW it is
  the row's key (the D2 identity) — the `$pk:<col>` pseudo-binding
  `ClassMapping.primaryKeyBinding`, registered beside the subtype
  pseudo-bindings and never serialized.
- **D3 — element reference = row** (`ElementReferences`, resolver): a
  reference to a tracked, system-mapped element (`ext::B1Mapping`) is
  its metaclass extent filtered on its key — an ordinary object-space
  filter, so it rides every position a class filter rides; a BARE
  reference (a `from()`/`execute()` argument) stays a value (`Anchors`:
  a reference anchors only as the SOURCE of a navigation). D1 widened:
  "intrinsic" = registry-tracked OR bound in the system mapping.
- **Chain-position `->cast(@Sub)`** = re-typing when the mapping PROVES
  totality: the hop is routed to one member set whose class conforms
  (`ModelContext.routedTargetClass`), or the extent's members all
  conform (`unionMemberClasses` / inheritance subclasses). Partial stays
  loud by name. A single-entry routed navigation into an Operation-mapped
  root with no set of its own now lands on the routed set's class
  (`JoinChainEmission`); multi-entry routes keep per-arm dispatch (the
  first cut of this re-targeted every entry and cost 30 inheritance
  tests — measured and reverted same session).
- **Normalizer**: every hierarchy-walk class lookup is native-first
  (`MappingNormalizer.classDef`, primitives excluded); inheritance
  members enumerate the native catalog too; a property-less union root
  with subtype-dispatch columns is allowed; `isSubclassOf` has a cycle
  guard (LeniencyD6/VarianceD4 caught the overflow the wider universe
  exposed). `SqlTextRatchetTest`'s string-literal regex is unrolled (a
  multi-KB text block overflowed the naive alternation).
- **Receipts**: ratchet **848/1725 UNCHANGED**; paired sweeps
  byte-identical on all four rosters; the only bucket movement vs batch
  2 is one test (`testBuildFilterWithValueThatCanBeNullWithIn`) reaching
  the new chain-cast wall by name instead of an earlier wall; quarantine
  pins 172/20 UNCHANGED (the real-name natives keep their refusal
  spelling); scoreboard unchanged (2350). Full core suite green. Java-eval
  ledger rows unchanged (MetamodelWalk 1307 / MetamodelSteps 196).

**Residue, named (the next resolver leg — "navigation depth"):**
1. The witness itself: `B1Mapping->classMappingById('b1')->cast(@Root)->map(x|$x->mainTable())`
   composes FOUR flatten hops; the nested-navigation machinery walls at
   the third hop after two association hops
   (`witnessResidueIsNamed` pins it: "navigation through class-typed
   slot property ... not supported yet" / "is not mapped"). Two-level
   navigation inside a nested predicate has the same limit. Fixing depth
   is the right move (the user's call: fix, don't reshape) — it is a
   resolver leg on `flattenSource`/`nestedAssocMaterials`, not a
   metamodel design question.
2. Under the lite names the corpus chain (probed) stops at the chain
   cast: `->cast(@Table) over RelationalOperationElement ... partial
   membership` — the flatten path reaches the cast without the route
   fact (the same cast is total by route from a `Root.all()` head).
3. Views as main tables: `table_aliases → tables` only (a `~mainTable`
   view yields no row; engine `mainTable` recurses into the view).
4. `SetImplementation.all().parent` from the UNION side (an end mapped
   on the member set, read off the inheritance row) walls; `parent` is
   read off `Root` rows fine.
5. Assert failure MESSAGE rendering over instance values
   (`toRepresentation for LinkedHashMap is not modeled`): the verdict is
   right, the failure text is not printable yet.
6. The Java-eval ledger did not grow (MetamodelWalk 1307 / MetamodelSteps
   196 unchanged); their `classMappingById`/`mainTable` arms still score
   the 6 extends tests until the platform flips them — the rename above
   deletes them.

**Navigation-depth leg — LANDED (batch 4, 2026-09-02).** Residue items 1
and 2 above are closed; 3, 4, 5, 6 remain (see the list below).
- **Mechanisms (resolver only, no dialect coupling, no test hooks)**:
  nav TAILS ride through `flattenSource`'s association branch (the whole
  remaining hop chain + each hop's consumed paths; provenance registered
  as `AssocSub`/`SubNav` trees relative to the materialization's ROOT
  target row — ONE prefix convention, `NavMaterializer.composeSubNavPrefixes`
  / `StoreResolver.rebaseSubNavs`); `Substitution.rewriteMultiHop` gained
  the chain-key + SubNav descent read (`chainKeySubNavRead`);
  `ClassSource.composedPrefix` re-points a chained condition after a
  filtered association hop; `AssociationJoins` materializes deeper tails
  recursively through `NavMaterializer`; DOTTED emptiness registers
  inside nested scopes exactly as at the root (`DottedExists`, extracted;
  the path collector `EmptinessPaths` takes the terminal's lambdas at
  the root and nothing else in a nested scope — no nullable mode flag);
  `Pipelines.walk` (join-slot materializer) gained arms for limit / drop
  / slice / sortBy / a resolver-synth join above the slots and its
  default arm is LOUD on a leftover navigate (it used to pass a `first()`
  wrapper through silently, leaving the slot unmaterialized);
  `FlattenOps.innerizeOrNull` descends projection / limit / distinct
  wrappers to find the nested navigate join; `AssociationSynthesis`
  injects routed PMs only when the binding path is impossible (an
  inheritance-mapped target end keeps its binding under a filter).
- **classMappingById is the NATURAL body now**:
  `$_this.visibility.visible.classMappings->filter(cm|$cm.id == $id)->first()`
  (the elementToPath reshape is gone; `elementToPath` stays as a native
  with its own pin). `MetamodelMappingStoreTest.witnessMainTableForB1`
  asserts the witness verdict TRUE and the main tables as rows.
- **Pinned**: `NavigationDepthTest` (4 cases / 22 shapes on an ordinary
  user model: 3–4 hops, ops between hops, nested-predicate depth 2,
  inheritance-mapped association ends) — registered in the JDBC census.
- **DuckDB driver re-pinned 1.5.0.0 → 1.4.4.0 (root pom)**: 1.5.0
  returns ZERO rows for `SELECT .. FROM (.. WHERE .. LIMIT 1) t WHERE
  t.c IS NOT NULL` over a join chain — reproduced in five lines, fails
  with the optimizer disabled too, correct on 1.4.4; upstream
  duckdb/duckdb#21160 ("duckdb 1.5: Issues around LIMIT", open). The SQL
  is the ordinary `first()` lowering and is legal everywhere; the corpus
  is indifferent to the driver (identical rosters). Revisit at 1.5.1.
- **Receipts**: ratchet 848/1725 → **847/1726** (+1:
  `testNestedExistsWithExistsInAbstractProperty`, wall-exec "predicate
  references column '_'" → platform pass); H2 verdict roster
  byte-identical to the c20859da baseline; paired same-tree sweeps
  byte-identical on all four rosters; quarantine 172/20 unchanged;
  extends 23/23 unchanged; dual-channel 613 agree / 0 disagree;
  exec-passing 345 unchanged; full core suite green (4378). Java-eval
  ledger rows unchanged (the walk still scores the extends witnesses
  under the real names — next batch).

**Batch 5 — the REAL-NAME switch — LANDED (579b1171, 2026-09-02).** Residue item 6
above is closed; 3, 4, 5 remain (plus the two named below).
- **Pure bodies under the real names** (SystemMetamodel):
  `meta::pure::mapping::classMappingById`, `meta::relational::metamodel::
  mainTable`, `meta::pure::mapping::superMapping`, `meta::pure::mapping::
  allSuperSetImplementations`, `meta::relational::mapping::resolvePrimaryKey`
  (the engine's this-vs-super precedence as ONE chain over the ancestry
  rows: filter by any key fact, sort by rank×1000+depth, first, then
  `.ancestor.primaryKey`). The five natives, the `MetamodelSteps` arms
  and `MetamodelWalk`'s classMappingById/superMapping/
  allSuperSetImplementations/resolvePrimaryKey/primaryKeyOf/mainTable/
  tableHandle(3)/classMappingByIdIn are DELETED (reach-back census 3→2).
- **New rows**: `set_ancestry` (reflexive-transitive extends closure with
  depth — `meta::lite::metamodel::SetAncestry` + associations
  `ancestry`/`ancestor`), `group_by_mappings` (m3 GroupByMapping, new
  native class), `primary_keys` (the compiler's population rule — user
  ~primaryKey, else ~groupBy columns, else ~distinct → own mapped
  columns, else the main table's PRIMARY KEY — one TableAliasColumn row
  per column), `columns` (Column rows); `class_mappings` gained
  `distinct_set`/`user_defined_pk`. `RelationalMappingSpecification`
  gained `userDefinedPrimaryKey`/`distinct`/`groupBy` (real
  relationalMapping.pure).
- **Compiled artifact**: `MappingDefinition.ClassBinding.Relational.declared`
  (`DeclaredKeys`: the set's OWN ~distinct/~groupBy/~primaryKey/column
  PMs, captured BEFORE the extends pre-pass merges the parent in —
  `SetKeyFacts`); a function-form binding declares NONE (named gap).
  `GroupBySynthesis`: a per-row PM outside the ~groupBy key list is
  WITHHELD (the Join-PM rule), no longer a poison — the engine compiles
  such mappings (the primaryKey fixtures map `id` beside ~groupBy(aName)).
- **Resolver (general, not metamodel-specific)**: D1 dispatch — an
  intrinsic metaclass (bound in the system mapping, or an abstract class
  whose subclass is) dispatches to the system mapping under an EXPLICIT
  user mapping too (the corpus runs every test under its mapping);
  `InferenceKernel.mostSpecific` — same-name module overloads resolve
  to the most specific class-typed parameters (the engine's
  `resolvePrimaryKey(RISI)` / `(ISI)` sit beside the root-set body);
  `SystemMetamodel.injectInto` shadows FUNCTIONS by signature (a same-
  name overload is not a shadow); a TO-MANY navigation after `first()`
  / limit / drop / slice stays in the chain and joins ABOVE the op
  (`A.all()->first().links.tag` returned ONE link — `rowCountOpBelow`,
  the tail/extra-head rules keep to-many tails out of a target beneath a
  row-count op); a sort below a flatten hop splices (`applyBelow`); a
  TO-ONE hop with ops below it joins FIRST and IS the below scope's
  material for its head (`preJoins` — the second join doubled every
  column name); a slot hop off a composed source whose step was
  stripped splices the class's own step (`NavProvenance.spliceOwnStep`).
  Extractions for the file guardrail: `NavProvenance`, `SetKeyFacts`,
  `FlattenOps.consumedPaths/rowCountOpBelow`, `Pipelines.
  widenPipeForJoinKeys`.
- **Pinned**: `MetamodelMappingStoreTest` (+extendsChainAsRows,
  +resolvePrimaryKeyPrecedence with the engine's own mapping shapes),
  `NavigationDepthTest.toManyAfterRowCountOps`.
- **Receipts**: extends 23/23 — ALL SIX formerly walk-scored tests now
  platform-scored; ratchet 847/1726 → **841/1732** (+6, exactly those
  six); quarantine witness rows 172 → **151** (the classMappingById
  refusal spelling retired), walls 20; H2 verdict roster byte-identical;
  paired sweeps byte-identical on all four rosters; dual-channel 613/0;
  exec-passing 345; core suite green; the global-compile failure of
  `pureToSqlQuery::getGroupBy` ("Unknown type GroupByMapping") is gone.

**Batch 6 — residues (2026-09-02).** `parent` from the union side reads
(closed by batch 5's dispatch/depth fixes — `SetImplementation.all()
.parent.name`, probed). Assert failure MESSAGE over instance rows renders
the spec's `<id instanceOf Type>` (toRepresentation.pure:28; id = the
synthetic site identity when carried, else the row's property values in
wire order — identity as data; T = the side's static class, `?` when the
side is not class-typed); pinned in `MetamodelMappingStoreTest.
instanceFailureMessageRenders`; corpus: `testSelfJoinPropertyMapping`
moved from the "toRepresentation … not modeled" wall to an honest
platform-fail with its message (ratchet unchanged 841/1732, paired
sweeps byte-identical). PureAsserts ledger pin 311 → 313 (message text,
no verdict).

**VIEWS AS MAIN TABLES — chartered, NOT built** (no corpus witness: the
only corpus caller of `mainTable()` is the extends file, 23/23). The real
body is `$_this.mainTableAlias.relationalElement->match([t:Table[1]|$t,
v:View[1]|$v->mainTable()])`; the honest relational model is View rows
(+ a view alias row keyed by the view, base table resolved transitively
at seed time — the extends-closure pattern) with `relationalElement`
routed to Table OR View, which makes the read a PARTIAL-membership
dispatch in CHAIN position. The mechanism gap (batch 2 built the
instance-variable form only), exact walls: "class query under
TypedMatchRuntime is not resolvable yet" (`chain->match([...])`),
"object-space expression node TypedMatchRuntime is not substitutable yet"
(`->map(x|$x.nav->match([...]))`), and for the cast form "->cast(@T)
over a chain of U whose mapped members do not all conform (partial
membership) is not supported in chain position yet" — that last one HAS a
corpus witness: `meta::relational::validation::tests::milestoning::
testValidateQueryWithUnion` (`->cast(@RelationalActivity)` over
Activity). Semantics to build: retype the chain to the member set, gate
each read on the witness with fail('Cast exception') for non-conforming
rows (never a silent filter), reads of member properties through the
SUBTYPE_KEY dispatch. Then the View rows land on top.

**Harness burn-down leg 1 — chain-position type dispatch — LANDED
(2026-09-02; user ratified the FULL burn-down: every Java-scored test
runs on the platform through the one compile path).** `chain->cast(@Sub)`
over a partial-membership row keeps the union row GATED: `ChainDispatch`
adds a filter whose predicate RAISES on a non-conforming row
(`if($v->instanceOf(Sub), |true, |fail('Cast exception …'))` — pure's
cast exception, never a silent filter) and stamps `ClassSource.castGate`
so reads of the target's own properties are the value-position witness-
gated subtype reads (`Substitution` RowScope.castGate → castLeafRead);
`chain->match([...])` IS `chain->map(v|$v->match([...]))`;
`->map(o|$o.nav->match([...]))` splices the source for the parameter (the
class-result-map rule) onto the chain form. Two general fixes:
`routedTargetClass` returns ONE class only when every route of a
`prop[set1]`/`prop[set2]` property lands on it (the first route used to
win and a cast over the union target was judged total); the peeled
scalar read's leaf carries the property's own multiplicity over an all-
to-one path (the chain's `[*]` tripped the carrier stamp of a gated
read). Pinned: `ChainTypeDispatchTest` (raises + rows, incl. a two-route
navigation). Corpus: ratchet unchanged 841/1732, paired sweeps byte-
identical; the one chain-cast wall (`->cast(@RelationalActivity)` inside
the inlined `validate` body) moved to its next wall (the plan/text
family). Named: a partial cast BELOW a flatten hop, or a second cast on
one chain, stays loud.

**Harness burn-down leg 2 — views as main tables + ROW-arm match —
LANDED (2026-09-02).** `View` native = the real class (NamedRelation +
RelationalMappingSpecification; schema/primaryKey/columnMappings); View
rows (`views`, top-level views in `default`); ONE alias table for every
main-table alias — owned by a set (mapping_fqn + id) or by a VIEW (its
database + `view:<schema>.<name>`, identity in view_* columns) — with the
BASE TABLE resolved transitively through views of views at seed time
(base_* columns; the extends-closure pattern) read through the lite
association `TableAlias.base` (`AliasBaseTables`); `relationalElement`
routed to Table OR View (`relationalElement[tbl]`/`[vw]` — the proven
two-route shape); `mainTable` body = `$_this.mainTableAlias.base` (the
engine body `match([t:Table|$t, v:View|$v->mainTable()])` recurses; ours
reads the seeded base — the classMappingById/include-closure precedent).
`chain->match([...])` with ROW arms = the UNION of one filtered, cast
branch per arm (`ChainDispatch.chainMatchAsUnion`; normalized before the
chain walk, also for a class-result `map(x|…)` whose spliced body is such
a match); a scalar map and the whole-instance terminal DISTRIBUTE over a
class concatenate like project does. Pinned: `viewAsMainTable`
(relationalElement->cast(@View).name = AV; mainTable().name = ABC),
`ChainTypeDispatchTest.chainMatchWithRowArms`. Corpus: ratchet unchanged
841/1732, paired sweeps byte-identical; `concatenate::testAll` moved from
a wrong-SQL platform-fail to the named wall "lowering not yet implemented
for TypedSerializeGraph" (whole-instance over a concatenate now reaches
the graph lowering).

**CHARTERED — cast then navigate through the member's own slot (re-root
at the member set).** The union-ROW form carries member properties as
thread-local columns but NOT a member's join slots, so
`chain->cast(@View).mainTableAlias…` (the real mainTable body's view
arm) cannot navigate on the row. Exact wall: "->cast(@View) over a chain
of RelationalOperationElement (partial membership) below a flatten hop,
or a second cast on one chain, is not supported yet". The mechanism:
re-root the gated chain on the member SET's own extent (inner-join the
union rows to `View[vw]` on the member's key thread), so the chain
continues natively with the member's slots. No corpus witness yet; the
metamodel witness is the real `mainTable` body verbatim.

**Harness burn-down — GROUP F LANDED (2026-09-02, batch 7): 841/1732 →
**820/1753** (+21 flipped: 16 typeInference row-navigated tests, the 4 constructed-instance tests incl. joinStrings, testSubTypeMappingValidWhenMappedExplicitly); paired sweeps 5/6 byte-identical rosters at the landed state; scoreboard `tests` family 33/39 unchanged, corpus total unchanged; exec-passing 345 → 344 and M1 verified 83 → 82 (lane move, charter §8.3); metamodel quarantine 151 rows / 20 walls → 125 / 9 (the four refusal spellings retired); dual-channel 613 agree / 0 disagree.** The 20 typeInference tests of testRelationalExtension.pure +
testSubTypeMappingValidWhenMappedExplicitly are platform-scored; every
function they compose is a Pure body under its REAL name over seeded rows
(the six natives DELETED; the walk's mapping / set / property-mapping /
view / type handles DELETED; MetamodelSteps' six cases DELETED; the four
quarantine spellings retired).
- **Rows (MetamodelSeeds + OpSeeds → `RelationalOpRows`)**: `databases`,
  `schemas` (+ the engine's `default` for top-level tables/views),
  `properties` (class-declared stored properties + association ends a
  mapping binds), `data_types` (one row per column declared type and per
  inferred node type, m3 subclass simple name + size/precision/scale),
  `relational_ops` (every mapping / view expression as a NODE TREE: kind,
  parent, ordinal, dyna name, literal, column reference, inferred type,
  and the compiled primary-key columns as TableAliasColumn nodes owned by
  their set — `primary_keys` and the second `TableAliasColumn` set are
  GONE), `view_column_mappings`, `property_mappings` (effective across the
  extends chain, `declared_depth` says own vs inherited);
  `class_mappings.root` (the `*` set, else the class's sole own set —
  MappingValidator.validateStar), `mapping_includes_closure.include_rank`
  (the engine's visit order: includes first, the viewer last).
  Physical columns renamed around the composed-row prefix collision
  (`mapped_class_fqn`, `prop_owner_fqn`/`prop_name`, `col_*`, `itype_id`,
  `dtype_id`).
- **Metaclasses**: `CoreDataType` + 21 datatype kinds (real
  relational.pure:392-520), `Property` (name), `SetImplementation.root:
  Boolean[1]` + `class: Class[1]` (declared RAW — the normalizer classifies
  class-typed properties by NameRef; the generic form is a normalizer leg),
  `PropertyMappingsImplementation.propertyMappings`,
  `PropertyMapping.property`. DataType is an inheritance op over 21
  FILTERED sets of one table (the engine's single-table-hierarchy idiom);
  the op-node kinds likewise (5 filtered sets of `relational_ops`).
- **Bodies**: `_classMappingByClass` =
  `$_this.visibility->sortBy(v|$v.includeRank).visible.classMappings->filter(cm|$cm.class == $class)`;
  `rootClassMappingByClass` = real (`->filter(root)->last()`); `view` =
  real; `allPropertyMappings` = `->cast(@Root).effectivePropertyMappings`;
  both `propertyMappingsByPropertyName` overloads = real filter over it;
  `inferRelationalType` = `$rop.inferredType` (the compiler's stamp — the
  include-closure precedent; the engine recurses the type lattice per
  query, ours reads the compile-time fact; the recursive form over the
  node rows is the honest end state and is NOT built); `dataTypeToSqlText`
  = the real match over the 21 subclass rows.
- **Mechanisms (all general, each forced by a named test)**: M3 primitive
  names win in name resolution (a class called Integer/Date is reachable
  only qualified — `NameResolver`); a property-less inheritance member
  still gets its membership witness (`UnionSynthesis`); `last()` over a
  sorted chain = `first()` over the reversed sort (`ChainNormalizer`);
  element identity equality `$x.cls == Element` = the navigation's
  FOREIGN-KEY IDENTITY pseudo-binding `$fk:<prop>` (registered beside
  `$pk:` when the slot's join is one equality on the target's key —
  `ForeignKeyIdentity`; a plain row read, resolves in every scope);
  three union-projection gaps (association keys through union member
  threads — `AssociationJoins.memberAssocKeyReads`; nested union targets
  widened for downstream hops — `NestedUnionKeys`; hoisted steps over a
  union source — `Pipelines.widenConcatenateBelow`); a stripped slot in a
  nested scope splices the class's own step (`CorrelatedSubselects` ←
  `NavProvenance.spliceOwnStep`); sibling key typing reads the member's
  SOURCE row (`Pipelines.widenUnionMember` bug); system function bodies
  REPLACE a same-signature model function (the corpus carries the
  engine's own `inferRelationalType` source; resolve → inject → resolve,
  types compared by spelling); the metamodel seeds skip a corpus class
  that does not compile (one dangling protocol type broke every
  store-reading test); `innerizeFlattenJoin` descends a spliced sort; H2 spells
  `format(<literal template>, …)` as `||` concatenation with `%d`/`%s`
  slots CAST to VARCHAR (H2 has no printf — `H2.formatAsConcat`); a
  FILTERED single-table hierarchy (one ~filter per subclass set — the
  engine's own idiom, the datatype metamodel's 21 kinds) synthesizes as
  ONE scan with filter-gated columns and witnesses, the thread restricted
  to rows some member claims (`UnionSynthesis.mergedScan`; H2's planner
  re-evaluated the 21-arm union derived table per outer row over the
  corpus-sized store and hung — DuckDB hash-joined it); a lone projected
  thread widens for join keys like a union member
  (`Pipelines.widenConcatenateForKeys`).
- **Constructed instances as ROWS (the ruling's side-output rows)**: a
  query's `^DynaFunction(...)` / `^Literal(...)` / `^LiteralList(...)` tree
  over constants becomes `relational_ops` rows (ONE builder,
  `RelationalOpRows`) keyed by a content id, the expression becomes the
  member class's extent filtered on that id (`ConstructedInstances`,
  anchored like an element reference), and the rows ride the resolver →
  `ExecEnv.constructedSeeds` → the execution setup seeds them after the
  model's own. A row-valued argument (a navigated element) is admitted
  only under an argument-free type rule (`joinStrings` = VARCHAR(4000)
  whatever it joins) and seeds no child row.
- **Pinned**: `MetamodelQueryFunctionsTest` (7 cases: schemas/views, view
  column inference incl. join and view-on-view, class mappings by class
  through includes and `*`, property mappings by name incl. inherited and
  association ends, property inference incl. concat/plus/case, constructed
  instances, Column.type + dataTypeToSqlText).
- **Pins moved**: ratchet EXACT 841/1732 → 820/1753; exec-passing lane
  345 → 344 (charter §8.3 record); metamodel quarantine 151 rows / 20
  walls → 125 / 9; required-over-nullable ceiling 520 → 529 (the
  single-table hierarchies' kind-specific columns, non-null on their own
  set's rows); H2 lane pass floor 1329 held; M1 h2-exec verified floor 83 → 82
  (testSubTypeMappingValidWhenMappedExplicitly's assertSameSQL row-verifies
  through the oracle SPI now — lane move); native class count 213 → 236;
  native catalog −6; Java-eval ledger StatementExecutor 2571 → 2594
  (ExecEnv carries the side-output rows, no evaluation), OpSeeds
  registered; reach-back census MetamodelWalk 2 → gone, MetamodelSeeds 1
  (property mappings live only on the parse artifact).
- **Named residue**: embedded / inline-embedded / otherwise-embedded and
  local-property mappings seed no rows; a join-slot mapping seeds its
  terminal column only (no JoinTreeNode rows); `Mapping.associationMappings`
  and the aggregation-aware half of `_classMappingByClass` are unseeded;
  `SetImplementation.class` is raw `Class[1]`; `dataTypeToSqlText` omits
  the Boolean / Json / DbSpecificDataType arms (no store type models them);
  `inferRelationalType` reads a stamp, not a recursive query;
  `testTranslateDbType` (extension-lambda `TranslationContext`) stays a wall.

**Batch 8 (2026-09-02) — chain speed: the system database + two normalizer
bugs (docs/GATES.md "Budget BREACH, 2026-09-02").** Ratchet UNCHANGED
820/1753; corpus report byte-identical; H2 verdicts identical. Landed:
- **THE SYSTEM DATABASE (user ruling: "insert once at first compile,
  separate from user connections").** `exec/SystemDatabase`: one in-memory
  database per GRAPH per engine (the engine follows the session: H2 lane
  stays on H2), created and written the first time a query of that graph
  reads the metamodel, alive as long as the graph
  (`ModelContext.derived` — graph-lifetime facts, overlays share them).
  The executor ROUTES a store-reading body to it (`routeSystemStore`);
  `seedMetamodelStore` (per-execution DROP+CREATE+INSERT) DELETED. A
  query's constructed instances insert content-addressed (once per id).
  A body reading the store AND a user store is LOUD (census: none).
- **Normalizer**: `mergedScan` compared branch expressions by PRINTING
  them (quadratic) — record equality; `collectInheritanceMembers` scanned
  the whole class universe with chain walks per inheritance op — a
  direct-subclass index (`ModelBuilder.directSubclasses`,
  `Pure.directNativeSubclasses`), same membership and order. Model
  compile 28.2 -> 8.0ms.
- **Harness**: `TimingLedger.addNamed` — the corpus ledger lists the 30
  slowest tests (`slowest` rows in target/timing-ledger.txt); allgates
  keeps g4/g5 logs on failure like g1. (The corpus filter ALREADY exists:
  `-Drcorpus.only=<family-substring>`, `-Drcorpus.test=<test>`.)
- **Vocabulary**: "model" here = the GRAPH (the whole compiled universe:
  every package, dependency and the platform; `Class.all()` = every class
  in it). One system database per graph; many graphs only in the test JVM.

**Batch 9 (2026-09-02) — ONE TABLE for the RelationalOperationElement
hierarchy.** tables/columns/views/table_aliases/relational_ops →
`relational_elements` (kind, id PK, 29 columns; the ONE row layout lives in
`RelationalOpRows`' factories); Table/View/Column/TableAlias and the five
node kinds are FILTERED sets over it; every cross-kind join is a self-join
(`{target}`). General fix: a self-join between two DIFFERENT classes over
one table oriented backwards in `AssociationSynthesis` (the same-class
convention was applied) — orientation now follows the property line's
source set. Rosters, corpus report and H2 verdicts byte-identical;
required-over-nullable ceiling 529 → 533 (Table/View/Column/TableAlias
`name` over the shared column — the idiom's cost, recorded on the pin).
The H2 lane is STILL ~139s: the extent is still a UNION ALL of six
branches because the single-scan collapse skips members WITH navigation
chains (Column's 21 type routes, View, TableAlias, TableAliasColumn), and
member keys are CASE-gated + OR-joined — item (2) below. Denormalization
check (user question): table-per-POLYMORPHIC-hierarchy, nothing repeated;
the one smell is TableAlias' nine name-triple reference columns — item (6).

**Batch 10 (2026-09-02) — UNION LOWERING for single-table hierarchies:
the H2 lane 137s → 41s (ten typeInference tests 9–18s → <1s).** Three
general mechanisms, rosters / H2 verdicts byte-identical, ratchet 820/1753:
- **Every filtered member of one table merges into the ONE scan**: a
  member's scan source is its table wrapped in its own navigation SLOTS
  (demand-driven, free when unused), so the group key is the innermost
  table and the merged scan carries the deduped union of the members'
  slots (`UnionSynthesis.ScanSource`; same-named slots must agree). The
  merged projection carries the UNION-SCAN MARKER `meta::legend::lite::
  unionScan` (identity native, INTERNAL_DESUGAR 13→14): the resolver's
  "is a union" facts (`Pipelines.containsConcatenate`, member-key
  widening, nested-slot demands, the slot walk, milestone pushdown) read
  the node kind where a concatenate no longer exists; lowering is erasure
  (`RelationPredicates.isRelationIdentity`).
- **THE SHARED TABLE KEY**: routes into members of ONE table keyed on its
  sole PRIMARY KEY emit `src = t.<key>__pk_<table> AND (t.<key>_<k> IS
  NOT NULL OR …)` — one indexable equality on the ungated shared key
  (projected once by every thread over that table; the merged scan
  collapses it to the plain column) AND the members' gated keys carrying
  membership exactly as the per-member OR did (`JoinChainEmission.
  sharedTableKey`; `UnionSynthesis.sharedKeyName`). PROBED AND REJECTED:
  the ungated key ALONE as the join key — two routes with different
  source columns (ResolveUnionTest FirmID/LegacyID) matched the wrong
  member's rows; a primary key names one row, not one member.
- **Same-source members coalesce**: a union's own lift whose members read
  the SAME source column against one target expression contributes ONE
  disjunct, `coalesce(s.col_k1, coalesce(…))` (at most one non-null per
  row; `UnionSynthesis.coalesceReads`) — the inferredType hop's 105
  entries (5 op kinds × 21 datatype kinds) are one probe. Identical
  disjuncts dedupe (`orDistinct`).
- Also: a self-join orientation fix rode in batch 9; `PhysicalTables`
  (schema-aware table lookup) split out of MappingNormalizer.

**Batch 11 (2026-09-02) — THE BOOT LAYER (option 1, user-ratified): the
system metamodel prepared once per process.** `Compiler.bootLayer()`: the
system elements are name-resolved and normalized ONCE, content-addressed
by the hash of their Pure source in a `ContentStore` (Invariant 3 — the
artifact persists across compiles), and entered into every graph's index
exactly like the graph's own elements (`normalizeWithSystem`: the graph's
own elements normalize, then the prepared system elements join the
normalized model; poisons / legacy surfaces / mixed unions / the
[1]-over-nullable census union). Protected as system: a graph element
redefining a system ELEMENT is an error (`SystemMetamodel.
withoutSystemShadows`); a same-signature system FUNCTION still replaces
the engine's own source riding in the corpus. The graph's name
resolution sees the system FQNs as known (`NameResolver.
resolveAlongside`) — a graph function calling `rootClassMappingByClass`
or `view` by imported bare name resolves as before (the corpus's
toPostgresModel / viewToTDS tests witnessed the miss). Model compile
8.0ms → **0.5ms** (2.3ms before group F); G1 57s → **29s** (33s before
group F). Rosters, corpus report, H2 verdicts byte-identical.
Why option 1, not a fall-through boot index: a context's compilers are
built over the graph's element INDEX directly, so a fall-through would
have to live in every lookup door of the index (dozens, id-numbered per
index, on the type-check hot path) plus the integrity pass — for ~0.3ms
per graph compile (indexing 78 prepared elements), which is once per
process outside the test JVM. Per QUERY both options cost nothing.

**Batch 12 (2026-09-02) — ELEMENT REFERENCES BY ID.** The alias rows'
nine name-triple reference columns (main/view/base as db, schema, name)
and the op nodes' four column-reference columns become three + one
element ids (`main_element_id`, `view_element_id`, `base_element_id`,
`col_element_id`; ids are the deterministic `tbl:` / `view:` / `column:`
spellings `RelationalOpRows.tableId/viewId/columnId` the rows already
carry). Every cross-kind join is now ONE primary-key equality
(AliasToTables/AliasToViews/AliasToBaseTable/ViewToAlias/OpToColumn);
relational_elements 29 → 21 columns. Rosters, report, H2 verdicts
byte-identical. `DynaFunction.parameters` via parent_id waits for a
witness.
**Regression fixed on the way (batch 11's):** the boot layer merged the
two layers' `requiredNullableRows` with a key-REPLACING map union; the
census is keyed by bucket ("direct"), so the system layer's 13 direct
witnesses replaced the corpus's 500 and the shrink-only ceiling passed
silently at 46. Now a per-key SET union; the count is 533 again. Lesson:
a shrink-only pin cannot catch a census that lost its input — print the
count in the lane log (it is) and READ it after a change to the merge.

**DESIGN — constructed instances as inline relations, scoped (user
ruling 2026-09-02: "I hate that we opened a write path to something that
should be read-only"; folding types onto the element row REJECTED as a
hack).** Facts: the resolver already computes every row of a constructed
tree (element rows + datatype rows + the type ids linking them); the
tree is CLOSED (its nodes reference only its own nodes and types — a
real column inside it is admitted only where no child row is needed);
`TypedTds` is an inline relation literal lowered to VALUES
(`Lowerer.tdsLiteral`, cells typed by the row schema, NULL cells). Plan:
(a) a class source resolved for a constructed root CARRIES ITS SCOPE
(`ClassSource.scope`, the tree's content id) and `ClassSources.get`
REQUIRES a scope argument (no scope-less overload — 61 lookup sites in
the resolver, each passes its source's scope; compile-enforced, not a
convention): navigation targets, subtype casts, correlated subselects
and graph emission inherit the scope of the source they serve. Dynamic
scoping (resolver state) was REJECTED: a real column's type read in the
same expression would wrongly read the inline rows. (b) Inside
`ClassSources`, under a constructed scope the table-scan leaves of the
element and datatype store tables become `TypedTds` literals of that
tree's rows typed with the leaf's own row type; the memo key includes
the scope. (c) DELETE the side-output seeds (`ConstructedInstances.
seeds`), `ExecEnv.constructedSeeds`, `SystemDatabase.insertConstructed`:
the system database is read-only after the graph's rows. Witnesses: the
four typeInference constructed tests + `MetamodelQueryFunctionsTest.
constructedInstances`; rosters byte-identical. ~3h + sweeps.
**FEASIBILITY HOMEWORK (2026-09-02, verified, not guessed):** (i) a
probe ran an inline VALUES relation with all-NULL columns, kind-gated
CASE reads and a join to a second inline relation on DuckDB AND H2 —
identical correct rows, an all-NULL column compared with a string
filters correctly; (ii) `TypedTds` copies rows null-rejecting — absent
cells must be `PlatformTypes.TDS_NULL_CELL` (`Scalars.tdsCell` maps it
to NULL); (iii) navigation targets have ONE owner, `ClassSources.
getForNav` (6 callers); the general `get` has 55 sites across ten
resolver files, every one with the source `cs` in hand — scope threading
is mechanical and compile-enforced once the scope-less overloads go;
(iv) the constructed root's class source is fetched in flattenSource with
`RoutingContext.contextKey(chainContext)` — the scope rides the Context
and the key; (v) `TypedTableReference.info()` IS the table's row type
(the literal's type); `ClassSource` is a record (copy with the rewritten
pipeline); the memo key already carries the context key; (vi) downstream:
`Pipelines.walk` passes a slot-free leaf (walkOpaque), milestoning never
touches the store, the lowering has the literal case, `routeSystemStore`
stays correct for pure-constructed (no store ref → user session, no
store needed) and mixed queries.

**Batch 13 (2026-09-03) — CONSTRUCTED INSTANCES AS INLINE RELATIONS; the
system database is READ-ONLY after the graph's rows.** Built exactly as
designed above: `ClassSource.scope` (the constructed tree's content id;
null for the graph's stores); `ClassSources.get` / `getForNav` REQUIRE
a scope (the scope-less overloads are gone — 71 sites now pass their
source's `cs.scope()`, a graph root's `context.constructedScope()`, or
`null` with a stated reason for the two binding-shape probes and
ClassSources' own building blocks, whose assembled pipeline is rewritten
ONCE by `ClassSources.scoped`); `NavMaterializer.navTargetMaterialized`
and `NestedUnionKeys.pipeline` carry the scope. Under a constructed scope
the store's table leaves become `TypedTds` literals of the tree's rows
(`inlineStoreLeaves`; absent cells = `TDS_NULL_CELL`), typed with the
leaf's own row type; the memo key includes the scope. The chain rooted at
a constructed instance sets `Context.constructedScope`
(`StoreResolver.collectOpChain`). DELETED: `ConstructedInstances.seeds`,
`ExecEnv.constructedSeeds` / `withConstructedSeeds`, `SystemDatabase.
insertConstructed` + the session's constructed ids. The generated SQL
carries `(VALUES (...)) AS _tdsN(...)` for BOTH the element rows and the
datatype rows of a constructed tree. Witnesses green (7/7 metamodel
query functions, the four corpus typeInference constructed tests);
rosters, report and H2 verdicts byte-identical; census 533; H2 lane 38s.

**DESIGN — THE NORMALIZER'S MAPPING-FACT OWNER (user condition 2026-09-02:
"I would do it if part of the work is to actually clean up the API so
these call sites have a clean owner").** Census (2026-09-03): nine
static helpers, 88 call sites — UnionSynthesis 26, MappingNormalizer 17,
AssociationSynthesis 9, XStorePureEnds 8, JoinChainEmission 5,
SetDispatch 4, ImplicitInheritance 4, M2mRouteGuards 1: `setIdOf` 28
(string-replace spelling per ask), `findSetById` 14 (linear walk over
own sets, then the include closure), `collectMappingClosure` 11,
`unionForClass` 10 (own sets first, then includes in order, first found
wins — recursive, never memoized), `collectIncludedSetIds` 8 (nearer
include wins; store SUBSTITUTION composes through the include chain into a
LOCAL map, then merges in include order), `memberOrdinalOf` 7 (set id OR
extends-lineage match), inheritanceMembers/collectInheritanceMembers 7,
`collectRootClassMappings` 3 (the `*` set, else a class's sole set).
OWNER: `normalizer/MappingFacts` — ONE object per `LegacyMappingDefinition`
INSTANCE (the mapping passes through five rewrites in normalizeMapping —
resolveExtends, ImplicitInheritance.apply, qualifyStoreRefs,
implicitOpsForAssociationEnds, injectMultiHopAssociationPMs — each a new
instance whose facts differ; key by IDENTITY, never by name), memoized on
the `ModelBuilder` for the model's lifetime (`ModelBuilder.mappingFact(md,
kind, derive)`, IdentityHashMap — the derived-fact idiom, no static
sinks). API: `idOf(set)`, `setById(id)` (own then included, own wins),
`includedSets()` (the substitution-composed map), `closure()`,
`unionOf(class)`, `rootSetOf(class)`, `extendsChain(set)`,
`memberOrdinal(memberIds, setId)`, `inheritanceMembers(op)`. Every
site becomes `MappingFacts.of(md, model).x(...)`; the nine statics are
DELETED (not kept as pass-throughs). Verification: byte-identical paired
sweeps (the include-order and first-wins rules are pinned by the
rosters); model compile stays 0.5ms (the system layer is already
prepared once) — the value is one owner for these questions.

**NEXT (user-ratified order 2026-09-02, enumerated):**
(1) DONE (batch 9). (2) DONE (batch 10). (3) DONE (batch 11, option 1). (6) DONE (batch 12). Inline relations DONE (batch 13). NEXT = MappingFacts (design above, ~3h fresh session), then group D. — was: UNION LOWERING for single-table hierarchies: merge
members WITH chains into the one scan (each chain a join on the shared
scan guarded by the member's kind predicate); emit the key UNGATED when it
is the scan table's PK and dedupe identical OR terms → `op_id = id`, an
indexed lookup on H2 (the ten 8–16s typeInference tests); re-arm the
5.5-minute chain budget. (3) BOOT LAYER. (4) NORMALIZER PER-MAPPING INDEX.
(5) CONSTRUCTED INSTANCES AS INLINE VALUES. (6) ELEMENT REFERENCES BY ID:
TableAlias main/base and Column's table as element ids, not name triples;
`DynaFunction.parameters` via the parent_id self-join when a witness
demands. (7) Group D → Q → A. Details of (1)–(5) as first written:
(1) ONE TABLE for the
`RelationalOperationElement` hierarchy — merge tables/columns/views/
table_aliases/relational_ops into `relational_elements` (kind, id PK,
superset columns; the datatype/op-kind idiom), with the plain-`id` key
read for merged members: the H2 lane's ten 9–18s typeInference tests
(UNION ALL extent, unindexable on H2) become indexed lookups. (2) The
BOOT LAYER: system elements normalized + compiled ONCE per process,
keyed by the hash of the system source in the content store (Invariant
3 — it persists across compiles, so it is content-addressed); every
graph's context falls through to it for names it does not define;
extents (`Class.all()`) union boot + graph; boot rows written once. A
user compile then normalizes ONLY its own elements (target: the
pre-group-F 2.3ms and below). (3) NORMALIZER PER-MAPPING INDEX (general,
speeds every corpus mapping): the remaining 5.7ms/compile is facts
re-derived per call — `setIdOf` re-spelled per lookup inside linear
`findSetById` scans, `collectRootClassMappings` rebuilt per
inheritance-member collection, `unionForClass` uncached across its ~10
call sites, `memberOrdinalOf` chain walks. One index object per mapping
normalization (set id -> set, root set per class, extends chain, union
per class) built at entry and read everywhere — a LOCAL with one call's
lifetime, not a cache (nothing to content-address). Verified by
byte-identical paired sweeps. (4) CONSTRUCTED INSTANCES AS INLINE VALUES
(user 2026-09-02): a query's `^DynaFunction(...)` trees today insert
content-addressed rows (once per id) into the graph's system database —
a per-query write into shared state; end state = the query carries its
own constants as an inline relation (VALUES), the system database holds
ONLY facts of the graph and is never written per query (the executor
loses write access to it). (5) Then group D below.

Batch 8 chain (gates9.log): G1 54s, G2 9s, G4 64s, G5 134s, G6 104s,
G7 28s, G9 20s, G8 72s — **8m05s** (was 12m54s).

**BURN TO ZERO — the next session's goal (user, 2026-09-03): compile and
run EVERYTHING from the platform and delete all harness badness.**
STATE at b103582a (all pushed): ratchet **820 fallbacks / 1753 flipped**
(WholeTestFlip, EXACT pins in RelationalCorpusRunner.scoreboard),
exec-passing lane 344, metamodel quarantine 125 rows / 9 walls,
required-over-nullable 533, chain ~5m50s (G1 39s, G4 62s, G5 ~40s, G6
~80s, G7 25s, G9 19s, G8 72s), model compile 0.5ms, the system database
read-only after the graph's rows (batches 8–13 above).
DEFINITION OF DONE: every test the harness still scores in Java runs
through the ONE compile/router path and the database's verdict — the
fallback lane, the quarantine channel and the census pins are DELETED,
the referees stay (H2Verify, TdsCompare, the replay oracle). RESIDUE IS
THE LAST RESORT (user, 2026-09-03): a test may be left behind ONLY when
(a) running it on the platform makes no sense (it asserts a fact about
the Java runtime itself) AND (b) no emulation or validation route exists
— and the search for that route is part of the batch, not an
afterthought. The precedent is the H2 SQL REPLAY ORACLE: an SQL-text
golden the platform will never spell byte-identically is VALIDATED by
executing the golden against the same data and comparing ROWS — the
verdict moved into the database instead of being declared out of scope.
Every other "can't" must first be tried as one of: seed the fact as rows
(metamodel-as-relations), carry it in the query (inline relations),
validate by replay/referee (rows, not text), or emulate the engine
behavior in the dialect. What remains is a NAMED list, one line of
reason each, never a bucket; "engine defect" (null-vs-value) counts
only with the foundation probe's adjudication cited.
THE BURN MAP (fallbacks by census group; homework §1 + the bucket dump —
cut legs BY GROUP, never by wall label; the 64-test "TypedMap (HN
vocabulary)" bucket is heterogeneous):
- D harness vocabulary 43 — `meta::legend::executeLegendQuery` /
  `compileLegendValueSpecification` as the ROUTER's string entry
  (compile-from-string through the one router). FIRST.
- Q plan reads 13 (+ printers ~26) — plan nodes as ROWS (homework §2e):
  name the tests from the bucket dump first.
- A expressionSequence / metaprogramming 70 + E scanRelations 21 + I
  LambdaFunction reads 6 + H InstanceValue trees 4 — EXPRESSION TREES AS
  ROWS (homework §2a: node table per tree kind, `expressionSequence`,
  `parametersValues`, `evaluateAndDeactivate`); the biggest group.
- J misc unported 17, Z other metamodel-typed 18, N unknown metamodel
  types 9, G toPostgresModel newState 10, P routerExtensions 5, O TDG
  Pair typing 4, B/C/M/K small named shapes.
- The non-metamodel buckets (from the dump): text-policy 65 (SQL-text
  verdicts — the charter's rows-are-the-verdict rule; each is a named
  decision), join-condition-reads-a-whole-variable 43, no-scalar-lowering
  36, filter-predicate-isolation 25, parametersValues binding 17,
  execution activities 14, unknown functions ~38, multiplicity 11, array/
  list/struct dialect capabilities ~30, misc.
HARNESS CODE THAT DIES WITH THE BURN (delete as each family flips, never
before): the rest of MetamodelWalk (905 lines), MetamodelSteps, PlanText,
AggAwareActivities, StatementExecutor's walk arms, the fallback lane in
RelationalCorpusRunner, WholeTestFlip's quarantine channel, the census
pins (JavaEvalLedger funnel registers shrink, never grow).
METHOD (unchanged, user-ratified): name the exact fallback tests a batch
targets and the expected flips BEFORE building; build the seeds/bodies or
resolver leg; delete the Java arms they retire; land only when the count
moved; a test that moves to a different wall is named and the family is
pursued until it flips; one gate chain per batch; push after green; pins
move only with their burn and a written justification.
SPEED LOOP (use it): `-Drcorpus.only=<family-substring>` /
`-Drcorpus.test=<test>` (scoped runs exit 1 on the full-run pins —
expected); `LEGEND_LITE_DUMP_SQL=1` prints every executed SQL;
`target/timing-ledger.txt` has bucket totals + the 30 slowest tests;
ALWAYS `cd /Users/neema/legend/legend-lite` in every command (the parent
directory holds a STALE core module); zsh does not word-split `"$R"` —
pass `-D` args as separate words; the H2 lane overwrites core/target
rosters — diff the DuckDB rosters BEFORE running H2; G8 cleans
core/target — save rosters before a chain.
DEFERRED WITH DESIGNS WRITTEN (ride a burn, do not stand alone):
MappingFacts (the normalizer's mapping-fact owner, design above);
the 5.5-minute chain ceiling re-arms when a chain measures ≤330s.

**Batch 14 — GROUP D leg 1, the ROUTER'S STRING ENTRY (2026-09-03):
ratchet 820/1753 → 791/1782 (+29, no losses; chain 5m49s, all green).**
Mechanisms (all platform-side, on the one compile/router path):
(1) `compileLegendValueSpecification($treeString)` folds AT PARSE TIME
when `$treeString` is a let-bound literal-string constant of the same
body (SpecParser keeps a scope stack of string constants; lambda
parameters shadow; QuotedSpecParser.fold resolves `$var` through it);
the `let tree = …->cast(@RootGraphFetchTree<T>)` binding parks as a
deferred let (Typer.deferredLetRhs) and resolves at its graphFetch/
serialize consumer (GraphFetchChecker.unwrapCompiledTree strips the
cast) — 13 testSubTypeGraphFetch flips. (2) `execute()` whose query is
`if(<literal>, |{|q1}, |{|q2})` selects the branch STRUCTURALLY
(ExecuteChainAssembly.peelSelections, the literal read through the let
prefix — the Impl(checked, expected) helper). (3)
`meta::legend::executeLegendQuery` (devUtils.pure:30/:35, both
signatures registered VERBATIM) is a RESULT FRAME beside
router::execute (PlatformTypes.EXECUTE_LEGEND_QUERY, HANDLE):
ExecuteChainAssembly.prepareLegendQuery binds the query lambda's
parameters from the vars pairs as LEADING LETS coerced by the declared
parameter type (enum name → enum value, date string → date literal —
the engine's JSON-borne variable coercion; lets keep the `$var`
spelling the serialize keys need), the chain rides `chain()` with a
null mapping ref (every branch carries its own from()), and
legendQueryEnvelope emits the engine's result JSON OVER THE CHAIN by
shape: serialize root → `{"builder":{"_type":"json"},"values":…}`
(joinStrings), primitive scalar root → toString (the platform-ops
witnesses assert 'false'); TDS/class/String roots are NAMED walls
(leg 2). A bare inline call splices to the envelope
(ResultEnvelopeSplice). Nil (the []-born value) now conforms to CLASS
formals in the kernel (`executeLegendQuery($f, [], ext)` against
`Pair<String, Any>[*]`). A from() whose runtime is a LET-BOUND
variable collects the let's setup SQL through the alias channel
(FromChecker + TypedFrom.sqlSetupsInRaw — the m2m2r
`getModelChainRuntime` shapes seeded only by neighbour tests before).
XStore milestoning 4, m2m2r milestoned 5, platformOperations 4 flips.
(4) `compileLegendGrammar(<const>)` over a FUNCTIONS-ONLY payload folds
at parse time to the two-faced QuotedGrammarCall (wire = the call,
pipeline = each function as its lambda); the typer types it as the
lambda collection and peelSelections reads `->at(i)->cast(
@FunctionDefinition<…>)` structurally — 3 testGraphFetchMilestoning
flips. Guards moved with receipts: native-catalog golden +2 lines;
string-dispatch count held (CoreFn.of for cast, a named FQN set in the
parser).
GROUP D REMAINDER (named, each with its route): JSON navigation over
the result string — `parseJSON()->cast(@JSONObject).keyValuePairs->
filter(kv|$kv.key.value=='result').value`, `.values`, `->size()`,
toCompactJSONString/toPrettyJSONString, `^JSONArray(values=…->sortBy)`
(runLegendTest 4: slice/take/limit/drop WithVariables; paginate 2;
enumPushDown testPushDownProjectWithParameter; subType
testInheritanceMappingWithoutSubType +
testSubTypeAtRootLevelWithInheritanceMapping) = LEG 2: the meta::json
classes (json.pure:32-70, verbatim) ride the VARIANT lane by emission
(parseJSON = JSON cast; member get = VARIANT_GET; `.values` =
VARIANT_ELEMENTS; `.value` = to(@String/@Number); casts within the
family identity; compact/pretty = the JSON text — DuckDB probe
2026-09-03: `-> '$.*'`, `CAST(… AS JSON[])`, list_transform/flatten,
json_group_array(json_object(…)), json_pretty all verified) plus the
tdsBuilder/classBuilder envelopes for TDS/class-rooted queries
(json_object('columns',…,'rows', json_group_array(json_object('values',
json_array(cols))))). testParametrizedEnumFilter — the from() runtime
is a `^$runtime(connectionStores=^$connectionStore(connection=
^$connection(testDataSetupCsv=…)))` COPY chain over a navigated
connection store: the CSV seed route (Ddl.setUpDataSqlsText over the
store's tables) through the alias channel, next. testSpecialUnion_m2m2r
— `class Person is not mapped in mapping FirmsAndEmployees_M2M`: an
M2M union-root mapping resolution gap (family: graphFetch union
rootLevel), not the string entry. XStore
testCrossStoreGraphFetchWithRelationalDatePropagationForMilestonedProperty
Constraint / …ZeroToOne — a MODEL in a string (classes, mappings,
connection, runtime + functions): the route is the compile-once
overlay admitting grammar payloads (the carrier refuses non-function
payloads loudly); a separate leg. Adjacent (use compileLegendGrammar,
other walls): testMilestonedProperty,
testMilestonedRootAndMilestonedProperty (graphFetch milestoning),
testFlatten_ViaNoArgMapping(_ViaAssociation) (from() mapping argument
is a let-bound helper CALL — `getNoArgMapping()` builds ^Mapping).
Harness arms still standing (they serve the named remainder): ElqSplice,
clgArm, the walk's QuotedSpecParser.fold site — each dies with its
last fallback.

**Batch 15 — GROUP D leg 2, the meta::json TREE on the variant lane
(2026-09-03): ratchet 791/1782 → 782/1791 (+9, no losses; chain 5m56s,
all green).** The `meta::json` classes (real json.pure:32-70, verbatim
— JSONBoolean/String/Number/Null/Array/Object + JSONKeyValue) are
registered natively and their VALUES ride the variant lane
(`PlatformTypes.isVariant` covers the family): a JSON element IS the
database's JSON value; the classes are its kinds. Reads type BY
EMISSION in `JsonChecker` onto two HIR nodes — `TypedJsonAccess`
(MEMBER = `keyValuePairs->filter(kv|$kv.key.value == key)` and
`getValue(key)`, MEMBERS = unfiltered `keyValuePairs` (`-> '$.*'` as a
list), ELEMENTS = `JSONArray.values`, TEXT/NUMBER/BOOLEAN = the scalar
kinds' `.value` through the `'$'` extraction, IDENTITY =
`JSONKeyValue.value`; a many receiver auto-maps and the list map
FLATTENS the MEMBERS/ELEMENTS mappers) and `TypedJsonResult` (the
string entry's tdsBuilder / classBuilder RESULT envelope over a
TDS/class-rooted chain — `{"builder":…,"activities":[{"_type":
"relational","sql":<engine-style render of the chain>}],"result":
{"columns":[…],"rows":[{"values":[…]}…]}}`, one scalar subquery; the
class kind wraps the graph emission's objects). `parseJSON` = the JSON
cast (with the engine parser's one tolerance the assert reader already
mirrors — `}{` reads as `},{`), `toCompactJSONString` = the JSON text,
`toPrettyJSONString` = `json_pretty` (Spellings row; typed VARCHAR),
casts within the family are identity, `^JSONArray(values=…)` emits
`toVariant(values)->cast(@JSONArray)`. New SQL node `SqlExpr.JsonArray`
(`json_array(…)` / H2 `JSON_ARRAY`), owner `lowering/JsonEmission`
(+ `JsonLane` for the rules, incl. fromJson — the variant lane's one
owner). Executor: a helper whose body is a STATEMENT SEQUENCE (non-let
intermediates, through thin forwarding overloads) runs as one
(`executeCallStatement`) — AFTER the assert root arms; an inlined
string-entry call re-offers to the frame splice after argument
substitution (its query argument is a helper parameter until then);
α-renamed query parameters (`_i<n>`) bind by POSITION; an inline frame
runs the runtime's setup SQL before its read; a helper that β-reduced to
an assert over a string-entry read is adjudicated post-inline (scoped —
adjudicating every inlined assert regressed ~200 text-golden flips).
List `sortBy` now zips element indices (`list_zip`) instead of indexing
the source inside lambdas (DuckDB refuses subqueries in lambdas; the
(k,i,v) struct sort keeps the stable tie-break; positional
`struct_extract(z, 1)` for list_zip's unnamed structs). Flips: slice/
take/limit/drop WithVariables 4, paginate 2 (the activity SQL text
matched the engine golden byte-for-byte), enumPushDown 1,
testSubTypeGraphFetch 2 (all 15 of that file now flip). Guards moved
with receipts: native-class count 236 → 243, JavaEvalLedger
StatementExecutor 2594 → 2680 (orchestration only), text-only 40 → 35;
Lowerer/Scalars/StoreResolver held under 3500 by owner extraction.
GROUP D REMAINDER (named): testParametrizedEnumFilter (CSV runtime COPY
chain over a navigated connection store — the CSV seed route through the
alias channel), testSpecialUnion_m2m2r (M2M union-root mapping
resolution), XStore …DatePropagationForMilestonedPropertyConstraint /
…ZeroToOne (a MODEL in a string — the compile-once overlay leg).
Adjacent: testFlatten_ViaNoArgMapping(_ViaAssociation) (from() mapping
argument is a let-bound helper CALL building ^Mapping),
testMilestonedProperty (plan-text golden), testMilestonedRootAndMilestonedProperty
("trailing JSON" — a JSON text shape).

**Batch 16 — GROUP D remainder, let-bound runtimes and CSV seeds
(2026-09-03): ratchet 782/1791 → 780/1793 (+2, no losses; chain 5m56s,
all green).** A from() whose runtime argument is a LET-BOUND variable
(the string-entry query shapes: `let runtime = ^EngineRuntime(…)` /
`getModelChainRuntime($m)` / a copy with inline test data) now TYPES
the let's rhs through the alias channel and the same collectors read it
— chain mappings (the ModelChainConnection an M2M union root resolves
through), JSON sources, setup SQL — plus the CSV half: `testDataSetupCsv`
on a LocalH2 specification / a TestDatabaseConnection is recorded on the
node as a FACT (`TypedFrom.csvSetups`: block text + the enclosing
connection store's `element`; a COPIED connection's store found by
structural navigation of the copy's source — `$runtime.connectionStores
->at(0).connection->cast(…)` through lets and zero-arg helpers) and the
EXECUTOR turns it into seed SQL (CsvSeed) when it establishes the
connection, exactly like the SQL setups (the compiler never reaches into
exec — invariant 6e caught the first cut). +2 = testSpecialUnion_m2m2r,
testParametrizedEnumFilter. Left in group D (named): XStore
…DatePropagationForMilestonedPropertyConstraint / …ZeroToOne — a MODEL in
a string (the compile-once overlay leg). Known limit: a copied
connection whose source navigates a one-ARGUMENT helper
(`testRuntime()` → `testRuntime(db)`) resolves no store — the CSV then
seeds without DDL (the family table already exists; loud otherwise).

**Batch 17 — GROUP Q opener (2026-09-03): ratchet 780/1793 → 778/1795
(+2; chain 5m56s, all green).** `meta::pure::executionPlan::executionPlan`
is registered VERBATIM (`f:FunctionDefinition<Any>[1]`,
executionPlan_generation.pure:25-50; the six per-arity
`Function<{Any[1]->Any[*]}>` overloads were an invention that rejected
`bd:Date[1]` / `Integer[0..1]` query parameters by contravariance —
deleted). +2 = testDefaultOptionalParamIsNullSafe,
testFilterInWithResultSorcedFromAnExpression. The other ten group Q
tests now type and reach ONE wall: "class query under TypedMap (HN
vocabulary)" on the plan-node navigation
`$result.rootExecutionNode.executionNodes->filter(n|$n->instanceOf(
RelationalInstantiationExecutionNode))->at(0).executionNodes->at(0)
->cast(@SQLExecutionNode).sqlQuery` read INSIDE an assert side
(assertEqualsH2Compatible): the statement executor's `planWalk` (a Java
plan-node evaluator — harness badness, dies with the burn) answers such
chains only at a statement ROOT; the honest route is PLAN NODES AS ROWS
(homework §2e): the plan handle's nodes seeded as rows of the system
database (kind, parent, ordinal, sql text, result columns), the reads
lowered as ordinary relation navigation, `sqlQuery` judged by the SQL-
text referee (replay). testLegacyFlagRestoresOptionalParamFreeMarkerSelector
is a plan-TEXT spelling (the legacy `optionalVarPlaceHolderOperationSelector`
freemarker form) — named. testMultiExpressionWithPlatformAndFromFunction:
"PureExp source printing for TypedMap pending" (plan-text of a map
expression) — named.

**Batch 18 — GROUP Q: PLAN NODES AS ROWS (2026-09-03): ratchet 778/1795
→ 729/1844 (+49, ZERO lost; chain 5m48s, all green).** The executor's
plan model (PlanNode — the lowering's product) rides the query as inline
rows of the system store's `plans` / `plan_nodes` / `plan_template_
functions` / `plan_function_parameters` / `plan_node_closure` tables
(`plan.PlanRows`, keyed by the handle's call-site id; `PlanAllocations.
registerPlanRows` registers them under the let binding; the graph-
lifetime store seeds NONE of them — MetamodelSeeds). The plan reads are
ordinary navigation over those rows, resolved by the ONE resolver:
- member-union hops COMPOSED: each hop's subtype witnesses register
  under the hop's own prefix (`registerSubTypeSubs(..., hopPrefix)`,
  belowScope registers per op without the sources registry), the
  top-level table under the composed prefix with a lenient anywhere
  fallback; a union-threaded key on a composed row reads as the coalesce
  over its member threads (`FlattenOps.coalesceThreadedReads`, applied by
  flattenSource / registerAssociationJoins / NavProvenance.spliceOwnStep);
  composed sources CARRY the constructed scope (three ClassSource sites +
  spliceOwnStep) so every hop's target reads the inline rows.
- `chain->cast(@Sub)` BELOW a flatten hop = a PSEUDO-HOP
  (`ChainDispatch.pseudoHop` → `CastReRoot.reRoot`): the gate filter
  (raise on a non-member) runs in the segment below, then the chain
  re-roots at the subtype's own extent joined on the shared primary key
  (`<prefix><pk>__pk_<table>` thread merge / plain column); the hop above
  (`.functionParameters`) is the subtype's own route (spliceOwnStep).
- `allNodes(node, ext)` is a Pure BODY over the closure rows
  (`$node.subtree.node`; associations PlanNodeSubtrees /
  PlanNodeClosureNodes on the lite class PlanNodeClosure) — the native
  signature and `MetamodelSteps`' Java arm DELETED.
- the plan-rows registration resolves under the runtime's chain mappings
  (planModel now passes them — the testModelConnection* M2M plans).
- `UserCallInliner` keeps binder NAMES unless an argument of the frame
  mentions the name (capture is the only hazard; the plan surface prints
  binders: `functionParameters = [optionalID:String[0..1]]`); `pair(a,b)
  .first/.second` folds to the component (the datetime helpers' pairs);
  `ExecuteChainAssembly.letBound` chases a let bound to another variable
  DOWN the prefix (a call frame's `let func = $func`).
- upgraded-H2 plan spellings (the assertEqualsH2Compatible pairs' UPGRADED
  golden is the oracle's): DATE placeholders `TIMESTAMP'${x}'`, optional-
  parameter equality null-safe for every kind (the DATE/DATETIME selector
  forms were the LEGACY halves), lowercase `dateadd(day, …)` on the
  milestoning adjust channel, `PureExp` `$names -> map([Routed Func:n:
  String[1] | $n -> toUpper();])`, FreeMarkerConditional block indent,
  RelationalBlockExecutionNode's `) ` closer, `tempTableColumns … )]`,
  renderCollection's `{"'" : "''" }`, dotted Integer placeholders bare
  (`${endDateCalendar.fiscalYear.value}` — `PlanParams.dottedPlanParam`).
- guardrail moves: `Callees`, `CastReRoot`, `ClassSorts` extracted from
  StoreResolver; plan-row registration in PlanAllocations; walk text-only
  asserts 35 → 27 (charter §8.0 row); metamodel quarantine rows 125 → 77
  (the plan-read refusal spellings are dead; walls 9); required-over-
  nullable ceiling 533 → 534 (sqlQuery over the single-table plan_nodes).
NAMED residue in executionPlan/tests after this batch (all walls or
text diffs, none silent): testTemporalDateVariableInFunctionExpression
WithPropagation (our milestoned derived-property join SHAPE nests the
exchange navigation — the engine flattens it; rows underivable: the
milestoning tables are never seeded in the executionPlan package, so
text is the contract — 4 tests count as our-rows-underivable in the
text-verdict census); testDatabaseConnectionSQLPopulation ×2 (the
SQLExecutionNode `connection` → datasource `testDataSetupSqls` rows —
next plan-row table); testGroupByWithOpenVariableInAgg ×2 (join ORDER +
`cast(0.0 as float)` literal), testMapWithOpenVariable /
testTwoMappingsOneRuntime ×2 (aggregation / union SQL shapes),
testLegacyFlag* ×2 (tests/query: the LEGACY_SQL_NULL_UNSAFE_EQUALS
feature flag must reach the plan dialect — withFeatureFlags is identity
today), the datetime `testPlanWithLocalH2ConnectionWithSQL`
(transformPlan protocol), testSupportStreamFlagWithGraphFetchAndFrom
(deferred graph-tree let), the model-connection agg/join/deep trio
(M2M shapes), withPlatform (STRING_AGG list encoding).

**Batch 19 — GROUP A: FUNCTION BODIES AS ROWS (2026-09-03): ratchet
729/1844 → 686/1887 (+43, ZERO lost; chain 5m49s, all green).** The 43
pkInferenceTests all read ONE helper: `$func.expressionSequence
->evaluateAndDeactivate()->at(0)` then `inferPrimaryKeyColumnNames($expr)`.
`FunctionDefinition.expressionSequence : ValueSpecification[1..*]` is
registered VERBATIM (real m3). A function reference eta-expands to a
lambda (existing Typer rule); the resolver meets `<lambda>.expressionSequence`
as a ROW ROOT (Anchors.functionBodyRead → ElementReferences.rowRoot): the
lambda's statements are `value_specifications` rows under the lambda's
content-id scope (`FunctionBodyRows`, registered on first meeting, riding
the query), each stamped with the compiler's inferred primary key
(`PkInference` — the engine's inferPrimaryKeyColumnNames RULES over the
typed tree: table accessor = declared pk; row-preserving ops keep; select
keeps iff it projects every key; rename maps; groupBy / distinct(cols)
key on their columns; INNER/LEFT join and asOf join union both sides;
aggregate / pivot / concatenate / other joins none). The read
`inferPrimaryKeyColumnNames(vs)` is a Pure body over the lite association
`InferredPrimaryKeys` (`$vs.inferredPrimaryKeyColumns->sortBy(ordinal).name`).
`evaluateAndDeactivate` is the IDENTITY over rows (resolveNode + the
object spine). The four row-root arms of collectOpChain (element ref, plan
handle, function value, constructed instance) now live in
`ElementReferences.rowRoot`. Quarantine rows 77 → 34. Named residue: none
in the family (43/43). Design debts named in the session report: the
analysis is Java-stamped (PkInference / PlanRows) and read by the
database; two parallel plan builders (planModel vs planToString);
content-id scopes; the lenient anywhere fallback in subtype registration.

**Batch 20 — GROUP E: LINEAGE TREES AS ROWS (2026-09-03): ratchet
686/1887 → 661/1912 (+25, ZERO lost; chain 5m42s, all green).** `scanRelations(f, m[, r], ext)`
is a PLATFORM HANDLE native (real scanRelations.pure:74/:341; the class
`RelationTree` registered, its engine properties are the lite node rows);
the engine's `scanRelations.pure` shipped beside its tests is SPEC and no
longer joins the family model (`RelationalCorpusRunner.ENGINE_
IMPLEMENTATION_FILES`). On the handle's let the executor registers the
tree's rows (`PlanAllocations.registerLineageRows` → `lineage.LineageRows`:
the lineage scan's printed lines as DATA — preorder, indent, kind
root/t/v, name, join label, sorted columns — the scan walks the raw query
lambda found by the let's name in the query's protocol body, now carried
on `ExecEnv.protocolBody`; `ScanRelations.lines` is the one walk both the
Java printer and the rows use). `relationTreeAsString(t[, withJoin])` is a
Pure body over the rows (`$t.nodes->sortBy(preorder)->map(...)->joinStrings`)
— the DATABASE prints the tree. Handles generalize: `PlatformTypes.
handleRowClass(fqn)` names the metaclass a handle's rows extend as
(ExecutionPlan / RelationTree); a `->toOne()` over a handle is the handle.
Residue, NAMED: 19 runtime-variant trees whose join labels carry the
engine's internal alias breadcrumbs (`Car_dy1c_PersonID`,
`AltID_View_d#5_d#2_m1entityID`, `Owner_f_d_rVEHICLE_ID`) — the Java arm
`LineageRelationsForm` stripped them from BOTH sides (a harness
compensation); the platform will not mint engine-internal alias names, so
those goldens stay Java-scored until the labels are adjudicated (engine-
internal spelling, not a lineage fact). 3 walls: `concatenate` of TDS
relations with differing columns (typer, ×2), scalar lowering of a
TypedPropertyAccess under a cross join (×1). Named resolver debt: an
aggregated to-many hop's join key resolves by COLUMN NAME across the
composed row (a node key spelled `id` collided with the tree's `id`) —
the store spells it `node_id`.

**Batch 21 — GROUP I: COLUMN LINEAGE AS ROWS (2026-09-03): ratchet
661/1912 → 656/1917 (+5, ZERO lost).** `scanColumns(tree, mapping)` is
a PLATFORM HANDLE native (real scanColumns.pure:30; `scanProperties`
:136 and `buildPropertyTree` :753 are the natives that feed it, their
classes `PropertyPathNode` / `Res` / `PropertyPathTree` registered); the
handle's rows are `column_contexts` (`lineage.ColumnLineageRows`: the
scan's (table, column, context) entries — `ScanColumns.entries`, the one
walk the Java arm and the rows share — each resolved to its owning
database/schema through the mapping's databases, includes-closed, all
databases as the fallback, loud on 0 or >1 owners). The read is pure
NAVIGATION: `ColumnWithContext.column` joins `relational_elements`
(`ColumnContextToColumn`), `Column.owner` is a self-join to the owning
table (`ColumnToOwnerTable`, typed `Table[0..1]` — real m3 says
`Relation[0..1]`; a nested union hop under map is not materialized yet,
a named debt). Two resolver legs, both real-pure semantics: `cast(@T)`
over a value whose static class already conforms to T is the IDENTITY
(`CastChecker`) so `$c.owner->cast(@Table).name` keeps its property-path
shape; instance `removeDuplicates` replayed over a materialized row
keeps the TO-ONE navigation slots' columns in its DISTINCT tuple (a
to-one slot is a function of the row — dedup-neutral; to-many exists
materials stay out — the two-exists witness `testAssociationToMany
WithTwoSeparateExists` guards it; `StoreResolver.instanceDistinct`).
The lowering's unresolvable-ref failure now names the columns it had.
Residue, NAMED: `testNonDataTypeProperty` — a CLASS-valued project
column (`p|$p.address`) walls in the inner lowering ("class query under
TypedMap"), the same 34-test bucket; the Java arm `LineageForm`'s
scanColumns branch serves that one test and dies with the bucket.

**Batch 22 — GROUP H: THE EXPRESSION TREE AS ROWS (2026-09-03): ratchet
656/1917 → 653/1920 (+3, ZERO lost).** Every node of a function body is
now a `value_specifications` row (`FunctionBodyRows.nodeRows`, preorder:
id, function id, ordinal, m3 kind, parent, depth, multiplicity bounds,
variable name); the kinds are the real m3 subclasses (`FunctionExpression`
/ `InstanceValue` / `VariableExpression` — Pure.java declares them with
their real properties, m3.pure bootstrap :1955; `func` is not modeled
yet: a function reference is not a row) as an Operation set over the one
table (`SystemMetamodel.VS_KINDS`, the plan-node idiom); `parametersValues`
are the children rows (`VsToChildren`); `expressionSequence` is the
depth-0 rows (the `FunctionToBody` join carries `depth = 0`). The node's
`multiplicity` is the REAL m3 object shape — `Multiplicity.lowerBound /
upperBound : MultiplicityValue.value` — mapped over the same row (`VsSelf`
self-joins; an unbounded upper bound is NULL); `getLowerBound` is the
real body verbatim (getLowerBound.pure:17) and the engine's
`expressionSequenceReturnsAtLeastToOneDataType` is a Pure body over it
(`$v.multiplicity->getLowerBound() >= 1` — the engine's
findFunctionSequenceMultiplicity fold and the typer's static multiplicity
agree on every witness). Two reflection folds at TYPING (real-pure
identities): `evaluateAndDeactivate` over a lambda literal is the literal
(`NormalizeFolds.foldReflection`, wired at `Typer.emitCall` — the generic
`<T|m>` signature used to strip the function carrier and lose
`.expressionSequence`), and `{..}->deactivate()->cast(@InstanceValue)
.values->at(0)->cast(@LambdaFunction<..>)` is the lambda
(`CastChecker.deactivatedLambda`). The Java arm `ReflectAsserts` (the
host multiplicity walk) is DELETED; the metamodel quarantine shrank 34 → 22
rows (the m3 classes type chains that walled as unknown types). Residue, NAMED — engine-GENERATOR
internal API with no platform counterpart (their bodies are the engine's
pureToSQLQuery.pure, never loaded): testFindFunctionSequenceMultiplicity
(`findFunctionSequenceMultiplicity` pairs + `.func`), testMergeOldAliasTo
NewAlias, testReAliasMergedJoinOperations, testFindAliasMappingBySchema
Name, addDriverTablePkForProject, testImportDataFlow (routeFunction /
toSQLQuery over RelationalExecutionContext); simpleFunctionExpression
TranslationNow/Adjust read `toSQLQuery(fe)->sqlQueryToString(H2)` — a
plan-text handle leg (the plan rows already hold the SQL text), not
built.

**Batch 23 — CONSOLIDATION (2026-09-03, after the user's design question
"is this all metamodel as data?"): ratchet unchanged 653/1920 (0 lost).**
Answer given: the READS are metamodel-as-data (rows, navigation, Pure
bodies, Operation sets, real m3 shapes); the FACTS are still Java-stamped
(PkInference rules, ScanRelations/ScanColumns walks over the lowered SQL,
the AggAwareActivities printer) — the harness smell moved into main, a
named debt with a port order (below). Three consolidations landed: (1) the
per-FQN `handleRowClass` table is GONE — a handle's row class is the
native's DECLARED return class when the registry labels it HANDLE
(`PlatformTypes.handleRowClass(fqn, returnType)`; executionPlan →
ExecutionPlan, scanRelations → RelationTree, scanColumns →
ColumnWithContext; execute's generic Result and preval's function value
yield none); (2) the let-time registration no longer sniffs shapes
(`->toOne()` / `->removeDuplicates()` unwrapping): every HANDLE call
anywhere in a let's binding registers (`PlanAllocations.registerHandlesIn`);
(3) the six identical StoreResolver constructions are one factory
(`StatementExecutor.resolver`). NOT consolidated, and why: registration
still happens at the let rather than on demand in the resolver, because
`ScanRelations.lines` walks the PROTOCOL lambda (found by the let's name
in the protocol body) — the lineage scans must first become Pure over
the expression rows + the mapping rows before the resolver can register
lineage on first meeting (function bodies already do). PkInference → Pure
over the expression rows needs bottom-up recursion over the tree (a
recursive CTE or a closure table, like plan_node_closure) — its own leg.

**Batch 24 — EXECUTION ACTIVITIES AS ROWS (2026-09-03): ratchet
653/1920 → 581/1992 (+72, ZERO lost).** An `execute()` call's Result is
a row under the call's scope (`results`) and its activities are kind
rows over one `activities` table (`SystemMetamodel.ACTIVITY_KINDS`: a
RelationalActivity carrying the SQL the platform ran — its own render,
the same pipeline as toSQLString; comment NULL — no trace id is
invented; AggregationAwareActivity rows exist as a kind but none is
recorded until the router rewrites). Registration happens where the
frame is built (`StatementExecutor.buildFrame` → `PlanAllocations.
registerActivityRows`, `activitySql` chasing let-bound query/mapping
arguments), so let-bound AND inlined user-call frames (`executeInternal`)
both carry rows; `execute` is a HANDLE whose declared `Result<T|m>` names
the row class (`handleRowClass` accepts a generic result's raw class)
and a handle call with rows roots a navigation whatever its declared
type (the chain-loop hop rule). `$r.activities` re-roots at the call
(`ResultEnvelopeSplice.activitiesRowsRead` — the inline form stands AS
WRITTEN, the same instance: the inliner's fixpoint; a fresh node each
visit looped the inliner into a stack overflow, the day's one real bug).
The referee's `sql()`/`sqlRemoveFormatting` arms STAY and now also serve
inline execute frames (a text-divergent render still row-verifies through
the oracle — deleting them lost 10 flips in the probe); the Java printer
`AggAwareActivities` and its `rewrittenQuery` fold were deleted and then
RESTORED: the corpus-regression gate caught the NOP family dropping
15 → 10 passes (those five tests had passed through the fold as
harness-scored tests). The fold stands, named, until batch 25 records
the aggregation-aware rewrite as routed-tree rows. Pins moved as lane
moves: M1 verified floor 82 → 54, M1 rescued floor 204 → 164,
exec-passing declines 344 → 275 (the flipped tests' sql-asserts left the
walk's lane; receipt: corpus passes 2355 → 2367, clean 2151 → 2201,
text-rescued 165 → 127, oracle disagreements 0). Honest fallback:
testSQLComments (engine trace-id comment). NOT a bug after all: the NavMaterializer "recursion"
was the inliner loop re-rendering the same query; a depth guard was
tried and REMOVED.

**Batch 25 — AGGREGATION-AWARE ROUTING DONE RIGHT (2026-09-03, the user's
challenge "why not fixing agg aware correctly?"): ratchet unchanged
581/1992 (0 lost; the five nonGroupBy rewrittenQuery reads now flip
through ROWS, not the Java fold).** The AggregationAware element is ONE
node on the mapping AST (no sidecar — the user's catch): the flattened
main Relational carries an `aggregation` component
(`ClassMapping.AggregationAware` → `AggregateView`s: index, canAggregate,
the group-by and map/aggregate specification lambdas as SYNTAX facts, and
the view's own Relational set, non-root, id `<outer>_Aggregate_<i>` — the
engine's spelling). Every rebuild site re-spells the component (16
constructions), the name resolver and the store-substitution pre-pass
recurse into the views (both bugs found by probing: an unqualified join
db, an unqualified class name). At normalization each view compiles as a
SET like any set (`AggregateViewLift`: lifted function + non-root
binding) and the main binding carries the views' facts
(`ClassBinding.AggregateViewFacts`); class-level lookup skips view ids
(`.all()` is the main set). The DECISION is the router
(`resolver.AggregationAwareRouting`, the engine's aggregationAware.pure
rules): a groupBy over a filtered getAll canonicalizes its group keys,
aggregate map/reduce lambdas and filter predicates to project paths
(`generateProjectPath`: property steps over the root, no-op functions
elided, automap folded, other functions by FQN); the specification
lambdas type once with `$this` bound to the class
(`SpecCompiler.typeExpression(expr, bindings)`); `canRewrite` = path in
the view's group-by paths or every sub-expression rewritable; the first
view whose group-by and aggregate matches hold (canAggregate=false
demanding mutual coverage) wins and `StoreResolver.resolveObject` builds
the class source from THAT set id. Receipt: testRewriteSwitchToProdLine
YearTable's SQL now reads FROM user_view_multi_agg.SalesTable_ProdLine_Year
(the golden's table). The activity row records the same choice
(`registerActivityRows` adds the AggregationAwareActivity first, the
engine's order; the routed print names the chosen set) and the
`rewrittenQuery` Java FOLD is deleted for good (`AggAwareActivities`
remains the routed-query PRINTER — the platform's spelling of the routed
tree; routed-tree rows + a Pure printer is the later leg). The
NavMaterializer depth guard tried in batch 24 was a misdiagnosis and is
gone: the "recursion" was the inliner loop.

**Batch 26 — THE REFEREE'S RENDER IS THE FRAME'S CHAIN (milestoning leg,
2026-09-03): ratchet 581/1992 → 505/2068 (+76, ZERO lost).** The census
had ~90 milestoning fallbacks under "join condition reads a whole
variable" / "class query under TypedMap". Root cause, found by probing:
the activity SQL (the text the referee's `sql()` arms fold) was rendered
by a SECOND pipeline — `engineSql` re-inlining the RAW query lambda —
which had neither the caller's body lets (`let businessDate = %2015-8-15`
used inside the lambda as a milestoning date) nor the query lets, so the
render failed (silently), the `sql()` read fell through to the activity
rows, and the resolver's automap of the final `.sql` read walled as "class
query under TypedMap" — the wall text the census showed was two
mechanisms away from the cause. Now `PlanAllocations.activitySql`
renders the frame's OWN assembled chain (the caller's lets β-folded, the
mapping attached — the same chain the frame runs; `StatementExecutor.
engineSql` also builds its resolver through the one factory, so the query
lets and registered rows ride the render). One pipeline, not two. +76:
milestoning businessdate 32, contextpropagation 18, processingDate 3;
in-list filters 6; TDS concatenation 4; routing/tds 4; others. Lane pins
moved as lane moves (M1 verified 54 → 22, M1 rescued 164 → 128,
exec-passing declines 275 → 198). Remaining milestoning walls are named
and small: `repeat` unported ×2, a nested-navigation milestoned property
×1, a multi-statement lambda ×1, `^SemiStructuredPropertyAccess` ×1, an
executeInDb binding read ×1, toVariant on the H2 dialect ×2. Temporary
diagnostics added and removed on the way: the join-condition wall now
names the variable it read.

## 6. HOST-SIDE EVALUATION REGISTER AND RETIREMENT PLAN (2026-09-03, user ask)

What the platform still evaluates in Java, by kind, and what to do about
each. The verdict path tenet: Java orchestrates, the database executes;
the metamodel is rows. "Stay" means the Java is orchestration or a
compile-time identity; "retire" means a Pure body or rows replace it.

**A. Compile-time folds in the typer — STAY (audit once against real
.pure).** Special forms the type checker resolves as identities, never a
verdict value: `deactivate` (the reflection carrier, TypedDeactivate);
`evaluateAndDeactivate` over a lambda literal (NormalizeFolds.
foldReflection); the `deactivate()->cast(@InstanceValue).values->at(0)
->cast(@LambdaFunction<..>)` round trip (CastChecker.deactivatedLambda);
`cast(@T)` over a value already of class T and `cast(@TabularDataSet)`
over a relation (CastChecker); `size` of a fixed-multiplicity value,
literal `eq`/`and`/`or`, `if` with a literal condition inside INLINED
platform bodies (NormalizeFolds.fold/foldInlined); the group D folds of
`compileLegendGrammar` / `compileValueSpecification` over let-bound
constant strings; `getRelationalCsvData` census and `generateTestData`
carrier (CsvCensusChecker / GenerateTestDataChecker). Owed: one audit
pass listing each fold beside the real .pure line that makes it an
identity.

**B. Effects and orchestration — STAY.** `executeInDb`, the DDL
natives, `setUpDataSQLs`, `print`/`println`, `connectionByElement`
(NativeImpl.EFFECT); CrossStoreGuard; ConnectionFlags (runtime-argument
readers); ConnectionLets (effectful-let chain analysis); MetamodelSeeds
(compile-time facts rendered as rows — the seed IS the store).

**C. Java-STAMPED FACTS behind rows — RETIRE, in this order.** The reads
are rows; the facts are Java walks:
1. Routed-tree ROWS + ONE Pure printer. Retires `AggAwareActivities`
   (the routed-query print) AND `PlanText` / `planToString` /
   `planToStringWithoutFormatting` (NativeImpl.JAVA_ROUTINE) — both are
   engine-spelled prints of a tree the compiler holds. The rows are the
   expression-tree rows (batch 22) after routing (set holders on getAll,
   automap hops explicit), printed by a Pure body in preorder-fragment
   form (the relationTreeAsString idiom: per-node open/close fragments
   joined in preorder — no recursion in SQL). FLIPS TESTS: group J
   (`->cast(@StoreMappingRoutedValueSpecification).sets.class.name`,
   `ClusteredValueSpecification.val`, ~16) and the plan-text residue.
   Part of the burn.
2. The RECURSION mechanism (bottom-up over tree rows): a closure table
   like plan_node_closure, or a recursive CTE in the lowering. Needed by
   PkInference→Pure; build it when the first test needs it (the routed
   print's fragment table may already be it).
3. PkInference → Pure over the expression rows (the engine's
   inferPrimaryKeyColumnNames rules, relationFunctionMapping.pure:94-
   170: property/operator arms bottom-up). Retires `resolver.PkInference`.
   Flips nothing (group A already flipped) — pure debt.
4. Lineage scans → Pure over expression rows JOIN mapping rows
   (property_mappings, joins, relational_elements): the engine's
   scanRelations/scanColumns walk the routed query + the mapping
   metamodel, not the lowered SQL. Retires `lineage.ScanRelations` /
   `ScanColumns`. Flips nothing (groups E/I flipped) — the largest port;
   may also close the 19 alias-breadcrumb residue if the labels derive
   from mapping rows the same way the engine's do.
5. `MetamodelSteps` (the recv-dispatched metamodel-walk vocabulary shared
   by planWalk and a map-lambda arm): the last "metamodel walked in
   Java" surface. Check which reads still route through it versus the
   system store; retire the ones the store answers. Part of the
   executionPlan residue.

**D. Assert-family arms in the verdict path — AUDIT, retire the value
derivations.** `AssertVerdicts` (~2000 lines; the assert family as
verdicts is BY DESIGN per the charter — audit for any arm that still
DERIVES a value instead of comparing two database results);
`ConnEquality` (harness) + `runRelationalRouterExtensionConnectionEquality`
— a metamodel comparison that belongs on connection ROWS (group P, the
testConnectionEquality* tests) — part of the burn; `LiteralFold` (bare
literal roots compile to their value — the engine's
ConstantExecutionNode; stays, it is a plan shape, not a verdict);
`AssertErrorNative` (the assertError K-arm; stays — reference contract
interpreted). The referee's `sql()`/`sqlRemoveFormatting` folds in
ResultEnvelopeSplice STAY (they feed the SQL-text referee; deleting them
lost flips in batch 24's probe). Harness arms that die with their
families: LineageForm, LineageRelationsForm, PlanAsserts, TestDataGenForm,
RuntimeIfForm, AssertLoopForm.

**ORDERING (recommendation).** Interleave by whether the item flips
tests: C1 (routed-tree rows + printer: group J + plan text) and D's
connection equality as rows (group P) are burn legs and go in the burn
order after the next census theme; C2 rides C1 (build the mechanism when
the first test needs it). C3, C4, C5 and the A audit flip nothing and go
AFTER zero — except that C3 should follow C2 immediately while the
mechanism is fresh. Do not start C4 before zero: it is the largest port
and the lineage families are already scored by rows today.

**Batch 27 — REFEREE RENDER COVERAGE (2026-09-03): ratchet 505/2068 →
487/2086 (+18, ZERO lost).** The post-batch-26 census's "class query
under TypedMap" (50) was, again, the referee render returning null:
(1) an execute whose mapping argument is a placeholder `^Mapping()`
while the query carries its mapping in-chain (`withMapping` / `from`) —
`PlanAllocations.activitySql` now takes the mapping the chain's TypedFrom
carries (`chainMapping`; fromMapping 5); (2) the H2 engine-style
renderer had no spelling for a LITERAL collection membership (a
let-bound list `$names->contains(x)`, a literal `->in([...])`): the
engine's in-list `x in ('John', 'Peter')` — `EngineStyleH2.membership`
over `literalElements` (filter::in 11, exists 2). The render's own wall
now prints under LL_TMP_DEBUG (`[render-debug]`), so the census can name
the cause instead of the automap wall two mechanisms downstream. Named
remainder in the functions families: H2 engine spellings for UNNEST
(concatenate projections), LIST_CONCAT, LIST_GET (percentile), JSON casts
— the DialectCapability bucket; `generateObjectReferences` unported (7,
objectReferenceIn); the concatenate `assertEquals` scalar-lowering wall
(7, the assert arm declines a class-frame `.values.name` read over a
concatenated qualified property); enum-decoded oracle declines (5).

**Batch 28 — INLINE HANDLES ON DEMAND + THE UNROLLED QUANTIFIED VERDICT
(2026-09-03): ratchet 487/2086 → 463/2110 (+24, ZERO lost).** (1) The
consolidation batch left handle-row registration at the let; an
`executionPlan(...)` INSIDE an inlined helper (`relationalMapperSqlQuery`,
whose runtime is a constructed `^Runtime(... relationalMapperPostProcessor
...)`) has no let to register at. The resolver now registers an inline
handle's rows on first meeting through a registrar the executor supplies
(`StatementExecutor.resolver` → `PlanAllocations.registerHandleRows`;
`ConstructedInstances.handleRows`) — the one row-registration idiom the
consolidation batch wanted, reached from the other side. relationalMapper
8 (the mapper renames already rode the plan model), executionPlan
datetime 3. Remaining relationalMapper 2: our join chain nests a filter-
carrying hop as a subselect where the engine flattens it into the ON
clause (an SQL-shape leg). (2) The toSQLString dialect-table idiom
`[pair(DB2, sql), pair(H2, sql), ...]->map(p| let driver = $p.first; ...;
assertEquals($expected, $result, fmt, args))->distinct() == [true]`: the
verdict UNROLLS per literal element (the collection chased through the
caller's lets), the inliner reduces the element's lets (`VerdictQueries.
unrolledElement` — the compiler layer mints the nodes, invariant 7), the
message-carrying assert normalizes to its two-argument form, and each
element adjudicates through the existing arms (the SQL-text arm judges
on rows; sqlstring 13). The AssertVerdicts ledger pin moved 1459 → 1511
with that justification (a verdict SHAPE, no host evaluation). Also:
the H2 in-list and chain-mapping render fixes of batch 27 carried into
these families. Diagnosed and NAMED, not built: the concatenate family's
7 "assertEquals scalar-lowering" walls are the SQL-text arm DECLINING
their assertSameSQL (text-divergent, exec-passing lane) and the
fallthrough lowering the assert's inlined body — a misleading wall text
over a text-policy fact; the toPostgresModel family (11 + 9) runs the
engine's SQL-AST conversion library (toPostgresModel.pure) over
constructed relational-metamodel instances — group G, a Pure-library-
over-rows leg.

**Batch 29 — SQL POST-PROCESSORS (2026-09-03): ratchet 463/2110 →
451/2122 (+12, ZERO lost).** The engine's CTE-extraction processor
(cteExtractionPostProcessor.pure:47-125) is an SQL-IR pass
(`SqlPostProcessors.extractSubqueriesAsCtes`): every subselect in the
FROM tree becomes `subquery_cte_<level>_<index>` — level = nesting depth
from the root, index = a per-level counter in tree order carrying across
siblings, a child's CTEs extracted before its parent's, the derived
table's alias kept on the reference. The result is a new query variant
`SqlWith(ctes, body)` (the sealed SqlQuery's third member; every
query switch gained an arm; both renderers spell it — the engine style
as `with a as (...), b as (...) select ...`). The runtime's
`sqlQueryPostProcessors = [{s | ^Result(values = $s->extractSubqueriesAsCTEs())}]`
hook is recognized (`SqlPostProcessors.hooks` → `Hooks(tableReplace,
extractCtes)`; the flag rides `PostProcessBoundary` beside the renames and
`SqlPostProcessors.applyRecorded` applies both at the execution and
render seams — exec never calls the middle-end, invariant 6d). replaceTables
pairs bound through the caller's lets (`let oldTable = ...; pair($oldTable,
$newTable)`, `[$pair1, $pair2]`) resolve: the recognizer chases variables
through a binder the frame builder supplies. And a real bug: a VERDICT
over a frame ran its rows leg under the STATEMENT env, not the frame's
post-processing env — the golden (renamed table, empty) gave 0 rows and
our re-execution 7; `tryAdjudicate` now runs under `frameReplaceEnv`.
Remainder in the family, named: `SQLQuery`-typed hooks (3 — other
processors: DB2 column rename, transformJoinOp, filter push-down),
`nonExecutable` (1).

**Batch 30 — EFFECTFUL HELPER VALUES + GENERIC MULTIPLICITY ARGUMENTS
(2026-09-03): ratchet 451/2122 → 446/2127 (+5, ZERO lost).** (1) `let
runtime = initDatabase()` — a helper whose body runs DDL effects and ends
in `^Runtime(connectionStores = ...)` — used to wall as "reading an
executeInDb result binding": the executor bound nothing. Now the helper's
effect-free VALUE binds as the let would have (`UserCallInliner.
helperValueLet`: the call's argument frame + the body's lets + its last
statement, inliner-reduced, refused when the value itself is effectful;
an execute() value becomes a frame, anything else a plain let with its
handles registered). Forced milestoning 4, businessdate 1. (2) The name
resolver's generic-type rebuild dropped the MULTIPLICITY arguments
(`Result<TabularDataSet|1>` → `Result<TabularDataSet>`), so every
`.values` read over a user function's Result typed `[*]` — the whole
validation family's wall ("toCSV argument 1: [*] not compatible with
[1]"). Fixed at the rebuild; the family now reaches its REAL wall: the
engine's `generateValidationQuery` library (validation.pure) has non-let
intermediate statements the inliner refuses — a Pure-library leg, named.
Ledger: StatementExecutor 2692 → 2696 (the value binding — orchestration);
exec-passing declines 180 → 171 (lane move).

**Batch 31 — THE QUERY FRONT DOOR (2026-09-03): ratchet 446/2127 →
430/2143 (+16, ZERO lost).** The relational `validate(...)` raw-space
desugar (`com.legend.validation.ValidateDesugar` — the engine's
generateValidationQuery synthesis rebuilt over the parsed AST, in MAIN
since feature #45) was wired only from the harness's `EngineTestExecutor`
preamble, so the harness lanes saw the synthesized execute while the FLIP
path resolved the raw statements, inlined the corpus's Pure `validate`,
and walled on its library ("generateValidationQuery has non-let
intermediate statements"). `Compiler.resolveQuery(statements, imports,
ctx)` is now the ONE query entry — desugars, sets the driver-pk option,
name-resolves — and `WholeTestFlip` resolves through it (the preamble's
own call stays for the lineage harness arms until they die). validation
complex 10, showcase 5, businessdate 1. A platform feature must never
depend on a harness preamble to fire — the same lesson as batch 26's
second pipeline.

**Batch 32 — PLAN-EXECUTE FRAMES (2026-09-03): ratchet 430/2143 →
416/2157 (+14, ZERO lost).** The census's "plan-execute: parametersValues
binding pending (17)" was NOT the chartered referee-binding cut at all:
every TDG helper call passes `[]`, but the executor tested the helper's
PARAMETER variable, not its let-bound value — chased through the lets, the
frame builds. Then the helper's read `$result.values->at(0)->cast(@Tabular
DataSet).rows->isNotEmpty()`: a relation's `.rows` ARE the relation and
`cast(@TabularDataSet)` over a relation is the identity (`Anchors.tdsErase`
— CastChecker's rule, which typing could not apply because an envelope read
becomes a relation only at the splice); and a TDS-typed root (`tableToTDS`,
a TabularDataSet-declared value) is a relation-rooted frame like a
schema-typed one. testDataGeneration 14. Remainder in the family, named:
`functionReturnType` unported (4), the tableToTDS plan frame still reading
its values as an instance list (3, `struct_extract` over a list), the
chained-fetch text declines (5, charter §5 item 3), one column-spec typing,
one generateTestData inline-shape wall.

**Batch 33 — RUNTIME CONNECTIONS THROUGH LETS (2026-09-03): ratchet
416/2157 → 394/2179 (+22, ZERO lost).** The XStore ordered / XStoreUnion /
relational chain / resultSourcing tests all build their runtime the same
way: `let jsonConnection = ^JsonModelConnection(class = S_Trade, url =
'data:...'); let runtime = ^$dbRuntime(connectionStores = $dbRuntime
.connectionStores->concatenate(^ConnectionStore(connection =
$jsonConnection, element = ^ModelStore())))`. The executor let-chased the
execute() runtime ARGUMENT itself but the connection extractors
(`TypedFrom.jsonSourcesIn` / `chainMappingsIn`) walked the copied
instance's children and stopped at the inner `$jsonConnection` variable —
so `S_Trade` never got its JSON source frame and the resolver walled with
"class S_Trade is not mapped". The extractors now take the SAME let-chase
the executor applies to the argument (`ExecuteChainAssembly.letBound`),
chasing let-bound variables met anywhere inside the runtime value.
Flipped: XStore ordered 8, XStoreUnion 4, relational chain 4,
resultSourcing 4, XStore JsonToDB 2. Lane move: M1 rescued 127 → 119 (the
flipped tests' text-rescued sql-asserts now row-verify as platform-arm
verdicts; passes 2367 → 2374, disagree 0). Remainder in graphFetch/tests,
named: testCrossMappingJsonToDBWithExplosion (a JSON source with an
explosion mapping), compileLegendGrammar 2, StoreMappingGlobalGraphFetch
ExecutionNode.children 1, objectReferenceIn over a limit 1, JSON verdict
mismatches 4 (defects / employees / union legalName / milestoned
property), "trailing JSON" 2 (embedded otherwise milestoned).

**Batch 34 — assertSameSQL(String, String) IS A ROWS VERDICT (2026-09-03):
ratchet 394/2179 → 379/2194 (+15, ZERO lost).** The census's "no scalar
lowering registered for assertEquals (29)" was a fall-through two mechanisms
away from its cause: engine `testAssert.pure` declares TWO assertSameSQL
overloads — over a `Result` (which calls `sqlRemoveFormatting` itself) and
over a `String` (`assertEquals($sqlString, $result)`). Our verdict arm knew
the Result overload only; the String form (`assertSameSQL($golden,
$result->sqlRemoveFormatting())`) tried the plan-text and TDG arms, returned
null, and the statement fell to the lowerer, which met the inlined
assertEquals. The String form now takes the SAME exec-read rows verdict as
assertEquals (`SqlTextVerdicts.tryArmExecRead`). Flipped: in-clause joins 3,
forced-filter projection overlap 2, concatenate 3, query::function
contains/endsWith/if 3, distinct-in-join, embedded exists, association
mixed deep, group open variable. Lane moves (all the flipped tests'
sql-asserts leaving the walk's lane): M1 verified 20 → 12, M1 rescued
119 → 109, exec-passing 167 → 149; passes 2374 stable, disagree 0. Still in
the bucket, named: toPostgresModel 9 (the C2 recursion leg), concatenate
4 (testAllWithProperty, DataType, DataTypeDiffProperty, DataTypeMerge),
stringToFloat::testProject, tds sort testSortQuotes — re-probe with the
swallowed reason printed before naming their mechanism.

**Batch 35 — THE REFEREE RENDER: literal reductions, firstNotNull, round
(2026-09-03): ratchet 379/2194 → 369/2204 (+10, ZERO lost).** The "class
query under TypedMap <<TypedMap(cast@RelationalActivity(at(filter(.activities
...))), v_amr | .sql)>>" wall (19) is NOT a resolver gap: it is the sql()
read reaching the resolver un-spliced because the frame's activity render
(engine-style H2) had thrown a DialectCapability — the swallowed reason
prints as `[render-debug]` under LL_TMP_DEBUG. Census of those reasons over
the fallback set: UNNEST placement 5 (to-many navigation under map /
concatenate flat), LIST_GET 5 (percentile 3, firstNotNull 2), banker's
ROUND 4, LIST_CONCAT 3 (concatenate of two navigations), LIST_BOOL_AND/OR 4,
array literal 2, LIST_PRODUCT 1. Landed as dialect spellings in
`EngineStyleH2` (backend idioms as dialect rules, never lowerer arms): a
reduction over a LITERAL collection is the engine's infix chain
(`and([a,b,c])` → `a and b and c`; `[x,y]->times()` → `(x * y)`; sum
likewise), `firstNotNull` (`$set->filter(v | $v != TDSNull)->first()`) is
`coalesce(a, b)`, `round` is the bare `round(x[, n])` (rows verdict judges
the half-even value). Flipped: round 4, tdsFilter and/or 2, firstNotNull 2,
divide precision 1, columnValueDifference 1. Lane move: exec-passing 149 →
140 (passes 2374 stable, disagree 0). Still named from the render census:
percentile 3 (the lowerer's sorted-list pick formula is a DuckDB idiom —
the semantic node should be a PERCENTILE aggregate the dialects spell:
quantile_cont / percentile_cont within group), UNNEST 5 + LIST_CONCAT 3
(the to-many navigation collection as a RELATION — a resolver leg: the
engine renders `concatenate` of two navigations as a union-all subquery
joined once), array literal 2 (a let-bound literal collection read by
at()), columnValueDifferenceWithoutPrevalTest = a real rows divergence now
(TDSNull vs value in the difference columns), tds extensions
testFirstNotNull = unresolved type variable T at the lowering boundary.

**Batch 36 — PERCENTILE IS ONE SEMANTIC REDUCER (2026-09-03): ratchet
369/2204 → 366/2207 (+3, ZERO lost).** The lowerer encoded a descending
percentile with DuckDB tricks — continuous: negate the values, negate the
quantile back; discrete: a `QDISC_DESC` pseudo-reducer expanding to
`list_extract(list_reverse_sort(list(v)), ceil(p*count(v)))` — so the H2
renders (execution AND the engine-style referee text) had nothing to spell
and the sql() reads walled. Now the reducer is `Reducer(QUANTILE_CONT|DISC,
[v, p], orderBy = [v desc])` — the SQL-standard `PERCENTILE_x(p) WITHIN
GROUP (ORDER BY v DESC)` as data on the node (`Lowerer.AggFlavor.descending`);
`QDISC_DESC` and `Aggregates.qdiscDesc` are DELETED; the DuckDB encodings
moved into a named MIR pass (`sql.dialect.QuantileOrder`, in DuckDb.passes()
after the carrier strategies; a window-positioned percentile windows every
reducer of its encoding); `H2` spells `PERCENTILE_CONT/DISC(p) WITHIN GROUP
(ORDER BY v [DESC])`; `EngineStyleH2` the engine's lowercase form with the
direction explicit (extensionDefaults.pure:790). Flipped: testGroupByPercentile,
testTDSGroupByPercentile, testPercentileWindowFunction. Lane moves:
exec-passing 140 → 135, M1 rescued 109 → 108 (passes 2374 stable, disagree
0); DuckDBIntegrationTest/TypeInference/GroupBy/ExtendWindow/Spellings green.

**Named residue with receipts (2026-09-03, after batch 36; each probed to
its first cause, none built):**
- `testToSQLStringForTDSStringJoin`, `testHashFunctions` — GOLDEN DEFECT: the
  engine's H2 joinStrings emission appends the separator arguments at the
  END of one flat `concat(a, b, '|')` (extensionDefaults; the batch-27
  render mirrors it for TEXT), so its rows are `Anthony Allen|` /
  `md5(concat(first, last, '|'))`; ours are the Pure value `Anthony| |Allen`
  / `md5('Anthony|Allen')`. The rows verdict is right to disagree; the
  engine side is wrong. Same class as null-vs-value (ENGINE defect).
- `testToSqlGenerationFirstDayOfWeek` — GOLDEN DEFECT: the golden text is
  `date_trunc('week', d)`, which on an en_US H2 session is SUNDAY-based
  (golden rows 2014-11-30); Pure/engine firstDayOfWeek is Monday-based (the
  engine's own h2Extension2_1_214 spells `dateadd(DAY, -(mod(dayofweek(d)+5,
  7)), d)`); ours (DuckDB date_trunc week) gives 2014-12-01.
- `testQualifierQueryWithOr` — REFEREE GAP: a CLASS-typed result's golden
  rows carry the join fan-out (7 × Firm X) that the engine collapses into
  objects by pk; ours DISTINCTs. The rows leg should compare class results
  as pk-keyed sets. The test's own asserts (size 1, Firm X) pass.
- `testConsistencyWithNullsInColumnToColumnComparison` — REFEREE GAP: the
  Address.type enum column reached the rows compare undecoded (golden `1`
  vs ours `CITY`) — the c46 per-column decode map was not derived for this
  class-query frame; `enumPrecheck` did not fire either.
- `testInExecutionWithTempTableForDates` (+ the tempTable family) — the
  golden's temp table is not replayable on the oracle (0 golden rows).
- UNNEST 5 + LIST_CONCAT 3 (concatenate flat / to-many map / distinct over
  an exploded collection): the platform lowers a per-row literal collection
  as `LEFT JOIN LATERAL (SELECT UNNEST(list_concat([a],[b])))` — correct on
  DuckDB; H2 2.1.214 has NO LATERAL (probed over JDBC: `from t, unnest(
  array[t.id])` → Column T.ID not found), so the referee spelling must be
  the engine's own shape — the explode DECORRELATED into a UNION ALL keyed
  by the base row identity (`_ROWID_` / SqlExpr.RowOrder) and joined once.
  A structural H2-family MIR pass (LateralExplodeToUnion); the next referee
  leg.
- `testJoinLambdaAsVariable`, `testJoinWithLiteralColumn` — TYPER GAP: a
  let-bound `{a:TDSRow[1], b:TDSRow[1] | $a.getInteger('eID') == ...}` is
  typed at the let, where the receiver is the bare TDSRow class;
  `Typer.tdsReceiver` admits only relation row types, so getInteger walls as
  an unknown function. Admit the declared TDSRow class with the getter's
  declared return type (the join re-types under the schema).
- `repeat` 2 (AlloyOnly plan tests), `routeFunction` 4, `functionReturnType`
  4 (TDG alloy), `generateObjectReferences` 7 (objectReferenceIn — engine
  internals): unported engine-internal functions; residue.
- toPostgresModel 20 + debugPrint 9 + post-processor lambdas over SQLQuery 6
  = the C2 RECURSION mechanism (Pure `match` recursion over constructed
  instance trees producing instance trees; verdict = structural equality of
  two constructed trees). Design: instance trees are rows (node table:
  id, kind, parent, ordinal, scalar props); a recursive Pure function over
  them is a recursive CTE whose step applies the match arms per node kind;
  copy-instances with mapped children are new node rows; the verdict is a
  relational anti-join of the two trees. Needs a design record before code
  (the metamodel-in-database ruling: recursive CTEs, never a Java
  interpreter).

**Batch 37 — THE TEXT-POLICY GATE IS DELETED (2026-09-03): ratchet 366/2207
→ 330/2243 (+36, ZERO lost).** User ask: "1-by-1 deep analysis on all the
SQL ones, not guessing or sampling — even if the test only has a SQL assert
can we not check the SQL output?" The homework (docs/SQLTEXT_HOMEWORK_2026_
09_03.md) measured both populations: (1) the 65 "text-policy" fallbacks were
never ATTEMPTED — `WholeTestFlip` pre-declined any body whose sql asserts
were not a "simple" shape (`SqlTextShapes.allSimple`); attempted with the
gate off, 36 flip on rows verdicts and 29 wall by their own named reason;
(2) every sql-text assert the platform arm judged by TEXT: 170 asserts in
154 tests (a new per-test ledger, `target/sqltext-text-verdict-roster.txt`,
attributed by counter deltas per test), against 1,506 row-verified — each
listed with its reason. Landed: the gate and its shape census deleted (every
sql-assert shape is attempted; walls count by reason), the text-verdict
roster kept. Lane moves (all 36 flipped tests' sql-asserts leaving the
walk's lane): M1 verified 12 → 9, M1 rescued 108 → 75, exec-passing 135 →
99, unable-to-exec 20 → 14; passes 2374 → 2375, dual-channel disagree 0,
canon lattice unchanged (the 21 float-ULP rows). FOUND IN THE HOMEWORK, in
order: (a) referee — plan-only tests (executionPlan/toSQLString) never run the
store's setup, so the rows leg's session lacks the store's tables (the
golden names the same tables): seed the referenced store first, 15 asserts;
2 fixture skews (`"Trades"."Trade"` declared in schema Trades, seeded in
main; a mapping `FULLNAME` column absent from the seed); (b) referee: enum decode maps from the
enumeration mapping (20 asserts + 2 walled tests); (c) verdict arm:
assertSameSQL(String) over a FOREIGN-dialect toSQLString takes the
foreign-dialect text contract (3 walled tests fall to the lowerer today);
(d) referee misroute: 12 plan-text goldens handed to the SQL oracle; oracle
fixture schema 2; H2 extension UDFs 2; N-th activity replay 2; (e) ports:
toSQLString 8-arg / SQLResult 5-arg, Duration; the H2VERSION probe decision
(4); (f) honest text contracts named: foreign dialects 35, FreeMarker plan
holes 37, plan params 10, TDG temp tables 11, datediff-to-now 8, forced
isolation 2, engine-feature predicates (removeUnionOrJoins 5, alias quoting
2), engine-internal functions 3; (g) our emission gaps a text predicate
caught: restrict drops unused aggregates, parseDate constant spelling.

**Batch 38 — THE NO-DECISION BURN FROM THE SQL-TEXT HOMEWORK (2026-09-03):
ratchet 330/2243 → 314/2259 (+16, ZERO lost).** User directive: "burn
everything you can down that does not need decision". Landed: (1) the
exec-read rows leg hands the frame's MAPPING and root class to the oracle —
an executed frame is no longer a let value, so the frame variable is
resolved through the SPLICE HOOK (`$result.activities` splices to the
frame's own execute() call; `SqlTextVerdicts.frameMappingAndClass`); the
oracle's enum decode (`H2Verify.decodeOf`) chases INCLUDED mappings
(`PlanText.enumMappingOf`) and is the IDENTITY when the enum has no
enumeration mapping at all (real pure decodes the source value by name —
a derived `dayOfWeek()` column likewise): enum-decoded text verdicts 20 →
0 in the flipped set; (2) a let-bound join condition (`let jc = {a:TDSRow[1],
b:TDSRow[1] | ...}; ->join(..., $jc)`) binds through the JoinChecker alias
chase, the declared TDSRow class is the NOMINAL row supertype in type-variable
binding (`InferenceKernel.isTdsRowClass`), and the TDSRow getters
getInteger/getFloat/getDecimal/getDate/getDateTime/getStrictDate/getBoolean
are declared (tds.pure:84-114; native-catalog golden regenerated, +7 lines);
(3) `assertSameSQL(String, String)` takes the GENERAL arm (a toSQLString
producer → the dialect-aware verdict; the query lambda is let-chased) — the
three foreign-dialect tests now reach the DB2 text contract and FAIL on
DB2 spelling (isDistinct as `case when ... then 'true' else 'false' end`,
divide) — an emission gap, named; (4) a REFEREE RULE found by the burn: a
paginated golden (offset/fetch/limit) whose rows diverge is NOT a row
verdict — which tied rows land in the page is the backend's tie order
(`order by firstName offset 0 fetch 4` over two Johns: golden
[22|John|Johnson] vs ours [12|John|Hill]); `H2Verify.compareFrame`
declines it by name (witness testPaginatedByVendor, which the let-chase had
exposed and would otherwise have been LOST). Flipped: groupBy agg-to-many 5,
enum-mapped embedded/multigrain/milestoning projections 6, tdsJoin 2,
testDayOfWeekFunction, testToSQLStringJoinStrings,
testConsistencyWithNullsInColumnToColumnComparison. Lane moves: M1 verified
9 → 4, M1 rescued 75 → 63, exec-passing 99 → 82, unable-to-exec 14 → 13;
text-verdict asserts 170 → 156; passes 2377 stable; disagree 0.

**Batch 39 — THE LATERAL EXPLODE ON H2 + plan-text replay (2026-09-03):
ratchet 314/2259 → 310/2263 (+4, ZERO lost).** (1) A per-row LITERAL
collection (`$t.id->concatenate($t.id + 18)` in a project column) lowers as
`LEFT JOIN LATERAL (SELECT UNNEST(list_concat([a],[b])) ...)` — right on
DuckDB, unspellable on H2 2.1 (no LATERAL: probed over JDBC). The
H2-family renderers now carry a structural MIR pass
(`sql.dialect.LateralExplodeToUnion`, in H2.passes() and
EngineStyleH2.passes()): the explode becomes the engine's own shape, a
UNION ALL of one select per literal element keyed by the base row identity
(`_ROWID_`), joined once; null-dropping singleton wrappers (`CASE WHEN e IS
NULL THEN [] ELSE [e] END`) become the branch's `WHERE e IS NOT NULL`. The
engine-style render entry now RUNS its dialect passes (it had bypassed
`passes()`). (2) A plan-text golden replays its ONE `sql =` node instead of
the whole plan text (the homework's 12 "Syntax error … Relational(" oracle
misroutes; a multi-node plan is a counted text contract). Flipped:
testConcatenateFlat, testConcatenateFlatWithOtherProperty,
testConcatenateWithFilter, testMapWithOpenVariableOutsideBlock. Lane move:
exec-passing 82 → 79; text-verdict asserts 156 → 147; passes 2377 → 2378;
disagree 0. NAMED (lowerer semantics, not referee spelling): a WHOLE
relation collected as a list then exploded — `Product.all().name->
concatenate(Product.all().name)` (testAllWithProperty) and `->map(...)->
distinct()` over a class query (testCollectionDistinctFunction) lower
through LIST aggregates + list ops + UNNEST; the engine's shape is UNION
ALL / SELECT DISTINCT at relation level — a lowering leg (2). The TDG
"alloy" family (8) walls in `planTestDataGenerationWithParameterValuePairs`
(functionReturnType + `_subTypeOf` folds) and then needs the TDG plan-text
printer (MultiResultSequence/Allocation nodes) — a port, no decision, sized
medium. DB2 text contracts (3 tests) need the engine's alias breadcrumbs
(`personTable_d#4_d_m1`) — a decision (batch 21 named residue).

**Batch 40 — THE TDG PLAN IS A PLATFORM VALUE (2026-09-03): ratchet
310/2263 → 308/2265 (+2, ZERO lost).** The 8 TDG "alloy" tests assert the
`planTestDataGeneration(...)->planToString()` text; the harness scored them
through its own arm (`TestDataGenForm.planText` → the platform's
`TestDataGenerator.planText` printer) while the platform walled in the Pure
body (`functionReturnType` / `_subTypeOf`, engine-internal). Now
`planTestDataGeneration` (7/8-arg, testDataGeneration.pure:818/823) is a
CoreFn (`PLAN_TEST_DATA_GENERATION`) minting the protocol-capturing carrier
`TypedTestDataGen` with flavor `plan` (typed ExecutionPlan[1]; the
orchestrator leaves it as a value; a let-bound row-identifier list binds
through the alias chase), and `planToString` over it prints the platform's
MultiResultSequence text (`TestDataGenerationNatives.planText`); a plan
carrier is not a fetch-text producer for the sql-text arm. Flipped:
testConstant_Alloy, testViewChild_Alloy. Lane move: text-only 27 → 26.
Remaining alloy 6, named: H2VERSION probe 3 (decision), view-backed relation
slice 1, `let tableRowIdentifiers = []` with an EMPTY collection still
unclassified 1 (testErrorDueToNoSeedForRoot — the alias resolves to `[]`;
classifyArg's empty-collection arm), column-spec typing 1. The harness arm
`TestDataGenForm.planText` is deleted when the family flips whole.

**Batch 41 — LET-BOUND COLUMN ARGUMENTS + the no-seed error plan
(2026-09-03): ratchet 308/2265 → 304/2269 (+4, ZERO lost).** (1)
`ProjectChecker.resolveLetBoundColumns`: a project whose column / name
argument is a let variable binds the raw literal through the alias chase
(paths, names, legacy `col(lambda, 'name')` calls, a cast around them
strips); `Typer.deferredLetRhs` parks a col-spec COLLECTION (legacy col
calls, with or without `->cast(@BasicColumnSpecification<T>)`) — a column
spec types only against its consuming call. Flipped: projection::simple
UsingVariable / UsingVariables / UsingOpenVariables (3); the ColVar
aggregation test now reaches the resolver's known "aggregate behind a
to-one head" residue. (2) The TDG plan for a ROOT WITHOUT ROW IDENTIFIERS
is the engine's own Error node (testDataGeneration.pure:511): the assert
message with the primary-key columns and the `select top 5` sample over the
pk columns (`TestDataGenerator.planRootErrorNode`). Flipped:
testErrorDueToNoSeedForRoot. Lane moves: M1 verified 4 → 1, exec-passing
79 → 76, text-only 26 → 25; passes 2378 → 2379; disagree 0. Alloy remainder
4: H2VERSION probe 3 (decision), view-backed relation slice 1; plus the
alloy extend-over-tableToTDS test whose col spec is typed standalone inside
the TDG relation scan (named).

**Batch 42 — THE PK-COLLAPSE FACT IN THE VERDICT-ARM LANE (2026-09-03):
ratchet 304/2269 → 303/2270 (+1, ZERO lost).** The walk lane armed the
graph compare's pk-collapse (`H2Verify.EXTENT_SUBSET`, charter §6.1: the
engine's join fan-out re-manufactures one object per pk) from its own
static read of the raw chain; the platform's sql-text arm never did, so a
class query with an OR of qualified-property navigations (7 golden rows,
one object) diverged. Now `SqlReplayOracle.verify` takes the STATIC
extent-subset fact computed on the TYPED chain (`SqlTextVerdicts.
extentSubset`: getAll through filter/sort/sortBy/limit/slice/drop/from/
first/last/toOne/take), recovered for exec-read frames through the splice
hook alongside the mapping (`FrameFacts`), and the harness oracle arms the
flag around its compare. Flipped: testQualifierQueryWithOr. Lane moves:
exec-passing 76 → 75, M1 rescued 63 → 62; passes 2379; disagree 0.

**Checkpoint after batch 42 (2026-09-03, "burn everything that does not need
decision"): 303 fallbacks / 2270 flipped; batches 38–42 = +29, 0 lost.**
Tried and REVERTED (receipt): routing a user helper whose body ends in an
assert-family call (toPostgresModel's `assertConversion`) to a call frame —
it moved that family's wall one step (to `newState()` reaching the lowerer,
the C2 constructed-state territory) and LOST testPaginated; net negative,
reverted. NO-DECISION legs still open, sized: (a) the H2-family referee
spelling of a WHOLE relation collected as a list then exploded
(`select unnest(list_filter(list_concat(list_filter((select list(x order
by …) from …)…))))` → the engine's UNION ALL / SELECT DISTINCT; a second
structural pass beside LateralExplodeToUnion; 2–3 tests); (b) zip+forAll
unrolled verdict (needs pair.first/second folds; 1); (c) enumValues()->
filter(in) fold for the dialect-loop forAll (1); (d) the extend col spec
typed standalone inside the TDG relation scan (1); (e) TDG view-backed
relation slice (1); (f) C2 — Pure recursion over constructed instance
trees (toPostgresModel 20, debugPrint 9, SQLQuery post-processor lambdas 6):
the design direction is ratified (metamodel as relations, recursive CTEs)
but the build is multi-day and wants a design record first. DECISION items
(named, not burned): H2VERSION probe 7, removeUnionOrJoins predicates 5,
engine alias breadcrumbs for foreign-dialect text 3, golden defects 3
(joinStrings separator ×2, Sunday-start firstDayOfWeek), engine-internal
functions (objectReferenceIn 7, routeFunction 4, repeat 2), the 15
plan-only text asserts whose stores no engine test seeds.

**Batch 43 — THE REFEREE RENDER RUNS THE H2 CARRIER STRATEGIES (2026-09-03):
ratchet 303/2270 → 297/2276 (+6, ZERO lost).** Root cause behind the whole
"UNNEST / LIST_CONCAT reached a dialect without…" render class: the
engine-style H2 renderer had NO dialect passes at all (`EngineStyleH2.
passes()` returned empty until batch 39 added the lateral explode), so a
semantic collection node (a collected relation exploded, a filtered
collect) never reached the H2 carrier rungs the execution dialect runs.
Now `EngineStyleH2.passes()` = `CarrierStrategies(Caps.H2,
foldLiteralReductions = false)` + `LateralExplodeToUnion`; the literal
STRING_AGG reduction stays a semantic node for the engine text (the
renderer's own `joinStringsFlat` spelling — folding it there regressed the
two joinStrings goldens' text, caught and gated). Two new explode rungs in
`CarrierStrategies.explode`: a null-dropping `list_filter` over an exploded
concat becomes each branch's WHERE (`filterBranches`), and the ordered-dedup
idiom (`ListEncodings.orderedDedup`) over rows is DISTINCT (`dedupList`,
`distinctOf`). Flipped: concatenate testAllWithProperty / DataType /
DataTypeDiffProperty / DataTypeMerge, testCollectionDistinctFunction,
testBuildFilterWithValueThatCanBeNullWithIn. Lane move: exec-passing 75 →
68; passes 2379; disagree 0.

**Batch 44 — NO-DECISION SINGLES (2026-09-03): ratchet 297/2276 →
291/2282 (+6, ZERO lost).** Three mechanisms, each the platform's own:
(1) `zip` is the POSITIONAL pairing of two ordered collections
(collection.pure: truncates to the shorter) — `ListEncodings.zip` (the
rule's existing owner; a duplicate Scalars rule shadowed it UNTYPED and
tripped the PCT census — check the owner before adding a rule) now spells
it `list_transform(list_zip(a, b, true), x -> {first: x.1, second: x.2})`
with a `SqlTyping` LIST_ZIP rule (positional struct fields "1"/"2"); the
former `list_get(a, i)` spelling put the sides INSIDE the lambda, which
DuckDB rejects for a collected-relation side. The resolver's per-row
project shape (`CorrelatedSubselects.zipPairMap`) stays for two direct
reads of ONE class chain and now also takes a NESTED zip as a `^Pair`
column (`renameParam`/`renameVar` over `mapChildren`), but DECLINES a
read through a hop (`$vals.address.name` auto-maps and drops empties —
positional, not per-row) instead of throwing; `StoreResolver` then
resolves the zip's sides structurally and leaves the map to the list
carrier. (2) The Result-envelope splice (`ResultEnvelopeSplice`) erases
`cast(@TabularDataSet)` and `.rows` AFTER splicing their SOURCE — the hook
is offered each node before its children, so `->at(0)->cast(@TabularDataSet)
.rows` over a `Result<Any>` (a helper's `FunctionDefinition<Any>` query
parameter — testDataGeneration `loadAndTestExecution`) never saw the
spliced relation; the same `isTdsType` test now covers the ClassType
spelling of the target (`Anchors.tdsErase` too). (3) `meta::pure::tds::
extend` — the FQN a TDS-typed receiver resolves the bare spelling to — is
a CURATED alias to the EXTEND checker (`CoreFn.of`, the `tds::distinct`
precedent): the generic path typed the legacy `[col(...)]` collection
standalone. Flipped: query::sort testSortByLambdaDeepOptional; TDG
testTableToTDSSimple, testTableToTdsWithAppliedFunctions (core + alloy),
testTableToTdsWithConcatenate, testTableToTdsWithGroupBy. Lane move:
exec-passing 68 → 63, M1 rescued 62 → 57; disagree 0. Debug aid kept: `[flip-wall-debug]`
prints the top 12 frames under LL_TMP_DEBUG (the "~hello" wall was two
frames past the old cut). STILL OPEN from the singles list: enumValues()
->filter fold (testSortQuotes) is MOOT — its golden carries the engine
alias breadcrumb (`addressTable_d#3_1_d#3_m2`), a decision item; the
lineage view trees (6: testSimpleViewRoot/RootToJoin/TableToViewJoin/
ViewEmbeddedInChainedJoin/UnionViewOnView/RelationalTreeCalculationWithView
InAnotherSchema) print the engine's view-expansion alias breadcrumbs
(`orderTable_d#2_d#2_m3`) — the same decision family, 6 more tests.

**Batch 45 — TWO MORE NO-DECISION MECHANISMS (2026-09-03): ratchet
291/2282 → 287/2286 (+4, ZERO lost).** (1) `if()` over a class query
decides STATICALLY when its condition is the emptiness of a literal
collection: `LiteralFolds.staticBool` folds `isEmpty`/`isNotEmpty` over a
`TypedCollection` literal and `not` over a static operand — the M3
`elementOverride` read already types to the empty literal (inheritance
`testGetAll`'s KeyInformation guard `if($r.elementOverride->isNotEmpty(),
|assert…, |true)`), so the guarded branch is never a runtime question
(+2: inheritance relational/union testGetAll). (2) A TDSNull-TYPED
collection root (`[^TDSNull(), ^TDSNull()]`, the assert's EXPECTED side)
reached the COLLECTION egress as SQL NULLs and walled as a lowering
defect; `Executor` now decodes such a root on the Any lane and egresses
each cell as the TDSNull VALUE — the wire's ONE spelling of it
(`PlatformTypes.TDS_NULL_CELL`, the [1..1] cell-read convention and the
referee's sentinel), so it compares equal both to a grid NULL slot and to a
getter's sentinel (+2: tree testProjectMerge, milestoning
testMilestoningColumnProjectionWithNonMilestonedTable). TRIED AND REVERTED
(receipt): stamping `rowCells` reads as [1] with an Any element type for
optional columns — LOST 20 (tds sort 12, association 8, realias 1): the
per-cell typed reads are load-bearing for sort/compare kinds; the null
cells were never on that path. NAMED after b45: `$tds->toJSON()` over a
TDS (`meta::json::toJSON` has no lowering — the engine's
`{"columns":[{name,type,metaType}],"rows":[{values}]}` envelope; 1 test:
testSimpleTypeMappingProjectNulls) — the executeLegendQuery TDS envelope
(`JsonEmission`) is the spelling to reuse. Lane move: exec-passing 63 →
61, M1 rescued 57 → 55; disagree 0.

**Batch 46 — RELATION-ROOTED PLAN TEXT + SCALAR-READ MAP COMPOSITION
(2026-09-03): ratchet 287/2286 → 285/2288 (+2, ZERO lost).** (1)
`planToString` over a body with NO class root (a table accessor or
`tableToTDS`) printed the "multi-node plans pending" wall: now
`PlanText.singleRelationRoot` prints the one node whose TDS tuples resolve
physically through the root table's database (`PlanText.rootTableReference`;
the plan-root finders `rootGetAllClass`/`rootTableReference` moved out of
`StatementExecutor` into `PlanText`), and a `#>{db.T}#` ACCESSOR root spells
its columns the engine's way — `meta::pure::precisePrimitives::Varchar`
with the pure type's DEFAULT relational spelling `VARCHAR(1024)`
(`plan/PreciseTypes`: RelationalCompilerExtension.convertTypes +
pureToRelational.pure pureTypeToDataTypeMap) while resultColumns keep the
physical width; `TypedTableReference.accessor` is the fact (the 2-arg
desugar). `PlanText.resolvePhysical` chases a STAR pass-through subselect
(an accessor's filter/limit stage) by name. +1 (relationalTDSType
ForColumnsAndQuoting). (2) `$chain.prop->map(v | f($v))` over an object
chain — a map whose SOURCE is a scalar read — composes the mapper over the
read (`map(chain, x | f($x.prop))`, `Pipelines.composeScalarReadMap`) so
the object-space map arm serves it (+1: exists
testComplexOrExistsToManyProperty). Lane move: exec-passing 61 → 60, M1
rescued 55 → 54; disagree 0. NAMED after b46: the two `take` table-accessor
plan goldens (testFilterLimit/LimitFilterInSequenceForTableAccessor) now
print the type/resultColumns blocks byte-exact but their `sql` differs in
engine alias breadcrumbs (`persontable_1`/`subselect`, star expanded) and
the golden is `planToStringWithoutFormatting` (whitespace-stripped: the
oracle cannot replay its sql node) — text-judged, DECISION family;
`execute(...).activities->filter(a|$a->instanceOf(RelationalActivity))
->at(0)->cast(...).sql` (stringToDate testToSQLString…UserDefinedFormat):
the instanceOf filter over activity rows is unserved (1); scanColumns
testNonDataTypeProperty maps over `removeDuplicates(scanColumns(...))`
rows with a string mapper (1); `sum` over `firm.employees.age` behind a
to-one head (testFilterTimesWithManyOperands, study #12).

**Batch 47 — parseDate AS A SEMANTIC NODE (2026-09-03): ratchet 285/2288 →
284/2289 (+1, ZERO lost).** `parseDate(text)` lowered straight to a typed
CAST, so the engine-style H2 referee text could never carry the engine's
spelling; now `Scalars` emits `Cast(Call(PARSE_DATE, text), DATE|TIMESTAMP)`
(the zone-carrying literal branch unchanged), `AnsiSqlRenderer` spells the
node `CAST(x AS TIMESTAMP)` (DuckDB / H2 execution), `EngineStyleH2` spells
the engine's `toTimestamp` idiom `cast(parsedatetime(x, 'yyyy-MM-dd
HH:mm:ss[.S…]') as timestamp)` (h2Extension2_1_214 transformToTimestampH2:
processParseDate appends the ONE fixed 'YYYY-MM-DD HH24:MI:SS' format);
`SqlTyping` types it TIMESTAMP; SpellingsTest lists it CODED. Flipped:
tdsExtend testParseDate (`assert($sql->contains('parsedatetime'))`). PROBED
AND NAMED (not burned): testRestrictOnGroupByEleminatesUnnecessaryAggs
WithDistinct asserts the engine DROPS the unused `max` aggregate from the
SQL after restrict (a projection-pruning optimisation across
distinct/restrict — design, 1); testProjectWithIfWhereOneSideIsEnumLiteral
(+2 siblings) index rows of an unsorted query (engine-cased scan order);
testExtendDigest_* (3) — `extendWithDigestOnColumns` is a
NormalizeRequiredFunction reading `$input.columns` metadata and `eval`ing a
returned accessor lambda per column (the α-rename's `_nr2` unbound is the
first symptom, the columns-metadata reflection the real leg);
iqrClassify/zScore (2) — `project([col(p|$p.first,'name')…])` over a
Pair[*] collection (col() unnormalized on a class-collection project);
testJoinIsolationDeeperTwoIsolations_LeftOuterLeftOuterThenInner — real
row divergence (last isolated join reads [] where the engine reads
'OrgName2'); graphFetch milestoning testMilestonedRootAndMilestonedProperty
(2) — "trailing JSON at 191" in the JSON verdict parse. Lane numbers
unchanged (exec-passing 60, M1 rescued 54; disagree 0).

**Batch 48 — ENUMERATION MAPPINGS AS ROWS (2026-09-03): ratchet 284/2289 →
282/2291 (+2, ZERO lost).** `toDomainValue` was an unported function and
`enumerationMappingByName` a K-side native signature with no evaluator.
Now the system store carries `enumeration_mappings(mapping_fqn, name,
enumeration_fqn)`, `enum_value_mappings(mapping_fqn, em_name, enum_value)`
and `enum_value_sources(…, source_value)` (`MetamodelSeeds.
enumerationMappings`, one row per (enum value, source value); a source
value spells as its text, an enum-ref source as its value name), the m3
surface grows `Mapping.enumerationMappings`, `EnumerationMapping.
enumValueMappings`, `EnumValueMapping{enum, sourceValues}` (real
mapping.pure:40-52), and the lite association `EnumValueSources
{sources: EnumSourceValue[*]}` carries the per-source rows; BOTH functions
are Pure bodies over the rows (`SystemMetamodel`): enumerationMappingByName
= `visibility->sortBy(includeRank).visible.enumerationMappings->filter(name)
->first()` (includes served by the closure, testEnumMappingsWithInclude),
toDomainValue = `enumValueMappings->filter(m|$m.sources.value->contains(
$sourceValue))->toOne().enum` — the resolver's to-many-association
membership arm (EXISTS) serves `contains` over an association hop, NOT
over a to-many primitive column, which is why the source values are rows
of their own. TWO FIDELITY NOTES (recorded in Pure.java): EnumerationMapping
drops its `<T>` (a GenericType hop breaks the object-space spine, which
keys on ClassType — the honest fix is `Anchors.objectSpine` accepting a
class-raw GenericType, a later leg); `enum: Enum[1]` reads as the value's
NAME (String) because an Any-typed leaf takes the variant lane and binds
no store column (the "property 'enum' … is not mapped" symptom). Native
class pin 255 → 256 (+EnumValueMapping); the documented-surface tables
updated; the native-catalog golden regenerated (enumerationMappingByName
left the native list).

**Batch 49 — LET-BOUND AGGREGATE VALUES (2026-09-03): ratchet 282/2291 →
281/2292 (+1, ZERO lost).** `let g = agg(x:Person[1]|$x.lastName, y:String[*]
|$y->count())` typed eagerly and walled ("no overload of 'agg' matches 2
arguments"): the engine's AggregateValue is a value that only types
against its groupBy. `Typer.deferredLetRhs` now parks a legacy 2-lambda
`agg(...)` (or a collection of them) like a column spec, and
`GroupByChecker.legacyToModern` chases the let (whole argument and per
element) through `env.resolveAlias`. +1 (executionPlan
testModelConnectionAgg). PROBED AND NAMED: `Unknown type: 'SQLQuery'`
(5: sqlQueryToString temp-table statements, pureToSqlQuery
simpleFunctionExpressionTranslation*, postProcessor filterPushDown /
Db2ColumnRename / TransformJoinOp) — engine SQL-generation library tests
(the C2 family's cousins); `StoreContract` (routing-context builders) and
the connection-equality five (`routerExtensions` over Extension[*] — the
real qualified property auto-maps; ours is a [1] native) — engine
internals; `groupByWithWindowSubset` — an unported tds.pure:867 Pure
function (6 args, a NormalizeRequired-style body).

**Batch 50 — THE MMMyyyy PARSE SPELLING (2026-09-03): ratchet 281/2292 →
280/2293 (+1, ZERO lost).** The lesson of batch 35 again: a "class query
under TypedMap(cast@RelationalActivity(at(filter(.activities(execute…))))
…sql)" wall is the referee render THROWING — `[render-debug]` named it:
"strptime format has no engine-H2 parsedatetime spelling yet: … FormatLit
[MONTH_ABBREV, YEAR4]". `EngineStyleH2.h2Pattern` now spells MONTH_ABBREV
as `MMM`, and the STRPTIME arm applies the engine's convertToDateH2 rule for
the month-year format (`parsedatetime(concat('01', x), 'ddMMMyyyy')`, the
FIXME hack the goldens pin). +1 (stringToDate
testToSQLStringconvertToDateinH2UserDefinedFormat; rows verdict, text
diverged-rescued). Lane move: exec-passing 60 → 59; disagree 0.

**Probe receipts after batch 50 (2026-09-03, no batch):** (1) REFEREE
RENDER CENSUS over the whole corpus under LL_TMP_DEBUG (`[render-debug]`
lines correlated to tests): 57 variant/JSON declines, 19 strftime pattern
gaps, 15 relation-value type gaps, 4 STRING_AGG list encodings — but only
ONE still-walled test carries a render gap (objectReferenceIn, a decision
family); the rest sit on flipped tests whose rows verdict passes, so the
referee-render legs no longer move the ratchet. (2) union relation
testUnionTwoRelationMappings_ManyColumnProject: both sides are
`->toString()` over a TDS (the engine has NO tds toString in Pure — it is
the compiled TabularDataSet's own print); our TDS LITERAL side prints a
blank cell as `null`, our QUERY side prints the same NULL cell as `''` —
an internal inconsistency between the literal and the relation print
paths (Render.tdsCell "null" vs the toCSVString '' rule), to be settled
against the engine's Java TabularDataSet.toString before touching either.
(3) selfJoin testSelfJoinPropertyMapping: the EXPECTED Pair literals
(`pair('Banking','Firm X')` with a TDSNull sibling) travel the variant
lane and their `second` field decodes as JSON TEXT (`"Firm X"`, quotes
kept) while the getter side is plain — `Executor.unwrap` returns a
JSON-typed STRUCT FIELD raw; decoding it as a value (decodeAny) is the
fix, gated on struct fields (a Variant ROOT keeps its JSON contract).
(4) injection testProjectThroughAssociation (+AutoMap, +multiJoins
testForcedSubTypeProjectDirect): the filtered read
`$t.products->filter(p|$p.date == $t.d)->toOne().name` lives INSIDE a
`->map(t|…)` over the to-many `$b.trades` — `SyntheticHeads.liftArms`
lifts filtered heads rooted at the QUERY variable only; a mapper-variable
root is the nested-lambda lift, a resolver leg (3). (5) sqlQueryMerging
testSQLQueryMergingForInnerJoins passes in isolation and fails in the
full sweep (order-dependent — a session/temp-table dependency to trace).

**Batch 51 — ANY-TYPED STRUCT FIELDS DECODE AS VALUES (2026-09-03): ratchet
280/2293 → 279/2294 (+1, ZERO lost).** Probe receipt (3) above, built:
`Executor.unwrap` decodes a JSON-typed STRUCT FIELD through `decodeAny`
(the Pair<String, Any> `second` slot: `"Firm X"` is the string, JSON null
the TDSNull slot); a Variant ROOT keeps its JSON-text contract, a struct
field is a value. +1 (selfJoin testSelfJoinPropertyMapping — its two
siblings already flipped). Lanes unchanged (exec-passing 59, M1 rescued
54; disagree 0).

**RECLASSIFICATION (user catch, 2026-09-03): post-processors are COMPILER
PASSES, not recursion.** The user asked earlier whether the "recursive"
family should be compiler walks; for the post-processor tests the answer
is yes and I argued past it. A post-processor (`sqlQueryPostProcessors` on
the connection: replaceTables, nonExecutable, CTE extraction, filter
push-down, DB2 column rename) is a rewrite over the SQL tree; our SQL tree
is the IR, and batch 29 already serves replaceTables + CTE extraction as
IR passes recognised from the connection (`Hooks`). The six tests filed
under "C2 recursion: post-processor lambdas" are therefore NOT tier-2
witnesses: `nonExecutable` (testReplaceTablePostProcessorWithSubQueries
+1) needs a pass that ANDs `1 = 2` into every select's filter; the
replaceTables-under-toSQLStringPretty tests need the recogniser to read
pairs built by model navigation (`db->schema('default')->table('personTable')`)
instead of let-bound literals only. Production users attach PLATFORM
post-processors; a custom Pure-bodied SQL-tree walk is a documented
boundary (the engine exposes its SQL tree as a Pure API, we expose IR
passes). The recursion design record now covers ONLY toPostgresModel (20)
+ debugPrint (9) + stragglers (4): tier 1 (unroll as M2M composition over
literal / statically-typed sources, `match` folded on the static class,
discriminated-row `match` shared with M2M inheritance dispatch) covers
them all; tier 2 (recursive CTE) has NO remaining corpus witness and is
parked as a written boundary.

**Batch 52 — POST-PROCESSORS AS COMPILER PASSES (2026-09-03): ratchet
279/2294 → 277/2296 (+2, ZERO lost).** The reclassification built: (1)
`SqlPostProcessors.nonExecutable` — the engine's nonExecutablePostProcessor
as an IR pass: every SELECT (root, FROM-tree subselects, union branches,
CTEs) takes `<filter> and 1 = 2` (bare `1 = 2` when unfiltered); join ON
conditions untouched; recognised from the connection's hook
`{query | nonExecutable($query, ext)}` (`Hooks.nonExecutable`,
`PostProcessBoundary.recordNonExecutable`, applied in `applyRecorded`
after table replacement and before CTE extraction). (2) The golden-vs-
render verdict arm (`SqlTextVerdicts.tryArm`) accepts the RUNTIME overload
`toSQLStringPretty(lambda, mapping, runtime, ext)`: the dialect is read
from the connection (`ConnectionFlags.databaseTypeOf` after the let chase
and a helper inline) and the runtime's replaceTables hooks ride the rows
leg through the env's tableReplace channel — the golden names the replaced
tables, so our rows must be read from them too (the H2 oracle gave 0 rows
from the empty replacement tables; ours read 7 from the originals until
the hooks applied). Flipped: nonExecutable
testReplaceTablePostProcessorWithSubQueries, testToSqlStringReplaceTables
PostProcessor. NAMED: the three `Unknown type: 'SQLQuery'` post-processor
tests are testDb2ColumnRename (`reAliasColumnName` under a DB2 connection
— the DB2 text family), testPostProcessTransformJoinOp (a CUSTOM
`postprocess` lambda rewriting join operations — the documented
non-goal), testPushFiltersDownToJoinsPostProcessorToSQL (filter push-down
— a platform pass not yet written; needs the abstract `relation::SQLQuery`
class declared for the hook lambda's parameter type first).

**Batch 53 — THE COMPILER COMPARES, THE DATABASE COMPUTES (2026-09-03): ratchet
277/2296 → 267/2306 (+10, ZERO lost; disagree 0).** The world map landed with it:
`docs/WORLD_MAP.md` (three kinds of Pure code — natives / platform semantics /
PROGRAMS; the deletion test; the prelude; compare-not-compute; the decision
procedure), `docs/TENET_CHARTER.md` Clause 6, the `AGENTS.md` pointer, and the
homework that decided it, `docs/OPTION2_HOMEWORK_2026_09_03.md` (all three Pure
sources read end to end; per-test traces for debugPrint 9 and toPostgresModel 21;
the probe receipt that without a Java `toLower` the family went 9/9 → 0/9 at the
un-applied filter lambda, not at the verdict).

Built (WORLD_MAP §4, Charter C6.2): (1) `UserCallInliner.literalArms` — the
tier-1 unroll: a recursive call re-enters while its literal argument STRICTLY
DESCENDS (`literalSizes`, no depth constant); `match`/`if`/`map`/`filter` act on
the literal BEFORE their bodies are rewritten; `filter` over a spelled list with
a predicate that stays a SQL boolean keeps each element under its own condition
(the CONDITIONAL-MEMBERSHIP residual, `if(cond, |e, |[])`); quoted code
(`TypedLambda.quoted`, minted by `CastChecker.deactivatedLambda`; `TypedDeactivate`
subjects) never folds; nothing folds at the query's own level (engine parity —
testIfIncludingQualifiers; the keyless-ctor-under-lambda decline). (2)
`LiteralUnroll` — compare-only folds over literals (arm by class, spelled fields,
`cast`, copy→instance, list shape: at/slice/limit/drop/concatenate/first/last/
toOne, same-kind scalar identity, `in`, `isEmpty`, short-circuit `and`/`or`,
`not`); the nine Java string folds of the first cut are DELETED;
`LiteralUnrollLedgerTest` pins the set. (3) The verdict for a shape-CASE:
`StoreNav.owns` no longer claims constructions (HOST_CONSTRUCTION_CLASSES deleted
— a `^Class(…)` is a VALUE); `StoreResolver.objectNode` treats a bare constructed
instance as a value (the rows lane had serialised it as rows and compared instance
trees by their ROOT NAME ONLY — the previously "passing" debugPrint verdicts were
that shallow); DynaFunction/Literal/Alias carry their `<<equality.Key>>` in the
prelude; identity layouts carry `__type` beside `__id` (`ClassLayouts.SYNTHETIC_TYPE`,
`MixedEncoding.syntheticField`); a struct-shaped value in a JSON slot takes the
variant carrier (`MixedEncoding.slotCarrier` — DuckDB then unifies the two CASE
branches to ONE struct type); the Executor decodes class values in JSON slots to
structures (`structured`); `EqualityKeys` no longer poisons a key whose DECLARED
class is keyless (the value is judged by its own classifier, per the engine's
recursive equal()); `ExecuteChainAssembly.narrowSideStamps` gives a verdict side
the COMPILED class of its let-bound program call (a `DynaFunction`, not the
declared abstract `RelationalOperationElement`); `AssertVerdicts.restrictNested`
restricts a polymorphic key slot's values by their own class (ledger 1511 → 1529,
justified in JavaEvalLedgerTest). Receipt: the debugPrint SQL is
`CASE WHEN len(list_filter([to_json(CASE WHEN lower('true') IN (…) THEN {…} ELSE NULL END), …])) > 0 THEN {castBoolean…} ELSE {case…} END`,
judged by the DynaFunction key tree.

NAMED after batch 53: (a) the SQL canon for a POLYMORPHIC nested key slot (a
`CASE` over `__type` per subclass) so the byte verdict of record holds there
instead of declining to the host referee; (b) the toPostgresModel leg per the
homework — slice A (13 literal-only tests: admit `toPostgresModel.pure` +
`dbExtension.pure` to the model, ≈60 SQL-node/relational declarations WITH keys,
signatures for 3 unreached library functions, folds for fold/tail/init/reverse/
defaultIfEmpty/newMap/get/groupBy/keyValues/enum toString/spelled-integer
compare/assert(true)/dynamicNew, unspelled-property defaults, static re-dispatch
of a runtime match on the narrowed type, qualified-property inlining, then DELETE
`NEW_STATE`/`CONVERT_ELEMENT`/`CONVERT_SELECT_SQL_QUERY`/`MODEL_CONVERSION_STATE`
and MetamodelWalk's conversion arms); slice B (6 store-leaf tests: a store read
in scalar position inside a constructed instance = scalar subquery; `Schema.tables`
+ `Table.schema` mappings; column ordinal); C (2 join-tree-row recursion tests) =
tier-2 residue.

**NEXT SESSION OPENS HERE — burn fallbacks, by census group (user
ruling 2026-09-02: every batch must move the ratchet; no mechanism-only
legs).** State: 267 fallbacks / 2306 flipped (batch 53, 2026-09-03; WORLD_MAP ratified — the toPostgresModel leg is next: slice A 13, slice B 6, C = tier-2 residue 2) (batches 14–43 = group D,
group Q plan nodes as rows, group A function bodies as rows, group E
lineage trees as rows, group I column lineage as rows, group H the
expression tree as rows, execution activities as rows, aggregation-aware
routing, the milestoning render leg), exec-passing 344, quarantine 34 rows / 9 walls (was 125 / 9;
group F LANDED — batch 7 above; batches 8–13 = speed + architecture,
ratchet unchanged), exec-passing 344, quarantine 125 rows / 9 walls.
NEXT = group Q (plan nodes as rows), then A/E/I/H (expression trees as
rows), then J/Z/N/G/P/O, then the non-metamodel buckets. Census after batch 15 (bucket dump): text-policy 65; "class
query under TypedMap (HN vocabulary)" 64 (heterogeneous); `mapping::sql`
45 (group C); FunctionDefinition.expressionSequence 43+26 (group A);
join-condition-reads-a-whole-variable 43; no-scalar-lowering 27+9;
scanRelations Join 21 (group E); plan parametersValues 17; activities
14; filter-predicate isolation 13+12; multiplicity 11; toPostgresModel
newState 11 (group G); group Q plan reads 12 (`expected Date/Integer/
String, got Any` — executionPlanTest's `$result.rootExecutionNode
.executionNodes->filter(instanceOf(RelationalInstantiationExecutionNode))
->at(0)…->cast(@SQLExecutionNode).sqlQuery` + assertEqualsH2Compatible;
the route is plan nodes as rows + the SQL-text referee).
1. **Group F — DONE (batch 7).** Was: mapping-metamodel query functions (27 tests; §1 of the
   homework: testRelationalExtension.pure 20, testExtendsForMainTable 5
   [DONE], testExtendsForPrimaryKey 1 [DONE], testSubtypeMapping 1)**:
   `_classMappingByClass` / `rootClassMappingByClass` / `view` as Pure
   bodies over rows. Real bodies (functions_Mapping.pure:28/:61,
   functions.pure:254): `_classMappingByClass` = includes' sets (recursive
   — the seeded include closure replaces it, as for classMappingById) ++
   own sets with `cm.class == $class` ++ AggregationAware members, then
   `addAssociationMappingsIfRequired`; `rootClassMappingByClass` =
   `_classMappingByClass->filter(s|$s.root == true)->last()`; `view` =
   `$_this.views->filter(t|$t.name == $name)->first()`. Seeds needed:
   `class_mappings.root` (m3 SetImplementation.root — declared `*`),
   `class_mappings.class_fqn` already there (map `SetImplementation.class`
   as an element reference — D3), `Schema` rows + `Schema.views`/`tables`
   associations, `Mapping.associationMappings` (association_mappings
   rows) for the addAssociationMappingsIfRequired half — grow by the
   20 tests' actual reads (census §2b/§2c). Retire the quarantine
   spellings `rootClassMappingByClass` / `_classMappingByClass` / `view`
   and MetamodelSteps' `rootClassMappingByClass` arm with the burn.
2. **Group D — harness vocabulary (43 tests)**: `meta::legend::
   executeLegendQuery` / `compileLegendValueSpecification` as the router's
   string entry (compile-from-string through the ONE router).
3. **Group Q — plan reads (13 + printers ~26)**: plan nodes as side-output
   rows (§2e) — name the tests from the bucket dump first.
The "class query under TypedMap (HN vocabulary)" bucket (64) is
HETEROGENEOUS (execute()+TDS-row reads on union tests, relationalMapper
SQL-text tests, tds unions, modelJoins) — never cut a leg by that label.

**Residue after leg 2**: (new) the
composed-row prefix scheme `<slot>_<column>` can COLLIDE with a physical
column of that spelling (two system-store columns were renamed around it:
`pk_column`, `super_mapping_fqn`/`super_id` — a user model with column
`ancestor_id` beside an association `ancestor` would hit
"duplicate column ... in relation type"); (new) a function-form mapping's
key facts are NONE (no text to read; an analysis of the lifted body would
derive them).

**Step 4 — prototype 2, testDynaAndOrInference** (homework §9 item 2), then
plan-nodes-as-rows, then the tree half — each decided on its own receipt.

Rules unchanged: one gate chain per batch, tree frozen during runs, pins
move only with their burn and a written justification, paired same-tree
sweeps byte-identical, save `core/target` rosters before a chain (G8
cleans), GMT test clock, no engine pure source in the platform, every
native signature verified against the real .pure.


**Batch 54 — OPTION S, SHAPES ARE DATA (2026-09-04): ratchet 267/2306 →
255/2318 (+12, 0 lost; disagree 0).** The prelude's library shapes are now
GENERATED: `PreludeGeneratorTest` indexes every `Class`/`Enum` header of the
engine + legend-pure checkouts, takes the DEMAND (corpus type positions and
enum-value references, every spelled FQN, corpus class supertypes, the FQNs
the platform's own Java and native signatures name, the admitted program
libraries in `Corpus.LIBRARY_FILES`), closes over what those declarations
name (loud on any dangling or bare name; loud on a hand class with no
provenance — spec index, m3.pure, or an allowlisted carrier), parses the
files with our parser in the platform dialect, resolves them with
`NameResolver` (which now carries real pure's IMPLICIT CORE IMPORTS —
`system::imports::coreImport`, 29 packages — as a tier below the file's
own imports), and prints `Prelude.java` (230 classes / 10 enums, equality
keys and verbatim defaults; round-trip parsed). `Pure.java` lost 217 hand
copies; the cut exposed three hidden gaps in them (ElementOverride in the
wrong package, two GUESSED sql literal classes that made the spec's own
`DateLiteral` ambiguous, `TabularFunction.schema` typed Any). m3 is a graph
file, so its shapes stay by hand — extracted by `tools/m3shape.py`
(AbstractProperty, QualifiedProperty, Association, Constraint, PackageableFunction,
Annotation/Stereotype/Tag/TaggedValue/Profile, Enum, Type with its real ends,
GenericType/TypeParameter/Generalization, ModelElement.name, Property<U,V|m>).
The m3 `Enum` metaclass classifies as an enumeration-VALUE type (any
enumeration or its name carrier conforms).

Compiler work the program needed (all WORLD_MAP §4 structural, pinned by
LiteralUnrollLedgerTest): `Typer.synthBody` — ONE rule for multi-statement
lambda bodies (let-inlining, a leading let before a non-let, `fail(..);v`,
`assert(c,m); v` = `if(c,|v,|fail(m))`) used by match arms, if thunks,
lambda arguments and eval; static re-dispatch of a runtime match on the
input's DECLARED type (arms no model class shares with it are dead and are
not rewritten); the effect scan skips an un-typeable dead callee; folds for
size/contains/isEmpty/at/first/last over SPELLED lists (elements may be any
expression; a [0..1] conditional-membership element is not spelled), spelled
maps (`newMap` over `pair`s with spelled keys: keyValues/get; groupBy over a
spelled collection whose key folds), defaultIfEmpty, isTrue, assert(true),
enumValues, dynamicNew over KeyValues, spelled-integer compares, an enum
value's `.name`, unspelled-property defaults (empty when the class declares
none), DECLARED DEFAULTS applied at ^new (PropertyDefinition/Property.Stored
carry the value; NewChecker synthesizes it), a dead intermediate statement
that folds to a literal is dropped, `toOne` of a spelled one. Verdict: a
helper-wrapped assert over CLASS values is adjudicated by the key tree (never
a TDS/relation/result carrier — those keep the grid arm and its float
policy); a wider-declared side (`Node[1]`) is narrowed by the wire `__type`;
a class-kind side that rode a JSON carrier is decoded to its structure.

Kept BY HAND with a receipt (`Pure.java`, "SYSTEM-STORE-COUPLED"):
SetImplementation (`class: Class[1]`, real `Class<Any>`), PropertyMapping
(`property: Property[1]`, real `Property<Nil,Any|*>`), Column (`owner:
Table[0..1]`, real `Relation[0..1]`; `nullable` added from the real shape),
EnumValueMapping (`enum: String[1]`, real `Enum[1]`), and the
PropertyMappingsImplementation/InstanceSetImplementation chain — the
metamodel store types element references as its raw row classes. The
generated versions regressed 20 tests through `$cm.class` whole-value reads,
the join-target rule (declared supertype vs mapped set — fixed for Generic
declarations in JoinChainEmission.classTypedTargetIfMapped) and instanceOf
over a row whose class gained a real ancestor. FOLLOW-UP: represent element
references as m3 rows in the store, then generate these six too.

toPostgresModel after batch 54: 12/21 pass (literal-only slice A). The
nine left: store-backed inputs (getTable/getColumn/TestDb reads inside a
constructed instance — slice B, "store read in scalar position = scalar
subquery"; the `toOne(filter(TestDb.schemas…))` stamp wall), the join-tree
tests (`$r->children()` — the arrow-call form of a QUALIFIED property, and
the join-tree recursion), and the standing sqlQueryToString helpers in live
arms of store-backed matches. Diagnostics added (property-guarded):
`-Dlegend.inliner.trace=1` (why a call stands), `-Dlegend.spec.trace=<fn>`
(who demanded a body), `-Dlegend.mapping.trace=<fragment>` (a resolution
wall's throw site).

Chain catch (G9 floors fell: essential 314 < 316, grammar 134, standard
203): the new core-import tier resolves a bare stereotype profile
(`<<equality.Key>>`, `<<temporal.businesstemporal>>`, `<<PCT.function>>`)
to its m3 FQN when the model declares it, and three consumers compared the
BARE spelling — every `<<equality.Key>>` class silently fell back to an
identity layout (`__id` per construction SITE), so `head`/`first`/`contains`
/`in`/`equal` over two spellings of one keyed instance disagreed. The rule is
now ONE: `PlatformTypes.isProfile(resolved, fqn)` — the exact FQN, or the
bare spelling in a model that does not declare the profile — used by
ClassCompiler (equality), FunctionCompiler (PCT) and MilestoningStrategy
(temporal); no bare profile compare remains in main. Restoring the keys
moved NOTHING in the corpus (per-family counts and decline rosters
identical). Second catch: the sqltypes ledger's `untyped=1` was the
FoldCall root of toPostgresModel's `$p->tail()->fold(...)` chain — `tail`
is a native, and the fold's source was `->cast(@Expression)` over the
spelled converted-parameter list; `tail` over a spelled list and a `cast`
over a spelled collection (every element accepted) now fold, the chain
unrolls to the struct literal, and the ledger is back to 0 (the test itself
stays a fallback: its verdict is the polymorphic nested-key canon, task #45).
Chain GREEN: G1 44s (4,396), G2 8s, G4 61s, G5 44s, G6 78s, G7 26s, G9 18s,
G8 72s.

**Batch 55a — THE JAVA PORT IS DELETED (2026-09-04): ratchet 255/2318 →
252/2321 (+3, 0 lost; disagree 0).** The user asked why batch 54's measure
of success — deleting the port — had not happened. Receipt first: a probe
that cut the walk at its one entry (`planWalk` at the statement root) left
the ratchet UNMOVED and dropped the family scoreboard by exactly three
(executionPlan 79 → 77, sqlDialectTranslation 12 → 11): the walk was
invisible to the ratchet (a walk-scored test is a fallback either way) and
still scored `testDatabaseConnectionSQLPopulation` + Legacy (the
`SQLExecutionNode.connection` read — the plan rows carried no connection)
and `testConvertLiteral` (`^NullLiteral()`, a property-less class with no
canonical layout). Landed: (1) the plan node's connection as ROWS —
`plan_connections(node_id, kind, db_type, test_data_setup_csv, ds_kind,
ds_test_data_setup_csv)` + `plan_connection_sqls(node_id, owner, ordinal,
text)` (PlanRows.connectionRows; MetamodelSeeds lists them as query-riding),
mapped as `TestDatabaseConnection[pcTest]` / `RelationalDatabaseConnection
[pcRel]` under a `DatabaseConnection` inheritance operation and
`LocalH2DatasourceSpecification[dsH2]` under `DatasourceSpecification`
(self-join on the connection row; `testDataSetupSqls` through the owner-
filtered join), routed from `SQLExecutionNode.connection`; the generator
now emits LocalH2DatasourceSpecification (platform demand). The cast raise
beside a to-many leaf (`Substitution.castLeafRead`) is stamped per joined
row — the list is the aggregation above the CASE (the stamp census caught
the [0..*] raise). (2) A property-less class's constructor is the synthetic
fields alone (`ClassLayouts.syntheticOnlyLayout`: `__id`+`__type` in an
identity lane, `__type` in a plain lane). FIRST CUT REVERTED: giving every
field-less class a layout made `Any` a struct and broke 144 verdicts — the
rule lives at the constructor, never on the declared type. (3)
`assertInstanceOf` over a class value: the platform verdict reads the wire's
`__type` (decoded through `Executor.structured`) up `ctx.isSubtype`; over a
conforming LITERAL it folds to the spelled-true assert (ledger
+assertInstanceOf). Then the deletion: `MetamodelWalk` (905), `MetamodelSteps`
(156), the executor's walk arms (583 lines), the harness's `instanceOfAssert`
NodeH arm; ledger rows/pins moved with receipts. Chain GREEN ~6 min.

What is LEFT on the Java-evaluation register after 55a (JavaEvalLedgerTest,
stripped lines): StatementExecutor 2,699 (the K-arm executor itself —
orchestration, frames, verdict routing), AssertVerdicts 1,576 (assert
verdicts over wire values), PlanText 845 (the engine-text plan PRINTER — a
Java-stamped printer the routed-tree-rows leg retires), SqlTextVerdicts 690
(SQL-text assert arms), TdsCompare 444, PureAsserts 313, AggAwareActivities
227 (the routed-query printer), StoreNav 199, DynamicPivot 118, JsonCompare
70, GridProbe 52, plus the pct bridge (ValueBridge 355, ModelPacker 266,
PctExecuteNative 131). Java-stamped FACTS still in main: PkInference,
ScanRelations/ScanColumns walks (lineage rows), the plan MODEL (planModel/
planConnOf — the lowering's product that PlanRows turns into rows). The
walk-era harness lane (`EngineTestExecutor`, core/src/test) still scores the
252 fallbacks. Hand shapes in Pure.java: 76 (m3 bootstrap, primitives,
carriers, six SYSTEM-STORE-COUPLED). Nothing of the toPostgresModel port
remains.

**THE 76 HAND SHAPES — census and the legs that retire them (user question
2026-09-04 "why is there still hand-crafted stuff in Pure.java?"):**
measured against the checkouts (`Class <fqn>` declarations in Pure source):
53 have NO Pure declaration anywhere — they are the m3 bootstrap
(`meta::pure::metamodel::*`: Any/Type/Class/Property/Function/
ValueSpecification/Multiplicity/Profile/… and the 12 primitives), whose only
source is legend-pure's m3.pure written in the bootstrap GRAPH notation
(`^Package(...)` instance literals; zero `Class` lines — our Pure parser
cannot read it; tools/m3shape.py scrapes receipts, it is not a loader); 3
more (Nil, ConcreteFunctionDefinition, relation::Column) sit in forms the
header scan does not index; 20 ARE declared in the spec and are hand only
because something of ours is coupled to a different shape: 10 platform
CARRIERS the checker/lowering key on (`Relation<T>`, ColSpec/FuncColSpec/
AggColSpec + arrays with function-type parameters, `Result<T|m>` with its
multiplicity parameter, Variant, TDSNull, Rows) and 10 SYSTEM-STORE-COUPLED
(Database, Mapping, Column, SetImplementation, PropertyMapping,
PropertyMappingsImplementation, InstanceSetImplementation,
EnumerationMapping, EnumValueMapping, RelationalActivity — the store types
element references as raw row classes: `class: Class[1]` vs `Class<Any>`,
`property: Property[1]` vs `Property<Nil,Any|*>`, `Column.owner: Table` vs
`Relation`, `enum: String` vs `Enum`; batch 54 generated six and lost 20).
Three legs, none a batch on its own (WORLD_MAP rule: a mechanism-only leg
with no named corpus tests rides a burn batch):
  (P1) element references as m3 rows in the system store (Class/Property/
       Enum rows, references by id) → the 10 store-coupled shapes generate;
       rides the first burn batch that needs class/property navigation from
       a mapping row (the class-query-under-map remainder, `$cm.class`).
  (P2) carrier parameterization in the checker: multiplicity parameters
       (`Result<T|m>`), function-type parameters on the column specs,
       `Relation<T>` as a declared generic → the 10 carriers generate and
       PlatformTypesDriftTest pins the generated text; rides a relation-
       function burn (the tds/relation families).
  (P3) an m3 GRAPH-notation loader (the `^Package(...)` bootstrap form) →
       the 53 m3 shapes + primitives come from m3.pure, tools/m3shape.py and
       the hand block die; pure cleanup, no corpus tests — after zero, or
       as the closing leg of the "flip to verbatim .pure files" trigger.
ORDER OF RECORD: toPostgresModel slice B (task #44) FIRST — it burns 6
named tests; P1 attaches to the next mapping-navigation burn; P2 to the
next relation burn; P3 last.

**[SUPERSEDED by batch 55b — see "NEXT SESSION OPENS HERE — the store-row
leg" at the end of this file. The record below is what that session tried;
its labels were audited in batch 55b and most of them reverted.]**

**slice B IN FLIGHT, UNCOMMITTED (2026-09-04,
session ended on the user's catch "are you shooting in the dark?" — yes:
three blind thread-dump attempts and a loading-rule change without a
measurement. Start by MEASURING, not editing.)**

Committed state: origin/main = 5da58631 (batch 55a 95ec1bcd + the hand-
shape census). Ratchet 252/2321, chain GREEN.

Working tree: 9 files changed, +264/−14, NOT committed, NOT gated:
`Compiler.java`, `builtin/Pure.java`, `compiler/NameResolver.java`,
`compiler/spec/CastChecker.java`, `compiler/spec/Typer.java`,
`compiler/spec/UserCallInliner.java`, `lowering/Scalars.java`,
`rcorpus/Corpus.java`, `rcorpus/Runner.java`. Per change, with its
receipt (the family runs are `-Drcorpus.only=sqlDialectTranslation`, logs
sliceb0..sliceb11 in the job tmp dir; the family stayed 12/21 throughout —
the walls MOVED, no test flipped yet):

  1. Corpus.LIBRARY_FILES += legend-pure `platform_store_relational/
     functions.pure` (+ `Corpus.PURE_ROOT`): the program helpers
     `children()`/`childByJoinName()` over RelationalTreeNode that
     toPostgresModel's preOrderTraversal calls (kind 3, corpus input).
     PRINCIPLED. Receipt: the 4 join-tree tests moved from "unknown
     function children" to the next wall (sliceb2).
  2. Runner.registerLibrarySource: the file's `native function`
     declarations must not enter the model (the registry is the definition;
     two same-FQN native overloads in that file also tripped the corpus
     duplicate check). First cut = a REGEX strip (hack, deleted). Second
     cut = Compiler.parseSources keys NativeFunctionDefinition duplicates
     by signature (principled, kept) AND the natives now ENTER the model —
     after which the family run HANGS (>10 min; sliceb12–14). A third cut IS in
     the working tree, UNRUN: `Runner.withoutNativeDeclarations` blanks each
     native declaration's span by the parser's element offsets (and
     `elementSource` skips natives). It was never executed — the user
     stopped the run. FIRST ACTION: measure the hang with that cut in
     place: rerun the family with `timeout 300`. If it completes, the
     parsed natives were the cause and the span-blanking stands as the
     loading rule (or, cleaner, drop NativeFunctionDefinition elements from
     LIBRARY sources at assembly — Runner line ~1238 where libraryRaw joins
     `sources` — if a structural filter over parsed elements is possible
     there). If it still hangs, bisect the other two edits of that step
     (orElse signature+rule; signature-keyed duplicates). Get a thread dump the
     way that works here: `jstack` from the SAME JDK as the fork
     (/Users/neema/.sdkman/candidates/java/25.0.1-tem/bin/jstack) failed
     with "not ready to participate in attach handshake"; SIGQUIT output
     did not reach the log — try `-DforkCount=0` (in-process) so the
     JVM is Maven's own, then jstack that.
  3. UserCallInliner.liveArms: the multiple-inheritance scan reads
     DECLARATIONS (`declaredSubtype`), never compiles every model class
     (a poisoned corpus protocol class — `raw::Lambda` unknown — was
     compiled by the old `ctx.isSubtype` scan and walled testConvertAlias/
     Table). PRINCIPLED; receipt sliceb2.
  4. UserCallInliner.staticArm: a runtime match over a value of DECLARED
     class C dispatches to the first arm whose class covers C when no
     earlier arm names a strict subclass of C (pure's first-arm-wins over
     the runtime class). PRINCIPLED. Not yet observed to fire: the
     `convertElement(getTable(..))` match still stands — see 6.
  5. NameResolver: a FUNCTION-call name keeps EVERY core-import package's
     definition as an overload candidate (`resolveNameMulti(.., true)`);
     TYPE positions keep first-match. PRINCIPLED (real pure matches by
     signature across the group). Needed because 7 added string::plus and
     bare `plus` then resolved to it alone.
  6. UserCallInliner.inlineCall recursion measure, now LEXICOGRAPHIC:
     (literal size strictly shrinks) OR (equal literal size AND a STORE-
     valued argument of a class no enclosing activation of that key holds
     — `argClasses` = declared classes of NON-literal args; `argClassSets`
     stack). Well-founded (finite class lattice). Receipt sliceb10: the
     inner `convertElement(Table row)` no longer stands on "does not
     descend"; the next wall was `orElse` unknown inside the
     SemiStructuredArrayFlatten arm — which means that arm was treated as
     LIVE for a Table-declared input, i.e. liveArms/staticArm did NOT
     narrow to the Table arm. UNEXPLAINED; the arm list order is in
     toPostgresModel.pure:84-135 (Table arm at ~105, no View arm). Check
     the input's typed class at the match (LL_TMP_DEBUG prints the wall
     node) before trusting 4.
  7. Pure.java `PLUS__STRING_MANY` = real `string::plus(strings:String[*])`
     (essential/string/plus.pure) + Scalars rule (concat over a literal
     list; string_agg '' over a list value) + Typer.checkGeneric fallback:
     `a + b` over strings whose pairwise overload fails (an optional
     operand) retypes as the collection form. PRINCIPLED. Receipt sliceb8:
     buildUniqueName's `plus(String[1], String[0..1])` wall gone.
     native-catalog.txt NOT regenerated yet (NativeFunctionTest will
     diff; regenerate from target/native-catalog-actual.txt, +2 lines with
     8).
  8. Pure.java `OR_ELSE__T_01__T_1` = real `lang::orElse<T>(maybe:T[0..1],
     dflt:T[1])` + Scalars COALESCE rule. PRINCIPLED, UNMEASURED (the run
     after it hung).
  9. CastChecker: a cast to a STRICT supertype keeps a TypedCast node so
     the static type WIDENS (pure types `->cast(@QueryBody)` as QueryBody;
     a fold seeded from it accepts Union) — SQL identity (CastPolicy: class
     targets flow). PRINCIPLED, UNMEASURED beyond the family (watch the
     resolver's chain-position cast rules and the PCT cast tests in G6).
  10. Compiler.parseSources: NativeFunctionDefinition duplicate key carries
     the parameter signature (as FunctionDefinition already did).
     PRINCIPLED, part of the hang suspect set.

Walls left per test (sliceb10, before the hang): testConvertAlias /
JoinStrings / SelectSQLQuery / SelectSQLQueryWithCTE / TableAliasColumn /
TabularFunction / Union = "class query under TypedNewInstance" (the
STORE READ IN A CONSTRUCTED INSTANCE'S SLOT — slice B's real leg, not
started); testConvertJoinTreeNode = "class query under TypedFold";
testConvertTable = the standing convertElement (item 6).

The slot leg, designed not built: in StoreResolver.anchoredNode, before
the default wall, a value node (TypedNewInstance / TypedUserCall /
TypedFold …) whose only anchored subterm is ONE object-space class chain
of multiplicity [1] (e.g. `getTable(..)` = `TestDb.schemas.tables->
filter(..)->toOne()`, spliced verbatim at every occurrence) rewrites to
`chain->map(_r | body[chain := $_r])` and resolves through
`resolvedScalarMapProject(chain, lambda, [1], context)` DIRECTLY (not via
resolveNode: Anchors.spaceOf puts a class-result map in OBJECT space and
objectNode would substitute the param back and loop). The mapper body is
a struct-valued column (TypedNewInstance lowers to StructLit; row-var
property reads become columns); the verdict's `side()` reads the single
row's struct. A body that still reads a to-many navigation off the row
var (`$table.columns->at(0)->cast(@Column)` — the "column ordinal") needs
the project-column lambda to carry that nested read (SubQueryLift-style
scalar subquery, or the nested-assoc materials); measure with
testConvertTableAliasColumn first. SubQueryLift is the closest existing
idiom (value-position class subqueries under lambdas).

Hacks removed this stretch (do not re-add): the regex native strip; a
static WeakHashMap "standing reason" side table on the inliner (static
diagnostic sink — INSTRUMENT STATE MATCHES FACT LIFETIME); an
LL_INLINER_TRACE rethrow that made every legitimately standing helper
fatal. A diagnostic for "why did this call stand" is still wanted: the
honest form is the reason RIDING the standing node (a field on
TypedUserCall, or the census reading the wall at the frontier), never
static state.

**Batch 55b — toPostgresModel slice B, the COMPILER side (2026-09-04):
ratchet 252/2321 → 251/2322 (+1 testConvertJoinStrings, 0 lost), chain
GREEN ~6m.** Audit of the nine uncommitted files (docs/GATES.md batch 55b
lists what was kept and reverted, with the reason per item). The lesson of
the audit: every "principled" receipt of that session was a wall INSIDE a
match arm that a Table input can never take (ViewSelectSQLQuery, the
flatten arm → pureToSqlQuery helpers → orElse / buildUniqueName / plus).
Narrow the match first; the walls behind dead arms are not walls.

Measured mechanism, in order (each moved walls in a family run):
  1. children()/childByJoinName() are SystemMetamodel VIEWS (the file
     they come from is the relational store's platform library, never
     corpus input; the hang was its 12 natives entering the model).
  2. The arm scan reads declarations (declaredSubtype) — the raw::Lambda
     poisoned-class walls.
  3. A runtime match over a SYSTEM-STORE ROW (UserCallInliner.
     systemRowClasses: a chain of property reads/filters/natives rooted at
     a TypedPackageableRef) keeps only arms some class bound in
     MetamodelMapping beneath the declared class reaches (Table → {Table,
     View} → the Table arm alone). A primitive input keeps its lattice's
     arms.
  4. Folds: scalar cast to its primitive (isEmptyStringLiteral's `value->
     cast(@String) == ''`), cast over `[]`, native concatenate + empty-side
     identity (preOrderTraversal over the root literal), zip (the
     convertJoinTreeNode fold's source), init (convertJoinStrings).
  5. The lexicographic recursion measure (convertElement re-entering on
     the Table row under an Alias literal).

**NEXT SESSION OPENS HERE — the store-row leg (task #44, the rest of
slice B). Family 13/21. Walls, measured 2026-09-04 with the batch-55b tree:**

  testConvertAlias / Table / TableAliasColumn / TabularFunction /
  SelectSQLQueryWithCTE / Union = "class query under TypedNewInstance":
  the assert operand is a CONSTRUCTED STRUCT whose slots hold reads off
  ONE toOne-wrapped element chain (`TestDb.schemas.tables->filter(..)->
  toOne()`, spliced by the inliner at every `$t.…` read — the same node
  object). testConvertJoinTreeNode / SelectSQLQuery = §7 row-backed
  recursion residue (their cascade currently errors inside the flatten
  arm; the honest wall is "recursion over row-backed children stands").

  DESIGN (user-ratified direction 2026-09-04: LEFT JOINS, never a
  scalar subquery per read — the scalar-subquery form was built, measured
  (TabularFunction passed on it), and REVERTED as the wrong shape):
  the constructed instance IS `chain->map(r | ni[chain := $r])` — one FROM
  source, the struct projected over the row, `$r.schema.name` a navigate
  step. Built as `StoreResolver.constructedRowForm` (collect toOne-wrapped
  object chains by identity; exactly one → substitute a row variable,
  `resolvedScalarMapProject(chain, mapper, [1], context)`), measured, and
  REVERTED with the batch because it cost two fallbacks elsewhere
  (testCreateTempTableStatement: a TableAlias instance value reached the
  lowering boundary once a constructed instance stopped walling;
  testChainedFiltersQuery: a user-mapping "property not mapped" wall —
  undiagnosed). Re-add it GUARDED (only when a row chain is found; a
  constructed instance without one must keep today's loud wall) and
  measure the full corpus before the family.

  Two questions it exposed, both in the ordinary user-query path on the
  metamodel store — answer them before anything else:
  (a) MetamodelMapping maps Table.name only and Schema.views only: no
      Schema.tables, no Table.schema, no Table.columns. Adding
      `tables[tbl]: @SchemaToTables` / `schema[schema]: @SchemaToTables`
      (the SchemaToViews join text) compiles, but the resolver registers
      NO navigate step for `schema` — `$r.schema.name` walls "navigation
      through class-typed slot property 'schema' [assocs=[]]"
      (Substitution.rewritePath:2420) while `Schema.views` over the same
      join works. Find where the class source's TypedNavigate steps are
      built from join-mapped class properties (NavigateChecker builds them
      from the synthesized `$class$` body — locate that synthesizer; the
      earlier grep found no `navigate(` emitter outside the resolver) and
      why the Table→Schema direction gets none. `columns[col]` (a to-MANY
      self-join, `{target}`) compiled as a COLUMN read ("expected
      RelationalOperationElement, got String") — a second gap; only
      testConvertTableAliasColumn needs it (`$table.columns->at(0)`, the
      column ordinal: relational_elements.ordinal exists).
  (b) Inside the projected struct, `parts->map(p | $p->toIdentifier())`
      over a String[*] value lowered as a ROW map (DuckDB: starts_with(
      VARCHAR[], …)) — the project-column rewriter must keep a list map
      over a non-class list as list_transform.

  Receipts to reuse: LL_TMP_DEBUG=1 prints the wall node (`Anchors.
  compact`) and `[flip-wall-debug]` stacks; LEGEND_LITE_DUMP_SQL=1 the
  SQL; the TEMP traces used this session (match input/live arms/stack at
  UserCallInliner's TypedMatchRuntime arm; map source shape at the TypedMap
  arm) are gone — re-add locally, never commit.

  ANSWERED after the batch (2026-09-04, probe reverted; the probe diff —
  mapping joins Schema.tables/Table.schema + the GUARDED row-map arm
  `constructedRowForm` + a registerNavigations trace — is the patch the
  session saved beside its logs; re-create from this description):
  (a) is NOT a join-direction problem and NOT a missing navigate step in
      the Table class mapping: with `schema[schema]: @SchemaToTables`
      declared, the emitter mints the `schema` slot. The wall is that the
      row source under the map is a COMPOSED chain row (`TestDb.schemas.
      tables` flattened by flattenSource): its binding for `schema` is
      `toOne($row.schemas_tables_schema)` (the slot under the composed
      prefix) while the flattened pipeline carries NO navigate steps
      (`navSteps=[]`) and no AssocSub for the head (`assocs=[]`), so
      registerNavigations (StoreResolver ~1607, `InnerDemand.navSlotAlias`)
      never demands it and Substitution.rewritePath (2420) walls. The
      existing route for "a tail continuing through the target's own
      class-typed slot" is the flatten's extraHeads/extraTails →
      NavMaterializer subNavs → provOut (flattenAssocs, StoreResolver 2911)
      — the LAST hop's own class-typed slot read off the map's row variable
      must be threaded into that hop's flatten (downstreamHeads reads the
      project column's paths, so `schema` should already be an extra head
      — verify with the trace; if it is, the miss is in provOut
      registration for the last hop). Witness: testConvertTable; four tests
      share it. TableAliasColumn additionally needs `Table.columns` (a
      to-many self-join — `columns[col]: @TableToColumns` with `{target}`
      compiled as a COLUMN read; `relational_elements.ordinal` carries the
      column ordinal for `->at(0)`).

**Batch 55c — the STORE-ROW leg + F10 proper (2026-09-04): ratchet
251/2322 → 246/2327 (+5, 0 lost), family 13/21 → 18/21 with every
verdict real, chain GREEN.** The record with the mechanism, the two
reverted designs (per-read scalar subquery; static `__type` dispatch —
infinite for Expression-typed keys) and the guardrail refactors is
docs/GATES.md batch 55c. The user's rulings this batch: LEFT JOINs, never
a scalar subquery per read; "why are we reimplementing things we already
have?" — the only nuance was a VALUE (a constructed struct over a store
row) reaching the assert, expressed as map-over-row; everything else was
the user-query path's own gaps on the metamodel store (composed-row
navigate slots, list maps in project columns, slot-prefix collisions).

**NEXT SESSION OPENS HERE — the last three of sqlDialectTranslation,
then the rest of the corpus (246 fallbacks; user directive 2026-09-04:
"keep going, burn down all the rest we can, one by one, correctly, no
hacks"):**
  (a) testConvertTableAliasColumn: `Table.columns` is unmapped in
      MetamodelMapping — a to-MANY SELF-JOIN (`columns[col]: @TableToColumns`
      with `{target}` compiled as a COLUMN read by JoinChainEmission:
      "expected RelationalOperationElement, got String"); the test reads
      `$table.columns->at(0)->cast(@Column)` — `relational_elements.ordinal`
      carries the column ordinal for `at(0)`.
  (b) testConvertJoinTreeNode / testConvertSelectSQLQuery: preOrderTraversal
      over the mapping's join-tree ROWS (`childrenData` to-many, row-backed
      — the §7 tier-2 residue; two witnesses now). The cascade today errors
      inside the flatten arm (the row-backed `$c` keeps every match arm
      live); the honest first step is the loud wall "recursion over
      row-backed children stands" (narrow the arms for a row VARIABLE by
      the system mapping's kinds beneath its declared class — the same
      systemRowClasses rule, for a variable bound to a system row), then
      the design: a recursive CTE over join_tree rows, or a bounded
      unroll by the mapping's join-tree depth (a compile-time fact from
      the rows).
  Then task #5: census core/target/wholetest-flip-fallbacks.txt by bucket,
  biggest no-decision buckets first, each batch measured + gated +
  committed + pushed.

**Design notes written while hot (2026-09-04, after batch 55c; user
order: TableAliasColumn first, then everything without a design, the
recursive CTE LAST):**

  testConvertTableAliasColumn — state: `Table.columns[col]: @TableToColumns`
  is mapped (self-join, `{target}`), `JoinChainEmission.classTypedTargetIfMapped`
  treats a property whose declared class is an abstraction of mapped
  classes as a navigation (Table.columns : RelationalOperationElement[*]
  routed to the Column set), and `mintNavSlotAlias` mints clear of the
  platform's relation accessors (`columns`, `rows`) — a slot named
  `columns` read as the row var's `.columns` accessor (typed String) — all
  three in the tree, wall moved to: "class-typed property '$_r0.columns'
  used as a whole value is graph output" (Substitution.rewriteHeadProp).
  The leg: a POSITIONAL PICK over a to-many navigation inside a row's
  projection (`$t.columns->at(0)->cast(@Column).name`). Join form (the
  ruling): a RANKED navigation material — the target rows joined with
  `row_number() OVER (PARTITION BY <join keys> ORDER BY ordinal) = k+1`
  (a new NavMaterializer material kind; at(k) → rank k+1, first → 1),
  never a correlated scalar subquery. Needs the store's column ORDINAL:
  `RelationalOpRows.columnRow` does not set `relational_elements.ordinal`
  (index 8) — MetamodelSeeds' column loops (lines ~447-461) must pass the
  declaration index. One witness today; rank by tests-per-design.

  testConvertJoinTreeNode / testConvertSelectSQLQuery — recursion over
  the mapping's join-tree ROWS (`preOrderTraversal`: `$r->concatenate(
  $r->children()->map(c|$c->preOrderTraversal()))`, children row-backed).
  Facts: the row-backed `$c` keeps every match arm live in the cascade
  (the honest first step is the loud wall — apply the systemRowClasses
  narrowing to a VARIABLE bound to a system row, then the recursion
  stands as "recursion over row-backed children"); the join-tree depth
  is a compile-time fact of the mapping's rows (the seed knows it), so
  a depth-bounded unroll is an honest alternative to a recursive CTE;
  the CTE form: WITH RECURSIVE over join_tree rows (parent_id) producing
  (node, depth, path) in pre-order, then the fold over the ordered rows
  — a new execution shape with these two witnesses only. LAST.

  The TableAliasColumn mechanism pieces (mapping `Table.columns[col]:
  @TableToColumns` + join; `classTypedTargetIfMapped` accepting an
  abstraction of mapped classes; `mintNavSlotAlias` clear of the
  relation accessors `columns`/`rows`; the `column()` SystemMetamodel
  view) are SAVED, not committed — they move a wall, not the ratchet:
  docs/patches-table-alias-column-leg-2026-09-04.patch (corpus-measured
  green at 246/2327 without the view; apply with `git apply`). Land them
  with the ranked-navigation leg.

  CENSUS after 55c (core/target/wholetest-flip-fallbacks.txt, 246): a long
  tail — the largest buckets are "Assert failed" 11 (real divergences, one
  probe each), "unknown function" 15 across 8 names (contextHasFlag 3 and
  isExecutionOptionPresent 1 are programs over a LITERAL execution
  context in engine core files — admit executionPlanFeature.pure /
  executionPlan_generation.pure's helpers as library input and let the
  unroll fold them; routeFunction 2 = engine internals, skip;
  tdsToJSONKeyValueObjectString 1 = a TDS→JSON-string lowering, one
  native rule with engine key-value spelling parity; `column` 1 =
  testImportDataFlow, which then needs pureToSqlQuery internals — skip),
  hNversion 7 (H2VERSION decision), TDG chained-fetch declines 12
  (decision), "TypeInferenceException in call to" 5, unbound variable 3,
  overload shapes 3+3, plan-text operation holes 3. Order: the singles
  that need no ruling first, then TableAliasColumn's ranked navigation,
  the recursive CTE last.

**Batch 55d — TableAliasColumn LANDED (2026-09-04): ratchet 246/2327 →
245/2328, family 19/21, chain GREEN 6m04s (docs/GATES.md batch 55d).**
The saved patch (docs/patches-table-alias-column-leg-2026-09-04.patch) is
now IN the tree — do not re-apply it. What the leg turned out to be: a
POSITIONAL pick over a to-many navigation is a synthetic head
(`columns#pN`, SyntheticHeads.POSITIONAL / parkPositional /
positionalRows) exactly like the filtered heads (`#fN`): the head's join
target is the physical row with `ordinal == k`, so `$table.columns->at(0)
->cast(@Column).name` is one more LEFT JOIN step on the row-form
projection — the SQL: `LEFT OUTER JOIN (SELECT * FROM relational_elements
WHERE kind = 'Column' AND ordinal = 0) ON db_fqn/schema_name/name =
table_name`. The store's ORDER column is named once
(SystemMetamodel.ORDINAL_COLUMN); a navigation whose target has no ordinal
walls loudly (no k-th row of an unordered collection). The one blind spot
that cost a session boundary: the lift walk (liftFilteredHeads →
descend) had no TypedNewInstance arm, so it never reached the fields of
the constructed instance — the row-form body — and the positional arm
never fired; a constructed instance is a value node like a collection.

**NEXT SESSION OPENS HERE — the burn continues (user directive: do not
stop; every batch moves the ratchet; no hacks).** Remaining in
sqlDialectTranslation: only the two row-backed-recursion tests
(JoinTreeNode, SelectSQLQuery) — LAST. Everything else: the CENSUS after
55c above (now 245 in core/target/wholetest-flip-fallbacks.txt after
the corpus run): singles needing no ruling first.

**Batch 56 — two no-decision singles LANDED (2026-09-04): ratchet
245/2328 → 243/2330, chain GREEN 6m10s (docs/GATES.md batch 56).** A
let-bound lambda literal in a CORE construct's argument position is its
literal (Typer.expandLetBoundLambdaArgs at applyCore — NOT at the generic
entry: that expanded every `execute($query)` and was withdrawn); a
mapping element read as a metamodel value (`mapping.enumerationMappings`)
is a property access over its system-store row (Typer.metamodelElementClass,
the same rule as `db.schemas`). Lane receipt: the walk's M1 text-match
lane is now EMPTY and pinned retired; exec-passing 58 → 57.

**Probe receipts (2026-09-04, after batch 56) — the census long tail is
now DESIGN legs, not singles.** Each of these was probed to its wall:
- objectReferenceIn 7 (`generateObjectReferences`): the spec is
  pathToElement + eval of a versioned protocol builder
  (generateAlloyObjectReference: classMappingById/resolveOperation,
  transformConnection with extensions, resolvePrimaryKeysNames,
  toJSON) and the consumer decodes base64 JSON. DECISION: a platform
  object-reference value (Java from spec — an opaque row handle over a
  set's pk columns) vs the whole protocol-transform program. Ask.
- `add(Date, Duration)` / `subtract` (testToSQLStringWithCodeBlock and
  whatever else spells `%date->add(^Duration(...))`): three spec PROGRAMS
  in the PLATFORM namespace (dateExtension.pure:507-520,
  `$date->adjust($duration.number, $duration.unit)`). The corpus refuses
  platform-namespace elements from reference sources (Runner guard), so
  they need a platform-owned Pure-text library owner (SystemMetamodel's
  SOURCE carries getLowerBound/allNodes today — a metamodel owner; a
  date program does not belong there). Leg: `builtin/PlatformLibrary`
  Pure text compiled like SystemMetamodel.source(), with spec citations.
- relation accessor on a VIEW (testRelationStoreAccessorOnView):
  TableReferenceChecker resolves `#>{db.personView}#` as a table and the
  lowering emits `FROM personView`. ViewRelation.viewRelationExpr builds
  the view's relation expression but takes a LegacyMappingDefinition
  (diagnostics + JoinChainEmission's class-typed arm) and a ModelBuilder.
  Leg: give the expansion a mapping-less owner (diagnostic context), then
  the checker synths the view relation for an accessor on a view.
- testJoinFunc (+4 in the "not a known class" bucket): `TestClass` in
  meta::pure::tds::toRelation resolves nowhere in the spec checkout
  (no such class in that package; the file imports only join::* and
  toRelation::*) — an engine oddity, or the bare name resolves through
  a package we do not model. Census the 5 before touching.
- testFirstNotNull: `[TDSNull, 1, 2]->firstNotNull()` — the generic T of
  firstNotNull stays unbound at lowering (the collection's element type
  over a TDSNull cell + Integer). Typer binding leg (the lub of the null
  cell and a primitive is the primitive).
- testExtendDigest_InMemory (+2 unbound `_nrN`): extendWithDigestOnColumns
  (tdsExtension.pure:209) inlines `toStringForColAccessor($col)->eval($row)`
  — a function-valued helper over a pair list; the α-renamed binder
  escapes its scope in the deferred colspec. Inliner leg.
- testSortQuotes (+1): assertEquals INSIDE a forAll lambda body over a
  spelled enum list — the verdict is statement-level only; an unroll of
  forAll over a spelled list into its bodies is the leg.
- testSQLComments: RelationalActivity.comment's executionTraceID —
  execution activity metadata; low value.
- testEnumTheSame's second assert (`EmployeeType.CONTRACT == $map1`)
  declines the sql-verdict ("enum kind has no literal channel", V7
  batch 2 decision) and is judged by the canon channel — an enum
  literal channel is a verdict-quality leg, not a pass.
- Dynamic mapping compilation (getNoArgFlattenMapping compiles a
  mapping from a STRING), FunctionExpression reflection
  (testRoutingContextBuilderFunctions `$fe.func`): decisions.

**Batch 57 — the mechanical type walls burned one by one (2026-09-04):
ratchet 243/2330 → 241/2332, chain GREEN 6m05s (docs/GATES.md batch 57
has the full per-wall ledger).** The user's question that shaped it: "why
do we keep skipping the supposedly easy mechanical ones?" — answered by a
one-pass census of all 78 type-related walls (LL_TMP_DEBUG over the whole
corpus prints every wall's message and stack in one run; never probe them
one JVM at a time). Verdict after landing: of the ~30 called mechanical,
2 flipped (repeat), ~12 landed as spec-verified truths that moved their
test to the NEXT honest wall, and 5 turned out not to be mechanical
(createTempTable K-arm, toSQLString/8 needs Format+DebugContext, the
4-arg loadCsvToDbTable has no spec declaration in the checkout,
createDbConfig returns a CORPUS class, asserts inside map/forAll need a
statement unroll).

**NEXT SESSION OPENS HERE.** The remaining census is design legs and
decisions (the 2026-09-04 breakdown in this handoff, plus GATES batch 57's
"where each probed wall went"). Ranked by tests per design: lineage
scanRelations tree print (21), the union family (9 Assert-failed), the
platform Pure-text library owner (Date add/subtract with Duration,
contextHasFlag, isExecutionOptionPresent — ~8), harness import-scope
module pull (6), resolver class-query shapes (7), the extension VALUE
leg (connection equality 5, now at the lowering's match over
extension-contributed arms), the digest MD5-input spelling (3 tests run
end to end; compare with the engine's H2 SQL for the joined string).
Decisions for the user: objectReferenceIn 7, routeFunction 5, hNversion
7 (+2 that now reach it), TDG chained fetch 12, dynamic mapping
compilation 4, protocol transforms 2, functionReturnType 1.

**Batch 58 — the H2VERSION decision LANDED (2026-09-04): ratchet
241/2332 → 234/2339, chain GREEN 6m07s (docs/GATES.md batch 58).** The
whole hNversion bucket is gone: the version probe answers the referee's
jar level at the raw-SQL boundary, and the helper's if-with-assert-
branches is a verdict form. One real divergence surfaced
(testDateFunctionInMilestonedPropertyWithMilestonedEntity: golden 0 rows
on H2 vs ours 2) — probe it next as a semantics bug, not a wall.

**Batch 59 — the lineage-tree ROW verdict LANDED (2026-09-04): ratchet
234/2339 → 213/2360, chain GREEN 6m01s (docs/GATES.md batch 59).** The
21 walk-carried lineage tests are platform verdicts now: both tree
prints become rows through one database query and compare as rows
(LineageTreeVerdicts beside SqlTextVerdicts). User rulings recorded: a
normalize-the-golden-then-byte-compare policy is NOT acceptable for a
platform verdict, however well counted — the golden must be brought to
ROWS by a referee, as the SQL-text charter does; the walk's
LineageRelationsForm (regex breadcrumb strip) is now deletable with the
walk. Remaining walk-carried fallbacks: 55 (the 76 minus these 21).

**Union-family probe receipts (2026-09-04, after batch 59; no batch):**
of the walk-carried union tests, the "Assert failed" ones (testChainedUnions,
testProjectThroughAsso, testProjectThroughAssoWithJoinInMapping,
testUnionWithSinglePropertyMapping, testUnionOnViewsMapping) fail ONLY on
`assert($result2->sql()->contains('union_gen_source_pk_0'))` — a text
assert on an ENGINE-INTERNAL generated column name (the removeUnion/
importDataFlow post-processor); their row asserts pass. The two bitemporal
union tests likewise assert `contains('"lake_thru_0"')` on our SQL text.
The walk passed all seven as "advisory by policy" (EngineTestExecutor:
a golden-SQL read anywhere in the assertion). DECISION for the user:
SQL-text `contains(<engine-internal spelling>)` asserts have no rows
form — either a counted text-census (the charter's census lane, adv
ceiling +7) or a design leg that emits the engine's post-processor
spellings. testRestrictOnGroupByEleminatesUnnecessaryAggsWithDistinct
asserts our SQL contains no `max(` after a restrict — ours still computes
the unused aggregate 5×: a REAL optimizer leg (prune unused aggregates
under restrict/project), text assert about our own emission.
testSQLQueryMergingForInnerJoins ×2: `rows.get(col)` column vectors of an
unordered union compared against a literal with `^TDSNull()` elements —
two probes (a rows-column-vector order rule; a spelled-TDSNull sentinel
on the expected side) were neutral on the corpus and REVERTED; the
remaining mismatch is one element of the expected literal's wire order.
testUnionTwoRelationMappings_ManyColumnProject ×2: the engine prints
`null` for firstName under unionTwoRelationMappings while the fixture
has firstNames — read the mapping (is firstName unmapped in the relation
mapping?) before touching the print.

**Batch 60 — the ASSERT LEDGER LANDED (2026-09-04, chain GREEN 6m03s;
docs/GATES.md batch 60).** The truthful per-assert accounting the user
asked for: docs/RELATIONAL_CORPUS.md "### assert ledger" — clean tests
count at the test level; partial/failing tests show one row per assert
(pass / sql-text-assert / referee-cannot-replay / decision:<name> /
wall:<owner> / divergence / zero-assert / not-reached). USER RULINGS:
never call a text assert or a zero-assert test a "decline"; name the
bucket. The burn list now reads straight off the ledger: 49 divergences
(real wrong answers) and 108 walls by owner are the platform work; 30
decision rows carry the user's names; 7 sql-text-assert + 16
referee-cannot-replay are the referee/contract legs.

**Batch 61 — acos/asin as the engine's bare spec cell (2026-09-04, chain
GREEN 6m04s; docs/GATES.md batch 61).** Ratchet 213/2360 → 211/2362
(testFilterUsingArcCos/ArcSinFunction). The Scalars rule raised the
interpreter's "Unable to compute acos" in SQL; the engine's relational
spec cell is bare `acos(%s)` (H2: NaN, row drops). Rule = the plain trig
family; the DuckDB dialect's domain guard (DuckDb.call, goal #18) yields
NaN where DuckDB would raise. PCT: the two Pure error tests become
expected failures with the engine's own relational precedent (h2 manifest
"No error was thrown"; channel B essential floor 316 → 314, deliberate).

RECEIPTS this stretch (bring to the user, not burns):
- testDateTimeInclusiveRangeQuery: golden 2 rows, H2 gives 1. Engine
  literal = nine digits (DateFormat 'S' with count>=3 appends the whole
  subsecond; client sends `$d->toString()`); relation fixture stores
  '2014-12-04 15:22:23.123' in TIMESTAMP(9); H2 2.1.214:
  `TIMESTAMP '…23.123456789' <= x` false, `…23.123456` false, `…23.123`
  true. The golden can only hold with a millis literal — not the
  engine's spelling. Bucket stays divergence (golden-vs-H2 skew).
- testHashFunctions / testToSQLStringForTDSStringJoin / digest ×2: the
  engine renders `joinStrings([a,b], sep)` as `concat(a, b, sep)` (the
  separator APPENDED — md5('PeterSmith|') = ee0af362… is the golden's
  digest); Pure semantics say 'Peter|Smith'. Reproducing the golden means
  emitting the engine's mis-rendering. DECISION for the user.
- testInExecutionWithTempTableForDateTimesWithTz: the ONLY fallback using
  `testRuntime('US/Arizona')`; ConnectionFlags.timeZoneOf reads an inline
  DatabaseConnection(timeZone=…) only — the string overload's call
  (relationalSetUp.pure:1218) is invisible, so literals are not shifted.
  NEXT LEG (small): read the call's own String argument, like the Boolean
  overload; verify with -Drcorpus.test.
- testRelationStoreAccessorOnView: `#>{db.personView}#` lowers to `FROM
  personView` (TableReferenceChecker finds the view as a table; the
  lowering has no view expansion). ViewRelation (normalizer) expands
  views for class mappings; the typed accessor path needs the same.
  Both asserts are then text contracts (executeLegendQuery JSON contains).
- Scoped runs: `-Drcorpus.only=<family substring>`; a TEST name needs
  `-Drcorpus.test=<name>` (the family filter selects nothing otherwise).
  The interactive shell's `grep` is a snapshot function that drops
  matches — use /usr/bin/grep.

**Batch 62 — the join chain's terminal column (2026-09-04, chain GREEN
5m50s; docs/GATES.md batch 62).** Ratchet 211/2362 → 210/2363
(testIsolatioWhereNoConstaintsAndInnerJoin). Engine rule
(resolveJoinElement): a `@J > @J | table.COL` terminal is re-resolved
in the joined cursor; ours read the spelled table (the root) and lost
the fan-out. RelOpTranslator.joinNavigation rebases terminal columns
the chain end declares (Pipeline.aliasToTargetColumns, recorded at
hoist time in JoinChainEmission); undeclared columns stay where spelled
(the view-hop witnesses testView/testViewWithJoinsAndDistinct broke on
the unguarded cut). Known gap: an `(INNER)` hop inside a chain still
emits LEFT OUTER (engine: LEFT OUTER to an inner-joined isolated
subselect) — rows agree on the fixture; a witness will name it.

USER RULING (2026-09-04): batch 61's PCT trade is kept. Surface any
PCT-affecting trade BEFORE committing (memory:
lane-seam-relational-precedent).

NEXT (measured, small): the three enum positional tests
(testProjectWithIfWhereBothSidesUseTheSameEnumMapping,
testProjectWithIfWhereOneSideIsEnumLiteral,
testProjectionWithEnumThroughAssociation) assert rows->at(i) over
`Product ⋈ Product_Synonym` with no sort; H2 emits product scan order
then synonym scan order (fixture 11→P1, 12→P2, 13→P1 ⇒ (P1,11),(P1,13),
(P2,12)). ScanOrder's key is the driving table's rowid only and only
over join trees with a SUBSELECT frame; the leg = a lexicographic key
(root rowid, then each joined base table's rowid in join order) and
plain-table joins in scope — engine-corpus-compat pass only
(StableScanOrder), measured by the full run.
Other receipts: testMultipleJoinsInPropertyMappingWithDatesInClass —
our execute SQL matches the golden (6 rows) but the assert side
re-derives `$result.values.tableProperty` as a fresh class query
(ResultEnvelopeSplice.valuesRead splices .values into the chain) whose
join pruning drops the fan-out (3): a navigation over an EXECUTED
result must keep the executed row set — a verdict-seam leg.

**Batch 63 — the joined table's scan order (2026-09-04, chain GREEN
6m02s; docs/GATES.md batch 63).** Ratchet 210/2363 → 207/2366 (the
three enum projection rows->at(i) tests). ScanOrder.ordered's key is
lexicographic over the join tree's base-table scans in join order and
plain-table joins are in scope (H2 nested loop vs DuckDB hash join).
Engine-corpus-compat only (StableScanOrder flag); the assert boundary
(CanonicalRenderSql) uses the same key. Nothing else moved.

NEXT candidates (each needs one -Drcorpus.test scoped run with
LEGEND_LITE_DUMP_SQL=1): testSimpleMappingQueryWithFilterInProject
(to-many filter inside a relation-mapping project — 5 expected rows with
TDSNull), testChainedJoinsWithUnionsAndIsolationWithProjectionQueryTableFilter
(Binder: t5 not found — an alias-scope bug in our union+isolation
emission), columnValueDifferenceWithoutPrevalTest (relational vs
in-memory TDS extension; #4 rendered-CSV row diverges), the union
relation-mapping print ×2 (engine golden prints firstName BLANK though
the fixture has names — read before touching), testRelationStoreAccessorOnView
(view expansion on the typed accessor path).

**Receipts after batch 63 (2026-09-04, probes stopped per the 3-probe
rule; nothing landed):**
- testSimpleMappingQueryWithFilterInProject: the golden encodes an ENGINE
  BUG — `$x.firm.employees->filter(e|$e.age < 35).firstName` lists
  employees exactly when the ROOT person is under 35 (John 30 → John;
  Oliver 26 → Fabrice 45 + Oliver; Fabrice 45 → none; David 52 → none;
  fixture relationMappingSetup.pure persons). Ours (Fabrice → Oliver,
  Oliver → Oliver, John → John, David → null) is Pure's semantics.
  Bucket: engine-golden-bug. A cap-key probe (ScanOrder keying a bare
  capped scan) was neutral for it and REVERTED (no witness).
- testChainedJoinsWithUnionsAndIsolationWithProjectionQueryTableFilter:
  DuckDB "Referenced table t5 not found (candidate t4), LINE 16" on the
  executed statement (dumped via [sql-fail]); the SAME text replays
  clean on duckdb_jdbc 1.4.4.0 and 1.5.0.0, prepared statement, with the
  fixture DDL (job tmp binder.sql). The difference lives in the harness
  session (settings / seeded table shapes) — next: dump `SHOW TABLES`
  and `SELECT * FROM duckdb_settings()` from the failing session.
- testUnionTwoRelationMappings_ManyColumnProject ×2: fixture stores
  firstName_s1 = '' (testUnion.pure myDB); our SQL is right and our
  actual prints blank; our EXPECTED side prints `null` (legend-pure's
  TDS literal maps '' and 'null' cells to NULL — TDSExtension
  CsvSpecs.nullValueLiterals — and Render prints a NULL cell as `null`).
  The engine passes, so its `#TDS` print of a NULL cell and of '' must
  coincide (blank). Before changing Render's `null` spelling, find one
  PCT relation expectation that prints a null cell (the DuckDB PCT suite
  passes with the current spelling — a witness must exist or not).
- testInExecutionWithTempTableForDateTimesWithTz: the runtime time zone
  reaches only the plan-text dialect (planDialect/EngineStyleH2); the
  executing lowering never shifts date literals. Leg: thread the
  connection's timeZone from the execute site into the DuckDB emission
  and shift DateTime literals (engine convertDateToSqlString with
  dbTimeZone) — one witness.

**Union relation-mapping print — TRACED (2026-09-04, supersedes the
receipt above):** the engine's relation toString (core_functions_relation
toString.pure) prints a cell through s.pure: an EMPTY cell prints the
word `null` (`'null'` quoted for Variant) — exactly Render's rule, so
Render stays. The TDS literal's blank cell is NULL (legend-pure
TDSExtension CsvSpecs nullValueLiterals '' / 'null'); the engine's test
H2 runs `MODE=LEGACY` (H2Manager defaultH2Properties) where an inserted
'' stays '' (jshell, h2-2.1.214: mem default '', MODE=Oracle NULL,
MODE=LEGACY ''); RelationalResult/ResultColumn read VARCHAR with
getString unchanged. So the engine itself would print `Anand,,…` against
an expected `Anand,null,…`. The golden is not explainable by the code
read — either not green upstream or a conversion not found. Bucket stays
divergence; do NOT touch Render for it. Also checked: the engine's H2
PCT manifest fails the joinStrings PCT tests for an unrelated reason
(toVariantList translation), so the digest family has no manifest
precedent — it remains the user's decision.

**Batch 64 — chained generator fetch as a ROW verdict (2026-09-04,
chain GREEN ~6m; docs/GATES.md batch 64).** Ratchet 207/2366 →
197/2376 (+10 chained testDataGeneration tests). USER DIRECTION after
the one-by-one review of the walk's pass mechanisms: delete the harness
code that does the platform's job — first make the goldens the referee
CAN replay into row verdicts at the platform seam. The walk's
tdgChainedVerify moved behind the oracle SPI (verifyFetchChain: hop
address from `$testData.sqls->at(i)` + the let-bound generator node;
ancestor temps from the attempt's remembered goldens; transcript rows
multiset-compared). Lane pins moved as migration (exec-passing 55→21,
rescued floor 52→18, ledger SqlTextVerdicts 690→765).

THE WALK'S PASS MECHANISMS (traced in EngineTestExecutor, 2026-09-04):
scoreAssert counts ADVISORY_MARKER and "sql-text:" divergences as
advisory, never failures; Runner.score passes a test with ≥1 verified
assert, or 0 asserts + executed statements; a byte-equal golden text
whose H2 replay is unavailable RETURNS NULL = counted verified
(sqlTextVerify "match-noreplay"); `assert(sql->contains(..))` evaluates
the predicate over our engine-style text = "verified". The 71
walk-only passes (of 207 fallbacks at batch 63): 26 text-policy, 34
Java forms (ObjectRefs = port of generateObjectReferences,
ConnEquality = port of storeContract equality, TestDataGenForm,
PlanAsserts/ElqSplice), ~4 real platform gaps, ~5 unclassified.
CORRECTION: the TDG chained fetches and the tempTableForIn goldens
WERE row-verified in the walk (tdgChainedReplay; extraSeeds) — the
platform seam lacked the plumbing, batch 64 moved the first.

NEXT in this leg (user go 2026-09-04): (c) numbered
`tempTableForIn_N` goldens (3 tests: values from the query's typed
in([...]) literal → a structured temp spec across the SPI; the oracle
spells the H2 DDL as the walk's literalTempSeeds did — TIMESTAMP/DATE/
VARCHAR(1024)/BIGINT); (d) compare on the golden's columns when our
frame projects more (graph keys mismatch, 1); (e) value frames for
forced-isolation goldens (2); (b) the population-golden temp
(`tempTableForIn_<var>`, 2 tests) is a two-statement engine plan vs
our one-statement plan — golden(0) has no counterpart; receipt, not a
burn. Then delete the walk's match-noreplay return and the forms one
by one; then objectReferenceIn and connection equality as Pure
programs over the metamodel relations.

**Batch 65 — the inline in-list temp table as a ROW verdict (2026-09-05,
chain GREEN ~6m; docs/GATES.md batch 65).** Ratchet 197/2376 →
193/2380. SqlTextVerdicts.inListTemps + SqlReplayOracle.TempTable +
ReplayOracle.tempSeeds: the walk's literalTempSeeds behind the SPI. The
frame's query reaches the rows leg via FrameFacts.query (the exec-read
arms' letPrefix is EMPTY — the frame is spliced; do not search
letPrefix for frame queries). Receipts by design: population-golden
temps (2), forced-isolation value frames (2), graph-keys over-fetch (1).

TALLY of the 26 text-policy walk-passers (batch 63 census): 13
recovered as row verdicts (10 chained TDG in batch 64, 3 in-list in
batch 65; a 4th in-list test that had counted as row-diverged flipped
too). Remaining 13: testQualifier (hop-0 golden spelled
sqlRemoveFormatting('literal') — small), population-golden 2 + forced
2 + graph-keys 1 (receipts), rows-diverged 3 (joinStrings ×2 = user
decision, firstDayOfWeek), `assert(sql->contains)` 6 + plan text 1 +
assert-free 2 (truthful buckets, never rows). NEXT = batch 66: delete
the walk's match-noreplay verified return and the contains-over-our-
text verified pass (EngineTestExecutor.sqlTextVerify / the assert arm)
— a deliberate scoreboard drop; the per-family baseline in
docs/RELATIONAL_CORPUS.md is the gate (readBaseline) and must be
re-baselined by hand with the note; then the forms (ObjectRefs →
objectReferenceIn as a Pure program; ConnEquality).

**Batch 66 — the golden PLAN replayed node by node (2026-09-05, chain
GREEN ~6m; docs/GATES.md batch 66).** Ratchet 193/2380 → 190/2383.
harness/PlanReplay (behind SqlReplayOracle.verifyPlan): Allocation
nodes (Constant literal / Relational rows fetched on the oracle) bind
the holes; the engine's template helpers evaluate by their published
bodies; the final SQL replays. VerdictQueries.refereeBindings binds
[*] String/Integer params (two elements, `lists`); the plan lambda's
leading lets scope our rows leg; tdgHop sees through
sqlRemoveFormatting(String). Receipts in the charter batch-66 entry.

ONE-BY-ONE ORDER AGREED (2026-09-05, user: "go one by one … we never
resolved assert free"): next batch 67 = (1) query-chaining golden(1)
replay (materialize tempTableForIn_<var> from golden(0)'s rows, attempt
cache; golden(0) itself = decision:plan-structure, no counterpart
statement on our one-statement plan); (2) forced-isolation VALUE frames
through the existing golden-side NULL-drop compare (H2Verify
FORCED_MECHANISM guard throws before reaching it; the worry was
strategy TEXT, rows are sound); (3) testToSqlGenerationFirstDayOfWeek
= a POSTGRES golden replayed on H2 (H2 DATE_TRUNC week starts Sunday:
golden 2014-11-30 vs ours/Postgres Monday 2014-12-01) — route the
explicit non-H2 DatabaseType to the foreign-dialect text residue;
(4) assert-free: bucket stays zero-assert; the platform lane runs the
body's statements (a clean run = zero-assert pass under the 0-assert
ceiling; inert = named zero-assert:inert row, not a fallback);
(5) graph-keys over-fetch (testQueryOfMilestonedTypeWithFilterInMapping):
compare on the golden's columns + an `over-fetch` census bucket —
POLICY, needs the user's yes. Then batch 68 = the deletion of the
walk's byte-match-without-replay and contains-over-our-text passes.

**Batch 67 — the two-statement in-list plan as rows; assert-free bodies
on the platform (2026-09-05, chain GREEN ~6m; docs/GATES.md batch 67).**
Ratchet 190/2383 → 187/2386. golden(0) = the let's value (rows leg =
the let expression wrapped in the frame's mapping); golden(1) = the
population temp filled from golden(0) at the oracle (attempt cache
ATTEMPT_SQL_GOLDENS). Assert-free bodies run through the platform and
score "N statements executed". Forced-isolation guard restored with
the measured reason. USER RULINGS this stretch: (1) firstDayOfWeek —
Pure normalizes; the engine's H2 dialect fails to (Sunday); ours is
Pure's Monday; (2) joinStrings — the engine's rendering is wrong on
every dialect; (3) both → an `engine-golden-defect` ledger bucket with
receipts, still failing on rows; (4) over-fetch → FIX it (batch 68:
implicitly inherited PMs excluded from the whole-instance projection —
StoreResolver's bindings iterations ~923/1109 — and served on demand
through the ancestor set; ImplicitInheritance records the inherited
names; witness testQueryOfMilestonedTypeWithFilterInMapping);
(5) then batch 69 = the deletion of the walk's text-only verified
passes (re-baseline families by hand with the note).

**Batch 67b — the `engine-golden-defect` ledger bucket (2026-09-05, chain
GREEN; GATES batch 67b; charter §8.0 receipt).** Ratchet unchanged
187/2386. AssertLedger.ENGINE_GOLDEN_DEFECTS (exact FQN, rows-differ
only): joinStrings-rendering ×3, h2-week-start ×1. testHashFunctions
stays `divergence` (hashes firstName + lastName; 7 golden rows vs ours —
probe with LEGEND_LITE_DUMP_SQL=1 -Drcorpus.test=testHashFunctions).
Residue after batch 68 (user asked): contains-over-text 11 (6 with
passing row asserts first + 5 plan/legacy-flag), foreign-dialect text
5, engine-defect candidates with receipts not yet registered 2
(filter-in-project, DateTime 9-digit literal), referee-cannot-replay 14
(SELECTTOP1/missing-schema goldens 3, forced value frames 2,
unformatted plan text 4, graph-keys 3 → batch 68, arity 2).

**Batch 67c — testHashFunctions traced (2026-09-05).** Its `tds_digest`
column is joinStrings('|')->hash(MD5); the golden renders the trailing
separator (md5('AnthonyAllen|') vs ours md5('Anthony|Allen')); all other
cells agree, lowercase hex both sides. Registered under
`joinStrings-rendering` (now 4 tests + the digest pair). Hash needs NO
platform work.

**Batch 68 — the instance over-fetch FIXED (2026-09-05, chain GREEN ~6m;
GATES batch 68; charter §8.0 receipt).** Ratchet 187/2386 → 185/2388;
unable-to-exec 11 → 9. Engine rule: a set projects its OWN property
mappings; implicitly inherited ones are served on access.
DeclaredKeys.ownProperties (SetKeyFacts, pre-merge) → ClassSources
.ownPropertiesOf (extends-less relational bindings only) →
GraphEmission.synthesizeScalarTree skips the rest. NEXT: batch 69 =
delete the walk's match-noreplay and contains-over-our-text verified
passes (EngineTestExecutor), re-baseline families by hand in
docs/RELATIONAL_CORPUS.md with the GATES note; small platform leg to
carry: the two group-by-with-filter-function arity declines (golden 10
columns vs frame 4). USER Q&A this stretch: the 26-row table was
walk-relative (walk PASS ∩ platform fallback ∩ text mechanism, batch
63); the 211-row ledger is every failing ASSERT — 11 contains rows (6
with passing row asserts first + 5 plan/legacy-flag) have no row form
and stay text-contract rows; of the 12 unreplayable, ~3 worth a look
(arity 2, missing schema 1), 9 are squashed/unformatted text or the
measured forced frames.

**Batch 69a — fix-before-delete, the walk-passing SQL-lane residue
(2026-09-05, chain GREEN ~6m; GATES batch 69a; charter §8.0 receipt).**
USER ORDER: fix the 11 (4 unreplayable, 5 rows differ, 2 odd) BEFORE
batch 69's deletion. Ratchet 185/2388 → 181/2392. Fixed: union
sqlQueryMerging ×2 (^TDSNull() instance + JSON-null value law),
ESTTimeZone plan templates (helper lets + parameter let), assert-free
let-execute body. Receipts: alloy-adjust-widening, no-fixture. Truth:
the forced pair = OUR inner-join bug (guard deleted; tests/advanced
re-baselined 66 → 64). datePeriods golden(0) verifies (statement lets),
golden(1) walls in the resolver. OPEN: isolation-family design (INNER
vs LEFT for value-position filtered navigations — USER DECISION
pending), isolationTest's silently dropped correlated predicate,
fetchDb Function-typed lowering wall. THEN batch 69 = the deletion.

**Batch 69b — isolationTest walls loudly (2026-09-05, chain GREEN; GATES
batch 69b).** Ratchet unchanged 181/2392. The silently dropped
correlated predicate (tail hop of a 3-hop chain whose parent alias a
plain path had already demanded) is StoreResolver.unappliedCorrelatedWall
now. LEG: apply a correlated predicate at depth ≥ 2 (extend the
parent-copy reroute's tail loop past aj.targetSubNavs(), and let a chain
reroute even when its parent alias is already on the spine). The
fix-before-delete order (user): of the 11 — 5 fixed/receipted, forced
pair = honest divergence pending the isolation-family DECISION (task
#17), isolationTest + datePeriods + fetchDb = named walls (task #18).
NEXT: user decision on #17, then batch 69 = the deletion.

**Batch 69c — fetchDb primary keys + datePeriods fixed (2026-09-05,
chain GREEN; GATES batch 69c; charter §8.0 receipt).** Ratchet 181/2392
→ 179/2394. ClassLayouts skips Function-typed properties; CatalogGrids
finds the PK grid's database through the typer's let channel;
SubQueryLift lifts let-bound instance reads, runs on the execute route,
stops at a TypedFrom; SqlTextVerdicts strips the chained-plan warning
and the toSQLString arm takes multi-statement lambdas. The
fix-before-delete order is COMPLETE except the isolation family: NEXT =
the isolation leg (task #17, user-approved rule: join kind follows the
mapper body's per-parent multiplicity — LEFT + keyed subselect when the
body reduces to one value per parent; the forced pair pass, the two
default structure goldens → engine-golden-defect; isolationTest's
depth-3 correlated predicate rides the same keyed subselect), THEN batch
69 = the deletion. METAMODEL-STORE LEG (user Q): key facts on Column
rows / a table_keys relation + Runtime/ConnectionStore rows; the PK grid
= one SQL over the store existence-filtered against information_schema.

**Batch 70 — the isolation join rule + the toOne correction (2026-09-05,
chain GREEN; GATES batch 70).** Ratchet unchanged 179/2394. Value
position: INNER when the body flattens or the read is toOne-narrowed;
LEFT when a BARE many read is reduced by the body (witness
ValueMapPlacementTest.bareManyReduceKeepsParents). The forced pair =
decision:empty-toOne-forced-isolation (pure: `[]->toOne()` errors; two
engine conventions); the default structure goldens are not defects.
SPEC GAP: `String[*] + String[1]` (plus(strings:String[*])) does not
type — typer leg. OPEN: isolationTest depth-3 correlated predicate.
NEXT: batch 69 = the deletion (the fix-before-delete order is
satisfied: 5 fixed, 2 legs fixed in 69c, 2 receipted, forced pair =
named decision, isolationTest = named wall).

**Batch 71 — fetchDb primary keys the engine's way (2026-09-05, chain
GREEN; GATES batch 71).** Ratchet unchanged 179/2394; 0 failed seeds.
The native emits the engine's constraints (quoted key names per
flavor); the PK grid is a live-catalog query; the model fact-walk and
the typer's db lookup are gone; CsvSeed stays unconstrained. Rulings:
fetchDb* = physical introspection, live catalog is the truth; the store
= what the model declares; no statement reads both stores; no runtime
rows. NEXT: batch 69 = the deletion (measured: ratchet unchanged,
unable-to-exec 8 → 9). THEN the 33 walk-only Java-form/wall tests (the
"Pure programs instead of Java" leg: objectReferenceIn 7, connection
equality 5, routeFunction 5, recursion 2, protocol 2, dynamic 2, ~10
walls).

**Batch 69 — THE DELETION (2026-09-05, chain GREEN; GATES batch 69).**
The walk's three text-only "verified" returns (byte-equal golden
without replay ×2, contains over our own text) are advisory. Ratchet
unchanged 179/2394; no family moved; unable-to-exec 8 → 9. Every
verified assert in the walk is a row or value verdict now. NEXT: the 33
walk-only Java-form/wall tests — objectReferenceIn (7) as a Pure
program first (present the plan: protocol transform is a decision
bucket), connection equality (5), routeFunction (5), recursion (2),
protocol (2), dynamic (2), ~10 resolver/typer/lowering walls; each
retires a Java form in EngineTestExecutor when its platform leg lands.

**Batch 72a — the four small walk-only legs (2026-09-05, chain GREEN; GATES
batch 72a).** Ratchet 179/2394 → 176/2397; exec-passing 9 → 7. Plan and audit
in docs/WALK_ONLY_PLAN_2026_09_05.md (16 walk-only, not 33). Landed: malformed
`]"` goldens registered (engine json-simple tolerance, probed); statement-level
self-alias let re-binds the outer alias (SpecCompiler.typeQueryBody — NOT
Env.withLet, which broke the plan printer's injected lets); statement-root map
over spelled execute bindings unrolls (LiteralMapUnroll, query front door);
relation concatenate positional (USER RULING; ConcatenateChecker; each argument
synthesized ONCE via Typer.checkGenericTyped — double synth re-registers typer
state). ALWAYS pass -Dlegend.engine.root=/Users/neemsandv/legend/legend-engine
(the $HOME checkout is stale: 2536 tests, phantom regressions). NEXT: leg A
objectReferenceIn (8 tests): natives generateObjectReferences[ForGivenSetId]
typed; Substitution.objectReferenceInRewrite reads the literal pkMaps from the
typed generator call (pair/newMap) — the harness ObjectRefs.java JSON-array
carrier goes; decode (WithObjReferenceOutput) = SQL decode + pk-name rows; the
non-literal refs test (UsingResultReferences) = pk membership over a decoded
reference list (DuckDB base64 only — H2 has no base64). Then leg B (connection
equality): fold relationalExtensions()/routerExtensions() to spelled arms,
MatchFold class dispatch, hierarchicalProperties from class property rows.
