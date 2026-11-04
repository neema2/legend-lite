# Core's SQL size, and the system metamodel's path (measured 2026-11-03/04)

Written so the next session does not rediscover it. The user's question (2026-11-03): "why core generating large sql,
I thought on product path the sql was super lean". The user's order of work (2026-11-04): finish core's walkers and
judge differences, then the relational corpus, then turn off every walker and every other door, and only then
optimize all of our SQL globally, **starting with the system metamodel**. This file is the input to that last step.

## 1. How it was measured

A diagnostics switch, `-Dlegend.diagnostics=sql-sizes` (`exec/StatementOrigin.sent`, commit `92de0aae`), prints one line
per statement before it is sent: `[sql-size] <chars> <running test> <origin>` (the origins are
`exec/StatementOrigin`'s: `body`, `statement`, `system`, `seed`, `session`, `referee_ours`, `fallback`, `other`, ...).

```
bazel test --keep_going --disk_cache= --local_test_jobs=4 \
  "--test_env=JAVA_TOOL_OPTIONS=-Dlegend.test.judge=HOST -Dlegend.diagnostics=sql-sizes" \
  //core:core_tests //core:core_tests_lanes
# then, per target:  grep -a '^\[sql-size\]' bazel-testlogs/core/<target>/test.log
```

Aggregate by origin (count, total, median, p99, max) and by test class; `-Dlegend.test.judge=DATABASE` measures the
database judge's lane (`testing/Program.laneJudge`).

## 2. What core sends (host judge, all core targets and the SQLite and H2 lanes, at `92de0aae`)

2,365 tests, 44,571 statements, **67.0M characters**.

| Origin | Statements | Characters | Share | Median | p99 | Max |
|---|---|---|---|---|---|---|
| `system` -- the system metamodel store's seed (DDL + every row) | 25,663 | 57.0M | 85% | 51 | 73,484 | 177,369 |
| `statement` -- a body's non-assert statement (mostly metamodel queries) | 379 | 3.9M | 6% | 718 | 84,442 | 84,444 |
| `other` -- unattributed | 10,263 | 3.3M | 5% | 112 | 3,507 | 76,623 |
| `referee_ours` | 122 | 1.2M | 2% | 92 | 28,774 | 28,774 |
| `body` -- the program's product statements | 459 | 1.0M | 1.4% | **1,921** | **9,353** | 20,384 |
| `seed`, `fallback`, `session`, `let`, `tdg`, `side`, `value` | ~7,700 | 0.7M | 1% | small | | |

Under the database judge alone: 33,407 statements, 35.2M characters (fewer tests: the H2 lane dies, section 4), the
same shape -- `system` 26.9M, the largest non-system statements the same metamodel queries (83K).

For scale: PCT's Snowflake channel B essential sends 2,749 statements, 4.37M characters, its largest 55K (round 19).

**Reading it:** an ordinary product query is lean (median 1.9K characters, p99 9K). Core's volume is two things:

1. **The system metamodel seed, re-sent for every test (85%).** Five test classes send all of it --
   `MetamodelMappingStoreTest` 23.4M (9,570 statements), `MetamodelQueryFunctionsTest` 19.3M (8,899),
   `MetamodelStoreSubstitutionTest` 8.2M (3,652), `MetamodelStoreTest` 4.5M (1,968), `BodyRouterParityTest` 1.5M
   (860); `DeclaredQuotedTableH2Test` and `RowLoadDuckDbTest` a little. Each test opens a fresh session, and the
   session is seeded with the WHOLE compiled model's metamodel rows, platform classes included
   (`planner/SystemRoutes.seeds` -> `MetamodelSeeds.rows` per table -> `setup/Ddl.metamodelSteps`): its DDL, then one
   INSERT per table, the largest 177K characters. It is data loading, not query SQL.
2. **Queries over the metamodel are genuinely bloated (product-path SQL).** The 84K statements are
   `MetamodelQueryFunctionsTest#propertyMappingInference` and `#viewColumnInference`: ONE query nested 41 derived
   tables deep (`t0` .. `t41`), every level re-projecting every column under a long generated path alias (about 100
   characters each, e.g. `relationalOperationElement_inferredType_stc_meta__relational__metamodel__datatype__Integer___$member`),
   plus a CASE over each datatype subtype (`... IS NOT NULL THEN 'INT' WHEN ... THEN 'FLOAT' ...`). A probe dump of
   one: 94,756 characters, 248 lines.

## 3. Is the system metamodel on the same path as any other model?

The intent (WORLD_MAP; the metamodel in Pure was to dogfood the whole pipeline) holds for **SQL generation**: the
lowering (`com.legend.lowering`) and the SQL layer and dialects (`com.legend.sql`) name the system metamodel nowhere
(grep for `SystemMetamodel`, `SystemRoutes`, `MetamodelSeeds`, `isSystemElement`: zero files in `lowering`, none
generating SQL in `sql`). A metamodel query is compiled, resolved, lowered and rendered exactly as a user model's.
**So the 84K query is the generic lowering's output for a deep mapping with subtypes** -- a user model with the same
shape gets the same SQL. That is the dogfood doing its job: it found a general SQL-quality problem.

Where the metamodel IS special (each a deliberate rule, each worth re-reading when optimizing):

| Where | What | Files |
|---|---|---|
| Its data | the rows are computed in Java from the compiled model and INSERTed into a separate system database, per session (the 85%) | `planner/SystemRoutes` (`seeds`, `renames`), `MetamodelSeeds`, `setup/Ddl.metamodelSteps`/`metamodelTemporary`, `exec`/`driver` `SystemDatabase` |
| Resolver routing | with no `from()`, a metaclass dispatches to the system mapping ("intrinsic store", D1, `docs/METAMODEL_STORE_HANDOFF.md` §2); under an explicit mapping that does not bind it, too | `resolver/StoreResolver` (`dispatch`, the context default) |
| Constructed scope | a query that constructs metamodel instances reads its system tables as inline rows of the constructed tree (user ruling 2026-09-02) | `resolver/ClassSources.scoped`/`inlineStoreLeaves`, `ConstructedInstances` |
| Element references | a class named as an element reads its metamodel row; identity uses the system mapping's key | `resolver/ElementReferences`, `ChainNormalizer.keyIdentity`, `compiler/spec/UserCallInliner` |
| Order | positional picks over a navigation use the store's ordinal column | `resolver/SyntheticHeads`, `FlattenOps` (`SystemMetamodel.ORDINAL_COLUMN`) |
| Cypher, lineage | element FQNs and the lineage prelude | `compiler/spec/CypherIslands`, `LineageTreeLines` |
| The test road | a system-store read is its own body kind; a body reading only it is "system-routed" to the system database; per-statement routing | `testplan/BodyClass` (`SYSTEM_READ`, `systemRouted`), `driver/BodyRouter`, `driver/*` |

## 4. The H2 out-of-memory it causes

Under the database judge, H2's lane dies in `MetamodelMappingStoreTest#witnessMainTableForB1`: a 6K-character verdict
statement whose two sides are metamodel navigations (mapping -> includes closure -> class mappings -> table alias ->
table), each nested eight derived tables deep inside a CTE. H2 2.1's parser re-reads nested derived tables and runs out
of heap (`Parser.readDerivedTableWithCorrelation` recursing, `Token.clone`). Under the host judge each side is its own
statement and H2 copes. The same shape as finding 2: the fix is the generic lowering's nesting, not an H2 workaround.

## 5. What to do when we come back (after the walkers and doors are gone)

1. **Seed once, not per test.** The seed is Knowledge derived once per graph and engine (`SeedMemo`) but SENT once per
   session; core's metamodel tests open a session per test. Options: one system database per graph per JVM (the
   memo's key extended to the session's database), or seed only the tables a plan reads (lazy Work, `docs/TENETS.md`),
   or both. Measure with `sql-sizes`: `system` should fall from 57M to the size of one seed per graph.
2. **Shrink the generic lowering's output, measured on the metamodel queries:** prune the columns a derived table's
   readers do not read (the `SubselectPrune` pass exists -- find why these 41 levels keep every column), shorten the
   generated path aliases (or alias by position), flatten nested joins where no aggregation or limit separates them,
   and look at the subtype CASE (one column per subtype per level). Target: `propertyMappingInference` from 84K to a
   few K; `witnessMainTableForB1` no longer nested eight deep (the H2 OOM gone).
3. **A size guard for core** (the user's "core test SQL much smaller than PCT/corpus"): a ratchet over the
   `sql-sizes` measurement -- a ceiling per statement for `body`/`statement`, a total for `system` -- that only falls.
4. Then the same across PCT and the corpus (the tagged-list statements, Track L; the judge's canon wrappers).
