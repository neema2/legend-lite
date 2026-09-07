# Two design legs — single-shot execution, and code+metamodel as data

**Date:** 2026-09-07. **Status:** research and design homework. No implementation.

Two large design directions were proposed for the `core_relational` program:

1. **Single-shot** — execute a whole test function (setup, query, and asserts) as one statement
   that returns one boolean row.
2. **Code + metamodel as data** — represent the program and the metamodel as relations, and query
   them with SQL rather than walking them in Java.

This document answers, for each: *is it realistic, what are the limits, how should we build it, and
which risks can we retire now instead of discovering mid-implementation?*

## 0. Method, and what this is not

Five parallel research threads, each restricted to reading the two checkouts
(`~/legend/legend-lite`, `~/legend/legend-engine`) and the corpus. Constraint honoured throughout:
**no Java, no Maven, no test runs** — the machine was loaded. Every number below therefore comes
from static analysis of source text, and every one is reproducible by re-reading the cited file.

That constraint has a consequence you must carry into any decision made from this document:

> **Nothing here has been executed.** Corpus counts come from a scanner that reproduces the
> harness's discovery count exactly (2,575 runnable), which is good evidence it agrees with the
> harness on *which* tests exist — but it is not the compiler, and it does not resolve names.
> §4 lists every question that needs a machine to answer, so they can be run on an idle box.

Where a claim is an estimate rather than a measurement, it says so. §6 lists hypotheses that were
investigated and **dropped**, so the next pass does not reopen them.

---

## 1. Executive verdict

**Leg 1 (single-shot): realistic, with the boundary drawn at the session rather than the test.**
77.6% of the corpus (1,998 of 2,575 tests) is already exactly one statement after inlining. The
remaining 22% splits into ~315 tests that are *inherently* not single-shot (they assert on compiler
artifacts, not on rows) and ~370 that are incidentally blocked with known, small fixes. Seeding
cannot come inside the shot on any dialect — that is a property of SQL, not of our design.

**Leg 2 (code+metamodel as data): largely already landed, and the part people worry about —
recursive CTEs — is the part we should not build.** The metamodel hierarchies are bounded and tiny
(max class depth 5, mapping-include depth 2, join chain 4), closure tables already exist and are
already the declared policy, and both pinned dialects have serious recursive-CTE defects. Stored
procedures are not available on the target backends, and would violate the execution tenet where
they are. Template-then-fill is a fine *reader* of someone else's format and a bad way to emit our
own.

Neither leg is blocked. Both have a sharp, small experiment that retires their main unknown.

---

## 2. Leg 1 — single-shot

### 2.1 What already exists

The MIR is further along than the design discussion assumed.

| Capability | Status | Evidence |
|---|---|---|
| CTEs in the MIR | **Already there** | `sql/SqlQuery.java:11` — `sealed interface SqlQuery permits SqlSelect, SqlUnion, SqlWith`; `SqlWith.java` is `record SqlWith(List<Cte> ctes, SqlQuery body)` with `record Cte(String name, SqlQuery query)` |
| Set operations | **Partial** | `sql/SqlUnion.java:13` — `record SqlUnion(List<SqlQuery> branches, boolean all, List<OutputCol> outputs)`. A **boolean**, not an operator enum. This is the one MIR change single-shot needs: `boolean all` becomes a `SetOp` (UNION / UNION ALL / EXCEPT / EXCEPT ALL / INTERSECT). |
| Statement-level inlining | **Already there** | `compiler/StatementInline.java` (282 lines) — beta-reduction of user program calls into the caller's statement list |
| The database as verdict of record | **Already there** | `AssertVerdicts.java:941` — `boolean held = byteHeld != null ? byteHeld : hostHeld;` — the DB byte wins when present. Best recorded agreement: **agree 1492, disagree 0, declined 91.** |
| A SQL spelling for `toRepresentation` | **Already there** | `lowering/Repr.java:28` — `static SqlExpr of(Type t, SqlExpr x)` |

So `let` to CTE needs **no MIR work at all**. The single structural change single-shot requires is
the `SqlUnion` set-operator enum.

`StatementInline` has exactly four stop conditions, and they matter to everything below:

- lambda bodies are not entered (`:38`; `hoistIn` returns `LambdaFunction` untouched at `:164-166`)
- cycles leave the inner call standing (`:101`, `:237`)
- verdict functions are never opened (`:192`, `:268`) — deliberate and correct; a trailing assert
  must survive as a statement-root assert for the channel to adjudicate
- natives win over model overloads (`:57-58`), and `resolvedDefinition` refuses to open a native
  (`:206-210`)

### 2.2 The setup-wrapper census — does `setupDbAndLoadCsvAndExecute()` exist?

**Yes, and it is small, shallow, and mostly already unrolled.**

The literal shape is `loadAndTestExecution(query, parametersValues, mapping, runtime, data, db)` at
`testDataGeneration/tests/testDataGeneration.pure:1783` — **30 tests**. It does DDL, CSV load, plan
build, query and boolean in one call:

```
let testConnection = $runtime.connectionStores.connection->toOne()->cast(@TestDatabaseConnection);
let setUpSQLs    = setUpDataSQLs($data, $db);                 // CSV -> SQL strings
$setUpSQLs->map(sql | executeInDb($sql, $testConnection));    // DDL + DML loop
let plan   = executionPlan($query, $mapping, $runtime, relationalExtensions());
let result = $plan->execute($parametersValues, relationalExtensions());
$result.values->isNotEmpty() && (...);
```

How the 2,575 runnable tests reach a platform effect:

| route | tests | % |
|---|---:|---:|
| directly in the test's own body (`execute(...)`, `executeInDb(...)`) | 1,915 | 74.4% |
| directly **and** through a user-space wrapper | 96 | 3.7% |
| **only** through a user-space wrapper | **117** | **4.5%** |
| not at all (session state + compile-time asserts only) | 447 | 17.4% |

So the wrapper concern touches **213 tests (8.3%)**, spread over **28 distinct wrapper names** in
**7 shapes**. Nesting is shallow: **2,352 of 2,575 tests are at program-call depth 0**; the deepest
chain from a test body is 4, and from a `BeforePackage` setup also 4
(`setUp` to `createTablesAndFillDb` to `createPersonTableAndFillDb`).

**`StatementInline` as written fully unrolls 6 of the 7 shapes.**

| # | Shape | Tests | Verdict |
|---|---|---:|---|
| S1 | bulk raw-SQL seeder, zero-arg — `createTablesAndFillDb()` (54), `initDatabase()` (10), `createTablesInDb()`, `createTablesAndFillDbUS()` (4), `setUp()` (4) | ~72 | **Fully unrolls.** Body is `isStatementOnly` to PROGRAM; the one `let connection` renames to `_s1_connection`. Expands to 222 statements (`createTablesAndFillDb`) / 320 (`initDatabase`). |
| S2 | seed-then-return-a-handle — `getConnection()` (11), `createDbAndGetConnection()` (4), `getM2M2RRuntime`, `getXStoreRuntime` | ~17 | **Fully unrolls.** Exactly the case `StatementInline:131-140` was written for ("a let-bound call: the callee's value is its last statement"). |
| S3 | CSV to generated SQL to **dynamic loop** — `setupTestData(csvs, db, runtime)` | 48 | **Splices, does not unroll.** `$sqls->map(sql \| executeInDb($sql, $connection))` where `$sqls` came from `setUpDataSQLsV2` on CSV text. Trip count and SQL text are runtime values. |
| S4 | the literal `setupDbAndLoadCsvAndExecute` — `loadAndTestExecution` | 30 | **Splices 5 of 6 statements**, but carries S3's dynamic loop and a plan handle. |
| S5 | query-and-assert wrapper, no setup — `testFirmAgg` (9), `runLegendTest` (4), `runTest`, `testTds`, `runQuery`, `runGraphFetchTest`, plus ~10 one-offs | ~30 | **Fully unrolls**, and the trailing verdict is correctly *not* opened. |
| S6 | runtime-AST-synthesising — `meta::relational::validation::validate` | 39 | **Not a blocker.** Already desugared at the front door by `com.legend.validation.ValidateDesugar` (`Compiler.resolveQuery:686-689`). |
| S7 | the Alloy shell — `mayExecuteAlloyTest({clientVersion, serverVersion, host, port \| ...})` | 28 | **Nothing unrolls.** It is a registered native (so `resolvedDefinition` returns null) *and* the whole test lives inside a 4-parameter lambda argument (so `hoistIn` refuses to enter). Opaque. |

**Two false-positive categories** that inflate any naive wrapper count, and were excluded:

- **Platform natives.** `relationalExtensions` (2,341 tests), `toSQLString` (79), `scanRelations`
  (49), `planToString` (91), `executionPlan`, `assertEqualsH2Compatible` are `native function`
  declarations in `builtin/Pure.java` (482 unique native FQNs). A naive transitive-call analysis
  reaches `executeInDb` through them **because the corpus tree contains the engine's own
  compiler** — `pureToSQLQuery.pure:215` to `relationalMappingExecution.pure:710 executeInDb` is an
  18-hop chain never taken at runtime. `StatementInline` correctly refuses to open natives.
- **Effect-free "programs".** `getTable` (7 tests, `helperFunctions/toDDL.pure:220`) is a pure
  helper classified PROGRAM only by the statement-sequence rule (a `fail()` guard before the last
  statement). Splices harmlessly. 3 names, 10 tests.

**Setup mechanisms are 95% straight-line literal SQL.** Across the 111 `BeforePackage` functions
(1,021 own statements): `executeInDb` 770, `dropAndCreateTableInDb` 61, `createTablesAndFillDb()`
56, `let ... cast` 34, trailing `true` 34, `initDatabase()` 19, `setUp()` 12,
`dropAndCreateSchemaInDb` 3. There is exactly **one** `loadCsvToDbTable` test in the corpus
(`functions/tests/loadCsvToDbTable/testLoadCsv.pure:21`).

**One finding that is not about single-shot at all:** the stereotype histogram is `test.Test` 2512,
`meta::pure::profiles::test.Test` 209, and **`paramTest.Test` 151** — a third profile the harness
does not discover. Those are connection-parameterised tests in `tests/semistructured/*`, each
delegating to `semiStructuredExecute($conn, funcName, expectedCsv)`. Out of scope today; they would
land in scope the day that profile is admitted.

### 2.3 What remains after full inlining

Inlining every user program into every test body yields **33,808 statements** across 2,575 tests.

| statement kind | count | tests | fits in ONE SQL statement? |
|---|---:|---:|---|
| **DDL/DML effect** (`executeInDb`, `dropAndCreateTableInDb`, `dropAndCreateSchemaInDb`, `loadCsvToDbTable`, `setUpDataSQLs`) | **21,676** | 132 | **NO.** `CREATE`/`DROP`/`INSERT` cannot be a CTE or a subquery. A statement that both creates a table and selects from it does not exist in SQL. 21,452 of these come from two callees: `createTablesAndFillDb` x54 and `initDatabase` x10. |
| **assert / verdict** | 5,601 | 2,451 | **YES.** `assertEquals` 2,927 (2,409 with a literal expected value), `assertSameElements` 708, `assertSize` 679, `assertSameSQL` 457, `assertJsonStringsEqual` 179. |
| **let: compile-time value** (lambda, graph-fetch tree, model ref, literal, `^new`, runtime handle, extension object) | 2,322 | — | **N/A — erased** by beta-substitution before lowering. |
| **query execution** (`execute`, `executeLegendQuery`, `generateTestData`) | 2,157 | 1,944 | **YES.** The relation is the shot's main CTE. 1,805 tests have exactly one, 631 none, 139 more than one. |
| **let: runtime value** | 1,181 | 747 | **Mostly.** 443 are projections/coercions of a prior value (`->at(0)`, `->toOne()`, `->cast`) that fold into the consuming CTE. |
| **other statement root** (statement-root `map`, `mayExecuteAlloyTest`, `validateNode`, trailing `true`) | 458 | 217 | **Mixed.** Trailing `true`/`^new`/`$var` are inert — drop them. Statement-root `map` (91 tests) is a dynamic loop: **no**. `mayExecuteAlloyTest` (28): **no**. |
| **engine TEXT** (`toSQLString`, `planToString`, `scanRelations`, `sqlQueryToString`) | 180 | 159 | **NO, and it should not be.** A compiler artifact, produced host-side before any SQL runs. |
| **plan build** (`executionPlan`, `planTestDataGeneration`) | 165 | 156 | **NO.** A plan is an object graph the host builds and the test then navigates. Not relational. |
| **inert diagnostic** (`println`) | 53 | — | dropped |
| **control flow** (`if`, `match`, `fold` at statement root) | 15 | 10 | **YES in principle** (`CASE`), but 3 of the 10 have effectful branches. |

### 2.4 `let` to CTE

**5,555 lets across 2,493 of 2,575 tests** (82 have none). Histogram: 1 let in 1,320 tests, 2 in
528, 3 in 181, 5-or-more in 359, max 24.

**Rebinding: zero.** Every test was checked for a top-level `let` name bound twice; none rebinds.
Pure's single-assignment discipline holds corpus-wide, so the CTE mapping is sound *as written*.
`StatementInline`'s `_s<N>_<name>` freshening is therefore belt-and-braces for corpus test bodies —
but it remains load-bearing for **inlined** bodies, where the same `connection` name appears in 30+
spliced helpers.

What the lets bind:

| binds | count | CTE-able? |
|---|---:|---|
| **execution result frame** (`execute(...)`) | 2,105 | **YES** — the relation is the CTE. Caveat: it is a `Result` *envelope*, not a relation. 1,414 tests read `.values`, 917 read `.values.rows`, but **20 read `.activities`** (plan/SQL provenance) and **22 read `.sql`/`.sqlQuery`**. Those two fields are host artifacts. |
| scalar literal | 478 | **YES** as 1x1 — better folded as a literal |
| **lambda / deferred query code** (`{\|Person.all()->project(...)}`) | 464 | **NO, and needs no CTE** — it is *code*, substituted at the use site |
| projection/coercion of a prior value | 443 | **YES** — folds into the consuming CTE |
| **model element reference** (bare `meta::relational::tests::db`) | 402 | **NO — compile-time constant**, erased |
| **runtime / connection handle** | 323 | **NO — session-level**, erased; the shot runs *on* the connection |
| **instance (`^new`)** | 207 | **NO** — a Pure object graph; compile-time in nearly all cases |
| graph-fetch tree literal `#{...}#` | 141 | **NO — compile-time**, consumed by `graphFetch` |
| collection literal | 136 | **YES** as `VALUES` when elements are scalars |
| **execution PLAN handle** | 128 | **NO, inherently.** Not a value a database can hold. 156 tests. |
| **engine TEXT** (sql/plan string) | 126 | **NO** — the database cannot produce it. 159 tests. |
| date literal | 91 | **YES** |
| alias of another var | 65 | **YES** |
| compiler/extension object | 50 | **NO — compile-time**, erased |
| **effect result** (`let r = executeInDb('select ...')`) | **8** | **NO** — reads rows back from raw SQL a previous statement wrote |
| other value expressions | 288 | mixed (`map` 25, `sort` 20, `executeInternal` 16, `generateObjectReferences` 12, ...) |

**Bottom line.** 3,318 lets (60%) map to CTEs directly. 1,713 (31%) are compile-time constants that
**need no CTE at all** — beta-substitution deletes them, which `StatementInline` and
`UserCallInliner` already do. The genuinely non-CTE-able residue is **262 lets in ~300 tests** (plan
handles 128, engine text 126, effect results 8) — and none of them are non-CTE-able because of CTE
*semantics*. They are non-CTE-able because they are host objects that never touch the database.

### 2.5 The right unit

| unit | coverage | cost |
|---|---|---|
| **whole test = 1 statement** | **1,998 / 2,575 = 77.6%** clean (strict variant, excluding benign trailing statements: 1,969 = 76.5%) | Requires the session already seeded. The 577 that fail split (overlapping) as: engine TEXT 159, plan handle 156, 2-or-more executions 139, inline DDL 132, statement-root `map` 91, Alloy shell 28, control flow 10. |
| **per query execution ("per-`from`")** | 2,157 executions; **2,436 / 2,575 tests (94.6%) have at most one**, so the unit collapses into the whole test for them. Only 139 tests need 2-12 shots. | Buys 139 tests for the price of a multi-shot driver. **Note: `->from(` appears 69 times in the whole corpus — "per-`from`" is not a corpus concept.** The corpus's unit is `execute(query, mapping, runtime, extensions)`. |
| per statement-group (maximal run of non-effect statements between effects) | ~100% by construction | Degenerates to today's statement-by-statement driver for the 132 DDL tests and buys nothing for the other 2,443. |
| per assert | 5,601 shots for 2,575 tests | Strictly worse — 1,335 of the clean tests have more than one assert (4,268 asserts in 1,969 tests), and the query would be re-run per assert. |

**Recommendation: one shot per test; the seeding boundary at the session; a documented multi-shot
fallback for 139 + ~10 tests.**

**1,697 tests are the canonical shape** — exactly one `execute`, zero DDL, zero plan, zero text, at
least one assert. A further 272 clean tests have no query at all (constant-folded asserts) and are
one statement trivially.

**Why the boundary must fall between session seeding and the test statement.** The 111
`BeforePackage` setups expand to **26,425 statements** (largest:
`classMappingFilterWithInnerJoin::setUp` at **1,679**); the shared fixture `createTablesAndFillDb()`
alone is 222. There are 311 distinct test packages, so 311 sessions, and seeds amortize roughly
**25:1** across the tests in a package — pulling seeding into per-test shots costs ~25x. Keep
`runSetups`/`seedLedger` exactly as they are (`MinimalCorpus.java:373-397`); make the *test* one
statement.

### 2.6 Blockers, ranked, inherent vs incidental

**1. Engine TEXT artifacts — 159 tests. INHERENT.**
`toSQLString` (79), `planToString` (91), `scanRelations` (49), `sqlQueryToString`,
`toSQLStringPretty`. The test asserts on a string *the compiler produced*, before any SQL runs.
*Design:* the string is a host constant by shot time — bind it as a literal into the shot
(`SELECT 'actual' = 'expected' AS pass`). The verdict stays one boolean row from the database
without pretending the database computed the value. **Do not blur this in reporting**: these are
"database-adjudicated", not "database-computed", and the ledger must say so.

**2. Execution-plan handles — 156 tests. INHERENT.**
`let plan = executionPlan(...); let node = $plan.rootExecutionNode.executionNodes->at(0)->cast(@RelationalRootQueryTempTableGraphFetchExecutionNode); assertEquals(..., $node.processedTempTableName)`
(`executionPlan/tests/executionPlanTest.pure:2795`). Object-graph navigation over a compiler
artifact. Same treatment as (1); also the strongest candidates for a separate scoring lane, since
they test engine internals rather than query semantics.

**3. More than one query execution — 139 tests. INCIDENTAL.**
Distribution: 106 tests with 2, 19 with 3, 7 with 4, 3 with 5, 2 with 8, 2 with 12.
*Design:* one shot with N CTEs and N boolean columns AND-ed. Free when the queries do not depend
on each other's side effects, which holds for all but the mutation-style tests in (4).
**Cheapest large win available.**

**4. Inline DDL/DML in the test body — 132 tests. MIXED.**
Per-test inlined DDL counts: 56 tests with 1, 11 with 2-10, then **6 with 317, 3 with 318, 29 with
334, 25 with 335, 1 with 636**. The 64 heavy ones are `createTablesAndFillDb()`/`initDatabase()`
called *in the body* — pure re-seeding, **incidental**: hoist the call into the session's setup list
(the harness already runs the same functions as shared setups) and the body goes clean.
The ~10 light ones are **inherent**. `meta::relational::tests::ddl::dropAndCreateTable`
(`helperFunctions/tests/testDdlGeneration.pure:53`) is the definitive counter-example:

```
dropAndCreateTableInDb(foo_db,'bar_t',$c); executeInDb('insert into bar_t ...',$c);
let result = executeInDb('select count(*) from bar_t',$c); assertEquals(1, ...);
dropAndCreateTableInDb(foo_db,'bar_t',$c);        // state changes mid-test
assertEquals(0, executeInDb('select count(*) from bar_t',$c)...);
```

The test's entire point is that the same query returns different rows before and after a DDL. That
is multi-shot by definition. Same for `graphFetch/tests/testGraphFetchSqlIsolation.pure:29`,
`functions/tests/loadCsvToDbTable/testLoadCsv.pure:21`, and
`helperFunctions/tests/testDdlGeneration.pure:74` (temp table create, select, drop).
*Design:* a named carve-out of ~10 tests. Do not be clever.

**5. Statement-root `map` (dynamic statement loop) — 91 tests. INHERENT today, incidental if
extended.** `$sqls->map(sql | executeInDb($sql, $connection))` where `$sqls` came from
`setUpDataSQLsV2($csvText, $db, $dbConfig)`. `hoistIn` returns lambdas untouched
(`StatementInline:164-166`) and `expand` only splices statement-root/let-bound calls.
`LiteralMapUnroll` already handles the *sibling* shape (a map over a spelled collection of bound
names); this is the same shape with a computed collection.
*Design:* two options. (a) Constant-fold `setUpDataSQLs` at compile time — the CSV is a literal at
**all 48** `setupTestData` call sites, so the SQL list is statically computable, and then
`LiteralMapUnroll` unrolls it. (b) Recognise that this **is** seeding and hoist it out of the shot.
**(b) is cheaper and correct.**

**6. Other unclassified statement roots — 48 tests. MIXED.**
`validateNode` (78 statements), `validate` (39), `runTest` (8), `applyMilestoningFilters` (6),
`createTempTable`/`dropTempTable` (1 each). **`validate`'s 39 are already handled** by
`ValidateDesugar`, so the true residue is ~10.

**7. `mayExecuteAlloyTest` lambda shell — 28 tests. INHERENT under the current inliner.**
26 in `testDataGeneration/tests/testDataGeneration.pure`, 2 in `tds/tests/testTDSJoin.pure`.
*Design:* a front-door desugar in the mould of `LiteralMapUnroll`/`ValidateDesugar` — apply the
lambda to the four known constants and splice its body. Small, self-contained, unlocks 28 tests.

**8. Control flow at statement root — 10 tests. INCIDENTAL.** `if` appears in 62 tests but is a
*statement* root in only 10; 5 of those have it as their only blocker. `CASE`/`COALESCE` covers
them.

**9. Cross-test session dependence — INHERENT, and already the design.**
Every test inherits the shared fixture (`sharedSetups` from `tests/relationalSetUp.pure`) plus every
`BeforePackage` of a prefixing package, run once per session (`MinimalCorpus:runSetups:373-397`),
and the `seedLedger` carries each test's non-query SQL forward to later tests of the same session
(`MinimalCorpus:245-249`). This is not a blocker to single-shot; it is *the reason* the boundary
must be session/shot. It does mean the shot's correctness depends on session state the shot does not
name — which is what step 6c of `END_TO_END_PLAN_2026_09_08.md` already flags as an
order-independence gate.

**10. Simple-name ambiguity in `resolvedDefinition` — ESTIMATED, not measured. MEASURE FIRST.**
`StatementInline.resolvedDefinition` returns null — silently leaving the call uninlined — when
several candidate FQNs have a user definition at the call's arity. The corpus defines **`setUp` 88
times, `createTablesAndFillDb` 23, `testRuntime` 17, `setup` 15, `createTablesInDb` 6**, all at
arity 0; **510 helper simple names are defined more than once.** Whether a given bare call collides
depends on the resolver's import scope, which cannot be evaluated without running the compiler.
*Basis for the estimate:* the per-package `setUp` convention means most collisions should be
resolved by the test's own package wildcard (`MinimalCorpus.discover` adds `pkg` to the import
scope), so the collision rate is expected to be low. **But it is unmeasured, and it fails silently.**
This is the one number in this document that a machine must produce, and it is cheap: instrument
that method to log every multi-candidate null return, then run the existing sweep.

**11. Non-relational stores — 50 cross-store/M2M/`ModelStore`, 194 graphFetch, 12
`modelToModelToRelational`. INHERENT for the M2M half.** A cross-store test's in-memory side is not
SQL; its relational leg can be one shot, the join to the in-memory side cannot. `graphFetch` alone
is fine — it produces a JSON document the database can build, and step 6b of the plan already
schedules the in-database graph verdict.

### 2.7 The in-database verdict, and the message

The verdict leg is further along than the discussion assumed: **the database byte is already the
verdict of record** (`AssertVerdicts.java:941`), with a best recorded agreement of
**agree 1492, disagree 0, declined 91**. What is missing is not the verdict. It is:

- **Fusion** — today the assert byte and the query are separate executions; single-shot fuses them.
- **The message.** `AssertVerdicts.java:948` — `String d = hostHeld ? null : hostMessage.get();` —
  the failure message still comes from the host, which is why host evaluation cannot be dropped.
  *Design: the repr rider.* Append a `__repr` column to the shot via the existing
  `Repr.of(Type, SqlExpr)` (`lowering/Repr.java:28`), so the database returns the rendered actual
  value alongside the boolean. That removes the message dependency **with zero extra executions**.

**One dialect finding invalidates part of the parked design.** `docs/parked/InDbVerdict.java` (300
lines) leans on `EXCEPT ALL` for multiset equality. **H2 2.1.214 has no `EXCEPT ALL`** —
`SelectUnion$UnionType` enumerates only UNION, UNION_ALL, EXCEPT, INTERSECT. Set equality on the H2
lane must be expressed differently (group-and-count both sides and compare, or `NOT EXISTS` over a
full outer join on the grouped counts). See §4 probe P-07.

**And one that constrains where CTEs can carry effects.** DuckDB has **no data-modifying CTE** —
"A CTE needs a SELECT". So the "wrap the whole test including its inserts in one `WITH`" shape is
not available even in principle on the primary lane. This is a second, independent reason the
seeding boundary is at the session.

**Scope note.** Roughly **22% of the corpus (~1,351 asserts) are SQL-text asserts** —
`assertSameSQL` 457, `assertJsonStringsEqual` 179, plus the `toSQLString`/`planToString` families.
Single-shot cannot make these database-computed. It can still make them database-*adjudicated* per
(1). Whatever the ledger reports, it must not count them as in-database verification.

### 2.8 The pilot

**Take the ~700 tests in `tests/mapping/*` + `functions/tests/projection/*`** — the densest slice
inside the 1,697 canonical-shape tests, all sharing one `setUp` and the `db`/`dbInc` fixture. For
each, emit **one** statement against the already-seeded session:

```sql
WITH q AS (<the lowered query, exactly what the executor renders today>),
     a1 AS (SELECT COUNT(*) = 7 AS ok FROM q),
     a2 AS (SELECT NOT EXISTS (SELECT * FROM q EXCEPT ALL SELECT * FROM (VALUES ...))
                AND NOT EXISTS (SELECT * FROM (VALUES ...) EXCEPT ALL SELECT * FROM q) AS ok)
SELECT (SELECT ok FROM a1) AND (SELECT ok FROM a2) AS pass;
```

Change **nothing** else: same `beginSession`, same `runSetups`, same seed ledger, same
`ReplayOracle`. Score the slice and diff the roster against today's, test for test.

**Why this experiment and not another.** The setup question is answered (setups are 95%
straight-line literal SQL, they inline fine, and they belong outside the shot). The wrapper question
is answered (28 names, 7 shapes, 6 of 7 unroll today). The `let` question is answered (zero rebinds,
60% CTE-able, 31% erasable). What has **no evidence** is whether *N asserts over one relation
compose into one boolean column-expression that agrees with today's verdicts on real data* — set
equality, ordering (**397 tests sort before asserting**), NULL/`TDSNull` cells, float tolerance,
enum decoding. Those fail only at scale, and they are exactly what
`REFEREE_IN_DATABASE_DESIGN_2026_09_07.md` and the parked `InDbVerdict.java` half-build. If the
pilot's roster matches today's on ~700 tests, the rollout to 1,998 is mechanical. If it does not,
the divergences **are** the design work — found for the cost of one family instead of one corpus.

---

## 3. Leg 2 — code + metamodel as data

### 3.1 It is five demands, not one

"Code+metamodel as data" was treated in discussion as a single feature. It is five, with very
different costs and very different value:

| # | Demand | What it means | Status |
|---|---|---|---|
| D1 | **Metamodel as relations** | classes, properties, generalizations, mappings, joins available as tables the compiler can query | **Largely landed** — `builtin/SystemMetamodel.java` (1,484 lines), `MetamodelSeeds.java` |
| D2 | **Closure/ancestry as relations** | transitive relationships precomputed so no query recurses | **Landed** — `includesClosure`, `setAncestry`, `plan_node_closure` |
| D3 | **The plan as relations** | execution plan nodes queryable rather than navigated in Java | **Partly landed** — `plan/PlanRows.java:30` `plan_node_closure(ancestor_id, node_id, depth)` |
| D4 | **The user program as relations** | the parsed Pure AST itself in tables | **Not started.** This is the big one. |
| D5 | **Re-hosting the engine's own compiler** | corpus tests that compile Pure *inside* the corpus | **24 of 45 candidate tests are this.** Do not confuse it with D1-D4. |

The important scoping finding: **of the 45 tests that look like "code as data" candidates, 24 are
D5** — they exercise the engine's Pure-implemented compiler (`pureToSQLQuery.pure` and friends),
which is a re-hosting problem, not a metamodel-as-relations problem. **A realistic phase 1 is 8
tests.** Anyone quoting 45 is counting the re-hosting work.

### 3.2 Recursive CTEs: do we need them?

**No — and it is already the declared policy not to.** `SystemMetamodel.java:260-263` states it
outright:

> "the engine's recursive `allNodes` walk as a row entity: every ancestor/descendant pair, self at
> depth 0 — **a query never recurses over the tree**"

All three closure tables exist: `mapping_includes_closure` and `set_ancestry`
(`MetamodelSeeds.java:43,159` and `:56,334`), and `plan_node_closure` (`:73`, shape at
`PlanRows.java:30`).

**And the hierarchies are bounded and tiny — measured on the real corpus:**

| structure | max depth | scale |
|---|---:|---|
| class inheritance | **5** | 11,080 class declarations, 7,200 in the extends graph, **0 cycles** |
| mapping includes | **2** | 916 mappings, 140 with includes |
| join chains | **4** | 84 chains of 2, 31 of 3, 19 of 4 |
| property navigation | **6** | 99.9% are 3 or less; the deepest is written out flat in source |

A fixed point over at most 5 levels is N joins or a closure table, not a recursive CTE. And the
closure is computed **once per graph in Java at seed time**, amortized over the graph's whole
lifetime, where a recursive CTE would pay per query.

### 3.3 Two dialect defects that make `WITH RECURSIVE` a design smell here

**H2 2.1.214's recursive CTE has no cycle protection whatsoever.** Source-confirmed: `findRecursive`
accumulates into a plain `LocalResult` with **no `setDistinct()`**, so `UNION` and `UNION ALL`
behave identically, and the recursive term only ever sees the previous iteration — never the
accumulated set. A cyclic edge set **loops forever**, and `setMaxMemoryRows(Integer.MAX_VALUE)`
disables spilling, so it **OOMs rather than erroring**. Identical in 2.4.240, so "upgrade H2" is not
a mitigation — and the pin exists for golden parity anyway.

**DuckDB returns silently-incomplete results when the recursive term references the CTE twice** —
`duckdb/duckdb#13974`, reproduced upstream, **closed as "not planned."** Not an error: fewer rows.
That is the worst possible failure mode for a project whose verdict path compares rows.

Plus we are pinned between two DuckDB defects: **1.4.4.0 cannot nest a `WITH` inside a recursive
term** (`#12256`, fixed only in 1.5.5), and the pin to 1.4.4.0 exists for an unrelated LIMIT bug.

### 3.4 Stored procedures: no, decisively

- **DuckDB has no `CREATE PROCEDURE` at all.** Macros only, documented as "pure SQL, without
  procedural control flow", and macros cannot be recursive.
- **H2's `CREATE ALIAS ... FOR "com.x.Y.method"` is a *Java* stored function** — precisely the thing
  the execution tenet forbids, wearing a SQL hat, and invisible to the eval ledger.
- `ConnectionDefinition.DatabaseType` has **22 members**. Hive, Presto, Trino, BigQuery, Athena,
  DuckDB and SQLite have **no procedural language at all**. A design that needs one is a design that
  works on a minority of our declared backends.

### 3.5 Template-then-fill: fine as a reader, bad as an emitter

We already have the 406-line receipt. `PlanReplay.java` re-implements five of the engine's freemarker
helpers, and what it teaches is damning for using that shape to emit our own queries:

- `spell()` is literally `String.valueOf(v)` with the comment "the template supplies its own
  quoting" — an injection surface by construction.
- `gmtToZone` has to **re-guess the date format from the string's shape** — type information is lost
  and then reconstructed by inspection.
- The second pass pulls rows out of the database to fill the template.

The comment at `PlanReplay.java:69-75` names both the cost and the cure: *"the ORACLE keeps its rows
as a table named by the allocation — **the values never leave the database**."*

**Verdict:** template-then-fill is the right shape for *reading someone else's* format, which is all
`PlanReplay` is and all it should stay. It is a bad way to emit our own SQL, and choosing it would
re-introduce the two problems (quoting, type loss) that the typed MIR exists to prevent.

### 3.6 Recommended design for leg 2

1. **Single-pass generation over closure tables.** Works identically on DuckDB, H2 and SQLite. This
   is already the policy; the work is extending it to D3/D4, not changing direction.
2. **For genuinely unbounded structure — which has exactly two named witnesses — a compile-time
   bounded unroll first.** The seed knows `MAX(depth)`; emit that many joins. Deterministic,
   portable, and it fails loudly if the bound is exceeded.
3. **`WITH RECURSIVE` last, DuckDB-only, with three non-negotiables:**
   - an explicitly carried visited-set (never rely on `UNION`'s dedup — H2 does not do it, and
     relying on it is what turns a cycle into an OOM),
   - a hard `depth <` kill-switch in the recursive term,
   - **exactly one self-reference**, structurally asserted in the MIR before rendering (this is what
     `#13974` silently breaks).
4. **On H2, raise a `DialectCapability` wall** rather than emitting something that might be wrong.
   A loud budgeted wall is in-tenet; a silently incomplete result is not.

---

## 4. Probes to run on an idle machine

Everything below is a capability question that static reading could not settle. Each is a few lines
of SQL against the pinned engines. Run them before implementation, not during.

**Set operations and multiset equality**

- **P-01** DuckDB 1.4.4.0: does `EXCEPT ALL` exist and preserve duplicate multiplicity?
- **P-02** H2 2.1.214: confirm `EXCEPT ALL` is rejected (expected: syntax error). Record the exact
  message so the capability wall can cite it.
- **P-03** H2: does the group-and-count formulation of multiset equality agree with DuckDB's
  `EXCEPT ALL` formulation on a table containing duplicates *and* NULLs?
- **P-04** Both: `INTERSECT`/`EXCEPT` NULL semantics — is `NULL` equal to `NULL` for set-operation
  purposes? (Standard says yes; verify both.)
- **P-05** Both: set-operation column-type unification when one branch is an untyped `NULL` literal.
- **P-06** SQLite: same as P-01/P-04 (SQLite has `EXCEPT` but no `EXCEPT ALL`).
- **P-07** The H2 replacement formulation for `InDbVerdict`'s multiset equality — write it, run it
  against the 20 hardest existing goldens, and confirm verdict-for-verdict agreement with the host.

**CTEs**

- **P-08** DuckDB: confirm no data-modifying CTE (`WITH x AS (INSERT ...)`). Record the message.
- **P-09** H2: same.
- **P-10** Both: maximum practical number of CTEs in one statement — the multi-execution design
  (blocker 3) emits up to 12, and the multi-assert design emits one CTE per assert (max observed:
  a test with 24 lets).
- **P-11** Both: is a CTE referenced twice evaluated once or twice? (Affects whether the shot's
  cost model is honest.)
- **P-12** Both: can a CTE reference an earlier CTE in the same `WITH`? (Standard: yes. Verify.)
- **P-13** DuckDB 1.4.4.0: confirm `#12256` — a `WITH` nested inside a recursive term fails.

**Recursive CTEs (only if leg 2 step 3 is ever reached)**

- **P-14** H2: confirm the no-cycle-protection finding empirically with a 2-cycle edge table, under
  a small heap and a hard timeout. **Run this one in a container.**
- **P-15** DuckDB: confirm `#13974` — two self-references in the recursive term returns fewer rows,
  no error.
- **P-16** DuckDB: does `WITH RECURSIVE ... UNION` (not `ALL`) actually dedup against the
  accumulated set?
- **P-17** Both: is there a configurable recursion depth limit, and what happens at it?

**Verdict shape**

- **P-18** Both: `SELECT <bool expr> AS pass` — what Java type comes back through JDBC? (The
  existing byte-verdict path already answers this for one shape; confirm for the fused shape.)
- **P-19** Both: three-valued logic in the fused verdict — if any assert's expression is `NULL`,
  does `AND` produce `NULL` and does the driver deliver it as `null` rather than `false`? **This is
  the single highest-risk item in the probe list**: a `NULL` silently read as `false` inverts a
  verdict.
- **P-20** Both: float comparison — what does the corpus's `assertEquals` tolerance become in SQL,
  and do the two lanes agree?
- **P-21** Both: `Repr.of` output for every type in the type matrix — does the DB-side rendering
  match the host's `toRepresentation` byte for byte? (Needed before the repr rider can replace the
  host message.)
- **P-22** Both: ordering — for the 397 tests that sort before asserting, does the shot's `ORDER BY`
  inside a CTE survive into the outer aggregate, or must the ordering be expressed as a
  `ROW_NUMBER()` comparison?
- **P-23** Both: enum decoding inside the shot.
- **P-24** Both: `TDSNull` — how does the harness's null sentinel round-trip through a fused
  comparison?

**Inlining and scale**

- **P-25** The `resolvedDefinition` multi-candidate census (blocker 10). **Run this first — it is
  the cheapest and it is the only number in this document that is an estimate.**
- **P-26** Statement-count ceiling: does any dialect or driver have a practical limit on generated
  statement text length? The largest inlined setup is 1,679 statements; if any of those ever get
  fused, the text is large.
- **P-27** Both: prepared-statement parameter limits, if the shot binds expected values rather than
  spelling them.
- **P-28** Confirm the scanner's 2,575 against the harness's own discovery on a real run, and diff
  the per-test classification for a sample of 50 against what the compiler actually inlines.
- **P-29** The `paramTest.Test` profile (151 tests): confirm the harness genuinely does not discover
  it, and decide whether that is intended.
- **P-30** Seed amortization: measure the real 25:1 ratio on a run rather than deriving it from
  statement counts.

---

## 5. Risks, ranked

1. **A `NULL` verdict read as `false`** (P-19). Three-valued logic is the classic fused-predicate
   trap, and it fails in the flattering direction on some drivers and the unflattering one on
   others. Retire this before anything else.
2. **Reporting "in-database verification" for the ~315 inherently-host tests.** The design is
   honest; the *ledger* is where it would go wrong. Make the distinction between
   database-*computed* and database-*adjudicated* a schema field, not a footnote.
3. **The silent `resolvedDefinition` null** (blocker 10 / P-25). Unmeasured, and it fails by quietly
   not inlining — which shows up as "this test isn't single-shot-able" rather than as an error.
4. **H2/DuckDB divergence in the fused verdict.** Today the two lanes agree because both defer to
   the same host comparison. Fusing moves the comparison into two different engines with different
   set-operation support. Expect divergence; budget for the H2 formulation being different code
   rather than the same code.
5. **Recursive CTEs, if anyone reaches for them.** Both defects (H2 OOM, DuckDB silent
   under-return) are unbounded-blast-radius and neither surfaces as an error.
6. **Scope creep from D5.** "Code as data" will keep attracting the 24 re-hosting tests. Name them
   out of scope in writing, once.

---

## 6. Investigated and dropped

Recorded so the next pass does not reopen them.

- **"Single-shot needs new CTE support in the MIR."** False — `SqlWith` and `SqlQuery`'s sealed
  hierarchy already have it. Only the `SqlUnion` operator enum is missing.
- **"The in-database verdict is unbuilt / parked."** False. It is live and is the verdict of record
  (`AssertVerdicts.java:941`). `docs/parked/InDbVerdict.java` is the **referee** leg — a different
  thing — and the two were conflated in early discussion.
- **"Per-`from` is the natural unit."** `->from(` appears 69 times in the entire corpus. Not a
  corpus concept.
- **"Hundreds of tests hide behind setup wrappers."** 213 tests, 28 names, 7 shapes, 6 of which
  already unroll.
- **"The 39 `validate` tests are a single-shot blocker."** Already desugared at the front door by
  `ValidateDesugar`.
- **"`let` rebinding will make the CTE mapping unsound."** Zero rebinds corpus-wide.
- **"Code-as-data is ~45 tests."** 24 of those are re-hosting the engine's own compiler. Phase 1 is
  8 tests.
- **"We need recursive CTEs for the metamodel."** Max depths are 5 / 2 / 4. Closure tables already
  exist and are already the declared policy.
- **"Stored procedures could carry the procedural parts."** DuckDB has none; H2's are Java; 7 of 22
  declared backends have no procedural language.

---

## 7. Recommended sequence

1. **P-25** (the `resolvedDefinition` census) and **P-19** (NULL verdict). Both are cheap; both are
   the only things that could invalidate the plan.
2. **P-01 through P-07** — settle the multiset-equality formulation per lane before writing the
   verdict emitter.
3. **`SqlUnion` set-operator enum.** The one MIR change.
4. **The ~700-test pilot (§2.8).** Diff the roster test-for-test. Do not proceed past this without
   a match.
5. **Blocker 3** (multi-execution, 139 tests) — the cheapest large win, and it exercises the
   multi-CTE shape the rest of the design depends on.
6. **Blockers 4-heavy (64 tests, hoist to setup), 5b (91, hoist to setup), 7 (28, Alloy desugar),
   8 (10, `CASE`).** All small, all independent.
7. **The repr rider** — removes the host-message dependency with zero extra executions.
8. Only then, if D3/D4 still matter, leg 2's closure-table extension.
