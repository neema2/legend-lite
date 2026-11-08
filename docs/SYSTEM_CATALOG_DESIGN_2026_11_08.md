# The system catalog: the metamodel as an ordinary database (design, 2026-11-08)

> **Status: DIRECTION AGREED WITH THE USER (2026-11-08), NOT IMPLEMENTED.** The open checks are in §8. Where this goes in
> the plan, and what holds until then, is open (§9). This file keeps the homework of one long session: the problem
> stated, every measurement, upstream read from the pinned sources, the options rejected and why. Each measurement can be
> re-run with the probes in `docs/system-catalog-homework/`.
>
> Related: `docs/CORE_SQL_SIZE_2026_11_04.md` (the 85% measurement and where the metamodel is special),
> `docs/METAMODEL_STORE_HANDOFF.md` (the store's original design, D1 and D2), `docs/PLAN_AS_DATA_2026_11_07.md` 2.6 and
> 2.10.4 (the system store's sessions, the out-of-plan channel this design closes).

## 0. The problem, with no solution in it

A Pure program can read the model it runs against as data: its classes, properties, mappings and tables (the
metamodel). Two rules hold:

1. **The database does all query-time work (G6).** A metamodel read is SQL over rows, compiled exactly as a user query
   (the lowering does not know the metamodel exists).
2. **A run reads only its plan, plus the rows the database returns (plan-as-data, G4 and G11).**

What makes the metamodel different: for every other query the data already sits in a database, and the plan only names
it. For a metamodel query the data sits in no database. It is the compiled model, and it exists only in the planner's
memory. So the model's rows must get into a database before the query can run, so that:

- the run reads only its plan, plus database state;
- the database does the query's work;
- the cost is proportional to what is read, not to the size of every model.

**Today** the query's own plan carries a seed step (every system table's DDL and every row, platform elements
included). The run executes it on a separate system database held per compiled model: `SystemDatabase`, hung on
`ModelContext.derived` and closed by a `Cleaner` when the model is garbage-collected. Two defects follow:

- **85% of core's SQL is that seed, re-sent for every test**: 57.0M of 67.0M characters (CORE_SQL_SIZE §2).
- **The run finds its database through the compiled model object**, which the door hands it. That is the out-of-plan
  channel the plan-as-data design's 2.10.4 names and step 8c left open.

## 1. Measurements (2026-11-08, at `9de03396`)

### 1.1 The seed: the platform's rows against the model's

The rows `MetamodelSeeds.rows` derives per system table, with characters summed over the cells (probe `SeedSplit`,
`StressSeed`):

| Model | Tables | Rows | Characters |
|---|---|---|---|
| one class (`Class u::A { name: String[1]; }`) | 34 | 2,582 | 327,148 |
| `MetamodelStoreTest`'s two classes | 34 | 2,584 | 327,224 |
| `MetamodelMappingStoreTest`'s model: three classes, a database with a table and a view, six mappings | 34 | 2,887 | 362,066 |
| **the stress corpus's model** (249 sources, 9,106 elements; built in 3.0 s) | 34 | **117,113** | **17,980,176** |

**Reading it:**
- **The platform's own rows** (metamodel classes, the system store's mapping and tables) are about 327K characters in
  2,580 rows, the same for every model.
- **A test model's own rows are tiny.** The two-class model adds 76 characters; the mapping model 35K.
- **A real model's own rows dominate.** The stress model's are 98% of its seed:
  - `relational_elements`: 38,418 rows, 9.8M characters;
  - `property_mappings`: 26,482 rows, 4.1M;
  - `data_types`: 33,842 rows, 2.6M;
  - `set_ancestry`: 4,333 rows, 0.46M;
  - `class_mappings`: 2,473 rows, 0.39M;
  - `properties`: 6,889 rows, 0.30M;
  - `classes`: 2,896 rows, 0.21M.
- **Deriving the stress model's rows in Java takes 296 ms**, so it is Knowledge and cheap.

### 1.2 The platform's rows are an exact part of every model's (probe `SplitCheck`)

Every platform row appears, unchanged, in the stress model's catalog: **0 missing or altered**. The model adds 114,533
rows. No row of the platform depends on which model it sits beside: no global ordinal, no derived column. "The
platform's catalog plus the model's rows" is an exact decomposition, not an approximation.

### 1.3 Who reads the metamodel at all

- **The stress corpus: no body.** Its `meta::pure::metamodel` names are type names (`Relation<Any>`,
  `meta::pure::metamodel::type::Any`), not reads of the store.
- **The relational corpus (DuckDB, the host judge): one test.** `testRelationalDatabaseMapper` sends 120 statements;
  `[corpus2] statement-origin system tests=1`.
- **Core: five test classes** send the whole seed (CORE_SQL_SIZE §2): `MetamodelMappingStoreTest` 23.4M,
  `MetamodelQueryFunctionsTest` 19.3M, `MetamodelStoreSubstitutionTest` 8.2M, `MetamodelStoreTest` 4.5M and
  `BodyRouterParityTest` 1.5M.

So the cost problem is core's tests re-seeding per test, not the product, and not the large corpora.

### 1.4 Registration inside a transaction, and isolation (probe `CatalogCheck`)

Catalog tables with a model column were created and the platform's rows committed. Then, in one transaction on session
A, the stress model's 114,533 rows were inserted. Session B, on the same in-memory database, counted `classes`; A rolled
back; A counted again. The platform has 507 classes; with the stress model, 2,896.

| Engine | How the rows were written | Register 114,533 rows | Session B sees | After rollback |
|---|---|---|---|---|
| DuckDB 1.5.6 | batched prepared `INSERT`s (5,000 a batch) | **41,274 ms** | 507 ✓ | 507 ✓ |
| DuckDB 1.5.6 | the appender, straight into the catalog table | 240 ms | **2,896 ✗** | **2,896 ✗** |
| DuckDB 1.5.6 | the appender into a session-private TEMP table, then one `INSERT ... SELECT` | **350 ms** | 507 ✓ | 507 ✓ |
| H2 2.5.252 | batched prepared `INSERT`s | **333 ms** (252 and 318 in other runs) | 507 ✓ | 507 ✓ |

Other timings:
- Creating the catalog with the platform's rows: 35 to 113 ms on DuckDB, 62 to 70 ms on H2.
- Rollback: 1 to 4 ms on DuckDB, 152 to 200 ms on H2.

**Two rules follow:**
- **DuckDB's appender commits on its own.** Its rows ignore the session's transaction and survive a rollback. A load
  inside a transaction must go through a TEMP staging table and an `INSERT ... SELECT`, which is the shape exec's own
  `DuckDbAppenderLoad` already uses.
- **H2 commits a transaction on any DDL.** Nothing inside a test's catalog transaction may create, alter or drop a table
  on H2: not registration, not the body's own statements (§8, the first check).

The probe used `VARCHAR` columns. The real catalog's typed columns may move these times, but not their order of
magnitude.

## 2. Upstream, read from the pinned sources (legend-pure 5.99.0, legend-engine 4.145.0)

**legend-pure keeps one global graph, and `Class.all()` lists every instance in it.**
- `all()` is the native `getAll<T>(type:Class<T>[1]):T[*]`, documented "Returns all instances of a Class"
  (`legend-pure-m3-core/.../platform/pure/grammar/functions/lang/all/all.pure`).
- **Interpreted:** `GetAll.java` answers `context.getClassifierInstances(class)`. That is `Context`'s one index of
  instances by classifier (`m3/compiler/Context.java:189`), filled for every compiled instance of everything loaded into
  the runtime: the platform, every repository, the user's sources.
- **Compiled:** `GetAll.java` emits `CompiledExecutionSupport.getClassifierInstances(name)`, which reads the runtime's
  `Metadata`. `Metadata.getClassifierInstances` defaults to `getMetadata(classifier).valuesView()`.

**legend-engine builds one `PureModel` per project, but its `Class.all()` would not list the project's own classes.**
- `PureModel` (`legend-engine-language-pure-compiler/.../toPureGraph/PureModel.java:144, 206-208`) compiles the project
  over `METADATA_LAZY`: the distributed metadata of every code repository on the classpath, except the `test_` and
  `other_` repositories.
- Its `MetadataWrapper` answers `getMetadata(classifier)`, and so the extent, from that classpath metadata alone. The
  project's elements are reachable one at a time (`getMetadata(classifier, id)` falls back to
  `pureModel.getTypeFromIndex`), not in the extent.
- So `Class.all()` executed in an engine `PureModel` lists the platform's and Legend's libraries' classes, not the
  project's own. This was traced through the code, not run.
- Engine's normal route for `X.all()` is a mapped store read, so this in-memory path is an edge case.

**The only upstream test of the extent is PCT `getAll::testBasic`.** It asserts `Class.all()->isEmpty()` is false;
nothing pins the scope further.

## 3. Options considered, and why each was set aside

| Option | Why not |
|---|---|
| Exec keeps a process-wide cache of system stores keyed by content, with bounded LRU eviction | It solves the plumbing, but "we have already lost if we are designing cache eviction strategies" (the user). A catalog has a lifecycle, not an eviction policy. |
| Every caller opens and closes a session source that owns the system stores | Every caller pays for a concern that belongs to the platform. |
| Each metamodel query carries the model's rows inline in its SQL | Purely stateless, but the stress model's rows are 17.65M characters per query. And the product could never use it, so it is a second road. |
| A test-only "ephemeral" mode that inlines rows, beside the product's stateful catalog (the user's first sketch) | A second road (G1, and the test-is-the-product rule). It doesn't scale (above), so it needs a knob to turn it off for large tests, and knobs breed knobs. Its one benefit, tests leaving no state, comes from transactions instead (§4.5). |
| Register every model eagerly in tests | Registration runs inside each test's transaction, so it would be paid per test: about 350 ms × 2,000 stress services for a model nothing reads. |
| `Class.all()` scoped to the query's own compiled model (my first recommendation) | Rejected by the user: "makes no sense to me that Class.all would not give you all the classes". My argument (the answer would depend on what else the process loaded) was wrong. Every database query depends on database state, and plan-as-data reads the plan plus the database's rows. Isolation belongs to WHICH catalog a session sees (a transaction, a test's own run), not to what `Class.all()` means. |

## 4. The design

### 4.1 One catalog per process, per database engine

- **The catalog's tables are created once**, with the platform's rows committed: at startup in the product, on first
  need in a test run.
- **Every table has a model column.** Registering a model inserts rows; it never creates a schema or a table (the H2 rule,
  §1.4). The platform's rows carry the platform's own key.
- **The catalog is an ordinary database.** A query's plan names it as the target it reads, as any plan names a database
  (`ExecutionPlan.Database.System` today). Exec opens it the way it opens any in-process database; nothing in it is keyed
  by a compiled model object.

### 4.2 What `Class.all()` means

**Every class in the catalog the session sees.** In the product that is the platform plus every loaded project,
filtered by what the caller may see (§4.4). In a test, it is the platform plus the models the test registered. That is
legend-pure's meaning, with the catalog standing in for its runtime graph, and a database's meaning (`information_schema`
lists what exists). The query is `SELECT ... FROM classes`: no per-model views, no unions, no filter the planner adds.

### 4.3 The registration plan

- **One plan per model, keyed by the content of its rows.** It inserts that model's rows, tagged with the key. The rows
  travel as row data the bulk loader takes (`SetupStep.Rows`), never as SQL text: on DuckDB through a TEMP staging table
  and an `INSERT ... SELECT`, on H2 as batched inserts.
- **It is derived once per model** (memoized as the seeds are today, about 300 ms for the stress model) and **never
  embedded in a query plan.** The 18M characters of a stress-sized model's rows are one plan, not part of every plan that
  reads it.
- **It is idempotent.** If the key is already registered and visible to the session, it does nothing.

### 4.4 The product

- **A loaded project is registered when it is loaded, and committed.** A browsable global catalog must list projects
  nobody has queried yet. This is the moment `CREATE TABLE` writes a database's catalog.
- **A reloaded project replaces its old version in one transaction**: the old key's rows deleted, the new key's
  inserted. `Class.all()` never shows both.
- **An unloaded project's rows are deleted.**
- **A model compiled for one request** (the ad-hoc model-text query) is registered inside that request's transaction and
  rolled back, as a test's is.
- **Permissions:** a multi-tenant server filters the catalog by what the caller may see, as databases filter
  `information_schema`. This is a feature of its own, designed when the server needs it.

### 4.5 Tests

- **A test's catalog session runs in a transaction that is rolled back at the end.** That is how database test suites
  isolate (Spring's transactional tests, for example).
- **The test registers its model inside that transaction, only when the body's plan reads the catalog.** The planner
  knows this statically; it is how a body is routed to the system store today. This is observably the same as always
  registering: nobody else can see uncommitted rows, and the test's own reads are known at planning.
- **Isolation, with no cleanup.** Another session never sees a test's rows (measured, §1.4), so results don't depend on
  test order or on tests running in parallel. Inside a test, `Class.all()` is the platform plus its model.
- **What each lane pays:** the stress corpus registers nothing (no body reads the catalog); the relational corpus, one
  model for one test; core's metamodel tests, their few rows each. The platform is written once per process.

### 4.6 When rows are inserted

| What | When | Committed? |
|---|---|---|
| The catalog's tables and the platform's rows | once per process, per engine: at startup (product), at first need (tests) | yes |
| A loaded project's rows (product) | always, when it is loaded; replaced on reload, deleted on unload | yes |
| A one-request model's rows (product) | inside that request's transaction | no, rolled back |
| A test's model's rows | inside the test's transaction, only when its plan reads the catalog | no, rolled back |

### 4.7 What it deletes, and what it keeps

- **Deleted:**
  - `SystemDatabase`, its holder on `ModelContext.derived` and its `Cleaner`;
  - the system-session function the door builds from the compiled model;
  - the per-session seed of every table, platform included, which is core's 85%;
  - the query plan's own seed step.
- **Kept:**
  - the planner's routing of a metamodel read to the system store (the resolver's intrinsic store, D1);
  - the generic lowering;
  - `MetamodelSeeds` as the one derivation of the rows, now split by owner (platform, model) instead of per session.

### 4.8 Plan-as-data

- **A metamodel query's run reads its plan, plus the catalog's rows.** It reads that database state as any query reads
  its database. It needs no compiled model and no Java object.
- **A planner-free runner (guard D) can run from files.** Given the catalog's creation plan, a model's registration plan
  and the query plan, all data, it runs from nothing.
- **Same plans, same catalog state, same answer** (G10).

## 5. The register row (proposed text; it lands with the implementation)

| Kind | Upstream | Lite |
|---|---|---|
| scope (a metaclass's extent) | legend-pure: every instance in the runtime's one graph (`Context.getClassifierInstances`, `Metadata.getClassifierInstances`). legend-engine `PureModel`: the classpath's code repositories, the project's own elements not in the extent (`MetadataWrapper.getMetadata(classifier)`). PCT pins only non-empty (`getAll::testBasic`). | every instance in the system catalog the session sees: the platform plus every registered model. The product registers loaded projects; a test registers its model in its own rolled-back transaction. |

## 6. Why the per-model store was where it was

Before plans existed (2026-09-02, "one per graph per engine"), the holder wrote the store as it opened the session. A
per-graph cache gave the right lifetime for free: the store lived as long as the model. Plans moved the writing into a
seed step (2026-11-03), but kept the holder. The holder is what keys the run's database by the compiled model object.

## 7. What it costs to build

- **A catalog owner:** creates the tables and the platform's rows, once per process per engine.
- **The registration plan:** derived per model, by owner, with the DuckDB staging rule.
- **The test door:** opens the catalog session in a transaction and rolls it back; registers the model when a plan reads
  the catalog.
- **The product's load, reload and unload hooks:** they run the registration plan, or delete the model's rows.
- **Removing `SystemDatabase`** and the door's system-session function.
- **The register row.** Core's metamodel tests should not change their answers, since a test still sees the platform
  plus its own model.

## 8. Checks before implementation, most important first

1. **DDL inside a test's catalog transaction on H2.** H2 commits on DDL. If a metamodel-reading body sends DDL on the
   catalog's session (a staged grid's table, a temp table), its uncommitted rows get committed and leak into later tests.
   Check what those bodies send on that session. If any DDL is sent, it goes to another session, or the body is refused
   on H2 by name.
2. **Reload in the product:** delete the old key's rows and insert the new key's in one transaction, under concurrent
   readers.
3. **Server engines** (Postgres, MariaDB, Snowflake and the rest): today's system store is session-TEMPORARY tables on
   the host's system connection. The catalog's persistent tables, transactions and staging must be checked per engine.
4. **Typed columns:** re-time registration with the real catalog's column types.
5. **Concurrency of a first registration in the product** (two requests reading a newly loaded model): the load commits
   before the model is served, so it does not arise. Check it anyway.

## 9. Placement in the plan, and the interim (OPEN, for the user)

- **Interim:** the model-keyed `SystemDatabase` stays, recorded as the one out-of-plan channel left (PLAN_AS_DATA section
  10, step 8c). Behaviour is unchanged until this lands.
- **This design removes one of the three things that had no home in the package split** (step 10). The other two:
  - the router's sequencing, which belongs in a thin test door;
  - the referee's read of the model (it decodes enum columns from it), whose facts can be planned into the appeal record
    as data.

  See PLAN_AS_DATA section 10, "Step 10's first attempt".
- **The order is to decide:** this before step 10, inside it, or after it. Also whether core's 85% (the cost half) and the
  out-of-plan channel (the invariant half) land together.

## 10. Lessons from the session that produced this

- **Measure a big model before designing by small ones.** Every option looked fine on the two-class model. The stress
  model (18M characters) is what disproved the inline option.
- **Read upstream; never recall it.** The engine's extent excluding the project's own classes was not what anyone
  expected.
- **A rule that "doesn't fit" is a question for the user, not a reason to build around it.** The system store's sessions
  needed the model and had no home in either half, and instead of asking I made a package for it (step 10's first
  attempt).
- **Isolation is a database's job.** Transactions gave the tests what a test-only mode was invented for.
