# Deephaven as a database of legend-lite: the design (2026-10-22, revised)

**Status: PROPOSED. Nothing here is built.** Revised the same day on the user's ruling: **Deephaven is just another
backend.** A user builds a `###Relational` store, mappings and classes, and everything works: the Relation API
straight on the store's tables, `project()` over class navigation. No store of its own, no table kinds. The first
version of this document (a `###Deephaven` store with source and input tables, `LONG`/`DATE`/`InputTable` grammar
additions, a shared `ServerUrl`) is superseded; git history keeps it (`26e37fc3`). What carries over unchanged: the
architecture agreed with the MongoDB line (§8), and DataCube's ticking mode agreed with the DataCube + Python line
(§10). **legend-engine's Deephaven extension is not the reference** (the user, 2026-10-22: half built, does not work);
its `###Deephaven` section and `DeephavenConnection` stay carried as today (PARK-38), unused.

## 0. The answer in one paragraph

A Deephaven server is a database of legend-lite. Its tables are declared in an ordinary `###Relational` store, with
the types, `PRIMARY KEY` and `NOT NULL` that store already has, and the same store can be bound to DuckDB in one
runtime and to Deephaven in another. The connection is `RelationalDatabaseConnection { type: Deephaven; ... }`, with
a connection specification and an authentication of its own, as Snowflake has its own. A query on a Deephaven
runtime is planned by **a render target over the same `SqlQuery` IR**, `sql/dialect/Deephaven.java`, implementing
`SqlDialect` whole as `MongoDb` does, which writes Deephaven's table operations (its `BatchTableRequest`) instead of
SQL text: Deephaven's own SQL front end handled 8 of DataCube's 14 query shapes, its table operations all 14. A
**connector in `core/src/main/deephaven`**, found through `ServiceLoader`, runs the plan: **typed rows** for a
snapshot, and, the one thing Deephaven adds, **a live handle** for a caller that subscribes. Live or snapshot is how a
caller executes a plan, not what was planned. Reference data gets into Deephaven the platform's own way: read from its
source of record on one runtime, **written through the same modelled table** on the Deephaven runtime.

## 1. Where Deephaven is not like the other databases

None of these touches the model. Each is decided below, where it lands.

1. **Results tick.** A subscription keeps answering until it is closed (DH10).
2. **One query runs on one database**, as everywhere in legend-lite, and Deephaven joins only tables it holds in
   memory (its update graph recomputes what a change touches, so every input is local). So reference data is copied
   in: written into Deephaven by legend-lite from its source of record (§6, §7).
3. **Deephaven keeps nothing across a restart** (Community: memory only; measured: back in about 3 s, empty). The
   Deephaven copy is a cache with a declared shape: the publisher recreates and reloads it (3 to 7 s at the spike's
   size); a query against a missing table is refused by name, never answered empty (DH19).
4. **Only the tables legend-lite creates are writable.** A table with a primary key becomes a keyed input table, one
   without an append-only input table; a table another program publishes on the server (a market-data feed from
   Kafka) is read-only. In the store both are tables; the difference is the server's, found at run time (DH15, DH16).
5. **The primary key does work.** Deephaven's fast join (`natural_join`) needs a unique key on the right and errors on
   a duplicate; the declared key is what lets the dialect choose it (DH20).
6. **Values behave differently.** Nulls of primitive columns are sentinel values; formulas are Java's (integer
   division, strings, dates, NaN); decimals are `BigDecimal` with fewer operations. As every dialect: measured on
   lanes, refused by name, a register row each (DH7).
7. **Results are typed rows only** until host renderers exist (DH9).
8. **The license shapes deployment** (Deephaven Community License): the connector never ships in `//core:drivers`;
   and since creating a table creates an input table, only internal users may run DDL against Deephaven (§3a, §4 of
   the license; DH11, DH15).

## 2. What exists

- **lite today**: `###Relational` stores (`DatabaseDefinition`; `ColumnDefinition` carries `primaryKey` and
  `notNull`), mappings, classes, runtimes binding a store to a connection; `RelationalDatabaseConnection` with lite's
  own types (`SQLite`, `MariaDB`, `MySQL`: `ConnectionSectionGrammar.LITE_DATABASE_TYPES`, refused at the engine
  dialect); DDL from the store's declaration (`setup/Ddl`, the `dropAndCreateTableInDb` boundary); Pure's
  `write(relation, #>{db.table}#)` type-checked (`WriteChecker`) but **not lowered to a store table**
  (`Lowerer.java:3291` refuses: "TypedWrite to a store destination is not yet implemented"); Pure's `asOfJoin`.
- **The spikes** (`experiments/deephaven-spike`, `experiments/streaming-risk-spike`): a plan written as
  `BatchTableRequest` JSON with no Deephaven library, run by a 200-line connector, subscribed from a browser through
  `dh-core.js`; and the homework that found Deephaven right on every test (deletes, as-of joins with late data, all 14
  DataCube query shapes as table operations, to 384,000 positions). This design's evidence.

## 3. The store: nothing new

A Deephaven table is a `###Relational` table:

```
###Relational
Database refdata::Ref
(
  Table instrument ( id VARCHAR(32) PRIMARY KEY, ccy VARCHAR(3) NOT NULL, multiplier DOUBLE, sector VARCHAR(64) )
  Table book ( id VARCHAR(32) PRIMARY KEY, desk VARCHAR(64) NOT NULL, trader VARCHAR(64) )
  Table position ( book VARCHAR(32) PRIMARY KEY, instrument VARCHAR(32) PRIMARY KEY, qty BIGINT NOT NULL )
  Join position_instrument ( position.instrument = instrument.id )
  Join position_book ( position.book = book.id )
)
```

Mappings and classes over it are what they are for any store. Which column types Deephaven can hold is the
dialect's knowledge, checked when a runtime binds a store to a Deephaven connection (the model door: binding is
knowledge, so it is checked eagerly), each refusal naming the table, the column and the type:

| store type | Deephaven column | Pure type |
|---|---|---|
| `BIT` | `Boolean` | Boolean |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `byte`, `short`, `int`, `long` | Integer |
| `REAL` | `float` | Float |
| `FLOAT`, `DOUBLE` | `double` | Float |
| `DECIMAL(p, s)`, `NUMERIC(p, s)` | `BigDecimal` | Decimal |
| `VARCHAR(n)`, `CHAR(n)` | `String` (the size not enforced, as DuckDB) | String |
| `DATE` | `LocalDate` | StrictDate |
| `TIME` | `LocalTime` | (its Pure type, S64) |
| `TIMESTAMP` | `Instant` | DateTime |
| every other (`BINARY`, `VARBINARY`, `ARRAY`, semi-structured, `OTHER`, `DISTINCT`, `TIMESTAMP_NS`) | refused until a case | |

The table is proposed; L3's lanes measure it. Nullability is the store's: a column is nullable unless `NOT NULL`, as
a Deephaven column is.

**Names on the server.** Deephaven has one namespace of tables. A table in a store's default schema is that name on
the server. A named schema is refused at the same binding check until a case decides it (candidates: a Deephaven
application per schema, read-only; a name prefix).

## 4. The connection

```
###Connection
RelationalDatabaseConnection risk::live::Conn
{
  store: refdata::Ref;
  type: Deephaven;
  specification: Deephaven { host: 'deephaven.internal'; port: 10000; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
```

- **`type: Deephaven`** is a lite type, joining `LITE_DATABASE_TYPES`; at `Dialect.LEGEND_ENGINE` (rule A, S89) it is
  refused by name and positioned, as `SQLite` is.
- **`ConnectionSpecification.Deephaven(host, port, useTls)`**, every field required (invariant 4: no default port, no
  default TLS). lite-only, refused at the engine dialect.
- **`AuthenticationSpec.PreSharedKey(keyVaultReference)`**: the model names the key in the vault, never the key
  (Charter C3b, as every connection's credentials). A user's own identity (OIDC or a bearer token, so Deephaven sees
  who asks) is the next authentication, when a deployment needs per-user access.
- Through the parser, the protocol records, the composer and the JSON as every connection; one `EngineDropsTest`-shaped
  test per construct (the lite dialect reads it and the round trip keeps it; the engine dialect refuses it);
  `//gates:parser_equivalence` guards that no engine text parses differently.

## 5. Decisions

Alongside the MongoDB line's (the user's rulings of 2026-10-16), so the two non-SQL databases share one shape:

| # | question | Mongo (decided) | Deephaven |
|---|---|---|---|
| DH1 | the compiler's shape | D1: a target over the `SqlQuery` IR | **the same** |
| DH2 | the seam's type | D2: `implements SqlDialect` whole; the `QueryTarget` split if the refusal list grows | **the same**, SQL-only members refused by name; the split taken up with the Mongo line from two real targets |
| DH3 | the surface | D3: the Relation surface first | **everything a relational store serves**: the Relation API on the store's tables and class queries through mappings, since both lower to the same IR. The dialect's coverage is the only limit; first the shapes DataCube writes |
| DH4 | the registry key | D4: `DatabaseType.MongoDB` | **`DatabaseType.Deephaven`** in `RelationalDatabaseConnection`, a lite type; every exhaustive switch gains an arm |
| DH5 | parameters | D5: placeholders, typed slots, the driver binds | **the same contract**: the connector binds a value as the request's own structured literal, never spliced into formula text |
| DH6 | typing | D6: the declared schema only | **the store's declared columns only** (§3) |
| DH7 | nulls and values | D7: SQL's semantics, emulated | **SQL's semantics.** Sentinel nulls measured on the first lane, emulated where an operand may be null; a value equal to a primitive column's sentinel refused at write, by name. Formula differences found on the lanes become register rows |
| DH8 | the test server | D8: a pinned `mongod`, Bazel-downloaded | **a pinned Deephaven server**, Bazel-downloaded from Maven Central, started by the test (no Docker); test-only, never on a product classpath |
| DH9 | what a result is | D10: database-written text | **typed rows** (Arrow Flight's cells, `Statements.rows`); a text output refused by name until host renderers exist (`docs/RESULTS_AND_RENDERERS_2026_10_22.md`, proposed) |
| DH10 | live results | (none) | **an execution mode, not a plan node**: `LiveStatements.subscribe(statement, slots, values, columns) → LiveHandle` beside `Statements`; the handle carries the server, the ticket, the column names, and `close`/cancel |
| DH11 | where the connector lives | D12: `core/src/main/mongodb`, bundled | **`core/src/main/deephaven`, `ServiceLoader`, not bundled**: Deephaven's client is under the Deephaven Community License, so the shipped product never carries it; an opt-in server target adds it for a deployment that fetches Deephaven itself; its jars from a `maven.install` pool of their own |
| DH12 | the connection's address | D4: a specification variant | **`ConnectionSpecification.Deephaven(host, port, useTls)`** (§4). Supersedes the shared `ServerUrl` agreed in §8's first version: Deephaven no longer needs it |
| DH13 | authentication | a vault reference | **`PreSharedKey(keyVaultReference)`** (§4) |
| DH14 | the metamodel store | D13: refused by name | **refused by name** |
| DH15 | creating tables | (none) | **the store's DDL**: a table with a primary key becomes a keyed input table (its key the declared key), one without an append-only input table. Internal users only (DH11) |
| DH16 | a table legend-lite did not create | (none) | **checked when a session first reads it**: the server's columns, types and nullability against the declaration, a mismatch refused by name; a write to it refused by name |
| DH17 | writes | (Mongo's own) | **the platform's write step (§6)**, rendered as an input table's add (replacing the row of the same key, on a keyed table) and delete by key |
| DH18 | names on the server | (none) | **the default schema's names**; a named schema refused until a case (§3) |
| DH19 | a restart | (none) | **refused by name**: a table missing from the server is reported as missing, never recreated empty by the connector; the publisher recreates and reloads (§7) |
| DH20 | joins | (Mongo's own) | **`natural_join` where the right side's key is unique** (its declared key covers the join columns, or it was grouped by them); the general `join` for an inner join otherwise; `asOfJoin` as `aj`; a to-many outer join (Deephaven's outer joins are experimental) and a non-equi join refused by name until measured |

## 6. Writes: one platform step, not Deephaven's

Writing a relation into a store table is platform work every database needs, and three lines want it:

- **BI/ETL**: F1 of `docs/BI_AND_ETL_PLAN_2026_09_29.md` (write a relation into a table, create from a relation's
  type, replace, merge by key, in one transaction), designed, not built.
- **Cypher**: `CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`, refused by name today (312 conformance scenarios,
  `docs/CYPHER_STUDY_2026_10_15.md`). Cypher writes objects (a node is an instance, a relationship an association),
  which is writing through a mapping. A user modelling Cypher's writes has been asked to push that work to a branch
  (2026-10-22); the write step's vocabulary waits for it.
- **Deephaven**: replace by key, delete by key, append.

What this line needs from it, and proposes:

- **The Pure surface** is Pure's own `write(relation, #>{db.table}#)`, lowered to a store table (the stub at
  `Lowerer.java:3291` replaced), plus whatever the shared design adds for replace, merge by key and **delete** (F1 has
  no delete; Cypher's design probably settles it).
- **Writes address tables.** If the shared design writes objects, an object write becomes table writes before any
  dialect sees it, so Deephaven's dialect only ever renders table writes.
- **On Deephaven**: an add to a keyed input table replaces the row of the same key (no duplicate-key error), a delete
  addresses keys, and there is no transaction across statements; each a register row, and whatever of the shared
  design Deephaven cannot keep (an atomic replace) refused by name.

The write step is its own landing (W, §9), owned by whoever builds F1 with the Cypher work, reviewed by this line.
Nothing before L3's writes waits on it: the model, reads, creating tables and live results land first.

## 7. The demo world and the publisher

- **One store, two runtimes.** `refdata::Ref` (§3) with classes (`Instrument`, `Book`, `Position`) and mappings, bound
  to DuckDB in `refdata::SourceRuntime` (the source of record) and to Deephaven in `risk::LiveRuntime` (the published
  copy). Market data (`price` keyed by instrument, `fx` keyed by currency, `quote` append-only for as-of joins) is in
  the same store or one including it, so the risk join is declared like any join.
- **The publisher** is legend-lite queries, no Deephaven code: for each reference table, create it on the Deephaven
  runtime from the store's DDL, read it on the source runtime, write the rows into the same table on the Deephaven
  runtime (two queries: one query, one database; the rows pass between them as typed values, the compiler checking
  them against the table). On a Deephaven restart it does the same again (DH19).
- **The market-data simulator** writes `price` and `fx` through the same modelled tables in the demo. In production,
  market data would arrive from Kafka straight into Deephaven: read-only tables, still declared in the store (DH16).
- **The risk view** is a legend-lite query (the Relation API or a class `project()`), subscribed by DataCube. Deephaven
  computes it live; identical views of one table are one table on the server, so viewers cost nothing measurable.

## 8. Agreed with the MongoDB line (2026-10-22)

1. ~~`ConnectionSpecification.ServerUrl` as a shared record~~: **withdrawn by this revision** (DH12). Mongo's own
   address is unaffected; the line is told.
2. **No store kind on `DatabaseDefinition`**: now simply true, since a Deephaven store is a `###Relational` store.
3. **Typed rows only, text outputs refused by name.** `RESULTS_AND_RENDERERS_2026_10_22` is proposed and unruled.
4. **`LiveStatements` beside `Statements`, no plan node**; the handle defines `close` and cancel.
5. **The connector absent by default**, found as Mongo's is (`Connectors.of(type)` by `ServiceLoader`, an `Unbuilt`
   refusing by name when none is on the class path), loaded from the opt-in server target. From the Mongo merge:
   `TestBackendsTest` wants a `DeephavenBackend` once the type has a dialect, and the tests iterating
   `Databases.implemented()` need the non-SQL filter Mongo's merge lands, reused.
6. **The `QueryTarget` split** taken up together once the Mongo branch is on main.
7. **Guard and registry edits additive**, each dated; whoever lands second re-adds. After the Mongo merge,
   `DatabaseConnector` gains `ownSessions()` and `open(ConnectionDefinition)`, and the connection codec is
   `model/ConnectionWire`: L3 uses those.

## 9. The landings (stacked branches, no PR until asked)

| # | branch | content | judge |
|---|---|---|---|
| L1 | `claude/deephaven-1-model` (on main) | `DatabaseType.Deephaven` (a lite type) and its switch arms, each refusing by name until L2/L3; `ConnectionSpecification.Deephaven`, `AuthenticationSpec.PreSharedKey`: parser (both dialects), protocol records, composer, JSON, `ConnectionWire`'s tags, `ConnectorsTest`'s rows; the binding check (§3's types, DH18), each refusal a `ReferenceIntegrityTest` case through both doors; register rows | `//gates:local`, `//gates:parser_equivalence`; the Studio line reviews the grammar |
| L2 | `claude/deephaven-2-target` (on L1) | `sql/dialect/Deephaven.java` (`SqlDialect` whole): reads first, the shapes DataCube writes (scan, filter, project, extend, group-by, sort, limit, joins per DH20), then class queries through mappings; DDL as input tables (DH15); every other IR node, function and aggregate refused by name; unit tests on the plan text, as `MongoDbDialectTest` | the dialect's tests; every existing lane's dump unchanged |
| L3 | `claude/deephaven-3-connector` (on L2, after the Mongo branch merges its seam) | `core/src/main/deephaven`: `rows` over Arrow Flight, `subscribe` with its handle, creating tables, the schema check (DH16), the missing-table refusal (DH19); the `@maven_deephaven` pool; the pinned test server and `DeephavenBackend`; the opt-in server target; the guards' additive lines, each dated | lite's own Deephaven cases, answered on DuckDB and on Deephaven and compared row for row; then the PCT lanes, as every database's |
| W | (the write step's own branch) | §6: writes to store tables on every database, with the Cypher author's design and F1 | its owner's tests on every dialect; this line reviews |
| L3w | on L3 and W | Deephaven's write arms (DH17) | the write cases, DuckDB against Deephaven |
| L4 | `claude/deephaven-4-datacube`, stacked on the DataCube line's `claude/datacube-query-base` (da0f5aec1) | DataCube's ticking mode (§10) | the DataCube line reviews |
| L5 | a demo module | §7: the store, its classes and mappings, the two runtimes, the publisher, the simulator | runs, and its risk agrees with DuckDB's over the same inputs |

## 10. DataCube's ticking mode (agreed with the DataCube + Python line, 2026-10-22; unchanged by the revision)

The user's shape: built here on a branch stacked on the DataCube line's, reviewed by that line.

- **Where it plugs in.** Every grid's rows go through one `QueryRunner`, fixed when the cube is built (`PlanThenRun`
  in the tab, `pythonRunner` on a page Python serves). A Deephaven runtime is another runner chosen at cube build, and
  a `subscribe` beside `QueryEngine.execute` answers the live handle (DH10).
- **The tick.** A `ticked` event, small and outside the transaction and undo machinery, so it survives the state
  owner's coming rework (the single writer's client side, after the line's S7): it replaces the grid's derived rows
  and nothing else. It never writes the cube or page document, never adds a command, never adds a page write: the
  one-API ledgers (`datacube/test/one-api-ledger.test.ts`, `one-api-gestures.test.ts`, `python/tests/test_one_api.py`)
  do not grow.
- **The first slice.** One grouping level, sums, no pivot, no tree expansion; charts out of it (a chart reads its own
  queries through its grid's runner, and per-view scales would rescale on every tick). Rows are matched by group key,
  since a sorted live table delivers a changed row as a removal and an addition.
- **Loading.** The ticking code, and Deephaven's `dh-core.js` (fetched from the Deephaven server at run time), load
  lazily, as the join and chart chunks do (`test/bundle-budget.test.ts`).
- **The rules.** The database executes and the client only arranges rows; no DataCube rule in Python (a Python side
  calls the TypeScript headless).
- **Stable to build on**: the command API (`grid-commands.ts`, `page-commands.ts`), the cube and page documents, the
  page keeper's write protocol, `DataGrid`, `treeview`. **Moving**: Python's command layer (S6, little overlap).

## 11. Register rows (`docs/SEMANTICS_REGISTER.md`: the next number free on main when L1 lands; main holds up to S96, the MongoDB branch S97-S104, so the number is checked at landing)

1. **Deephaven is lite's own design**, not legend-engine's Deephaven extension: a `###Relational` store, `type:
   Deephaven`, its specification and pre-shared key as lite's, execution by table operations (this document).
2. On Deephaven, a write into a keyed table replaces the row of the same key; there is no transaction across
   statements (with W, L3w).
3. On Deephaven, a value equal to a primitive column's null sentinel cannot be stored, and is refused (L3).
4. A Deephaven result is typed rows; its served text forms are refused until host renderers exist.
5. Formula semantics that differ from SQL's, one row each as the lanes find them (L3).
