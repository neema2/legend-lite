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
target's knowledge, as every dialect's types are: the target refuses a column it cannot hold when it plans a query or
creates a table over it, naming the table, the column and the type. **Not the model door** (2026-10-22): the model
carries a database's name and decides nothing by it (D9, the dialect program's census, which refused L1's first
draft of a binding check in `ModelIntegrity` and a type, specification and authentication check in `FromProtocol`);
a mismatched connection is the connector's to refuse when a session opens. The table:

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
the server. A named schema is refused by the target until a case decides it (candidates: a Deephaven application per
schema, read-only; a name prefix).

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
- Through the parser and the protocol records as every connection, and the lite plan format (`ConnectionWire`) as
  every connection the runner opens. **No legend-engine JSON and no composer rule**: legend-engine's protocol has no
  field for either word, so each is refused by name there, as SQLite's specification is (S89: "refused by name where it
  has none"; `ProtocolEmitter.liteOnly`). Tests: `parser/DeephavenConnectionTest` (lite's dialect reads it; the
  engine's refuses each word at its token; every key required, an unknown or repeated key refused; a port is a TCP
  port; a mismatch held as written (D9); the JSON refusal); `//gates:parser_equivalence` guards that no engine text parses differently.

**L1's precedents** (the Studio line's review of the revision, 2026-10-22; all in
`parser/section/ConnectionSectionGrammar.java`):

1. `type: Deephaven` joins `LITE_DATABASE_TYPES`; the existing gate refuses it at `Dialect.LEGEND_ENGINE` in the
   engine's own words ("Unknown database type 'Deephaven'"), at the token.
2. The specification parses as `parseSqliteSpec` does: `refusesLiteExtensions()` first ("unsupported datasource
   specification: Deephaven"), then the key loop with `TokenStreamCursor.once` (a repeated key refused), an unknown key
   refused by name, a missing key refused by name. `host` and `port` are the engine's network specs' words; for TLS the
   engine has no shared word (Trino's `sslSpecification`, Databricks' `protocol`), and `useTls` is chosen because it
   says what it does.
3. `PreSharedKey { keyVaultReference }` follows the relational strategies' naming (`passwordVaultReference`,
   `privateKeyVaultReference`). It is **not** the engine's `#PSK{ psk: '...'; }#` island (`Protocol.PPskAuth`, JSON
   `_type` `PSK`), which carries the key itself: a record of its own, so a reference never reads as a key.
4. The extension is declared: a `LITE-DESIGN-deephaven-backend` family in `docs/OWN_CORPUS_DECISIONS.md`, modelled on
   §9 `LITE-DESIGN-sqlite-backend`, so `OwnCorpusConformanceTest` classifies own-corpus text that uses it.
5. The JSON (rule A): the Studio line asked for `_type`s of their own read back in `protocol/ConnectionReader`; L1 follows
   the code's precedent instead (SQLite's specification, `ProtocolEmitter.liteOnly`, S89's "refused by name where it
   has none"): legend-engine's protocol has no field for either word, so the emitter and the composer refuse each by
   name. The Studio line is told; a lite JSON of its own, if a consumer needs one, is a decision of its own.

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
| DH17 | writes | (none planned: test-data loads only) | **the Cypher writes line's four verbs (§6)**, each a table-operation plan then one input-table add or delete; `write` into a keyed table and every verb but `write` on an append-only one refused by name; one write per program |
| DH18 | names on the server | (none) | **the default schema's names**; a named schema refused until a case (§3) |
| DH19 | a restart | (none) | **refused by name**: a table missing from the server is reported as missing, never recreated empty by the connector; the publisher recreates and reloads (§7) |
| DH20 | joins | (Mongo's own) | **`natural_join` where the right side's key is unique** (its declared key covers the join columns, or it was grouped by them); the general `join` for an inner join otherwise; `asOfJoin` as `aj`; a to-many outer join (Deephaven's outer joins are experimental) and a non-equi join refused by name until measured |

## 6. Writes: the Cypher line's write design, rendered on Deephaven

Writing into store tables is platform work, and it exists: the Cypher writes line built it
(`docs/WRITES_DESIGN_2026_10_18.md`, branch `claude/cypher-writes`, not on `main` yet: eight files conflict, none in
the write path's core) and wrote a note for this line (`docs/WRITES_FOR_STORE_BACKENDS_2026_10_22.md` on that branch).
It answers what this section left open:

- **Four table verbs (L1)**, each answering the rows the database affected: append (`rel->write(#>{db.t}#)`), update
  (`#>{db.t}#->update(r | pred, ~[c: r | expr])`), delete (`#>{db.t}#->delete(r | pred)`), merge
  (`rel->merge(#>{db.t}#, ~[keys])`); and `stage()`, rows computed once into a session table a program's later
  statements read. The MIR is `SqlDml` (`InsertSelect`, `Update`, `DeleteWhere`, `Merge`, `CreateTempTableAs`), pure
  data: **a new backend is a render arm per record, or a refusal by name.**
- **Class writes (L2)** over a direct mapping (one table, one-column key) are rewritten by the store resolver into L1
  (`resolver/ClassWrites`), so a dialect only ever sees table writes, as this section asked. Cypher and `#SQL` DML land
  on the same verbs.
- **Typing by name** (S97): each column of the rows names a column of the table; `merge`'s keys are columns of both.
- **All-or-nothing:** a program that writes is one transaction, on one target; the plan path runs it as a `Sequence`
  of `Update` and `Check` steps on one session. A write across two databases is not covered.

**On Deephaven** (DH17), a keyed input table takes two operations, an add (replacing the row of the same key) and a
delete by key, and an append-only input table takes an add. Each verb is a table-operation plan computed on the server,
then one add or delete of its result:

| verb | keyed input table | append-only input table |
|---|---|---|
| `write` (append) | **refused by name**: an add would replace a row whose key exists, where SQL's insert fails; `merge` says what Deephaven does | the add of the rows |
| `merge` | keys exactly the table's declared key, else refused by name. The rows naming every column: their add. Naming fewer: the rows natural-joined to the table for the columns they omit, then the add. Every column a key: the rows the table lacks (`where_not_in`), then the add | refused by name |
| `update` | the table's rows matching the predicate, the assignments applied (`update_view`), then the add; an assignment to a key column refused by name | refused by name |
| `delete` | the keys of the matching rows (`where`, then the key columns), then the delete | refused by name |
| `stage` | a table held by the session (an export), released when the program ends | |
| a `Check` step; minted ids (D4) | refused by name until a case | |

The count a verb answers is the size of the table added or deleted, which the server reports with the export, so
Deephaven counts, not Java.

**What Deephaven cannot keep**, each refused by name or registered, never papered over:

1. **No transaction.** Each add or delete applies on its own (in one update cycle: to measure on L3w). A program with
   more than one write is refused by name on Deephaven; a caller that wants two writes runs two programs, visibly not
   atomic. A single verb is the server's compute-then-add, so a partial `merge` or an `update` racing another writer
   can lose that writer's change: registered, as S100's ids are under concurrent writers.
2. **No defaults:** a `merge` inserting a row whose omitted column is `NOT NULL` is refused by the server's input
   table, and the connector names it.

**The gap for this line: rows from outside a database.** The design's parameters are values (a write binds them in
its statement); a write's rows come from a query of the same target. The publisher (§7) and the simulator write rows
read elsewhere (from DuckDB, from a feed), so they need a **relation-valued parameter**: a slot whose value is typed
rows, checked against the plan's columns, carried (never computed) by the connector into the target. For Deephaven
that is an Arrow upload (`DoPut`), for a SQL database the bulk staging `exec.BulkLoad` already does. To propose to the
Cypher writes line, since it extends their parameter contract.

The write step's landing is the Cypher writes branch reaching `main` (W, §9). Nothing before L3's writes waits on it:
the model, reads, creating tables and live results land first.

## 7. The demo world and the publisher

- **One store, two runtimes.** `refdata::Ref` (§3) with classes (`Instrument`, `Book`, `Position`) and mappings, bound
  to DuckDB in `refdata::SourceRuntime` (the source of record) and to Deephaven in `risk::LiveRuntime` (the published
  copy). Market data (`price` keyed by instrument, `fx` keyed by currency, `quote` append-only for as-of joins) is in
  the same store or one including it, so the risk join is declared like any join.
- **The publisher** is legend-lite queries, no Deephaven code: for each reference table, create it on the Deephaven
  runtime from the store's DDL, read it on the source runtime, write the rows into the same table on the Deephaven
  runtime (two queries: one query, one database; the rows pass between them as typed values through the
  relation-valued parameter, §6's gap, the compiler checking them against the table). On a Deephaven restart it does
  the same again (DH19). Kept in step afterwards: a `merge` of the source's rows, then a `delete` of the keys the
  source no longer holds, as two programs (§6: one write per program on Deephaven).
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

## 9. The landings, in the order agreed (the user, 2026-10-22: "do everything, then a fake UI until the DataCube session has cleaned up")

A ticking grid comes early, before writes and cross-store land, so the live experience is judged first; the modelled
publisher replaces the demo's seeding once they do. Stacked branches, no PR until asked.

| # | branch | content | judge |
|---|---|---|---|
| L1 | `claude/deephaven-1-model` (on main) | `DatabaseType.Deephaven` (a lite type) and its switch arms, each refusing by name until L2/L3; `ConnectionSpecification.Deephaven`, `AuthenticationSpec.PreSharedKey`: parser (both dialects), protocol records, `ConnectionWire`'s tags, `ConnectorsTest`'s rows; no legend-engine JSON and no composer rule (each refused by name, as SQLite's specification: S89's "refused by name where it has none"); no decision at the model door (D9: the connector refuses a mismatch, the target a store it cannot hold, L2); S48 extended; `LITE-DESIGN-deephaven-backend` declared | `//gates:local`, `//gates:parser_equivalence`; the Studio line reviews the grammar |
| L2 | `claude/deephaven-2-target` (on L1 merged with the MongoDB branch, whose dialect hooks a non-SQL target uses) | `sql/dialect/Deephaven.java` (`SqlDialect` whole), designed against the IR DataCube's 14 query shapes actually lower to, then class queries through mappings; **a parameter as a one-row table joined in** (a formula reads its column; no value is ever written into formula text, DH5); DDL as input tables (DH15); a column type (§3), a schema or a name the server cannot hold refused by name; every other IR node, function and aggregate refused by name; unit tests on the plan text, as `MongoDbDialectTest` | the dialect's tests; every existing lane's dump unchanged |
| L3 | `claude/deephaven-3-connector` (on L2 merged with the Mongo branch, whose connector seam it uses) | `core/src/main/deephaven`: `rows` over Arrow Flight, `subscribe` with its handle, creating tables, the schema check (DH16), the missing-table refusal (DH19); the `@maven_deephaven` pool; the pinned test server and `DeephavenBackend`; the opt-in server target; the guards' additive lines, each dated | lite's own Deephaven cases, answered on DuckDB and on Deephaven and compared row for row; then the PCT lanes, as every database's |
| D0 | a demo module (on L3) | **the first demo**: the store, classes and mappings of §7; reference data and ticks loaded the way tests seed data (marked temporary); the simulator; a **restart watcher** that re-seeds after a Deephaven restart and tells subscribers; **a plain live-grid page** (dh-core.js from the server) standing in for DataCube | a ticking grid whose values agree with DuckDB's over the same inputs; a restart survived |
| W | `claude/cypher-writes` reaching `main` (the Cypher writes line's landing, its owner's call) | §6: the four verbs, staging, class writes, write plans | its own gates |
| Wr | on main after W (built here, reviewed by the Cypher writes line) | the relation slot: a parameter whose value is typed rows, staged by the runner (§6's gap) | its own tests on DuckDB, H2, Postgres |
| X | on Wr | **cross-store with explicit boundaries**: each `from(runtime)` a split, the inner part planned on its runtime and its rows carried into the outer part's slot; refused by name inside a live plan and for writes to two databases | a read on DuckDB merged into Deephaven as one program |
| L3w | on L3, W and Wr | Deephaven's write arms (DH17, §6's table) | the write cases, DuckDB against Deephaven |
| L5 | on D0, X and L3w | the modelled publisher (§7) replaces D0's seeding | D0's checks, with no seeding |
| L4 | `claude/deephaven-4-datacube`, stacked on the DataCube line's branch once it has settled | DataCube's ticking mode (§10) replaces the plain page | the DataCube line reviews |

**L2, measured (2026-10-23):** the target's batches for 18 query shapes (DataCube's pivot as conditional sums, its
grand total over a literal key and over no rows, distinct, top-N, nested groupBy-extend, avg, count distinct, std,
median, left and inner joins with null keys, three-valued NOT and OR, a boolean column, IN, IS NULL, a HAVING-style
filter), run on a Deephaven 42.7 server (its release tarball, Java 21) over 200 rows with about 12% nulls in every
column, gave DuckDB's rows on all 18 (the harness: a scratch check through the protocol's own stubs, which L3's
differential test replaces). Two shapes were the compiler's refusals, not the target's.

**Before internal users rely on it:** the firm's counsel reviews the deployment against the Deephaven Community
License (this line's reading, `experiments/deephaven-spike/README.md`, is not legal advice).

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

## 11. Register rows (`docs/SEMANTICS_REGISTER.md`: L1 extends S48, lite's database types; the rows below take the next number free on main when their landing lands, since the MongoDB and Cypher writes branches both claim S97 onward)

1. **Deephaven is lite's own design**, not legend-engine's Deephaven extension: a `###Relational` store, `type:
   Deephaven`, its specification and pre-shared key as lite's, execution by table operations (this document).
2. On Deephaven, a program writes at most once (no transaction), and a partial `merge` or an `update` is a
   compute-then-add that a concurrent writer can race (L3w).
3. On Deephaven, a value equal to a primitive column's null sentinel cannot be stored, and is refused (L3).
4. A Deephaven result is typed rows; its served text forms are refused until host renderers exist.
5. Formula semantics that differ from SQL's, one row each as the lanes find them (L3).
