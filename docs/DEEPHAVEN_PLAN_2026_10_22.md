# Deephaven as a database of legend-lite: the design (2026-10-22, revised)

> **A session picking this line up starts at `docs/DEEPHAVEN_START_HERE.md`** (where things stand, how to run and
> check it, what is open). This document is the record.

**Status: L1, L2, L3b, D0, J1 (DataCube's own app on Deephaven, DH23), T1 (real ticking, DH24), F1 (filters the
server reads without compiling, DH25), B1, G1, and V1-V3 (a flat grid of any size, a page of several grids, charts
that tick: DH27), P1 (pivots the server compiles nothing for), O1 and N1 (a gesture's time, and over a network),
B2 (the page saved, opened and restarted) and C2 (a chart's scale held while it ticks) built, and A1 (the
single-user audit, docs/DEEPHAVEN_SINGLE_USER_AUDIT_2026_10_24.md) done (their branches, §9); the rest PROPOSED.** Revised the same day on the user's ruling:
**Deephaven is just another backend.** A user builds a `###Relational` store, mappings and classes, and everything
works: the Relation API straight on the store's tables, `project()` over class navigation. No store of its own, no
table kinds. The first version of this document (a `###Deephaven` store with source and input tables,
`LONG`/`DATE`/`InputTable` grammar additions, a shared `ServerUrl`) is superseded; git history keeps it (`26e37fc3`).
What carries over unchanged: the architecture agreed with the MongoDB line (§8), and DataCube's ticking mode agreed
with the DataCube + Python line (§10). **legend-engine's Deephaven extension is not the reference** (the user,
2026-10-22: half built, does not work); its `###Deephaven` section and `DeephavenConnection` stay carried as today
(PARK-38), unused.

## 0. The answer in one paragraph

A Deephaven server is a database of legend-lite. Its tables are declared in an ordinary `###Relational` store, with
the types, `PRIMARY KEY` and `NOT NULL` that store already has, and the same store can be bound to DuckDB in one
runtime and to Deephaven in another. The connection is `RelationalDatabaseConnection { type: Deephaven; ... }`, with a
connection specification and an authentication of its own, as Snowflake has its own. A query on a Deephaven runtime is
planned by **a render target over the same `SqlQuery` IR**, `sql/dialect/Deephaven.java`, implementing `SqlDialect`
whole as `MongoDb` does, which writes Deephaven's table operations (its `BatchTableRequest`) instead of SQL text:
Deephaven's own SQL front end handled 8 of DataCube's 14 query shapes, its table operations all 14. **DataCube reaches
Deephaven from the browser, with no server in the middle** (the user, 2026-10-23; DH21): the planner DataCube already
runs in the tab (WebAssembly) plans the query, lite's own encoder turns the plan into Deephaven's messages, the tab
sends them and reads the rows back as Arrow. DataCube's own app runs on it unchanged (DH23): its engine is a Deephaven
session that keeps alive the tables each query made, so a regroup, a repivot or a filter sends only what differs, and
every result on screen is followed live in the tab (DH24), so a tick re-runs the cube from rows already in the page:
nothing sent, nothing fetched. A server-side sender (typed
rows for the server's API, Python and the tests; writes) speaks the same protocol from the JDK's own HTTP client,
later. Live or snapshot is how a caller executes a plan, not what was planned. Reference data gets into Deephaven the
platform's own way: read from its source of record on one runtime, **written through the same modelled table** on the
Deephaven runtime.

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
8. **The license shapes deployment** (Deephaven Community License): no Deephaven code enters lite's build (DH11:
   every Deephaven jar on Maven Central, its protocol stubs included, is under that license); the JS library is
   fetched from the user's own Deephaven server; and since creating a table creates an input table, only internal
   users may run DDL against Deephaven (§3a, §4 of the license; DH11, DH15).

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

The table is proposed; the lanes measure it (L3b, L3s). Nullability is the store's: a column is nullable unless `NOT
NULL`, as a Deephaven column is.

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
| DH11 | the client | D12: `core/src/main/mongodb`, bundled | **lite's own, no Deephaven code in the build** (revised 2026-10-23; the first answer, a `@maven_deephaven` pool of Deephaven's stubs with gRPC, was pinned and reverted the same day): every Deephaven jar on Maven Central is under the Deephaven Community License, so lite speaks Deephaven's browser protocol (gRPC-web: each message a 5-byte frame, the status in a trailer frame, over HTTP/2) with an encoder of its own, from the plan's JSON to Deephaven's binary messages, each field's number and wire type checked against Deephaven's published message definitions (one is a zigzag `sint32`: the measured check, §9). One encoder, in Java beside the target, so the WebAssembly planner carries it to the tab and the server-side sender reuses it |
| DH12 | the connection's address | D4: a specification variant | **`ConnectionSpecification.Deephaven(host, port, useTls)`** (§4). Supersedes the shared `ServerUrl` agreed in §8's first version: Deephaven no longer needs it |
| DH13 | authentication | a vault reference | **`PreSharedKey(keyVaultReference)`** (§4) |
| DH14 | the metamodel store | D13: refused by name | **refused by name** |
| DH15 | creating tables | (none) | **the store's DDL**: a table with a primary key becomes a keyed input table (its key the declared key), one without an append-only input table. Internal users only (DH11) |
| DH16 | a table legend-lite did not create | (none) | **checked when a session first reads it**: the server's columns, types and nullability against the declaration, a mismatch refused by name; a write to it refused by name |
| DH17 | writes | (none planned: test-data loads only) | **the Cypher writes line's four verbs (§6)**, each a table-operation plan then one input-table add or delete; `write` into a keyed table and every verb but `write` on an append-only one refused by name; one write per program |
| DH18 | names on the server | (none) | **the default schema's names**; a named schema refused until a case (§3) |
| DH19 | a restart | (none) | **refused by name**: a table missing from the server is reported as missing, never recreated empty by the connector; the publisher recreates and reloads (§7) |
| DH21 | DataCube's route | (none) | **the browser, straight to Deephaven** (the user, 2026-10-23: "we definitely have to find path for browser direct to DeepHaven without server in the middle"; measured, §9): the tab plans (the WebAssembly planner), asks the same module for the plan's bytes, posts them to Deephaven over HTTPS (Deephaven refuses its browser protocol without HTTP/2, which browsers speak only over TLS), publishes the result under a shared name, opens it with `dh-core.js` (`getSharedObject`) and subscribes to the visible rows. The tab's session owns the shared table: it refreshes its token while the grid is open and releases the table on close. Each tab builds its own table; identical ones are one table on the server (measured in the spike). **Who signs in:** the user, from the browser, to Deephaven; the connection's pre-shared key is a server secret and is never sent to a browser (a test key in the demo; Deephaven's per-user login, its code console off, for real users). **Where the page is served from:** an internal site, as Deephaven is: Chrome keeps a page on a site it counts as public from a loopback or private address without the person's permission (its Local Network Access rule, measured on the pinned Chromium, 2026-10-23), and does not keep one internal site's page from another's |
| DH22 | the plan's form | D10: database-written text | **text: the plan stays Deephaven's own JSON form** of its batch message (shown in DataCube's query pane, compared in tests, quotable); the bytes are the sending step's, which fills what exists only at run time (this session's result ticket, an uploaded parameters table) and encodes |
| DH23 | a grid's queries on one session | (none) | **one engine per grid, a session that keeps alive what it made** (the user, 2026-10-23: "is there a base query that then you can do different aggs/filters on?"; measured, §9 J1): `DeephavenEngine`, a `QueryEngine` like the tab's DuckDB, so DataCube's own app runs on it unchanged (`new CubeApp(root, snapshot, {engine, planner})`, the planner on a Deephaven runtime). **Reuse:** the wire names each operation's table by a key (SHA-256 of the operation as the plan writes it, its inputs named by their keys) and sends only the operations the session does not keep alive, every one it sends exported to be read again; an operation kept alive is read by its ticket. The target writes a source alike wherever it stands, so every query over one source shares the source's operations: Deephaven's own memoizing leaves out `view`, `naturalJoin` and `updateView`, the operations a source starts with. Idle tables are kept, the least recently read let go past a bound (128: an idle table still ticks on the server). **Rows:** Arrow Flight's `DoGet` on the session's own export, read by the tab's own Arrow decoders (Deephaven's `BigDecimal` and `BigInteger` forms by their `deephaven:type`), named by the plan by position. **Ticks:** DH24, which replaced the session's `ExportedTableUpdates` stream this row first chose (it said a result changed, and the result was then read again whole: measured 15 ms slower and a read more) |
| DH24 | how a tick reaches the grid | (none) | **every result on screen followed live in the tab** (the user, 2026-10-23: "focus on making real ticking work and making it super super super super fast"; measured, §9 T1, `datacube/bench/deephaven/README.md`): once a view is painted, `DeephavenEngine.follow` publishes each result it read and subscribes to it with Deephaven's own JS library (`dh-core.js`, loaded from the server at run time, never bundled), asking for updates every 10 ms -- a cycle's change reaches the page 7 to 23 ms after the cycle that made it, where asking at the cycle waits up to a cycle more -- and lets go of every result no view reads. At a change the cube runs again (`CubeApp.tick`, the state owner's `rerun`: no transaction, no history, nothing written) and every query is answered from the live rows: nothing sent, nothing fetched. A view's FIRST rows are read by `DoGet` (35 to 40 ms sooner than a subscription's first update, measured); a result is never subscribed whole past a grid's bound (a full subscription of 100,000 rows in a storm fell 15 s behind and took the server down) |
| DH25 | a filter's form | (DH5: no value in formula text) | **what the server reads without compiling**: a filter is the list of filters Deephaven ANDs, its conjunction split, each comparison of a column with a literal written as the server's filter parser reads it -- a match (`x == v`, `x != v`, `x in v1, v2`) or a range filter (`x < v` ...), no compile -- its null guard an entry of its own that names only its column; anything else a formula. Measured on 42.7: a formula with a new literal cost a filter 240 ms of compiling (each value a person filters on, each group a tree opens), written bare none. Pure's `!=` (null-safe) is the inverted match, which keeps the nulls Pure keeps |
| DH26 | a query's shape, for the server's cost per request | (none) | **as little for the server to parse and make as the answer allows** (G1, measured 2026-10-23: the eight operations of a level query cost the server about 15 ms in its console and 100-125 through the protocol -- a cost per request, per formula and per aggregation): DataCube's unique column (the lowering's `COUNT(DISTINCT v) = 1 ? MAX(v) : null`) is the server's one `unique` aggregation; an average reads its column as it is (Deephaven's average is a fraction of any number, so the lowering's `1.0 *` promotion is the server's own); a key or an argument that is a column as it is is read by its name, with no operation before the group; a grid's base declares `NOT NULL` what the lowering never leaves null (the planner's `nullable`: the relation type keeps a LEFT-joined column's own multiplicity, the lowered outputs do not), so no null flag or guard is written for it; and a result's rows are asked for as soon as its export is named, the server answering once it has made it |
| DH27 | what may be slow, and what may not | (none) | **a change may take time; a tick may not** (the user, 2026-10-23: "changes can take 'longer' but after that ticks should be super fast"; scope: one person, the browser straight to Deephaven, DH21): a gesture -- a filter, a grouping, a pivot, a calculated column, a scroll past the rows the page follows -- may make tables, compile formulas and fetch, once. Once a view is painted, a change on the server reaches the screen with no request: every grid, pivot and chart on the page follows what it shows, and a tick's work in the page is bounded by what is on screen, never by the size of the result (a flat grid follows its visible rows, a viewport: V1). A tick that would need a request -- a pivot value appearing, a group's new child -- is a change, made as one, and never holds the page's other ticks |
| DH20 | joins | (Mongo's own) | **`natural_join` where the right side's key is unique** (its declared key covers the join columns, or it was grouped by them); the general `join` for an inner join otherwise; `asOfJoin` as `aj`; a to-many outer join (Deephaven's outer joins are experimental) and a non-equi join refused by name until measured |
| DH28-DH32 | the trader screens (2026-10-24) | (none) | in `docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md` §1: the data (a simulator always, a crypto feed where reachable), time on Deephaven in UTC, `now()` refused until a live window is designed, this line owning every missing Deephaven feature, bespoke work written down |

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

The write step's landing is the Cypher writes branch reaching `main` (W, §9). Nothing before L3w waits on it:
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
   refusing by name when none is on the class path), loaded from the opt-in server target (2026-10-23: with no
   Deephaven code in the build, DH11, the license no longer asks for the opt-in target; whether the server-side
   sender is bundled is L3s's to decide). From the Mongo merge:
   `TestBackendsTest` wants a `DeephavenBackend` once the type has a dialect, and the tests iterating
   `Databases.implemented()` need the non-SQL filter Mongo's merge lands, reused.
6. **The `QueryTarget` split** taken up together once the Mongo branch is on main.
7. **Guard and registry edits additive**, each dated; whoever lands second re-adds. After the Mongo merge,
   `DatabaseConnector` gains `ownSessions()` and `open(ConnectionDefinition)`, and the connection codec is
   `model/ConnectionWire`: L3s uses those.

## 9. The landings, in the order agreed (the user, 2026-10-22: "do everything, then a fake UI until the DataCube session has cleaned up")

A ticking grid comes early, before writes and cross-store land, so the live experience is judged first; the modelled
publisher replaces the demo's seeding once they do. Stacked branches, no PR until asked.

| # | branch | content | judge |
|---|---|---|---|
| L1 | `claude/deephaven-1-model` (on main) | `DatabaseType.Deephaven` (a lite type) and its switch arms, each refusing by name until L2/L3; `ConnectionSpecification.Deephaven`, `AuthenticationSpec.PreSharedKey`: parser (both dialects), protocol records, `ConnectionWire`'s tags, `ConnectorsTest`'s rows; no legend-engine JSON and no composer rule (each refused by name, as SQLite's specification: S89's "refused by name where it has none"); no decision at the model door (D9: the connector refuses a mismatch, the target a store it cannot hold, L2); S48 extended; `LITE-DESIGN-deephaven-backend` declared | `//gates:local`, `//gates:parser_equivalence`; the Studio line reviews the grammar |
| L2 | `claude/deephaven-2-target` (on L1 merged with the MongoDB branch, whose dialect hooks a non-SQL target uses) | `sql/dialect/Deephaven.java` (`SqlDialect` whole), designed against the IR DataCube's 14 query shapes actually lower to, then class queries through mappings; **a parameter as a one-row table joined in** (a formula reads its column; no value is ever written into formula text, DH5); DDL as input tables (DH15); a column type (§3), a schema or a name the server cannot hold refused by name; every other IR node, function and aggregate refused by name; unit tests on the plan text, as `MongoDbDialectTest` | the dialect's tests; every existing lane's dump unchanged |
| B0 | scratch (measured 2026-10-23, below) | **the browser check**: a page served by another site posts a plan to Deephaven over HTTPS, publishes it, opens it with `dh-core.js` and watches it tick | done |
| L3b | `claude/deephaven-3-browser` (on L2), **built 2026-10-23** | **the browser route (DH21)**: the encoder (DH11, DH22) beside the target, its field table checked against Deephaven's message definitions; the target and the encoder in the WebAssembly planner; the tab's sender (post the bytes, publish, refresh the session, release on close) and the subscription through `dh-core.js`; the pinned test server (HTTPS, a test certificate) | the encoder's tests against Deephaven's own definitions; the browser cases in headless Chromium against the pinned server: L2's 18 shapes give DuckDB's rows, and a live one ticks |
| D0 | `claude/deephaven-3-browser` (on L3b), **built 2026-10-23** | **the first demo**: the store, classes and mappings of §7; reference data and the simulator as a **Deephaven startup script** (marked temporary: lite's modelled publisher replaces it, L5); a **restart watcher** that re-seeds after a Deephaven restart and tells subscribers; **a plain live-grid page** in the browser (the planner, the sender, `dh-core.js`; no server in the middle) standing in for DataCube -- deleted with its test once DataCube ticked (T1), whose cube test checks all it did | a ticking grid whose values agree with DuckDB's over the same inputs; a restart survived |
| J1 | `claude/deephaven-3-browser` (on D0), **built 2026-10-23** | **DataCube's own app on Deephaven (DH23)**: the wire's keys and sends of what the session does not keep alive, `DoGet`'s ticket and the update stream's messages (Java, in the WebAssembly planner); the session's tables kept alive and its update stream (the stream gone with T1), `DeephavenEngine` (`engine-client/src/deephaven-engine.ts`) with its Arrow reader (`deephaven-rows.ts`), `LitePlanner.deephaven` so the cube and its engine share one module; the cube page (`datacube/demo/deephaven/cube.html`), re-running the cube on a tick until L4 | `//core:core_tests_deephaven`, `//engine-client:deephaven_rows_test` (an answer the pinned server sent, every kind of column), `//datacube:deephaven_cube_test` in the pinned Chromium |
| T1 | `claude/deephaven-3-browser` (on J1), **built 2026-10-23** | **real ticking (DH24)**: the tick benchmark (`datacube/bench/deephaven/`), the engine's `follow` (Deephaven's JS library's subscriptions, `engine-client/src/deephaven-live.ts` `LiveClient`) and its live rows (`deephaven-rows.ts` `followedTable`), `CubeApp.tick` and the state owner's `rerun`; the update stream, its Java message and the plain D0 page deleted | `//datacube:deephaven_cube_test` (a tick re-runs the cube from live rows, nothing sent or fetched, its time on screen said), `//datacube:cube_state_test`, `//engine-client:deephaven_rows_test`; `//datacube:deephaven_tick_bench` and `deephaven_first_bench` (manual) |
| F1 | `claude/deephaven-3-browser` (on T1), **built 2026-10-23** | **filters the server reads without compiling (DH25)**: `Deephaven.filters`; Pure's null-safe `!=` and `==` shaped | `//core:core_tests_sql` (`DeephavenDialectTest`), `//datacube:deephaven_test`'s ten filter answers over rows with nulls, worked out by hand and checked first on the formulas |
| L3s | `claude/deephaven-3-server` (on L3b, merged with the Mongo branch, whose connector seam it uses) | **the server-side sender**: the same encoder over the JDK's HTTP client (no jar), found through `ServiceLoader`; `rows` over Arrow Flight with lite's own Arrow reader, `subscribe` with its handle, creating tables, the schema check (DH16), the missing-table refusal (DH19); `DeephavenBackend`; the guards' additive lines, each dated | lite's own Deephaven cases, answered on DuckDB and on Deephaven and compared row for row; then the PCT lanes, as every database's |
| W | `claude/cypher-writes` reaching `main` (the Cypher writes line's landing, its owner's call) | §6: the four verbs, staging, class writes, write plans | its own gates |
| Wr | on main after W (built here, reviewed by the Cypher writes line) | the relation slot: a parameter whose value is typed rows, staged by the runner (§6's gap) | its own tests on DuckDB, H2, Postgres |
| X | on Wr | **cross-store with explicit boundaries**: each `from(runtime)` a split, the inner part planned on its runtime and its rows carried into the outer part's slot; refused by name inside a live plan and for writes to two databases | a read on DuckDB merged into Deephaven as one program |
| L3w | on L3s, W and Wr | Deephaven's write arms (DH17, §6's table) | the write cases, DuckDB against Deephaven |
| L5 | on D0, X and L3w | the modelled publisher (§7) replaces D0's seeding | D0's checks, with no seeding |
| L4 | retired 2026-10-23 | was: a ticked result's rows matched into the grid by group key. T1 measured the whole cube re-run from live rows at 1.5 to 2.2 ms, every level and the pivot included, so the matching buys nothing; the order below replaces it | -- |
| B1 | `claude/deephaven-3-browser` (on F1), **built 2026-10-23** | **ONE BASE PER GRID** (agreed with the user 2026-10-23): the grid's base query and its leaf-level calculated columns made once on the server as one ticking table, its calculations stored (`update`) and kept current; every query the cube sends reads that table and adds only its filter, grouping, pivot, sort and limit -- DataCube's `rebased(runner, live, copy)`, the seam Snap uses, the copy the live table on the server. Today the target's keys share the source's joins only: the lowering folds a calculated column into each query that reads it, so every followed result recomputes it every tick. A new calculated column makes a new base once (its formula compiled once; the joins under it still kept alive) | the cube test (its views' answers unchanged, nothing sent on a tick); the server's work per tick with five results followed, measured before and after in the tick benchmark |
| G1 | on B1, **built 2026-10-23** | **a gesture's time, broken down, and budgets the tests hold**: planning, making, reading, painting, per gesture; the cube test fails past a budget (a tick on screen 10 ms after its rows; a repeated filter or regroup, its formulas compiled, 100 ms to the view; a group opened 50 ms once R1 lands). Something new -- a calculated column, a pivot value, a formula's first appearance on a server -- costs once (a compile, about 240 ms a formula) and is not held to them | the cube test |
| R1 | on G1 (agreed with the user 2026-10-23: try it, views not shared; **spiked and measured 2026-10-23**, below) | **DataCube's tree as Deephaven's rollup** (`HierarchicalTableService.Rollup`, in 42.7's protocol): the deepest level's grouping planned from Pure as today and written as a rollup over its key prefix -- one server object holding every level, kept current incrementally, each level re-aggregated from the one below; a group opened or closed is a viewport change (`View`), no query, no compile; one subscription for the grid, only the rows on screen sent. A spike first: which of DataCube's aggregates a rollup re-aggregates (an aggregate it cannot stays on per-level queries, chosen per aggregate and said); group-level calculated columns per level (node operations, through the protocol or the JS library); the grid's windowed rows over the rollup's viewport | each level's answers equal to DuckDB's per-level queries over the same seeded rows, nulls included; the cube test's gestures; the tick benchmark with every level of a tree open |
| P1 | on R1, **built 2026-10-23** | **pivots**: each pivot value's conditional sum a formula of its own text (a new value compiles once, the server keeping the rest); the pivot's values followed live, so a value appearing on the server becomes a column; on the rollup, each value's conditional column a calculation of the base, summed at every level | DuckDB's pivot answers; a value appearing live |
| C1 | V3, on V2, **built 2026-10-23** | **charts tick**: a chart's queries followed as a grid's are; its scale's rule on a tick decided with the DataCube line | the chart's test in the pinned Chromium |
| S1 | after L5 and L3s | **views shared between viewers**: a base and a rollup per saved grid, owned on the server (the modelled publisher's side, not a browser), each viewer its own expansion; who may read one, and when it is let go, decided then | two viewers of one grid: one base on the server |
| V1 | on G1 (the user, 2026-10-23: "flat grid with lot of rows"; DH27), **built 2026-10-23** | **a flat grid of any size**: a flat view's result made once on the server, sorted and filtered there, and followed by a VIEWPORT -- the rows on screen and a margin -- so a tick carries those rows only and the page's work does not grow with the result; a scroll past the margin moves the viewport (no table made, no formula compiled); the grid paints a block of the whole (`DataGrid.setRows(block, offset, total)`); a person's row limit still applies | the cube test over 100,000 positions: a tick's time on screen, a scroll's, the server's update graph busy |
| V2 | on V1 (the same ask: "few grids in same datacube with different pivots"), **built 2026-10-23** | **several grids on one page, one session**: each grid its own follower over the engine's one session, live reader and bases (a grid following its view lets go of nothing another grid reads); grids over the same source read one base; each grid's pivot ticks; a tick re-runs only the grids whose results changed; P1's live pivot values here: a value appearing on the server is a change (DH27) | the page test: three grids, two pivots, every grid's tick on screen |
| O1 | on P1 (the user, 2026-10-23: the roadmap's #1), **measured 2026-10-23** | **a gesture's time, where it goes**: each step on the browser's timeline, the server's operation log beside it; what is ours cut | the cube test's per-gesture lines |
| N1 | on O1 (the same ask), **built 2026-10-23** | **over a real network**: the cube page behind a link of its own round trip (a delaying proxy), each gesture's time to the view and to the view live, its requests, a tick's; the round trips a gesture waits on cut to what its reads need | `//datacube:deephaven_network_bench` (manual) |
| B2 | on V2 (the same ask), **built 2026-10-23** | **the page of grids, whole**: a server restart recovered by every grid and chart; the page saved and opened again over Deephaven, each grid's source written down | the page test: a restart, a save and an open, every grid and chart ticking after each |
| A1 | on B2 and C2 (the user, 2026-10-23: "a full audit/review of the whole single user experience"), **done 2026-10-24** | **the single-user audit**: a walk-through as a person drives both pages, a long session's cost on the server and in the page, a code review of the path; findings fixed or recorded (docs/DEEPHAVEN_SINGLE_USER_AUDIT_2026_10_24.md) | `//datacube:deephaven_walkthrough`, `deephaven_session_bench` (manual) |
| C2 | on V3 (the same ask), **built 2026-10-23** | **a chart's scale held while it ticks**: its value axes as the view drew them, widened only where a tick's values leave them, drawn afresh at the next view | the chart's tests (the rule, drawn by ECharts); the page test: the axes during six seconds of ticks |
| K1-K13 | `claude/deephaven-3-browser` (the user, 2026-10-24: trader screens) | the trader screens and the Deephaven features they need: `docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md` §2 | each landing's own |

**L2, measured (2026-10-23):** the target's batches for 18 query shapes (DataCube's pivot as conditional sums, its
grand total over a literal key and over no rows, distinct, top-N, nested groupBy-extend, avg, count distinct, std,
median, left and inner joins with null keys, three-valued NOT and OR, a boolean column, IN, IS NULL, a HAVING-style
filter), run on a Deephaven 42.7 server (its release tarball, Java 21) over 200 rows with about 12% nulls in every
column, gave DuckDB's rows on all 18 (the harness: a scratch check through the protocol's own stubs, which L3b's
browser cases and L3s's differential test replace). Two shapes were the compiler's refusals, not the target's.

**The browser route, measured (2026-10-23, B0):** a page served from `http://127.0.0.1:8081`, Deephaven 42.7 at
`https://localhost:8443` (a self-signed certificate; headless Chromium told to accept it). The page signed in with the
key and read back the session token across sites (Deephaven's CORS handler allows any origin and exposes
`Authorization` and `grpc-status`); sent one batch it encoded itself (a 0.2 s time table, derived columns, a sum by
key, a sort), all four steps created; published the result under a shared name; and `dh-core.js`, loaded from the
Deephaven server, opened it and ticked: every 200 ms with the server's update cycle at 100 ms (24 grid updates in 5
s), once a second at Deephaven's default 1 s cycle (so the demo sets the cycle). Over plain HTTP/1.1 Deephaven refuses
its browser protocol ("not using http2"), so HTTPS is required. The first attempt failed on the encoding (a table
reference's offset is a zigzag `sint32`), which is why the encoder's field table is checked against Deephaven's own
definitions (DH11).

**L3b, built (2026-10-23):** the platform plans a Deephaven runtime and a host runs it (`Databases.hostExecuted`; no
platform session or test backend opens on Deephaven); the wire is `//core:deephaven_wire` (four plans encode to the
bytes protobuf's own library writes; every row of its table checked against the `.proto` files of Deephaven 42.7's
protocol jar, `//core:deephaven_messages_test`); the planner protocol's `deephaven` kind reaches it from the tab
(TabExports), Python (the native library) and the TypeScript protocol; the tab's sender and live reader are
`engine-client/src/deephaven.ts` and `deephaven-live.ts`. `//datacube:deephaven_test` (manual, in `//gates:heavy`)
runs it in the pinned Chromium against the pinned server: seven answers worked out by hand from six rows with nulls,
L2's eighteen shapes created and answering, a ticking grouping ticking, a refusal in the server's words. A scratch
run of the same route gave DuckDB's rows on all eighteen shapes over 200 rows. Found on the way: TeaVM 0.15.0 writes
a data segment's address as an unsigned LEB128 that WebAssembly reads signed, which broke the planner's module once
its static data passed 1 MiB (`tools/teavm/SegmentOffsets` rewrites it, `//tools/teavm:segment_offsets_test`); a
shared name lives as long as the export it was published from, and the server refuses to release the name itself.

**D0, built (2026-10-23; its plain page deleted the same day, T1):** `bazel run //datacube:deephaven_demo` served the live risk page (`datacube/demo/deephaven/`) and
the pinned Deephaven running its start-up script (`risk.groovy`, marked temporary: 40 instruments, 8 books, 400
positions, three currencies, prices re-quoted ten times a second). The page plans the risk view (four keyed left joins,
a market value, a grouping by desk and sector) with the WebAssembly planner, sends it, streams it, and after a
Deephaven restart dims the last rows, says Deephaven is not answering, and comes back by itself (the start-up script
seeds again). Its judge was `//datacube:deephaven_demo_test` (manual, in `//gates:heavy`): sixteen desk and sector rows,
each with the seeding's own number of positions, worked out by the test from the same formulas; the values tick; the
page sees a restart and comes back with the same positions; screenshots of each stage are its outputs. Not yet in it:
a comparison of the market values with DuckDB's over a frozen copy of the prices, and class queries through mappings
(the store's tables are queried directly).

**J1, built (2026-10-23): DataCube's own app on Deephaven.** `bazel run //datacube:deephaven_demo` now serves
`cube.html` (at `/` since T1): DataCube's `CubeApp`, unchanged, over the risk source (the four joins and the market
value), its engine `DeephavenEngine` and its planner the WebAssembly `LitePlanner` on the Deephaven runtime. Its judge,
`//datacube:deephaven_cube_test` (manual, in `//gates:heavy`), drives the app's own changes in the pinned Chromium:
the tree by desk has the seeding's positions; a pivot on region (two queries: its values, then the level) planned 31
operations and sent 13; a regroup by region and currency planned 16 and sent 7; a filter to the FX desk planned 15 and
sent 6 -- every query reading the source's nine operations (five views, four joins) from what the session held; the
pivot's columns keep their names (`AMER__|__pid`, not the target's `__c<i>`); over the whole run 25 queries planned
378 operations and 42 were sent, and no table was made twice; with the prices re-quoted ten times a second the cube
re-ran 20 times in 5 s with nothing sent (each re-run: the plans from the planner's cache, the rows read again); and
after a Deephaven restart the page said so, signed in again and came back with the same view, ticking. Measured on
the way: `DoGet` over gRPC-web answers a held result in 10 to 20 ms; the planner's part of a query is 1 to 20 ms
(planning, keys, the send's bytes); what a new query costs is Deephaven compiling its new formulas (a pivot's level,
eight conditional sums: about 1.5 s the first time, its compiler keeping the classes by formula text); the update
stream sends one message per changed export each cycle, a static export's once. The rows reader is checked on an
answer the pinned server sent (`engine-client/test/deephaven/doget-answer.hex`): a 64-bit integer past 2^53, an
`Instant` to the microsecond, a `LocalDate`, a `LocalTime`, a negative `BigDecimal`, a `BigInteger`, nulls.

**T1, built (2026-10-23): real ticking.** Measured first, with the tick benchmark
(`datacube/bench/deephaven/README.md`: every quote stamped with the server's clock in the cycle that computes it, so a
latency runs from the cycle that made a change to the page seeing it): Deephaven's JS library, asked every 10 ms, hands
the page a cycle's change 7 to 10 ms after the cycle (a 400-position book) and 13 to 23 ms in a storm (100,000
positions, every one re-valued every cycle); the update stream and a `DoGet` of the result was about 15 ms slower and
a 10 ms read more. So the engine follows every result a painted view read (DH24) and the cube re-runs from those rows:
in `//datacube:deephaven_cube_test` 50 runs in 5 s, nothing sent or fetched, a tick on screen 3.7 to 4.8 ms (p95 7 to
8) after its rows arrive, the cube's run 1.5 to 2.2 ms of it. Profiled on the way: a re-run as a transaction cost 12 ms
(the grid's forced layout, twice, and the side panels rebuilt); `rerun` and `CubeApp.#onTicked` repaint only the
rows when the columns are the ones on screen. Charts do not tick yet. The plain D0 page and its test went (the cube's
test checks what they did); the cube is served at `/`. First rows, measured (`//datacube:deephaven_first_bench`, a
new result over the risk source): `DoGet` 45 to 84 ms at p50, a publish and a subscription's first update 80 to 125 --
`DoGet` keeps the first rows.

**F1, built (2026-10-23): filters the server reads without compiling.** The first-rows benchmark found the cost of a
new view was not the read but making it: 250 ms for four to thirteen operations. Deephaven compiles each distinct
formula text to a Java class, and the target wrote every comparison as a formula (`(t0__pid < 1004L)`): every new
literal was a compile. Written as its filter parser reads a match or a range (`t0__pid < 1004`), the same filter made
in 44 ms (DH25). After it: a new view's table made in 30 to 56 ms and its first rows in 45 to 84 ms (from 270 to 300);
the cube's filter gesture 155 to 199 ms to the view and a regroup 188 to 254, end to end in the pinned Chromium. What
compiles still: a formula's first appearance on a server (opening the tree's first group, 689 ms on a fresh server
against 283 once its formulas were compiled), and a pivot's conditional sums, whose formulas carry the pivot's values
(1,784 ms fresh, 473 warm) -- the next measured target: a pivot value as a parameter's column (DH5's one-row table) or
no formula at all.

**B1, built (2026-10-23): one base per grid.** `datacube/src/server-base.ts` `ServerBaseRunner`, the grid's runner
(CubeApp's `runner` form): for each snapshot it plans the base -- `sourceWithDerived`, the base query and its
leaf-level calculated columns -- and the engine keeps it on the server STORED (`DeephavenWire.stored`: the plan's last
`view` written as a `select` of the same columns, so a calculated column is computed once a change and kept) under the
name `legend_base_<its key>`; a model fragment declares that table in `legend::Bases`, bound with the grid's own stores
to the grid's own connection (`legend::BasesRuntime`), and every query is rebased onto it (`rebased`, Snap's seam); the
engine fills the table's ticket with the base's export on every send (`DeephavenWire.send`'s `supplied`, the sending
step's ticket, DH22). A base whose columns no table can declare -- a name the server does not hold as written, a type
the fragment does not write (a decimal, a boolean, a time of day) -- is not made, and its grid's queries read it as
written (`ServerBaseRunner.counts.asWritten`, which the page shows). Since G1 it declares `NOT NULL` each column the
planner's lowering never leaves null (`LitePlanner.nullable`). The fragment is written by the runner, not by the
platform's model writer (`Boundary.tableModel` writes a host connection of its own, not a Deephaven one): folding it in
is a follow-up. In `//datacube:deephaven_cube_test`: one base made for the grid, every gesture's queries read from it
(the FX desk opened: 2 queries, 17 operations planned where 33 were; the pivot 15 where 31; a regroup 8 where 16); a
calculated column added (`$x.mv / 1000.0`) made one new base, 478 ms to the view, each row's value right; ticking and
a restart unchanged. Found on the way, DataCube's own: the state keys (`cube-state.ts` `queryKey`, `stateKey`) wrote the
state with `JSON.stringify`, which a decimal literal (an `ExactNumber`) refuses, so on main a calculated column with
one could not be applied; they write it with the protocol's `toJson` now. The server's work per tick with and without
the base is measured with R1's tree benchmark (a 400-position book shows none).

**G1, built (2026-10-23): a gesture's time, broken down and cut.** Broken down in the pinned Chromium (temporary
probes, gone): a warm regroup's 163-192 ms was the server's `Batch` 100-127, its rows' `DoGet` 12-22, the planner's
calls about 12, the page's own work 35-50. The batch's operations cost the server about 15 ms in its console (each
timed); the rest was the protocol's per request -- 20-38 ms before its first operation, the server parsing every
formula of the batch, and 35-50 around an aggregation of twelve, four of them count-distinct-and-max pairs. Not the
update cycle: a 1,000 ms cycle changed nothing, a 20 ms one made it slower. So the target writes less (DH26): the unique
columns as the server's `unique` aggregation (four count-distincts, four maxes and four formulas gone from the demo's
every level), averages without the promotion, plain columns read by name (the operation before every group gone), a
base's `NOT NULL` columns declared from the lowering (the null flag's formula and two operations gone from a sorted
level), and the rows asked for while the batch is made. After it, over three runs: a regroup 102-145 ms to the view, a
filter 107-121, a group closed 33-64, a tick on screen 4.3-4.4 ms; something new as before, once (the first group opened
on a fresh server 474-980 ms, a pivot 1.7-1.9 s, a calculated column 352-385 ms). `//datacube:deephaven_cube_test` holds
the budgets: a regroup and a filter 250 ms, a group closed 150, a tick 10 ms on screen. Measured on the way and left: the
grid's own `get window`, a forced layout of 4-12 ms a gesture (DataCube's DataGrid, shared by every backend); and a
pivot's conditional sums, a compile for each new value set (P1).

**R1, the spike (2026-10-23): measured, not built.** `//datacube:deephaven_tree_bench` (its README): a tree by region,
desk, sector and currency over the tick world in a storm, its path EMEA > Rates > Tech open, as one aggregation a level
and as Deephaven's rollup made from the page. Over 100,000 positions the rollup cost the server as much as the levels or
a little more (busy above the floor, four runs: levels +2.0 to +7.4 points, rollup +4.6 to +6.7) -- it keeps every node
of every level current, opened or not -- and its median tick was 5 to 8 ms later (p95 no worse). Its gain is opening a
group: 62 to 117 ms in its viewport, no query and no compile, against 283 ms warm for DataCube's level query (planning,
making, reading, painting). Its costs: no median or percentile (the server refuses them in a rollup), no group-level
calculated column (not through the protocol, not through the JS library), a filter on a column it does not group by a
new rollup (180 to 245 ms), and DataCube's tree driven by the server's viewport. A regroup made warm in 62 to 281 ms.
Whether to build it is the user's call, on these numbers; G1 first would say how near a level's query can come.

**V1, built (2026-10-23): a flat grid of any size.** A flat view is read by a WINDOW (`QueryEngine.executeWindow`,
`QueryRunner.window`, `CubeControllerOptions.window`): the result made once on the server, sorted and filtered there,
and followed by a viewport (`LiveClient.followWindow`, `createViewportSubscription`, moved by `update`) -- the rows on
screen and a margin of twice a screen; the view carries where they are (`CubeView.window`) and the grid paints a block
of the whole (`DataGrid.setRows(block, offset, total)`); a scroll past the margin moves the viewport, as a tick runs
(`CubeApp.tick`, one run at a time); an export or a copy reads the whole result once. The tick's own path was then cut
by measuring it (`//datacube:deephaven_flat_test`, PROFILE=1): a tick re-drew every row's DOM and re-fitted every
column (66-80% of the page's thread); now `DataGrid.tickRows` writes only the changed cells of the rows on screen, in
place (a row in the buffer is drawn when a scroll brings it on), and the columns are fitted once a view. Over 100,000
positions in a storm, sorted by the market value every price moves: the book made in about 1 s (a gesture: DH27), a
tick 111-185 rows, the cube's run 2.2-2.5 ms, on screen 8.7-9.9 ms after its rows alone (p50; 10-14 beside another
browser test), a scroll to row 50,000 shown in 100-157 ms with nothing made, a scroll within the margin nothing asked.
What is left of a tick is the browser's own layout and paint of the cells that changed.

**V2, built (2026-10-23): a page of several grids.** One engine, one session, one live reader (`DeephavenEngine`); a
reader per grid (`DeephavenEngine.reader()`): it follows what its grid read since its view LANDED -- the view's
queries, its charts', a tick's re-reads, each followed as it is read -- and lets go of the rest when the next view
lands, never another grid's; a whole result two grids read is one subscription. The page's bases are one set
(`ServerBases`): grids over the same base read one table, made once and let go when no grid reads it, and one model
fragment declares them all (a grid's new base no longer takes another's from the planner). A tick is its own event
(`ticked`, `CubeAppBaseOptions.onTicked`), no longer a view landing. `//datacube:deephaven_board_test`: DataCube's own
page over 100,000 positions in a storm -- the book flat, the desks pivoted by region, the sectors by currency, a chart
on each pivoted grid -- live in 7.4 s; the three grids one base on the server; six seconds of ticks, nothing sent or
fetched, 7 results followed; each tick on screen (p50) the book 4.7 ms, the pivoted grids 10.9-12.6, each grid's run
1.2-2 ms of it. A pivot value appearing on the server is still a change (DH27), made by the next gesture: its
following is P1's.

**V3 (C1), built (2026-10-23): charts tick.** A chart reads through its grid's runner, so its grid's reader follows
it; at the grid's tick the page draws each chart that follows the grid again at once (`ChartPanel.tick`: its query
answered from the live rows, no debounce -- an edit's 150 ms debounce had starved a chart under ten ticks a second --
and no animation left running behind the next tick). On the page above: 69-70 draws in six seconds each, on screen
7.3-7.8 ms after the tick's rows, its values moving with the prices. A chart's scale follows its data at each tick
(ECharts' own); held between ticks since C2 (below).

**P1, built (2026-10-23): pivots the server compiles nothing for.** Measured first with the server's own query
operation log (`demo/deephaven/server-log.ts`, read by the cube test after each gesture: its operations' time and its
compilations): a pivot's cells were conditional formulas -- one for each value and measure, the value in the text -- so
a first pivot compiled for 1.7-1.9 s on a fresh server and every new value set compiled again. Now
(`DeephavenBatch.group`, `Deephaven.pivotCell`): a cell's count is the server's count of the rows its match filters take
(`countWhere`), in the group's own aggregate -- 0 where none, as SQL's; its sum, average, minimum or maximum one
aggregation over the keys and the pivot's columns, each cell's rows taken from it by their match filters and joined back
by the keys -- null where no row is the cell's. A condition other than columns equal to literals, or cells over
different pivot columns, is written as before. Found on the way by the same log, and cut: every sort by a column that
may be null compiled its null flag once a column (`isNull(region)`), now read through the key's place (`__k0 = region`,
a rename; `isNull(__k0)`, one text for every key of a type); and every null guard was a formula (`!isNull(x)`), now the
server's match filter on null (`x != null`, no compiling). The cube test: the pivot 21 operations in 112 ms on the
server, no compilation, 216-309 ms to the view; the six seeded rows' pivot -- counts, sums, an average, nulls --
answered as worked out by hand (`//datacube:deephaven_test`). A pivot value appearing on the server is a column at the
next tick (DataCube's own path: a tick re-runs the cube, its pivot's values read from their live rows): a book moved to
APAC on the server, its column in the grid 382-414 ms later, FX's 80 positions in it; moved back, gone at a tick. The
residue: the old pivot's tables stay followed until the person's next gesture.

**O1, measured (2026-10-23): where a gesture's time goes.** Each step on the browser's performance timeline (`dh:`
measures: `timed` in deephaven.ts, the planner's plan in runner.ts, the landed view's paint in app.ts), and the server's
operation log beside them. A warm regroup, 91 ms to the view: planning, naming the plan's tables and encoding its
request 8 ms; the server's `Batch` 43 ms, its operations 16 ms of it; the rows (asked while the batch is made) 5 ms
after; reading them 1 ms; painting the view 4-7 ms. So a request's cost beyond its operations, 25-40 ms, is inside
Deephaven (its export scheduling, about 3-5 ms an operation, and HTTP/2): the lever is fewer operations a query (G1's);
the next candidate is the column renaming that starts every query of one table, left for now. Cut: Deephaven's JS
library (8 MB) parsed on the page's thread in the middle of the person's first gesture -- now loaded when the engine
opens (`DeephavenEngine.open`), parsed while the first view waits on the server: the first group opened in 328 ms, from
465-1,195. A fresh server's first operations of a kind still run slower (its JVM's), not ours.

**N1, built (2026-10-23): over a network.** The benchmark (`datacube/bench/deephaven/network-bench.mjs`,
`//datacube:deephaven_network_bench`, manual) starts the pinned server with the demo's application, puts a TCP proxy
between the browser and the server that holds every chunk half the round trip each way (so TLS, HTTP/2 and each request
meet it as on a link), and runs the cube page's gestures three rounds, each round from the page's opening view and each
gesture after the view before it is live. Each gesture's time to the view, then to the view LIVE (followed, its rows in
the page), first round / warm (ms):

| round trip | page live | expand | collapse | pivot | regroup | filter | a tick on screen |
|---|---|---|---|---|---|---|---|
| 0 ms | 3,043 | 312 / 34, live 152 | 30 / 23 | 241 / 73, live 143 | 99 / 48, live 124 | 86 / 34, live 110 | 3 ms after its rows |
| 20 ms | 3,037 | 370 / 134, live 535 | 21 / 47 | 270 / 176, live 582 | 112 / 95, live 520 | 90 / 98, live 440 | 3 ms |
| 80 ms | 3,661 | 406 / 148, live 704 | 30 / 29 | 391 / 305, live 820 | 165 / 124, live 672 | 163 / 137, live 616 | 3 ms |

A tick sends nothing (its rows are pushed: the link adds its one-way delay, never a request). A warm gesture waits on
ONE round trip -- its rows read from tables the session keeps alive -- and a first one on one too, the rows asked
while the server makes them (G1); a collapse waits on none; a pivot on two (its values, then the level that reads
them). Traced request by request (`BENCH_TRACE=1`) and cut, three round trips the reads did not need: (1) a tree's
levels were read one after another -- the grand total, the root, every open branch -- now each ROUND's levels at once
(`treeview.ts fetchTree`: one round trip a level of the tree, whatever its branches; every engine's, and DuckDB's
connection queues them as before); (2) a result's following published it only after its view landed -- now it is
published as soon as its export is named, while the server makes it (`DeephavenSession.publishKept`, once an export);
(3) `follow` resolved before a following started at a result's read was done, and a following before its first rows
arrived, so a gesture made at once read again what was about to be live -- now it resolves when the rows are in the
page: the warm expand at 80 ms 148 ms, from 258-281. Found by the same run: the timeline's measures broke the headless
runner (no `performance.measure`), now said only where the runtime keeps a timeline (`engine-client/src/timeline.ts`).
The residue, Deephaven's JS library's own: a view goes live four of its sequential calls after it lands (the shared
ticket into its session, its schema, its preview columns, the subscription) and its first snapshot at the server's
next cycle, 0.3-0.6 s over 20-80 ms (0.1 s over none); until then the view on screen is the one read, and its first rows tick it. The
page's start (3.0-3.7 s) is the planner's start and the 8 MB library over the link.

**C2, built (2026-10-23): a chart's scale held while it ticks.** ECharts fits a value axis to its data at every
draw, so under ticks the scale breathed: a sum crossing a round number moved the axis's top, and every bar with it.
Now a drawing says what its values reach on each value axis -- from zero, a stack's sums each sign apart
(chart-option.ts `hold`) -- and a TICK draws with the axes as the chart drew them last (chart-render.ts `heldOption`,
the extent read from ECharts' own model, `drawnExtents`): a side the tick's values stay inside is held, a side they
leave let go for ECharts to widen, so the scale only ever widens while it ticks; a landed VIEW draws afresh. Not held:
a percent stack, a log axis, a rank's, one with breaks. Checked by ECharts itself in node (`chart_echarts_test`: a
tick inside keeps the axis, one past it widens that side only, a stack's sums) and on the page: each chart's axis
through 60-75 draws in six seconds of ticks, never narrowed (`deephaven_board_test`). The chart's first chunk 67
bytes over its budget: raised, dated (`bundle-budget.test.ts`).

**B2, built (2026-10-23): the page of grids, whole.** SAVED AND OPENED AGAIN: each grid writes its source down as the
risk query it is -- a saved query of the demo's project by its text and runtime, its columns, never its rows
(`risk.ts riskSource`) -- and the page's menu has Save and Open (this browser's storage); Open makes each grid of the
risk source again over the source as it is now (`openCube`: a column gone or new said), under its saved id and title,
its charts and layout as saved, and a grid of another source is left out and said. RESTARTED: Deephaven keeps no
session across a restart; every grid's reader hears the live side end, the page signs in again once the server
answers, runs every grid again (its base made again: `ServerBases.current`) and only then says it is live. A grid
that goes -- its tile taken off, or its page replaced by an open -- lets its reader and its base go with it
(`CubeAppOptions.onDispose`, `ServerBaseRunner.close`). The page test over 100,000 positions in a storm: saved (three
grids, two charts, 7 KB), opened again in 1.2-1.3 s with the same grids, columns, charts and one base; restarted,
back in 5.6 s (the server's own start included); after each, three seconds of ticks run every grid and draw every
chart, nothing sent or fetched. Found on the way: the reconnect count "0" the cube test printed was a snapshot read
before the page refreshed its counts -- the engine counts the sign-in (1).

**A1, done (2026-10-24): the single-user audit** (docs/DEEPHAVEN_SINGLE_USER_AUDIT_2026_10_24.md). A walk-through
of both pages as a person drives them, a long session measured on the server and in the page, and the path read
again whole. Fixed: a closed or reloaded tab left its session ticking on the server (its close now outlives the
page); a session the server forgot failed every gesture (it now ends the live side and the page signs in again, 1.3
s); live rows the tab does not read hung their view; a refused tick was silent; the status bar's count and the
selection's figures went stale under ticks; DataCube's windows took no focus, so Escape did nothing; a ticked number
now flashes, only where its row is the same row each tick. Measured: after 59 distinct views the server keeps 130
tables at 0.5 cores (0.3 on the first view), the page's ticks unchanged and its heap flat over 600 more. Recorded,
each with its reason: a pivot's two round trips, the JS library's own chain to live, the idle cache's cost for a
shared server, and nine more.

**Before internal users rely on it:** the firm's counsel reviews the deployment against the Deephaven Community
License (this line's reading, `experiments/deephaven-spike/README.md`, is not legal advice).

## 10. DataCube's ticking mode (agreed with the DataCube + Python line, 2026-10-22; unchanged by the revision)

The user's shape: built here on a branch stacked on the DataCube line's, reviewed by that line.

- **Where it plugs in.** Every grid's rows go through one `QueryRunner`, fixed when the cube is built (`PlanThenRun`
  in the tab, `pythonRunner` on a page Python serves). A Deephaven runtime is another engine chosen at cube build --
  built (J1, DH23): `DeephavenEngine` beside the planner, `PlanThenRun` unchanged -- and its `onTick` names the epochs
  whose results changed. It runs in the tab, with no server in the middle (DH21): the WebAssembly planner plans and
  encodes, the tab sends, reads a view's first rows (`DoGet`) and then follows them live (DH24: Deephaven's JS
  library's subscriptions, the cube re-run from their rows at each change, `CubeApp.tick`).
- **The tick.** A `ticked` event, small and outside the transaction and undo machinery, so it survives the state
  owner's coming rework (the single writer's client side, after the line's S7): it replaces the grid's derived rows
  and nothing else. It never writes the cube or page document, never adds a command, never adds a page write: the
  one-API ledgers (`datacube/test/one-api-ledger.test.ts`, `one-api-gestures.test.ts`, `python/tests/test_one_api.py`)
  do not grow.
- **What ticks (T1, 2026-10-23).** Every grid level, the tree opened or not, and the pivot: the whole cube re-runs
  from the live rows (1.5 to 2.2 ms), so no row is matched by key. Charts tick (V3, 2026-10-23: drawn again at their
  grid's tick from the live rows, `ChartPanel.tick`); a flat grid ticks by its window (V1). The order from here is §9's
  B1, G1, R1, P1, C1, S1.
- **Loading.** The ticking code loads lazily, as the join and chart chunks do (`test/bundle-budget.test.ts`). The
  engine fetches `dh-core.js` from the Deephaven server at run time to follow a view (DH24), never bundled (DH11).
- **The rules.** The database executes and the client only arranges rows; no DataCube rule in Python (a Python side
  calls the TypeScript headless).
- **Stable to build on**: the command API (`grid-commands.ts`, `page-commands.ts`), the cube and page documents, the
  page keeper's write protocol, `DataGrid`, `treeview`. **Moving**: Python's command layer (S6, little overlap).

## 11. Register rows (`docs/SEMANTICS_REGISTER.md`: L1 extends S48, lite's database types; the rows below take the next number free on main when their landing lands, since the MongoDB and Cypher writes branches both claim S97 onward)

1. **Deephaven is lite's own design**, not legend-engine's Deephaven extension: a `###Relational` store, `type:
   Deephaven`, its specification and pre-shared key as lite's, execution by table operations (this document).
2. On Deephaven, a program writes at most once (no transaction), and a partial `merge` or an `update` is a
   compute-then-add that a concurrent writer can race (L3w).
3. On Deephaven, a value equal to a primitive column's null sentinel cannot be stored, and is refused (L3w).
4. A Deephaven result is typed rows; its served text forms are refused until host renderers exist.
5. Formula semantics that differ from SQL's, one row each as the lanes find them (L3b, L3s).
