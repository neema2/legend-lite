# Deephaven as a database of legend-lite: the design (2026-10-22)

**Status: PROPOSED. Nothing here is built.** The architecture follows the MongoDB line's
(`docs/MONGODB_PLAN_2026_10_16.md`), reviewed and agreed by that line on 2026-10-22 (§7); the model and grammar part
reviewed and agreed by the Studio line on 2026-10-22 (§2's two dialects, PARK-38); the DataCube part agreed with the
DataCube + Python line on 2026-10-22 (§9). **legend-engine's Deephaven
extension is not the reference** (the user, 2026-10-22: it is half built and does not work): what the store means,
which types it has, what is checked and how it executes are decided here, on what is right for legend-lite. The model
part is the Studio line's leg 10, which the Studio line reviews.

## 0. The answer in one paragraph

Deephaven becomes a database of legend-lite the way MongoDB is becoming one. The `###Deephaven` store and
`DeephavenConnection`, which lite parses today and drops at the model door, compile into the records every database
uses (`DatabaseDefinition`, `ConnectionDefinition`), keyed by a lite extension `DatabaseType.Deephaven`. A query on a
Deephaven runtime is planned by **a render target over the same `SqlQuery` IR**, `sql/dialect/Deephaven.java`,
implementing `SqlDialect` whole as `MongoDb` does, which writes Deephaven's table-operation plan (its
`BatchTableRequest`, as protobuf JSON) instead of SQL text. A **connector in `core/src/main/deephaven`**, found through
`ServiceLoader` as Mongo's is, runs that plan with Deephaven's Java client: **typed rows** for a snapshot, and, as the
one thing Deephaven adds, **a live handle** (server, ticket, column names, `close`) for a caller that subscribes. The
same plan serves both: live or snapshot is how a caller executes it, not what was planned. DataCube is the first live
caller.

## 1. What exists

- **lite today**: the grammar, protocol records (`PDeephavenDatabase`, `PDeephavenConnection`, `PPskAuth`) and the
  composer round-trip (leg 8). At the door, the store becomes a named carrier with no tables
  (`ModelFromProtocol.java:203`) and the connection a generic element keeping `serverUrl` only
  (`FromProtocol.java:431`, `:522`). So an inline `DeephavenConnection` lands in `foreignConnections`, counts as model
  data, and the query runs on DuckDB; a standalone one is not indexed as a connection. PARK-38 records the carrier.
- **The spikes** (`experiments/deephaven-spike`, `experiments/streaming-risk-spike`): a plan written as
  `BatchTableRequest` JSON with no Deephaven library, run by a 200-line connector, subscribed from a browser through
  `dh-core.js`; and the homework that found Deephaven right on every test (deletes, as-of joins with late data, all 14
  DataCube query shapes as table operations, to 384,000 positions) where Deephaven's SQL front end refused 6 of 14.
  Those findings are this design's evidence: table operations, not Deephaven's SQL; typed rows over Arrow Flight; a
  live table per subscription.
- **legend-engine's Deephaven extension** (pinned 4.145.0) exists and is not followed: its columns are all declared
  non-null, its types stop at eight, its execution generates Java at plan time. lite's grammar keeps reading its
  syntax, which is adequate as a start (§2); nothing else of it is taken.

## 2. The store: what a Deephaven table is to the model

A Deephaven server holds named tables. legend-lite meets two kinds, and the store says which each is:

| kind | what it is | who defines its columns | example |
|---|---|---|---|
| **source table** | a table someone else publishes on the server (a market-data feed, an application's output) | its publisher; the store **declares** them, and the connector checks the declaration against the server's table before it runs anything (a mismatch is refused by name, never coerced) | `prices`, `fx`, `fills` from the simulator |
| **input table** | a table legend-lite creates and owns, whose rows legend-lite's users write (keyed: an upsert and a delete by key; or append-only) | the model | reference data published from legend-lite; positions a user edits |

What every table declares:

- **Columns, typed by what Deephaven holds**, each mapped to the Pure type that holds its every value:

  | store type | Deephaven column | Pure type (precise) |
  |---|---|---|
  | `BOOLEAN` | `Boolean` | Boolean |
  | `INT` | `int` | Integer (Int) |
  | `LONG` | `long` | Integer (BigInt) |
  | `FLOAT` | `float` | Float (Float4) |
  | `DOUBLE` | `double` | Float (Double) |
  | `DECIMAL(p, s)` | `BigDecimal` | Decimal (Numeric(p, s)) |
  | `STRING` | `String` | String |
  | `DATE` | `LocalDate` | StrictDate |
  | `DATETIME` (`TIMESTAMP` the same) | `Instant` | DateTime |

  `INT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `STRING`, `BOOLEAN`, `DATETIME` and `TIMESTAMP` parse today; `LONG` and `DATE`
  are lite's additions (`LONG` is Deephaven's usual integer: a quantity, an id, an epoch count).
- **Nullability.** Every Deephaven column can hold null (a primitive column through its null sentinel), so a column
  is `[0..1]` unless declared `NOT NULL`, which the connector then holds the server's data to.
- **A key**, for a keyed input table (`PRIMARY KEY`, as a `###Relational` table spells it), which is also what an
  upsert, a delete and a DataCube edit address.

The grammar grows by those words, in the `###Relational` spelling a lite user already knows:

```
###Deephaven
Deephaven risk::live::Store
(
  Table prices ( instrument: STRING NOT NULL, px: DOUBLE, ts: DATETIME )
  InputTable positions ( book: STRING PRIMARY KEY, instrument: STRING PRIMARY KEY, qty: LONG NOT NULL )
)
```

**The grammar's two dialects (rule A, S89; the Studio line's convention, 2026-10-22).** At `Dialect.LEGEND_LITE`
(the compile paths) the parser reads the additions; at `Dialect.LEGEND_ENGINE` (`grammarToJson`, the SDLC entity
JSON, `ModelChanges`) it reads exactly what legend-engine's grammar reads, so `LONG`, `DATE`, `NOT NULL`,
`PRIMARY KEY` and `InputTable` in a `###Deephaven` section are refused there, by name and positioned. lite's JSON
carries the new fields whole and reads them back. One test per construct, in `EngineDropsTest`'s shape: the lite
dialect reads it and the round trip keeps it; the engine dialect refuses it. `//gates:parser_equivalence` guards that
no engine text parses differently.

A table reference `#>{risk::live::Store.prices}#` resolves as any table's does (`TableReferenceChecker`, the records
`DatabaseDefinition` holds); a Deephaven store has no schemas, so a three-part reference is an unknown table.

## 3. The connection

```
###Connection
DeephavenConnection risk::live::Conn
{
  store: risk::live::Store;
  serverUrl: 'https://deephaven.internal:10000';
  authentication: # PSK { psk: 'vault:deephaven.psk'; }#;
}
```

- **`serverUrl`** becomes `ConnectionSpecification.ServerUrl(scheme, host, port, path)`, a record the non-SQL stores
  share (agreed with the MongoDB line, §7): `http` or `https` (the scheme decides TLS), a host, and a port, each
  required; anything else is refused at the model door, naming the URL.
- **Authentication** becomes `AuthenticationSpec.PreSharedKey(vaultReference)`: the text after `psk:` names the key
  in the vault, never the key itself, so no secret sits in a model and the platform holds none (Charter C3b, as every
  connection's credentials). A user's own identity (OIDC or a bearer token, so Deephaven sees who asks) is the next
  authentication to add, when a deployment needs per-user access.

## 4. Decisions

The architecture, alongside the MongoDB line's decisions (the user's rulings of 2026-10-16) so the two non-SQL
databases share one shape:

| # | question | Mongo (decided) | Deephaven |
|---|---|---|---|
| DH1 | the compiler's shape | D1: a target over the `SqlQuery` IR | **the same** |
| DH2 | the seam's type | D2: `implements SqlDialect` whole; the `QueryTarget` split if the refusal list grows | **the same**, SQL-only members refused by name. A second non-SQL target is D2's recorded trigger: taken up with the Mongo line after its branch merges, from two real targets |
| DH3 | the surface | D3: the Relation surface first | **the Relation surface** (`#>{store.table}#`, `filter`, `extend`, `groupBy`, `join`, `sort`, `limit`, windows), which is what DataCube writes. A class mapping onto a Deephaven store waits for a case |
| DH4 | the registry key | D4: `DatabaseType.MongoDB`, a lite extension | **`DatabaseType.Deephaven`**, the same way: every exhaustive switch gains an arm; `RelationalDatabaseConnection { type: Deephaven }` refused with a test |
| DH5 | parameters | D5: placeholders, typed slots, the driver binds | **the same contract**: a slot where a value goes; the connector binds it as the request's own structured literal (`FilterTableRequest`'s `Literal`), never spliced into formula text |
| DH6 | typing | D6: the declared schema only | **the store's declared columns only** (§2), checked against the server's table at run time, never inferred from it |
| DH7 | nulls | D7: SQL's semantics, emulated | **SQL's semantics.** Deephaven's nulls are sentinel values with their own comparison rules: measured on the first lane, emulated where an operand may be null |
| DH8 | the test server | D8: a pinned `mongod`, Bazel-downloaded | **a pinned Deephaven server**, Bazel-downloaded from Maven Central and started by the test (no Docker); test-only, never on a product classpath |
| DH9 | what a result is | D10: database-written text, per row for large results | **typed rows**: the cells Arrow Flight carries, read by the plan's columns (`Statements.rows`). Deephaven writes no CSV or JSON text a SQL engine would, so a text output on Deephaven is refused by name until host renderers exist (`docs/RESULTS_AND_RENDERERS_2026_10_22.md`, proposed) |
| DH10 | live results | (none) | **an execution mode, not a plan node**: a streaming connector implements `LiveStatements.subscribe(statement, slots, values, columns) → LiveHandle` beside `Statements`; the handle carries the server, the ticket, the column names, and `close`/cancel, so a sink that stops reading releases the server's table; the server gains a route answering it |
| DH11 | where the connector lives | D12: `core/src/main/mongodb`, `ServiceLoader`, bundled with `//core:drivers` | **`core/src/main/deephaven`, `ServiceLoader`, the same shape, not bundled.** Deephaven's client is under the Deephaven Community License, so the shipped product never carries it: an opt-in server target adds it, for a deployment that fetches Deephaven itself. Its jars (gRPC, Netty, Arrow) come from a `maven.install` pool of their own |
| DH12 | the connection's address | D4 named a new specification variant | **`ConnectionSpecification.ServerUrl`**, shared (§3) |
| DH13 | authentication | a vault reference the model names | **a pre-shared key's vault reference** (§3) |
| DH14 | the metamodel store | D13: refused by name first | **refused by name** |
| DH15 | input tables | (none) | **created by the connector from the store's declaration**, keyed or append-only, before the first write; rows written through the same connector (an upsert, a delete by key, an append); internal users only (the Deephaven Community License's §4: no third party defines an input table's schema) |
| DH16 | a source table's schema | (none) | **checked when a session first reads it**: the server's columns, types and nullability against the declaration, a mismatch refused by name |

## 5. What is checked, and where

All in lite's own words, each a case in `ReferenceIntegrityTest` (or the model-door tests) through both doors:

| check | where |
|---|---|
| a table the store lacks; a three-part reference | the table reference, as any store's |
| two tables, or two columns of one table, of one name | the model door |
| an unknown column type; `DECIMAL` without its precision and scale | the parser and the model door |
| a key column declared on a `Table` (keys are an input table's) | the model door |
| a `serverUrl` that does not parse, has no port, or whose scheme is not `http` or `https` | the model door |
| a connection whose `store` is not a Deephaven store; a runtime binding a Deephaven store to another kind of connection | the model door and the runtime check |
| a declared source table that differs from the server's | the connector, at a session's first read (DH16) |

## 6. The landings (stacked branches, no PR until asked)

| # | branch | content | judge |
|---|---|---|---|
| L1 | `claude/deephaven-1-model` (on main) | `DatabaseType.Deephaven` and its switch arms (each refusing by name until L2); the grammar's additions (§2: `LONG`, `DATE`, `NOT NULL`, `PRIMARY KEY`, `InputTable`) through the parser, the protocol records, the composer and the JSON; `model/DeephavenDeclarations.java` beside `MongoDeclarations` (the store into `DatabaseDefinition`, the connection into `ConnectionDefinition` with `ServerUrl` and `PreSharedKey`); `ConnectionWire`'s tag and `ConnectorsTest`'s row for `ServerUrl`; the `foreignConnections` gaps closed; §5's model-side cases; register rows; PARK-38's entry rewritten for Deephaven (lite's own checks, in lite's words: its acceptance clause names the engine's extensions) | `//gates:local`, `//gates:parser_equivalence`; the Studio line reviews |
| L2 | `claude/deephaven-2-target` (on L1) | `sql/dialect/Deephaven.java` (`SqlDialect` whole): the relation shapes DataCube writes first (scan, filter, project, extend, group-by with sums, sort, limit, natural and left joins), every other IR node, function and aggregate refused by name; unit tests on the plan text, as `MongoDbDialectTest` | the dialect's tests; every existing lane's dump unchanged |
| L3 | `claude/deephaven-3-connector` (on L2, after the Mongo branch merges its seam) | `core/src/main/deephaven`: `rows` over Arrow Flight, `subscribe` with its handle, input tables (DH15), the schema check (DH16); the `@maven_deephaven` pool; the pinned test server and `DeephavenBackend`; the opt-in server target; the guards' additive lines, each dated | lite's own Deephaven cases, each answered on DuckDB and on Deephaven and compared row for row; then the PCT lanes, as every database's |
| L4 | `claude/deephaven-4-datacube`, stacked on the DataCube line's `claude/datacube-query-base` (da0f5aec1) | DataCube's ticking mode (§9) | the DataCube line reviews |
| L5 | a demo module | the demo world: reference data in a legend-lite model, a market-data simulator writing Deephaven's source tables, a publisher writing input tables from legend-lite | runs, and its risk agrees with DuckDB's over the same inputs |

## 7. Agreed with the MongoDB line (2026-10-22)

1. **`ConnectionSpecification.ServerUrl`** as a shared record, landed in L1 with its `ConnectionWire` tag and a row in
   `exec/ConnectorsTest`'s specification table (`everySpecificationAndAuthenticationIsInTheTable` fails until every
   connector says what it does with it). Mongo is not migrated in L1: it moves to `ServerUrl` when it needs a server
   list, as its own change.
2. **No store kind on `DatabaseDefinition`**: a Deephaven store is a database to the model; its tables resolve as any.
3. **Typed rows only, text outputs refused by name.** `RESULTS_AND_RENDERERS_2026_10_22` is proposed and unruled, so
   nothing is built against its renderer names.
4. **`LiveStatements` beside `Statements`, no plan node**; the handle defines `close` and cancel.
5. **The connector absent by default**, found as Mongo's is (`Connectors.of(type)` by `ServiceLoader`, an `Unbuilt`
   refusing by name when none is on the class path), loaded from the opt-in server target. From the Mongo merge:
   `TestBackendsTest` wants a `DeephavenBackend` once the type has a dialect, and the tests iterating
   `Databases.implemented()` need the non-SQL filter Mongo's merge lands, reused.
6. **The `QueryTarget` split** taken up together once the Mongo branch is on main.
7. **Guard and registry edits additive**, each dated; whoever lands second re-adds. After the Mongo merge,
   `DatabaseConnector` gains `ownSessions()` and `open(ConnectionDefinition)`, and the connection codec is
   `model/ConnectionWire`: L3 uses those.

## 8. Register rows (`docs/SEMANTICS_REGISTER.md`: the next number free on main when L1 lands; main holds up to S96, the MongoDB branch S97-S104, and the imports branch may renumber around S89-S96, so the number is checked at landing)

1. **Deephaven is lite's own design**, not legend-engine's Deephaven extension: types, columns `[0..1]` unless
   `NOT NULL`, input tables, the connection's key as a vault reference, execution by table operations (this document).
2. `DatabaseType.Deephaven` is lite's, as S48's types and Mongo's are.
3. A Deephaven result is typed rows; its served text forms are refused until host renderers exist.

## 9. DataCube's ticking mode (agreed with the DataCube + Python line, 2026-10-22)

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
