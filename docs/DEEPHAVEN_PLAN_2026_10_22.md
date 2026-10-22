# Deephaven as a database of legend-lite: the design (2026-10-22)

**Status: PROPOSED. Nothing here is built. For the user's approval, after review by the MongoDB line (whose
`docs/MONGODB_PLAN_2026_10_16.md` this design follows decision by decision) and the Studio line (whose leg 10 the
model part is).**

## 0. The answer in one paragraph

Deephaven becomes a database of legend-lite **the way MongoDB is becoming one**, not by a road of its own. The
`###Deephaven` store and `DeephavenConnection`, which lite parses today and drops at the model door, compile into the
records every database uses (`DatabaseDefinition`, `ConnectionDefinition`), keyed by a lite extension
`DatabaseType.Deephaven`. A query on a Deephaven runtime is planned by **a render target over the same `SqlQuery`
IR**, `sql/dialect/Deephaven.java`, implementing `SqlDialect` whole as `MongoDb` does, which writes Deephaven's
table-operation plan (its `BatchTableRequest`, as protobuf JSON) instead of SQL text. A **connector in
`core/src/main/deephaven`**, found through `ServiceLoader` as Mongo's is, runs that plan with Deephaven's Java client
and answers through the same `Statements` seam Mongo's branch introduces: **typed rows** for a snapshot, and, as the
one thing Deephaven adds, **a live handle** (server, ticket, column names) for a caller that subscribes. The same plan
serves both: live or snapshot is how a caller executes it, not what was planned. DataCube is the first live caller.

## 1. What exists

- **legend-engine's own Deephaven extension** (pinned 4.145.0, `legend-engine-xts-deephaven`): the store, the
  connection, the `DeephavenApp` activator, a compiler extension (`DeephavenCompilerExtension`,
  `HelperDeephavenStoreBuilder`), a Pure translation of the Relation surface to a Deephaven query
  (`pure_to_deephaven.pure`), an execution node (`DeephavenExecutionNode`) whose executor builds a Deephaven table
  specification and streams the rows back over Arrow Flight (`DeephavenExecutionNodeExecutor`), and a PCT suite
  (`legend-engine-xt-deephaven-PCT`). **It is the spec** (the reference-checkout tenet): its words for refusals, its
  type mapping, its PCT expectations. Its Pure translation is not ported (D1 below, as Mongo's D1 rejected porting
  `pureToMongoQuery`).
- **lite today**: the grammar, protocol records (`PDeephavenDatabase`, `PDeephavenConnection`, `PPskAuth`) and the
  composer round-trip byte for byte (leg 8). At the door, the store becomes a named carrier with no tables
  (`ModelFromProtocol.java:203`) and the connection a generic element keeping `serverUrl` only
  (`FromProtocol.java:431`, `:522`). So an inline `DeephavenConnection` lands in `foreignConnections`, counts as model
  data, and the query runs on DuckDB; a standalone one is not indexed as a connection. PARK-38 records the carrier.
- **The spikes** (`experiments/deephaven-spike`, `experiments/streaming-risk-spike`): a plan written as
  `BatchTableRequest` JSON with no Deephaven library, run by a 200-line connector, subscribed from a browser through
  `dh-core.js`; and the homework that found Deephaven right on every test (deletes, as-of joins with late data, all 14
  DataCube query shapes as table operations, to 384,000 positions) where Deephaven's SQL front end refused 6 of 14.

## 2. Decisions, against Mongo's

Each row says what Mongo decided (the user's rulings of 2026-10-16), what Deephaven does, and why where it differs.

| # | question | Mongo (decided) | Deephaven (proposed) |
|---|---|---|---|
| DH1 | the compiler's shape | D1: a target over the `SqlQuery` IR | **the same.** The engine's own route is a Pure translation; lite's is the IR target, for D1's reasons |
| DH2 | the seam's type | D2: `MongoDb implements SqlDialect` whole; the `QueryTarget` split if the refusal list grows | **the same** (`Deephaven implements SqlDialect`, SQL-only members refused by name). A second non-SQL target is D2's recorded trigger: noted, and taken up with the Mongo line after its branch merges, from two real targets |
| DH3 | the surface | D3: the Relation surface first; `#>{db.Coll}#` a lite extension | **the Relation surface only**, and here it is the engine's own (`#>{store.table}#` is how the engine reaches a Deephaven store; it has no class mapping for one). The reference resolves as a relational one does (`TableReferenceChecker`), exactly as Mongo's (§5.4 there) |
| DH4 | the registry key | D4: `DatabaseType.MongoDB`, a lite extension; refused in a `RelationalDatabaseConnection` | **`DatabaseType.Deephaven`**, the same way: every exhaustive switch gains an arm; `RelationalDatabaseConnection { type: Deephaven }` refused with a test; a register row |
| DH5 | parameters | D5: placeholders, typed slots, the driver binds | **the same contract.** The writer writes a slot where a value goes; the connector binds the slot as the request's own structured literal (`FilterTableRequest`'s `Literal`), never spliced into formula text |
| DH6 | typing | D6: the declared schema only | **the store's declared columns only**, by the engine's mapping (`DeephavenCompilerExtension.convertTypes`): `STRING` text, `INT` Int, `FLOAT` Float4, `DOUBLE` Float, `BOOLEAN` Boolean, `DATETIME` and `TIMESTAMP` DateTime, `DECIMAL(p,s)` Numeric; every column `[1]` as the engine declares (its TODO says so) |
| DH7 | nulls | D7: SQL's semantics, emulated | **SQL's semantics.** Deephaven's nulls are sentinel values with their own comparison rules: measured on the first lane, emulated where an operand may be null |
| DH8 | the test server | D8: a pinned `mongod`, Bazel-downloaded | **a pinned Deephaven server**, Bazel-downloaded from Maven Central, started by the test (no Docker; the engine's own tests use Docker). Test-only: never on a product classpath (DH11) |
| DH9 | what a result is | D10: database-written text, per row for large results | **typed rows** (the cells Arrow Flight carries, read by the plan's columns: `Statements.rows`), because Deephaven writes no CSV or JSON text a SQL engine would. The served text forms then need the host renderers of `docs/RESULTS_AND_RENDERERS_2026_10_22.md` (proposed); until those are ratified, a text output on Deephaven is refused by name |
| DH10 | live results | (none) | **an execution mode, not a plan node.** The plan is the base query, as the results proposal's D1 says; a connector that can stream offers `subscribe(plan) → LiveHandle` beside `rows`, and the server a route that answers the handle. No `ExecutionPlan` node is added |
| DH11 | where the connector lives | D12: `core/src/main/mongodb`, `ServiceLoader`, bundled with `//core:drivers` | **`core/src/main/deephaven`, `ServiceLoader`, the same shape; not bundled with `//core:drivers`.** Deephaven's client is under the Deephaven Community License, so the shipped server never carries it: an opt-in server target adds it for a deployment that fetches Deephaven itself. Its jars (gRPC, Netty, Arrow) come from a `maven.install` pool of their own |
| DH12 | the connection's address | D4 named one new specification variant (`MongoServers`); the branch reused `StaticDatasource` | **a new `ConnectionSpecification.ServerUrl(scheme, host, port, path)`**: Deephaven's `serverUrl` is a URL whose scheme decides TLS, which `StaticDatasource` cannot hold. Proposed as the one URL record the non-SQL stores share (Mongo's server list and Elasticsearch's URL are URLs too) |
| DH13 | authentication | the vault reference the model names; a pre-shared key refused | **a new `AuthenticationSpec.PreSharedKey(vaultReference)`**: the grammar's `psk: '...'` is read as the vault reference that names the key, so no secret sits in model text and the platform holds none (C3b, as Mongo reads its password). The engine reads the key itself: a register row |
| DH14 | the metamodel store | D13: refused by name first | **refused by name** |

## 3. The engine's checks, in its words (PARK-38's acceptance)

Each is a case in `ReferenceIntegrityTest` (`refused` through both doors where the engine's grammar reads the text):

| check | the engine's words | where lite makes it |
|---|---|---|
| a table the store lacks | `The table X can't be found in the store Y` | **lite's relational words** (`unknown table ...`), as Mongo's reference gives: one register row for the wording (DH3). The alternative, a store kind on `DatabaseDefinition` so the checker can say Deephaven's words, is open with the Mongo line (§6) |
| a reference with three parts (`#>{s.schema.t}#`) | `Deephaven accessor requires 2 params (Store and Table) ...` | refused by the same register row's rule (a Deephaven store has no schemas, so the third part is an unknown table) |
| a URL with no port | `Deephaven URL is missing port: <url>` | the model door, from `PDeephavenConnection` |
| a URL whose scheme is not `http` or `https` | the engine's unknown-enumeration-value error for `meta::pure::functions::io::http::URLScheme` | the model door |
| a URL that does not parse | `URL format is not valid` (a parser error in the engine) | the model door (lite's parser keeps the text) |
| a column type the engine cannot compile | `CustomType is not yet supported in Deephaven store compilation` | unreachable from the grammar; refused at the door if the JSON carries it |
| the activator's function | resolved by its descriptor, as every activator's | already the activators' path, if it checks one; else parked with PARK-38's remainder |

## 4. The landings (stacked branches, no PR until asked)

| # | branch | content | judge |
|---|---|---|---|
| L1 | `claude/deephaven-1-model` (on main) | `DatabaseType.Deephaven` and its switch arms (each refusing by name until L2); `model/DeephavenDeclarations.java` beside `MongoDeclarations` (store, connection, `ServerUrl`, `PreSharedKey`); the door's three call sites; the `foreignConnections` gaps closed by the connection becoming a `ConnectionDefinition`; the §3 cases; register rows; PARK-38 marked | `//gates:local`, `//gates:parser_equivalence`; Studio reviews |
| L2 | `claude/deephaven-2-target` (on L1) | `sql/dialect/Deephaven.java` (`SqlDialect` whole): the relation shapes DataCube uses first (scan, filter, project, extend, group-by with sums, sort, limit, natural and left joins), every other IR node, function and aggregate refused by name; unit tests on the plan text, as `MongoDbDialectTest` | the dialect's tests; every existing lane's dump unchanged |
| L3 | `claude/deephaven-3-connector` (on L2, after the Mongo branch merges the `Statements` seam) | `core/src/main/deephaven` (`rows` over Arrow Flight; `subscribe` answering the handle); the `@maven_deephaven` pool; the pinned test server; the opt-in server target; the guards' additive lines (`JDBC_AND_DRIVERS`, `PRODUCT_LIBRARIES`, `DialectLeakRatchetTest.PRODUCTS`, `DatabaseOwners.DATABASES`), each dated | a Deephaven lane of lite's own cases, rows compared with DuckDB's; then the engine's Deephaven PCT as test input |
| L4 | the DataCube line's (or stacked on theirs) | DataCube's ticking mode over the handle | the DataCube line's |
| L5 | `experiments/` or a demo module | the demo world: a reference model in legend-lite, a market-data simulator, a publisher | runs |

## 5. Register rows (`docs/SEMANTICS_REGISTER.md`, numbered after S96)

1. `DatabaseType.Deephaven` is lite's (the engine has no database type for its Deephaven store), as S48 and Mongo's.
2. A Deephaven table reference refused in lite's relational words, not the engine's two Deephaven sentences (DH3; or
   no row, if §6.2 decides a store kind).
3. A `DeephavenConnection`'s `psk` read as a vault reference, not as the key (DH13).
4. A Deephaven result read as typed rows; its served text forms refused until host renderers exist (DH9).

## 6. Open with the MongoDB line

1. **`ConnectionSpecification.ServerUrl`** (DH12): one URL record for the non-SQL stores. Would Mongo's `MongoServers`
   take a list of them, or keep `StaticDatasource`?
2. **A store kind on `DatabaseDefinition`**: none (Mongo's choice: the store is a database, its references relational)
   or one field both lines set (so a store's own refusals can be in its own words, and a relational mapping onto a
   non-relational store can be refused). Recommended: none, for now; the register row above instead.
3. **`subscribe` beside `rows`** (DH10): a capability interface a streaming connector implements, or a method on
   `Statements` the others refuse by name. Recommended: a separate interface (`LiveStatements`), so no JDBC or Mongo
   code changes.
4. **The order of the guard and registry edits**: additive lines, dated; whoever lands second re-adds (agreed
   2026-10-22).
5. **The `QueryTarget` split** (DH2): taken up together once the Mongo branch is on main.

## 7. Open with the user

1. Approve the design, or name what to change.
2. DH13: the key as a vault reference (recommended), or as written, as the engine reads it.
3. DH11: the opt-in server target, so the shipped product carries no Deephaven code (recommended), or bundled.
