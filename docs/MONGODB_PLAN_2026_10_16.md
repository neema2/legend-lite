# MongoDB as a first-class database of legend-lite: the study and the plan (2026-10-16)

**Status: decided and in execution (2026-10-16). The user ruled on every decision of §4 (D1 to D14) and asked for a
viable v0 (§11); the line's announcement in `docs/IN_FLIGHT.md` is part of v0's first landing.** The ask (the user, 2026-10-16): learn what backends legend-lite supports
today (all SQL), then plan MongoDB as a first-class database, PCT lanes included, built "the pristine way": the way the
dialect consolidation program (the ninth line, `docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md`) lays out the end
state, so that Mongo is done right the first time rather than consolidated later. That program names MongoDB as out of
its own scope ("MongoDB, Elasticsearch and SingleStore are new targets, not consolidation", its §0) and the backend
order already places it next after the corpus-on-every-database work (`docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §2 item 4,
§11, §20; `docs/IN_FLIGHT.md`, the eighth line's "Next").

§0 is the answer; §1 the backends today; §2 the dialect program's end state and where Mongo fits; §3 what the tree
already holds for Mongo; §4 the decisions; §5 the design; §6 the PCT lane; §7 the landings; §8 the asks to the other
lines; §9 the risks and the measurements that settle them; §10 the sources.

---

## 0. Start here

1. **What exists.** Eleven executing databases, every one SQL over JDBC: DuckDB (the platform's own engine and the PCT
   reference lane), H2, Postgres, SQLite, MariaDB, MySQL, Trino, ClickHouse, Snowflake, Databricks and BigQuery, each a
   class in `core/src/main/java/com/legend/sql/dialect/` chosen by one registry, `database/Databases.dialect(type)`;
   plus three engine-text printers (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`) that write legend-engine's
   golden SQL text and execute nothing. The `DatabaseType` enum (`model/ConnectionDefinition.java:54`) is legend-engine's
   21 values plus lite's SQLite, MariaDB and MySQL (register row S48); the 13 others (DB2, SqlServer, Oracle, ...) are
   refused by name in every switch. The database enters the pipeline at three seams: plan side, `Databases.dialect`
   (phase J); execution side, `exec/Sessions` and the JDBC runners (phase K); the test harness, `PctBackend.Lane`.
   Every JDBC use is funnelled to `exec`, `server`, `testdatagen` and the root by `ArchitectureTest
   .javaSqlIsFunnelledToTheCharteredSeam`.
2. **The answer in one paragraph.** MongoDB is a **render target of the one compiler over the same `SqlQuery` IR**: a
   class beside the SQL dialects that decides, for every IR node, function and aggregate, what the aggregation pipeline
   spells, emulates or refuses by name; a **connector beside JDBC** that opens a `MongoClient`, runs the rendered
   command with its bound values and carries the one text the database wrote; and a **test backend beside the embedded
   servers** that starts a pinned `mongod` Bazel unpacked, as `EmbeddedClickHouse` starts ClickHouse. That is exactly the
   dialect program's acceptance test (§4 there: "a new database is three classes and one line in each of three
   registries"), and exactly what `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §11 already says of the non-SQL stores:
   "beside `SqlDialect`, not inside it: the Lowerer's output (the `SqlQuery` IR) stays the input, and a new target
   translates it into the store's own query language. The executor gains a second chartered seam beside JDBC." The MIR
   does not change (AGENTS.md invariant 3a: "new dialect = one class implementing `render(SqlQuery)`"); the Lowerer does
   not change; the PCT harness gains a lane the way ClickHouse's was gained. The execution tenet holds unchanged: the
   store executes, the pipeline builds the wire text, Java carries a string, and what Mongo cannot express is a
   `DialectCapability` wall, never a value computed in Java.
3. **The one structural dependency, and the ask to make now.** The dialect program's Phase 5 (the connector interface,
   `DatabaseConnector` and `Connectors.of(type)`) and Phase 6 (`TestBackend`) are not built yet (Phases 0 to 4 are on
   main: neema2/legend-lite#39, #45, #49). Today a session IS a `java.sql.Connection` (`Sessions.Session.connection()`,
   `PlanSessions`, `PlanRunner`, `QueryService.execute(..., Connection, ...)`). If Phase 5 is designed JDBC-only, Mongo
   has to fork the executor and the harness, which is the consolidation debt this plan exists to avoid. **The ask to the
   ninth line (§8.1): a connector's session is a sealed type over transports, the connector owns `check`, `version`,
   `run` and the bulk load, and `RenderedStatement` carries the language its text is in.** Mongo is then Phase 5's and
   Phase 6's first non-JDBC instance, built with the dialect session or right after it.
4. **The language is the engine's own.** legend-engine's `###MongoDB` store (collections with a JSON schema),
   `MongoDBConnection` and the `MongoDB` class mapping already parse, compose and round-trip in lite byte for byte
   (`parser/section/MongoDBSectionGrammar.java`, `protocol/Protocol.PMongoDatabase`, `PMongoDbConnection`,
   `PClassMappingMongoDb`), carried opaque in the model and never compiled or run. First-class means those compile,
   resolve, lower, render and execute; and the Relation surface (`#>{db.Collection}#`, `project`, `filter`, `groupBy`,
   ...) over a collection, which is what PCT and DataCube use, comes first (decision D3).
5. **Order.** After the eighth line's corpus-on-every-database harness, as the user ordered (§20 there), and with the
   dialect program's Phase 5 and 6: the seam first (one pure landing, measured by every existing lane's SQL dump staying
   byte-identical), then the server and the lane scaffold, then the target in two halves (scalar suites, then the
   relation suites), then the store and the mapping, then the corpus. Six landings, about 12k to 16k lines (§7).
6. **The rules this line keeps**, inherited from the Postgres, IQ and Cypher lines and the dialect program: zero FAIL,
   zero BUG, zero ERROR, a refusal by name beats a possibly wrong answer; Java never computes a value; every per-database
   decision in Mongo's own files (the owner guard holds it there); every pin dated with its reason; every landing
   announced in `IN_FLIGHT` first and recorded in `docs/GATES.md`.

---

## 1. The backends today

| database | dialect class | lines | how its PCT lane runs | lane in `//gates` | notes |
|---|---|---:|---|---|---|
| DuckDB | `DuckDb` | 1,027 | in-process, `jdbc:duckdb:` | `pct_duckdb` (per push) | the platform's own engine (`Databases.PLATFORM`), the PCT reference lane; refuses nothing in the capability matrix |
| H2 | `H2` (+`H2Modern`) | — | in-process, `jdbc:h2:mem:` | `pct_h2` (Relation only) | also the SQL-text replay referee (`Databases.REPLAY_REFEREE`); refuses 48 functions (lists, DuckDB-only functions, D1) |
| Postgres | `Postgres` | 2,343 | embedded 16, Bazel-unpacked (`@embedded_postgres`) | `pct_postgres` (per push) | refuses root: the unprivileged recipe in a cloud container (`docs/GATES.md`) |
| SQLite | `SQLite` | 3,156 | in-process, `jdbc:sqlite::memory:` | heavy | regular expressions as a recursive walk in SQL; the 1 MB statement cap pass |
| MariaDB, MySQL | `MariaDb`, `MySql` | — | embedded 11.4 / 8.4 servers (`tools/mariadb`, `tools/mysql`) | heavy | MySQL a family member of MariaDB (one grouped arm delegates) |
| Trino | `Trino` | — | embedded 483 coordinator, memory connector | heavy | one JVM child per target |
| ClickHouse | `ClickHouse` | 2,880 | embedded 25.8 static build (`tools/clickhouse/clickhouse.bzl`) | heavy | the pattern a `mongod` lane copies: a pinned tarball, sha256, one binary kept, Linux x86_64 only |
| Snowflake, Databricks, BigQuery | one class each | — | a real account (`CloudLane`, credentials from a file outside the repo) | heavy, `external`, `local` | |

Facts the plan stands on, each read in the tree:

- **The plan-side seam** is `SqlDialect` (`sql/dialect/SqlDialect.java`, 198 lines): `render(SqlQuery)` → text,
  `renderStatement(SqlQuery)` → `RenderedStatement(sql, binds)` (text with one `?` per bound parameter, in placeholder
  order, and `TypeHole`s where H2 needs a type written at prepare time), plus the SQL-only members: `render(SqlDdl)`,
  `render(SqlDml)`, `sessionSetup()`, `sessionTable`, `raw`/`fromEngineText` (decision D2), `physicalName`/`storedName`
  (D8), `catalogType`, `bareNameReaches`, `copySelectList`, `hostOperations`, `script`/`scriptAbort`, and the reads of the
  driver's answers (`normalize`, `wireTypeName`, `typingWireName`, `deliversOwn`, Phase 4). Every dialect writes
  through one `SqlWriter` (text in order, each bind at its place; `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §10, E).
- **The base renderer spells nothing** since Phase 1 (2026-10-13): `AnsiSqlRenderer` is abstract in every vocabulary
  point (`call`, `reducer`, `scalarTypeName`, `formatText`, `structTypeName`, `passes`); each concrete dialect's `call`
  is one exhaustive switch over `SqlFn` (206 constants) with one expression per arm, its `reducer` one over
  `SqlAgg.Fn` (21), and `DialectVocabularyTest` fails a `default`, a block arm or a function handed to the base. The
  capability matrix (`docs/dialect-capability/`, `//core:capability_matrix`) reads those switches from the source of
  every class in the package, and `//core:capability_reach` measures which arms the five PCT suites reach on each
  database. A new class in `sql/dialect/` is read with no BUILD line (the program's §4 acceptance test).
- **The IR** (`com.legend.sql`, 15 files): `SqlQuery` permits `SqlSelect` and `SqlUnion`; `SqlSource` has `Pivot`,
  `SourceUrl`, `Table`, `Cte`, `VarSetPlaceholder`, `Dual`, `Subselect`, `Values`, `RawSql`, `Catalog`, `Join` (kinds
  INNER, LEFT, RIGHT, FULL, CROSS, CROSS_LATERAL, ASOF_LEFT, LEFT_LATERAL); `SqlExpr` has 46 variants, lambdas among
  them (`Lambda`, `FoldCall`); `SqlWith` carries CTEs with a `recursive` flag. Every dialect runs its own `passes()`
  (`SqlRewriter`s) at render entry: the carrier strategy first (`CarrierStrategies`, over the dialect's `CAPS`), then
  its own shapes (`PivotAsGroupBy`, `AsOfAsPick`, `QualifyToSubselect`, `LateralExplodeToUnion`,
  `FoldsAsRecursiveCtes`, `CommonSubexpressions`, ...). This is the mechanism a Mongo target uses to turn IR shapes it
  cannot spell directly into ones it can.
- **The execution side.** The new path: `PlanMaker.plan` renders through `dialect.renderStatement` into an
  `ExecutionPlan.Sql(statement, slots, target, ...)`, and `exec/PlanRunner` runs it with no model and no dialect:
  opens a `Sessions.Session` (a `java.sql.Connection`), checks the product name and version (`Sessions.check`,
  `Sessions.version`), runs the target's session statements, prepares the statement, binds each slot
  (`setObject`, `setNull`, `createArrayOf`), and carries the one text column the database wrote. The legacy path
  (`StatementExecutor`, `exec/Executor` with its 24 `ResultSet` sites, `Execution.execute*`) still serves the server, the
  corpus and PCT channel A (`PctExecuteNative` → `QueryService.execute(model, query, runtime, Connection,
  ExecuteOptions.TDS_TEXT)`, `PCT_RENDER` until 2026-10-17); the database owner's line is deleting it ("step 4: the callers switch, the old paths
  deleted", `docs/IN_FLIGHT.md` line 3).
- **The wire is written by the database**, never assembled in Java: `lowering/WireRender.wrap` composes the CSV or JSON
  text of the whole result as one SQL value (`Render.csvWire`, `jsonWire`, `jsonWireRows`), and PCT's TDS text the same
  way (`Render.tdsText`: header from the typed schema, rows newline-prefixed, cells in Pure's print forms). The runner
  carries a string (Charter C1.2). Parameters never touch text (`EXECUTION_PLAN_BOUNDARY` P3: "JDBC bind parameters for
  SQL; a command document's values for a future Mongo-like store").
- **The harness.** `PctBackend.Lane` (an enum, one arm per database: `open()`, `openSystem()`, the storeless runtime's
  declaration), chosen by `LEGENDLITE_PCT_BACKEND`; `test/StorelessRuntime` declares an empty `###Relational Database`
  bound to a `RelationalDatabaseConnection` of the lane's type (legend-engine's PCT adapter's shape); five suite
  classes per lane (`Test_LegendLite_<Db>_<Suite>Functions_PCT`), each expected failure pinned by a stable part of its
  message, a pinned test that starts passing failing the run; `PctCensusGate`'s per-suite ceilings; channel B
  (`channelb/ChannelB`, `ChannelBLane`'s PASS floors and wire-bug ceilings per lane); the lanes in `pct/BUILD.bazel`
  (`pct_<db>_<suite>`, five `junit_test`s and a suite, `manual` and in `//gates:heavy` for every database but DuckDB,
  H2 and Postgres). A server lane also hosts the platform's **system database** (`exec/SystemDatabase.routeFor`: the
  metamodel store seeded as session-private temporary tables on a system connection the host configures).
- **The guards a new database meets** (each moves only with a dated reason, AGENTS.md): `ArchitectureTest
  .JDBC_AND_DRIVERS` (the driver packages funnelled to the seam; `com.mongodb..` and `org.bson..` join it),
  `DialectBoundaryTest` (counts dialect constructions, 11 today, and every driver), `DialectLeakRatchetTest` and
  `DialectOwnerExclusivityTest` (`DatabaseOwners.DATABASES` already lists `MongoDB` as `mongo`: every identifier naming
  Mongo outside an owner is a counted leak, so Mongo's files must be owners by path or class name),
  `DialectVocabularyTest`, `CodeShapeGuardrailTest` (250 lines a method), `CarrierPurityRatchetTest`,
  `JavaEvalLedgerTest` (the named residue), `SqlTextRatchetTest`, `JdbcSurfaceCensusTest`, the capability matrix's
  diff test in `//:generated`, `PctCensusGate` and `ChannelBLane`.
- **This container** reaches `fastdl.mongodb.org` (a pinned `mongod` tarball downloads; HTTP 200 measured 2026-10-16)
  and Maven Central (`org.mongodb:mongodb-driver-sync`; 200), and has a Docker client; GitHub release pages are
  refused by the proxy (403, `docs/CLOUD_SESSIONS.md`). Bazel is not installed in this session; nothing here was
  built or run.

---

## 2. The dialect program's end state, and where Mongo fits

The program's §4, the target every phase builds toward, restated with Mongo's row filled in:

| layer | the one class per database | Mongo's | registry |
|---|---|---|---|
| **query target** (plan side, no driver) | `sql/dialect/<Db>.java`: all of the database's text, its function, type and keyword decisions, its capabilities, reading the driver's names back | `sql/dialect/MongoDb.java`: the aggregation pipeline for every IR shape, the operator for every function and aggregate, `$documents` for literal relations, the wire text as the pipeline's tail, BSON type names, every refusal by name | `Databases.dialect(type)` (today) |
| **connector** (execution side) | `exec/connect/<Db>Connector.java` (Phase 5, not built): product, URL from a connection spec, driver, private instance, bulk load, driver cells, raise, in-memory declaration | `MongoDbConnector`: a `MongoClient` from a `MongoDBConnection`'s servers and auth; runs an `aggregate` command with its `let` values; carries the text; `insertMany` as the bulk load; the server's version check; the system database as a session-private database | `Connectors.of(type)` (Phase 5; `Sessions`' switches until then) |
| **test backend** (harness) | `testing/.../<Db>Backend.java` (Phase 6, not built; absorbs `PctBackend.Lane`'s arms): start the server, a fresh workspace, fixture isolation, the referee's reader, the connection block, seeding, pins and ceilings | `MongoDbBackend` over `EmbeddedMongoDb` (`tools/mongodb/mongodb.bzl`): a workspace is a database per package, isolation a database copy (`$out`/`$merge` or re-seed), the connection block a `MongoDBConnection` | `TestBackends.of(type)` (Phase 6; `PctBackend.Lane` until then) |

What Mongo reuses from the program **verbatim**: the exhaustive-switch discipline and its guard; the capability matrix
and the reach measurement (a Mongo column appears on the next regeneration, and `reach.tsv` gains its lane); the
carrier-strategy pass over a `CAPS` row (Mongo's lists, structs and maps are native, like DuckDB's); the shared shape
passes (`PivotAsGroupBy` for a static pivot, `AsOfAsPick` for an as-of join, `QualifyToSubselect`); decision D2 for raw
text (text written for Mongo runs as written; the engine's H2 text is refused by name); Phase 4's reads
(`wireTypeName`: BSON type names into the wire vocabulary); Phase 0's guards.

What Mongo **stresses**, each a question the program's remaining phases must answer with a non-SQL member in mind:

1. **`SqlDialect` is the SQL interface.** `render(SqlDdl)`, `render(SqlDml)`, `sessionSetup()` (SQL strings the executor
   runs), `fromEngineText`, `catalogType`, `copySelectList`, `script` are SQL-only. Two shapes are possible (decision
   D2 of §4): Mongo implements `SqlDialect` and refuses every SQL-only member by name; or a supertype `QueryTarget`
   holds what every target has (`renderStatement`, `passes`, the vocabulary switches, the wire reads, `raw`,
   `sessionSetup`) and `SqlDialect extends QueryTarget` keeps the rest.
2. **The session is a `java.sql.Connection` by type.** `Sessions.Session.connection()`, `Sessions.Opener`,
   `PlanSessions.Source`, `PlanRunner.text` (prepare, bind, `executeQuery`, `getString(1)`), `Sessions.check` and
   `Sessions.version` (JDBC metadata), `QueryService.execute(..., Connection, ...)`, `PctBackend.Lane.open()`. Phase 5 is
   where this becomes a connector's own (`check`, `version`, `run`), and the sealed session type is the ask of §8.1.
3. **The statement is SQL by name and by type.** `ExecutionPlan.Sql`, `RenderedStatement.sql`, `ExecutionPlan.Format`,
   `SetupStep.Statement(String sql)`. The plan's text is the TARGET's language; the record needs to say which
   (`Language.SQL`, `Language.MONGO_COMMAND`), and the runner dispatches on it (a sealed switch, no default).
4. **`DatabaseType` is relational.** Every registry, `Target.type()`, `ExecutionPlan.Target.Database`,
   `StorelessRuntime`, the connection grammar and the owner guard key on it. Decision D4 of §4.
5. **The legacy executor path.** `Executor`'s `ResultSet` readers type cells from JDBC (24 sites); PCT channel A and the
   corpus reach the database through it. Mongo should never gain a second reader there: it runs through `PlanRunner`
   only, which means channel A on Mongo takes the plan path (§6, §8.2).

---

## 3. What the tree already holds for Mongo

Legend-engine's MongoDB store, the vocabulary (decision D9: the lexer, parser, protocol and model may carry any
database's name and decide nothing by it), parsed and composed since the parser drop-in program:

- **The store**: `###MongoDB` `Database qn ( include qn; Collection name ( validationLevel: strict|moderate;
  validationAction: error|warn; jsonSchema: { ... }; ) Join ... )` (`parser/section/MongoDBSectionGrammar.java`, 277
  lines; `Protocol.PMongoDatabase` with `PMongoCollection(name, validationLevel, validationAction, PBsonSchema)`;
  `TailEmitter.mongoDatabase` writes the engine's JSON). `include` and `Join` parse and are dropped, as the engine's
  walker drops them (the fixture `scripts/parser/fixtures/t2-mongodb.pure` records why). The JSON schema is read into
  typed BSON schema nodes at parse time, as the engine does ("`bsonType: object` is mandatory").
- **The mapping**: `*Class[id]: MongoDB { ~filter [db] name  ~mainCollection [db] Coll  ~binding qn }`
  (`MappingProtocolParser.parseMongoDbClassMapping`, `Protocol.PClassMappingMongoDb`; `MappingFromProtocol.classMapping`
  returns null for it: "a FOREIGN store class mapping rides the protocol only").
- **The connection**: `MongoDBConnection { database: name; store: qn; serverURLs: [host:port, ...]; authentication:
  # UserPassword { username: '...'; password: <vault secret> }#; }` (`ConnectionReader.mongo`, `PMongoDbConnection(databaseName,
  serverUrls, auth, ...)`, `AuthenticationComposer`, `MongoComposer`); in the model it is a
  `GenericSectionElementDefinition("Connection", "MongoDBConnection", ...)` carrying only the database name
  (`FromProtocol.java:426`), so `Compiler.executesOn` refuses a runtime binding it ("connection ... is not defined").
- **The guards** already know the name: `DatabaseOwners.DATABASES` maps `MongoDB` to the pattern `mongo`;
  `DialectLeakRatchetTest`'s product pattern lists `mongo|elastic`; `DialectOwnerExclusivityTest`'s allowlist names
  `PMongoDbConnection`. So a `sql/dialect/MongoDb.java` is an owner of Mongo by its class name with no guard edit, a
  `core/src/main/mongodb/` source set needs one line in `DatabaseOwners.of` beside `core/src/main/duckdb/`, and
  anything else naming Mongo is a leak the ratchet counts.
- **Nothing executes.** No `DatabaseType`, no dialect, no session, no lane, no data seeding. `docs/EXECUTION_PLAN
  _BOUNDARY_2026_10_05.md` already decided the parameter form for "a future Mongo-like store" (P3: a command document's
  values, never text), and that legend-engine's Mongo plan nodes need its generated Java, which lite never runs.

---

## 4. Decisions for the user

**Every decision below was ruled by the user on 2026-10-16 (the "Decided" line under each heading); the options are kept as the record of what was weighed.** Each decision below says what is being decided, every option worth taking with its pros, cons, cost and complexity,
the recommendation and why, and what reversing it later would cost. The scale: **cost** S is under 300 lines in one
line's own files; M is 300 to 1,500 lines, or one landing another line must co-sign; L is 1,500 to 6,000 lines; XL is a
program of its own. **Complexity** names what makes the option hard, not how long it takes. **Reversal** is the price of
changing the decision after it has landed.

| # | decision | recommendation | cost | reversal |
|---|---|---|---|---|
| D1 | the compiler's shape | a target over the `SqlQuery` IR | L (the target itself) | XL: a second lowering |
| D2 | the seam's type | `MongoDb implements SqlDialect` whole; no split now (decided) | S | M: the split, if the refusal list grows |
| D3 | the model surface first | the Relation surface, the class mapping on it | M | none: order only |
| D4 | how the registries name Mongo | `DatabaseType.MongoDB`, a lite extension | S to M | M: a second enum re-keys every registry |
| D5 | parameters | the command's `let` variables | S | S |
| D6 | typing a collection | its declared JSON schema, and only that | S to M | S: a sugar grammar can be added |
| D7 | the null model | SQL's, emulated; nullability stamps as the optimisation | M | unbounded pins if not |
| D8 | the lane's server | a pinned Community `mongod`, Bazel-downloaded | S | S |
| D9 | the version floor | pin 8.0; floor 7.0 if PR0 finds nothing 8.0-only | S | S |
| D10 | the wire text | the pipeline writes it; per-row carriage for large results | S + M | a tenet exception if not |
| D11 | server-side JavaScript | never | none | none |
| D12 | where the connector lives | `core/src/main/mongodb`, found by `ServiceLoader` | S | M |
| D13 | the metamodel store on Mongo | refused by name first; seeded collections in the burn-down | S, then M | none |
| D14 | the line and its order | start everything now, under three constraints (decided) | coordination | none |

### D1. The compiler's shape: a target over the IR, or a lowering of Mongo's own

**Decided (the user, 2026-10-16): a target over the `SqlQuery` IR.**

**What is decided.** Whether Mongo's pipeline is produced from the Lowerer's output (the `SqlQuery` IR, phase I),
by a class that decides each IR node as the SQL dialects do, or from the typed Pure (phase G½/H) by a lowering written
for Mongo, as legend-engine's `pureToMongoQuery` is.

**Option A, a target over the IR (recommended).** One class in `sql/dialect/`, `MongoDb`, with its own `passes()`
(IR rewrites into shapes it can spell: joins as lookups, subselects as sub-pipelines, static pivots through
`PivotAsGroupBy`) and the exhaustive switches over `SqlFn` and `SqlAgg.Fn`. Inside the class the pipeline may be built
as stage records first and written once (a dialect-private intermediate, as the carrier pass already builds its own
nodes), which keeps each stage testable; the MIR itself does not change.
- Pros: invariant 3a holds ("new dialect = one class; MIR does not change"); the Lowerer, the resolver, the mappings,
  the folds, the windows and the carriers are reused whole; the vocabulary guard, the capability matrix and the reach
  measurement apply with no edit; the wire wraps (`csvWire`, `jsonWire`, `tdsText`) are IR compositions, so they are
  rendered by the target like any select; PCT's storeless path reaches it; Elasticsearch later takes the same seam.
- Cons: the IR is SQL-shaped (one select with FROM, JOIN, WHERE, GROUP BY; subselects as the one nesting) and a
  pipeline is linear, so joins, correlated subqueries and exists tests are emulations (`$lookup` with `let`, then
  `$unwind` or `$size`); the IR's meanings are the platform's written contract (the dialect program's D3: DuckDB's
  typing rules, three-valued nulls) and Mongo emulates them (D7); a few IR constructs are refusals by name (a
  recursive CTE beyond `$graphLookup`'s shape, raw SQL text, the catalog grid); pipelines are more text per shape than
  SQL, so the class is the largest dialect.
- Cost: L for the class (4,000 to 6,000 lines in PR3, about 3,000 in PR4); nothing outside it. Complexity: the shape
  passes and the null emulation; architecturally nothing new.

**Option B, a lowering of Mongo's own from the typed Pure.** `TypedSpec` to a Mongo IR to text, beside the Lowerer.
- Pros: the pipeline is built in its own shape from the start; for the engine's narrow subset (filters on root
  fields, graph fetch) it is the shorter path; no fight with SQL semantics inside the translator.
- Cons: a second compiler: the resolver's and Lowerer's structural work (joins through mappings, inlining, folds,
  windows, carriers, the wire wraps) is done twice and drifts; it is invisible to the vocabulary guard, the matrix and
  the reach measurement; it ports the engine's Pure program to Java (Charter C6.1); every later IR feature is built
  twice; PCT's reach cannot measure it.
- Cost: XL (10,000 to 15,000 lines plus its own guards). Complexity: high, and permanent.

**Option C, translate rendered SQL text to a pipeline.** Rejected: text parsing of the platform's own SQL is the leak
the dialect program spent Phase 1 removing, and a SQL parser for one database is a program of its own.

**Reversal.** From A to B is a second lowering (XL). From B to A is a rewrite. A is also the one the program's §11
already names.

### D2. The seam's type: implement `SqlDialect` whole, or split a `QueryTarget` above it

**Decided (the user, 2026-10-16): **Mongo implements `SqlDialect` whole; no split now.** A re-read of the interface showed nearly every member has a Mongo meaning once "SQL text" is read as "the text the connector runs": DDL is a `create` with a validator, DML an `insertMany`, `sessionSetup` and `sessionTable` commands, `raw` a user's pipeline run as written, `storedName` a field path, `catalogType` the collection's validator. The members with no meaning are `fromEngineText` and `copySelectList` (already refusals on most SQL dialects) and three JDBC cell conventions a non-JDBC runner never calls. No `Language` field either: the plan's target carries `DatabaseType.MongoDB`, and the connector registered for that type runs its text. The split is reconsidered only if the refusal list grows past that. Atlas SQL (the `mongosql` dialect, Atlas-only) and the deprecated BI Connector are not reusable: SQL text in, Atlas endpoints only, a compiler of its own; their existence is evidence that MQL emulates SQL well.**

**What is decided.** What type a non-SQL target has, and what the plan records and the runner switch on.

**Option A, split (recommended, with the dialect program's Phase 5).** `QueryTarget` holds what every target has
(`renderStatement`, `render` as text, `passes`, the vocabulary switches, `sessionSetup`, `raw`, `forServer`,
`needsStaticPivot`, `wireTypeName`, `typingWireName`, `deliversOwn`, `normalize`); `SqlDialect extends QueryTarget`
keeps the SQL-only members (DDL, DML, `fromEngineText`, `physicalName`/`storedName`, the catalog, `copySelectList`,
`hostOperations`, `script`). `Databases.target(type)` answers a `QueryTarget`; `Databases.dialect(type)` answers a
`SqlDialect` and refuses a non-SQL type by name. `RenderedStatement` gains `Language` (`SQL`, `MONGO_COMMAND`), carried
into `ExecutionPlan.Sql`, and `PlanRunner` switches on it with no default.
- Pros: the types say the truth (a non-SQL target cannot be asked for DDL or engine text, by compile error, not by
  refusal arm); the plan record names its language; javac's exhaustiveness covers the runner's dispatch; no SQL-only
  member leaks (`sessionSetup` strings would otherwise be run as SQL); Elasticsearch reuses it; it is §11's own
  sentence, "beside `SqlDialect`, not inside it".
- Cons: an interface edit under the dialect program's ownership, touching every caller's declared type
  (`Compiler.dialectOf`, `PlanMaker`, `StatementExecutor`, `Execution`, `Executor.execute`, the wasm `Boundary`): a
  pure move the program must co-sign and prove by its lane dumps; it ties PR1 to Phase 5's timing.
- Cost: M (600 to 1,000 lines moved or re-typed, one co-signed landing). Complexity: low in code; the coordination is
  the work.

**Option B, `MongoDb implements SqlDialect`, refusing every SQL-only member by name.**
- Pros: no interface edit; lands without Phase 5; the smallest first cut; the capability matrix reads it as it reads
  any class in the package.
- Cons: about a dozen refusal arms that exist only because the type is wrong; `ExecutionPlan.Sql.statement` and
  `RenderedStatement.sql` carry a command under a SQL name; `sessionSetup()` must answer empty or the executor runs
  its strings as SQL; the runner and the PCT bridge still need a non-JDBC session, so the execution-side edit happens
  anyway; it is the identity-in-disguise shape (`rawH2IsNative`, `rawTarget`) the program removed in Phase 1c.
- Cost: S now (about 100 lines), M later to split regardless. Complexity: none now, debt later.

**Option C, a separate `DocumentTarget` interface and registry, no supertype.**
- Pros: `SqlDialect` untouched; Mongo wholly apart.
- Cons: every consumer (the plan maker, the runner, the PCT bridge, DataCube's planner boundary) grows a second
  branch; `Target.type()` must say which registry answers; Elasticsearch adds a third branch everywhere.
- Cost: M, growing with every consumer. Not recommended.

**Reversal.** B to A is the same edit as A, done later against more code. A is the one to make once.

### D3. The model surface first: the Relation surface, or legend-engine's class mapping

**Decided (the user, 2026-10-16): **the Relation surface first, under one rule: the document is a Variant, and typed columns are views on it.** A collection with no schema is `(_id, document: Variant)`, the shape the IR already has for `sourceUrl` and the Variant navigation the typer does for jsonb; a declared schema adds typed top-level columns, nested objects staying Variant; a class mapping types what it maps, a to-one nested object as an embedded property mapping into the same document at a deeper path, a nested array as an explosion (the lateral join `flatten` uses); `graphFetch` over a Mongo-mapped class is a `$project` of the subdocument. The engine's `~binding` schema says the same thing in the engine's words and is read when the class mapping lands.**

**What is decided.** Which way a user's query reaches a collection first.

**Option A, the Relation surface first (recommended).** A `###MongoDB` collection with a JSON schema is a relation:
`#>{db.Collection}#`, then `project`, `filter`, `groupBy`, `join`, `extend`, windows, the wire. The engine's
`MongoDB` class mapping (`~mainCollection`, `~filter`, `~binding`) is then a relation class mapping over that relation,
each property the same-named field.
- Pros: it is the IR path, so PCT, DataCube, Query and the corpus's relation tests use it unchanged; the normaliser
  already turns a legacy class mapping into synthesised functions, so the class mapping is a mapping of the relation,
  not new typing; the engine's drop-in surface follows in the same landing (PR5).
- Cons: a table reference over a `###MongoDB` store is a lite extension (the engine has none), a register row; the
  engine's surface (`getAll`, `filter`, `graphFetch`, `serialize` over the class mapping) comes a landing later.
- Cost: M (PR5, about 2,500 lines, both surfaces). Complexity: the schema-to-columns rule (D6) and the class mapping's
  reading of `~filter` and `~binding`.

**Option B, the engine's class-mapping surface first.**
- Pros: drop-in parity with the engine sooner; the engine's own Mongo test cases judge it.
- Cons: the engine's subset is narrow (root-field filters, graph fetch; no relation operations), and it still needs
  the IR target underneath to execute anything, so PR3 and PR4 are not saved; graph fetch's JSON wire is PR4's
  `jsonWire`; everything PCT measures is deferred.
- Cost: the same total in a worse order.

**Reversal.** None: this is order, not exclusion; both land in PR5.

### D4. How the registries name Mongo: `DatabaseType.MongoDB`, a wider engine enum, or no enum

**Decided (the user, 2026-10-16): `DatabaseType.MongoDB`, a declared lite extension, as SQLite, MariaDB and MySQL are.**

**What is decided.** The key every registry, `Target`, `ExecutionPlan.Target.Database`, `StorelessRuntime`,
`PctBackend.Lane` and the owner guard switch on.

**Option A, `DatabaseType.MongoDB` as a declared lite extension (recommended).** S48's precedent (SQLite, MariaDB,
MySQL): accepted by the model, refused on the drop-in surface (`LITE_DATABASE_TYPES`), declared only by a
`MongoDBConnection`, which `FromProtocol` reads into a `ConnectionDefinition(MongoDB, MongoServers(urls, database),
auth)` with one new `ConnectionSpecification` variant.
- Pros: javac forces an arm in every exhaustive switch over the type (`Databases` ×4, `Sessions` ×5,
  `StorelessRuntime`, `EngineText`, the grammar): every database decision is asked "and Mongo?" and answers by name;
  nothing re-keys; the owner guard already lists the name.
- Cons: the enum's javadoc ("the engine's `DatabaseType` verbatim plus lite's extensions") stretches to a store the
  engine does not call a database type; a `RelationalDatabaseConnection { type: MongoDB }` must be refused with a test;
  one register row.
- Cost: S to M (about 300 lines, mostly switch arms). Complexity: none.

**Option B, a wider enum (`Engine = DatabaseType ∪ {MongoDB}`) threaded through every registry and record.**
- Pros: `DatabaseType` stays the engine's relational enum untouched; "relational type" and "execution engine" are
  distinct notions.
- Cons: `Target.type()`, `ExecutionPlan.Target.Database.Platform`, `Databases`, `Sessions`, `StorelessRuntime`,
  `PctBackend`, `DatabaseOwners` and the plan's JSON all re-key; two enums kept in step; the register row is needed
  anyway.
- Cost: M to L (800 lines or more of re-keying, every plan fixture regenerated). Complexity: breadth.

**Option C, no enum entry: a connection kind read by class (`instanceof MongoConnectionDefinition`).**
- Cons: exhaustiveness is lost, which is the program's whole method; `DialectOwnerExclusivityTest` forbids deciding a
  database by a class test. Rejected.

**Reversal.** A to B is B's cost later; B to A is cheap. Start with A.

### D5. Parameters: the command's `let` variables, or literals in the text

**Decided (the user, 2026-10-16): **the SQL pattern exactly**: placeholders in the text, typed slots in order, the driver binds the values. The marker's spelling is PR0's measurement, between a `let` variable the pipeline references and a marker document the connector replaces in place before sending; the in-place form renders a comparison with a field as a plain `$match` (index use), takes a `$limit`, and needs no 5.0.**

**What is decided.** How a plan's declared parameters reach the server.

**Option A, `let` variables (recommended).** The writer writes `"$$p<n>"` where the SQL writer writes `?`, in the same
bind order; the connector builds `let: {p0: v0, ...}` from the plan's slots; a list binds as one BSON array; no
`TypeHole` (a BSON value carries its type).
- Pros: no value in text (the plan program's P3); one command text for every value; the same bind-order contract as
  `?`; typed values end to end.
- Cons: a variable is visible to `$match` only through `$expr`, and whether the planner uses an index for a
  comparison inside `$expr` against a `let` variable is a PR0 measurement (a cost on stored collections, none on
  PCT's literal relations); stage options (`$limit`, `$skip`) take constants, so a parameter used as a limit is refused
  by name (today's IR carries `limit` and `offset` as plan-time constants, so nothing is lost); `let` on `aggregate`
  needs 5.0 (D9).
- Cost: S (the writer's bind arm; the connector's `let`). Complexity: none.

**Option B, values written into the command text as Extended JSON literals at run time.**
- Pros: no `$expr` for a constant comparison; nothing a stage option cannot take.
- Cons: the plan's text changes with each value (no reuse, no stable plan to review); it is the value-in-text form P3
  rules out, and the execution-plan boundary program decided "a command document's values" for exactly this store.
- Cost: S. Rejected on the rule, not the cost.

**Reversal.** S either way; the connector and one writer arm.

### D6. Typing a collection: its declared JSON schema, a lite column grammar, or inference

**Decided (the user, 2026-10-16): **the document is a Variant**; a declared JSON schema types the top-level columns; a class mapping types what it maps; no sampling, ever.**

**What is decided.** Where a collection's columns and their Pure types come from.

**Option A, the declared `jsonSchema`, and only that (recommended).** Top-level `properties` by `bsonType`: string to
String, int and long to Integer, double to Float, decimal to Decimal, bool to Boolean, date to DateTime, objectId to
String (its hex), object and array to Variant; a property not in `required` is nullable; `number` (either int or
double) and a property with no `bsonType` are refused by name; a collection with no schema is refused by name;
`additionalProperties` adds nothing. DataCube's catalog reads the server's own validator (`listCollections`, the same
JSON schema shape) through `catalogType`.
- Pros: no guessing (invariant 4; Charter C2.2); the engine's own DSL requires the schema, so the model stays the
  engine's; the catalog and the model agree by construction.
- Cons: a collection with no validator cannot be read until someone declares it; `jsonSchema` is verbose; BSON to
  Pure is lossy at the edges (an objectId is text to Pure, a date has milliseconds, both register rows).
- Cost: S to M (about 400 lines: the schema reader, the type table, the rows). Complexity: the edges.

**Option B, a lite column grammar (`Collection X ( id: objectId; name: string; ... )`) beside the schema.**
- Pros: concise; typed exactly, as a `###Relational` table is.
- Cons: a lite-only grammar the engine refuses (the drop-in model breaks); it duplicates the schema's role; parser
  parity and the composer need an arm; a register row.
- Cost: M. A later sugar if users ask; not first.

**Option C, infer columns from documents (`$sample`, `$type`).** Rejected: deciding types from values (C2.2), and
nondeterministic.

**Option D, flatten nested properties into dotted columns instead of Variant.** Later, as a `flatten` native over the
Variant, once a cube needs it: dotted names collide with the quoting rules, and arrays cannot flatten.

**Reversal.** S: B can be added over A without moving A.

### D7. The null model: SQL's semantics emulated, or Mongo's own

**Decided (the user, 2026-10-16): **SQL's semantics.** A declared column exists for every document, so there is never a missing column; an absent field reads as NULL, as Postgres's jsonb reads an absent key; a field of the wrong type is a loud error on read; BSON's total comparisons are emulated as SQL's three-valued ones where an operand may be null, which the IR's nullability stamps say.**

**What is decided.** What a comparison, a boolean and a sort do when a field is `null` or missing. BSON distinguishes
missing from `null`, compares totally (`null` sorts first, then numbers, then strings, by BSON type order), and a
comparison with `null` is `true` or `false`, never unknown.

**Option A, SQL's three-valued semantics, emulated (recommended).** A missing field reads as `null` at the scan
(`$ifNull`); a comparison with a `null` operand answers `null` (`$cond` over `$type`); `AND` and `OR` are three-valued;
`=` never matches `null`; `ORDER BY` follows the IR's `SortKey` nulls placement through a computed key. The
capability matrix records each as emulated.
- Pros: PCT's null tests and every cross-database answer agree; it is the dialect program's D3 ("keep the meanings as
  the platform's written contract; every dialect already emulates them").
- Cons: verbose pipelines and slower comparisons (an emulated comparison cannot use an index); missing and `null`
  are conflated (a register row); every expression arm pays the tax.
- Cost: M (about 500 lines of expression helpers in PR3, then a tax per arm). Complexity: getting every operator's
  null case right; PCT measures it.

**Option B, Mongo's own semantics.**
- Pros: the shortest translation; native index use.
- Cons: wrong answers against Pure's semantics (`filter(x | $x.a < 5)` with `a` missing: SQL yields no row, `$expr`
  yields the row, since missing reads as `null` and `null < 5` in BSON order); every PCT null case pins forever.
- Cost: S now, unbounded pins.

**Option C, A where a value may be null, direct translation where the IR's nullability stamp says it cannot be.** The
IR carries nullability (`OutputCol.nullable`, the join pad truths, the census's zero "label lies" pins), so a comparison
of two non-null operands needs no guard.
- Pros: cheaper pipelines on declared-required fields and literal relations; semantics kept.
- Cons: relies on the stamps, which are guarded but were made for typing, not for this; more arms.
- Cost: M, in the burn-down.

**Recommendation.** A first, C as the optimisation once the lanes are green. Reversal from A to C is additive.

### D8. The lane's server: a pinned `mongod`, Docker, FerretDB, or Atlas

**Decided (the user, 2026-10-16): **a pinned Community `mongod`, Bazel-downloaded, for the lane** (flapdoodle's and `mongodb-memory-server`'s model, done by the build and checksummed); users bring their own server, as for every other database.**

**What is decided.** What the PCT lane runs against, and the licence posture.

**Option A, MongoDB Community `mongod`, pinned and downloaded by Bazel (recommended).** The ClickHouse rule's shape
(`tools/clickhouse/clickhouse.bzl`): version and sha256 per platform, one binary kept, started as the test JVM's child
on a free loopback port (`EmbeddedMongoDb`), Linux x86_64 first. Only the Apache-2.0 Java driver is on a classpath.
- Pros: the real reference, so every measurement is of MongoDB; the same mechanism as the Postgres, MariaDB, MySQL,
  Trino and ClickHouse lanes (no Docker, CI-able, `target_compatible_with` where no build is pinned); this container
  reaches the download (HTTP 200 measured).
- Cons: the server is SSPL-1.0, not Apache-2.0 as ClickHouse's binary is: nothing of it enters the repository or a
  shipped artifact, and running it in a test is not offering it as a service, but the posture is the user's to
  confirm; the tarball is larger than ClickHouse's; macOS and arm64 pins are later work.
- Cost: S (about 300 lines). Complexity: none; the licence question is a decision, not code.

**Option B, the `mongo` Docker image.**
- Pros: the simplest launch; every architecture.
- Cons: the lanes are Docker-free by design (CI runners, the Bazel sandbox, the five existing server lanes); a second
  mechanism to keep.
- Cost: S, and a pattern break.

**Option C, FerretDB (Apache-2.0, the wire protocol over Postgres or SQLite).**
- Pros: licence-clean; could run over the embedded Postgres already in the tree.
- Cons: it is not MongoDB: its aggregation support is a subset, so the lane would pin FerretDB's gaps as Mongo's and
  pass things Mongo refuses. The wrong oracle.

**Option D, MongoDB Atlas, as the three cloud lanes.**
- Pros: nothing to download; the real server.
- Cons: credentials, a running cost, a network round trip per query; the user's rule is "the open ones run locally".
  A second lane later, if a user runs Atlas.

**Reversal.** S: the launcher is one class; the lane does not care which server starts it.

### D9. The version floor

**Decided (the user, 2026-10-16): pin 8.0 for the lane; floor 7.0 unless PR0 finds an 8.0-only need.**

**What is decided.** The oldest server the connector accepts, and the server the lane pins.

**Option A, pin 8.0 LTS for the lane; floor 7.0 unless PR0 finds an 8.0-only need (recommended).** What the plan uses
(the manual, re-checked at the pin): `let` on `aggregate` and `$setWindowFields`, `$dateAdd`/`$dateDiff`/`$dateTrunc`
(5.0); `$documents` at the database level (5.1); `$sortArray` (5.2); PCRE2 (6.1); the bitwise operators (6.3);
`$median` and `$percentile` (7.0). Nothing listed needs 8.0; 8.0 is the current long-term release and the one to
measure on.
- Pros: every operator the plan names is available; one measured server; users on 7.0 are not refused.
- Cons: users on 6.0 and 5.0 servers are refused by name until a per-feature floor exists.
- Cost: S (a version check in the connector, as `PlanRunner.check` does for H2). Complexity: none.

**Option B, per-feature floors through `forServer(version)`** (the hook H2 already uses): a 6.0 server refuses only the
arms it lacks (`$median`, `$percentile`, the bitwise operators).
- Pros: the widest reach.
- Cons: each floor is a measurement on that version, so a second pinned server per floor; more arms.
- Cost: M per supported version. Later, when a user needs it.

**Reversal.** S either way.

### D10. The wire text: written by the pipeline, or formatted by the connector

**Decided (the user, 2026-10-16): **stream by default**: one document per row, the connector writes the punctuation, exactly as the JDBC runner does over a `ResultSet`; the whole-text form for PCT's TDS text and small results; the connector sets `allowDiskUse`.**

**What is decided.** Who turns a result into the CSV, JSON or PCT TDS text a consumer receives.

**Option A, the pipeline writes the whole text, as SQL does (recommended for PCT and whole results).** The IR wraps
(`csvWire`, `jsonWire`, `tdsText`) are compositions over the lowered query, so the target renders them like any select:
each cell to text (`$toString`, `$dateToString`, the Float and Decimal forms emulated where `$toString` differs from
`Scalars.floatRepr`), rows joined in order, the whole as one string in one document's one field, carried by the
connector.
- Pros: Charter C1.2 and C2.5 (Java carries; ruled representations render in the database); byte-identical judging
  with every SQL lane; the census sees one text column as on every lane; nothing new is written.
- Cons: a whole-text result past the 16 MB BSON document limit fails in the server (a named wall; SQL has no such
  cap); the ordered `$push` is a PR0 measurement; the text forms need emulation work; string building in MQL is
  slower than in SQL engines.
- Cost: S to reuse, M for the text forms (in PR3). Complexity: the forms.

**Option B, the connector reads documents and formats the text in Java.**
- Pros: no 16 MB cap (a cursor streams); faster; Java's formatting is easy.
- Cons: it renders ruled representations in Java (C2.5) and reshapes results (C2.3); it is the host formatter the
  eviction program deleted (`formatAsTds`, E1), and `JavaEvalLedgerTest` would need a new carriage site with a
  written justification; a second wire renderer to keep in step with the IR's.
- Cost: M, and a tenet exception. Not recommended.

**Option C, per-row carriage: the pipeline writes each row's text, the connector joins rows with separators.** The
runner already does this for `JSON_PER_ROW` ("the array's brackets and commas are the runner's"); a per-row CSV and
TDS wrap is a small IR composition (about 100 lines) beside `jsonWireRows`.
- Pros: no cap per result (the cap is per row); the ruled representations stay in the database; streaming for the
  product.
- Cons: the whole-text and per-row forms must agree byte for byte (one test per format).
- Cost: S.

**Recommendation.** A for PCT and small results (the simplest judge), C as the product's streaming form; together they
are the two forms the runner already distinguishes. Reversal: none, they coexist.

### D11. Server-side JavaScript: never

**Decided (the user, 2026-10-16): never. The JavaScript in question would be the dialect's own, authored and maintained in Java.**

**What is decided.** Whether the target may ever write `$function`, `$accumulator` or `$where`, or use `mapReduce`.

**Option A, never (recommended).**
- Pros: no program written by the dialect (Charter C6.1), no security surface (hardened servers and Atlas disable
  scripting), no JavaScript judged by nobody; the refusals it causes are few and honest: the digests (MD5, SHA), the
  string distances (Levenshtein, Jaro-Winkler, which BigQuery also refuses), and any walk needing recursion inside an
  expression.
- Cons: those refusals are permanent until Mongo adds operators.
- Cost: none.

**Option B, `$function` behind a feature flag, its JavaScript as data in the dialect, for the few.**
- Pros: three or four more PCT passes.
- Cons: a program in text, disabled on many servers, outside every guard; one flag that makes a lane's verdict depend
  on the server's configuration.
- Cost: S, and a rule broken. Not recommended.

### D12. Where the connector lives: a `core/src/main/mongodb` source set, a module of its own, or in core

**Decided (the user, 2026-10-16): **`core/src/main/mongodb`, bundled with `//core:drivers`**: the same place the driver-specific JDBC code (`core/src/main/duckdb`) sits; found through `ServiceLoader`.**

**What is decided.** Where the one class that imports the driver (`com.mongodb`, `org.bson`) compiles.

**Option A, `core/src/main/mongodb/`, found by `ServiceLoader` (recommended).** The `core/src/main/duckdb` precedent:
`DuckDbAppenderLoad` and `DuckDbCells` compile beside the DuckDB driver as `//core:duckdb_load`, core compiles against
no driver, and `BulkLoad`/`DriverCells` find them at run time. Phase 5's `Connectors.of(type)` names the Mongo
connector the same way (a `DatabaseConnector` service), or directly if that registry lives beside the drivers.
- Pros: core still compiles against no driver (the browser planner, `//core:plan_side`, never sees one); the owner
  guard gains one path beside `core/src/main/duckdb/`; the funnel rule names the packages and nothing in
  `core/src/main/java` can import them.
- Cons: a service lookup rather than a direct call; a missing jar is a run-time refusal by name, not a compile error.
- Cost: S (one library target, one service file). Complexity: none.

**Option B, a module of its own (`//mongodb`) depending on `//core`.**
- Pros: the cleanest separation.
- Cons: core's registry cannot name a class in a module above it, so the service lookup is needed anyway; a module
  needs its own guards, lanes and BUILD plumbing.
- Cost: M. No gain over A.

**Option C, the driver as a compile dependency of core.**
- Cons: breaks "core compiles against no driver"; the wasm and native builds would have to exclude it. Rejected.

**Reversal.** M: moving a class between source sets and re-pointing the service.

### D13. The metamodel store on Mongo: refused by name, or seeded as collections

**Decided (the user, 2026-10-16): refused by name now. Follow-up, recorded: a document-shaped metamodel store (one document per class, its properties nested), which fits Mongo better than row tables.**

**What is decided.** What a query that reads the platform's system tables (`meta::...` metamodel reads, `getAll`'s
`testBasic`) does on a Mongo runtime. On a server engine today, `SystemDatabase.routeFor` seeds every metamodel table
once per graph as session-private temporary tables on a system connection, through `SqlDialect.sessionTable`.

**Option A, refused by name in the first landings (recommended first).** The few PCT tests that read it are pinned
with that reason, as they were on each server lane before its system connection existed.
- Pros: nothing to build before the lanes measure; the refusal is honest and counted.
- Cons: a handful of pins; DataCube's model-reading features do not work on a Mongo runtime until B.
- Cost: S.

**Option B, seeded as collections (the burn-down).** `metamodel_<table>` collections in a session-private database
named by the session and dropped with it; the target renders `sessionTable`'s `CreateTable` and `InsertValues` records
as `create` and `insertMany` commands; the reads go through the same table renames.
- Pros: the feature on Mongo as on every server; the IR's DDL and DML records are the input, so nothing new is
  carried.
- Cons: the seed is the metamodel's size per graph (the ClickHouse session's limits show what that costs on a slow
  server); the session-private database's lifetime must be measured (the ClickHouse HTTP session's hour-long idle
  tail is the cautionary case).
- Cost: M (about 400 lines and a measurement).

**Option C, route metamodel reads to the platform's own DuckDB while the data query runs on Mongo.** Rejected: a
cross-engine query ("one database per query is supported", `Compiler.executesOn`), and a join across them would be
Java's.

**Reversal.** None: A then B is the normal burn-down order.

### D14. The line and its order: when the Mongo work starts, and what it waits for

**Decided (the user, 2026-10-16): **start everything now**, with three constraints that keep it from being a fork: (1) Mongo executes only through `PlanRunner`; the session type gains a sealed variant and the runner one arm; no reader is added to the legacy `ResultSet` executor; (2) PCT channel A on Mongo takes the plan path, which gains a PCT TDS output (the database owner's files, announced); (3) the dialect class in `sql/dialect/` is announced in the ninth line's section and merged between its phases. The seam then reaches Phase 5 with a real second transport in hand.**

**What is decided.** Whether Mongo is a line of its own, what it owns, and how it is sequenced against the dialect
program's Phase 5 and 6 and the eighth line's corpus harness.

**Option A, a twelfth line; start the parts that need no seam now; land the seam with Phase 5 (recommended).** PR0
(the measurement), PR2's server and launcher, and PR3's translator class (unit-tested through `renderStatement` and
run against the embedded server from a dialect test, as `ClickHouseDialectTest` runs its own) need neither Phase 5 nor
the PCT lane; PR1 (the seam) lands with Phase 5, co-signed; the PCT lanes follow; PR6 waits for the corpus harness.
The line owns `sql/dialect/MongoDb*.java`, `core/src/main/mongodb/`, `testing/.../EmbeddedMongoDb.java`,
`tools/mongodb/`, the Mongo suites and lanes, and the Mongo arms of the registries; every edit in `sql/dialect/` is
announced in the ninth line's section and merged between its phases.
- Pros: no idle wait on another line; Phase 5's interface is designed with a real non-JDBC connector in hand, not an
  imagined one; the seam lands once; two sessions never restructure `sql/dialect/` at the same time.
- Cons: the translator's first judge is its own tests until the lane exists; PR1's timing is the ninth line's.
- Cost: coordination. Complexity: the handshake in `IN_FLIGHT`.

**Option B, start everything now with D2's option B (implement `SqlDialect` whole) and a Mongo session type in the
harness only; the program consolidates later.**
- Pros: the earliest PCT numbers.
- Cons: the executor and the harness fork for one database; the consolidation is a landing of its own later, against
  more code; the ninth line's Phase 5 then has two shapes to reconcile.
- Cost: S now, M later, plus the risk this plan exists to avoid.

**Option C, wait for Phase 5, Phase 6 and the corpus harness, then start.**
- Pros: every interface exists first.
- Cons: Phase 5 and 6 are unscheduled; Phase 5 designed without a non-JDBC member in hand is the risk of §9.1; nothing
  of Mongo is measured meanwhile.
- Cost: time.

**Reversal.** None: order only.

---

## 5. The design

### 5.1 The query target: `sql/dialect/MongoDb.java`

One class, with helpers beside it in the same package (`MongoDbStages`, `MongoDbExpressions`, `MongoDbWire`), every
one an owner by its class name. It writes **Extended JSON (canonical mode)** into the one writer: the text of an
`aggregate` command document, with `"$$p<n>"` at each bind. The command is text because the plan is text
(`ExecutionPlan.Sql.statement`), and Extended JSON is lossless for every BSON type; the connector parses it with the
driver's own `BsonDocument.parse`.

**The IR to the pipeline**, shape by shape (the `passes()` first, then the writer):

| IR | pipeline |
|---|---|
| `SqlSource.Table` (a collection) | the command's `aggregate: "<collection>"`; a scan projects every declared column with `$ifNull` for a missing field (D7), `_id` only when declared |
| `Values`, `Dual` | `aggregate: 1` with `$documents: [ {...}, ... ]` (`Dual` is `$documents: [{}]`): PCT's storeless expressions need no collection and no loading |
| `SqlSelect.where` | `$match: { $expr: <expr> }` |
| `projections` | `$project` (computed columns as `$addFields` first where a later projection reads an earlier one, as `Fold` already isolates) |
| `groupBy`, aggregates, `having` | `$group: { _id: {k: ...}, agg: {...} }`, then `$project` to the output names, then `$match` for `having` |
| `orderBy`, `limit`, `offset` | `$sort` (over computed keys for NULLS FIRST/LAST and the IR's collation), `$skip`, `$limit` |
| `distinct` | `$group` on every output |
| `Join` INNER, LEFT | `$lookup` with `let` and a `pipeline` (the ON condition as `$expr`), then `$unwind` (`preserveNullAndEmptyArrays` for LEFT); RIGHT as the mirrored LEFT; FULL as LEFT `$unionWith` the unmatched right (`$lookup` + `$match` on an empty array); CROSS as `$lookup` with no condition; `CROSS_LATERAL` and `LEFT_LATERAL` as `$lookup` whose pipeline reads the left row's `let` variables; `ASOF_LEFT` through the shared `AsOfAsPick` pass |
| `Subselect`, `Cte` | a sub-pipeline inside `$lookup` (over `$documents` or the collection), or inlined where it is the only source; a recursive `SqlWith` is `$graphLookup` only where the shape is a plain reachability walk, else refused by name (the Cypher line's recursion native decides its own rule per dialect) |
| `SqlUnion` | `$unionWith` (ALL), then `$group` for DISTINCT |
| `Pivot` static | the shared `PivotAsGroupBy` pass; a dynamic pivot through `needsStaticPivot()` (the two-phase staticization) |
| `WindowCall`, `Frame` | `$setWindowFields: { partitionBy, sortBy, output: { x: { <fn>, window: { documents: [a, b] | range: [a, b] } } } }`; ranking functions as `$rank`, `$denseRank`, `$documentNumber`; `lag`/`lead` as `$shift`; `ntile`, `percent_rank`, `cume_dist` emulated from counts |
| `Exists`, `InSubquery`, `ScalarSubquery` | `$lookup` with the subquery's pipeline, then `$size`/`$arrayElemAt` in `$expr` |
| `Lambda`, `FoldCall`, lists | native: `$map`, `$filter`, `$reduce`, `$concatArrays`, `$slice`, `$sortArray`, `$zip`, `$range`, `$arrayElemAt`, `$size`, `$setUnion`, ...; structs and maps as embedded documents (`$getField`, `$setField`, `$objectToArray`, `$arrayToObject`, `$mergeObjects`); `CAPS` says lists, structs and maps are native, as DuckDB's does |
| `Cast` | `$convert` with the BSON type the Pure type names (`scalarTypeName`'s switch), `onError` refused (an error is an error) |
| `DateLit`, `TimestampLit`, date parts and formats | `{"$date": ...}` literals, `$dateAdd`, `$dateDiff`, `$dateTrunc`, `$dateToString` with the format the `formatText` switch spells, `$year`.. `$isoWeek`, `$dayOfWeek` |
| `RawSql` (written for Mongo) | the text is a pipeline array written by the user, run as written (D2); the engine's H2 text refused by name |
| `Catalog` | `listCollections` is a command, not a stage: refused as a grid in the first landings (Postgres refuses it too today); DataCube's catalog goes through `catalogType` (D6) |
| the wires (`csvWire`, `jsonWire`, `tdsText`) | the same composition, in `$project`/`$setWindowFields`/`$group`/`$reduce` (D10) |

**The vocabulary**: `call` is one switch over `SqlFn` (206), `reducer` one over `SqlAgg.Fn` (21), each arm one
expression: spelled (`$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$sqrt`, `$pow`, `$exp`, `$ln`, `$log10`, `$mod`,
the trigonometric operators, `$concat`, `$toUpper`, `$toLower`, `$strLenCP`, `$substrCP`, `$indexOfCP`, `$split`,
`$trim`/`$ltrim`/`$rtrim`, `$regexMatch`, `$regexFind`, `$regexFindAll`, `$replaceOne`/`$replaceAll`, `$toInt`,
`$toLong`, `$toDouble`, `$toDecimal`, `$toString`, `$toDate`, `$toBool`, `$type`, `$cond`, `$switch`, `$ifNull`,
`$in`, `$size`, `$rand`, ...), emulated (integer division and `MOD` over the sign rules, `GREATEST`/`LEAST` with null
handling, `REPEAT`, `LPAD`/`RPAD`, `REVERSE` over `$reduce`, `SPLIT_PART`, `STRING_AGG` as `$push` then `$reduce` with
`$concat`, `MODE` as a nested `$group`, `VAR_SAMP`/`VAR_POP` from `$stdDevSamp`/`$stdDevPop` squared, `CORR`/`COVAR`
from sums, `ARG_MAX`/`ARG_MIN` as `$top`/`$bottom`, `QUANTILE` as `$percentile`, `MEDIAN` as `$median`), or refused by
name (`MD5`, `SHA1`, `SHA256`, Levenshtein, Jaro-Winkler and every other walk that needs recursion in an expression;
`HUGEINT` arithmetic past a Long; a date before year 1 in `$dateToString`, measured). Every window function is written
by `windowCall`, every lowering marker refused, exactly as the other tables do (`DialectVocabularyTest`'s
`ONE_EXPRESSION_TABLES`).

**Raising an error** (Pure's `assert`, a `raise` the SQL dialects spell through `raiseEnvelope`): MQL has no `$error`;
the measured candidate is a conversion that fails naming its input (`$toInt` of the message text), whose message the
connector's `RaisedErrors` reads back, as the SQL dialects' envelopes are read. Measured in PR0, refused by name until
then.

**What it reads back**: `wireTypeName` folds BSON type names (`int`, `long`, `double`, `decimal`, `string`, `bool`,
`date`, `objectId`, `object`, `array`, `null`) into the wire vocabulary; `deliversOwn` says a Variant arrives as a
document and a Decimal as Decimal128. There is no `normalize` of JDBC cells: the connector hands the one text through.

### 5.2 The connector: `core/src/main/mongodb/com/legend/exec/MongoDbConnector.java`

Its own source set with the driver on its compile classpath, as `core/src/main/duckdb` holds `DuckDbAppenderLoad` and
`DuckDbCells` beside the DuckDB driver and core compiles against no driver; found through `ServiceLoader` (the
`BulkLoad`/`DriverCells` precedent, `core/BUILD.bazel:329`), or named directly by Phase 5's `Connectors.of(type)` if that
registry lives beside the drivers. It owns:

- `open(ConnectionDefinition)`: a `MongoClient` from the `MongoServers` specification (hosts, the database) and the
  authentication (user and password from the vault reference the model names, as the cloud lanes read theirs: the
  platform holds no credential, C3b);
- `check(session, type)` and `version(session)` from `buildInfo`/`hello` (the version floor, D9);
- `run(RenderedStatement, values)`: `BsonDocument.parse` of the command text, `let` from the slots, `runCommand` on the
  session's database, the one text field of the one result document returned as the plan's text (or the per-row
  documents for `JSON_PER_ROW`), the cursor exhausted;
- the bulk load (`SetupStep.Rows` → `insertMany` of documents the dialect's seed render spells, every cell typed by
  BSON: the loader "spells no SQL and types no value" rule holds because the dialect wrote the documents);
- `raisedMessage` (`RaisedErrors`): the raised text read out of a command error, as MariaDB's is read out of a session
  variable;
- the system database (`SystemDatabase.routeFor` on a server engine): the metamodel tables seeded once per graph as
  collections `metamodel_<table>` in a session-private database (named by the session, dropped with it), read through the
  same table renames; or, for the first landings, metamodel reads refused by name on Mongo (the few PCT tests that read
  it, `getAll`'s `testBasic`, pinned with that reason).

Nothing in `core/src/main/java` imports `com.mongodb` or `org.bson`; `ArchitectureTest.JDBC_AND_DRIVERS` gains both so
the funnel rule sees them, and `DialectBoundaryTest`'s driver census names the driver.

### 5.3 The test backend: `EmbeddedMongoDb`, `tools/mongodb/`, the lane

- `tools/mongodb/mongodb.bzl`: the pinned Community tarball per platform (version, sha256), one binary kept (`mongod`;
  `mongosh` is not needed), a `MONGODB_ROOT` marker, the hub `select` over `//tools/platforms` and
  `EMBEDDED_MONGODB_COMPATIBLE`, the ClickHouse rule's shape line for line (`tools/clickhouse/clickhouse.bzl`). Linux
  x86_64 first; macOS arm64 and Linux arm64 are pins to add when a runner needs them. The module extension in
  `MODULE.bazel` beside `embedded_clickhouse`. The cloud-session distdir script covers the tarball if the proxy refuses
  Bazel's downloader (`docs/CLOUD_SESSIONS.md`).
- `testing/src/main/java/com/legend/testing/EmbeddedMongoDb.java`: started once per JVM on a free loopback port with
  `--dbpath` in the test's temporary directory, `--bind_ip 127.0.0.1`, a user created at start when the lane's
  connection declares one (the engine's `MongoDBConnection` requires an `authentication` block, measured in PR0: the
  grammar may need a lite `# None {}#` arm, a register row if so), stopped at exit, restarted if it dies, its log's tail
  in the failure, as `EmbeddedClickHouse` does.
- The lane: `PctBackend.Lane.MONGODB` (`LEGENDLITE_PCT_BACKEND=mongodb`; its `open()` answers the connector's session,
  which is why the lane enum's return type moves with Phase 5), the storeless runtime for Mongo
  (`StorelessRuntime.onServer` writing a `###MongoDB Database storeless::Store ( )` and a `MongoDBConnection` with the
  server's URL and database `pct`), the five suite classes `Test_LegendLite_MongoDB_<Suite>Functions_PCT` with every
  failure pinned at the first measurement, `PctCensusGate`'s ceilings for the lane, `ChannelBLane`'s pins, and in
  `pct/BUILD.bazel` five `junit_test`s plus a suite and the channel B target, `manual`, with the driver jars as their
  runtime deps and the server as `data`; `//gates:heavy` lists them. The driver's three jars in
  `tools/deps/jars_table.bzl` (`users = ["pct"]` until the server opens Mongo, then core's connector library).
- Phase 6's `MongoDbBackend` (when the interface exists): `openWorkspace()` is a database per package, `isolateFixture()`
  a copy of the fixture's collections (`$out` into the workspace, or a re-seed), `referee()` none (the H2 referee judges
  SQL text; a Mongo lane judges rows only, the `OTHER_LANE` shape the corpus already has for Postgres), `seed` through
  the connector's bulk load.

### 5.4 The model side

- **The store** (`###MongoDB`): a `PMongoDatabase` becomes a store the compiler knows (today it is "carried"): its
  collections are tables with the columns D6 derives, their declared types the BSON types (`storedTypes`, so the
  dialect can read an `objectId` as text and a `date` as DateTime). `include` and `Join` are parsed and dropped by the
  engine itself; lite does the same until a case needs them.
- **The table reference** `#>{db.Collection}#` resolves as it does for a relational store (`TableReferenceChecker`), the
  scan a `SqlSource.Table` whose `name` is the collection, `quoting` bare; a collection or field name with `$` or `.`
  is refused by name.
- **The class mapping** (`*Class: MongoDB { ~mainCollection [db] Coll ~filter ... ~binding ... }`): a relation class
  mapping over the collection's relation, each property the same-named field (nested paths as Variant reads), the
  filter the named one, the binding checked for the class and otherwise unread; `MappingFromProtocol.classMapping` stops
  returning null for it. This is the engine's drop-in surface; parity with the engine's own Mongo test cases in the
  pinned 4.145.0 tree is PR6's.
- **The connection**: `MongoDBConnection` → `ConnectionDefinition(MongoDB, MongoServers, auth)` (D4); the server's
  `ConnectionResolver` opens it through the connector; `EngineText` refuses Mongo by name (legend-engine's Mongo plans
  are not SQL text; their plan nodes are out of scope, `EXECUTION_PLAN_BOUNDARY` §1).
- **Data**: `###Data` rows and `SetupStep.Rows` seed a collection through the bulk load; the `###Relational` CSV seed
  forms do not apply.
- **The register** (`docs/SEMANTICS_REGISTER.md`) gains a row for each deliberate difference: `DatabaseType.MongoDB` as
  a lite extension; a `StrictDate` stored as a BSON date at midnight UTC (BSON has no date-only type); Decimal at
  Decimal128's 34 digits (DuckDB's 38); integers as 64-bit only; a missing field read as null (D7); strings measured in
  code points (`$strLenCP`), which is Pure's rule; the regular-expression dialect (PCRE2 against DuckDB's RE2); the
  Float's text where `$toString` differs from `Scalars.floatRepr` (measured).

---

## 6. The PCT lane on Mongo

- **Channel A** (`PctExecuteNative`): the expression and its injected model execute on the lane's storeless runtime.
  Today the bridge hands a `java.sql.Connection` to `QueryService.execute` and the result comes back through the
  legacy executor (`ExecutionResult.TdsText` under `ExecuteOptions.TDS_TEXT`, named `PCT_RENDER` until 2026-10-17). On Mongo the bridge hands the connector's session,
  and the result comes through the plan path: `TypedQuery.executionPlan` gains a `TDS_TEXT` output (the plan emits
  `Render.tdsText`'s text, the E1 precedent; landed 2026-10-17, §11.2) and `PlanRunner.run` carries it. That is the database owner's step 4 ("the
  callers switch") applied to one caller first (§8.2); it also means the Mongo lane never touches the 24 `ResultSet`
  sites of `Executor`.
- **Channel B** (`ChannelB`): lite's own compiler runs the PCT sources on the lane's database; nothing Mongo-specific
  beyond the session.
- **The census** (`SqlTypeCensus`, `PctCensusGate`): the wire is one text, as it is on every lane under the TDS text option;
  the lane's ceilings are measured on its own target.
- **What to expect** (from the shape of the other lanes, to be measured, never promised): lists, structs, maps and
  Variant are native, so the H2-class refusals (48) do not appear; dates carry milliseconds (microsecond cases pin,
  as on MariaDB); Decimal128 and 64-bit integers pin the big-number cases (`testBigFloatAbs`, `AdjustBy*BigNumber`,
  as they do on ClickHouse); the digests and the string-distance walks refuse (2 on BigQuery, the same here);
  regular-expression dialect differences pin a few (SQLite's 10); the platform's own 34 (instance identity, error
  positions, the 0-based `indexOf`) are the DuckDB lane's. Other lanes first measured between 40 (Snowflake, Trino) and
  83 (BigQuery) own pins and burned down to 4 to 42; a Mongo lane is likely to land in that range and burn down the same
  way. The number is the first run's to tell.
- **The capability matrix** gains a Mongo column on regeneration (`bazel run //docs:update_dialect_capability`), and
  `reach.tsv` the lane's reach: that page is the acceptance artifact for "every function decided, by name".

---

## 7. The landings

Each lands green on its own (`//gates:local`; every existing lane's whole-statement SQL dump unchanged for a pure move;
the corpus rosters unchanged; the leak ratchet only shrinking), announced in `IN_FLIGHT` first, with an independent
review and a `docs/GATES.md` entry, as every line does.

| # | landing | content | judge | size |
|---|---|---|---|---:|
| **PR0** | the measurement | no product code: 20 to 30 PCT cases translated to pipelines by hand and run on `mongod` 8.0 in a container (the tarball or Docker), settling the open measurements of §9 (ordered `$push`, `$toString` of doubles and decimals, `$documents` with `let` at the database level, the 16 MB limit's error, the raise envelope, `$dateToString` before year 1, PCRE2 against the PCT regular expressions, the auth block the grammar needs); the answers recorded in this document's §9 | the measurements, each with its command and answer | about a week; a §9 revision |
| **PR1** | the seam (with the ninth line: Phase 5's landing) | `QueryTarget` above `SqlDialect` (D2); `RenderedStatement.Language` into `ExecutionPlan.Sql`; the sealed session and the connector interface (`check`, `version`, `run`, `bulkLoad`, `raisedMessage`, `openPrivate`); `PlanRunner`'s dispatch on the language; `Connectors.of(type)` replacing `Sessions`' switches; `DatabaseType.MongoDB` with its grammar arm and register row; `MongoDBConnection` read into a `ConnectionDefinition`; the guards' pins with dated reasons (`JDBC_AND_DRIVERS`, `DialectBoundaryTest`, `DatabaseOwners`) | a pure move: all 102 lane targets on the 11 databases pass with the baseline's SQL by digest (the program's §0 proof), `//gates:local` green; Mongo's arms all refuse by name | about 1,000 lines, mostly moved |
| **PR2** | the server and the lane scaffold | `tools/mongodb/mongodb.bzl`, the module extension, `EmbeddedMongoDb`, the driver jars, `PctBackend.Lane.MONGODB`, the storeless runtime for Mongo, the lanes in `pct/BUILD.bazel` and `//gates:heavy`, the five suite classes with every test pinned (the first measurement: the target refuses everything), channel B's pins | the lane runs end to end and every pin is dated | about 800 lines |
| **PR3** | the target, scalar half | `MongoDb` with its passes and `CAPS`, `$documents` and `$project`, the 206-arm function table and the 21-arm aggregate table over literal relations, parameters as `let`, the wires (D10), the raise envelope; the Essential and Grammar suites burned down; the capability matrix regenerated with the Mongo column | the Essential and Grammar lanes, both channels; the matrix diff test; every other lane's SQL unchanged | 4,000 to 6,000 lines (Postgres is 2,343, ClickHouse 2,880, SQLite 3,156; a pipeline is more text per shape) |
| **PR4** | the target, relation half | `$match`, `$group`, `$sort`/`$skip`/`$limit`, `$lookup` joins in every kind, `$unionWith`, `$setWindowFields`, subselects as sub-pipelines, `PivotAsGroupBy` and `AsOfAsPick` applied, the TDS, CSV and JSON wires over relations; the Relation, Standard and Unclassified suites burned down | the three lanes, both channels; the matrix and `reach.tsv` re-measured | about 3,000 lines |
| **PR5** | the store and the mapping | `###MongoDB` compiled as a store (D6), `#>{db.Collection}#`, the `MongoDB` class mapping over it, `MongoDBConnection` with its authentication, the server's resolver, seeding through the bulk load, the system database on Mongo (or its named refusal), DataCube's catalog through `catalogType` | lite's own Mongo cases (a `lite/*.yaml` set as the SQL lines keep), each judged by the same Pure query over the same rows on DuckDB and on Postgres (the cross-database differential, rows compared); zero FAIL, zero BUG, zero ERROR | about 2,500 lines |
| **PR6** | the corpus and the engine's cases | the corpus's 77 covering tests on the Mongo lane through the eighth line's harness (a `MongoDbBackend` of Phase 6), then whatever of the corpus reaches a `###MongoDB` store; the engine's own MongoDB store test cases from the pinned tree as test input (its expected pipelines are not the spec; its expected answers are), each passing or refused by name | the rosters, pinned and shrinking | about 1,500 lines |
| later | the product opens Mongo (the server's driver set, `//core:drivers`), DataCube on a Mongo store, the recursion native's Mongo rule (`$graphLookup`), Elasticsearch as the second non-SQL target over PR1's seam | | |

Total: about 12,000 to 16,000 lines over six landings. PR1 is the only one that touches every database's path, and it
is a pure move proven by the lane dumps; PR2 to PR6 add Mongo's own files and the Mongo arms of exhaustive switches.

---

## 8. The asks to the other lines (to post in `IN_FLIGHT` when the user starts the line)

1. **To the ninth line (the dialect program), for Phase 5 and 6, now.** Design the connector interface with a non-JDBC
   member: `Connectors.of(type)` answers a `DatabaseConnector` whose session is a sealed type (`Jdbc(Connection)`, and a
   second variant the Mongo connector defines), and the connector owns `check`, `version`, `run(RenderedStatement,
   values)`, `bulkLoad`, `raisedMessage`, `openPrivate` and `inMemoryDeclaration`; `RenderedStatement` carries its
   language; `TestBackend.open()` answers that session, not a `Connection`. `Sessions.jdbcProduct(type)` becomes the
   JDBC connector's own, so a type with no JDBC product is not a hole in a shared switch. Whether `SqlDialect` splits a
   `QueryTarget` supertype in Phase 5 (recommended, D2) or Mongo implements `SqlDialect` whole as a first cut is the
   program's call to make with the user; either way `Databases` keeps one exhaustive switch per fact. This program
   owns every file under `sql/dialect/`: Mongo's files there are announced in its section and merged between its phases.
2. **To the fourth line (the database owner), for step 4.** A `TDS_TEXT` output of the plan path (`TypedQuery.Output`,
   `PlanMaker`), so PCT channel A can run through `PlanRunner` on a lane with no JDBC session; and the order in which
   the old executor paths go, so Mongo never gains a `ResultSet` reader of its own. The plan's text format and the
   runner's dispatch on a statement's language are its files (`executionplan/ExecutionPlan`, `exec/PlanRunner`,
   `exec/PlanSessions`).
3. **To the eighth line (the corpus on every database).** PR6 waits for its harness (`MinimalCorpus`'s backend behind
   Phase 6's `TestBackend`): a Mongo backend is one more `openWorkspace()` with no referee, the `OTHER_LANE` shape.
4. **To the compiler line.** Nothing in `builtin/`: legend-engine's `DynaFn` table has no Mongo column, so D10's split
   does not reach it; the Mongo natives' meanings are the IR's, decided in the dialect.
5. **To the Studio and DataCube lines.** Nothing until PR5: then a `MongoDBConnection` in the connection form and a
   collection in the catalog (D6).

---

## 9. Risks, and the measurements that settle them (PR0)

1. **The seam is designed JDBC-only** (the one structural risk): Mongo would fork the executor and the harness, the
   debt this plan exists to avoid. Settled by §8.1, before PR1.
2. **Row order of the wire's `$push`**: the manual documents `$setWindowFields` output as ordered by `sortBy`; `$group`
   after `$sort` is commonly relied on and not guaranteed in so many words. Measured: 10,000 sorted rows pushed both
   ways, compared to the sorted order, repeated.
3. **The 16 MB document limit**: a result text past it fails in the server. Measured: the error's code and text, so the
   wall is named; `JSON_PER_ROW` (one document a row) is the form for large results, as streaming is today.
4. **Text forms**: `$toString` of a double (`1.0`, `1e21`, `0.1+0.2`), of a Decimal128 (scale kept?), of a date
   (`$dateToString` with milliseconds and the `+0000` zone text PCT expects), of a Variant (key order, spacing) against
   `Scalars.floatRepr`, `DateFmt` and the JSON key order the SQL dialects produce. Each difference is an emulation or a
   register row.
5. **Null and type order**: BSON's total order and missing-vs-null (D7) against the PCT null tests; measured on the
   collection suite's null cases.
6. **Decimal128** (34 digits, rounding mode) against the PCT decimal cases that DuckDB passes at 38.
7. **Regular expressions**: PCRE2 against the PCT patterns (`REGEXP_EXTRACT_ALL`, lookarounds, `\d` classes).
8. **The raise envelope** (§5.1): whether a failing `$convert` carries the message text whole in the command error.
9. **`$documents` with `let` at the database level** (`aggregate: 1`): measured, since the PCT's storeless expressions
   depend on it.
10. **The authentication block**: the engine's `MongoDBConnection` grammar has no "none" form; whether the lane creates a
    user at start or lite accepts a declared lite extension (`# None {}#`, a register row) is decided by what the
    grammar accepts (read in the pinned tree) and measured on the embedded server.
11. **The system database on Mongo** (the metamodel store as collections): the cost of seeding per graph on a server
    engine was measured for the SQL servers (the ClickHouse session's limits, §0 of the dialect plan); Mongo's
    session-private database is cheaper to drop but the seed is the same size. Refusal by name is the honest first
    answer if the measurement says so.
12. **Licence posture** (D8): the user's confirmation.
13. **Size**: a pipeline is more text per IR shape than SQL; PR3's estimate is the widest. The capability matrix keeps
    the count honest: every arm is decided, spelled or refused, and the pages show which.

**Measured, 2026-10-17 (the homework before the PCT lane, §11.2; MongoDB 8.0.17 from `@embedded_mongodb`, in
`//core:mongodb_plan_test`), each with where it is read back:**

- *3, the 16 MB limit* (`MongoDbPlanTest.theWholeTextWiresAreBoundedBySixteenMegabytes`): a CSV wire of 100 rows x
  100 KB (10,000,495 characters) passes in about 1.3 s; 200 rows x 100 KB fails in the server with error 16493
  (`Location16493`): `PlanExecutor error during aggregation :: caused by :: RCString too large. Requires
  size=16800896 < limit=16777216`. The wall is named; the runner carries the server's message. A second finding: the
  wire's row join is `$reduce` over `$concat`, quadratic in the text (2,000 rows of 100 B in 139 ms; 8,000 in
  1,829 ms), so a large whole-text result is slow before it is too big. `STREAMED_JSON` (a document a row) is the
  form for both; the whole-text wires are the PCT harness's (`TdsText`, one text) and the product's snapshot forms.
- *4, text forms* (`MongoDbPlanTest.theServersNumberTexts`, the raw operators then the target's spelling): `$toString`
  of a double is the shortest round-trip text (`0.1+0.2` is `0.30000000000000004`, as Pure prints it) but in
  scientific notation from 1e16 up (`1e+16`, `1e+20`; `1e15` is plain) and below 1e-4 (`1.5e-08`), and without a
  fraction when integral (`2.0` is `2`); a Decimal128's text is its own scientific form (`$convert` of 1e20 is
  `1.00000000000000E+20`, 15 significant digits; of 2.0 `2.00000000000000`; 1.5e-8 rounded at 18 `1.5000000000E-8`);
  `$convert` of `"+20"` to long is 20; `$type` of a double literal is `double`. The target's answer: a number's text is
  `$toString`'s bound once, and the platform's portable spelling (`FloatCanonText`: the exponent unfolded as text, a
  fraction digit kept) is written over it -- a double gets Pure's whole Float spelling, a decimal its exponent
  unfolded, a HUGEINT its digits alone -- in the cast to text and in the JSON wire's cells. After it: `2.0`,
  `100000000000000000000.0`, `0.000000015`, `0.30000000000000004`, as Pure and DuckDB spell them. Dates:
  `$dateToString` with `%L` carries the millisecond and the `+0000` text is literal; a Date spells `%Y-%m-%d`.
- *5, null and type order*: a select's `$sort` carries each key's null flag (`__nullN`, D7), so nulls place as the
  key's `NULLS FIRST/LAST` says, and an ordered aggregate's pushed values carry the flag too (`pushedWithKeys`); the
  differential's `extend_if_null`, `group_join_strings` and `sum_all_null` cases are EQUAL to DuckDB. BSON's order
  across types is not reached: every comparison is typed by the lowering.
- *8, the raise envelope*: a `$convert` to `int` of the message between the platform's U+001F sentinels, concatenated
  with an empty text that depends on the row (`UNFOLDED`): measured, the server's optimizer folds a constant
  conversion in a branch not taken and fails the pipeline before any row (`Failed to optimize pipeline`), so no raise
  may be a constant; with the row dependency the message arrives whole in the command error and the connector unwraps
  it into the platform's `DataError`.
- *9, `$documents` with `let` at the database level*: `aggregate: 1` with a `$documents` stage runs the storeless
  expressions (every scalar case of the differential); a `$lookup` under it takes `let` variables and a `from` where
  the inner reads a collection, none where it is literal.
- *10, the authentication block*: the engine's `MongoDBConnection` grammar has no "none" form; the lane makes the
  user (`EmbeddedMongoDb`: a root administrator through the localhost exception, then the user `pct` with `dbOwner`
  on its database) and the connection names it with a `SystemPropertiesSecret` password, so no lite extension of the
  grammar is needed.
- *2, the ordered `$push`*: not measured in bulk yet (the 10,000-row repetition); the differential's ordered
  aggregates are EQUAL on three rows. Open until the PCT lane's ordered cases run.
- *6, 7, 11, 12, 13*: open; 6 (Decimal128) is partly answered by item 4's texts (15 significant digits through
  `$convert` from a double; a Pure Decimal literal keeps its own digits).

---

## 10. Sources

Read in this tree at 59b547a (main, 2026-10-16), each cited above by path:

- `AGENTS.md` (the invariants: 3, 3a, 4, 7, 8; the execution and reference-checkout tenets);
  `docs/TENET_CHARTER.md` (Clauses 1, 2, 6); `docs/WORLD_MAP.md`.
- `docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md` (§0 the handoff and the proof method; §2 the verdict; §4 the
  target; §5 the phases, 0 to 4 done; §6 decisions D1 to D11, P1b); `docs/dialect-capability/README.md`;
  `docs/IN_FLIGHT.md` (lines 3, 1183 to 1300 the eighth line, 1301 to 1482 the ninth line, 1689 the rules).
- `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` (§2 the order, §11 the candidates and the non-SQL stores, §16 channel B,
  §19 ClickHouse, §20 the corpus on every database); `docs/CYPHER_PLAN_2026_10_15.md` and
  `docs/CYPHER_STUDY_2026_10_15.md` §0 (the method: a spec, an oracle, a ledger, refusals by name);
  `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` (P3, §10 E); `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md`;
  `docs/STORE_TYPES_HOMEWORK_2026_10_02.md`; `docs/SEMANTICS_REGISTER.md` (S43, S48, S63); `docs/GATES.md` (the
  lanes); `docs/CLOUD_SESSIONS.md`; `docs/PCT_AUDIT.md` §1.
- Code: `sql/dialect/SqlDialect.java`, `AnsiSqlRenderer.java` (`passes`), `SqlWriter.java`, `RenderedStatement.java`,
  `DialectCapability.java`; `sql/SqlSource.java`, `SqlSelect.java`, `SqlExpr.java`, `SqlFn.java`, `SqlAgg.java`;
  `database/Databases.java`, `Target.java`; `model/ConnectionDefinition.java`; `exec/Sessions.java`, `Executor.java`,
  `PlanRunner.java`, `BulkLoad.java`; `executionplan/ExecutionPlan.java`; `PlanMaker.java`; `Compiler.java`
  (`dialectOf`, `executesOn`); `lowering/WireRender.java`, `Render.java` (`tdsText`), `TdsTextWrap.java`;
  `test/StorelessRuntime.java`; `parser/section/MongoDBSectionGrammar.java`, `MappingProtocolParser.java`,
  `protocol/Protocol.java`, `ConnectionReader.java`, `model/FromProtocol.java`, `MappingFromProtocol.java`;
  `core/src/test/java/com/legend/ArchitectureTest.java` (`JDBC_AND_DRIVERS`, the funnel rules), `DatabaseOwners.java`;
  `pct/BUILD.bazel`, `pct/.../extension/PctBackend.java`, `PctExecuteNative.java`, `channelb/ChannelBLane.java`,
  `Test_LegendLite_ClickHouse_EssentialFunctions_PCT.java`, `PctCensusGate.java`;
  `testing/.../EmbeddedClickHouse.java`, `tools/clickhouse/clickhouse.bzl`, `tools/deps/jars_table.bzl`,
  `core/BUILD.bazel` (`drivers`, `duckdb_load`), `gates/BUILD.bazel`; `scripts/parser/fixtures/t2-mongodb.pure`.
- MongoDB facts (the manual, from memory, each marked "re-check at the pin" and settled by PR0): the version floors of
  §4 D9; `let` on `aggregate`; `$documents`; `$setWindowFields`; Decimal128; the 16 MB document limit; PCRE2; no
  digest or recursion operators in the expression language; `$function` as server-side JavaScript. Nothing from
  MongoDB's documentation or code enters the repository.

---

## 11. v0: the first viable implementation (started 2026-10-16)

**The ask (the user, 2026-10-16): "write this all down and then execute; don't stop until we have a viable v0
implemented that works."** v0 is the smallest thing that is real end to end, built to the decisions above, each piece
in its final place:

1. **The vocabulary**: `DatabaseType.MongoDB` (D4), its grammar arm and register row; `MongoDBConnection` read into a
   `ConnectionDefinition` with a Mongo specification; a `###MongoDB` database read as a store whose collections are
   tables, their columns from the declared schema, else `(_id, document: Variant)` (D3, D6).
2. **The target**, `sql/dialect/MongoDb.java` (D1, D2): an `aggregate` command as Extended JSON for a collection scan
   and a literal relation (`$documents`), `$match`, `$project`, `$group` with the common aggregates, `$sort`, `$skip`,
   `$limit`, `$unionWith`, `$lookup` for inner and left equality joins, the wire wraps, parameters as markers (D5);
   every function and aggregate decided by name, the rest refused with `DialectCapability`.
3. **The connector**, `core/src/main/mongodb` (D12): opens a `MongoClient` from the connection, runs the command with
   its bound values, carries the text; the embedded `mongod` for tests (D8, D9) under `tools/mongodb/` and
   `testing/EmbeddedMongoDb`.
4. **The seam** (D14's constraints): the runner executes through the session, the session type gains the Mongo
   variant, nothing is added to the legacy executor.
5. **The proof**: end-to-end tests on the embedded server (a literal relation, a collection scan with filter,
   projection, group and sort, a join, the wires, a bound parameter), green in the Bazel build with the guards; then
   the PCT lane scaffold and the first measurement as far as the session reaches.

What v0 leaves for the landings of §7: the class mapping and `~binding` (PR5), the corpus (PR6), windows, pivots and
as-of joins (PR4), the full function table's burn-down (PR3), the metamodel store (D13). The record of what landed, with
its judges, is kept here as each piece lands.

### 11.1 What landed, 2026-10-17 (branch `claude/upbeat-johnson-l4hogx`)

v0 is real end to end: a model with a `###MongoDB` store and a `MongoDBConnection` is planned by the one compiler into
an `aggregate` command and run by the plan runner through the MongoDB connector on a real MongoDB 8.0.17, which the
test JVM starts from the pinned server Bazel unpacked. Every piece is in its final place (§5), none in the legacy
executor.

**The vocabulary (D3, D4, D6).** `DatabaseType.MongoDB` (`model/ConnectionDefinition`), carried by the engine's own
`MongoDBConnection` form and never writable in a `RelationalDatabaseConnection` (the grammar's lite types are untouched).
`model/MongoDeclarations` reads the connection (its one server as a static datasource; a user name with the vault
reference its password names, `systemproperties:<name>`; a replica-set list, Kerberos-only, API-key, EPK, PSK and GCP
authentications and an AWS secret refused by name) and the store (each collection a table; its columns the declared
schema's top-level properties by `bsonType` -- string, long, int, bool, double, a nested object or array as a Variant,
an objectId as text; `_id` the key where declared; `required` is NOT NULL; a decimal refused by name; nothing inferred
from documents). `StorelessRuntime.onMongo` writes the storeless runtime in that form; `Databases`, `Sessions`,
`EngineText` and `PlanJson` take the type (every switch exhaustive; the refusals by name).

**The target (D1, D2, D5, D7, D10, D11).** `sql/dialect/MongoDb` (the dialect class, its whole vocabulary decided:
the function table, the aggregate table, the scalar names, the date formats, the DDL and DML as commands),
`MongoDbPipeline` (the stages: scan with `$ifNull` per declared column, `$documents` for literal relations and the
database-level `aggregate: 1`, `$match` with `$expr`, `$group` with its accumulators read back through markers and the
one-row-over-no-input union, sort carries with their null flags, `$replaceWith` projections, `$skip`/`$limit`,
`$unionWith`, `$lookup` with `let` variables for inner, left and cross joins) and `MongoJson` (the text helpers and
D7's null guard: a `$let` binding each operand once, null when a nullable operand is null). Parameters are
`{"$legendParam": n}` markers in binding order. A raise is a `$convert` to `int` of the message between the platform's
U+001F sentinels, concatenated with an empty text that depends on the row: **measured on 8.0.17, the server's
optimizer folds a constant conversion in a branch not taken and fails the whole pipeline before any row** ("Failed to
optimize pipeline"), so no raise may be a constant. The JSON wire is written by the pipeline as text (D10): the
`CAST(json_object(...) AS VARCHAR)` the wire builds becomes a `$concat` of each key's text and each value's text by its
kind at run time (null, a string escaped and quoted, a date in its format, a number or a boolean by `$toString`; a
nested document raised by name), since MongoDB has no JSON writer for a value. The capability matrix's MongoDb row
(regenerated): 171 functions and aggregates decided through MongoDB's operators (the matrix reads each as an emulation,
every arm composing JSON through a helper), 33 refused by name, 15 lowering markers; no PCT lane reaches it yet.

**The seam (D14's constraints).** `exec/Statements` is what a session does with a plan's text (`check`, `execute`,
`query` with the slots bound and each row's one text carried); `exec/JdbcStatements` is the runner's JDBC moved whole;
`Sessions.Session.statements()` answers JDBC's by default; `exec/DatabaseConnector` is the seam a database with no
JDBC joins through, found by `ServiceLoader` as the bulk loaders are (`exec/Connectors`); `PlanRunner` runs through the
session's statements and holds the plan and the punctuation alone; `PlanSessions.openShared` asks the connectors first
for a declared connection (a database its connector opens is the user's: a plan with setup for it is refused).
`Executor` and `Execution` are untouched: the legacy path refuses MongoDB by name (`Sessions.openingFor`).

**The connector (D12).** `core/src/main/mongodb/com/legend/exec/MongoDbConnector` (a `MongoClient` from the
connection, the password read from the system property the vault reference names) and `MongoDbStatements` (the markers
replaced in place by the driver's own encoding of each bound value -- Int64, Double, Decimal128, String, Boolean, a
date at UTC midnight, a date-time to the millisecond, finer refused; the `aggregate` command through the driver's
cursor; each row's one field a string or null, anything else refused; a server failure as the platform's `DataError`
with a raise's text unwrapped). `//core:mongodb_connector` compiles against the driver (5.13.0: sync, core, bson and
bson-record-codec, `tools/deps/jars_table.bzl`) and rides with `//core:drivers`; core compiles against none of it.

**The server (D8, D9).** `tools/mongodb/` pins MongoDB 8.0.17's Ubuntu 22.04 x86_64 Community tarball by sha256
(`@embedded_mongodb`, the server binary and its licence kept; SSPL test input in Bazel's cache, never redistributed);
`testing/EmbeddedMongoDb` starts it on a free loopback port with access control on and makes the user `pct` on the
database `pct` through the localhost exception (a root administrator first, then the user with its leave).

**The proof.** `//core:mongodb_plan_test` (manual, in `//gates:heavy`; Linux x86_64): six cases on the real server,
all green -- a filtered, projected, sorted select streamed as JSON rows; an absent field read as null; a parameter
bound in place; a group counting and sorting; the whole result as the JSON wire and as the CSV wire; a storeless
expression on the database alone. `MongoDbDialectTest` (in `//core:core_tests`) pins the rendered command's shape,
the marker, the group stage and a refusal by name. Every guard is green with its pins moved by dated reason:
`DialectBoundaryTest` (Databases' construction and arm, Sessions' two refusals, the storeless runtime's one, the
mapping parser's wire discriminator now counted), the leak ledger (Sessions, StorelessRuntime, the end-to-end test),
`JdbcSurfaceCensusTest` and `JavaEvalLedgerTest` (the seam's four classes and the connector's two), `TenetRatchetTest`
and `ArchitectureTest` (the driver's packages funnelled as a driver's), `CoreClosureTest` (the four jars),
`DatabaseOwners` (the connector's source set an owner), `CodeShapeGuardrailTest` (the pipeline's stage counter).
`//core:core_tests` (27 lanes), `//core:guardrails`, `//core:census`, `//docs:update_dialect_capability_test`,
`//tools/deps:core_closure_test` and `core_layering_test` pass; `//pct:pct_tests_lib` builds. The differences from
SQL that v0 kept were `docs/SEMANTICS_REGISTER.md` S72 to S75; the homework's differential (§11.2) showed S72 to S74
to be defects of the target and they are fixed and gone, and S75 to be the SQL side's (a platform item, kept).

**What v0 leaves, in order.** (1) **The PCT lane**: the harness's channel A hands a `java.sql.Connection` to
`QueryService.execute` (`PctExecuteNative`), the legacy route; the Mongo lane needs §6's plan route (`TDS_TEXT` as a
plan output, the bridge through `PlanRunner`), which is the next landing -- so no `PctBackend.Lane.MONGODB` is
scaffolded that could not run. (2) Joins, unions and the ordered and distinct aggregates are written and rendered but
not yet measured on the server; windows (`$setWindowFields`), pivots, as-of and lateral joins, subqueries in
expression position, CTEs, raw text as a source, catalog grids, `DISTINCT` with `ORDER BY`, and a star with exclusions
are refused by name. (3) The server's resolver (`ConnectionResolver`) and the system database refuse MongoDB by name:
the product opens no Mongo session yet. (4) The class mapping and `~binding`, the corpus, the metamodel store (D13).

### 11.2 The homework before the PCT lane, 2026-10-17

**The ask (the user, 2026-10-17): "should we do homework before we go down this path and get stuck?" -- "do it; I
don't think upstream very useful except for spec/syntax."** Five measurements before the lane is built, each with its
command and its answer; the sixth (what upstream's MongoDB support does) dropped by the user's word. Every answer
is a fact the lane's design rests on, not a promise.

1. **The shape census of channel A on DuckDB** (`-Dlegend.diagnostics=pct-shapes`; `PctCaseRecorder.recordShape`
   writes `pct-shapes.tsv` into each suite's outputs; run: `bazel test //pct:pct_duckdb_essential
   //pct:pct_duckdb_grammar //pct:pct_duckdb_relation //pct:pct_duckdb_standard //pct:pct_duckdb_unclassified
   --jvmopt=-Dlegend.diagnostics=pct-shapes`, 372 s; read with a 40-line script). 2,655 cases executed: 1,935
   `Scalar` (73%), 524 `TdsText` (20%: the whole Relation suite but 14, and 64 of Standard), 162 `Collection`
   (6%), 34 errors before a result (17 `DataError`, 8 `ModelException`, 3 each `ResolutionException` and
   `TypeInferenceException`, 2 `NumberFormatException`, 1 `NotImplementedException`). The scalar values by Java
   class: 581 `Boolean`, 292 `String`, 275 `Double`, 237 `Long`, 154 `Integer`, 108 `StrictDate`, 104
   `DateWithSubsecond`, 54 `DateWithSecond`, 43 null, 43 `BigDecimal`, 16 `LinkedHashMap` (class instances and
   `Map`s), 8 `BigInteger`, 7 `Year`, 6 `YearMonth`, 3 `ArrayList` (a `List` value), 2 each `DateWithHour` and
   `DateWithMinute`. The collection values: 39 `String`, 38 `Integer`, 30 `Long`, 35 empty (typed `Nil`,
   `Integer`, `String`, `Pair`...), 10 mixed `Long|String` (an `Any[*]`), 9 `LinkedHashMap` (class instances,
   `Pair`s), 1 `Double|Long` (a `Number[*]`). What this says for a lane whose results cross as one text: the
   text route must carry every primitive with its precision (six date precisions, `BigDecimal`, `BigInteger`),
   null, class instances, `List`, `Map`, `Pair`, enumeration values (4 `DayOfWeek`), a `Type` (4), and a
   collection with a declared element type when empty -- the `TDSResult` form carries all of it today for 20% of
   the cases, and the scalar and collection forms are the `value` relation of the same text (§6's one representation).
2. **The TDS text as a plan output, and the shadow run.** (a) *The rename* (the user, 2026-10-17: "why would a plan
   have a reference to a test harness?"): the product code is named for what it is -- `lowering/TdsTextWrap` (was
   `PctTdsWrap`), `Render.tdsText` (was `pctTds`), `exec/ColumnProbe` (was `PctProbe`), `ExecuteOptions.TDS_TEXT` (was
   `PCT_RENDER`); the SQL aliases inside the text (`_pct0`, `_pct_line`, the `pctTds` column) are unchanged, so every
   lane's SQL is byte-identical. (b) *The plan output*: `TypedQuery.Output.TDS_TEXT` and `ExecutionPlan.Format.TDS_TEXT`
   -- Pure's TDS text as the plan's text, a relation's own columns, a value's the one-column relation `value` (its
   column optional only when the value is), a pivot refused by name (its columns exist only once the database is
   asked: the legacy executor's probe); `executionPlan(runtime, output, features)` carries the engine's feature flags
   to the lowering (`CORRECT_SQL_SUBSTRING_INDEXING`, the PCT's). `PlanMakerTest` judges the form against today's path
   on DuckDB and H2 for every case it has (a let-bound literal types its column more precisely than a declared
   parameter -- `Decimal(38,2)` against `Decimal`, `StrictDate` against `Date` -- so those are judged by the plan's own
   header). (c) *The shadow run* (`-Dlegend.diagnostics=pct-shadow-plan`; `PctShadowPlan` beside `PctExecuteNative`:
   each case planned to `TDS_TEXT` under the same flags, run by `PlanRunner` on the lane's storeless runtime, compared
   with the legacy route's result; `pct-shadow-plan.tsv` in each suite's outputs; the run:
   `bazel test //pct:pct_duckdb_{essential,grammar,relation,standard,unclassified}
   --jvmopt=-Dlegend.diagnostics=pct-shadow-plan`, about 7 minutes, the suites' own pins untouched). 2,621 cases
   shadowed (the 34 that error before a result are not). **Agree: 2,121 (81%)** -- 273 relation texts equal whole,
   1,848 values equal row for row. **Relation texts, 251**: 230 differ in row order alone (no `ORDER BY` in the query;
   the database's order for the two paths' different SQL shapes), 1 by design (`generateGuid`), 20 refused (the
   pivots). **Values, 248**: 170 dates -- a Date value through the legacy route prints with the precision the value
   has (`2015-04-15T13:12:11+0000`; `.338001` after a microsecond adjust), the TDS text with three millisecond digits
   (`T13:12:11.000+0000`; `.338`) as the engine's TDS writer spells a `TIMESTAMP` column, since a SQL slot carries an
   instant and not a precision (the first run also had 114 strftime errors over date values carried as text, fixed:
   `Render.tdsTextCell` prints a text-carried date as its text); 35 empty collections (the text is the header and a
   newline, the legacy no values: the same to the reader); 27 strings in an `Any[*]` quoted by the TDS cell (`'a'`,
   which the reader unquotes); 16 class instances and maps (the TDS text has no form for them; the legacy route hands a
   map, the Pure side an instance). **What this settles for the lane**: one text per case carries 81% as is, the row
   order and the quoting are the reader's, and the two gaps are named -- a Date value's PRECISION (Pure's value
   semantics, which the legacy route infers from the instant's zero fraction and the TDS text cannot carry over a
   `TIMESTAMP` slot: either the lowering spells a date value as Pure's own literal text, with its precision, or the
   lane pins the date-precision cases as upstream's relational adapters do) and class instances (16 + 9 cases: the
   graph route). Both are the user's decision before the Mongo lane's channel A is wired; the measurement is recorded,
   the route is built.
3. **The differential in miniature** (`MongoDbDifferentialTest`, in `//core:mongodb_plan_test`; the verdicts in
   `mongodb-differential.tsv` under the test's outputs and on its standard output): 55 storeless cases shaped like the
   suites', planned to the CSV wire (two to the JSON wire) on DuckDB and on MongoDB, run on both, the texts compared.
   The first run (40 cases): 23 EQUAL, 1 DIFFERENT, 14 REFUSED, 2 the reference's. What it exposed, and what each
   became: (a) a `HUGEINT` cast refused -- now a Decimal128 (`scalarTypeName`); (b) `UNNEST` of a collection root
   refused -- now the list projected whole and exploded after (`$unwind`; several in step through `$zip`); (c) a
   subquery in expression position refused -- now a `$lookup` under `let` variables into a `__sqN` field, read as
   the first document's one field (`ScalarSubquery`), its size (`Exists`) or its values (`InSubquery`), the field
   named as the pipeline's projection stage named it (a bare `COUNT(*)` has one); (d) `DISTINCT` with `ORDER BY`
   refused -- allowed, the sort carries being functions of the row; (e) an ordered aggregate placing nulls first
   (S73) -- the pushed values carry each key's null flag; (f) `SUM` over an all-null group answering 0 (S72) -- a
   count rides beside the sum; (g) an integral double printing as `2` and a big or small one in scientific notation
   (S74 and the §9 item 4 measurement) -- a number's text is spelled through the platform's portable form over
   `$toString`'s text. The last run (63 cases, the eight TDS text cases of item 2 included): 59 EQUAL, 2 DIFFERENT, 2 REFUSED (a pivot
   and a window function, named later landings). The two differences are the DuckDB side's, recorded for the platform and not Mongo's to fix:
   `(0.1 + 0.2)->toString()` prints `0.3` on DuckDB's plan path (the NUMERIC CHARTER's bare float literal is typed
   `DECIMAL` by DuckDB, so the sum is exact decimal arithmetic) where Pure and MongoDB print `0.30000000000000004`;
   and the JSON wire's date-time text is each database's JSON writer's (`2020-01-15 11:20:30` on DuckDB) where Pure
   and the MongoDb target spell `2020-01-15T11:20:30.000+0000` (S75, recast). A third finding of the same kind, found
   and removed from the cases: a list of float literals (`[1.5, -2.0]`) is typed `DECIMAL(3,2)` by DuckDB and prints
   `1.50`; a mixed `Number[*]` list unifies to `DOUBLE` and prints an Integer element as `10.0`. Each is the
   reference's deviation from Pure's one answer, to be fixed on the SQL side (a literal list's element type, a
   Float literal's own type) -- a measurement for the DuckDB lane's plan path, outside this line's files.
4. **The 16 MB bound** (`MongoDbPlanTest.theWholeTextWiresAreBoundedBySixteenMegabytes`): §9 item 3's record --
   16,777,216 bytes is the wall, the server names it (error 16493), 10 MB passes in about 1.3 s, and the row join
   is quadratic (`$reduce` over `$concat`). The lane's whole-text results are small (a PCT case's TDS is rows);
   the product streams.
5. **Channel B's bodies** (the PCT `.pure` sources under `meta::pure::functions::*::tests`, read for the statement
   count of each `<<PCT.test>>` function body): 920 of 921 are one statement (`|expr` or a single `let`-free
   expression); the one with more, `meta::pure::test::surveyor::runPCTTests` (12 statements), is the surveyor's
   driver, not a case. So channel B on Mongo needs no `Sequence` plan: every case is one statement, one command.

What the homework changes in the plan: §6's channel A route is confirmed as one text per case in Pure's own TDS form
(item 1: the forms are three, the text is one), built as `Output.TDS_TEXT` and measured by the shadow run (item 2:
81% carried as is, two named gaps for the user to rule on -- a Date value's precision, class instances); the
differential stays in `//core:mongodb_plan_test` as the line's standing measurement, grown with each landing (item 3,
now with TDS text cases); the lane's size estimate is unchanged. The measurements that remain open are §9's items 2,
6, 7, 11, 12 and 13.

### 11.3 The PCT lane, 2026-10-17: the first measurement

**The ask (the user, 2026-10-17): "Why did you stop? Did we pass all the PCT tests already?"** The lane is built and
run; the first measurement is pinned, as every other lane's was.

**The lane.** `LEGENDLITE_PCT_BACKEND=mongodb` (`PctBackend.Lane.MONGODB`: the storeless runtime on the embedded
MongoDB 8.0.17, `StorelessRuntime.onMongo`, the password a system property the connection's vault reference names).
Channel A runs through the plan path (`PctPlanRoute`, `PctBackend.throughThePlan`): the case's expression and its
injected model planned by the one compiler to Pure's TDS text (`TypedQuery.Output.TDS_TEXT`, under the PCT's feature
flag) and run by `PlanRunner` on the session the connector opens; the lane has no JDBC session and touches nothing of
the legacy executor. ONE representation crosses: a relation as its TDS text, a value as the one-column relation
`value`, which the adapter (`pct_adapter.pure`) unwraps by the function's own return type -- a Date or a Decimal read
from the text's lines by Pure's own parsers, since legend-pure's TDS reader builds neither from a cell; the value's
strings quoted whole, since the reader types an unquoted cell by its text. The adapter's repository declares the
engine's relation functions (`core_functions_relation`) for the one `map` it uses. The five suites are
`Test_LegendLite_MongoDB_*Functions_PCT` (`//pct:pct_mongodb_*`, `//pct:pct_mongodb` in `//gates:heavy`).

**The first measurement** (two runs the same day; the second after the fixes the first drove):

| suite | tests | pass | pinned | the pins, by cause |
|---|---:|---:|---:|---|
| Essential | 345 | 241 | 104 | 24 value differences (dates printed with three millisecond digits where the value has none, 8; the mixed-Number list unified, the big Float, the error positions and messages -- the platform's own); 15 the reader's cell typing; 11 a List, Map or class instance crossing as text; 8 maps from entries; 8 the empty string (the reader's null); 6 two-parameter lambdas; 4 dates past year 9999 (`$dateToString`); 3 `%f` directives with a precision; the rest named refusals |
| Grammar | 137 | 113 | 24 | 4 List or instance values as text; 3 `Any`-typed values; 3 the reader's cell typing; 6 value and error differences (the platform's own); the rest named |
| Relation | 469 | 143 | 326 | 188 window functions (`$setWindowFields`); 67 quantified comparisons (ANY/ALL over a subquery); 27 a Variant's JSON text; 20 pivots (the plan path has no probe); 17 lateral and as-of joins; 7 others (full and right joins, QUALIFY, currentUserId, a lambda, a value) |
| Standard | 205 | 124 | 81 | 32 window functions; 11 `timeBucket`; 10 value differences; 6 the reader's cell typing; 6 mode; 2 each regex replace, hashCode, generateGuid, weekday names; bit shifts, CORR |
| Unclassified | 93 | 57 | 36 | 13 ascii and char; 5 base64; 4 string distances; 3 digests; 5 value differences; 2 regex replace; 2 List values; currentUserId |
| **all** | **1,249** | **678 (54%)** | **571** | |

The DuckDB lane pins 24, 4, 1, 4 and 0 in the same suites: the platform's own. The other lanes' first measurements
were 40 (Snowflake, Trino) to 83 (BigQuery) own pins over the whole platform; this lane's 572 is of another kind,
since the target is three days old and its relation half (§7 PR4: windows, quantified subqueries, pivots, lateral and
as-of joins, the Variant text) is the named refusals that make up 320 of the 326 relation pins. (The counts are the
lane's as committed: 571 pins after the number text's spelling let `testFormatFloat` pass.) The differential in
miniature stands at 68 EQUAL of 72 (the two differences the DuckDB side's; two refusals, pivot and window).

**What the first run fixed in the target** (each a measured defect, now a differential case): a literal array handed
to a one-argument operator was read as an argument list (`$reverseArray`, `$size`); `$concatArrays` answered null for
a fold's empty accumulator; a string converted to Boolean was true whatever it said; `$strLenCP` of null was the
server's error; the list slice, the `%s` format and the array and map casts were refused; and a number's text,
spelled through the portable composition, rendered to 17 KB per cast to text -- a print path that casts a value a
few hundred times (Pure's print of an `Any`-typed Float through the CSV wire, 2 MB; a `%f` of a mixed list's
element, 147 MB) ran the driver out of memory, so the target now spells the text once in the pipeline's own
operators (the MariaDb precedent: one `$regexFind` for the sign, digits and exponent, the point moved as text; about
a kilobyte per cast; `1.5->toString()` 18 KB, the mixed-list element 79 KB, the `%f` case 6 MB -- the lowering's own
multiplication, measured and left). **What the harness's own
reader showed** (legend-pure's `stringToTDS`, the form every lane's relation results cross in): it types an unquoted
cell by its text, trims an unquoted cell's spaces, reads an empty cell as null, builds no Date or Decimal through
`map`, and takes no type variable or simple class name in its header -- each now handled in the TDS text or the
adapter, except the empty string (8 pins: quoted or not, the reader reads nothing).

**Next, in the order the pins say:** `$setWindowFields` (220 pins over two suites), the quantified subquery as a
`$lookup` (67), the Variant's JSON text (27), a static pivot's grouped form (20), lateral and as-of joins (17); then
the essential suite's two-parameter lambda, map from entries and `timeBucket`. The date-precision and
class-instance pins stay the user's decision (§11.2).

### 11.4 The burn-down, 2026-10-17: the relation half shaped, the long tail measured

**The ask (the user, 2026-10-17): "then we want to burn down pct to as close to zero as possible."** In the order
the first measurement's pins said, each shape landed with its differential cases (the differential in miniature now
stands at 147 EQUAL of 154 cases), then the five suites re-measured and re-pinned.

**What landed in the target.**

1. **Windows** (`MongoDbPipeline.windows`, `MongoDb.windowOutput`): a `$setWindowFields` stage per distinct (partition,
   order). MongoDB's ranking operators take a `sortBy` of exactly one element, so an ordered group whose calls rank or
   range to the current row sorts by ONE computed field holding the keys' document, each key beside its null flag (the
   SQL engine's null placement, in the first key's direction; keys sorted both ways are refused by name); the other
   groups sort by a field and a flag per key. A RANGE window bounded by unbounded or the current row -- an unframed
   ordered aggregate, a cumulative distribution -- ranges over the order's dense rank, computed in a first stage,
   since `$setWindowFields`' `"current"` is the current document alone where SQL's RANGE reads the row's peers, and
   its range windows take a numeric field. A RANGE window by offsets sorts by the one key itself, ascending (a
   descending order's bounds negated and swapped), a null key as the infinity of the key's kind so an offset from a
   null row reaches its null peers alone and an unbounded side reaches the partition's edge (DuckDB's RANGE over
   nulls); two fixed-length units count in the finer. Ranking: `$documentNumber`, `$rank`, `$denseRank`; a percent
   rank, a cumulative distribution and an ntile from a rank or a document number and the partition's count; `$shift`
   for a lag or lead by the literal the Lowerer passes; `$first`, `$last`, a pushed array for an nth value. The push
   route (ordered, distinct, median, quantile, mode, the two-column statistics) pushes the frame's values as the group
   stage does. 202 of the 220 pins passed on the first run; the rest named what was left (above).
2. **Quantified comparisons** (`Subqueries.apply`): `value op ANY|ALL (subquery)` rewritten to the list predicate
   over the lookup's rows -- `exists` for ANY, `forAll` for ALL -- whose lambda compares the value with each row; the
   Lowerer drops the rows' nulls and guards the value, so the two-valued predicates are SQL's. All 67 pass.
3. **A Variant's JSON text** (`MongoDb.cellText`, `jsonLevel`): MongoDB has no JSON writer, so the text is written by
   the value's kind at run time -- null, a string escaped and quoted, a date, a number as Pure spells it, a boolean --
   and an array or a document element by element, keys escaped and quoted, unrolled to six levels of nesting (eleven
   BSON levels each; the server's limit is two hundred, and a Pair collection's text overflowed at eight levels of
   fifteen); deeper is refused by a raise. A Variant literal (a TDS cell) is the document it spells, under `$literal`
   so its fields are not expressions; a computed text parsed as a Variant is refused by name (no JSON parser in a
   pipeline). A Variant element read as text is the element's own string (DuckDb's `->>`), the JSON path root `$` the
   value itself. A lambda parameter's field reads as the variable's field (a Pair's `first`).
4. **Lateral and as-of joins** (`MongoDbPipeline.join`): the lookup's `let` already bound the left row's aliases, so a
   lateral join's correlated right side is a cross or left join's shape; an as-of join keeps, of the right rows the
   ON admits, the one nearest the left row's key (`$sort` by the match inequality's right-side column, `$limit: 1`),
   then pads as a left join. Full and right joins stay refused by name.
5. **The statistics**: mode (the sorted values' runs counted, the smallest of the most frequent -- Pure's fold), the
   two-column statistics pushed as pairs (E[xy] - E[x]E[y]; the sample covariance scaled; the correlation over the
   population deviations), a descending percentile over the value's descending order (the Lowerer's flag; it read
   ascending before), the list forms' subselect columns qualified by their alias in the Lowerer (`_uz.a`, a target that
   keeps a subselect's row under its alias reads them there).
6. **The long tail**: two-parameter lambdas (the list zipped with its 1-based positions, both bound; a null list
   staying null); a map from entries (`$arrayToObject`); the time zone conversion (UTC the instant itself, a zone the
   `$dateToParts`/`$dateFromParts` shift, the direction by the value's kind) and the zoned cast (`$convert` to date, a
   single-digit month or day padded, which Pure reads and `$convert` does not); `format`'s `%d` and `%.Nf` (the Float
   spelling of the rounded double, its fraction cut or padded, a negative argument's sign kept); `timeBucket` by
   arithmetic (the engine's origins, 1970-01-01 and 1969-12-29 for weeks; months counted from 1970-01; a literal finer
   than a millisecond bucketed at the millisecond, the one place a BSON date may cut such digits, since no bucket is
   finer); `ascii` and `char` by the ASCII table spelled as text (a character or code beyond it raised); bit shifts by
   Decimal128 arithmetic (exact to 34 digits where a double loses the bits past 2^53); the composed date formats (a
   weekday's or a month's name by its number, the 12-hour clock and its marker, the minimal fraction) beside
   `$dateToString`'s codes.

**What landed in the harness** (`pct_adapter.pure`; `PctDisciplineTest`'s pin 500 -> 535, dated): the reader types a
cell by its text, not by the header -- a Number's `3` read as 3.0, a String's `'0'` as an Integer, a null Number cell
as the String `null` -- so the value relation's Number cells are read as Pure spells them (an Integer or a Float) and
its String cells as the quoted text itself, a cell spanning lines read whole (`quotedCells`).

**The second measurement:**

| suite | tests | pass | pinned | the pins, by cause |
|---|---:|---:|---:|---|
| Essential | 345 | 280 | 65 | 18 a List, Map or class instance crossing as text (the route's); 14 value differences (dates printed with three millisecond digits where the value has none, the error positions and messages -- the platform's own); 10 `Any`-typed values and user elements by simple name; 10 error positions and messages; 7 dates past year 9999, a month count past `$dateAdd`'s range, the microsecond unit (MongoDB's own); 3 `Pair` without type arguments; 2 the reader's cell typing; a `TypedDeactivate` |
| Grammar | 137 | 116 | 21 | 5 instances as text; 4 Decimal arithmetic (Decimal128's 34 digits where Pure keeps 16); 3 `Any`-typed values; 2 extended primitives; 2 non-primitive equality; the rest named (a schema-qualified table, a date part, a range) |
| Relation | 469 | 441 | 28 | 20 pivots (the plan path has no probe); 5 a Variant holding JSON null (BSON null, printed as a null cell where Pure prints `'null'`); full and right joins; currentUserId (the second pass below: 449 and 20) |
| Standard | 205 | 184 | 21 | 10 value differences (greatest and least over dates and mixed numbers); 4 regex replace; 2 each hashCode, generateGuid; ARG_MAX and ARG_MIN; a percentile's server error |
| Unclassified | 93 | 74 | 19 | 5 base64; 4 string distances; 4 regex extract and index (values); 3 digests; 2 regex replace; currentUserId |
| **all** | **1,249** | **1,095 (88%)** | **154** | 82 named refusals or the server's own errors; 417 pins fewer than the first measurement's 571 |

**The second pass, the same day** (the differential at 157 EQUAL of 165 cases): right and full joins (a right join looks the left
side up from the right; a full join appends, by `$unionWith`, the right rows no left row matched, the left side's
null columns under `$literal` since `$addFields` merges a plain document into the field's own); `regexpExtract`'s
group 0 (the whole match); `range(n)` from 0; `maxBy` and `minBy` on the pair route (the value at the greatest or
least key); a list element past the end as null (`$arrayElemAt` answers nothing, which `$eq` does not read as null);
a power as a Float (`$pow` of two integers answers an integer); and the **static pivot**: a pivot whose values are
given has its columns at type time (`PivotChecker`: one per value and aggregate, named bare as the database names
them, which the headers present quoted and a `cast(@Relation<...>)` names quoted -- `Lowerer.relationCast` finds the
column under either spelling), so the plan's TDS text carries them without asking the database (`PlanMaker` refuses
a dynamic pivot alone now), and the target shapes it as two groups and a projection (`MongoDbPipeline.pivot`: by the
keys and the ON value with each aggregate accumulated, then by the keys with the ON value and the aggregates
pushed, then each pivot column read as its value's aggregate). The dialect resolves an unqualified column under a
select's one alias while the row is wrapped (`MongoDb.soleAlias`), as SQL resolves a column over one source.
After it the lane stands at 1,121 of 1,249 (89%; 128 pinned: Essential 65, Grammar 20, Relation 20, Standard 10, Unclassified 13); the relation suite's pins are the dynamic pivots (14), the Variant holding JSON null (5) and currentUserId.

**The third pass, the same day** (the differential at 165 EQUAL of 173): the regular-expression replace, which MongoDB has no operator for (`$replaceAll` replaces text), as the matches found and the text spliced -- the stretch before each match kept, the replacement put in its place, a `$N` in it the match's Nth capture, the tail kept; the first match alone without the 'g' option (the three-argument form); `generateGuid` as thirty-two random hexadecimal digits (`$rand`, one per step of a reduce) in a uuid's five groups; `hashCode` as the hashed index key (`$toHashedIndexKey`: the database's own hash, as every lane answers its own -- DuckDB's is not Java's either). The adapter reads a Number cell spelled with the Decimal suffix (`2D`) as a Decimal.

**The cross-lane pass, the same day** (the user: "did you look at how other database do all of these failures to see
if there is inspiration for us here"; the differential at 182 EQUAL of 190): the 128 pins read against the eleven
other lanes' pin files. 80 pass on every other lane, 13 are pinned on one to four others, one on seven, 34 on all of
them (the platform's own). Of the 80, what the other targets COMPOSE without a native operator composes here too:
Postgres and SQLite walk the Levenshtein table in a recursive CTE and Postgres, Databricks, Snowflake and Trino the
Jaro-Winkler match, so `MongoDb.levenshtein` and `jaroWinkler` are the same walks as folds (`$reduce` over `$range`,
the row above and the match flags carried by `$let`; the score in doubles in DuckDB's order of operations, 0.88
bit-equal to DuckDB's); SQLite composes base64 from hex, and MongoDB 8.0 converts text to binary data and back, so
base64 is `$convert` (to `binData` reading UTF-8, to `string` writing base64; the reverse padded to a multiple of four
first, since the Pure function takes unpadded text) -- nine pins. The 17 date pins were the plan path's, not
MongoDB's: the legacy route prints a date with the precision the value has, the TDS writer three millisecond digits
(the DuckDB shadow had the same 170 differences, §11.2), so a value root's DateTime cell prints a whole second without
a fraction now (`Render.valueDateTimeText`: the value equal to its second truncation; shared code, every lane's plan
path), and the day truncation (`datePart`, the day comparisons) is typed DATE in the MIR (`SqlTyping`), as every
dialect renders it (DuckDb `CAST(x AS DATE)`, MariaDb `DATE()`, ClickHouse `toDate32`), so `datePart` prints
date-only. What has no shape anywhere stays: the digests (composed on no lane; SQLite walls them, Postgres pins
SHA1) and `currentUserId` (a native on every SQL lane; a pipeline names no user). The rest of the 80 is the route's
(a List, Map, Pair or class instance as text; `Any`-typed values; the Decimal suffix; the dynamic pivots) and
MongoDB's own (the microsecond unit; `testDatePart`'s seven-digit literal). After it the lane stands at 1,145 of
1,249 (92%; 104 pinned: Essential 58, Grammar 20, Relation 20, Standard 2, Unclassified 4).

**MongoDB's own, measured and named** (the pins that are the target's): a Variant holding JSON null is BSON null, so
its cell prints as a null cell where Pure prints `'null'` (five relation tests); dates past year 9999 and the
microsecond unit (`$dateToString`, `$dateAdd`); a dynamic pivot (the plan path has no probe; a static one has its grouped form); the digests and currentUserId (no
operator; regex replace, hashCode, generateGuid, base64 and the string distances were composed, above); a List, Map or class instance crossing as text (the route's, shared with the DuckDB shadow). The
class-instance pins stay the user's decision (§11.2); the date-precision pins were the plan path's and are gone.

**The text carrier, 2026-10-18 (the thirteenth line's step 3, `docs/PLAN_PATH_PROGRAM_2026_10_17.md` §4; GATES "step 3,
the text carrier"):** the route's own refusal -- "a List, Map or class instance crossing as text", the `Any` values,
the `Pair`s, the user elements -- is gone from this lane: a value that is no primitive crosses as Pure's own literal
grammar, which the pipeline writes (`$concat` over the document's fields, the same owner as DuckDB's; the
differential's one case per form EQUAL) and the adapter compiles and reactivates. 29 pins deleted, 9 moved to what
stays named (the identity tests, as on the DuckDB lane; a user element by its simple name; an instance nested past
the layout's depth). The lane: 1,249 tests, 75 pinned.
