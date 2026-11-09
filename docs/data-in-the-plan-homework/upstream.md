# Upstream homework: how data reaches a database before a query runs

Read on 2026-11-09 from the pinned sources only (nothing recalled). Each section separates **Read** (what the
sources say, with path:line and short quotes) from **Inferred** (my reading of what it means). Section 7 holds the
implications for lite's "data in the plan" design (PLAN_AS_DATA §2.6, §11, holes 7 and D3).

## Path shorthands

- `E` = `/root/.cache/bazel/_bazel_root/e8817d619d2dbe75028d8b0114f08e2c/external/+http_archive+legend_engine_src`
- `P` = `/root/.cache/bazel/_bazel_root/e8817d619d2dbe75028d8b0114f08e2c/external/+http_archive+legend_pure_src`
- `[RS-PURE]` = `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational`
- `[RS-PROTO]` = `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-protocol/src/main/java/org/finos/legend/engine/protocol/pure/v1/model`
- `[RS-EXEC]` = `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-execution/legend-engine-xt-relationalStore-executionPlan/src/main/java/org/finos/legend/engine/plan/execution/stores/relational`
- `[RS-CONN]` = `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-execution/legend-engine-xt-relationalStore-executionPlan-connection/src/main/java/org/finos/legend/engine/plan/execution/stores/relational/connection`
- `[DUCK]` = `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb`
- `[CORE-PURE]` = `E/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core`
- `[PROTO]` = `E/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-protocol-pure/src/main/java/org/finos/legend/engine/protocol/pure/v1`
- `[COMPILER]` = `E/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph`
- `[TESTABLE]` = `E/legend-engine-core/legend-engine-core-testable`
- `[SVC-TEST]` = `E/legend-engine-xts-service/legend-engine-test-runner-service/src/main/java/org/finos/legend/engine`
- `[URL]` = `E/legend-engine-core/legend-engine-core-shared/legend-engine-shared-core/src/main/java/org/finos/legend/engine/shared/core/url`
- `[PCT]` = `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-PCT/legend-engine-pure-functions-relationalStore-PCT-pure/src/main/resources/core_external_test_connection`
- `[P-REL]` = `P/legend-pure-store/legend-pure-store-relational`

---

## 0. The answer in one table (Read; details below)

| Mechanism | Where the data is | In the serialized plan? | When it is loaded | Identity / key |
|---|---|---|---|---|
| Relational test connection (`LocalH2DatasourceSpecification` / `DuckDBDatasourceSpecification`: `testDataSetupCsv`, `testDataSetupSqls`) | CSV text, turned into SQL **at plan generation** | **Yes, inline**: the CSV *and* the generated SQL, copied into **every** SQL node's `connection` | On **every** connection acquisition, into a **private** in-memory database | The whole setup-SQL list (`equals`), CRC32 in the pool name |
| `###Data` (`RelationalCSVData`, `RelationElementsData`) in service/mapping/function tests | Resolved to content, converted to a `testDataSetupCsv` on a fresh test connection | **Yes, inline** (same as above) | Same as above | Same as above |
| `DataElementReference` | Resolved to the referenced element's content before the connection is built | Content, not the reference | n/a | n/a |
| M2M `JsonModelConnection` test data (`ExternalFormatData` on a connection) | `data:application/json;base64,...` URL on the connection | **Yes, inline** (base64) | At exec, URL opened by the `data:` handler | n/a |
| M2M `ModelStoreData` (store test data for a ModelStore) | Thread-local stream, URL `executor:default` | **No**: the plan names a slot; exec reads a thread-local | At exec | Plan shared across tests ("plans do not embed the data content") |
| ServiceStore / MongoDB store test data | A local mock server started per test | **No**: the plan carries the server's dynamic address | Server started before the run | n/a |
| PCT relational (`TDS` literals) | Turned into CSV, then into DDL + inserts | **Yes, as SQL nodes** in a `SequenceExecutionNode` before the query, in one `RelationalBlockExecutionNode` | As plan steps; temp tables, dropped on connection close | n/a |
| legend-pure / engine Pure tests (`executeInDb`, `loadCsvToDbTable`) | Side effects on a shared named in-memory H2 database `testDB` | **No**: the plan names nothing but "the test DB"; the address is executor configuration | In `<<test.BeforePackage>>` setUp functions, before the test | Fixed name `testDB` + port system property |
| Legacy service tests (`ServiceTest_Legacy`) | Setup SQL stripped out of the test runtime, run into a freshly started H2 server's `testDB` | **No** (stripped before planning) | Once, before the asserts | n/a |
| Engine temp-table load from file (graph fetch) | A temp CSV the executor writes from rows it already holds | **No path**: the plan carries the placeholder `${csv_file_location}` | At exec, bound by the executor | n/a |
| Test-data generator (`meta::relational::testDataGeneration`) | Output CSV text (`schema\ntable\ncols\nrows\n-----\n`) or `RelationalCSVData` | n/a: an authoring tool; its output is pasted into `###Data` / `testDataSetupCsv` | n/a | n/a |

---

## 1. Engine execution plans and connection data

### 1.1 Every SQL node carries its whole connection (Read)

- `[RS-PROTO]/executionPlan/nodes/SQLExecutionNode.java:26-36`: `public DatabaseConnection connection;` beside
  `sqlQuery`, `onConnectionCloseCommitQuery`, `onConnectionCloseRollbackQuery`, `resultColumns`.
  `RelationalExecutionNode.java:26-33` is the same. The plan root (`[PROTO]/model/executionPlan/SingleExecutionPlan.java:29-36`)
  has `authDependent, kerberos, serializer, templateFunctions, rootExecutionNode, globalImplementationSupport`: **no
  plan-level data or connection table**. Data, if any, rides inside each node's `connection`.
- The executor never mutates the shared plan: `[RS-EXEC]/plugin/RelationalExecutionNodeExecutor.java:207-215` --
  "`SingleExecutionPlan` instances are handed out by `ExecutionPlanCache` (Guava-backed, keyed by
  `PureExecutionCacheKey` which contains neither identity nor allocation payload)" -- so it shallow-copies a node with
  a preprocessed connection (`SQLExecutionNode.shallowCopyWithConnection`, `SQLExecutionNode.java:73-100`;
  `RelationalExecutionNodeExecutor.java:232-281`).

### 1.2 The test data fields (Read)

- Protocol: `[RS-PROTO]/packageableElement/store/relational/connection/specification/LocalH2DatasourceSpecification.java:21-36`:
  `public String testDataSetupCsv; public List<String> testDataSetupSqls; ... boolean disableDatabaseToUpper;`
- Pure metamodel: `[RS-PURE]/runtime/connection/datasourceSpecification.pure:34-39`, all three fields
  `<<equality.Key>>` -- the data is part of the connection's equality.
- DuckDB: `[DUCK]/legend-engine-xt-relationalStore-duckdb-protocol/.../DuckDBDatasourceSpecification.java:19-25`:
  `public String path; public String testDataSetupCsv; public List<String> testDataSetupSqls;`
- Auth: `TestDatabaseAuthenticationStrategy` is a marker subclass of `DefaultH2AuthenticationStrategy`
  (`[RS-PROTO]/.../authentication/TestDatabaseAuthenticationStrategy.java`); runtime key
  `[RS-CONN]/authentication/strategy/keys/TestDatabaseAuthenticationStrategyKey.java`: `shortId()` = `"type:TestDB"`,
  equal to every other instance. No secret, no user.
- Legacy Pure `TestDatabaseConnection` serializes to the same thing:
  `[RS-PURE]/protocols/pure/vX_X_X/transfers/connection_relational.pure:41-58` -- a `RelationalDatabaseConnection` with
  `_type = 'h2Local'`, `testDataSetupCsv = $test.testDataSetupCsv, testDataSetupSqls = $test.testDataSetupSqls`, auth
  `_type = 'test'`.

### 1.3 CSV becomes SQL at plan generation, never at exec (Read)

- `[CORE-PURE]/pure/router/metamodel/clustering.pure:65-66` (also :47-48, :51-52): "This is required to populate the
  setup sql for test connections where csv data has been supplied. It is done here to ensure that the plan
  generation is always the only place where sql is generated for this case."
- The hook: `[RS-PURE]/contract/storeContract.pure:119-157` (`processRuntimeTestConnections`). For a
  `TestDatabaseConnection` (:124-129): if `testDataSetupSqls` is empty, `testDataSetupSqls =
  testDataSetupCsv->setUpDataSQLs(<the databases of this connection>)`. For a `LocalH2DatasourceSpecification`
  (:136-143): `testDataSetupSqls = $l.testDataSetupSqls->concatenate(<csv>->setUpDataSQLs(...))`. Other
  specifications go through the dialect's `processTestConnectionDatasourceSpecification` (:146-150). The CSV field is
  **kept** (`^$l(...)` copies it).
- DuckDB's handler: `[DUCK]/legend-engine-xt-relationalStore-duckdb-pure/.../duckdbExtension.pure:52-63` -- same, with
  `setUpDataSQLsV2` (quoted-CSV aware).
- Java's execution side never reads `testDataSetupCsv`: the only non-test Java uses are grammar, composer, compiler
  and the protocol class (grep of `testDataSetupCsv` in main sources).

### 1.4 The CSV format and the SQL it produces (Read)

`[RS-PURE]/sqlQueryToString/DDL/toDDL.pure`:
- `loadCsvDataToDbTable` (:94-127): a fold over rows; a row with **one value that is empty or starts with `-`**
  ends a block; a block is line 0 schema, line 1 table, line 2 column names, the rest data rows. Schema and table
  must exist in the runtime's `Database` ("There should be exactly 1 schema with name ..." :108, "Table ... not
  found" :110, "Column ... not found" :113): the data is checked against the store at planning.
- `setUpDataSQLs` (:186-195): `schemaAndTableSetup($dbConfig, [])` for **every schema and every table of the
  database** (:155-184 with an empty pair list: `Drop schema if exists ... cascade; Create Schema if not exists ...;`
  then `Drop table if exists ...; Create Table ...` per table), then the inserts.
- `setUpDataSQLsV2` (:198-207, DuckDB and PCT): only the schemas/tables named in the CSV, and `parseCSV` respects quotes.
- Inserts: `[RS-PURE]/sqlQueryToString/extensionDefaults.pure:644-656` (`loadValuesToDbTableDefault`): one
  `insert into ... values (...), (...);` per table; values typed by column (`helperFunctions/databaseHelperFunctions.pure:310-322`:
  `''` and `---null---` become `null`, strings quoted with `''` escaping).
- Producer side: `[RS-EXEC]/test/HelperRelationalCSVBuilder.java:30-44` writes `schema\ntable\n<values>` per table,
  joined with `"----\n"`. A fixture plan shows the text verbatim:
  `"testDataSetupCsv": "default\naddressTable\nname\nA1\nA2\nA3\nA4\nA5\n---"` (see 1.6).

### 1.5 Exec: a private database per connection, setup re-run on every acquisition (Read)

- `[RS-EXEC]/connection/manager/strategic/DataSourceSpecificationTransformer.java:56-78`: an `h2Local` with non-empty
  `testDataSetupSqls` becomes `new LocalH2DataSourceSpecification(testDataSetupSqls, new H2Manager(), new
  TestDatabaseAuthenticationStrategy(), properties)`; with empty SQL it becomes a `StaticDataSourceSpecification` on
  the executor's test DB (see 3.2). `EmbeddedH2DatasourceSpecification` throws "Embedded H2 currently not supported" (:54).
- `[RS-CONN]/ds/specifications/LocalH2DataSourceSpecification.java:53-95`: `getConnection(...)` takes a pooled
  connection, then **every time** runs `for (String sql : _key.getTestDataSetupSqls()) statement.executeUpdate(sql);`
  (:81-87). (It also installs a Northwind stored procedure, ~5 s, only if a setup SQL calls `loadNorthwindData()`, :64-78.)
- The database is private to the connection: `[RS-EXEC]/H2LocalServer.java:29-30` -- "The important part is to have
  an empty name in 'this.buildDataSource("127.0.0.1", localH2Port, "", null)' / It ensure the database creation is
  'private' to the connection." URL: `[RS-CONN]/driver/vendors/h2/H2Manager.java:76` --
  `"jdbc:h2:tcp://" + host + ":" + port + "/mem:" + databaseName + ...` with `databaseName` `""`
  (`LocalH2DataSourceSpecificationKey.java:28`, `LOCAL_H2_DB_NAME = ""`).
- DuckDB is the same pattern: `[DUCK]/legend-engine-xt-relationalStore-duckdb-execution/.../ds/specifications/DuckDBDataSourceSpecification.java:52-70`
  runs `testDataSetupSqls` on every `getConnection`; URL `"jdbc:duckdb:" + path` (`DuckDBManager.java:37-38`); test
  connections use `path = ""` (in-memory).

**Inferred.** Upstream never keeps a filled test database across acquisitions: each acquisition gets a database that
is private to the physical connection and is (re)built by drop/create/insert. A pooled connection that is reused is
re-seeded (hence the `Drop ... if exists` prefix). A multi-node plan (e.g. graph fetch with four SQL nodes) re-seeds
once per node acquisition unless a block node retains the connection.

### 1.6 The data is inside the serialized plan JSON (Read, measured on upstream fixtures)

- `E/legend-engine-xts-service/legend-engine-language-pure-dsl-service-execution/src/test/resources/plans/org/finos/service/SimpleRelationalService.json`
  (2,414 bytes, a service plan loaded as a classpath resource): `rootExecutionNode/executionNodes[0]/connection` =
  `{"_type":"RelationalDatabaseConnection","authenticationStrategy":{"_type":"test"},"datasourceSpecification":{"_type":"h2Local","testDataSetupSqls":["drop schema if exists MAIN_SCHEMA cascade;","create schema MAIN_SCHEMA;","create table MAIN_SCHEMA.PERSON_TABLE(...)","insert into MAIN_SCHEMA.PERSON_TABLE(...) values(1, 'f1', 'l1');"]},"element":"store::MyDatabase",...}`.
- `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-execution/legend-engine-xt-relationalStore-executionPlan/src/test/resources/org/finos/legend/engine/plan/execution/stores/relational/test/full/graphFetch/crossStore/withByteCode/plan1.json`:
  4 SQL nodes, each with its **own copy** of the connection carrying **both** `testDataSetupCsv` and the 9 generated
  `testDataSetupSqls` (`Drop schema if exists default cascade;` ... `insert into addressTable (name) values ('A1');` ...).
- Measured breakdown (my script over the JSON): `withByteCode/plan1.json` total 385,440 chars, of which
  `globalImplementationSupport` (Java source/bytecode) 365,300 and `rootExecutionNode` 19,000; `withSourceCode/plan1.json`
  197,620 / 177,480 / 19,000. Test-data SQL across the 4 copies: 1,997 chars.
- Upstream fixture sizes of authored test data (my scan of `.pure` files under `E`): 43 `Relational #{...}#`
  `###Data` blocks, median 180 chars, max 405; 18 `testDataSetupCsv` literals, max 557 chars.
- No plan-size limit exists in the pipeline: no hit for "plan size", "too large", `StreamReadConstraints` in
  main sources. `executePlan` accepts zlib bodies (`EXEC-CORE .../legend-engine-executionPlan-execution-http-api/.../ExecutePlanStrategic.java:55-60`:
  `@Consumes({MediaType.APPLICATION_JSON, APPLICATION_ZLIB})`).

**Inferred.** Upstream tolerates heavy redundancy for test data (CSV + SQL, once per SQL node, and on the H2 path DDL
for every table of the database even without data), because test data is small and Java code dominates plan size.
Nothing upstream is designed for megabytes of data inside a plan.

### 1.7 Content keying of the test database (Read)

- `[RS-CONN]/ds/specifications/keys/LocalH2DataSourceSpecificationKey.java:35-78`: the key holds the full
  `testDataSetupSqls` list; `equals` compares the **whole list** and the port (:69-71); `shortId()` is
  `"LocalH2_port:" + port + "_sqlCS:" + setupCheckSum` where the checksum is CRC32 of the SQL joined by `;` (:40-42, :53-55).
- `[RS-EXEC]/connection/manager/strategic/DataSourceSpecificationKeyGenerator.java:54-61`: non-empty setup SQL ->
  `LocalH2DataSourceSpecificationKey(testDataSetupSqls)`; empty -> `StaticDataSourceSpecificationKey(LOCAL_HOST,
  testDbPort, TEST_DB)` with `TEST_DB = "testDB"` (:33).
- Pools are named by the key: `[RS-CONN]/ds/state/ConnectionStateManager.java:268-272` --
  `DBPOOL + key.shortId() + "_" + identity.getName() + "_" + credentials`; an LRU of pool states evicted after 10
  minutes unused (:52-56, :116 `DEFAULT_EVICTION_DURATION_IN_SECONDS = Duration.ofMinutes(10)`).
- DuckDB: `[DUCK]/.../keys/DuckDBDataSourceSpecificationKey.java:36-42, 66-69, 84-85` -- key is `(path,
  testDataSetupSqls)`, CRC32 `_sqlCS:` in the short id.

**Inferred.** The data IS the identity: the pool, and so the reuse of physical connections, is keyed by the setup
statements' content. There is no separate hash-addressed store of data and no shared filled database: equal content
shares a pool, not a database.

---

## 2. `###Data` and the testable framework

### 2.1 Protocol (Read)

- `[PROTO]/model/packageableElement/data/DataElement.java:27-33`: `stereotypes, taggedValues, EmbeddedData data,
  List<DataResolver> dataResolvers`.
- `[PROTO]/model/data/EmbeddedData.java:20-23`: abstract, `@JsonTypeInfo(... property = "_type")`. Subclasses in main
  sources: `ExternalFormatData` (`contentType`, `data`, both required: `ExternalFormatData.java:19-25`),
  `ModelStoreData` (`List<ModelTestData> modelData`: `ModelStoreData.java:34-37`; `ModelTestData.model` is the class
  path, with `ModelEmbeddedTestData.data: EmbeddedData` or `ModelInstanceTestData.instances: ValueSpecification`),
  `DataElementReference` (`PackageableElementPointer dataElement`: `DataElementReference.java:21-23`),
  `RelationElementsData` (`relationElements` of `paths, columns, rows`), `ServiceStoreEmbeddedData`, and relational
  `RelationalCSVData` (`List<RelationalCSVTable> tables`) with `RelationalCSVTable(schema, table, values)`
  (`[RS-PROTO]/packageableElement/store/relational/data/RelationalCSVData.java:21-24`, `RelationalCSVTable.java:19-24`).
- Newer: `DataResolver` (`BaseDataResolver`, `ReferenceDataResolver`) merged by priority in
  `[COMPILER]/DataResolverHelper.java` ("DataElement reference < Direct PackageableElement ... < base data").
- DataSpace: `DataSpaceExecutionContext.testData: DataElementReference`
  (`E/legend-engine-xts-data-space/legend-engine-xt-data-space-protocol/.../DataSpaceExecutionContext.java:28`).

### 2.2 How a test attaches data to a connection (Read)

- Extension point: `[PROTO]/extension/ConnectionFactoryExtension.java:31-95` -- "Used to build connections using
  test data"; `tryBuildTestConnection(Connection sourceConnection, List<EmbeddedData> data[, hints])`,
  `tryBuildTestConnectionsForStore(Map<String, DataElement>, Store, EmbeddedData)`, `tryBuildConnectionForStoreData(...)`;
  each returns `Pair<Connection, List<Closeable>>` (a new connection plus resources to close).
  Registered: `RelationalConnectionFactory`, `ModelStoreTestConnectionFactory`, `ServiceStoreTestConnectionFactory`,
  `MongoDBStoreTestConnectionFactory` (the `META-INF/services` files).
- Dispatcher: `[TESTABLE]/legend-engine-testable/src/main/java/org/finos/legend/engine/testable/connection/TestConnectionBuilder.java:58-75`
  -- the first factory that answers wins, else "Unsupported test data type ...".
- References are resolved to content first: `[SVC-TEST]/testable/service/extension/TestRuntimeBuilder.java:68-73, 103-106`:
  `if (connectionTestData.data instanceof DataElementReference) { EmbeddedData embeddedData =
  EmbeddedDataCompilerHelper.getEmbeddedDataFromDataElement(...) }`; then `connectionPointer.accept(new
  TestConnectionBuilder(data, ...))` (:80). The mapping and function test runners do the same
  (`MappingTestRunner.java:342-343`, `FunctionTestRunner.java:279-280, 326-327`).
- Relational factory: `[RS-EXEC]/test/RelationalConnectionFactory.java:100-125, 187-220`: all data must be
  `RelationalCSVData` / `RelationElementsData`; it builds a brand-new `RelationalDatabaseConnection` (`type = H2`,
  `TestDatabaseAuthenticationStrategy`, `LocalH2DatasourceSpecification` with `testDataSetupCsv = new
  HelperRelationalCSVBuilder(data).build()`, `disableDatabaseToUpper = true`), or, when the hints say relation
  (`hints.isRelation()`, :189), a DuckDB one with `path = ""` and the same CSV. `RelationElementsData` is first turned
  into CSV (:167-180).

**Inferred.** For relational stores, every testable path ends where 1.3 starts: the data becomes a connection's
`testDataSetupCsv`, the runtime is rewritten to use that connection, and the plan generated against it carries the
data inline (1.6).

### 2.3 How each runner plans and reuses (Read)

- Service tests (`[SVC-TEST]/testable/service/extension/ServiceTestRunner.java:126-139, 363-437`): a per-suite session;
  `PureSingleExecution`: "one plan, shared by every atomic test in the suite"; the runtime is rewritten by
  `TestRuntimeBuilder.getTestRuntimeAndClosableResources` (:384), then the plan is generated once and kept
  (`this.plan = singleExecutionPlan`, :436).
- Mapping tests (`[TESTABLE]/legend-engine-test-runner-mapping/src/main/java/org/finos/legend/engine/testable/mapping/extension/MappingTestRunner.java:100-117`):
  one plan per suite when every test yields equivalent connections, "(a common case - e.g., all tests reference the
  same DataElement, or all bind ModelStore data, whose plans do not embed the data content)"; otherwise plans are
  "cached, keyed on connection identity"; "some (notably ModelStore's) pass the test's data to the execution through
  thread-local state bound when the connection is built". `computeSharedPlan` (:550-615, method at :563): shared only if rebuilding
  every test's connections yields the same instances; "freshly-built service-store / MongoDB connections, which carry
  a dynamically allocated local server address, do not" (:561).
- Function tests (`[TESTABLE]/legend-engine-test-runner-function/.../FunctionTestRunner.java:186, 335-347`): the
  compiled runtime's `ConnectionStore._connection(...)` is **swapped in place** for the mocked test connection, the
  plan is generated per test (:210), and the real connection is restored when the session closes (:186).
- Legacy service tests (`[SVC-TEST]/test/runner/service/ServiceTestRunner.java:225-232, 441-498`):
  `extractSetUpSQLFromTestRuntime` collects the setup SQL of every `LocalH2` / DuckDB spec and **nulls**
  `testDataSetupCsv`/`testDataSetupSqls` before the plan is generated; the SQL is run by
  `[RS-EXEC]/TestExecutionScope.java:67-95` (`setupTestServer`: starts an H2 server, runs the SQL through
  `getTestDatabaseConnection()`, i.e. the named `testDB`) before the asserts execute the plan.

---

## 3. legend-pure: `executeInDb`, `loadCsvToDbTable`, PCT, the generator

### 3.1 The natives (Read)

- `[P-REL]/legend-pure-m2-store-relational-pure/src/main/resources/platform_store_relational/functions.pure:26-32`:
  `native function ...::executeInDb(sql:String[1], databaseConnection:DatabaseConnection[1], timeOutInSeconds:Integer[1], fetchSize:Integer[1]):ResultSet[1];`
  `native function ...::loadCsvToDbTable(filePath:String[1], table:Table[1], databaseConnection:DatabaseConnection[1], numberOfRows:Integer[0..1]):Nil[0];`
  `native function ...::loadValuesToDbTable(tableData:List<List<Any>>[*], table:Table[1], databaseConnection:DatabaseConnection[1]):Nil[0];`
- `loadCsvToDbTable` (compiled: `[P-REL]/legend-pure-runtime-java-extension-compiled-store-relational/src/main/resources/org/finos/legend/pure/runtime/java/extension/store/relational/compiled/RelationalGen.java:315-322`):
  Java reads the file with `CsvReader.readCsv(codeStorage, null, filePath, 500, rowLimit, ...)`, drops the header,
  converts by column type, and `bulkInsertInDb` batch-inserts through a prepared statement (:352-410, `insertBatch` :392). Only for a
  `TestDatabaseConnection`: "Bulk insert is only supported for the TestDatabaseConnection" (:354-357). The
  interpreted native does the same (`.../interpreted/natives/LoadCsvToDbTable.java:76, 84`).
- The file: `[P-REL]/legend-pure-runtime-java-extension-shared-store-relational/.../shared/CsvReader.java:37, 46-80` --
  looked up in the **code storage** first (`codeStorage.exists(filePath)`), else `new File(filePath)`; refused above
  `ONE_MB_SIZE_LIMIT = 1` MB ("File is too large ... limit is 1 Mb").
- The one engine use: `[RS-PURE]/functions/tests/loadCsvToDbTable/testLoadCsv.pure:22-38`:
  `loadCsvToDbTable('/core_relational/relational/functions/tests/loadCsvToDbTable/employees.csv', db->schema('default')->toOne()->table('personCsvTable')->toOne(), $connection);`
  then `execute(|Person.all(), csvMapping, testRuntime(), ...)` reads it back. The path is a **repository resource
  path**, not a machine path.

### 3.2 Pure tests seed a shared named database by side effect (Read)

- Pattern: `<<test.BeforePackage>>` setUp functions (112 in `legend-engine-xts-relationalStore`) call helpers like
  `[RS-PURE]/tests/relationalSetUp.pure:1367-1390` (`createTablesAndFillDb`: `executeInDb('Drop table if exists ...;',
  $connection); executeInDb('Create Table ...'); executeInDb('insert into ...')`); `executeInDb(` appears 2,255 times
  in engine `.pure` sources.
- The database: `[P-REL]/legend-pure-runtime-java-extension-shared-store-relational/.../connectionManager/TestDatabaseConnect.java:120-125`:
  `jdbc:h2:tcp://127.0.0.1:<legend.test.h2.port>/mem:testDB` when the port property is set, else `jdbc:h2:mem:`.
  The engine executor uses the same property and name: `[RS-EXEC]/plugin/RelationalStoreExecutorBuilder.java:51, 73`
  (`TemporaryTestDbConfiguration(Integer.parseInt(System.getProperty("legend.test.h2.port", "-1")))`) and
  `DataSourceSpecificationKeyGenerator.java:61` (`testDB`); `[RS-EXEC]/test/H2TestServerResource.java:28-38` starts
  the server and sets the property.

**Inferred.** In upstream's Pure-test road the plan carries no data and no address: a `TestDatabaseConnection` with
no CSV serializes as `h2Local` with empty setup (1.3), which exec resolves to "the executor's `testDB`", which the
setUp functions filled by side effect. This is the opposite of plan-as-data.

### 3.3 PCT relational: the seed is SQL nodes inside the plan (Read)

- `[PCT]/pct_relational.pure:97-127`: the adapter reprocesses the function (TDS literals become tables + CSVs),
  generates the plan, then wraps it: `^RelationalBlockExecutionNode(executionNodes = $setup->concatenate($rawPlan.rootExecutionNode), ...)`
  (:113-118), binds Java, transforms to protocol, `toJSON` (:124), and runs it with `meta::legend::executePlanAsJSON($planAsJson, [])` (:126).
- `setupDatabase` (:429-460): `setUpDataSQLsV2` over the CSVs (:449) with a translator that never drops schemas ("best
  to reuse them between tests specially on shared (not in memory) resources", :440) and creates **temp** tables
  ("to best isolate tables on shared resources", :442); each SQL becomes a `SQLExecutionNode` in a
  `SequenceExecutionNode`; a `drop ... table` node also gets `onConnectionCloseCommitQuery`/`RollbackQuery = $sql` (:457).
- Connections: DuckDB `path = ""` (`[DUCK]/legend-engine-xt-relationalStore-duckdb-PCT/.../DuckDBTestConnectionIntegration.java:48`);
  H2 a `StaticDatasourceSpecification` on a started server, database `"default"` (`.../H2TestConnectionIntegration.java:63-70`).

### 3.4 The test-data generator (Read)

- `[RS-PURE]/testDataGeneration/testDataGeneration.pure:50-55`: `Class TestDataGenResult { dataCsvString : String[1];
  relationTree : RelationTree[1]; sqls : String[*]; }`. `generateTestData` (:184-214) scans the query's relations,
  samples rows from the **real** database starting from row identifiers or default PKs (with milestoning dates),
  and renders each table as `schema\ntable\n<csv>` joined by `'-----\n'` (:204-208); nulls as `---null---` (:634-638);
  optional string hashing.
- Plan form: `planTestDataGeneration*` (:818-1023) builds an execution plan that does the sampling; Java runs it and
  renders the same text: `[TESTABLE]/legend-engine-execution-test-data-generation/src/main/java/org/finos/legend/engine/execution/test/data/generation/TestDataGeneration.java:300-325`
  (`schemaName + "\n" + relationName + "\n" + columnNames + "\n" + rows`, joined by `"-----\n"`, returned as the HTTP entity).
- A second API returns a `RelationalCSVData` skeleton (column headers only):
  `E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-testDataGeneration-http-api/.../service/TestDataGenerationService.java:36-45`
  via `getRelationalCSVDataFromQuery` (`testDataGeneration.pure:753-785`).

**Inferred.** The generator is an authoring tool: its output is the `testDataSetupCsv` / `###Data` text format that a
user pastes into the model. The executor never consumes it directly.

---

## 4. JSON model data for M2M (Read)

- Connections: `[PROTO]/model/packageableElement/store/modelToModel/connection/JsonModelConnection.java:21-26`
  (`_class`, `url`); `ModelChainConnection.java:23-26` (`List<String> mappings` only -- no data; it chains mappings and
  is passed through untouched by the test builder, `TestConnectionBuilder.java:109-112`).
- Plan: the connection rides in the node: `[PROTO]/model/executionPlan/nodes/graphFetch/store/inMemory/StoreStreamReadingExecutionNode.java:23-29`
  (`Connection connection`). A plan string in `[CORE-PURE]/pure/graphFetch/tests/testUnion.pure:69` shows
  `connection=JsonModelConnection(url=data:application/json,{"b1":{...}},class=...)`: the data inline in the URL.
- Test connection with data (`[COMPILER]/test/ModelStoreTestConnectionFactory.java:161-212`): for a
  `JsonModelConnection` + one `ExternalFormatData` of `application/json`, a new connection whose `url =
  "data:" + type + ";base64," + Base64(data)` (:209-211). **Inline, base64 (+33%).**
- Store test data for a ModelStore (`ModelStoreTestConnectionFactory.java:70-104`): `url = "executor:default"` and the
  data is put in `StreamProviderHolder.streamProviderThreadLocal` (:84-87); the closeable removes it. **Not in the plan.**
- Exec: URLs are FreeMarker-processed then opened: `EXEC-CORE .../legend-engine-executionPlan-execution-store-inMemory/.../StoreStreamReadingExecutionNodeContext.java:65-69`
  (`UrlFactory.create(FreeMarkerExecutor.process(url, this.state))`). `[URL]/UrlFactory.java`, `EngineUrlStreamHandlerFactory.java:44`
  (`JRE_PROTOCOLS = file, ftp, http, https, jar, mailto` fall through to `java.net.URL`); `DataProtocolHandler.java:85-88`
  (base64 or URL-decoded payload); `ExecutorProtocolHandler.java:62-73` (reads the thread-local, else "Input stream
  was not provided").
- Pure MFT builds `JsonModelConnection(class=..., url='data:application/json,'+$json)` (unencoded):
  `[CORE-PURE]/store/m2m/mft.pure:66-71`.

---

## 5. Content-addressed or cached test databases (Read)

- No upstream store of databases keyed by a data hash. What exists:
  - pools keyed by connection content (1.7), LRU-evicted after 10 minutes;
  - the shared named `testDB` keyed by nothing but its name and the port property (3.2);
  - the plan cache, keyed by the **serialized runtime** (therefore any inline test data) + function + mapping + model
    pointer: `E/legend-engine-core/legend-engine-core-query-pure-http-api/src/main/java/org/finos/legend/engine/query/pure/cache/PureExecutionCacheKey.java`
    (`this.runtime = objectMapper.writeValueAsString(runtime)`), used only with a `PureModelContextPointer` and the
    `LEGEND_USE_PLAN_CACHE` header (`.../query/pure/api/Execute.java:140-143, 316-318, 356-358`);
  - test-suite plan reuse (2.3).
- **Inferred.** Upstream's reuse unit is the plan and the pool, never a filled database. Reuse of filled state is
  avoided by design (private per-connection databases, re-seeded per acquisition).

---

## 6. Local paths in plans; moving plans between processes (Read)

- **Paths that can appear in a plan:**
  - DuckDB `path` (the database file) inside the connection (1.2); `""` in tests.
  - `EmbeddedH2DatasourceSpecification.directory` (protocol field), refused at exec (1.5).
  - Model-connection URLs of any JRE scheme, `file:` included, FreeMarker-processed at exec (4). I found no fixture
    using `file:`.
- **Data-loading paths are never planning-time values.** The one plan-level file load is the temp-table strategy:
  `[RS-PURE]/graphFetch/relationalGraphFetch.pure:181-186` builds `LoadTableSQL(absolutePathToFile =
  ^VarPlaceHolder(name = 'csv_file_location'))`, rendered by H2 as `INSERT INTO t SELECT * FROM CSVREAD('${csv_file_location}')`
  (`[RS-PURE]/sqlQueryToString/dbSpecific/h2/h2Extension1_4_200.pure:74-78`; Snowflake `PUT file://...`, MemSQL
  `LOAD DATA LOCAL INFILE`, SybaseIQ `USING CLIENT FILE` likewise from the placeholder). The executor writes the file
  from rows it already holds and binds the placeholder: `[RS-EXEC]/plugin/RelationalExecutionNodeExecutor.java:1063-1072`
  (`new TemporaryFile(<config>.tempPath, fileName)`, `tempFile.writeFile(csvSerializer)`,
  `prepareExecutionStateForTempTableExecution("csv_file_location", ..., tempFile.getTemporaryPathForFile())`).
  `tempPath` is executor configuration (`[RS-EXEC]/config/RelationalExecutionConfiguration.java:31`).
- **Moving plans:**
  - Plans are JSON (`SingleExecutionPlan` protocol) and travel as text: service jars ship them as classpath resources
    (`E/legend-engine-xts-service/legend-engine-language-pure-dsl-service-execution/.../AbstractServicePlanExecutor.java:577-595`,
    `readPlanFromResource` -> `PlanExecutor.readExecutionPlan`);
  - HTTP `executePlan` takes a plan body (`ExecutePlanStrategic.java:55-60`);
  - PCT serializes and posts its plan (3.3).
- **"Plan once, run many":** service jars, the plan cache (5), and per-suite test plans (2.3). Inline test data rides
  along each time; the plan cache key includes it.

---

## 7. What this implies for lite (Inferred)

1. **Upstream's dominant answer is content inline, and the content is the identity.** Relational test data, from a
   connection or from `###Data`, ends as text inside the plan's connection description, with the SQL generated at
   planning ("plan generation is always the only place where sql is generated", clustering.pure:65-66). Exec only
   runs it. That matches lite's ruling for hole 7 v1 (the plan carries the data as instructions; exec finds or makes
   the database from them) and supports "content in the plan" for D3. Lite's hash key is a stronger form of
   upstream's `equals(list)` + CRC32 pool name.
2. **Cadence: a fidelity point to post.** Upstream builds a private database and runs the setup on **every
   acquisition**: no filled database survives between queries. Lite's "made once, kept" project database (C5) is
   cheaper but observably different wherever a query or test **writes** to that database: upstream would reset it at
   the next acquisition, lite would not. The doc's two cadences (declared test data per acquisition) match upstream
   for the connection's own data. The made-once list is lite's own design and should be checked against the
   semantics register.
3. **`###Data` in the product has no upstream counterpart I could find.** Upstream consumes `DataElement` only in the
   testables (service, mapping and function tests), the ServiceStore and Mongo test factories, and DataSpace
   `testData` (a reference resolved to content). Search scope: non-test Java referencing `DataElement` or
   `DataElementReference`. If lite's server fills a held project database from `###Data` for ordinary queries, that is
   a lite behavior, likely a SEMANTICS_REGISTER row, unless the user rules it as edge or tooling.
4. **D3: upstream never puts a planning-time file path for data into a plan.** Its two nearest cases point two ways:
   - legend-pure's `loadCsvToDbTable` reads a **repository resource** in Java (code storage, with a file fallback),
     caps it at **1 MB**, and inserts the rows itself, at Pure run time, outside any plan.
   - The engine's only plan-level file load is a **placeholder** (`${csv_file_location}`) that exec binds to a temp
     file it wrote from rows it already holds.

   So the upstream-faithful options for lite are:
   - (a) read the resource at planning and carry its rows inline, as INSERT or VALUES steps, with the 1 MB cap as
     upstream's own bound. This is the closest to legend-pure's semantics: Java reads, the database gets rows.
   - (b) a content-keyed blob plus a named slot in the plan, bound by exec. This is the placeholder pattern, but the
     bytes still have to travel with or beside the plan for G11.

   Lite's current `SqlDml.LoadCsv(file=<absolute path>)`, with the database reading the file, has no upstream
   precedent as a planning-time value. Its javadoc cites H2 `CSVREAD`, but upstream's `CSVREAD` argument is the
   run-time placeholder.
5. **Size tolerance.** Upstream fixtures hold test data of at most a few hundred characters per element. It tolerates
   heavy redundancy: CSV plus SQL, one copy per SQL node, and DDL for every table of the database on the H2 path.
   Its plans are dominated by Java code (177 to 365 KB of 198 to 385 KB). It has no plan-size guard; zlib is
   accepted on input. Base64 `data:` URLs add a third.

   For lite this means:
   - inlining is upstream-faithful for test-sized data;
   - large project data has no upstream design to copy. Measure it (the doc's "measure the plan's size on the demo
     project"), and consider carrying the data once per plan (a target-level table, which lite already has) rather
     than once per statement, which is upstream's redundancy.
6. **Upstream's reference-style mechanisms all break lite's rules.** Four of them keep data outside the plan:
   - Pure-test side effects on a shared `testDB`;
   - the legacy runner stripping the setup out of the plan;
   - the ModelStore `executor:` thread-local;
   - mock servers at dynamic addresses.

   All four rely on run-context state that exec reads but the plan does not carry, which is what G4 (exec runs only
   the plan's statements) and G11 (a portable plan) forbid. The parts lite can borrow:
   - the **shape** of the placeholder: a named slot whose value exec binds from something it owns;
   - **PCT's form**: seeds as SQL steps in the same block as the query, temp tables, dropped on connection close.
     This is the closest upstream precedent for "the test plan carries its seeds as steps".
7. **Paths and URLs as database locations are upstream-normal.** These are the DuckDB `path`, URLs in model
   connections (`file:`, `http:`) and executor-configured `tempPath` and test-DB port. Lite's G11 is stricter than
   upstream here, and that is a design choice, not a fidelity requirement. A database location in the connection
   description is upstream-faithful; data loaded from a local path is not.
