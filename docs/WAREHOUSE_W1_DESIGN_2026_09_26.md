# Warehouse W1: the core and the HTTP SQL API — design (2026-09-26)

Leg W1 of `SERVER_PROGRAM_2026_09_26.md`, built on the measurements in
`WAREHOUSE_W0_HOMEWORK_2026_09_26.md`. **W1 has no authorizer yet**:
grants, ACL views and the authorizer are W2. W1 runs every statement as
its signed-in user, and sets that user's identity before every
statement, so W2 adds checks to a path that already carries identity.

## 1. Where it lives

A new Bazel package **`//warehouse`** (Java package `com.legend.warehouse`;
"warehouse" is a working name):

| Target | What | Depends on |
|---|---|---|
| `:sqlapi` | the HTTP SQL API as Java records, their JSON codec, the **binding** interface (a sans-I/O state machine, program §2c), and the native binding | **java.base only**. It must compile into the WebAssembly module beside the planner (ruling 7), and a guardrail pins that |
| `:client` | the JVM driver for a binding (`java.net.http`), and a **`java.sql.Driver`** over it (`jdbc:warehouse:http://host:port/catalog`) | `:sqlapi`, java.net.http, java.sql |
| `:server_lib` / `:server` | the warehouse process: DuckDB's **C API through `java.lang.foreign`** (W1d), no JDBC class | `:sqlapi`, jdk.httpserver; DuckDB **1.5.5.1**'s native library, taken at run time from its JDBC jar (its own Maven set; legend-lite core stays on 1.4.4 until its own upgrade leg) or passed with `--duckdb-library` |
| `:tests` | conformance, cancel/timeout, concurrency, identity | all of the above |

Rules carried from core: no reflection (ArchUnit), NullAway, the
Windows portability guardrail. **DuckDB's library** is passed with
`--duckdb-library` (a native image ships it beside the binary), or else
extracted once from the JDBC jar on the classpath into a cache file that
every later start reuses (W1d).

## 2. The process

- **HTTP:** the JDK's built-in server (no dependencies; it compiles in a
  native image, W0 Q1), with virtual threads for requests.
- **Catalogs:** W1 uses one DuckDB database file per catalog, owned by
  this process. The interface is `Catalogs`, so DuckLake (W0 Q5) slots in
  when on-demand readers are built, without changing the API.
- **A statement's life:**
  1. auth;
  2. a fresh DuckDB connection that belongs to the principal:
     `system.main.authenticated_user()` answers with it, from outside SQL
     (W1d; program §3 0b);
  3. `current_user` / `session_user` installed as temp macros over it;
  4. the statement, run on the **executor**;
  5. results into chunks;
  6. the connection closed.
- **Executor:** a concurrency limit (default: 1, then tuned; W0 Q4 shows
  one query already uses every core), a FIFO queue with a length cap, a
  per-statement timeout, and cancel through `duckdb_interrupt`.
- **Results:** held per statement until fetched or expired: in memory up
  to a size cap, then spilled to a temp directory. **JSON** chunks of a
  fixed row count; **Arrow** chunks of whole 2,048-row batches (W1d).
- **Query history:** each statement's id, user, SQL text, state, timings,
  rows and error, in a `system.query_history` table, which users will
  query through grants in W2.
- **Tokens (W1):** users with passwords in the server's config.
  `POST /sql/v1/login` returns a short-lived HMAC-signed token. OIDC
  verification against an identity provider replaces the password
  store in W2. The principal is read **only** from a verified token.

## 3. The API (JSON)

Every call carries `Authorization: Bearer <token>`, except `login`. All
bodies are JSON (`application/json`), except Arrow chunks.

```
POST /sql/v1/login            {"user": "u", "password": "p"}
  → 200 {"token": "…", "expiresAt": "2026-09-26T12:00:00Z", "principal": "u"}

POST /sql/v1/statements
  {"sql": "SELECT …", "catalog": "main", "parameters": [ {"type": "INTEGER", "value": 3} ],
   "timeoutMs": 60000, "waitMs": 2000, "resultFormat": "json", "rowsPerChunk": 10000}
  → 200 when done within waitMs: {"statementId": "…", "state": "succeeded", "result": {…meta…}, "firstChunk": {…}}
  → 202 otherwise:                {"statementId": "…", "state": "queued" | "running"}

GET  /sql/v1/statements/{id}
  → {"statementId", "state", "submittedAt", "startedAt"?, "finishedAt"?,
     "result"?: {"columns": [{"name", "type", "nullable"}], "rowCount", "chunkCount"},
     "error"?: {"code", "message", "position"?}}

GET  /sql/v1/statements/{id}/chunks/{n}
  → json:  {"index": n, "rows": [[…], …]}
  → arrow: application/vnd.apache.arrow.stream: a whole stream per chunk (W1d)

POST /sql/v1/statements/{id}/cancel   → {"statementId", "state": "cancelled"}

DELETE /sql/v1/statements/{id}        → {"statementId", "closed": true}: the result freed now

POST   /sql/v1/sessions       {"catalog": "main"}
  → {"sessionId", "catalog", "engine": "DuckDB", "engineVersion": "v1.5.5"}
DELETE /sql/v1/sessions/{id}

GET  /sql/v1/history?limit=100         → the caller's own statements, newest first:
     [{"statementId", "catalog", "sql", "state", "submittedAt", "finishedAt"?, "rowCount"?, "errorCode"?}]

GET  /sql/v1/catalogs                  → [{"name"}]
GET  /sql/v1/catalogs/{c}/objects      → [{"schema", "name", "kind": "table"|"view", "columns": [{"name", "type"}]}]
```

- **Sessions:** a statement with a `sessionId` runs on that session's own
  connection, after the session's earlier statements, so `USE`, `SET`, temp
  tables and transactions carry over. A session reports the engine behind it
  (name and version as that engine's driver gives them): the SQL it accepts
  is that engine's.
- **`describeOnly: true`:** the statement is prepared, never run, and the
  result carries its columns and no rows (a compiler asking what a query
  returns; Snowflake's query API has the same flag).
- **States:** `queued`, `running`, `succeeded`, `failed`, `cancelled`.
- **Error codes:** a closed set:
  - `AUTH_REQUIRED`, `AUTH_INVALID`, `FORBIDDEN` (W2);
  - `SQL_PARSE`, `SQL_BIND`, `SQL_EXECUTE`;
  - `TIMEOUT`, `CANCELLED`, `QUEUE_FULL`, `NOT_FOUND`, `TOO_LARGE`.
- **Types:** a closed list, one table mapping DuckDB type → API type →
  Arrow type → Pure primitive:
  - `BOOLEAN`, `TINYINT` … `BIGINT`, `HUGEINT`, `FLOAT`, `DOUBLE`,
    `DECIMAL(p,s)`;
  - `VARCHAR`, `BLOB`, `DATE`, `TIME`, `TIMESTAMP`,
    `TIMESTAMP WITH TIME ZONE`, `UUID`, `INTERVAL`;
  - `LIST`/`STRUCT`/`MAP`, as JSON.

  A type outside the list is an error, never a guess.
- **JSON values:** numbers as JSON numbers, except BIGINT/HUGEINT/DECIMAL,
  which are **strings** so no precision is lost in a JavaScript client.
  Dates and times use ISO-8601 strings; TIMESTAMP WITH TIME ZONE is the
  UTC instant (`…Z`), which a client shows in its own zone, as DuckDB's
  JDBC driver does (measured: its offset is the client JVM's). NaN, the infinities and **-0.0**
  travel as their names (JSON cannot hold them, and loses -0.0's sign).
  Blobs are base64; JSON is its text.
- **Column types are DuckDB's own type names**, passed through unchanged
  (`INTEGER[]`, `STRUCT(x INTEGER, y DECIMAL(2,1)[])`, `MAP(VARCHAR, INTEGER)`),
  read into a tree by `DuckType` at both ends.
- **Nested values (W1b):** a list is an array, a struct an object, a map an
  array of `[key, value]` pairs. A top-level nested cell is
  `{"value": …, "text": "…"}`, where `text` is DuckDB's own text for it
  (what its driver's `getString` gives), so no client re-derives DuckDB's
  quoting rules.

## 4. The binding (program §2c), in `:sqlapi`

```java
interface SqlApiBinding {
  HttpCall login(Credentials c);
  HttpCall submit(StatementRequest r, Token t);
  Step next(HttpCall sent, HttpResult got);   // Poll | Fetch | Done | Failed
  ResultChunk decode(HttpResult chunk);
  HttpCall cancel(String statementId, Token t);
}
```

`NativeBinding` speaks §3 directly. A driver loop (`java.net.http` in
`:client`; `fetch` from the WebAssembly module in D1) performs the
calls. Vendor bindings (V*) implement the same interface.

## 5. Legs inside W1, each proven before the next

| Step | Builds | Proven by |
|---|---|---|
| **W1a** | `:sqlapi` (records, JSON codec, native binding) + `:server` (login, statements, poll, chunks as JSON, cancel, executor, history, identity per statement) | endpoint tests; identity per statement (two users read their own `app_user` at once); cancel and timeout; queue full |
| **W1b** | `:client`: the `java.net.http` driver and the `java.sql.Driver` | a JDBC conformance suite: types round-trip, nulls, big results across chunks, errors carry codes |
| **W1c** | **The corpus proof:** legend-lite's DuckDB lane with its connection pointed at the warehouse through `jdbc:warehouse:` (data loaded by an owner user) | the lane's pass count equals the in-process DuckDB lane's; every difference is a red row, explained, never masked |
| **W1d** | Arrow chunks: `nanoarrow` vs our writer, measured, and the winner shipped. **Became:** the server on DuckDB's C API through FFM, Arrow from `duckdb_data_chunk_to_arrow`, the identity function | 1M-row timing; a standard Arrow reader reads every chunk; the JSON and Arrow values of one result are identical; W1c re-run |
| **W1e** | The native image of `:server`, a CI build, and tests run **against the binary** | the conformance suite on the native executable |

**Not in W1:**
- grants, ACL views and the authorizer (W2);
- the PostgreSQL wire protocol (W3);
- DuckLake and on-demand instances (with deployment);
- OIDC (W2);
- the static-pivot dialect rule (before D1).

## W1b: the JDBC driver (2026-09-26)

`jdbc:warehouse:http://host:port/catalog?user=…&password=…`. **Its values
are the same Java objects DuckDB's own driver returns**, measured
identical on 1.4.4 and 1.5.5.1 for every type: `LocalDate`,
`java.sql.Timestamp` (re-readable as `LocalDateTime`), `OffsetDateTime`,
`BigInteger`, `BigDecimal` with its scale, `UUID`, and `java.sql.Array` /
`java.sql.Struct` / `LinkedHashMap` for nested values, printing as DuckDB's
do. The proof is `WarehouseJdbcTest`, a **differential**: the same SQL
through this driver over HTTP and through DuckDB's driver in-process,
compared cell by cell (class, printed form, `getString`, type name, JDBC
code, nested elements, update counts). **Named differences:** a BLOB is
compared by bytes and JSON by text, since DuckDB's classes for those are
its own internals. JDBC calls the executor does not use are generated
stubs that throw "not supported", never a wrong answer. A connection is a
session, so autocommit off, commit and rollback work as on DuckDB.

## Found while building W1a (2026-09-26)

- **DuckDB JDBC 1.5.5.1 loses errors on `Statement.execute`.** A binder or
  catalog error ("Table with name … does not exist", "Referenced column
  … not found") comes back as a generic "Invalid Input Error: Attempting
  to execute an unsuccessful or closed pending query result". 1.4.4
  reports them correctly on the same path, and `PreparedStatement`
  reports them correctly on 1.5.5.1. So the server prepares every
  statement, then executes it. Worth reporting to DuckDB (not done yet;
  it would go out under the user's name).
- **A script travels whole.** DuckDB's `prepareStatement` runs a script's
  leading statements and prepares the last, so legend-lite's effect-body
  scripts need nothing special (corrected 2026-09-26: this line first said
  a prepare takes one statement). W2's authorizer allows one statement for
  end users.
- **A queue that refuses works:** with a concurrency of 1 and a queue of
  1, the third slow statement is refused with `QUEUE_FULL` (HTTP 503)
  instead of waiting forever.

## W1c: the corpus through the warehouse (2026-09-26)

**Proven: the DuckDB corpus lane gives the same verdicts through the
warehouse as in process.** `bazel test //spec:corpus_warehouse` (manual,
not in the chain) is `:corpus_duckdb` with every connection a session on a
warehouse the harness starts as a child process (its deploy jar, DuckDB
1.5.5.1 on its own classpath; the harness runs 1.4.4). Workspaces keep their
shape: the root session ATTACHes `__ws_N`, each workspace and aside is its
own session doing `USE`, and closing one DETACHes it.

| Lane | host judge | database judge | per pass |
|---|---|---|---|
| in process (DuckDB 1.4.4) | 2,472 pass / 107 fail | 2,474 / 107 | ~35 s |
| warehouse (HTTP, 1.5.5.1) | 2,472 / 107 | 2,474 / 107 | ~52 s |

Same roster, same database-mode lines. What it took, each a fact the lane
measured, not a guess:

- **Two API facts:** a session reports its engine (core picks its SQL dialect
  from `DatabaseMetaData.getDatabaseProductName`), and `describeOnly`
  (core's `WireTypes` reads a prepared statement's columns before running
  it).
- **Column metadata as DuckDB's driver gives it:** precision, scale,
  nullability, signedness and class names are per-type constants, measured
  on 1.5.5.1 and pinned by the differential test.
- **One core fix, the one red row** (`testAggToManyWithFilter`,
  `ClassCastException`): the executor decoded a JSON cell only when the
  driver's object was named `org.duckdb.JsonNode`, so a JSON column arriving
  as text (this driver) went undecoded. It now decodes when the PLAN types
  the column JSON. That is a class name no longer driving logic; the
  in-process lane is unchanged by it.
- **Nothing else:** the bulk loader falls back to the text path (the
  appender is DuckDB's in-process API), and the graph's system database stays
  in process (it holds the metamodel, not the test data).

The ~50% time cost is one HTTP round trip and JSON per statement; W1d
(Arrow chunks) and connection reuse are where it would come back.

## W1d: the server on DuckDB's C API (2026-09-26)

The Arrow leg grew into a move, decided with the user after the homework
(docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md): **the server calls DuckDB's
public C API through `java.lang.foreign`** and uses no JDBC class (it
compiles without DuckDB's jar; the jar is a runtime source of the native
library). No reflection, no new dependency. nanoarrow was rejected by
measurement: an ENUM, even nested, invalidates the database for every
connection.

- **`server.duck`:** `Duck` (every signature read from duckdb.h v1.5.5),
  `Database`, `Conn`, `Result`; results come chunk by chunk through
  `duckdb_data_chunk_to_arrow` (one C call per 2,048 rows; a call costs
  ~2.3 us in a native image), copied to the heap in bulk (`ColumnData`),
  then read by the JSON encoder (`JsonCells`), DuckDB's own text for
  nested cells (`DuckValues`) and the Arrow framer (`ArrowStreams`,
  `FlatBuilder`).
- **Identity:** `system.main.authenticated_user()`, registered per
  database, bound per query from the caller's connection id (program §3
  0b). `SET VARIABLE app_user` is gone. Tests:
  `noStatementCanChangeWhoTheUserIs`, `anAclViewShowsEachUserTheirOwnRows`.
- **Scripts** split by DuckDB's own parser (`duckdb_extract_statements`),
  run in turn, the last one's result returned; **describe** from
  `duckdb_prepare` (leading statements of a script run first, as DuckDB's
  JDBC driver's prepare did); **errors** keep DuckDB's messages.
- **Type names** are spelled from the logical type (the C API has no
  function for it): STRUCT field names quoted by DuckDB's rule with its
  own keyword list; a top-level ENUM is `ENUM`, a nested one
  `ENUM('a', 'b')`, as DuckDB's driver names them (measured).
- **A nested cell's text** is DuckDB's: the cell is built back into a
  `duckdb_value` and DuckDB casts it (`duckdb_get_varchar`): identical to
  DuckDB's driver's `getString` for every case tested.
- **Arrow** (`resultFormat: "arrow"`): each chunk a whole IPC stream of
  whole batches (at least `rowsPerChunk` rows, but the last); UHUGEINT and
  ENUM as text, TIMESTAMP WITH TIME ZONE tagged UTC, the rest as DuckDB
  writes it (INTERVAL as month-day-nano, `T[n]` as FixedSizeList).

**Proven:**

- `//warehouse:tests` 35/35, including the JDBC differential against
  DuckDB's own driver (every type, edges, nulls, nested values of every
  kind with DuckDB's text and type names, 25,000 rows, counts, errors,
  sessions, transactions, scripts, describe, metadata) and
  `WarehouseArrowTest`: pyarrow reads every Arrow chunk and every value
  equals the JSON API's (35 columns of every type, extremes and NULLs;
  25,000 rows in 3 chunks): **0 differences**. CI's app lane installs
  pyarrow and sets `WAREHOUSE_ARROW_CHECK=required`, so it cannot skip.
- **W1c re-run through the C-API server:** host 2,472 / 107, database
  2,474 / 107, the committed roster, ~52 s a pass (as through JDBC).
- **1M rows x 8 columns over HTTP, end to end** (submit, then every
  chunk, this machine): JSON 734–950 ms and 92.4 MB; **Arrow 76–135 ms
  and 62.3 MB**.

**Owed:**

- **JSON inside a nested value** renders quoted in `getString` (DuckDB's
  driver prints it raw): the C API cannot build a JSON-typed value. The
  value itself is identical. Pinned by
  `jsonInsideANestedValueIsTheOneNamedTextDifference`.
- **UHUGEINT nested** in a list or struct travels in Arrow as DuckDB
  writes it (decimal128, right up to 2^127); top-level UHUGEINT is text.
- **`java_language_version = 21`** in `.bazelrc`: `java.lang.foreign` is
  final only from 22. It compiles against JDK 25's classes today; W1e
  (the native image) should raise the level for the warehouse.
- A nested TIMESTAMP before year 1 in JSON: DuckDB's driver reads nested
  timestamps through `java.sql.Timestamp` (wrong for BC years); ours is
  right. Not in the differential.

## W1e: the server as a native image (2026-09-26)

**Built by Bazel (2026-09-26, after the break below).** `//warehouse:server_native` is GraalVM's
`native-image` over `:server_lib`'s runtime class path, supplied by Bazel (rules_graalvm 0.12.0,
GraalVM CE 25.0.2 fetched by Bazel), with `--link-at-build-time`: a class missing from the class
path fails the build. `//warehouse:duckdb_library` takes DuckDB's library for the platform out of
its JDBC jar with Bazel's zipper (`warehouse/defs.bzl`). `//warehouse:tests_native` is the same
suite with `WAREHOUSE_BINARY` and `WAREHOUSE_DUCKDB_LIBRARY` pointing at those two: every warehouse
test starts that executable (the tests' `TestServer`) instead of an in-process server, on a free
port, with the same users and limits, and talks to it over HTTP only. `bazel test //...` builds and
runs it, so both sessions' local chains judge the binary (~40 s to build, cached until warehouse or
core changes). Linux and macOS; Windows is owed.

- **The break that moved it into Bazel:** a script (`warehouse/tools/build-native.sh`, deleted)
  listed the class path by hand as "the first file of `//core`". When core split into 29 targets,
  `//core` became an umbrella whose jar is empty; the image shipped with no core class, and
  native-image's default (link at run time) turned every method naming one into a
  `NoSuchMethodError` at startup. The local chain never built the image, so no one saw it before
  CI did.
- **A Mac with Command Line Tools only** (no Xcode.app, as on this repository's machine): Bazel
  knows no Xcode, and rules_graalvm's macOS path needs one. A patch applied by Bazel
  (`third_party/rules_graalvm_command_line_tools.patch`) runs native-image directly there, as on
  Linux; with Xcode (CI's macOS runners) the rule's own path is unchanged.

- **Metadata** (`META-INF/native-image/com.legend/warehouse/reachability-metadata.json`, in the
  server jar, so native-image reads it with no flags): 39 FFM call shapes, the identity function's 3
  upcalls, the JDK's HTTP server and crypto providers, time-zone data. Recorded by GraalVM's agent while
  the whole suite runs against the JVM server (one agent directory per process, merged); re-recording
  reproduced the committed file byte for byte. **Owed:** re-recording as a Bazel target (the suite
  under the agent, then `bazel run` writing the merged file back, as `//:update_generated` does);
  the script that did it is gone. Needed only when the server's FFM or reflection use changes.
- **`GET /sql/v1/history`:** the caller's own statements, newest first (the history test reads it
  through the API, so it judges the binary too; another user's statements are not in yours).
- **CI:** the `native` lane (Linux, macOS) runs `bazel test //warehouse:tests_native`, with the Arrow
  check required.

**Measured (this machine, GraalVM CE 25.0.1, a 21.5 MB binary, built in ~23 s):**

| | native image | JVM |
|---|---|---|
| start to listening (DuckDB opened, system database, identity function) | **161 ms** | |
| idle memory | **60 MB** | |
| `//warehouse:tests` | **35/35** | 35/35 |
| 1M rows x 8 as Arrow over HTTP | **158–167 ms** | 76–135 ms |
| 1M rows x 8 as JSON over HTTP | **2.7–4.0 s**, 2.7 GB resident after | 0.73–0.95 s |

**JSON chunks written straight to bytes** (the JSON path built a tree of objects per value and held
it for the retention period; the native image's serial collector paid most). Each chunk is now written
once, through core's streaming `Json.Writer` (the same writer the tree was printed with, so the text is
identical), kept as bytes, served as they are, and the first chunk spliced into the status:

| 1M rows x 8 as JSON over HTTP | before | after |
|---|---|---|
| native: total | 2.7–4.0 s | **1.20–1.27 s** |
| native: fetching the chunks | 0.63–1.74 s | **92–95 ms** |
| native: resident after six results held | 2.7 GB | **745 MB** |
| JVM: total | 0.73–0.95 s | **0.63–0.72 s** |

**Owed:** Windows native builds (a separate toolchain setup). (Results held for their retention:
done, "Results: freed when done" below.)

## W1f: the JDBC driver reads Arrow by default (2026-09-26)

Decided with the user: the API's default stays JSON (curl, scripts; Snowflake's and Databricks'
defaults too); **our own clients ask for Arrow**.

- **One set of value rules** (`:sqlapi`, java.base only): `Columnar` (one column of one batch in
  Arrow's layout) and `ApiValues` (the API's JSON values from it). The server fills `Columnar` from
  DuckDB's buffers; the driver fills it from an Arrow chunk (`ArrowIpcReader`, plain Java, no Arrow
  library). So an Arrow result and a JSON result give a client the same values, by construction.
- **DuckDB's nested text over Arrow:** with `cellText: true`, each batch carries DuckDB's text for its
  nested cells in the Arrow message's metadata (`legend.cell_text`), where other Arrow readers do
  not look. INTERVAL text is `Intervals.text`, DuckDB's rule, pinned against DuckDB's own cast over
  486 intervals (`intervalsSpellAsDuckDBCastsThem`); the server uses it too, so it no longer asks DuckDB.
- **The driver:** `resultFormat=arrow` (the default) or `resultFormat=json` in the URL;
  `getClientInfo("resultFormat")` says which. `HttpResult` carries bytes.
- **Proven:** the whole differential against DuckDB's own driver runs twice, on Arrow
  (`WarehouseJdbcTest`) and on JSON (`WarehouseJdbcJsonTest`): `//warehouse:tests` 57/57.
- **1M rows x 8 through the driver, every cell read** (JVM server): executeQuery JSON 910–1,134 ms,
  Arrow 460–783 ms; in all, JSON 1.53–1.82 s, Arrow 1.07–1.39 s.

**Owed:** the driver still turns each Arrow value into the API's JSON form, then into a Java object
(~600 ms of the above in both formats); reading straight from `Columnar` would drop that step.


## Results: freed when done, spilled past a budget (2026-09-26)

A finished statement's result waits to be fetched: the API is asynchronous, and a client may fetch
any chunk again. Until now every result stayed in memory for its whole retention (10 minutes), even
once fetched. Now (`ResultStore`):

- **Freed when the client is done:** `DELETE /sql/v1/statements/{id}` gives the memory back and
  deletes the files at once; our client sends it once it has read every chunk. The retention stays
  only as a safety net for a client that never comes back.
- **A memory budget across every result** (`--result-memory-mb`, 1 GiB by default): past it, a chunk
  goes to a file under the data directory (`results/<statement>/<chunk>`) and is read from there.
  Whatever a previous run spilled is removed at start.
- **Chunks go to the store as they are written:** a large result never sits in memory whole.
- **`GET /health`** reports `results.inMemoryBytes` and `results.spilledBytes`.

Tests (in process and against the native binary): `aResultTheClientIsDoneWithIsFreedAtOnce`,
`theClientFreesWhatItHasRead` (both formats), `aResultPastTheMemoryBudgetSpillsToFilesAndReadsTheSame`
(a 1 MB budget, 200,000 rows: memory within the budget, the rest in files, every checked row right,
the files deleted on close). `//warehouse:tests` 60/60.
