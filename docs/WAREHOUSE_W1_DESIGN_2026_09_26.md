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
| `:server_lib` / `:server` | the warehouse process | `:sqlapi`, jdk.httpserver, java.sql, DuckDB JDBC **1.5.5.1** in its own Maven set (legend-lite core stays on 1.4.4 until its own upgrade leg) |
| `:tests` | conformance, cancel/timeout, concurrency, identity | all of the above |

Rules carried from core: no reflection (ArchUnit), NullAway, the
Windows portability guardrail. **DuckDB is loaded from disk**, never
unpacked from the jar (W0 Q1). The server uses the driver's classes
without its bundled library, and the library ships beside them.

## 2. The process

- **HTTP:** the JDK's built-in server (no dependencies; it compiles in a
  native image, W0 Q1), with virtual threads for requests.
- **Catalogs:** W1 uses one DuckDB database file per catalog, owned by
  this process. The interface is `Catalogs`, so DuckLake (W0 Q5) slots in
  when on-demand readers are built, without changing the API.
- **A statement's life:**
  1. auth;
  2. a fresh DuckDB connection (`duplicate()`);
  3. `SET VARIABLE app_user = <principal>` (plus roles, once W2 has
     them);
  4. the statement, run on the **executor**;
  5. results into chunks;
  6. the connection closed.
- **Executor:** a concurrency limit (default: 1, then tuned; W0 Q4 shows
  one query already uses every core), a FIFO queue with a length cap, a
  per-statement timeout, and cancel through `Statement.cancel`.
- **Results:** held per statement until fetched or expired: in memory up
  to a size cap, then spilled to a temp directory. Chunks of a fixed row
  count. **JSON in W1a; Arrow in W1e**, chosen by measurement between
  DuckDB's `nanoarrow` extension and our own writer (W0 Q6).
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
  → arrow: application/vnd.apache.arrow.stream (W1e)

POST /sql/v1/statements/{id}/cancel   → {"statementId", "state": "cancelled"}

GET  /sql/v1/catalogs                  → [{"name"}]
GET  /sql/v1/catalogs/{c}/objects      → [{"schema", "name", "kind": "table"|"view", "columns": [{"name", "type"}]}]
```

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
  Dates and times use ISO-8601 strings.

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
| **W1d** | Arrow chunks: `nanoarrow` vs our writer, measured, and the winner shipped | 1M-row timing; a standard Arrow reader reads every chunk; the JSON and Arrow values of one result are identical |
| **W1e** | The native image of `:server` (metadata from DuckDB's official file, W0), a CI build, and tests run **against the binary** | the conformance suite on the native executable |

**Not in W1:**
- grants, ACL views and the authorizer (W2);
- the PostgreSQL wire protocol (W3);
- DuckLake and on-demand instances (with deployment);
- OIDC (W2);
- the static-pivot dialect rule (before D1).

## Found while building W1a (2026-09-26)

- **DuckDB JDBC 1.5.5.1 loses errors on `Statement.execute`.** A binder or
  catalog error ("Table with name … does not exist", "Referenced column
  … not found") comes back as a generic "Invalid Input Error: Attempting
  to execute an unsuccessful or closed pending query result". 1.4.4
  reports them correctly on the same path, and `PreparedStatement`
  reports them correctly on 1.5.5.1. So the server prepares every
  statement, then executes it. Worth reporting to DuckDB (not done yet;
  it would go out under the user's name).
- **Prepared statements take one statement.** legend-lite's executor sends
  multi-statement scripts for effect bodies, so W1c (the corpus proof)
  must decide how a script travels: split by the client, or accepted as
  a script by an owner role. W2's authorizer allows one statement for end
  users either way.
- **A queue that refuses works:** with a concurrency of 1 and a queue of
  1, the third slow statement is refused with `QUEUE_FULL` (HTTP 503)
  instead of waiting forever.
