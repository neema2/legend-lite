# Server program — the warehouse, the SQL API, and DataCube's server modes (2026-09-26)

**Goal (user, 2026-09-26):** a production-grade database server built on
DuckDB that behaves like a real cloud warehouse, and real server modes for
DataCube + legend-lite on top of it, ending in full compatibility with
legend-engine on a single code path.

"Warehouse" is a working name. No product or vendor names appear in code,
files, UI, docs or commits (standing rule).

## 0. Rulings (user, 2026-09-26)

1. **Entitlements live in the warehouse.** Not in legend-lite, not in
   DataCube. Every mode inherits them because every mode's SQL ends in the
   warehouse.
2. **Always the user's own identity.** Every query runs as the person
   viewing it: the token travels from the browser, through legend-lite when
   it is in the path, to the warehouse. **No service accounts**, not even
   one that switches role per user.
3. **Role-level views, never query rewriting.** What a role may see is a
   set of views: rows are the view's `WHERE`, columns and masks are its
   `SELECT`. The server never changes a statement.
4. **One HTTP SQL API, modeled in legend-lite.** The browser only ever
   speaks it. Vendor APIs are reached by binding that one API on the Java
   side, so another vendor's warehouse can be added later without touching
   the client.
5. **Direct first.** The browser path (the tab plans, the warehouse runs,
   over the HTTP SQL API) comes before the server-side JDBC path.
6. **Entitlements are ACL tables joined in views** (user, 2026-09-26):
   `security.acl(username, <key>)`, one row per value a user may see, at
   whatever granularity, joined to the data in the role's view.
7. **The SQL-API client and every vendor binding run in WebAssembly**
   (user, 2026-09-26): the same Java code, in the module beside the
   planner, not a separate TypeScript client.

## 1. The pieces

```
                ┌──────────── browser (DataCube) ────────────┐
                │  legend-lite planner (WebAssembly)         │
                │  one SQL-API client · one engine-API client│
                └──────┬──────────────────────────┬──────────┘
      HTTP SQL API     │                          │  engine HTTP API (legend-engine's)
   (user's token)      │                          │  (user's token)
                       ▼                          ▼
        ┌─────────────────────────┐   ┌───────────────────────────────┐
        │ WAREHOUSE (native)      │   │ legend-lite server            │
        │  HTTP SQL API           │◄──┤  engine API · planner · exec  │
        │  PostgreSQL wire (JDBC) │   │  SQL gateway: vendor bindings │
        │  auth · roles · views   │   └──────────────┬────────────────┘
        │  authorizer · DuckDB    │                  │ JDBC or a vendor's
        └─────────────────────────┘                  ▼ HTTP API (user's token)
                                            other warehouses (later)
```

### 1a. The warehouse

A server process around DuckDB that behaves like a hosted warehouse *as a
service*: its protocols, identity, isolation, limits and history. It
speaks **DuckDB SQL**. It does not emulate another vendor's dialect;
legend-lite already renders DuckDB, and a second dialect would be a
different program.

- **Protocols:**
  - the **HTTP SQL API** (§2), which is what the browser calls;
  - the **PostgreSQL wire protocol** (simple and extended query,
    prepared statements, cancel, TLS), so every Postgres JDBC/ODBC
    driver, psql and BI tools connect, and so does legend-lite;
  - **Arrow Flight SQL** later, for fast columnar reads.
- **Identity:**
  - users, roles and role grants;
  - short-lived tokens (password or OAuth exchange);
  - sessions carrying the user and their active role.
- **Entitlements (§3):**
  - role-level views in role schemas, with `search_path` set per session;
  - locked per-session identity variables;
  - a statement authorizer that allows or denies each statement and
    never rewrites it.
- **Catalogs:**
  - DuckDB database files per catalog;
  - external data (Parquet / Iceberg on object storage) behind views,
    so users are granted views and never paths.
- **Service behaviour:**
  - a connection pool;
  - a concurrency limit and a queue per "warehouse size";
  - per-statement memory limits and timeouts;
  - cancellation;
  - a result cache keyed by statement text, role and catalog version;
  - query history as a queryable table;
  - metrics.
- **Durability:**
  - DuckDB's WAL and checkpoints;
  - one writer per catalog (DuckDB's model), many readers;
  - scheduled backups, and restore tested.
- **Runs everywhere CI runs:** macOS, Linux and Windows (the portability
  guardrail applies).

**Where it lives:** a Java module in this repo under Bazel. That reuses
DuckDB JDBC, the build, CI and the Windows lane. The process is separate
from legend-lite's, so legend-lite reaches it exactly as it would reach
any remote database, and the remote path is proven, not simulated.

### 1b. legend-lite as an engine server

legend-lite serves **legend-engine's exact HTTP API**
(`datacube/docs/ENGINE_API_CONTRACT.md`) and executes against the
warehouse. It gains:

- **Connection types** for the warehouse, over JDBC (the PostgreSQL
  wire) and over the HTTP SQL API. The dialect is DuckDB either way.
- **Identity pass-through:** the incoming user's token opens (or
  borrows) a connection as that user. There is no shared pool of
  service credentials.
- **The SQL gateway** (§2c): the HTTP SQL API served by legend-lite,
  bound to a vendor's own API or to JDBC.

### 1c. DataCube's modes

| Mode | Plans | Runs | Path | Needs |
|---|---|---|---|---|
| **Local** (today) | the tab | DuckDB in the tab | — | — |
| **Direct** (first) | the tab | the warehouse | HTTP SQL API, user's token | 1a + §2 |
| **Engine** | legend-lite server | the warehouse | engine API → JDBC, user's token | 1a + 1b |

All three plan with one compiler: the WebAssembly build of legend-lite's
planner and the JVM build are held equal by the WASM differential. So one
query is one SQL in every mode, and a **mode differential** can hold all
three to identical rows.

## 2. The HTTP SQL API

### 2a. Shape

This is our own, clean API: the asynchronous "statements" pattern hosted
warehouses share. It isn't copied from any one vendor. It is
**defined once in legend-lite**, as Java records plus a TypeScript
client generated from them. It is **served natively** by the warehouse,
and **by binding** in the legend-lite gateway.

| Call | Does |
|---|---|
| `POST /sql/v1/statements` `{sql, catalog?, schema?, parameters?, timeoutMs?, resultFormat: arrow\|json, maxRowsPerChunk?}` | submit; returns `{statementId, state}`, or the whole result when it finishes within a short wait |
| `GET /sql/v1/statements/{id}` | state (`queued`, `running`, `succeeded`, `failed`, `cancelled`), result metadata (columns and types, row count, chunk list), error |
| `GET /sql/v1/statements/{id}/chunks/{n}` | one result chunk: Arrow IPC stream, or JSON rows |
| `POST /sql/v1/statements/{id}/cancel` | cancel |
| `POST /sql/v1/sessions` / `DELETE …/{id}` | a session, for `search_path` and the identity variables; optional, since a statement may carry its own context |
| `GET /sql/v1/catalog/...` | catalogs, schemas and objects **the caller may see**, with their columns |

**Auth:** `Authorization: Bearer <user token>` on every call. **Errors:**
one error document with a stable code, a message and, where it applies,
the SQL position. **Types:** a closed list mapped to Arrow types and to
legend-lite's Pure primitives in one table, so a result's types never
come from guessing.

### 2b. Why Arrow

DataCube already receives Arrow from DuckDB-wasm and turns it into its
result table (`result.ts`). The warehouse streams Arrow IPC, so Direct
mode reuses that reader unchanged. JSON exists for tools and tests.

### 2c. Bindings: the same Java in the tab and on the server

**It must compile to WebAssembly (ruling 7).** TeaVM's WebAssembly has
no `java.net` and no blocking I/O. So a binding does **no I/O**: it
builds the next HTTP call and interprets each response. It is a pure
state machine:

```java
interface SqlApiBinding {                    // compiled to WASM and run on the JVM alike
  HttpCall submit(Statement s, Token t);    // what to send
  Step next(HttpResult r);                   // Poll(call) | Fetch(call) | Done(meta) | Failed(error)
  RowBatch decode(HttpResult chunk);         // a vendor's result → the canonical batch
}
```

A small **driver** performs the I/O: `fetch` in the browser,
`java.net.http` on the JVM. Both drive the identical binding.
- **Proof:** a WASM differential like the planner's. Recorded exchanges
  replay through both builds, with identical steps and rows required.
- **Arrow:** Arrow from the warehouse stays in JavaScript, which
  DataCube already decodes. A binding only converts a vendor's own
  format to canonical batches.
- **CORS:** a vendor API that refuses browser origins, or needs a
  browser sign-in flow, is reached through the legend-lite gateway,
  running the same binding.

Implementations:

- **native:** the warehouse (pass-through);
- **JDBC:** any JDBC database. Asynchrony is emulated with a statement
  thread, cancel is `Statement.cancel`, and chunks are paged from the
  `ResultSet`;
- **vendor HTTP APIs:** added one at a time, later. Each maps submit,
  poll, cancel and fetch onto that vendor's own statement API, carrying
  the user's OAuth token.

The same interface is also a **connection type** for legend-lite's
executor, so a binding written once serves both the gateway (Direct mode
through legend-lite) and Engine mode. **Caveat:** a vendor binding moves
bytes. Running legend-lite's SQL on that vendor also needs legend-lite to
render its dialect, which is a separate leg per vendor.

## 3. Entitlements

The warehouse enforces them. Nothing upstream of it is trusted.

**Measured on DuckDB 1.4.4 (2026-09-26):**

| What | Result |
|---|---|
| `current_user`, `user`, `session_user` | exist, but always return the constant `duckdb`: there is no login to carry |
| `SET VARIABLE` / `getvariable()` | **per connection**: another connection to the same database does not see it |
| ACL join in a view on `getvariable` | works: user1 → EMEA rows, user2 → AMER + APAC |
| `lock_configuration = true` | locks settings but **not** variables: a session can still `SET VARIABLE`, so the authorizer must forbid it |
| `json_serialize_sql` | lists every table, including in subqueries and CTEs, table functions and file paths used as tables; **fails on anything but SELECT**: `SET`, `ATTACH`, `COPY`, `PRAGMA`, `INSTALL` |

This mirrors real servers: Postgres checks the querying role's grants
on each referenced relation (a view needs only its own grant) and adds
row policies inside the engine; hosted warehouses grant to roles, expose
`CURRENT_USER()` / `CURRENT_ROLE()`, and implement rows with secure
views or row policies joined to a mapping table. That is the pattern
here, without the engine rewriting anything.

0. **One DuckDB connection per session.** Variables are per connection,
   so one user's identity cannot be seen from another's session.
1. **Roles and grants:** users hold roles; roles are granted catalogs,
   schemas and views. The base tables are granted to no end-user role.
   Grants live in the server's own tables: `security.user_roles` and
   `security.grants(role, object)`.
1b. **ACL tables:** `security.acl(username, <key>)`, one row per value a
   user may see, at whatever granularity, e.g.
   ```sql
   CREATE MACRO app_user() AS getvariable('app_user');
   CREATE VIEW sales.trades AS SELECT t.* FROM base.trades t
   WHERE EXISTS (SELECT 1 FROM security.acl a
                 WHERE a.username = app_user() AND a.region = t.region);
   ```
   Several keys mean several `EXISTS` clauses, and masks are
   expressions in the view's `SELECT`.
2. **Role-level views:** a role's schema holds views over the base data.
   - **Rows:** `WHERE region = 'EMEA'`, or per user:
     `WHERE region IN (SELECT region FROM entitlements.user_regions
     WHERE user_id = getvariable('user_id'))`.
   - **Columns:** included or not.
   - **Masks:** an expression, e.g. `'***' AS account_no` or a
     hash.
3. **Name resolution, not rewriting:** a session's `search_path`
   starts at its role's schema, so the `TRADES` in legend-lite's SQL
   resolves to that role's view of trades. The statement text is never
   edited.
4. **Identity variables:** at session start the server sets
   `user_id`, `roles` and user attributes as DuckDB variables, then
   locks the session so a statement cannot change them.
5. **The authorizer** allows or denies each statement; it never rewrites
   one. It parses with DuckDB's own parser (`json_serialize_sql`) and
   denies:
   - any object the role is not granted;
   - `SET`, `ATTACH`, `INSTALL`/`LOAD`, `COPY … TO`, `PRAGMA`;
   - file- and URL-reading table functions;
   - anything its parser cannot classify: an unclassifiable statement
     is **denied**, never passed.
   - `SELECT` is the default for end-user roles. DML/DDL go to owner
     roles only.
6. **Audit:** every statement lands in query history with its user,
   role, decision and objects.

**Proof:**
- an entitlement differential: one query, three users, each seeing only
  their rows, in every mode;
- an adversarial suite that must be denied: base-table names, other
  roles' schemas, file functions, `SET VARIABLE`, statement stacking,
  comment tricks, and the parser's edge cases.

## 4. Legs, in order

Each leg lands with its proof, and CI is green before the next starts.

| Leg | What | Proof |
|---|---|---|
| **W0** | Homework: DuckDB's concurrency model under a server (one process, a connection per session, the writer), `json_serialize_sql` coverage for the authorizer (every SELECT form; CTE names vs objects; multi-statement text), views over external data with file access denied to users, Arrow IPC streaming from DuckDB JDBC, **what TeaVM's WebAssembly accepts for a sans-I/O binding** (JSON handling, byte arrays, the driver seam to `fetch`), the Windows lane. Measured, written down | the homework doc, with probes |
| **W1** | Warehouse core: process, catalogs, users/roles/tokens, sessions, the **HTTP SQL API** natively (§2a), Arrow + JSON results, limits, cancel, query history | a Java client suite over HTTP; **legend-lite's DuckDB corpus run against the warehouse through the HTTP API as a connection type**, so thousands of queries prove "the same as DuckDB, remotely" |
| **W2** | Entitlements: role schemas, views, identity variables, the authorizer (§3) | the entitlement differential; the adversarial deny suite |
| **D1** | **DataCube Direct mode:** a warehouse source (URL + sign-in), the tab plans, the SQL-API client runs it, Arrow in; the catalog call lists what the user may see | the browser harness over a running warehouse; the **mode differential**: Local vs Direct, identical rows for every harness cube |
| **W3** | PostgreSQL wire protocol, on the same sessions, auth and authorizer | psql and the Postgres JDBC driver; the corpus again, over JDBC |
| **E1** | legend-lite **Engine mode**: warehouse connection types (JDBC, HTTP), identity pass-through, `execute` and `generatePlan` in legend-engine's exact JSON (contract P1, E8/P4, E9/P5), DataCube calling only the engine API (C1) | the mode differential across all three modes; the per-user entitlement differential through legend-lite |
| **G1** | The **SQL gateway** in legend-lite: the HTTP SQL API served by bindings (native, JDBC) | the SQL-API conformance suite, run against the warehouse **and** the gateway |
| **E2** | The rest of the engine API: relation and return types (E5/E6: calculated columns validated upstream's way), the grammar composer (E4), the query store in the warehouse (Q1/Q2/S1: save/load as `DataCubeSpecification`), model pointers | the contract's endpoint tests |
| **X1** | **Compatibility with legend-engine:** the real legend-engine and legend-lite both pointed at the warehouse (legend-engine through its Postgres connection type). Every request DataCube sends is recorded and replayed against both, and the responses diffed byte for byte; nondeterministic fields are masked, and each mask is named | the differential; a red row is never normalised |
| **V*** | Vendor bindings, one leg each: the binding (§2c) plus that vendor's dialect in legend-lite | per vendor, the SQL-API conformance suite and the corpus in that dialect |

**Production grade runs through every leg:**
- load and concurrency tests (N users, mixed statement sizes, queue
  behaviour);
- cancel and timeout tests;
- crash and restart with the WAL;
- backup and restore;
- TLS;
- the Windows lane.

## 5. Decisions (status)

| # | Decision | Status |
|---|---|---|
| 1 | Entitlements in the warehouse; the user's identity always; role views, no rewriting | **decided** (user) |
| 2 | One HTTP SQL API in legend-lite; vendors by Java bindings | **decided** (user) |
| 3 | Direct mode before the JDBC path | **decided** (user) |
| 4 | Warehouse as a Java module in this repo, separate process | recommended |
| 5 | PostgreSQL wire before Arrow Flight SQL | recommended |
| 6 | The warehouse's real name | open |
| 7 | Contract §4.3: the referee engine: build legend-engine 4.145.0 (the contract's source) vs pin to the 4.138.5 jar on this machine | recommended: build 4.145.0, before X1 |
| 8 | Contract §4.4: the query store's home | recommended: the warehouse (E2) |
| 9 | Contract §4.2: model pointers (Depot) | recommended: with E2 |
| 10 | Contract §4.1: code completion | recommended: match legend-engine (no endpoint) |
