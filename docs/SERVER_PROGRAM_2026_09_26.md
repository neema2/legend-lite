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
  - the identity as a server-registered function (`authenticated_user()`), never a variable;
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
**defined once in legend-lite**, as Java records. Its client is the
same Java, compiled into the WebAssembly module for the browser (§2c).
It is **served natively** by the warehouse,
and **by binding** in the legend-lite gateway.

| Call | Does |
|---|---|
| `POST /sql/v1/statements` `{sql, catalog?, schema?, parameters?, timeoutMs?, resultFormat: arrow\|json, maxRowsPerChunk?}` | submit; returns `{statementId, state}`, or the whole result when it finishes within a short wait |
| `GET /sql/v1/statements/{id}` | state (`queued`, `running`, `succeeded`, `failed`, `cancelled`), result metadata (columns and types, row count, chunk list), error |
| `GET /sql/v1/statements/{id}/chunks/{n}` | one result chunk: Arrow IPC stream, or JSON rows |
| `POST /sql/v1/statements/{id}/cancel` | cancel |
| `POST /sql/v1/sessions` / `DELETE …/{id}` | a session: one connection belonging to the user, for `search_path`, temp tables and transactions; optional |
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

**W2 as built (2026-09-26): lockdown, owners and readers, SELECT grants.** The user cut the plan
below to the simplest thing that holds: *deploy tables, views and table functions, and let users
query them if they have grants*. What exists:

- **Lockdown, for everyone.** Every database the server opens (each catalog, and its own
  `system.duckdb`) is set `allowed_directories = [<data>/import]` and then
  `enable_external_access = false`, which DuckDB will not turn back on. No file outside the import
  directory, no URL, no `ATTACH` of a file, no `COPY … TO` a file, no `INSTALL`/`LOAD`.
  `lock_configuration` is not used: it also blocks `SET TimeZone`, which sessions need. (Measured:
  `ATTACH ':memory:'` still works, so `//spec:corpus_warehouse` is unchanged, pass 2,474 / fail 107.)
- **Owners and readers.** `--owner NAME` (repeatable) names the users who may do anything:
  DDL, DML, grants, loading from the import directory. Everyone else is a **reader**. The server
  runs its own internal statements as the reserved principal `warehouse`, which is an owner.
- **A reader runs one SELECT**, checked before it runs by `Authorizer` (allow or deny, never
  rewrite), which walks DuckDB's own parse (`json_serialize_sql`, which refuses anything but
  SELECT):
  - every table or view it names, every table function it calls, and every function that is not
    one of DuckDB's own (a deployed macro) must be **granted** to the reader or one of its roles;
  - `range`, `generate_series` and `unnest` need no grant; `query()`/`query_table()` never (they
    read a table named in a string); `sleep_ms`, `pg_sleep`, `nextval`, `currval`, `setseed`,
    `current_setting` and `write_log` never;
  - CTE names are scoped the way DuckDB binds them: a CTE's own body does not see its name, and in
    a recursive CTE only the recursive term does. **Found while testing:** in
    `WITH RECURSIVE secret AS (SELECT * FROM secret UNION …)` DuckDB binds the anchor's `secret`
    to the base table, so a check that put the name in scope for the whole CTE would have let a
    reader read it;
  - only kinds the check knows pass (query nodes, table references, modifiers, sort directions,
    type details); anything else (`SHOW`, a kind a later DuckDB adds) is denied.
  - Denied is `FORBIDDEN`; SQL DuckDB cannot parse is `SQL_PARSE`. `describeOnly` is checked the
    same way.
- **Grants**, in Postgres's spelling, parsed by the server (`AdminStatements`), owners only, kept
  in the system database's `security_roles`, `security_members` and `security_grants` (no catalog
  can reach them):
  `CREATE ROLE r`, `DROP ROLE r`, `GRANT r TO user_or_role`, `REVOKE r FROM …`,
  `GRANT SELECT ON [TABLE|VIEW|FUNCTION] [[catalog.]schema.]name TO grantee`,
  `GRANT SELECT ON SCHEMA [catalog.]schema TO grantee` (everything in it), `REVOKE SELECT … FROM`,
  `SHOW GRANTS` (a result like any other).
- **What a grant gives:** a view or table macro runs with its owner's rights, as in Postgres, so a
  reader granted `sales.v_orders` reads through it without any grant on `sales.orders`. Row
  filtering is a view over `system.main.authenticated_user()` (0b below; pinned by
  `aViewCanFilterRowsByWhoIsAsking`).
- **The catalog API** (`/sql/v1/catalogs/{c}/objects`) is read by the server and filtered: an owner
  sees everything, a reader what it may SELECT.
- **Proof:** `WarehouseEntitlementsTest`, run in process and against the native binary: 23 statements
  a reader must not run (base tables through subqueries, CTEs, the CTE-name tricks, lateral, set
  operations, scalar subqueries, other catalogs, `duckdb_tables()`, `query()`, file paths, an
  ungranted macro), 19 that are not SELECT, and 15 ordinary SELECTs that must pass (casts, sorted
  windows, QUALIFY, GROUPING SETS, ASOF and positional joins, UNPIVOT, lambdas, FROM-first).

**Not built, and why:** USAGE/EXECUTE as separate privileges (SELECT covers "may read this
object"), `CREATE USER` (users come from `--user`), PUBLIC, role schemas with `search_path`, and
`information_schema` filtering (readers cannot query `information_schema` at all; the catalog API
is the filtered view). Each is a later leg if a use needs it. The numbered plan below is the
original design, kept for the reasoning.

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
0b. **An identity nothing can spoof** (revised 2026-09-26, measured:
   docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §5). The server talks to
   DuckDB through its C API and registers `system.main.authenticated_user()`.
   DuckDB calls its bind once per query, on the calling query's own
   context; the context gives the connection id, and the server's own map
   gives the principal. It lives nowhere SQL can write:
   - the principal comes **only** from the verified token; a client never
     sends a user name;
   - `SET VARIABLE app_user = …` and a `TEMP MACRO` over the function's
     name both **failed** to spoof it, where the earlier design (a
     variable the server re-set before every statement) was spoofed by
     both, and by a temp macro over `getvariable`;
   - views call it **fully qualified**: a user's temp macro shadows any
     unqualified name (`current_user` and `getvariable` included);
   - a connection the server never mapped **fails the query** at bind;
   - `current_user` and `session_user` are per-connection temp macros
     over it, a display convenience no view reads. DuckDB's own are
     internal macros returning `'duckdb'` and cannot be replaced.
   - Still true: end users get no DDL (a temp view shadows a real one for
     that connection too), and the authorizer allow-lists what runs.
   - **No identity extension (decided with the user, 2026-09-26):** none
     was needed; the function lives in the server.
1. **Roles and grants, emulated Postgres-style:**
   - **Privileges:** SELECT on tables and views; USAGE on catalogs and
     schemas; EXECUTE on functions, macros and table functions; roles
     granted to roles; PUBLIC; ownership; REVOKE; `SHOW GRANTS`.
   - **Admin statements** (`CREATE ROLE`, `CREATE USER`, `GRANT`,
     `REVOKE`) are parsed by the server itself, since DuckDB does not
     know them, in Postgres syntax so pgwire tools work. They are stored
     in `security.*`.
   - **Checks:** every object a statement references needs SELECT, and
     every function it calls needs EXECUTE. Pure built-ins are granted
     to PUBLIC; file and system functions (`read_parquet`,
     `duckdb_tables()`, …) are not.
   - **Views run with their owner's rights, as in Postgres:** a user
     needs SELECT on the view, not on the base tables beneath it. The
     base tables are granted to no end-user role.
   - **Metadata is filtered by grants:** the catalog API and
     `information_schema` show only what the caller may see.
1b. **ACL tables:** `security.acl(username, <key>)`, one row per value a
   user may see, at whatever granularity, e.g.
   ```sql
   CREATE VIEW sales.trades AS SELECT t.* FROM base.trades t
   WHERE EXISTS (SELECT 1 FROM security.acl a
                 WHERE a.username = system.main.authenticated_user()
                   AND a.region = t.region);
   ```
   Several keys mean several `EXISTS` clauses, and masks are
   expressions in the view's `SELECT`.

   **EXISTS, never a straight join** (measured): a user with EMEA twice
   in the ACL, directly and through a group, got 4 rows totalling 30.0
   through a join, but 2 rows totalling 15.0 through EXISTS, which is
   correct. A join multiplies rows whenever a user matches more than one
   ACL row. DuckDB plans EXISTS as a hash **semi join**, and the user's
   own filter still pushes down into the base scan.

   **"Sees everything"** is its own table,
   `security.full_access(username, key)`, joined with `OR EXISTS`, not
   a `'*'` value in the ACL: a wildcard inside the join condition blocks
   the hash plan.
2. **Role-level views:** a role's schema holds views over the base data.
   - **Rows:** `WHERE region = 'EMEA'`, or per user:
     `WHERE region IN (SELECT region FROM entitlements.user_regions
     WHERE user_id = system.main.authenticated_user())`.
   - **Columns:** included or not.
   - **Masks:** an expression, e.g. `'***' AS account_no` or a
     hash.
3. **Name resolution, not rewriting:** a session's `search_path`
   starts at its role's schema, so the `TRADES` in legend-lite's SQL
   resolves to that role's view of trades. The statement text is never
   edited.
4. **Identity from the server, not variables:** the user comes from
   `system.main.authenticated_user()` (0b); the active roles and user
   attributes are further functions of the same kind, owed with W2.
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
| **W2** | Entitlements: lockdown, owners and readers, SELECT grants, row views over `authenticated_user()`, the authorizer (§3, **built 2026-09-26**) | the entitlement differential; the adversarial deny suite (`WarehouseEntitlementsTest`) |
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
