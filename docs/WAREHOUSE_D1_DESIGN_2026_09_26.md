# D1: DataCube Direct mode (2026-09-26)

Direct mode (docs/SERVER_PROGRAM_2026_09_26.md §1c, leg D1): **the tab plans, the warehouse
runs.** The planner is the one DataCube already uses (legend-lite's, compiled to WebAssembly);
only the engine changes, from DuckDB in the tab to the warehouse over the HTTP SQL API, as the
signed-in user. Entitlements hold because the warehouse enforces them (W2): the browser sends raw
SQL and the warehouse allows or denies it.

## What exists, and the one seam D1 uses

- `PlanThenRun(planner, engine)` (datacube/src/runner.ts) is how both browser planes run a cube:
  the planner turns Pure into SQL, a `QueryEngine` turns SQL into a `ResultTable`.
- `QueryEngine` (datacube/src/engine.ts) was written for exactly this: "SQL run against a
  warehouse on a server, or against DuckDB in the browser. Both receive the SAME SQL."
- So Direct mode is `PlanThenRun(the WASM planner, WarehouseEngine)`. Nothing above the engine
  changes: tree assembly, the SQL panel, snapping, and every feature see the same planner and the
  same `ResultTable`.

## The client is the Java binding, in the module (ruling 7)

The SQL-API client is **not** written again in TypeScript. `//warehouse:sqlapi`'s
`SqlApiBinding` (a sans-I/O state machine: it says which HTTP call to make, and reads what came
back) and its decoding rules (`Columnar`, `ApiValues`, `Intervals`, `ArrowIpcReader`) compile
into the planner's WebAssembly module, beside the planner. The TypeScript side is a driver: it
performs the HTTP calls the binding asks for (`fetch`) and nothing else. The JDBC driver and the
server's own tests already run the same binding on the JVM.

- **Exports** (a boundary class in `//wasm`, beside `planner.Wasm`): `login(user, password)` →
  the call; `token(status, body)` → the token or an error; `submit(sql, catalog, token)` → the
  call; `next(status, body, token)` → a step (poll: the next call; done: the status; failed: the
  error); `fetchChunk(id, index, token)` → the call; `decodeChunk(status, bytes)` → the chunk's
  columns for the grid; `close(id, token)` → the call. Every answer is one string (JSON), the
  planner's existing convention (`planOrError`); bytes go in as a typed array.
- **Where it runs:** in the planner's worker, which already holds the module. A query's HTTP
  calls and decoding happen off the main thread; the main thread gets finished columns by
  `postMessage`, as it gets SQL today.
- **Format: Arrow.** The chunk is decoded by `ArrowIpcReader` in the module (one pass over
  bytes), then written once as the grid's JSON. JSON chunks would be parsed twice (the
  server's JSON by the binding, then the grid's JSON by the page).

## Values: the same cells as the local plane

The mode differential holds Local and Direct to identical rows, so the cells Direct produces
follow the local plane's rules (`toScalar`, `decimalToScalar`, `dateOnly` in duckdb.ts), decided
in Java, written as the grid's JSON:

| Column | Cell |
|---|---|
| integers | a number when within ±(2^53 − 1), else the exact digits as a string |
| DECIMAL | a number when the unscaled value is within ±(2^53 − 1), else the exact decimal string |
| FLOAT, DOUBLE | a number |
| BOOLEAN, VARCHAR | as is |
| DATE | `YYYY-MM-DD`; the page makes the local-midnight `Date` the local plane makes |
| TIMESTAMP | epoch milliseconds; the page makes the `Date` |
| nested | JSON text |
| NULL | null |

Each column carries its Pure type in the local plane's vocabulary (`String`, `Boolean`,
`Integer`, `Float`, `Decimal`, `StrictDate`, `DateTime`, `Unknown`), read from the warehouse's
column type by the generated `PURE_KIND_BY_SQL_NAME` facts where they reach, never guessed. (The
result.ts note stands: the plan should carry its types across the boundary, which would delete
every one of these converters. D1 adds no second vocabulary.)

## A warehouse source

- **Sign-in:** the source is a warehouse URL, a user and a password. `login` returns a token;
  the page keeps it in memory only (never storage), and every call carries it. A 401 asks for
  sign-in again.
- **Picking a table:** the catalog call (`GET /sql/v1/catalogs/{c}/objects`) lists what the user
  may read (W2 filters it). Its columns are `{name, type}` in DuckDB's names, which is what
  `inferModel` (infer.ts) already turns into a model for an uploaded file. So a warehouse table
  becomes an ordinary model over an ordinary table, exactly as an upload does.
- **CORS:** the warehouse answers the page's origin when started with `--allow-origin ORIGIN`
  (repeatable; none by default): preflight `OPTIONS`, `Access-Control-Allow-Origin` for that
  origin only, `Authorization` allowed. No wildcard.

## Proof

1. **JVM tests of the cells** (`//warehouse:tests`): every row of the value table, including the
   2^53 edges, negative decimals, dates before 1970, NULLs in every type.
2. **The module differential** (`//wasm`): the new exports answer recorded server responses
   (bytes) identically on the JVM and in WebAssembly, the planner differential's pattern.
3. **The mode differential** (datacube, Node): start the warehouse, load the demo's trades data
   as an owner, grant it to a reader, then run every cube case through Local (DuckDB-WASM) and
   Direct (the warehouse, as the reader, through the module's client) and compare rows: in order
   where the case sorts, as a multiset where it does not (verify-engine-differential.mjs's rule).
4. **The browser check:** the demo page in Direct mode against a running warehouse, driven by the
   existing browser harness: sign in, pick the table, the grid fills, a denied table says so.

## Stages

- **D1a** Java cells: `BrowserCells` in `:sqlapi` (Columnar → the grid's JSON) + tests.
- **D1b** the module: the client exports in `//wasm` (a cross-area edit, announced in
  docs/IN_FLIGHT.md first), the module differential.
- **D1c** the warehouse: `--allow-origin`.
- **D1d** the page: `WarehouseEngine`, sign-in, the warehouse source in the source picker.
- **D1e** the mode differential and the browser check.
