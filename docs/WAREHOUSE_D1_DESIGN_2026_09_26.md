# D1: DataCube Direct mode — homework and plan (2026-09-26)

Direct mode (docs/SERVER_PROGRAM_2026_09_26.md §1c, leg D1): **the tab plans, the warehouse runs.**
The planner is the one DataCube already uses (legend-lite's, in WebAssembly); only the engine
changes, from DuckDB in the tab to the warehouse over the HTTP SQL API, as the signed-in user.

This replaces the first version of this document, which was written before the homework and
proposed a Java cell encoder that the facts below make unnecessary. Every fact here was measured
(probes kept out of the repository, in the session's scratch directory).

## Facts

**H1. Versions.** The page's DuckDB-WASM (`@duckdb/duckdb-wasm` 1.33.1-dev57) is DuckDB **v1.5.4**;
the warehouse runs **v1.5.5**. Arrow JS is `apache-arrow` 17.0.0, reached through DuckDB-WASM and
already inside the page's bundle (`demo/bundle.js` carries `RecordBatchReader`); the page does not
yet declare it as its own dependency.

**H2. Values: no converter anywhere.** The same SQL run by DuckDB-WASM and by the warehouse (its
Arrow chunks read by `apache-arrow`'s `tableFromIPC`), both handed to the page's own
`toResultTable` (datacube/src/duckdb.ts): identical Arrow schemas, identical cells, identical Pure
types, for every type probed — BOOLEAN, all integer widths, HUGEINT, UBIGINT, FLOAT, DOUBLE with
NaN and infinities, DECIMAL(9,2)/(18,2)/(38,10), DATE before 1970, TIMESTAMP (µs, ns, s, with time
zone, before 1970), TIME, INTERVAL, UUID, BLOB, VARCHAR, JSON, LIST, STRUCT, MAP, fixed arrays,
and a grouped aggregate. So Direct mode feeds the warehouse's Arrow to the code Local mode already
uses; cells agree by construction.

**H3. Every SQL the cube corpus plans, run by a warehouse READER** (granted only `TRADES`) and by
DuckDB-WASM over the same rows (44 cases, `//datacube:cube_jvm_answers`):

| | cases | what |
|---|---|---|
| identical cells | 36 | |
| same rows, different order | 2 | `children-*`: the outer query has no ORDER BY, so SQL promises none — compare as multisets |
| order-dependent | 2 | `window-row-level-*`: `ROWS` frames ordered by `year` alone; tied years may frame in either order, in either engine. The cube's own answer is order-dependent there. |
| **refused (FORBIDDEN)** | 4 | the pivots: legend-lite emits a **dynamic** `PIVOT` (no value list), which DuckDB expands into more than one statement, so the reader check cannot see one SELECT (W0 found the same) |

Pivots, measured on data with a NULL pivot key:

| SQL | rows | reader may run it |
|---|---|---|
| today's dynamic `PIVOT … ON "year"` | 5 | **no** |
| `SELECT DISTINCT year` (to learn the values) | | yes |
| legend-lite's valued pivot, upstream spelling `pivot(~year, [values], ~agg)`: static `IN` **plus a pre-filter** `WHERE list_contains([values], year)` (legend-engine's semantics) | **3** (groups whose key is NULL are dropped) | yes |
| static `ON "year" IN (every distinct value)`, **no pre-filter** | **5, identical to dynamic** | yes |

Also found: the cube's own `pivotValues` path writes `pivot(~[col], [values], ~[aggs])`, which
matches **no** overload, upstream or in legend-lite (upstream's valued form takes one column and
one aggregate). That path does not compile today.

**H4. The protocol in the module.** The SQL-API binding (`//warehouse:sqlapi`: submit, poll,
fetch, close, login, token, errors) compiled into the planner's module adds **44.5 KB raw, 14.8
KB gzipped** (~1%) to a 4.45 MB (1.56 MB gzipped) module.

**H5. The browser and the warehouse.** The warehouse answers no CORS preflight and sends no
`Access-Control-*` header, so a page on another origin cannot call it. Tokens live 1 hour (the
server's default); there is no refresh.

**H6. Sources.** The catalog call (`GET /sql/v1/catalogs/{c}/objects`, filtered by W2's grants)
returns `{schema, name, kind, columns: [{name, type}]}` with DuckDB's type names, which is what
`inferModel` (infer.ts) already turns into a model for an uploaded file. Nested columns (STRUCT,
LIST, MAP) map to `VARCHAR(4096)` there today; JSON columns as Variant live on
`feature/datacube-variant`, not on main.

**H7. Proof inside Bazel.** DataCube's browser harnesses (`verify-*`) are `js_binary` targets run
by hand, not tests. DuckDB-WASM and the WASM planner already run inside `js_test`s, and the
warehouse is a Bazel-built native binary (`//warehouse:server_native`), so a mode differential can
be an ordinary `js_test` in `bazel test //...` with no JVM in it.

## Plan

**Decisions needed first (the user's):**

1. **Arrow JS as the page's own dependency** (`apache-arrow` 17.0.0 in datacube/package.json). It
   ships already, inside DuckDB-WASM; declaring it adds no bytes. Recommended: yes.
2. **Pivots in Direct mode.** The only correct reader-runnable form (static `IN` of every
   distinct value, no pre-filter) is one the planner does not emit, and the planner is the
   untangle's area. Recommended: D1 ships with pivots **refused in Direct mode with a message
   that says so**, and the planner form (plus the cube's distinct-values pre-query, and the
   broken `pivotValues` spelling) goes to the untangle's queue as a written request.
3. **Browser access.** Recommended: `--allow-origin ORIGIN` on the warehouse (repeatable, exact
   origins, no wildcard, preflight answered), rather than serving the page from the warehouse.

**Stages, each landed green before the next:**

- **D1a — the protocol in the module** (a cross-area edit to `wasm/`, announced first): a boundary
  class beside `planner.Wasm` exporting the binding's steps (login, submit, next, fetch chunk,
  close, cancel, catalog objects), each answering one JSON string; the chunk's bytes are not
  decoded in Java. The module differential (the planner differential's pattern) holds the exports
  equal on the JVM and in WebAssembly over recorded responses.
- **D1b — the warehouse:** `--allow-origin` and preflight; the catalog call added to the binding
  (both directions), tested through the server.
- **D1c — the page:** `WarehouseEngine` implements `QueryEngine`: the planner's worker drives the
  protocol through the module's exports and `fetch`; each Arrow chunk goes through `tableFromIPC`
  into the existing `toResultTable`. `signal` cancels the statement. Sign-in (token in memory
  only), and a warehouse source in the source picker: the catalog call's objects through
  `inferModel`.
- **D1d — the mode differential,** a `js_test` in `//...`: starts `//warehouse:server_native`,
  loads the cube corpus's data as an owner, grants it to a reader, and runs every cube case
  through Local and Direct as that reader. Ordered cases compare exactly; cases without an order
  compare as multisets; the corpus data has no ties on a window's order keys (so no case is
  order-dependent by accident); pivots are expected refusals until decision 2's planner leg lands.
- **D1e — the browser check:** the demo in Direct mode against a running warehouse (sign in, pick
  a table, the grid fills, a denied table says so), in the existing harness pattern.

**Not in D1:** JSON/Variant columns (the variant branch), the static-pivot planner form (the
untangle's area, decision 2), re-recording the native-image metadata as a Bazel target (owed from
the native-build fix).
