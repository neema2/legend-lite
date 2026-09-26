# Warehouse: DuckDB's C API through Java's FFM (homework, 2026-09-26)

**Question.** Can the warehouse server talk to DuckDB through DuckDB's public C API, using
Java's Foreign Function & Memory API (`java.lang.foreign`, standard since JDK 22),
instead of through JDBC? That route has no reflection and no new dependency: it calls the
native library DuckDB's JDBC jar already ships, which exports the whole C API. Does it
work as a GraalVM native image? Is it faster? And does it make entitlements stronger?

**Answer: yes on all counts, with one design rule for the native image (few C calls).**

Machine: Apple M4, macOS. DuckDB v1.5.5 (d8cdaa33fd), the library inside
`duckdb_jdbc-1.5.5.1.jar`. JDK 25.0.1 (Temurin), GraalVM CE 25.0.1. Every signature
used was checked against `duckdb.h` at v1.5.5. Probes: `experiments/warehouse-ffm/`
(outside the Bazel build).

## 1. Why not the JDBC routes

| Route | Result |
|---|---|
| `DuckDBReadableVector` (public, 1.5) | per-value getters only; DATE, TIMESTAMP and DECIMAL only as objects (`getInt`/`getLong` refused) |
| `DuckDBBindings.duckdb_vector_get_data` | a direct buffer over the vector, but **package-private** |
| `DuckDBResultSet.arrowExportStream` | uses reflection itself, and needs Apache Arrow Java |
| the nanoarrow extension (`to_arrow_ipc`) | an ENUM, even nested, **invalidates the database for every connection**; UHUGEINT's maximum comes back as −1 |

## 2. Speed: 1M rows × 8 columns into Arrow buffers

| Route | JVM | native image |
|---|---|---|
| JDBC `getObject` + our writer | ~650 ms, 747 MB garbage | — |
| nanoarrow `to_arrow_ipc` | ~45 ms | — |
| FFM, vector by vector (~13,000 C calls) | 21–26 ms | ~88 ms |
| **FFM, `duckdb_data_chunk_to_arrow` (~2,000 C calls)** | ~26 ms | **~45–49 ms** |

**The native-image rule.** An FFM call costs ~8 ns on the JVM and **~2.3 µs** in a
GraalVM CE 25 native image (1M calls to a function that returns a constant). A handle
made at build time does not build (`linkToNative` "should not reach here"), and
`-H:+ForeignAPISupport` changes nothing. So the design makes FEW calls:
`duckdb_data_chunk_to_arrow` turns a 2,048-row chunk into Arrow buffers in one call, and
Java then only bulk-copies buffers. Per-value loops over `MemorySegment` are also slow in
the native image (a DECIMAL column: 9 ms JVM, 51 ms native); bulk-copy into a `byte[]`
first, then loop over the array.

**Small queries** (per statement, one 5-row result): JDBC ~80 µs, FFM ~66 µs on the JVM,
FFM ~99 µs in the native image (~14 calls × 2.3 µs). DuckDB's own parse/plan/execute
dominates; the route matters for big results, not small ones.

## 3. Types: DuckDB's Arrow conversion, framed by us, read by pyarrow

`duckdb_to_arrow_schema` + `duckdb_data_chunk_to_arrow` produce standard Arrow for every
API type. Three are ours to decide:

- **UHUGEINT:** DuckDB writes `decimal128(38,0)`, which is wrong above 2^127: sent as its text.
- **ENUM:** DuckDB writes a dictionary: decoded to plain text.
- **TIMESTAMP WITH TIME ZONE:** DuckDB tags the SESSION's zone: tagged `UTC` (same instants).

Better than the hand-written writer: INTERVAL as Arrow's month-day-nano interval, and a
fixed array `T[n]` as `FixedSizeList`. Default settings, not `arrow_lossless_conversion`
(it makes HUGEINT and UUID opaque bytes).

**Proof** (`cdata/CDataIpc.java` + `cdata/check_types.py`): 40 streams (39 single types
with a NULL row each, including a nested ENUM inside a struct inside a list, BC dates,
NaN, −0.0, both 128-bit extremes, nanosecond timestamps; plus 5,000 rows across chunks)
framed as Arrow IPC with our `FlatBuilder`, read by **pyarrow**, and compared with DuckDB's
own answer for the same SQL: **0 differences.** Owed: a UHUGEINT nested in a list or struct
still comes through as a decimal (right up to 2^127); the text conversion must follow the
column's type tree.

## 4. The statement surface

Through the C API, on the JVM and identically in the native image (`ExecProbe`):

- **Errors:** message for message identical to DuckDB's JDBC driver, plus a typed kind
  (`PARSER`, `CATALOG`, `BINDER`, `CONVERSION`, `CONSTRAINT`, …) where JDBC gives a string.
  (JDBC 1.5.5.1's lost messages on `Statement.execute` do not exist on this route.)
- **Write counts:** INSERT 3, UPDATE 2, DDL 0 (`duckdb_rows_changed`).
- **Prepared statements:** parameter types and column names BEFORE running (the API's
  `describeOnly`), binding, execution; a bad statement fails at prepare with the real message.
- **Scripts:** `duckdb_extract_statements` splits with DuckDB's own parser; each statement
  runs in turn; a syntax error anywhere refuses the whole script before anything runs.
- **Cancel:** `duckdb_interrupt` from another thread stopped a huge query at 301 ms with
  `INTERRUPT`; the connection stayed usable.

## 5. Entitlements: the principal from outside SQL

A scalar function registered through the C API, implemented in Java (an FFM upcall).
Its **bind** callback runs once per query on the calling query's own client context
(`duckdb_scalar_function_get_client_context` takes the BIND info — measured: passing the
execute info gives garbage), reads the connection id, and looks the principal up in the
server's own map; the principal becomes the function's bind data, and the execute callback
writes it. A connection the server never mapped fails the query at bind ("no principal for
connection 5"): fail closed.

| Attack, from Bob's own SQL | `SET VARIABLE` + `getvariable` (the W1 design) | the C-API function |
|---|---|---|
| `SET VARIABLE app_user = 'alice'` | **spoofed** (Bob reads Alice's rows) | immune |
| `CREATE TEMP MACRO getvariable(x) AS 'alice'` | **spoofed** | — |
| `CREATE TEMP MACRO <fn>() AS 'alice'` | — | spoofed if the view calls it unqualified; **immune** as `system.main.<fn>()` |

Correct under DuckDB's worker threads (100 concurrent 1M-row scans, every answer the
caller's own), and identical in the native image (upcalls from worker threads included).

**The name.** DuckDB's `current_user`, `session_user` and `current_role` are internal
macros returning `'duckdb'`: registering a function under that name fails, `DROP MACRO`
refuses ("Cannot drop internal catalog entry"), and the system catalog cannot be written.
And even `current_user` itself is shadowed by a temp macro. So: the identity function has
its own name and views call it fully qualified (`system.main.<name>()`); the server installs
`current_user()` / `session_user()` as temp macros over it on each connection, a
convenience that only ever shows a user their own display (no view reads it).

**Rules this adds to W2:** every identity reference in a view is fully qualified; end users
get no DDL (no temp macros, no temp views: a temp view named like a real one shadows it for
that connection); the variable-based identity is retired.

## 6. Embedded legend-lite and the browser

- **Embedded core:** not worth moving now. Small statements gain ~18%; the corpus is
  ~11,700 small round trips (~0.2 s). H2 has no C API, so core would carry two executors.
  Both DuckDB copies (JDBC's and FFM's) coexist in one process.
- **Browser:** FFM does not apply (legend-lite's WASM module only plans; DataCube's
  TypeScript runs the SQL on duckdb-wasm and reads its Arrow). Arrow from the warehouse lets
  the browser read remote results through the same Arrow path as local ones.
