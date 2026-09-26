# Warehouse W0 homework: measured (2026-09-26)

Leg W0 of `SERVER_PROGRAM_2026_09_26.md`. Each question is answered by a
probe and a number, not by expectation.

## Q1. Can the warehouse's server wrapper be Java, compiled native for on-demand start?

**Asked by the user:** should the wrapper around DuckDB be a native
language, so it can start on demand (e.g. AWS Lambda), or can Java get
there through a GraalVM native image with reflection and dynamic features
banned?

**The probe:** `experiments/warehouse-w0/Probe.java` (outside the Bazel build; `.bazelignore`) is the
wrapper's skeleton: DuckDB over JDBC, an HTTP `POST /sql/v1/statements`
taking SQL, a fresh DuckDB connection per request, `SET VARIABLE
app_user` from a header before every statement (the identity rule), and
JSON rows out. A `--once "<sql>"` mode starts, answers one query and
exits: the cold-start measure.

**Machine:** Apple M4, macOS; Temurin JDK 25.0.1; GraalVM CE 25.0.1
(native-image); DuckDB JDBC 1.4.4.0.

### Results

| Configuration | Process start → first result (wall) | In-process: open DB → first result | Memory (RSS) |
|---|---|---|---|
| JVM, DuckDB JDBC jar **as shipped** | **~1,000 ms** | ~995 ms | — |
| JVM, jar as shipped + library on `java.library.path` | ~1,000 ms (the driver ignores it) | ~975 ms | — |
| **JVM, jar without its native files + library on disk** | **~50 ms** | ~27 ms | 128 MB (server, after 550 requests) |
| **GraalVM native image**, same setup | **~10 ms** (240 ms on the very first run of a new binary) | ~9 ms | **29 MB** (one query); 79 MB (server, after 550 requests) |
| for scale: DuckDB's own CLI, `select 42` | ~10 ms | — | — |
| for scale: Python `import duckdb` + connect | ~50 ms (import 31 ms, connect 3 ms) | — | — |

**Warm HTTP request** (HTTP + a fresh DuckDB connection + the identity
`SET` + a trivial query, 500 requests after 50 warm-up):

| | p50 | p99 |
|---|---|---|
| JVM | 0.36 ms | 0.57 ms |
| native image | 0.30 ms | 0.40 ms |

The per-connection identity was checked: a request as `user2` read back
`user2`.

### What the second of cold start was

Not the JVM and not DuckDB. **The DuckDB JDBC jar ships its native
library inside itself** (106 MB for macOS, a two-architecture build; 51 to
57 MB per Linux architecture). On **every start** its loader
(`DuckDBNative.loadNativeLibrary`) unpacks that library to a **new temp
file** and loads it. It skips `System.loadLibrary` whenever the jar
contains the library ("There is no fallback… only the '-nolib' JAR can be
used with an external native lib"). Stack sampling during the first
`getConnection` showed inflating the jar entry, writing the file, then
`NativeLibraries.load` of a brand-new, unsigned 100 MB file. macOS verifies
a new file before loading it, and the file is new every time.

**The fix is packaging, not code:**
- deploy the driver's classes **without** its native files (DuckDB
  publishes a `-nolib` jar for this; the probe stripped them with
  `zip -d`);
- ship the platform's library once, beside the program, loaded by
  `System.loadLibrary`.

With that, the plain JVM starts to first result in about 50 ms.

### Native image, as built

- `native-image --no-fallback`: reachability metadata recorded by
  GraalVM's tracing agent from one run (9 KB of JNI and resource
  entries), plus `--initialize-at-run-time=org.duckdb`.
- The build took **18 s**, giving a **17.8 MB** executable plus the 51 MB
  arm64 DuckDB library.
- Nothing in the probe needed reflection beyond what the agent
  recorded for the driver's JNI callbacks.

### Answer so far

**Java is enough.** A native image of the Java wrapper starts and
answers in about 10 ms, the same as DuckDB's own C++ CLI. It warm-serves
a request in 0.3 ms and uses 29 MB for a single query. Even the plain JVM
is at about 50 ms once the library is packaged correctly. A wrapper in
another language would win nothing measurable here, and would cost the
shared Java code: the authorizer, the SQL-API records, and the bindings
that also compile to WebAssembly.

### For the production module (what the probe teaches)

- **The agent records only what the run exercised.** The probe touched
  integers, text, doubles and dates. DuckDB's C++ builds other Java
  objects (through JNI, by name) for decimals, time-zone timestamps,
  blobs, lists and structs, UUIDs, intervals, errors, the appender and
  prepared-statement metadata. Generate the reachability metadata from
  a thorough run, and check it in.
- **Test the native binary itself** in CI (the conformance suite and the
  corpus against the executable), so a missing entry is a red test, not
  a production failure.
- **One build per platform**, since native-image does not
  cross-compile: Linux x86_64/arm64 in containers, macOS, Windows. Each
  ships with the matching DuckDB library, loaded from disk (the
  `-nolib` driver).
- **Keep the no-reflection rule** for our own code: that is why our side
  needed no metadata; only the driver's JNI callbacks did.

### Published metadata (checked 2026-09-26)

- **DuckDB's own driver repo** now carries
  `META-INF/native-image/org.duckdb/duckdb_jdbc/reachability-metadata.json`
  inside the jar, so native-image picks it up with no agent. Related
  changes:
  - the metadata itself (duckdb-java PR #849, merged 2026-08-30);
  - a GraalVM CI job (#850);
  - a time-zone timestamp fix (#852).

  All merged after the latest release (1.5.5.1, 2026-08-03), so the
  metadata is **unreleased** and ships with the next one. It has 49
  entries to the probe's 45: UUID, maps, structs, arrays, time-zone
  timestamps and the timeout exception, which the probe never hit.
- **GraalVM's shared reachability-metadata repository has nothing for
  DuckDB.** A few projects keep their own copies.
- **"Load the library by name first"** (#421) was closed unmerged: the
  driver keeps unpacking a bundled library, so the `-nolib` driver stays
  the way to load from disk.
- **Plan:** take DuckDB's file from their repo now, pinned to a commit;
  switch to the one inside the jar when it is released; still test the
  native binary itself in CI.

### Correction: the Java driver now has host functions

DuckDB JDBC **1.5.x** has Java scalar and table functions
(`DuckDBScalarFunctionBuilder`, added 2026-04, released in 1.5.x; the
classes are in the 1.5.5.1 jar). The program doc's "the Java driver
cannot register a host function" held only for **1.4.4**, the version
legend-lite pins. The warehouse starts on 1.5.x. Upgrading legend-lite
is a separate change, with its own gate run.

**Open question for W0:** DuckDB registers functions for the whole
database, not per connection, and runs queries on its own worker
threads. Can a Java `current_principal()` tell which connection called
it? If it can, a setter-less identity is possible in pure Java, which
reopens the identity-extension decision on better terms.

**User, 2026-09-26: good enough to move on.** The Linux, Lambda and
cold-data items below stay open, measured when deployment is built.

### Still to measure before this is closed

1. **Linux**, which Lambda runs: the same probe as a native image built
   in a Linux container (x86_64 and arm64), cold start in the container,
   and on Lambda itself (a custom runtime), including Lambda's own init
   overhead. Docker was not running on this machine at the time of the
   macOS measurements.
2. **The JVM with Lambda SnapStart**, for comparison.
3. **Cold data:** the first query over Parquet on S3 (metadata plus data,
   empty caches) against the same query warm. This is expected to
   dominate a cold invocation whatever the language.
4. The probe's server with realistic results (thousands of rows, Arrow
   out) and concurrent requests.
5. The rest of W0: DuckDB's concurrency model under a server, the
   authorizer's parser coverage, views over external data with file
   access denied to users, Arrow IPC streaming from JDBC, what TeaVM's
   WebAssembly accepts for a sans-I/O binding, and the Windows lane.


## Q2. Can a Java function know which connection called it?

**No, measured on DuckDB JDBC 1.5.5.1**
(`experiments/warehouse-w0/UdfIdentity.java`):

- **Functions are database-wide.** A function registered through
  connection 1 is visible from connection 2. When connection 2
  registered the same name, connection 1 got connection 2's version.
- **A thread-local does not reach it.** Even `SELECT principal()` ran the
  Java function on a DuckDB worker thread (`Thread-0`), not on the thread
  that set the identity. The function failed closed ("no principal").

So identity stays a per-connection variable, set by the server before
every statement, with the authorizer as its guard. The no-extension
decision stands.

## Q3. Does DuckDB's parser expose every way a statement reaches data?

**The probe:** `experiments/warehouse-w0/authz_probe.py`. It runs 44
statements through `json_serialize_sql` against a naive rule: one
statement, every referenced table granted, no table functions.

**Caught (denied):**
- the base table reached through a subquery, a CTE, a recursive CTE, a
  CTE shadowing a granted name, a lateral join, a set operation, a
  scalar subquery, or FROM-first syntax;
- a file or S3 path used as a table, and another attached database;
- every table function: `read_csv`, `read_text`, `query('…')`,
  `query_table`, table macros, `duckdb_secrets()`, `duckdb_tables()`,
  `glob`, even `range`;
- two statements in one request;
- every non-SELECT, which cannot be serialized: `SET VARIABLE`,
  `PREPARE`/`EXECUTE`, `CALL`, `ATTACH`, `COPY`, `INSERT`,
  `CREATE TEMP MACRO`, `EXPLAIN`.

**Slipped through the naive rule, each a design rule:**

| Statement | Why | Rule |
|---|---|---|
| `SHOW TABLES` | parses as a SELECT over a special source node that the walker did not recognise; it lists every table | **allow-list node types**; deny any node the authorizer does not know |
| `SELECT current_setting('s3_secret_access_key')` | a scalar function reading server settings | **allow-list scalar functions** by name |
| `SELECT getenv('HOME')` | a scalar function reading the server's environment | same |

**What legitimate SQL uses:** every query in DataCube's planner
differential (44 cube shapes, including windows and child groups) was
catalogued.
- **Node kinds:** SELECT, subquery, base table, CASE, CAST, column,
  constant, comparisons, AND/OR, IN, COALESCE, IS [NOT] NULL, NOT, star,
  ORDER / LIMIT modifiers, and window nodes (aggregate, rank, ntile,
  lag, last value, cumulative distribution).
- **Functions:** `avg count count_star cume_dist lag last_value max
  median min ntile rank sum`.

These seed the allow-lists; each new kind is added deliberately.

**A blocker, with a fix on legend-lite's side:** 4 of the 44, the column
pivots, **cannot be serialized**. legend-lite emits a **dynamic** DuckDB
`PIVOT` (`ON "year"` with no value list), which needs a pre-pass over the
data, and the parser refuses it. A **static** pivot (`ON "year" IN (2021,
2022)`, or the standard `FROM t PIVOT (… FOR year IN (…))`) serializes,
exposes its tables, and answers correctly. So for the warehouse,
legend-lite fetches the pivot values first with a plain `SELECT
DISTINCT`, itself authorized, and sends a static pivot. This is a dialect
rule, and the authorizer stays strict.

(Measured with DuckDB 1.4.4's parser via Python; W1 re-runs the suite on
the warehouse's 1.5.x, as a test.)
