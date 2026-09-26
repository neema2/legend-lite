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
