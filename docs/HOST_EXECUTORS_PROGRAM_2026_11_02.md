# The host executors: one core embedded in every host, Arrow between the pieces, thin host shells -- in Java, TypeScript and Python

**Status: rev 3, 2026-11-02, the clean-sheet design, ratified by the user that day** ("Yes I love it"). Rev 1's five
rulings (D1-D5) stand. Rev 3 replaces rev 2's principles, its proposed decisions (D6-D10) and its roadmap with the
clean-sheet design's (§1, §3, §5); rev 2 is in git history (`33c915d2`). A change to a principle or a decision is a
decision, recorded here with its date and reason, never an edit in passing. Status lives in §8.

The user's words that started it (2026-11-02): *"Let's start the typescript and python executors, let's look at how to
make those executors similar to the java executor in design as well if you think that makes sense? And for both let's
see if we can also add calling snowflake as remote engine too not just duckdb/warehouse - for TS we should use snowflake
http sql API the snowflake people have told me they do support cors even though not documented, and for python make sure
we use whatever gives arrow natively so it's fast."*

And the step back, the same day: *"take a giant step back to know what we want to achieve, fast and safe execution but
more importantly good clean design and consistency between java python TS/browser - so maybe I've been pushing the
performance angle too much, what is the right roadmap unbiased let's write down full roadmap and then execute on it
expertly."*

**How rev 3 came about (2026-11-02).** Rev 2 had each host hand its rows back to Java's writers as text. The user:
*"My main worry is that now each host also has a dependency on the java executor which is different from the
compiler/planner which is not going to work."* A one-way design followed, with every output written by each host on its
own. The user again: *"Has this gone to far the other way? If there is a java speller that makes sense to embed into jvm
wasm and native and that makes everything nice and fast maybe we should use that? ... I think instead of black or white we
should step back and think what is the right architecture and the right thing to do to make everything fast and correct
both"*, then *"Clean sheet how would you do what we want to do as world's expert? Starting from scratch no bias"*. Two
facts read from the code settled it:

- **The writers are already a library apart from the executor, and already in every host.** `//core:result` (`CellText`
  and the JSON, PureV1, CSV and Arrow writers) depends on no JDBC, no session and no planner; `//core:exec` depends on it,
  not the reverse. `//core:plan_side` includes it, so the tab's WebAssembly module and Python's native library carry it
  (the tab's JSON answer is `PureV1Api.jsonAnswer`, `wasm/src/main/java/planner/Boundary.java`).
- **The slow part is the handoff, not where the writer lives.** The embedded boundary carried strings only (until R3), so each host
  spells every cell as text, and Java reads the text back and spells it again: Python's JSON execute takes 23.2 s at
  1,000,000 rows against its Arrow execute's 188 ms (GATES, H0).

A scratch probe then ran Java's Arrow writer in both embedded builds on the same plan and rows: the answers read back
whole, identical to each other but for the trace id (§3.6).

## 0. What we want, in order

1. **A clean design, the same in Java, Python and the browser.** One plan, one set of rules, one result, one owner of
   each spelling. A host differs from another only in how it reaches its database.
2. **Safe execution.** The same plan on the same data answers the same values, and is refused the same way, on every
   host. Nothing defaults silently (AGENTS.md invariant 4). Consistency is tested, never assumed.
3. **Fast execution.** It follows from the design (columnar end to end, nothing spelled before it is read). It is
   measured at every step, and a regression is caught by a budget, not found by a user.

**Where the system stands.** The planner is already one piece of code: Java, compiled to the JVM, to WebAssembly (the
tab) and to a native library (Python). Its three builds are held to each other by tests that compare their plans with
the JVM's (`//wasm:differential_test`, `//python:bindings_test`, over `//wasm:corpus`). Execution is where the three
drift apart:

- the tab reads three plan shapes, and Python has no runner;
- each host decodes and spells values its own way (the tab spells every decimal and timestamp as text when its rows
  arrive);
- no test compares what the hosts answer when they execute (§2).

So the program is about the hosts' execution. The planner stays as it is.

Java runs a query one way (`docs/PLAN_AND_RESULT_MODEL_2026_10_18.md`):

- the planner writes one plan record (`executionplan.ExecutionPlan`);
- one runner (`exec.PlanRunner`) reviews it whole and runs it on a session it opens through one seam;
- the cells are decoded once, by the plan's own column facts (`exec.JdbcDecode`);
- the writers at the boundary spell them, one owner of every spelling (`result.CellText`).

The hosts take that design in their own grain. JDBC hands Java one row at a time, so Java's batches are rows. Every
database the tab and Python reach hands them Arrow, which is columnar, so the hosts stay columnar end to end.

## 1. Principles and rulings

### 1.1 The principles (rev 3)

This is how DuckDB, Polars and Arrow's database drivers (ADBC) are built: one portable core, data as Arrow, thin
bindings per language.

- **P1. One core, written once, embedded in every host, doing no input or output.** The planner (a model, a query and
  the host's profile, to a plan) and the writers (a plan and Arrow batches, to bytes) are Java, compiled to the JVM, to
  WebAssembly (the tab) and to a native library (Python). They open no connection, wait on no network, keep nothing
  between calls and never call back into a host. Every decision is the core's: a type, a plan, a refusal, a value's
  spelling in any output. That is the repository's tenet ("Java orchestrates, the database executes";
  `docs/WORLD_MAP.md`'s "compare, never compute") applied to the hosts.
- **P2. One plan kind, a straight line.** The plan record (`legend-lite-plan`, `PlanJson.VERSION`) is the only thing a
  host runs, for every path: a grid, an execute, a write, an export. A host's plan has every decision already made: no
  branch and no step that depends on data. Whatever depends on data is SQL, so the database decides.
- **P3. Arrow between the pieces, a batch at a time.** Never one cell at a time, and never text until an output that is
  text asks for it. The answer's Arrow is in the plan's types on every host, the schema Java's `ArrowWriter` writes:

  | Pure type | Arrow |
  |---|---|
  | integers | `Int64` |
  | floats | `Float64` |
  | `Decimal` | `Decimal128(p, s)` |
  | `Boolean` | `Bool` |
  | `String` | `Utf8` |
  | `StrictDate` | `Date32` |
  | `DateTime` | `Timestamp(NANOSECOND, UTC)` |
  | `StrictTime` | `Time64` |
  | nested values | lists, structs and maps |
- **P4. Thin host shells: all input and output native, no decisions.** Each host has one runner (carriage: the plan's
  statements in order), connectors (SQL in, Arrow out, over the host's own drivers) and its native consumers (the grid,
  a dataframe, an HTTP answer).
- **P5. One implementation of each output.**
  - **Wire text and the Arrow answer** (legend-engine's JSON and TDS, lite's JSON rows, PureV1, CSV, `ARROW_IPC`): the
    core's writers, on every host.
  - **The text on screen:** DataCube's formatter, from the typed value, for the cells on screen. The grid renders only
    in a browser, so it has one implementation.
  - **A value's exact text in the tab** (the literal a filter writes back, a pivot's column name, a saved document's
    value, an equality key): `engine-client/src/values.ts`, held to the core's spelling of the same value by the
    conformance suite.
- **P6. Consistency is tested.** One conformance suite runs the same cases on every host, every database and every
  output, against the JVM's answers. A difference is a defect, or a row of `docs/SEMANTICS_REGISTER.md` the user ruled.
  Its ledger only shrinks.
- **P7. Safe by construction.** No silent default (AGENTS.md invariant 4). A refusal is named, and the same on every
  host. No output's spelling changes without the user's word, asked case by case.
- **P8. Fast by design, held by budgets.** An answer costs the database's time, one pass over the Arrow for a text
  output, and at most one copy across a boundary. Each host's benchmarks are kept, with budgets, and an interaction is
  measured end to end: plan, SQL, decode, draw.

### 1.2 The rulings (the user, 2026-11-02)

Rev 1's (ratified with the user):

- **D1. The hosts' result is Arrow, columnar.** TypeScript's and Python's runners answer Arrow record batches, not Java's
  row batches.
- **D2. Text stays Java's.** JSON rows, PureV1 and CSV text is written by Java's writers (`CellText`), reached through the
  WebAssembly planner in the tab and the native library in Python. The cost of handing the rows over is measured before
  the handoff changes. (Rev 3: stands; the rows reach the writers as Arrow, D8.)
- **D3. Snowflake authentication.** In the browser: OAuth or a programmatic access token, never a private key. In
  Python: key-pair, OAuth or a programmatic access token.
- **D4. The tab's Snowflake session goes direct, over CORS** (undocumented; the user has it from Snowflake), and the same
  session code can go through a proxy instead (the warehouse server, or Python's engine) should CORS be withdrawn.
- **D5. Python's Snowflake client is ADBC** (`adbc-driver-snowflake`), in a `snowflake` extra of its own.

Rev 3's, the clean-sheet design (ratified by the user, "Yes I love it"):

- **D6. What a host can run is decided at planning.** The host gives the planner its profile (the database types it
  reaches, the outputs it writes); the planner plans only what that host can run, or refuses at planning, by name, the
  same on every host. A host's runner checks the plan's format and version and refuses an unknown step by name; it
  repeats none of `PlanRunner`'s review. (Rev 2 proposed the review asked of the embedded planner at run time; a
  straight-line plan makes it planning's.)
- **D7. The answer's Arrow is in the plan's types on every host (P3).** Python's Arrow answers keep DuckDB's own types
  today (`frames.py` `typed_by_plan`; fourteen ledger entries). Each change to an answer an existing path gives (an
  `Int32` widened to `Int64`, microseconds to nanoseconds in UTC, a computed decimal's precision) is posted to the user
  case by case before it lands.
- **D8. A host hands rows to the core's writers as Arrow bytes**, across a boundary that carries bytes. The reader is the
  core's own, beside the hand-written writer (`//arrow_ipc`, no Arrow library in core). It retires the text handoffs
  (`hostRows`, `compiler.host_rows`) and the tab's own lite-JSON writer for a write's answer (`plan-runner.ts`
  `rowsOf`). The handoff's options are measured on both hosts and posted before it is built.
- **D9. The tab's result model is typed columns over the Arrow**, with the exact text made only when it is asked for
  (§3.6a).
- **D10. The conformance suite starts from today's behaviour.** Its first run recorded every difference as a ledger
  (R1, built). An entry goes by a fix, or becomes a register row the user rules. Nothing is "fixed" by changing an
  expected answer.
- **D11. One plan kind (P2).** The tab's `Plan {sql, columns}` and `ExecutePlan`, and Python's `{sql, type}` and
  `{sql, metadata}`, retire into the plan record. legend-engine's own plan (`UpstreamPlanner`, read from an upstream
  server's `generatePlan`) is another planner's, and stays.
- **D12. The runner is carriage.** A step that depends on data is expressed in the plan -- the `Keyed` node: a keys
  statement whose rows the body statement's per-key regions are repeated for (D18 D, 2026-11-03: it is carriage
  already, so no stepper beside it) -- and every runner carries it; no host grows logic of its own.
- **D13. Connectors: SQL and its bound values in, Arrow batches out,** over each host's native driver: DuckDB in all
  three; ADBC for Snowflake in Python (D5); Snowflake's SQL API in the tab (D4), its text turned into the plan's Arrow at
  the connector; JDBC on the JVM.
- **D14. The core stays Java.** From zero, Rust would be the choice (small WebAssembly, memory shared with JavaScript and
  Python without a copy). The shape, not the language, is what makes it fast and correct, and the core exists, held
  identical across three builds. Java's costs, accepted: the bundle's size, and one copy into the tab's WebAssembly memory
  for a text answer (the grid sends no data into the core).
- **D15. The JVM keeps `PlanRunner` as the reference.** Test bodies stay on the JVM (`Execution.runProgram`).

R2's (the user, 2026-11-02):

- **D16. The plan's columns gain one fact: whether the compiler typed the column with an enumeration.** The user
  asked what R2 adds, since the plan already carries each column's Pure type, SQL type and store type. The answer:
  legend-engine's answer header names an enumeration column `String` (`VARCHAR(1024)`), and in the plan an
  enumeration's type is its path, which a reader cannot tell from a variant's or a class's path without guessing. That
  bit is the one fact the plan lacked. A precision decimal's plain name (the header's, and the hosts' type tables',
  `Decimal`) is read from its Pure type by one rule: `Decimal(p,s)` is `Decimal`, its precision and scale the SQL
  type's (`ExecutionPlan.Column.plainType`). A first cut that added the plain name as a second field repeated the Pure
  type and was withdrawn at the user's word.
- **D17. Lite's own Arrow is the plan's types; legend-engine's formats are written at the edge, exactly as
  legend-engine writes them** (the user, 2026-11-03: "arrow for us should be super clean and then legend
  arrow/json/csv for engine at the edges for compatibility"). Two kinds of output, each implemented once, in the core:
  - **lite's own:** the Arrow the pieces pass inside every host and lite's own answers (Python's `Table.execute`,
    plan-and-run, the grid) carry the plan's types (P3): lossless, the same on every database;
  - **legend-engine's, for its clients:** the TDS JSON (as today), the `ARROW_IPC` execute and the CSV are
    compatibility writers that reproduce legend-engine's answer exactly. The `ARROW_IPC` writer types each column as
    legend-engine does, from the database's reported type (§1.3 A), which travels with each column as a carried fact
    (JDBC's metadata on the JVM, the connector's own Arrow schema on a host).

  It settles §1.3 A and register S125 toward convergence: the JVM's `ARROW_IPC` answer changes to legend-engine's
  schema, Python's barely moves, and Python's own dataframes move to the plan's types; each concrete change is posted
  to the user case by case before it lands (D7). Open: whether the edge writer also copies legend-engine's data loss
  where a driver misreports a precision (H2's computed sum as `decimal(27,0)`), recommended copied with a register row.
- **D18. Clean inside, legend-engine replicated at the edges, for every open decision** (the user, 2026-11-03: "in the
  clean version we should do the clean thing, in the edges we should replicate engine"; then, on the refinements and
  D below, "Yes do it, keep going don't stop"). §1.3's decisions settle so:
  - **the edge is the shape and content of an answer to legend-engine's API** (its status, envelope, error kind, value
    spellings, types and header), never text that describes legend-engine's own internals (its compiler's pass
    names);
  - **where legend-engine refuses what lite does** (an uncast dynamic pivot on H2), lite's answer is recorded as an
    extension, not copied;
  - **A:** D17; a misreported precision's data loss (H2's computed sum) copied at the edge, a register row naming it
    upstream's defect;
  - **B:** one error writer in the core answers a database's refusal at run time as legend-engine does -- 500,
    `{"code":-1,"message":<the root cause's short class name>: <its message>,"status":"error","trace":...}`, the JVM
    unwrapping to the root cause; `trace` carries lite's own stack (no host has legend-engine's), a register row;
  - **C:** legend-engine's fixed words for the nullable-column refusal, its compile-pass context a register row;
  - **D:** an uncast dynamic pivot answers on every host as legend-engine answers it on DuckDB: the hosts' runners
    run the `Keyed` plan, which is already carriage (the keys statement answers each key's text and column name, the
    body's per-key regions repeat mechanically, the key texts are bound values) -- so D12's stepper is the `Keyed`
    node itself, carried by every runner; lite's two statements (keys, then the pivot) against legend-engine's one
    `PIVOT` a representation register row;
  - **F:** the JVM's answer header made from the plan's facts by the one maker hosts use; a dynamic pivot's
    generated columns typed as legend-engine types them, from the database's report;
  - **G:** the tab's own display, the clean thing (R5).
- **D19. Everything lite builds and uses runs on lite's own pieces; legend-engine's formats exist only at the edge**
  (the user, 2026-11-03, answering R4 step 3's question: "Everything we build should use our internal stuff, the engine
  stuff is only at the edges for backwards compatibility for upstream tools or users - everything we build and use
  should be fast and clean"). So:
  - lite's own consumers -- DataCube, Query, Studio, the notebooks, Python's dataframes -- read lite's own Arrow, in the
    plan's types, never the `ARROW_IPC` answer; DataCube asking Python's engine for `ARROW_IPC` moves to lite's own
    Arrow route;
  - legend-engine's `ARROW_IPC`, TDS JSON and CSV answers are written exactly as upstream writes them (R4 step 3's
    table), for upstream's tools and users alone;
  - both are fast: a measurement decides where either is slow (the 1,000,000-row JSON answer's breakdown, posted with
    this ruling).
- **D20. One UI over one typed result; two thin adapters; each connection talks lite or legend-engine** (the user,
  2026-11-03: "this is awesome! Exactly what I want", with the tweak "UIs should be able to talk to remote as either
  full lite arrow OR as backwards compatible engine"). It answers the question D19 left: lite's apps (Query, Studio,
  DataCube) must still work against an upstream legend-engine, and lite itself answers upstream's protocol, without
  that protocol being the path lite's own work takes.
  - **One UI code path:** every app works on one typed result model (the plan's types, Arrow columns; D9), as
    DataCube's grid does; nothing above the adapters knows which backend answered. A branch on the backend in UI code
    is a defect.
  - **Two thin adapters, each only a decode into that model:** *lite* (the plan record and Arrow in the plan's types;
    planned and run by whoever holds the data) and *engine* (legend-engine's TDS JSON or `ARROW_IPC`, its cells typed by
    the query's relation type where upstream's answer is lossier, a sum as text read back as its integer).
  - **The mode is the connection's:** a remote lite server is talked to as full lite Arrow or as a backwards-compatible
    legend-engine, either one, chosen per connection; an upstream legend-engine only as an engine; the tab's own
    in-process engine is lite. What a backend cannot do is a capability it declares, so the engine mode refuses or hides
    lite's extras (a dynamic pivot on H2) by name, never silently.
  - **Lite's servers always answer upstream's routes** exactly as upstream does (the core's edge writers, shared by the
    JVM, Python and the tab), beside lite's own route. No server mode is needed for compatibility; a switch that turns
    lite's own routes off proves, in CI, that the engine mode never leans on them.
  - **The judges:** the conformance suite holds the edge answers to upstream's; the apps' suites run in both modes and
    show the same text where upstream's types allow, and name the cells where they do not.

- **D21. Compatible at the edge, clean in lite** (the user, 2026-11-03, ruling the three cases posted in §7: "for our
  lite stuff we should stick to best/cleanest conventions now that we are unshackled from backwards compat"; "the
  whole point [of the engine adapter] is to stay backwards compat so we should try as much as possible to be compat
  when it makes sense"):
  - **Q1, the engine adapter keeps a Decimal's digits:** legend-engine's JSON writes `12.30`, and the adapter reads a
    `Decimal` column's numbers by the digits written, as the lite adapter does; an `ARROW_IPC` sum of integers (utf8)
    is read back as its integer.
  - **Q2, Query and Studio on the typed result, the cleanest spelling:** both apps read the typed result through either
    adapter and spell each cell as DataCube's grid does (its formatter): exact digits, a decimal at its own scale, a
    timestamp as stored; the three apps alike ("use whatever is best").
  - **Q3, lite's own Arrow, clean conventions:** a `FLOAT` its own digits as float64, a `DateTime` timestamp[ns, UTC]
    (a frame's zone kept), each field's full metadata; an Integer column always int64 -- never decimal128 -- a value
    past 64 bits refused by name, never wrapped or rounded (Arrow's integers stop at 64 bits; DuckDB's decimal128(38, 0)
    for a 128-bit integer is its workaround, and a type that changes with the data, Python's `_integer` today, is not
    clean).
  - **A Variant in lite's own Arrow is `arrow.json`** (the user, 2026-11-03, after "why not use the new variant for
    arrow?"): its compact JSON text (the decoders' carrier on every path; digits as written, `2.50`), the field tagged
    with Arrow's canonical JSON type (`ARROW:extension:name` `arrow.json`) beside the plan's facts, on every host. Arrow's
    binary Parquet variant (`arrow.parquet.variant`, canonical, the shape and number types kept per row) once the
    readers read it: measured on the pins, pyarrow 23.0.1 has no type for it and DuckDB 1.5.5 exports none of its own
    `VARIANT` ("Unsupported Arrow type VARIANT"), so a Python user would get two binary blobs today. One encoder writes
    the Variant, so that move stays contained.
  - **legend-engine's `ARROW_IPC` route frozen** (the user, 2026-11-03, asked "are we over indexing on engine Arrow?",
    then "Sounds good" to freezing it): what is built stays and is kept green; no more R4 work on it. What it answers
    is upstream's byte for byte; everything else is refused by name (register S136: a map, a struct, a binary value, a
    time with a zone, and -- the one change with the freeze -- a timestamp before 1582-10-15, which it wrote on the
    proleptic calendar where upstream's `java.sql.Timestamp` reads the Julian-hybrid one). A refusal is closed only
    when a real client needs it. The reasons, measured: upstream's pinned 4.145.0 names `ARROW_IPC` in no test; none of
    lite's apps asks for it (D20's rule that a lite server answers upstream's routes is why it is served); its writer
    shares no code with lite's other writers (`CellText`), where upstream's shares only the row read.
- **D22. legend-engine's `ARROW_IPC` answer is legend-lite's SERVER's alone** (the user, 2026-11-05, after "I love that
  you think now it's okay to take out of the browser one even though our tenet is that all three deployments/hosts
  should be the same ... maybe it is worth keeping on the remote server path to mimic engine": "The reason I thought to
  keep it in server is as drop in replacement for legend-engine which really only can be run as a server, thought if
  people in production using arrow then this gives them that"). The JVM server answers `?serializationFormat=ARROW_IPC`
  exactly as upstream writes it (frozen: S136's refusals); Python's engine and the tab -- the hosts lite is embedded in,
  which no legend-engine client points at -- refuse it by name, the core's own route's words, and stay alike. The one
  named exception to the three hosts being the same: engine compatibility at the deployment that replaces legend-engine.
  How: `PureV1Api` names no engine Arrow writer; the server hands one in (`LegendHttpServer.ENGINE_ARROW`), as it hands
  in its runner, so the tab's WebAssembly and Python's native library no longer compile it (register S137).

### 1.3 Open decisions, each with upstream's answer (posted to the user 2026-11-02)

Upstream read from the pinned sources: legend-engine 4.145.0 and legend-pure 5.99.0 (`release.MODULE.bazel:24-29`),
the extracted archives under Bazel's `external/+http_archive+legend_engine_src` and `+legend_pure_src`. File paths are
legend-engine's unless marked; `SER` is `legend-engine-xts-relationalStore/.../relational/serialization/
RelationalResultToArrowIPCSerializer.java`, `RR` and `MD` are `RelationalResult.java` and
`SQLResultDBColumnsMetaData.java` in that module's `relational/result/`.

- **A. The Arrow answer's schema (S125, D7): gates Python's Arrow execute, the last host path off the plan record.**
  *Ruled 2026-11-03 (D17): neither A1 nor A2 alone -- lite's own Arrow the plan's types (A2), legend-engine's
  `ARROW_IPC` answer an edge writer exactly as upstream's (A1 for that one output).*
  - *Upstream:* one class answers `ARROW_IPC` (RR:690-691 to SER). Each column's Arrow type comes from JDBC's
    `ResultSetMetaData` alone (SER:117-121), never the plan's TDS types (SER:126):
    - BIT/BOOLEAN bool; TINYINT int8; SMALLINT int16; INTEGER int32; BIGINT int64 (SER:144-154);
    - FLOAT/REAL float32; DOUBLE float64 (SER:155-159);
    - DECIMAL/NUMERIC decimal128(p, s), `setScale(s, HALF_UP)`, p 38 when reported 0 or less, capped at 38
      (SER:160-171, :307-309);
    - DATE date32; TIME time32[ms] (SER:173-176);
    - TIMESTAMP and TIMESTAMP WITH TIME ZONE timestamp[us, "UTC"], sub-microsecond digits truncated (SER:177-179,
      :322-323);
    - anything else utf8, arrays as JSON text (SER:180-181, RR:493).

    Every field is nullable and carries no metadata (SER:122). The schema metadata is `legend.builder`,
    `legend.activities`, `legend.generationInfo` (when non-null) and `legend.columns` in a HashMap (SER:125-135).
    Batches are 1,024 rows (SER:49) and the stream is one zstd frame (SER:84-86). legend-pure has no Arrow.
  - *Lite:* the JVM writes the plan's types (int64, float64, decimal at its slot's scale, timestamp[ns, UTC],
    time64[ns], nested types, `legend.*` field metadata); Python writes DuckDB's own (on DuckDB, upstream's answer
    but for the UTC tag on timestamps).
  - *S125 needs correcting either way:* "other kinds as the driver's Java type gives them" is contradicted (the JDBC
    code alone decides, SER:140-182), and the row misses int32/16/8, float32 and time32[ms].
  - *A1, exactly upstream* (one writer typing from the database's reported types): faithful; Python barely moves on
    DuckDB; the JVM's answer changes; lossy where upstream is (microseconds, HALF_UP, float32, time32[ms], nested as
    text) and copies its data loss (H2's computed sum reported decimal(27,0): `e8-arrow-fractional-sum.json`).
    Medium: the writer needs the reported types (JDBC metadata on the JVM, the host's Arrow schema on a host).
  - *A2, the plan's types everywhere:* lossless, uniform, the JVM unchanged; a ledgered divergence kept; Python's
    answer changes in three ways and gains field metadata. Low.
  - *Recommended:* A1, the user to rule whether upstream's data loss is copied too or is A1's one ledgered exception.
- **B. A database's refusal at run time (the ledger's three errTypeMismatch entries).**
  - *Upstream:* the SQLException is wrapped (RR:179-193) and answered by `ExceptionTool` (`Execute.java:208-211`,
    `ExceptionTool.java:35-38, 79`) as 500 `{"code":-1,"message":<root cause's short class name>: <its message>,
    "status":"error","trace":<the stack>}` (`ExceptionError.java:27-36, 50-78`), nulls dropped and keys sorted
    (`ObjectMapperFactory.java:35-40`). On DuckDB: `"SQLException: Conversion Error: ..."`.
  - *Lite:* the JVM has the shape without `trace`, unwrapping one level, not to the root (on H2 it would name
    `JdbcSQLDataException` where upstream names `NumberFormatException`); Python `ConversionException: ...`; the tab no
    envelope.
  - *B1:* one error writer in the core, every host passing the database's message, the JVM unwrapping to the root,
    `trace` ledgered (a host has no Java stack). Small. *B2:* keep each host's words and the ledger.
  - *Recommended:* B1.
- **C. The words of the nullable-column refusal (an unledgered divergence).**
  - *Upstream refuses the same queries:* legend-engine `ValueSpecificationBuilder.java:883-896`, pinned by
    `TestRelationFunctions.java:477-490` (`#>{a::A.tb}#->extend(~nid:x|$x.id + 1)` refused; passes with `NOT NULL`
    or `->toOne()`, :462-505); legend-pure `InstanceValueValidator.java:48-68`, `TestRelationTypeInference.java:170-185`;
    upstream's PCT writes `->toOne()` (`extend.pure:113`). The seven refused corpus queries (§7) are faithful.
  - *The words:* engine `Collection element must have a multiplicity [1] - Context:[...], multiplicity:[0..1]`; pure
    `Required multiplicity: 1, found: 0..1`; lite `..., found [0..1]` (`compiler/spec/Typer.java:1110-1113`).
  - *Options:* engine's words exactly (its Context names engine's compile passes: expensive); its fixed parts only; a
    register row. *Recommended:* the register row.
- **D. An uncast dynamic `pivot()` on a host.**
  - *Upstream:* `pivot` is legend-engine's (`pivot.pure:18-31`), typed `Relation<()>` (`Handlers.java:418-440`),
    planned as dynamic-column SQL (`pureToSQLQuery.pure:6360-6503`; `isResultColumnsDynamic`,
    `relationalMappingExecution.pure:244-247`), run on DuckDB (`duckdbExtension.pure:584-619`) with its columns from
    JDBC's metadata (`SQLExecutionResult.java:95-105`, RR:248-256); H2 refuses it.
  - *Lite:* the JVM runs it as a keyed plan; the tab and Python refused it before R2 and refuse it now at planning.
  - *D1:* hosts keep refusing, a register row; none. *D2:* the core's stepper (D12), so hosts run the keyed plan;
    medium. *D3:* upstream's one dynamic statement, columns from the database (gives up "the plan types every
    column"); medium. *Recommended:* D1 (DataCube plans static pivots itself, `datacube/src/plan.ts` `planPivot`).
- **E. A dynamic pivot's refusal in a grid, now at planning in new words:** the user, 2026-11-02: "i dont mind new
  wording if better".
- **F. The JVM's answer header from the plan's facts (one maker).**
  - *Upstream:* only a dynamic pivot's generated columns bear on it: typed from the database's report
    (`ResultColumn.labelTypePair`, RR:217-242; DOUBLE and DECIMAL are Float).
  - *F1:* the JVM's writer moves to `planEnvelope` now (stated columns byte-identical, held by `PureV1ApiTest`'s
    differential), generated pivot columns typed upstream's way (which also answers a precision-decimal pivot lite
    refuses today). *F2:* leave it to R8. *Recommended:* F1.
- **G. The tab's behaviours, R5's to settle (§3.6a's six and §7's NaN):**
  - *G1:* upstream's TDS JSON writes a DECIMAL as a number, trailing zeros kept (`12.30`: `BigDecimal.toString`,
    `ExecutionResultObjectMapperFactory.java:70-87`); the tab's remote plane reads it as `12.3`. Read it exactly.
  - *G2:* a nested timestamp has no upstream rule to copy (a list or map's is epoch milliseconds, RR:482-514; a
    variant's the database's text, RR:519; a struct's the driver's `toString`). Keep full precision in the tab.
  - *G3:* a decimal under a format code through a double: DataCube's own (legend-studio is not pinned here). Format
    the exact digits.
  - *G4:* Query's filter from a TDS row drops a timestamp's microseconds; upstream writes nine digits
    (`ValueTransformer.java:25-26, 35-37`). Fix.
  - *G5:* XLSX's date reader refuses negative and five-digit years (Excel holds neither). Keep the refusal.
  - *G6:* a chart's time axis read in local time: test in a zone other than UTC, then fix.
  - *G7:* a timestamp past `Date`'s range spelled `NaN`: refuse by name.

## 2. The code as it is

A code map at `main` `cf73e6ef` (2026-11-02), by an agent reading the code, every claim with its file and line.

### 2.1 The tab (TypeScript)

The tab has two interfaces. `QueryEngine` (`engine-client/src/engine.ts:35-64`) has `execute(Plan)`, `stream(Plan)` and
`run(sql)`; `typedByPlan` (`:85-97`) attaches the plan's types and refuses a column the plan does not type. The second
is `legend/engine.ts`'s `Engine.execute(ExecuteInput)`, which answers legend-engine's TDS JSON (`wire.ts:44-62`). Three
plan shapes reach them:

| Planner operation | What it answers | Read as |
|---|---|---|
| `plan`, `planJson`, `planModel` | `{sql, type}` | `Plan {sql, columns}` (`relation-type.ts`) |
| `executePlan` | `{sql, run, shape, resultColumns, builder}` | `ExecutePlan` (`legend/wasm-grammar.ts:115-125`) |
| `executionPlan` | the plan record's JSON, version 19 | `plan-runner.ts` |

The paths that run a query:

- **A. A DataCube grid on the tab's DuckDB.** `PlanAndRun` / `localHost` (`datacube/src/runner.ts:243-257`): `plan`,
  then `DuckDbEngine.execute` or `stream` (`duckdb.ts:427-475`). DuckDB-WASM's Arrow is decoded per cell by `toRawTable`
  and `decoderFor` (`:199-285`) into `ResultTable`'s `Scalar`s (`result.ts:53-62`).
- **B. A grid on the warehouse.** `WarehouseEngine.execute`/`stream` (`warehouse.ts:335-373`): each chunk an Arrow IPC
  stream (`tableFromIPC`), then the same `toRawTable`.
- **C. A grid on Python's engine.** `pythonRunner` (`datacube/src/services/python-host.ts:295-359`) posts the query to
  `/plan-and-run.json`. The answer is Arrow IPC with the plan in its schema's metadata (`legend.plan`), read by
  `arrowIpcAnswer`, then `readPlan`, `toRawTable` and `typedByPlan`, as one chunk. Headless V8 uses the same runner.
- **D. A remote legend-engine.** `LegendEngineExecutor` (`engine-remote.ts:164-204`): TDS JSON (`toResultTable`) or zstd
  Arrow (`arrowResultTable`). Only Query's server mode uses it.
- **E. Query's and Studio's execute in the tab.** `BrowserEngine.executeText` (`legend/browser-engine.ts:63-68`):
  `executePlan`, then `QueryEngine.run(plan.run)`, then `hostRows` (`:94-109`: the cells handed back as tokens), then
  `executeAnswer` (the WebAssembly planner writes legend-engine's JSON: Java's writer), then `JSON.parse`.
- **F. Query's results grid.** `CubeApp` with `CubePlanner` (`query/src/app/cube.ts:64-66`), planned by `planModel`.
- **G. Writes.** `runPlan` (`plan-runner.ts:78-138`) reads the plan record (version 19) and runs it on a `PlanSession`.
  It writes lite JSON rows itself (`rowsOf`, `:158-168`), an integer, a text or a boolean cell only. Its one caller is
  `writeTable` (`datacube/src/table-output.ts:61,80`), from the tab and from Python's headless `flow.run`.
- **H. SQL no planner wrote.** `run`: a session's setup, the host operations, snaps (`snap.ts`, `loadArrow`), Studio's
  SQL playground.

Where the tab spells values itself: `values.ts` (`dayText`, `timestampText`, `timeText`, `decimalText`,
`timestampFromText`, `parseExact`); `duckdb.ts` (`toScalar`, `exactJson`, `decimalToScalar`, `decoderFor`, `nestedJson`);
`engine-remote.ts:104-109` (timestamps rewritten); `browser-engine.ts` `hostRows` (the tokens handed back);
`plan-runner.ts` `rowsOf`; and Query's own CSV, written from the TDS JSON (`query/src/ui/results.ts:288-293`). Java's
writers spell an answer only on path E (`executeAnswer`).

The warehouse driver (`warehouse.ts`) leaves every HTTP call and every answer to its WebAssembly binding
(`SqlApiCalls`: `login`, `loginKey`, `submit`, `poll`, `fetchChunk`, `cancel`, ...), keeps the result's bytes itself (one
Arrow IPC stream per chunk), and holds a bearer token refreshed at 80% of its life.

### 2.2 Python

- **`Table.execute`** (`frames.py:248-252`): `lite_plan_json_held` answers `{sql, type}`; `Frames.run` (`:390-395`) runs
  it (`to_arrow_table`) and `typed_by_plan` (`:89-108`) puts each column's Pure type in its field metadata and casts
  integers. A `pyarrow.Table`.
- **The engine's Arrow execute** (`engine.py:1633-1647`): `lite_execute_plan` answers `{sql, metadata}`; `to_arrow_table`;
  `_arrow_ipc` writes upstream's Arrow, zstd-compressed. Python writes it.
- **The engine's JSON execute** (`engine.py:1649-1664`): `lite_execute_json_plan`; the `run` statement; `fetchall()`;
  `compiler.host_rows` (`compiler.py:275-316`: every cell as Python object, then JSON text); `lite_execute_json_answer`
  (Java's writer, in the native library). Three conversions per cell.
- **`/plan-and-run.json`** (`engine.py:1482-1528`): a frame's, a project's or a warehouse table's rows, typed by the plan,
  as uncompressed Arrow IPC with the plan in its metadata (`_arrow_rows`).
- **Headless DataCube** (`_headless.py`): V8 asks Python's DuckDB for rows (`to_arrow_table`, then IPC, then base64) and
  Python's planner for plans; a page's `flow.run` writes run on the tab's `runPlan`, from `lite_execution_plan_json_held`.
- **The warehouse** (`warehouse.py`): HTTP through urllib, the same binding (`lite_warehouse_call`), its chunks as Arrow.
- `lite_execute_arrow_answer` is declared and called nowhere (`_library.py:37-39`: "wired ... when the user rules").
  *Since R3 its one caller is `//python:arrow_handoff_test` (the text handoff's Arrow answer, compared with the Arrow
  handoff's).*

### 2.3 What is missing

- **No test compares a host's executed rows with Java's.** The checks that exist compare plans' SQL with the JVM's
  (`//python:bindings_test`, `//datacube:wasm_differential_test`), the tab's execute text with Python's, both written by
  the same Java writer (`//datacube:python_execute_test`), and each warehouse driver's HTTP calls with a recorded corpus.
- **No benchmark runs under Bazel** before this program (`datacube/bench/*.mjs` are only typechecked).
- **No Snowflake** in either executor.
- **Database types:** the tab runs DuckDB (`DuckDbEngine.databaseType`) and, through the warehouse, the catalog's type
  (DuckDB or Postgres); Python hardcodes `'DuckDB'` in five places (`frames.py:180,333`, `_host.py:14`, `projects.py:46`,
  `engine.py:1421`) and takes a warehouse table's from its catalog.


## 3. The design (rev 3)

```
             ┌──────────── the core (once, embedded x3, no I/O) ─────────────┐
model+query →│ planner ──► plan (one kind, a straight line)                  │
             │ writers:  plan + Arrow batches ──► legend JSON / CSV / Arrow  │
             └───────────────────────────────────────────────────────────────┘
host shell:   runner: the plan's statements ──► connector ──► Arrow batches
              ├─► a native consumer (the grid, a dataframe) reads the Arrow
              └─► the core's writers, for an answer that is text
```

### 3.1 The core

- **The planner:** `//core:plan_side`, embedded through `//wasm:boundary` (the tab: TeaVM to WebAssembly GC,
  `//wasm:planner`) and `//native:compiler` (Python: GraalVM). Its builds are held to the JVM's by
  `//wasm:differential_test` and `//python:bindings_test`.
- **The writers:** `//core:result`: `CellText`, and the JSON rows, legend-engine JSON, PureV1, CSV and Arrow writers.
  Its deps are `//arrow_ipc`, `//base`, `//json`, `:error`, `:execution_plan` and `:values`. `plan_side` includes it, so
  both embedded builds carry it today.
- **What the core gains:** an Arrow reader (D8); a writer fed a batch at a time (begin, each batch, end), so a large
  answer streams in bounded memory; and a boundary that carries bytes (§3.6).

### 3.2 The plan

The record (`executionplan.ExecutionPlan`, its JSON `legend-lite-plan`, version 19 today) is what every host path runs
(D11). A host's plan is a straight line:

- the target and its session statements;
- the statements in order, in one transaction, with a count a later statement binds;
- which statement's rows are the answer, and each answer column's name, Pure type and multiplicity, and its Arrow type;
- every fact a writer needs for each output the answer may be written as (legend-engine's builder, its activities).

What the record lacks for a host path today is R2's to add. The host's profile (D6) is the planner's input: a host is
never handed a plan it cannot run.

### 3.3 The runner

One per host, carriage (P4, D12): open the session the plan names and run its session statements; run the statements
in order in one transaction, binding a count where a later statement takes it; stream the answer's batches to a sink. It
checks the plan's format and version, and refuses an unknown step by name (D6). The tab's `plan-runner.ts` is its start
(today writes only, with a review of its own), Python gains its twin, and the JVM's `PlanRunner` stays the reference
(D15).

### 3.4 Connectors

A connector is the one way in to a database: a statement and its bound values in, a stream of Arrow record batches out,
and its transaction's three statements (the planner's `transaction` host operations, as `hostSession` asks them today).

| Host | Connector | Wire | Status |
|---|---|---|---|
| the tab | DuckDB-WASM | Arrow (`send`, batch by batch) | exists (`DuckDbEngine`), moves under the interface |
| the tab | the warehouse (DuckDB on a server) | Arrow IPC over HTTP | exists (`WarehouseEngine`), moves under it |
| the tab | Snowflake | the SQL API's `jsonv2`, turned into the plan's Arrow here | new (§4.2) |
| Python | duckdb-python (the frames' database) | Arrow (`to_arrow_table`, record batch readers) | exists, moves under it |
| Python | Snowflake | Arrow, through ADBC | new (§4.1) |
| the JVM | JDBC (`exec`'s session seam) | rows, read by `exec.JdbcDecode` | exists, the reference |

A database's Arrow can differ from the plan's types (DuckDB's `INTEGER` an `Int32`, its `TIMESTAMP` in microseconds with
no zone). Reading it into the plan's types is done once per column, never per cell, by the plan's column facts. A
column whose wire type the plan's type does not admit is refused by name (AGENTS.md invariant 4), never guessed. Where a
consumer takes the database's Arrow directly (a dataframe, the grid), it reads it by the plan's column types; whether
Python's Arrow answers are cast to the plan's types by pyarrow or by the core is measured in R4 and posted with D7's
cases.

### 3.5 Consumers and outputs

- **The grid** reads typed Arrow, and only the cells on screen become text, by DataCube's formatter (§3.6a).
- **A dataframe** takes the Arrow.
- **A text output and the Arrow answer** are the core's writers' (D2, D8), fed Arrow (§3.6).
- **A refusal at planning** is the core's, the same on every host. **A database's error** is put in the answer's error
  frame by the writers; only the database's own words differ by driver (DuckDB's JDBC driver, duckdb-python and
  DuckDB-WASM word the same failure differently: R1's ledger), each such difference posted to the user case by case.

### 3.6 The handoff into the writers: Arrow bytes

The boundary carried strings only before R3 (`wasm/src/main/java/planner/Folded.java`): a binary answer travels as base64,
and rows go in as JSON text. The new boundary carries bytes:

- **Native (Python):** a pointer and a length, so the library reads Python's Arrow buffer where it lies, with no copy.
- **The tab:** a byte array copied into the module's memory (WebAssembly GC's arrays are not linear memory), and the
  answer's bytes copied out. The grid never takes this path; Studio's execute answers, usually small, do.

R3 measures both before anything is built, and posts the numbers.

**The writer runs in both embedded builds (a scratch probe, 2026-11-02, not committed).** The corpus's `TYPES` table
(`//conformance`) was planned by `lite_execute_json_plan`, run on duckdb-python, its rows handed over by `host_rows`, and
written by `lite_execute_arrow_answer` in the native library and by `executeArrowAnswerOrError` in the tab's module
(Node 22.16, `--experimental-wasm-exnref`, as `//tools/js` runs it):

- pyarrow reads both answers: the plan's types (`Int64`, `Decimal128`, `bool`, `date32[day]`, `timestamp[ns, tz=UTC]`),
  every edge value intact (`9007199254740993`, `99999999999999999999999999999999999.99`, `é漢字🙂`, `9999-12-31`,
  `2262-04-11 23:47:16.854775`, a row of nulls);
- both are 4,393 bytes, with the same schema and values, differing only in the trace id each answer draws fresh, as
  upstream's does;
- neither export had a caller or a test before (§7).

### 3.6a The tab's result model (H1b; proposed 2026-11-02, for the user's review before code moves)

**Why.** A tab result holds every cell as a finished `Scalar` (`engine-client/src/result.ts`), and a decimal, a date, a
time or a timestamp as its exact text (`values.ts`: JavaScript has no exact type for any of them). The text was the
vehicle for exactness; Arrow already holds each exactly in binary (a decimal's unscaled integer and scale, a timestamp's
microseconds, a date's days). Two costs follow, measured and mapped (GATES "H1a", item 3; the consumer map below):
every cell is spelled when the rows arrive, whether or not anything reads it; and the consumers that need a number or a
date parse the text back (43 `numberOf` calls, 24 `toNumber`, the formatter's `instantOf` and `fieldsOf`, `periodText`,
XLSX's `DATE` reader). The user's own reading (2026-11-02): "should decimal and timestamp not be converted to string
until render... is it also pointing out to us that maybe our data structure is wrong?"

**The consumer map** (an agent reading the code at `cf73e6ef`; about 45 files in `datacube/src`, 6 in `engine-client`,
one each in `query`, `studio` and `pure-protocol`, two in `python/legend_lite`). There are two kinds of reader:

- **Readers that need the exact text.** The text is their meaning, so it must stay exactly as spelled today:
  - every filter built from a cell: the click, value lists, tree keys, chart marks, brush and drill, Dynamic Replace.
    The text becomes a Pure literal (`literalNode`, `lit.of`);
  - pivot values: they name columns and become literals in step 2;
  - persisted document fields: filter values, tree paths, pivot values, a chart's drill path;
  - equality keys (`keyText`, `groupValue`, value-list keys) and text-ordered comparisons;
  - raw CSV, clipboard, flow Output, chart CSV and TSV;
  - `hostRows`;
  - Python's `GridView` and `pin_pivot`.
- **Readers that need only a number or a date:** charts' and timeseries' values (`numberOf`, `toNumber`), styles and
  conditional formatting, the selection's average and its min and max ordering, counts, the formatter's dates, XLSX's
  serial days. The selection's sum needs the exact digits (`exactSum`).

**The model.** A result column keeps its rows as the database answered them (the Arrow vectors, or the decoded arrays a
non-Arrow plane gives). It answers, for row `i`:

- `number(i)`: the value as a number, for the numeric readers. It is what `numberOf` answers today, computed from the
  exact value, never re-parsed.
- `text(i)`: the exact text, spelled as `values.ts` spells it today, computed when asked. Every reader of the first
  kind moves to it, and none of them sees a different character.
- `exact(i)` for a decimal (its unscaled integer and scale: the sum's digits), and `instant(i)` / `day(i)` for the
  temporal types (what `instantOf` and `fieldsOf` parse today).
- `values`: today's `Scalar[]`, made on first access, so a reader not yet moved keeps working unchanged. It goes when
  the last reader moves.

The display spelling (`format.ts`, `format-code.ts`) reads the typed value it parses today, and only for the cells on
screen. Every displayed text stays identical. Where today's chain goes through a double (a decimal under a format code:
`Number(text)`), the new chain makes the same double the same way. Changing that is a separate decision.

**The order, each step its own commit, every displayed and persisted text unchanged:**

1. **H1b.1:** the column interface over today's decoded values, with `values` made on first access. No reader moves
   and nothing visible changes.
2. **H1b.2:** the grid's cells, tooltips and the display formatter read the typed interface, for the cells on screen
   only.
3. **H1b.3:** the numeric readers (`numberOf`, `toNumber`, styles, conditional formatting, the selection's totals) read
   `number`, `exact`, `instant` and `day`.
4. **H1b.4:** the readers of the first kind read `text`.
5. **H1b.5:** the columns read the Arrow vectors themselves, and the eager decode goes.

Each step is measured, including an interaction end to end: planning, SQL, decode and drawing, at the sizes DataCube
fetches (up to 1,001 rows a level; `DEFAULT_MAX_ROWS`) and an uncapped flat grid. The judges, unchanged:
`//datacube:typed_values_test` in its three zones, `values`, `format`, `format_code`, `format_scale`, `style`,
`conditional`, `query`, `value_filter`, `selection`, `pivot_values`, `pivot_total`, the chart and timeseries suites, the
export suites, `grid`, `grid_dom`, `app_dom`, `treeview`, `cube_document`, `saved_documents`, `headless`,
`live_snap`, and Python's `engine_test` (`tests/test_page.py` pins `'12.30'`, `'2024-01-31'` and
`'2024-02-01T09:00:00'`).

**Behaviours the map found that differ between planes or lose precision.** These are not changed by H1b. Each is a
spelling or behaviour change, posted to the user case by case before any is built:

- On the remote legend-engine plane (`engine-remote.ts:93-110`), a decimal becomes a JavaScript number whenever a double
  reproduces its digits, its trailing zeros dropped (`12.30` arrives as `12.3`), where the Arrow planes keep `"12.30"`.
  A filter made from the same cell differs by plane.
- A nested timestamp keeps milliseconds only (`duckdb.ts` `nestedJson`).
- A decimal under a format code is displayed through a double (`format-code.ts:256-271`), so a long one rounds.
- Query's filter from a TDS row drops a timestamp's microseconds (`query/src/ui/results.ts:225-243`).
- XLSX's date reader refuses a negative or a five-digit year (`export-xlsx.ts:202-232`).
- A chart's time axis hands ECharts the timestamp's text, which ECharts reads in local time (`chart-option.ts:505, 554`;
  `chart-render.ts:165`): to check whether a zone other than UTC draws it shifted.

### 3.7 Conformance

R1's suite (`//datacube:conformance_test` over `//conformance`) is the judge every step runs under (P6). It grows a case
for each path, output and database a step brings, and its ledger only shrinks (D10). DuckDB in CI; Snowflake on the
Snowflake lane session (credentials stay there, §6), against Java's answers through snowflake-jdbc.

### 3.8 The core's language

Java (D14). TeaVM's WebAssembly GC build costs the tab's bundle its size and a copy for a text answer; GraalVM's native
build costs nothing material. Both are measured (R3, R8), not assumed.


## 4. Snowflake

The planner already plans for Snowflake (core's `Snowflake` dialect; Java runs it through snowflake-jdbc, as
legend-engine does). R7 first checks that the WebAssembly and native planners carry the dialect.

### 4.1 Python: ADBC

`adbc-driver-snowflake` with `adbc-driver-manager`, in a `snowflake` extra (a native library of about 74 MB, built in Go).
Measured on the Snowflake lane, 2026-11-01 (`docs/GATES.md`, the payoff entry's item 11), at 10,000,000 rows:

- ADBC streaming record batches: 2.16M rows/s from Python, 1.84M from Java through `adbc-driver-jni`;
- the Python connector's Arrow batches: 1.54M rows/s, but `NUMBER(18,2)` arrives as `float64` (lossy) by default, and
  integer widths change chunk by chunk;
- ADBC answers exact `decimal128(p, s)` for every `NUMBER` and `timestamp[ns]` for `TIMESTAMP_NTZ`, a schema that does
  not depend on the data.

Authentication (D3): key-pair JWT (`auth_jwt`, the key's PEM text and its passphrase: `jwt_private_key_pkcs8_value`,
`jwt_private_key_pkcs8_password`; the lane found that the file option takes only an unencrypted key), OAuth, or a
programmatic access token. The proxy's CA bundle and `HTTPS_PROXY` reach the Go library through the environment.

### 4.2 The tab: the SQL API

`POST /api/v2/statements` with the statement, its bindings and the session's parameters; a `202` answers a statement
still running, polled by its handle (`GET /api/v2/statements/{handle}`); a large result comes in partitions
(`resultSetMetaData.partitionInfo`, each fetched with `?partition=n`). Facts from Snowflake's reference
(https://docs.snowflake.com/en/developer-guide/sql-api/reference):

- **One result format, `jsonv2`:** "The data is encoded as JSON strings, regardless of the Snowflake datatype." No Arrow.
  The connector (§3.4) turns each column's strings into the plan's Arrow by `resultSetMetaData.rowType` (type, precision,
  scale), exactly: a number's digits as written, a timestamp's epoch and fraction. It suits interactive results; a large
  extract is the server's to run.
- **Tokens:** `OAUTH`, `KEYPAIR_JWT` and `PROGRAMMATIC_ACCESS_TOKEN` (`X-Snowflake-Authorization-Token-Type`). The tab
  uses OAuth or an access token (D3); a key never reaches a page.
- **A `User-Agent` is required.** CORS is not documented (D4): the session takes its base URL and its headers from the
  caller, so the same code reaches Snowflake through a proxy unchanged. R7 checks CORS from a real browser before any
  page depends on it.

### 4.3 Fidelity

Java's Snowflake results (through snowflake-jdbc, legend-engine's own driver) are the reference: the tab's and Python's
must equal them, value by value, on the lane. Every difference found is posted to the user, case by case, before it lands,
and ledgered (AGENTS.md, the upstream-fidelity tenet).


## 5. The roadmap (rev 3)

Each step has a goal, a scope and an exit test, and runs as one cycle: a brief, the build, an independent audit (an
agent that wrote none of it), its fold, the records (GATES, IN_FLIGHT, §8), then a push to the branch. No PR and no push
to `main` without the user's word. A step changes no output's spelling unless the user ruled it, case by case. Every
step runs under the conformance suite (§3.7).

| Step | Goal | Exit |
|---|---|---|
| **R1. The conformance suite** (done) | The safety net, before any host changes. | Green against its ledger (seventeen entries); CI on every OS. |
| **R2. One plan kind** | Every host path runs the plan record (D11), planned for the host's profile (D6). | No host reads another plan shape; the ledger the same or smaller. |
| **R3. The writers take Arrow** | A boundary that carries bytes, the core's Arrow reader, a writer fed a batch at a time (D8). | On the suite, the writers' answers from Arrow equal today's, byte for byte. |
| **R4. Python's shell** | One runner over its connectors; JSON through the writers from Arrow; the Arrow answer in the plan's types (D7). | The fourteen schema entries gone or ruled; Python's JSON execute in its budget. |
| **R5. The tab's shell** | One runner over its connectors; the grid on typed Arrow (§3.6a); writes as carriage. | The suite and DataCube's suites green, every displayed and saved text unchanged. |
| **R6. The deletions** | The text handoffs, the parse-backs, the retired plan shapes. | A census reads zero on each. |
| **R7. Snowflake** | Python through ADBC (§4.1), the tab through the SQL API (§4.2). | The suite green on the Snowflake lane against Java's JDBC answers. |
| **R8. Budgets** | Speed held, not chased (P8). | Budgets per host in a lane; an interaction measured end to end. |
| **Z** | The landing. | An independent final audit, the records, CI on every OS; handed to the user. |

**R1. The conformance suite (done, GATES "R1": its first plan, then its design as built).**
- **The cases:** the planner corpus `//wasm:corpus` (a trades model and its queries, already the planner's shared
  judge), the type matrix of `//engine-client:column_decode_test` (every type DuckDB writes, with nulls and edge
  values), and the typed-values cases DataCube pins (`datacube/test/typed-values`).
- **The data:** seeded by the planner's own test-data operation, so every host loads the same rows.
- **The runs:** each case planned once by Java, then run by:
  - Java (`PlanRunner` on DuckDB through JDBC, with the Arrow and JSON-rows writers);
  - the tab (DuckDB-WASM in Node, through the tab's engine as it is);
  - Python (the frames' DuckDB, through `Table.execute` and the engine's executes as they are).
- **The comparison:** the typed values, the Arrow, the text.
- **The ledger:** today's differences (D10). The suite is the judge every later phase runs under.

*R1's design (2026-11-02, from a survey of the building blocks every host already has).*
- **The interface.** Every host already answers legend-engine's own API, `POST /api/pure/v1/execution/execute`, in JSON
  and, with `serializationFormat=ARROW_IPC`, in Arrow:
  - **Java:** the JVM server (`PureV1Api`, through `Execution.run` and `PlanRunner`);
  - **Python:** its engine (`Engine.answer`);
  - **the tab:** in the page (`BrowserEngine.executeText`), in JSON only.

  Every host also loads a model's rows from its `###Data` element by the same planner operation (`projectData`; the
  server through `ConnectionResolver`, Python through `projects.py`, the tab through `ProjectRunner`). So the suite sends
  one request, with one model and its data, to the three hosts through their product path, and no new entry point is
  made for it.
- **The corpus**, `conformance/` (its own package; any host's suite may read it):
  - `model.pure`: one DuckDB database, its rows in a `###Data` element. A table of every type, with its edge values
    and nulls, and a table of trades for the relational operations.
  - `queries.tsv`: each query sorted, so an answer's order is the query's own, plus refusals.
  - `ledger.json`: D10's ledger.
- **The driver**, a Node test in `//datacube` (the one package that reaches all three hosts already: the JVM server's
  launcher, the native library, the WebAssembly planner, DuckDB-WASM). It starts the JVM server and Python's engine,
  opens the tab's engine in-process, and asks each case of each. It compares:
  - **legend-engine's JSON text:** all three, byte for byte, each trace id masked;
  - **the Arrow answer:** Java's against Python's, the schema and every value;
  - **the grid's own path:** the tab's `DuckDbEngine.execute` values against Java's, value by value (a decimal by its
    value and its scale apart, a timestamp by its instant);
  - **a refusal:** its status and its words.
- **The ledger.** Every difference found is either in it or a failure. An entry that no longer differs is a failure
  too, so the ledger only shrinks.
- **The lane:** `//gates:warehouse`, where the tab-against-Python execute test already runs.

**R2. One plan kind.**
- **The survey's findings (2026-11-02):**
  - **Every query of both corpora gives the same SQL from the grid's plan (`{sql, type}`) as from the plan record**
    (74 of 74 that plan; both refuse the same queries in the same words): moving a grid onto the record changes no
    statement it runs.
  - **The readers of the old shapes read three fields:** the SQL, each column's name, and its type path.
  - **The grid never plans a statement that depends on data:** DataCube fetches a pivot's values first, then plans a
    static pivot (`datacube/src/plan.ts` `planPivot`); a `Keyed` plan arises only from a person's own dynamic
    `pivot()`, and the hosts' routes refuse it at planning.
  - **What the record lacked:** the enumeration fact (D16), for legend-engine's answer header, which Java built from
    the compiler's typed query, not from the plan.
- **The commits:**
  1. The enumeration fact (D16, plan version 20); the hosts' JSON execute answers the plan record, and its answer's
     header is made from the plan's facts alone (`PureV1Api.planEnvelope`), held query by query to the header the
     server makes from the compiler's types (`PureV1ApiTest`). The server's own writer moves to the plan's facts in a
     later commit, under the PCT lanes.
  2. The host's profile and the planning-time review (D6); the tab's runner keeps the format and version check.
  3. The grid's paths onto the record (§2.1 paths A, B, C, F), each column's type by `plainType`.
  4. Python's `Table.execute` and plan-and-run onto the record, and the tab's reader of Python's answer.
  5. The old shapes retired, a census reading zero; Python's Arrow execute keeps `arrowPlan` until R4 (D7).
- **The survey first:** the record's JSON against each retired shape's fields and each caller (§2.1 paths A, B, C, E,
  F; §2.2), and what the writers need for each output (legend-engine's builder, its activities, the trace id).
- **The host's profile (D6):** the database types a host reaches and the outputs it writes, as the planner's input; what
  a host cannot run refused at planning, by name.
  *As built (the R2 audit, 2026-11-03): no profile is a planner input. What a host cannot run is refused at
  planning by kind (`HostPlans.read`, `program`, `query`); that the plan's database is the host's own is still checked
  by the tab's runner at run time (`plan-runner.ts`), as before R2. A profile carrying the host's database waits for a
  host that reaches more than one (R7, Snowflake).*
- **The tab:** the callers of `Plan {sql, columns}` and `ExecutePlan` move to the record, and the runner reads the
  answer's step.
- **Python:** `Table.execute`, the engine's executes and plan-and-run move to it.
- No output changes: the suite and each host's own suites hold it.

**D. A keyed plan on a host (D18 D; designed 2026-11-03, built after F).** The `Keyed` node is carriage, so a host
runs it with the steps the JVM's runner takes, the core doing every step that is not a statement:
1. **The expansion is the core's, on the plan side.** `PlanRunner.keyed`'s pure steps -- the body's per-key regions
   repeated once per key (`expand`), the answered columns (a per-key column named by its key's cell), their order
   (`perKeyAt`, `place`), the names each statement's cells are read under, the column cap (`maxColumns`) -- move out of
   `exec` into the plan record's package, one function the JVM's runner and the hosts' boundary both call
   (`exec` keeps the sessions and the sending; the planner-side builds carry no `exec`).
2. **The host runs the keys and the body** in one transaction (with the body's description between them, item 5): the keys statement (its rows the keys' texts and names), then the
   body the core expanded for them, each key's text BOUND as the JVM binds it (a `?` per key cell; the tab's held
   connection prepares, `duckdb.ts` `exclusive`; Python's cursor binds).
3. **The answer is the core's**: the host hands back the keys' rows, the body's rows (`hostRows`) and each body
   column's type as its database reported it; the core writes legend-engine's answer, its header from that report
   (F's `dynamicEnvelope`), the grid's typed rows from the expansion's columns.
4. **`HostPlans.query` accepts a `Keyed` root** whose statements bind only key cells (the execute route's profile); the grid's `HostPlans.read` keeps refusing it, and anything else stays refused.
5. **The tab's report, measured (2026-11-03, DuckDB-WASM under Node, a pivot body's eight columns):** its Arrow
   schema answers `Int32, Decimal[38e0], Float64, Decimal[38e+2], Bool, Date32<DAY>, Timestamp<MICROSECOND>, Int64`
   -- a `HUGEINT` (a sum of integers, legend-engine's `Integer`) is a 128-bit decimal there, which the prefix rule
   reads as `Float`, so the Arrow schema is not the report. `DESCRIBE` of the same body answers DuckDB's own names,
   `INTEGER, HUGEINT, DOUBLE, DECIMAL(38,2), BOOLEAN, DATE, TIMESTAMP, BIGINT` -- the names DuckDB's JDBC driver reports
   as `getColumnTypeName` -- and does so for a prepared statement with the keys' values bound, planning the body
   without running it. So the tab's report is a `DESCRIBE` of the expanded body (the connector's how, as D76 put a
   first step's). Python describes the body the same way. The description and the transaction are host operations the planner spells by the plan's database (`describe`, `transaction`), never SQL a host writes (`//tools/guards:host_sql_test`, `//python:host_sql_test`).

**R3. The writers take Arrow.**
- **The measurement first** (scratch, both hosts, posted): the bytes boundary's cost in the native library (a pointer
  and a length) and in the tab's module (a copy each way), against the text handoff (H0) and the Arrow execute's 188 ms.
- **The reader:** in `//arrow_ipc`, the types the plan declares, each connector's Arrow read into them (§3.4).
- **The boundary:** `wasm/.../Boundary.java`, `TabExports.java`, `native/.../nativelib/Compiler.java`, and the hosts'
  bindings.
- **The writer fed a batch at a time:** begin, each batch, end.
- **The exit:** on the suite, the JSON and Arrow answers the writers give from Arrow equal today's, byte for byte.

*R3 as built (2026-11-03).*
- **The measurement first (scratch probes in both embedded builds, reverted; this container, 4 vCPU):**
  - **The tab** (TeaVM 0.15, WebAssembly GC, Node 22.16): a `byte[]` handed in (a typed array copied into the module)
    costs 1.4 ms a megabyte, a string 7 ms; a `byte[]` handed out 1.0 ms a megabyte, a string 36 ms at 1 MB, 625 ms at
    10 MB and 2.7 s at 30 MB (past linear). So the answers go out as bytes too, never base64 or a long string.
  - **The native library** (from duckdb-python, H0's five columns, 1,000,000 rows): the rows' JSON text takes 10.4 s
    to make in Python (94 MB) and 441 ms to hand in; DuckDB's Arrow IPC takes 106 ms to make (50 MB), and a pointer and
    a length cost nothing to hand in (75 ms is the probe reading every byte through a view: an upper bound on a read).
- **The reader**, in two layers:
  - `//arrow_ipc` `ArrowReader`: the format alone -- the schema, then each record batch, each column an array of views
    over the bytes (never copied); the stream format and the FILE format (DuckDB-WASM's `runQuery` answers a file:
    found by the tab's check); a dictionary, a compressed batch, a big-endian stream, a union, a view, an interval, a
    large list refused by name. `ArrowType` gains `LargeUtf8` and `LargeBinary` (pyarrow's large strings).
  - `//core:result` `ArrowRows`: the plan's columns over the stream, the twin of `HostRows` (the rows as JSON text) and
    of the JDBC session's decode (`exec.JdbcDecode`). Each Arrow value is read as the value a JDBC driver hands for its
    SQL type, then decoded by that decode's arms, so the carriers are the JVM's: a REAL its own four-byte digits, a
    HUGEINT (DuckDB's Arrow: a 128-bit decimal of scale 0) an exact integer, a timestamp nine subsecond digits. Each
    column's decode kind and wire reader are decided once. The slot-word table moved from `JdbcDecode.Kind` to
    `RowDecode.Kind` so the two decodes read one table (behaviour unchanged). Registered in the result package's closed
    register and, spelling by spelling, in `WriterCarriageGuardTest`.
- **The boundary carries bytes:** `Folded.httpBytes` (the fold's lines in UTF-8, then the body's own bytes);
  `Boundary.executeJsonAnswer(plan, ByteBuffer)` and `executeArrowAnswer(plan, ByteBuffer)`
  (`PureV1Api.jsonAnswer`/`arrowAnswer` over one body for either source); the tab's
  `executeJsonAnswerFromArrowOrError` and `executeArrowAnswerFromArrowOrError` (`byte[]` in and out); the native
  library's `lite_execute_json_answer_arrow` and `lite_execute_arrow_answer_arrow` (a pointer and a length in, read in
  place; the answer's bytes out, their length through an out-parameter, copied in bulk); Python's
  `Library.call_bytes` and `compiler.execute_json_answer_arrow` / `execute_arrow_answer_arrow` (a pyarrow `Buffer` at
  its own address, or `bytes`).
- **The writer fed a batch at a time** was already the writers' contract (`ResultSink`: columns, each batch, end); the
  reader feeds it a stream's batches in order. A host hands one whole stream per call; handing it across calls waits
  for a measurement that needs it (R8).
- **The exit, met:**
  - **Python** (`//python:arrow_handoff_test`): every case of the conformance corpus planned and run as the engine
    plans and runs it, its rows handed over both ways: the 17 cases with rows answer byte for byte alike in
    legend-engine's JSON and value for value in the Arrow answer (the two refusals and the one keyed pivot counted).
  - **The tab** (`//datacube:conformance_test`, a new check, `handoff`): DuckDB-WASM's own Arrow (its bindings'
    `runQuery`, no JavaScript re-encoding) handed to the module: the same 17 cases' JSON byte for byte as the tab's
    text handoff, and their Arrow answer equal to Java's in schema and values (lite's Arrow writer in the plan's types:
    the answer D7 asks of Python, which Python's own Arrow execute does not give yet).
  - **The JVM** (`ArrowRowsTest`): DuckDB's encodings built with the framer decode to the cells the same rows decode to
    as JSON text; lite's own Arrow round-trips; a tagged column; the file format; the refusals.
- **End to end** (`//python:executor_bench`, a new path `handoff`, its answer asserted equal to `json`'s, the trace
  masked; planned on every call as `json` is, corrected by R3's audit -- the first figures planned once, outside the
  timer): 1,000 rows 35.0 -> 22.6 ms; 100,000 rows 1,855 -> 230 ms; 1,000,000 rows 23,626 -> 1,899 ms (12.4 times).
- **R3's audit** (an agent that wrote none of it), folded: the exit test laned (`//gates:warehouse`; it ran by name only)
  with H0's and H1a's unlaned targets (CI's lane guard refuses a test in no lane); a DuckDB JSON cell's non-finite token
  read as the JDBC decode reads it (`NonFiniteJson`, moved from `exec` to `result`); a text slot's cell the driver's own
  text, as JDBC's `getString` hands it (a decimal's `1E-10`), a temporal one there refused; what DuckDB's Arrow cannot
  carry whole refused by name (an unsigned 128-bit integer, wrapped; a zoned time, its offset dropped); a day of
  milliseconds checked whole and a column checked as long as its batch; the exit test asserting an answer for every case
  with rows and passing `bytes` too; "the carriers are the JVM's" scoped (§7 names where the two decodes part); the
  benchmark's planning made like for like. Found while checking the fold: two defects of R2's own commits that suites
  this container cannot run whole had hidden -- plan-and-run's test read the old plan shape (`test_engine.py`), and
  `compiler.plain_type` spelled a type name outside `_types.py` (`//python:frames_test`'s guard); both fixed.
- **Not in R3, by design:** no product path switched (Python's `_execute_json` and the tab's `executeText` keep the
  text handoff until R4 and R5); the tab's planner protocol has no bytes kind yet (R5: Python's headless host must
  answer every kind, and a kind's bytes do not cross its JSON channel as they are); a keyed pivot's answer from Arrow.

**R4. Python's shell.**
- **The runner:** the twin of the tab's (§3.3), over duckdb-python.
- **The consumers:** `Table.execute` (a `pyarrow.Table`), the engine's Arrow and JSON executes, plan-and-run, and the
  headless page's writes.
- **The outputs:** JSON through the writers from Arrow (R3); the Arrow answer in the plan's types (D7, its cases posted
  first).

*The order after D19 and D20 (2026-11-03), which reorders what follows:* (a) lite's own route on the three servers
(the plan record and Arrow in the plan's types; Python's plan-and-run its start); (b) the core's columnar decode (both
paths read it); (c) the apps' two adapters, Query's and Studio's results onto the typed model (R5's ground, done for
the three servers at once), DataCube's remote reads onto the lite adapter; (d) the edge writers exactly as upstream (step
3 below) with the CI switch; (e) then the edge's speed (the JSON writer's bytes, the copies), the rest of R4 and R5.

*R4's design (2026-11-03, after R3; D17 and D18 read onto it).* Its steps, in order:
1. **Dictionaries read** (R3's finding): `ArrowReader` reads a dictionary-encoded column (DuckDB answers an `ENUM` so;
   a dictionary pyarrow registers comes back plain), a replacement dictionary taking the place of the one before, a
   delta refused by name.
2. **Python's JSON execute hands Arrow** (`engine.py` `_execute_json`: DuckDB's record batches written as one IPC
   stream, read by the core where it lies). Its one change of text: a `FLOAT` column's digits, the JVM's and
   legend-engine's (`0.1`) where Python's answered the widened double (§7). A keyed pivot keeps the text handoff until
   `keyedAnswer` takes Arrow.
3. **legend-engine's `ARROW_IPC` answer as legend-engine writes it** (D17: an edge writer; upstream read in the pinned
   sources: `RelationalResultToArrowIPCSerializer`, `RelationalResult.getValue`, `SQLResultDBColumnsMetaData`):
   - **each column typed from the database's JDBC report alone** (its type code, precision, scale): `BIT`/`BOOLEAN`
     bool; `TINYINT` int8; `SMALLINT` int16; `INTEGER` int32; `BIGINT` int64; `FLOAT`/`REAL` float32; `DOUBLE`
     float64; `NUMERIC`/`DECIMAL` decimal128(p, s) (p 38 where reported 0 or less, capped at 38; s at least 0, at
     most p); `DATE` date32; `TIME` time32[ms]; `TIMESTAMP` and `TIMESTAMP_WITH_TIMEZONE` timestamp[us, "UTC"];
     anything else utf8; every field nullable, with no metadata;
   - **each value as upstream writes it**: a decimal `setScale(s, HALF_UP)`; a timestamp's microseconds (finer digits
     dropped); a time's milliseconds; a utf8 cell the text of the driver's value (an array or a JSON or map value its
     JSON text, a binary value its hex);
   - **the schema's metadata** `legend.builder`, `legend.activities`, `legend.generationInfo` (when present),
     `legend.columns`, in the order Java's `HashMap` gives them; batches of 1,024 rows; one zstd frame;
   - **the report:** on the JVM the result's `ResultSetMetaData` (the session already reads it for a dynamic pivot);
     on a host DuckDB's own type name (duckdb-python's `description`, DuckDB-WASM's `DESCRIBE`) read as DuckDB's JDBC
     driver reports it -- measured for every DuckDB type (2026-11-03, `duckdb_jdbc` 1.5.6.0): a `HUGEINT` (a sum of
     integers) `OTHER` and so utf8, `UTINYINT` `SMALLINT`, `USMALLINT` `INTEGER`, `UINTEGER` `BIGINT`, `UBIGINT`,
     `UHUGEINT`, `INTERVAL`, `UUID`, `JSON`, `MAP` and `ENUM` `OTHER`, `FLOAT` `FLOAT`, every `TIMESTAMP_*`
     `TIMESTAMP`, `TIMESTAMPTZ` `TIMESTAMP_WITH_TIMEZONE`, `TIMETZ` `TIME_WITH_TIMEZONE`, a list `ARRAY`, a struct
     `STRUCT`, `BLOB` `BLOB`, `DECIMAL(p,s)` its p and s;
   - **where upstream fails, lite answers** (D18: an extension, a register row): upstream reads a `TIMESTAMPTZ`
     column's value as DuckDB's `OffsetDateTime` and casts it to a `Timestamp` (a `ClassCastException`); lite writes
     its microseconds at UTC.
   It changes the JVM's `ARROW_IPC` answer (today lite's own Arrow, the plan's types) column kind by column kind, each
   listed in its commit and posted.
   **Its cases, posted to the user 2026-11-03 before anything is built (D7), each `before -> after` on the conformance
   corpus (DuckDB; upstream's rules above, DuckDB's JDBC report measured):**

   | Column | The JVM's answer today | Python's today | Upstream's, both after |
   |---|---|---|---|
   | an `INTEGER` (`TYPES.ID`, `TRADES.QTY`) | int64 | int32 | int32 |
   | a `TIMESTAMP` (`TS`, `BOOKED`, a min of one) | timestamp[ns, UTC] | timestamp[us], no zone | timestamp[us, UTC], digits past a microsecond dropped |
   | a sum of integers (`groupSum.q`, DuckDB's `HUGEINT`) | decimal128(38,0) | decimal128(38,0) | utf8, its digits as text |
   | a computed decimal (`extendArith.d2`, `D * 2`) | decimal128(29,2) (the typer's) | decimal128(18,2) | decimal128(18,2) (DuckDB's report) |
   | a `FLOAT` (none in the corpus) | float64 | float32 | float32 |
   | a `TIME` (none in the corpus) | time64[ns] | time64[us] | time32[ms] (upstream itself fails on DuckDB's `LocalTime`: lite answers, a register row) |
   | a `TIMESTAMPTZ` (none) | timestamp[ns, UTC] | timestamp[us, UTC] | timestamp[us, UTC] (upstream fails on DuckDB's `OffsetDateTime`: lite answers, a register row) |
   | a list, struct or map (none) | Arrow's nested types | DuckDB's nested types | utf8, its JSON text (upstream: an array's JSON by Jackson; a struct's and a map's to be read before they land) |
   | `UBIGINT`, `UUID`, `INTERVAL`, `JSON`, `ENUM` (none) | the plan's types | DuckDB's | utf8 |
   | every other corpus column (`BIGINT`, `DOUBLE`, `DECIMAL(p,s)`, `VARCHAR`, `BOOLEAN`, `DATE`) | unchanged | unchanged | unchanged |
   | each field's metadata | `legend.*` per field | none | none; every field nullable |
   | the schema's metadata | insertion order | insertion order | Java `HashMap`'s order |

   *The question with them:* whether lite's own consumers that read `ARROW_IPC` today (DataCube asking Python's engine)
   keep reading it -- and then see upstream's types (a sum of integers as text, as from a real legend-engine) -- or
   move to lite's own Arrow in the plan's types (D17's reading). The fourteen schema entries of the ledger go when both
   the JVM and Python write upstream's answer.
4. **Python's Arrow execute onto the record and the edge writer**: `arrowPlan`, `lite_execute_plan` and Python's own
   Arrow writer (`engine.py` `_arrow_ipc`) retired, the census at zero; the fourteen schema entries of the ledger gone
   (Java and Python both upstream's).
5. **Python's own Arrow in the plan's types** (`Table.execute`, plan-and-run; D7, D17): each change posted.
6. **Python's runner**, the tab's twin, for the headless page's writes.

**R5. The tab's shell.**
- **The runner** over its connectors: DuckDB-WASM, the warehouse, a remote engine's Arrow.
- **The grid:** H1b's steps 1 to 5 (§3.6a).
- **Studio's and Query's execute:** through the writers from Arrow (R3).
- **Writes:** carriage; `rowsOf` goes, the answer is the writers'.

**R6. The deletions.** `hostRows`, `compiler.host_rows`, `rowsOf`, `PureV1Api.jsonAnswer`'s text input, the
parse-backs (`numberOf` and its kin reading text), and the retired plan shapes, each with a census that reads zero.

**R7. Snowflake.**
- **Python:** the ADBC connector (§4.1).
- **The tab:** the SQL API connector (§4.2), its CORS checked from a real browser first, its proxy the same code.
- **The judge:** the suite on the Snowflake lane, against Java's answers through snowflake-jdbc.

**R8. Budgets.**
- The H0 benchmarks, and a benchmark of an interaction end to end (plan, SQL, decode, draw) at the sizes DataCube
  fetches (up to 1,001 rows a level, `DEFAULT_MAX_ROWS`) and an uncapped flat grid.
- A budget each, in a manual lane.
- A speed fix only where a measurement shows one.

**What is done already.**
- **H0.** The baselines and the code map.
- **H1a.** The tab's column decode, R5's first piece. Every value is the same; an audit found one defect, fixed.
- **H1b's design (§3.6a).** R5's plan for the grid.
- **R1.** The conformance suite and its first ledger.

## 6. Rules

- No output's spelling changes without the user's word, asked case by case with the expression and its before and after.
- A guard's pin or allowlist moves only with a dated justification (AGENTS.md; `docs/FOUNDATIONS_PLAN.md` §0.4).
- Credentials are never read, printed or copied by a session; the Snowflake lane measures only.
- The start page's checks run before each commit; records land with the code.
- The core does no input or output (P1): no export of the embedded boundary reaches a connection, a socket or a file.

## 7. Open questions and findings

- **The per-cell decoder's raw read on a sliced vector** (found 2026-11-02, H1a). `duckdb.ts` `rawAt` reads
  `values[offset + i]` from a value buffer Arrow-JS has already sliced to the chunk, so a sliced date, time or timestamp
  vector reads past its values and throws. No product path decodes a sliced vector: every chunk DuckDB-WASM and the
  warehouse hand over is at offset 0. Recorded, not changed; H1's move to Arrow results retires the per-cell decoder.
- **A timestamp past JavaScript `Date`'s range spelled as `NaN` text** (found 2026-11-02 by H1a's audit). Arrow-JS's
  `get` accepts a timestamp up to ±2^53 milliseconds, beyond `Date`'s ±8.64e15. The per-cell decoder's `timestampText`
  then makes `"0NaN-NaN-NaNT..."` of one past year 275,760 (`TIMESTAMP '287396-10-12 08:59:00.991999'`): a silent
  default. It predates the program, and the column decode answers the same. Its fix is a refusal, and so a behaviour
  change, to be posted to the user with H1's Arrow results.
- **Seven queries of the planner's corpus are refused on both the JVM and WebAssembly** (found 2026-11-02, R2's
  survey): `extendArith`, `extendConcat`, `extendThenFilter`, `pipelineExtend`, `extendTwo`, `extendChained` and
  `deepPipeline` in `wasm/corpus/queries.tsv`, each with "Collection element must have a multiplicity [1], found
  [0..1]" (arithmetic or a concatenation over a nullable column). The differential test passes, since both builds
  refuse alike. *Settled 2026-11-03:* legend-engine and legend-pure refuse the same queries (§1.3 C), so the refusals
  are faithful and stay; their words are legend-engine's since D18 C (`Context:[Applying times], multiplicity:[0..1]`,
  register S128).
- **The hosts send a statement without legend-engine's execution-trace line** (found 2026-11-03, by B's conformance
  run). legend-engine's `RelationalExecutor` puts `-- "executionTraceID" : "<id>"` before every statement it runs, and
  the JVM's seam does the same (`exec/ExecutionTrace`); the tab and Python send the plan's statement alone. Answers are
  the same but for a database's message that counts lines (DuckDB's `LINE 4` on the JVM, `LINE 3` on a host): two
  ledger entries. The fix is the hosts' runners' (R4/R5): the statement stamped by the core, its id the answer's
  activity's, so a host's statement and its answer carry one id as the JVM's do.
- **The text handoff and the Arrow handoff answer differently in these cases, each toward the JVM's answer** (R3 and
  its audit, 2026-11-03; pinned in `//python:arrow_handoff_test`), so switching a host's JSON execute to Arrow (R4, R5)
  changes that host's text there:
  - a `FLOAT` (next item);
  - an infinite `DATE` or `TIMESTAMP`: the text handoff answered duckdb-python's stand-in (`9999-12-31`), the Arrow
    handoff DuckDB's own value (`5881580-07-11`, `294247-01-10T04:00:54.775807000+0000`; `-5877641-06-24` and
    `-290308-12-21T19:59:05.224193000+0000` for minus infinity), as the JVM's JDBC read gives them;
  - a BC date: the text handoff refused the whole answer (500: duckdb-python spells `0054-05-07 (BC)`, no date the core
    reads), the Arrow handoff answers it (`-53-05-07`).
- **Where the Arrow decode and the JDBC decode part** (R3's audit, measured on DuckDB 1.5.5/1.5.6): a HUGEINT under a
  column with no slot and a Pure type of `Number` or `Any` is a decimal from Arrow and an integer from JDBC (DuckDB's
  Arrow carries a HUGEINT as a decimal of scale 0, indistinguishable from a `DECIMAL(38,0)`); a TINYINT under a Boolean,
  Decimal or Number column, and a UUID under a String column with no slot, are read from Arrow where JDBC's decode
  refuses the driver's `Byte` and `UUID` (gaps in the JDBC decode's arms). Recorded, not changed. (Fixed instead: a text
  slot reads the driver's own text; an unsigned 128-bit integer and a zoned time are refused.)
- **A REAL's digits differ between the hosts and the JVM** (found 2026-11-03, R3). A DuckDB `FLOAT` (four bytes):
  the JVM's session reads `Float.toString`'s digits (`0.1`), as legend-engine's JDBC read does; Python's JSON execute
  hands the widened double (`0.10000000149011612`, duckdb-python's `fetchall`), and the tab's by the same mechanism (a
  JavaScript number is the widened double; not measured). The conformance corpus has no `FLOAT` column, so R1 did not
  see it. The Arrow handoff answers the JVM's digits (`//python:arrow_handoff_test` pins both); so switching a host's
  JSON execute to it (R4, R5) changes that host's text for a `FLOAT` column. Posted to the user before the switch.
- **A dictionary-encoded column is refused by the reader** (2026-11-03, R3): DuckDB answers an `ENUM` as an Arrow
  dictionary, which `ArrowReader` refused by name; the text handoff read it as its strings. (A pandas categorical a frame
  registers does not reach it: frames hand DuckDB Arrow, and DuckDB reads an Arrow dictionary back as plain text,
  measured.) *Settled by R4 step 1 (2026-11-03): the reader reads a top-level dictionary-encoded column through its
  dictionary, a replacement dictionary taking the place of the one before; a delta and an encoded field inside another
  are refused by name.*
- **Python's 1,000,000-row JSON answer takes about 2 s, and most of it is the core's own work** (measured 2026-11-03,
  scratch probes, reverted; H0's five columns; the answer 84 MB of legend-engine's JSON, the rows 50 MB of Arrow):

  | Stage | Native library (Python) | Same code on the JVM |
  |---|---|---|
  | plan | 5 ms | |
  | DuckDB runs the statement, its Arrow (one IPC stream) | 100-150 ms | |
  | the core reads the stream's batches (views only) | | 0.06 ms |
  | the core decodes 5,000,000 cells into batches | | 190 ms |
  | ... and writes legend-engine's JSON, then copies it out | 1,640-1,800 ms | 690 ms |
  | ... or writes lite's own Arrow and zstd instead | 870-900 ms | 454 ms |
  | Python: the answer's bytes to a `str` and back to bytes | 67 ms | |

  So: "zero copy" is the input alone; the time is (a) the native image running the core about 2.5 times slower than
  the JVM's JIT (GraalVM Community Edition: serial GC only, no profile-guided optimization, `-march=compatibility`),
  (b) the decode boxing every cell into an object and a list per row that the batch then splits back into columns, (c)
  the JSON writer spelling 84 MB as characters (~170 MB/s on the JVM) before UTF-8, and (d) the answer copied about
  five times on its way out (Java string, UTF-8 bytes, the folded array, the native buffer, Python's bytes, `str`, bytes
  again). Under D19 lite's own consumers take lite's Arrow (DuckDB's own 100 ms is the floor there), and the JSON
  answer is the edge's -- both to be made fast. Posted to the user with the fixes proposed, in order: a columnar decode
  straight into the batch's typed arrays; the writers writing UTF-8 bytes into one buffer; the answer written into the
  host's buffer once, kept as bytes in Python; then the native image's own settings (its young generation, the CPU
  baseline), each measured.
- **Two exports with no caller and no test** (found 2026-11-02, while checking the Arrow writer runs in both embedded
  builds): `executeArrowAnswerOrError` in the tab's module and `lite_execute_arrow_answer` in the native library. Both
  answer correctly (§3.6). R3 gives them callers, or R6 deletes them. *After R3 (its audit): `lite_execute_arrow_answer`
  has a test caller (`//python:arrow_handoff_test`), `executeArrowAnswerOrError` still none; R3's four new exports
  (`executeJsonAnswerFromArrowOrError`, `executeArrowAnswerFromArrowOrError`, `lite_execute_json_answer_arrow`,
  `lite_execute_arrow_answer_arrow`) have test callers only until R4 (Python) and R5 (the tab) give them product
  ones; R6 deletes whichever text handoff export is left without one.*
- **The two adapters differ in two classes, and no other** (found 2026-11-03 by the conformance suite's `adapters`
  check, D20 (c) step 1; both ledgered):
  - *A `Decimal` cell's scale.* Expression: `#>{conf::DB.TYPES}#->select(~[ID, D, DL])` (`D` a `DECIMAL(10,2)` holding
    `12.30`). Upstream's answer: legend-engine's JSON writes the number with its scale, `12.30` (lite's edge writer
    copies it: `CellText.writeEngine`, `toPlainString`). The engine adapter today: the JavaScript number `12.3`
    (`values.ts` `parseExact` keeps a number's text only where a double would change its value, so the scale goes);
    `0.00` reads `0`, `-0.01` a number. The lite adapter: the text `"12.30"`, as the tab's Arrow planes have it. So the
    same cell differs by connection on DataCube's engine plane today -- what the grid shows (`12.3` against `12.30`),
    and the literal a filter from the cell writes. Proposed (posted to the user, not built): the engine adapter reads a
    `Decimal` column's numbers by their digits as written, so both adapters hold `"12.30"`; on the engine plane a
    decimal then shows and filters with its scale, as on every other plane. Eight cases, both servers: `typesAll`,
    `typesSelect`, `filterDecimal`, `extendArith`, `literals`, `groupSum`, `groupMinMax`, `limit`. *Ruled D21 Q1 and
    built 2026-11-03 (§8): both adapters hold `"12.30"`.*
  - *A pivot whose columns its data names* (`pivotDynamic`): the engine adapter reads it; the lite adapter refuses it by
    name (`recordPlan`: a host's plan is one typed result). DataCube plans its pivots static, so none of its queries is
    one; Query and Studio run whatever a person writes, so their move onto the adapters (step 3) reads it, each column
    typed by its field's `legend.type` (the plan's type, which lite's writer writes beside every field).
- **Query's and Studio's cells on the typed result: what would change** (measured 2026-11-03 on the conformance corpus,
  Java's server: each cell as Query's grid spells it, `cellText`, and as Studio's run view does, `String(v)` -- today
  over legend-engine's JSON read by `JSON.parse`, against the same function over each adapter's typed result; posted
  to the user, not built). Five classes, and no other cell changes:
  1. *An integer past 2^53* (`typesAll`, `I` = `9007199254740993`): today Query `9,007,199,254,740,992` and Studio
     `9007199254740992` -- wrong digits, the double's; typed, both adapters, `9007199254740993`, without Query's
     grouping (a bigint is not a JavaScript number). An integer a double holds exactly but past 2^53 (`extendArith`,
     `9007199254740994`) loses only the grouping.
  2. *A decimal a double cannot hold* (`typesAll`, `DL` = `12345678901234567.89`, `99999999999999999999999999999999999.99`):
     today Query `12,345,678,901,234,568` and `100,000,000,000,000,000,000,000,000,000,000,000`, Studio
     `12345678901234568` and `1e+35` -- wrong; typed, both adapters, the exact digits, ungrouped.
  3. *A decimal's scale* (the lite adapter; `D` = `12.30`): today `12.3`, lite `12.30` (the previous finding's class);
     and a decimal held as text loses Query's grouping (`99,999,999.99` -> `99999999.99`).
  4. *A timestamp* (both adapters): today legend-engine's wire text, `2024-01-02T08:30:00.000000000+0000`; typed, the
     stored text DataCube shows, `2024-01-02T08:30:00` (UTC as stored, the fraction to the microsecond, its trailing
     zeros dropped: `values.ts` `timestampFromText`).
  5. *A `Number` column holding decimals* (`groupMinMax`, `hi`): as class 3.

  Upstream's answer for the display is legend-studio's, whose sources are not among the pinned checkouts; the wire text
  in class 4 is legend-engine's. Proposed: one spelling of a cell from its value and its column's type, shared by Query
  and Studio over either adapter -- the exact digits always (1, 2), Query's grouping kept for every numeric column and
  applied to the exact digits (`9,007,199,254,740,993`, `12,345,678,901,234,567.89`), a decimal with the scale the result
  holds (3, after the previous finding's fix the same on both adapters), a timestamp as the stored text (4), Studio
  ungrouped as today.
- **R4 step 5's cases: Python's `Table.execute` on lite's own Arrow** (measured 2026-11-03 by a scratch probe, deleted:
  one frame of every kind of column, three queries, `Table.execute` today -- DuckDB's Arrow typed by the plan,
  `frames.typed_by_plan` -- against lite's own execute for the same query; posted to the user, not built). Every other
  column the same, type and value:
  1. *A `FLOAT` (DuckDB `REAL`) column:* float32 today (its four-byte value, `0.10000000149011612` in Python), lite's
     float64 of its own digits (`0.1`), as the JSON answer has had since R4.2.
  2. *A `DateTime` column:* timestamp[us] with no zone today, lite's timestamp[ns, UTC]: Python's values zone-aware
     (`datetime(..., tzinfo=UTC)`), pandas' `datetime64[ns, UTC]`.
  3. *A frame's zoned timestamp* (timestamp[us, UTC] in): `Table.execute` drops its zone today; lite keeps UTC.
  4. *A sum of integers* (DuckDB's `HUGEINT`): int64 today (`typed_by_plan` narrows it), lite's decimal128(38, 0) (its
     `HUGEINT` slot: a Pure Integer is unbounded): Python's values `Decimal`, pandas' `object`.
  5. *Each field's metadata:* `legend.type` alone today; lite's adds `legend.multiplicity`, `legend.slot` and
     `legend.precision`, and spells a precision decimal's type as the typer does (`Decimal(10,2)`, today `Decimal`).

## 8. Status

- 2026-11-02: ratified (rev 1); H0 started (branch `claude/wizardly-dirac-rw6tim`, from `main` at `cf73e6ef`).
- 2026-11-02: H0's baselines measured (`docs/GATES.md`, "The host executors' baselines (H0)"). At 1,000,000 rows the
  tab's decode takes 3,384 ms over DuckDB-WASM's 161 ms. Python's JSON execute takes 23.2 s against its Arrow
  execute's 188 ms.
- 2026-11-02: H1a, the tab's column decode: every value the same (`//engine-client:column_decode_test`, the
  benchmark's digests); `execute` 2.6x to 4.4x faster from 10,000 rows (3,384 -> 1,036 ms at 1,000,000). Three bundle
  budgets moved by about 2 KB each, with dated notes. GATES "H1a: the tab's column decode".
- 2026-11-02: H1a's audit folded: an infinite timestamp is refused again, as before (the audit's one defect); a
  malformed chunk is read cell by cell; the ASCII path bounded. GATES item 7.
- 2026-11-02: rev 2, after the user's step back: the principles (§1.1), D6-D10 proposed (§1.3), the roadmap R0-R7 and Z
  (§5). R1, the conformance suite, comes before any host changes.
- 2026-11-02: R1 built: `//datacube:conformance_test` over `//conformance`, green against its first ledger of
  seventeen entries (Python's Arrow schema, fourteen; one database refusal, three); GATES "R1". The user then asked
  whether the design makes every host depend on Java past the planner ("each host also has a dependency on the java
  executor ... which is not going to work"): a revision to a one-way design (the plan in, every output the host's own,
  held by this suite) is posted to the user, and D2, D6 and D8 stand open until the user rules.
- 2026-11-02: rev 3, the clean-sheet design, ratified by the user ("Yes I love it"): one core embedded in every host
  doing no input or output (the planner and the writers), Arrow between the pieces, thin host shells (§1, §3); D6-D15
  ruled; the roadmap R2-R8 and Z (§5). Before it, the user's two questions (rev 3's preface) and the scratch probe of the
  Arrow writer in both embedded builds (§3.6).
- 2026-11-02: R2 started. The survey (the old shapes' callers and the fields they read; the grid's SQL equal on the
  record for every corpus query). D16 ruled: the plan's columns gain only the enumeration fact, a decimal's plain name
  read from its Pure type by one rule. R2's first commit: the fact (plan version 20), the hosts' JSON execute on the
  plan record, its answer's header from the plan alone.
- 2026-11-02: R2's first step done (GATES "R2, first step"): the enumeration fact, the hosts' JSON execute on the plan
  record, no output changed; 51 of 53 checks green, the other two a V8 crash in this container that the commit before R2
  shows too.
- 2026-11-02: R2, the rest (commits `1aa8e7d1`, `62a86074`): every host path but Python's Arrow execute on the plan
  record; HostPlans (D6, read and program) refusing at planning what a host does not run; the plan doors proved on
  the tab's module and Python's native library. The local gate and the affected suites green but for the
  container's: the Postgres tests (initdb refuses root), the V8 crash two Python suites show at the commit before R2
  too. The open decisions A-G posted to the user with upstream's answers (§1.3).
- 2026-11-03: D18 ruled (§1.2), and D12 rewritten (the `Keyed` node is the stepper). C built: the nullable-column
  refusal in legend-engine's words, `Collection element must have a multiplicity [1] - Context:[Applying <the call>],
  multiplicity:[0..1]` (`compiler/spec/CollectionElementRefusal`, raised by `Typer.collection` and given the innermost
  call's name in `Typer`'s applied-function arm); its context a register row (S128). The tests that pinned lite's old
  words follow (core's compiler and lowering lanes, DataCube's `column_editor`, `infer`, `calc_fix`); DataCube's fix
  offer matches both words, as before. Rows S129-S131 record B's trace, D's two statements and H2's dynamic pivot.
- 2026-11-03: B built: a database's refusal at run time answered on every host as legend-engine answers it -- 500,
  `{"code":-1,"message":"<root cause's short class name>: <its message>","status":"error","trace":...}`. The JVM names
  the driver's root cause (a `DataError` among the failure's causes, walked to the end; `ClassUtils`' short name) and
  writes its own stack as `trace`; a host hands the core the plan it ran, the database's words and its own account of
  the failure (`PureV1Api.databaseRefused`, exported as `lite_execute_refusal` and `executeRefusalOrError`), the class
  the one legend-engine's JDBC driver for the plan's database throws (`Databases.jdbcFailureClass`: DuckDB's
  `SQLException`; any other refused by name). The tab's execute throws legend-engine's error (an `EngineError` of
  status 500); Python's JSON execute answers it. Before and after, on `errTypeMismatch`
  (`->filter(x|$x.QTY == 'notanumber')`): Java `SQLException: Conversion Error: ...` (unchanged but for `trace`),
  Python `ConversionException: Conversion Error: ...` -> `SQLException: Conversion Error: ...`, the tab a `QueryError`
  of no status with DuckDB's words alone -> 500 with `SQLException: Conversion Error: ...`. Lite's other failures
  keep their words and carry no trace (S129). The ledger: three entries gone, two narrower ones in (the trace line,
  §7); sixteen entries. Python's Arrow execute keeps its own words until it moves onto the plan record (R3/R4).
- 2026-11-03: F, part 1: the JVM's JSON and Arrow writers make legend-engine's header for a plan that states its
  columns from the plan's own facts (`PureV1Api.planEnvelope`, the one maker every host's answer uses), no longer from
  the compiler's types; every output the same (`PureV1ApiTest`'s differential of the two makers; the PCT lanes that read
  the JVM's answer as legend-engine's adapter does, `//pct:pct_duckdb_essential`, `pct_duckdb_relation`, `pct_h2`,
  green). A dynamic pivot's header is part 2.
- 2026-11-03: F, part 2: a dynamic pivot's answer header on the JVM made as legend-engine makes it
  (`RelationalResult.buildTransformersAndBuilder` with `isResultColumnsDynamic`): EVERY column rebuilt from the
  database's report, its label and the type `ResultColumn.labelTypePair` reads from the driver's type name, with no
  relational spelling. The report travels with the columns: `Statements.rows` hands the result's reported columns over
  once it opens (JDBC's `getColumnTypeName`; MongoDB reports none, S132), the keyed run announces its columns with them
  (`ResultSink.columns(columns, reported)`, through `Batcher.start(reported)`), and the JSON and Arrow writers make
  their header from them (`PureV1Writer.dynamicRelation`, `ArrowWriter`'s report-taking constructor,
  `PureV1Api.dynamicEnvelope`). Measured (`ExecutorTest.pivotDynamicHeaderIsTheDatabasesReport`),
  `T_TREES->pivot(~CITY, ~[total: sum, mean: average])` on DuckDB: every builder column was `{"name", "type",
  "relationalType"}` (the old maker, `builderColumn`) and is `{"name", "type"}`; `YR` `Integer` (DuckDB's `INTEGER`),
  the sums `Integer` (`HUGEINT`), the averages `Float` (`DOUBLE`). By the rule, not yet measured on a run: a column
  DuckDB reports `DECIMAL(p,s)` is `Float` where the plan said `Decimal`, and one it reports `BOOLEAN` is `String`
  (none of legend-engine's prefixes matches it; its own last line answers String, copied as it is). The values are the
  database's, unchanged. A pivot whose template is a precision decimal, refused by the old maker ("is no primitive"),
  is answered. The compiler-types maker (`PureV1Api.envelope`) is now used by the differential test alone; it goes
  with R2's census.
- 2026-11-03: D built (commits `de533ae5`, `df357180`, and the hosts' step): a pivot whose columns its data names
  answers on every host as on the JVM. The keyed run's pure steps are the plan record's (`KeyedRun`); the execute
  route's plan half answers a keyed plan (`HostPlans.query`; the grid's `HostPlans.read` keeps refusing it); the host
  reads the keys, the core writes the body (`keyedBody`), the host `DESCRIBE`s it and runs it with the keys bound on a
  connection held for it (the tab's `exclusive`, now part of its engine interface and forwarded by Query's and
  Studio's project engines; Python's cursor in one transaction), and the core writes legend-engine's answer from the
  rows and the report (`keyedAnswer`). Proved: `ExecutorTest.pivotOnAHostAnswersTheJvmsText` (the host path's answer is
  the JVM's text but for the trace id), and the conformance suite's new `pivotDynamic` case, whose JSON answer is the
  same text on the JVM, Python and the tab. Its two ledger entries: Python's Arrow execute (the old shape until R3/R4)
  and the grid's refusal (DataCube plans its pivots static). Ledger 16 -> 18.
- 2026-11-03: R2 closed. The compiler-types header maker left the product (`5bb0f492`). An independent audit of R2's own
  commits found no defect in the product paths and these, folded:
  - **A regression, fixed:** Query's Show SQL went through the grid's door, which refuses a plan the grid does not run,
    so an uncast dynamic pivot's SQL could no longer be shown (it was before R2). Show SQL has its own door again
    (`sqlModel`: `Boundary.sqlJson`, the same `Compiler.plan` the server's `generatePlan` uses; exported by the tab's
    module and the native library).
  - **The census, built:** `plan-runner.test.ts` names every export of the planner whose name says it plans, each
    answering the record or named (a new one fails until it is listed), beside the door tests on both hosts.
  - **Tests for every refusal moved to planning** (`HostPlansTest`: per-key regions, a key's cell, a keyed root and
    step, a nested sequence, an effect; `query`'s keyed profile); the door test now checks refusals too.
  - **Python's reader as strict as the tab's** (each column's name, type and enumeration fact; slots a list).
  - **Corrected claims:** D6's profile (above); `HostPlans`' refusals are the runner's, six of their words reworded; the
    header differential asserts its Boolean and date columns by name and has a Variant case; stale version notes
    (`plan-runner.ts`, `plan-record.ts`, `compiler.py`, `lite-planner.ts`, the native library's comments).
  - **Left as found:** commit `1aa8e7d1` does not compile on its own (restored in `62a86074`): bisection only.
  Python's Arrow execute keeps the old shape until R3/R4, as ruled.
- 2026-11-03: R3 built: the core reads a host's Arrow (`ArrowReader`, `ArrowRows`) and the boundary carries bytes both
  ways; the exit met on the suite for Python and the tab (§5 R3 "as built"); Python's JSON answer at 1,000,000 rows
  23.6 s -> 1.9 s through it. Two findings (§7): a REAL's digits, and dictionaries refused. No product path switched.
- 2026-11-03: R3's independent audit folded (§5 R3 "as built", its last items; §7): the exit test laned, the JSON
  decode's non-finite tokens, a text slot's text, two refusals, two reader checks, the claims scoped, the benchmark like
  for like; two R2 test defects the container had hidden found and fixed.
- 2026-11-03: R4 step 2 built: Python's JSON execute hands DuckDB's Arrow to the core (`engine.py` `_execute_json`;
  a keyed pivot keeps the text handoff). `//python:executor_bench`'s `json`: 1,000 rows 35.0 -> 21.2 ms, 100,000
  rows 1,855 -> 228 ms, 1,000,000 rows 23,626 -> 1,958 ms. Python's answer changes where the two handoffs part (§7):
  a `FLOAT`'s digits, an infinite date or timestamp, a BC date (500 -> its rows), each now the JVM's.
- 2026-11-03: R4 step 1 built: `ArrowReader` reads a dictionary-encoded column (DuckDB's `ENUM`; pyarrow's replacement
  dictionaries), judged in `//python:arrow_handoff_test`.
- 2026-11-03: R4 step 3's cases posted to the user (§5 R4, the table under step 3) with the question of lite's own
  consumers of `ARROW_IPC`; step 3 and step 4 wait on the ruling (D7). Steps 5 and 6 do not depend on it.
- 2026-11-03: D19 ruled (§1.2): everything lite builds runs on lite's own pieces, legend-engine's formats only at the
  edge for upstream's tools and users. R4 step 3 builds the edge writer exactly as upstream (its table) and moves lite's
  own consumers of `ARROW_IPC` to lite's own Arrow. The 1,000,000-row JSON answer's time measured and its fixes posted
  (§7).
- 2026-11-03: D20 ratified (§1.2): one UI over one typed result, two thin adapters, each connection talking lite or
  legend-engine; the order of the remaining work rewritten before R4's design (§5).
- 2026-11-03: F's scope question settled by D19 and D20 (the user: "Everything should use new lite plan right? Unless
  backwards compatible adapter?"). Posted after F part 2 -- whether a dynamic pivot's fixed grouping columns are also
  retyped from the database's report -- it concerns legend-engine's JSON answer alone, which is the edge and copies
  upstream: every column retyped from the report, as built (`PureV1Api.dynamicEnvelope`, `RelationalResult.java:248-256`).
  Lite's own route reads no report: a keyed pivot's columns are typed by the plan record (`KeyedRun.answer`), as every
  answer of lite's own is.
- 2026-11-03: D20 (a) built: lite's own execute, `POST /api/lite/v1/execution/execute`, on the JVM server and Python's
  engine. Its request is legend-engine's execute request (so a client's two adapters differ only in the route and the
  decode); its answer lite's own Arrow -- one uncompressed IPC stream in the plan's types, written by the core's
  `ArrowWriter` on both servers, the plan record in its schema's metadata (`legend.plan`) -- or a refusal as the execute
  route answers one. Python runs the plan's statement (a keyed pivot's keys and body, in one transaction) and hands
  DuckDB's Arrow to the core (`PureV1Api.liteAnswer`, `keyedLiteAnswer`), which writes the same answer the JVM writes.
  Judged by the conformance suite's new `lite` check: Java's and Python's answers alike on every case -- schema with
  each field's metadata, values, plan record -- but the one line DuckDB's refusal counts (the hosts' missing trace line,
  §7: one ledger entry, 18 -> 19). The tab's lite path is in-process (no route): the apps' adapters, item (c).
- 2026-11-03: D20 (b) built: the core's Arrow decode fills a column at a time (`ArrowRows`: each column's fill chosen
  once -- an integer read as longs straight into the batch's array, a double, a boolean, a 128-bit decimal that fits 64
  bits from its long, a text from the buffer it lies in -- every other column the cell decode, the definition the fast
  fills equal; `Batch.Builder` filled by column and row). 1,000,000 rows, H0's five columns: on the JVM, decode 190 ->
  105 ms, decode and legend-engine's JSON 691 -> 507 ms, lite's own answer 229 ms; in the native library from Python,
  the JSON answer 1,640-1,800 -> 1,460 ms, lite's own 625 ms; Python's whole lite execute 842 ms. The native image still
  runs the core about 2.7 times slower than the JVM: the next measurement.
- 2026-11-03: the native library's gap to the JVM, measured (scratch rebuilds, reverted; 1,000,000 rows): `-O3` takes
  lite's own answer 625 -> 565 ms and the JSON answer 1,460 -> 1,315 ms (about 10%); a 1 GB young generation on top
  changes nothing (579 and 1,323 ms), so the collector is not the gap: the image's code is (no JIT, Community Edition).
  The lever left is less work per cell, which every host gains -- lite's own answer written from the host's Arrow column
  by column, without a carrier per cell; the JSON writer writing bytes. `-O3` is not adopted (the wheel's build, for 10%).
- 2026-11-03: D20 (c), step 1 built: the lite adapter. engine-client's remote connection is declared per connection
  (`protocol`: `engine` or `lite`); a lite connection asks lite's own execute with legend-engine's request and reads the
  rows into the apps' typed result, each column typed by the plan record (`liteResultTable`), refusing by name what it
  cannot read. Judged by the conformance suite's `adapters` check: both adapters on both servers give the same typed
  result -- every column, type and value -- but for two named classes, ledgered (§7): a `Decimal` cell's scale, which the
  engine adapter's parse drops (`12.30` read as `12.3`; the JSON carries it), and a pivot whose columns its data names,
  which the lite adapter refuses. Next: DataCube's remote reads and Query's and Studio's results onto the adapters.
- 2026-11-03: D20 (c), step 2: DataCube against Python's engine talks lite (D19): `//datacube:python_engine_test`'s
  remote reads (the cube corpus, the written page) through the lite adapter, the legend-engine routes (JSON,
  `ARROW_IPC`) kept as the edge's checks and answering the same rows. Step 3 -- Query's and Studio's results onto the
  typed result -- changes cells they show today, so its cases were measured and posted to the user before any is built
  (§7, the next finding).
- 2026-11-03: D20 (d), the switch built: `LegendHttpServer --engine-only` serves legend-engine's routes alone (lite's own
  `/api/lite/v1/` unserved, 404), and Query's end-to-end server suite (`//query:verify_test`, its connection talked to as
  legend-engine; a CI Linux lane) runs against a server so started and passes whole. Still to come under (d): the
  `ARROW_IPC` answer written exactly as upstream writes it (R4 step 3's table), on the JVM and the hosts.
- 2026-11-03: R4 steps 3 and 4 built (D20 (d)): legend-engine's `ARROW_IPC` answer is written as upstream writes it
  (`result/EngineArrowWriter`: each field typed from the database's report of its column, every value as upstream sets
  it, the metadata in Java HashMap's order, 1,024-row batches) on the JVM, Python and the tab alike; the report the
  runner's (JDBC) or the host's (DuckDB's `DESCRIBE`, read as DuckDB's JDBC driver reports it). Python's Arrow execute
  is on the plan record; `arrowPlan`, `lite_execute_plan` and Python's own Arrow writer are deleted. The conformance
  ledger's 14 schema entries are gone (37 -> 23), none new. Register: S125 removed; S133-S135 added. Not yet as
  upstream (each refused by name, never written another way): a struct's, a map's and a binary value's text, a list of
  anything but text, whole numbers and booleans (upstream's text read before each lands), and on Python's route a pivot
  whose columns its data names (the JVM answers it).
- 2026-11-03: R4's audit folded (fix forward, the user's go): a dynamic pivot's `ARROW_IPC` fields named as upstream
  labels them, on every host (Python now answers the pivot as the JVM does); a plan of several steps answers again
  (its result step's report handed over); Jackson's spellings copied (upper-case hex, a JSON column re-spelled, lists
  of numbers) and a nested list refused; six more DuckDB spellings; the metadata in the order of Arrow's copy;
  upstream's two recorded answers compared (equal but for H2's own reported precision: lite pins H2 2.2.224, upstream
  2.1.214). Register: S28 and S134 removed, S136 added. The conformance ledger 23 -> 22.
- 2026-11-03: D21 recorded (§1.2) and Q1 built: the legend-engine adapter keeps a `Decimal`'s digits as the engine
  wrote them. Its JSON is read keeping each fraction's digits (`values.ts` `parseKeepingDigits`, a `WrittenNumber` the
  value and its text), and each column reads them by its type (`engine-remote.ts` `toResultTable`): a `Decimal` its
  digits (`12.30`, `0.00`, `-0.01` as written; a decimal written with an exponent refused by name), any other number
  as `parseExact` reads it. From `ARROW_IPC`, an Integer column that arrives as utf8 (a sum of integers DuckDB reports
  as `HUGEINT`, which JDBC reports `OTHER` and upstream writes as its text, §1.3 A) is read back as its integer. The
  conformance suite's `adapters` check now reads the engine adapter from `ARROW_IPC` as well as JSON: every column,
  type and value equal to the lite adapter's on both servers, but for the one class already ledgered (a pivot whose
  columns its data names, which the lite adapter refuses until Q2 reads keyed answers). The ledger 22 -> 10: the 14
  `Decimal` entries gone, the pivot's two `arrow` twins added (a check new with this step). The grid page's script
  budget 433,500 -> 434,000 bytes (measured 433,597: the adapter's new reader is in every remote grid's path; dated
  in `datacube/test/bundle-budget.test.ts`). What a person sees: on DataCube's engine plane, a `Decimal` cell now shows
  and filters with its scale (`12.30`, was `12.3`), as on every other plane. Next: Q3, then Q2.
- 2026-11-03: D21 Q3, its first part: lite's own Arrow writes an Integer as int64 whatever its slot -- a `HUGEINT` (a
  sum of integers) too, which wrote decimal128(38, 0) -- and a value past 64 bits is refused by name, never wrapped or
  rounded (`result/ArrowWriter`), on the JVM's lite route, Python's and the tab's alike (one core). Its second part,
  Python's `Table.execute` onto lite's own Arrow, is built and measured (a scratch probe over a frame of every kind of
  column: the five ruled cases and one more) and held: a Variant (JSON) column's text, DuckDB's text as stored
  (`{"a": [1, 2.50]}`) today against lite's compact JSON (`{"a":[1,2.50]}`), posted to the user, who asked why a
  Variant is text at all rather than a variant type; not landed until ruled.
- 2026-11-03: legend-engine's `ARROW_IPC` route frozen (D21, §1.2): a timestamp before 1582-10-15 is now refused by
  name (`EngineArrowWriter`), so everything the route answers is upstream's and S136 is a list of refusals, closed on
  a real client's need. Q3's second part (Python's `Table.execute`) still held for the Variant ruling.
- 2026-11-03: D21 Q3 done. Python's `Table.execute` answers lite's own Arrow (the plan record's statement run, its Arrow
  handed to the core's writer), as the JVM's and the tab's lite routes do: measured before and after on a frame of
  every kind of column, the changes are the five ruled (a `FLOAT` float64 of its own digits, a `DateTime`
  timestamp[ns, UTC], a frame's zone kept, an Integer sum int64 with one past 64 bits refused by name, each field's full
  metadata) and the Variant (`arrow.json`, its compact text: `{"a": [1, 2.50]}` read back as `{"a":[1,2.50]}`).
  Every other column the same, type and value. `typed_by_plan` remains for plan-and-run (DataCube talking to Python),
  the next step, its cases posted first. The TeaVM build's heap 2g -> 3g: the tab's planner has grown and its live set
  measured about 1.8 GB, so the action failed for memory at 2g (CI runs it at the same heap).
- 2026-11-05: D22 built: legend-engine's `ARROW_IPC` answer is the JVM server's alone. `PureV1Api` takes the engine
  Arrow writer from its host (`EngineArrow`; the server hands in `EngineArrowWriter`), and without one refuses the
  format by name before anything compiles; the hosts' Arrow-answer entries are deleted (`PureV1Api.arrowAnswer`,
  `keyedArrowAnswer`, the boundary's and the tab's exports, three native exports, Python's two Arrow routes and its
  wrappers, `ColumnReport.duckDb`), and Python routes `ARROW_IPC` to the core's refusal. The tab's planner no longer
  compiles the writer: TeaVM's live set 1.9 -> 1.17 GB, its heap back to 2g (3g since 2026-11-03), the WebAssembly 31 KB
  smaller. The conformance suite reads Java's `ARROW_IPC` and checks Python's refusal; the ledger 10 -> 9 (Python's
  engine-Arrow pivot row gone). Register S137 added, S136 the server's. Next: plan-and-run onto lite's own Arrow (its
  cases posted first), then Q2.
