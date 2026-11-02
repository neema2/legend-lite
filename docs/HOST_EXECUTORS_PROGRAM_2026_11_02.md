# The host executors: one plan, one set of rules, one result, one owner of each spelling -- in Java, TypeScript and Python

**Status: rev 2, 2026-11-02, after the user's step back** (below). Rev 1's five rulings (D1-D5) stand, ratified with
the user that day. Rev 2 adds the principles (§1.1) and five decisions proposed for the user's ruling (D6-D10, §1.3).
It also replaces rev 1's order with the roadmap (§5). A change to a principle or a decision is a decision, recorded here
with its date and reason, never an edit in passing. Status lives in §8.

The user's words that started it (2026-11-02): *"Let's start the typescript and python executors, let's look at how to
make those executors similar to the java executor in design as well if you think that makes sense? And for both let's
see if we can also add calling snowflake as remote engine too not just duckdb/warehouse - for TS we should use snowflake
http sql API the snowflake people have told me they do support cors even though not documented, and for python make sure
we use whatever gives arrow natively so it's fast."*

And the step back, the same day: *"take a giant step back to know what we want to achieve, fast and safe execution but
more importantly good clean design and consistency between java python TS/browser - so maybe I've been pushing the
performance angle too much, what is the right roadmap unbiased let's write down full roadmap and then execute on it
expertly."*

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

### 1.1 The principles

- **P1. Decide once, in Java; carry in each host.** Whatever decides is Java's, reached by every host through the
  planner it embeds: a type, a plan, a plan's review and its refusals, a value's spelling on any wire. A host's own code
  moves data. It opens a session, runs a statement, turns its database's wire into the result (P3), and hands the
  result to a consumer, or to Java's writers when text is wanted. That is the repository's tenet ("Java orchestrates,
  the database executes"; `docs/WORLD_MAP.md`'s "compare, never compute") applied to the hosts.
- **P2. One plan.** The plan record's JSON (`legend-lite-plan`, `PlanJson.VERSION`) is the only thing a host runs.
- **P3. One result.** A result is typed columns. Logically it is the plan's columns with their exact values. Physically
  it is Arrow, in the schema Java's `ArrowWriter` writes for the plan:

  | Pure type | Arrow |
  |---|---|
  | integers | `Int64` |
  | floats | `Float64` |
  | `Decimal` | `Decimal128(p, s)` |
  | `Boolean` | `Bool` |
  | `String` | `Utf8` |
  | `StrictDate` | `Date32` |
  | `DateTime` | `Timestamp(NANOSECOND)` |
  | `StrictTime` | `Time64` |
  | nested values | lists, structs and maps |

  The same plan on the same data is the same Arrow on every host.
- **P4. One owner of each spelling.**
  - **Wire text** (lite JSON rows, legend-engine's JSON and TDS, PureV1, CSV) is Java's alone (`CellText`).
  - **A value's exact text in the tab** (the literal a filter writes back, a pivot's column name, a saved document's
    value, an equality key) is `engine-client/src/values.ts`'s, held to Java's spelling of the same value by the
    conformance suite (P5).
  - **The text on screen** is DataCube's formatter's, from the typed value.
- **P5. Consistency is tested.** One conformance suite runs the same plans on the same data on every host and compares
  three things: the typed values cell by cell, the Arrow byte for byte, and any wire text byte for byte against Java's
  writers. A difference is a defect, or a row of `docs/SEMANTICS_REGISTER.md` the user ruled.
- **P6. Safe by construction.** No silent default. A refusal is named, and the same on every host. Every step runs
  under the suite. No output's spelling changes without the user's word, asked case by case.
- **P7. Fast by design, held by budgets.** Columnar end to end, nothing spelled before it is read. Each host's
  benchmarks are kept, with budgets, and the time of an interaction is measured end to end: plan, SQL, decode, draw.

### 1.2 The rulings (the user, 2026-11-02)

- **D1. The hosts' result is Arrow, columnar.** TypeScript's and Python's runners answer Arrow record batches, not Java's
  row batches.
- **D2. Text stays Java's.** JSON rows, PureV1 and CSV text is written by Java's writers (`CellText`), reached through the
  WebAssembly planner in the tab and the native library in Python, as today. The cost of handing the rows over is
  measured before the handoff changes.
- **D3. Snowflake authentication.** In the browser: OAuth or a programmatic access token, never a private key. In
  Python: key-pair, OAuth or a programmatic access token.
- **D4. The tab's Snowflake session goes direct, over CORS** (undocumented; the user has it from Snowflake), and the same
  session code can go through a proxy instead (the warehouse server, or Python's engine) should CORS be withdrawn.
- **D5. Python's Snowflake client is ADBC** (`adbc-driver-snowflake`), in a `snowflake` extra of its own.

### 1.3 Proposed, for the user's ruling (rev 2)

- **D6. A plan's review is Java's (P1).** Today the tab's runner repeats Java's checks in TypeScript (`plan-runner.ts`):
  the format and version, the bindings, the target, what a host may run. Python would need a third copy.
  - Proposed: the planner each host embeds answers the review: may this host run this plan, and if not, the refusal in
    words. The host keeps only the loop that runs the steps in order, binds a count and begins and ends the
    transaction.
  - The alternative: port the checks to each host and hold the copies to Java's by the suite.
- **D7. The result's Arrow schema is Java's (P3).** Python's Arrow answers today keep DuckDB's own types, with the Pure
  type in each field's metadata (`frames.py` `typed_by_plan`): an `Int32` stays `Int32`, a timestamp stays in
  microseconds. Aligning them to P3 changes the schema those answers carry, so each change is posted to the user case
  by case before it lands (an `Int32` widened to `Int64`, microseconds to nanoseconds).
- **D8. A host hands rows to Java's writers as Arrow.** Today Python hands them as JSON text (`compiler.host_rows`), and
  so does the tab (`hostRows`): every cell a Python or JavaScript value, then a token, then parsed again by Java. That is
  23 s at 1,000,000 rows in Python (GATES, H0).
  - Proposed: the handoff is the result itself (P3), as Arrow IPC, read in Java by a reader beside the hand-written
    writer (`arrow_ipc/`, no Arrow library in core).
  - That retires both JSON handoffs, and the tab's own lite-JSON writer for a write's answer (`plan-runner.ts`
    `rowsOf`): P4's wire text has one owner.
- **D9. The tab's result model is typed columns over the result's Arrow**, with the exact text made only when it is
  asked for (§3.6a).
- **D10. The conformance suite starts from today's behaviour.** Its first run records every difference between the hosts
  as a ledger. The ledger only shrinks: an entry goes by a fix, or becomes a register row the user rules. Nothing is
  "fixed" by changing an expected answer.

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

### 2.3 What is missing

- **No test compares a host's executed rows with Java's.** The checks that exist compare plans' SQL with the JVM's
  (`//python:bindings_test`, `//datacube:wasm_differential_test`), the tab's execute text with Python's, both written by
  the same Java writer (`//datacube:python_execute_test`), and each warehouse driver's HTTP calls with a recorded corpus.
- **No benchmark runs under Bazel** before this program (`datacube/bench/*.mjs` are only typechecked).
- **No Snowflake** in either executor.
- **Database types:** the tab runs DuckDB (`DuckDbEngine.databaseType`) and, through the warehouse, the catalog's type
  (DuckDB or Postgres); Python hardcodes `'DuckDB'` in five places (`frames.py:180,333`, `_host.py:14`, `projects.py:46`,
  `engine.py:1421`) and takes a warehouse table's from its catalog.

## 3. The design

### 3.1 One plan

Every executor runs the plan record the planner writes, as its JSON (`legend-lite-plan`, `PlanJson.VERSION`, 19 today).
The tab's three shapes (§2.1) become one: `Plan {sql, columns}` (`engine-client/src/relation-type.ts`, paths A, B, C,
F) and `ExecutePlan` (path E) are retired into the record, their callers moved to it, and the record's columns carry every
fact the decode needs (the Pure type and multiplicity, the SQL slot, the store's declared type, the written precision).
Python's `{sql, type}` and `{sql, metadata}` answers go the same way. The WebAssembly and native planners already write
the record (`executionPlan`, `lite_execution_plan_json_held`; §2). legend-engine's own plan (`UpstreamPlanner`, read from
an upstream server's `generatePlan`, `datacube/src/planner.ts:132-217`) is another planner's, and stays as it is.

### 3.2 One runner per language, with Java's rules

`engine-client/src/plan-runner.ts` already reads the record for a host's writes. It grows reads (`tdsResult`), and
Python gains its twin. Whether the review below is computed by Java and asked by the host (D6, proposed) or repeated
in each host, every runner keeps `PlanRunner`'s rules:

- The plan is reviewed whole before any session opens: its format and version, its parameters, every slot's binding,
  every step's target. What a runner cannot run is refused by name, never run some other way (a binding of a key's cell,
  per-key regions, a step of another database or of several, setup steps).
- A sequence runs in one transaction; an `update`'s count is kept where a later step binds it; a `check` is run for what
  its database may raise; the one typed result is read once the transaction has committed.
- The target's session statements run first, on the session the plan names.

### 3.3 One session seam per language

A session is the one way in to a database: a statement and its bound values in, a stream of Arrow record batches out,
and its transaction's three statements (the planner's `transaction` host operations, as `hostSession` asks them today).
The sessions:

| Language | Session | Wire | Status |
|---|---|---|---|
| TypeScript | DuckDB-WASM in the tab | Arrow (`send`, batch by batch) | exists (`DuckDbEngine`), moves under the seam |
| TypeScript | the warehouse (DuckDB on a server) | Arrow IPC over HTTP | exists (`WarehouseEngine`), moves under the seam |
| TypeScript | Snowflake | the SQL API's `jsonv2` over HTTPS | new (§4.2) |
| Python | duckdb-python (the frames' database) | Arrow (`to_arrow_table`, record batch readers) | exists, moves under the seam |
| Python | Snowflake | Arrow, through ADBC | new (§4.1) |

### 3.4 The typed decode: by the plan's facts, once per column

A batch's columns are converted to the plan's types once per column, never per cell: the converter for a column is chosen
once from the plan's column facts and the batch's Arrow storage type, and it works on the whole vector (a decimal's
digits from its unscaled integers, a timestamp's unit scaled once). The answer is Arrow in the schema Java's
`ArrowWriter` writes for the same plan, so the three executors answer the same Arrow for the same plan and database --
which the conformance suite (§3.7) checks byte by byte. A column whose wire type the decode does not know for its plan
type is refused by name (AGENTS.md invariant 4), never guessed.

### 3.5 Text: Java's, unchanged

When a consumer needs text (an engine client asking legend-engine's JSON, a CSV download), the host hands its rows to the
planner it embeds, and Java's writers spell them (D2). Today's handoff is JSON text (Python's `compiler.host_rows`, the
tab's `executeAnswer`); it is measured first (H0), and replaced only if the numbers say so -- with a choice posted to the
user, since a faster handoff (Arrow into the planner) needs an Arrow reader on the Java side.

### 3.6 The fast path

A grid's query renders no text: the runner's Arrow goes straight to the consumer (DataCube reads Arrow already), with the
plan's facts carried in the schema's metadata as today.

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

One suite of plans and data, run by every executor on the same database, the answers compared: the typed values cell by
cell, the Arrow byte by byte, and any text byte by byte against Java's writers. DuckDB in CI; Snowflake on the Snowflake
lane session (credentials stay there, §6). This suite is what "the same design" buys: a difference is a defect, or a row
of `docs/SEMANTICS_REGISTER.md` posted to the user first.

## 4. Snowflake

The planner already plans for Snowflake (core's `Snowflake` dialect; Java runs it through snowflake-jdbc, as
legend-engine does). H3 and H4 first check that the WebAssembly and native planners carry the dialect.

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
  The decode (§3.4) turns each column's strings into the plan's Arrow by `resultSetMetaData.rowType` (type, precision,
  scale), exactly: a number's digits as written, a timestamp's epoch and fraction. It suits interactive results; a large
  extract is the server's to run.
- **Tokens:** `OAUTH`, `KEYPAIR_JWT` and `PROGRAMMATIC_ACCESS_TOKEN` (`X-Snowflake-Authorization-Token-Type`). The tab
  uses OAuth or an access token (D3); a key never reaches a page.
- **A `User-Agent` is required.** CORS is not documented (D4): the session takes its base URL and its headers from the
  caller, so the same code reaches Snowflake through a proxy unchanged. H4 checks CORS from a real browser before any
  page depends on it.

### 4.3 Fidelity

Java's Snowflake results (through snowflake-jdbc, legend-engine's own driver) are the reference: the tab's and Python's
must equal them, value by value, on the lane. Every difference found is posted to the user, case by case, before it lands,
and ledgered (AGENTS.md, the upstream-fidelity tenet).

## 5. The roadmap

Each phase has a goal, a scope and an exit test, and runs as one cycle: a brief, the build, an independent audit (an
agent that wrote none of it), its fold, the records (GATES, IN_FLIGHT, §8), then a push to the branch. No PR and no push
to `main` without the user's word. A phase changes no output's spelling unless the user ruled it, case by case.

| Phase | Goal | Exit |
|---|---|---|
| **R0. The charter** | This note's principles and D6-D10 ruled by the user. | The user's word. |
| **R1. The conformance suite** | The safety net, before any host changes. | The suite green against its ledger in CI, on every OS. |
| **R2. One plan** | Every host runs the plan record (P2). | No host reads another plan shape; the suite's ledger the same or smaller. |
| **R3. One session seam per host** | One way in to every database, streaming Arrow. | A guard: the runner is the one caller of a session (as Java's `theRunnerIsTheOneCallerOfTheSessionsStatements`). |
| **R4. One result** | Typed columns, P3's Arrow on every host. | The suite compares the hosts' Arrow byte for byte. |
| **R5. One owner of each spelling** | Wire text Java's alone (D8); the tab's exact text held to Java's. | No wire text spelled outside Java (a guard). |
| **R6. Snowflake** | A Snowflake session in Python (ADBC) and in the tab (the SQL API). | The suite green on the Snowflake lane against Java's JDBC answers. |
| **R7. Budgets** | Speed held, not chased. | Budgets per host in a lane; an interaction measured end to end. |
| **Z** | The landing. | An independent final audit, the records, CI on every OS; handed to the user. |

**R1. The conformance suite.**
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

**R2. One plan.**
- **The tab:** the callers of `Plan {sql, columns}` and `ExecutePlan` move to the record (§2.1 paths A, B, C, E, F),
  and the runner reads a `tdsResult`.
- **Python:** `Table.execute`, the engine's executes and plan-and-run move to the record.
- **The review:** per D6.
- legend-engine's own plan (`UpstreamPlanner`) is another planner's, and stays.

**R3. One session seam per host.**
- **The tab:** DuckDB-WASM, the warehouse and a remote legend-engine sit behind one interface: a statement and its values
  in, Arrow batches out, and the transaction's statements.
- **Python:** the frames' DuckDB and the warehouse, the same.
- The runner is their one caller.

**R4. One result.**
- **The tab:** typed columns over the Arrow (§3.6a, D9), its readers moved one group at a time. H1a's column decode is
  this phase's first piece, already built.
- **Python:** its answers in P3's schema (D7's cases ruled first).
- Every displayed and saved text stays the same, held by DataCube's suites and the conformance suite.

**R5. One owner of each spelling.**
- **The handoff:** Arrow IPC into Java's writers (D8), which retires `host_rows`, `hostRows` and `rowsOf`.
- **The tab's exact text** (`values.ts`) is held to Java's spelling of each value by the suite.
- **The behaviours found to differ** (§3.6a's six, and §7's) are posted to the user one by one.

**R6. Snowflake.**
- **Python:** the ADBC session (§4.1).
- **The tab:** the SQL API session (§4.2), its CORS checked from a browser first, its proxy the same code.
- **The judge:** the conformance suite on the Snowflake lane, against Java's answers through snowflake-jdbc.

**R7. Budgets.**
- The H0 benchmarks, and a benchmark of an interaction end to end (plan, SQL, decode, draw) at the sizes DataCube
  fetches (up to 1,001 rows a level, `DEFAULT_MAX_ROWS`), and an uncapped flat grid.
- A budget each, in a manual lane.
- A speed fix only where a measurement shows one.

**What is done already.**
- **H0.** The baselines and the code map: R1's and R7's inputs.
- **H1a.** The tab's column decode: R4's first piece. Every value is the same, and an audit found one defect, fixed.
- **H1b's design (§3.6a).** R4's plan for the tab.

## 6. Rules

- No output's spelling changes without the user's word, asked case by case with the expression and its before and after.
- A guard's pin or allowlist moves only with a dated justification (AGENTS.md; `docs/FOUNDATIONS_PLAN.md` §0.4).
- Credentials are never read, printed or copied by a session; the Snowflake lane measures only.
- The start page's checks run before each commit; records land with the code.

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
