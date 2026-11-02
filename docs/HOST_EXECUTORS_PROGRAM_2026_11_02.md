# The host executors: TypeScript and Python run the plan as Java does, and both reach Snowflake

**Status: ratified with the user 2026-11-02** ("Yes I love it a lot", answering the five decisions below as posted). This
note is the program's design and its plan. A change to a decision is a decision, recorded here with its date and reason,
never an edit in passing. Its status lives in §8.

The user's words that started it (2026-11-02): *"Let's start the typescript and python executors, let's look at how to
make those executors similar to the java executor in design as well if you think that makes sense? And for both let's
see if we can also add calling snowflake as remote engine too not just duckdb/warehouse - for TS we should use snowflake
http sql API the snowflake people have told me they do support cors even though not documented, and for python make sure
we use whatever gives arrow natively so it's fast."*

## 0. What the program is for

Java runs a query one way (`docs/PLAN_AND_RESULT_MODEL_2026_10_18.md`): the planner writes one plan record
(`executionplan.ExecutionPlan`); one runner (`exec.PlanRunner`) reviews it whole and runs it on a session it opens through
one seam; the cells are decoded once, by the plan's own column facts (`exec.JdbcDecode`); and the writers at the boundary
spell them, one owner of every spelling (`result.CellText`). The tab's executor (TypeScript, `engine-client/`) and
Python's (`python/legend_lite/`) grew their own shapes beside it (§2). This program gives them Java's design -- the same
plan, the same seams, the same rules -- in each language's own grain, and gives both a Snowflake session.

It is not a port of Java's row model. JDBC hands Java one row at a time, so Java's batches are rows; every database the
tab and Python reach hands them Arrow, which is columnar. Java's own profile (`docs/GATES.md`, "The product path's payoff
measured", items 1-10) found its cost in per-cell objects and per-cell dispatch. So the hosts stay columnar end to end.

## 1. The rulings (the user, 2026-11-02)

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

## 2. The code as it is

(Filled from the code map, 2026-11-02; every claim with its file and line.)

## 3. The design

### 3.1 One plan

Every executor runs the plan record the planner writes, as its JSON (`legend-lite-plan`, `PlanJson.VERSION`, 19 today).
The tab's own `Plan {sql, columns}` (`engine-client/src/relation-type.ts`) is retired into it: its callers move to the
plan, and the plan's columns carry every fact the decode needs (the Pure type and multiplicity, the SQL slot, the store's
declared type, the written precision). The WebAssembly and native planners already write the record (their
`executionPlan`/`executePlan` operations; §2).

### 3.2 One runner per language, with Java's rules

`engine-client/src/plan-runner.ts` already reads the record for a host's writes. It grows reads (`tdsResult`), and
Python gains its twin. Both keep `PlanRunner`'s rules:

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

## 5. The order

Each step: measured first, its outputs proven unchanged where it changes no behaviour, an independent audit, its records
(GATES, IN_FLIGHT, §8), then pushed to the branch. No PR and no push to `main` without the user's word.

- **H0. This note, and the baselines.** The tab's engine and Python's paths measured at 10 to 1,000,000 rows
  (`//engine-client:executor_bench`, `//python:executor_bench`, both manual). No behaviour change.
- **H1. The tab's runner reads.** `plan-runner.ts` runs a `tdsResult` on a session that streams Arrow; DuckDB-WASM and the
  warehouse move under the seam; the decode per column (§3.4); the tab's `Plan` shape retired. Every answer the same
  values as today (the benchmark's digests, the existing suites), then measured.
- **H2. Python's runner.** The same, on duckdb-python; the JSON route's handoff measured, its options posted (D2).
- **H3. Snowflake in Python, through ADBC.** The session, the decode of ADBC's schema to the plan's, the extra; measured
  and checked against Java's answers on the lane.
- **H4. Snowflake in the tab, through the SQL API.** The session (polling, partitions), the decode of `jsonv2`, CORS
  checked from a browser; checked against Java's answers on the lane.
- **H5. The conformance suite** (§3.7) across the three executors, on DuckDB in CI and on the Snowflake lane.
- **Z.** An independent final audit, the records, CI on every OS; the landing handed to the user.

## 6. Rules

- No output's spelling changes without the user's word, asked case by case with the expression and its before and after.
- A guard's pin or allowlist moves only with a dated justification (AGENTS.md; `docs/FOUNDATIONS_PLAN.md` §0.4).
- Credentials are never read, printed or copied by a session; the Snowflake lane measures only.
- The start page's checks run before each commit; records land with the code.

## 7. Open questions

(None yet.)

## 8. Status

- 2026-11-02: ratified; H0 started (branch `claude/wizardly-dirac-rw6tim`, from `main` at `cf73e6ef`).
