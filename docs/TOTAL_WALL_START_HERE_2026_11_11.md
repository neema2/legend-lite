# The total-wall program: start here (2026-11-11)

> **Who this is for:** a fresh session with no memory of earlier sessions, on any account. With this file and the
> repository it can run the program from where it stands to its PR, without stopping and without asking the user
> anything answered here.
>
> **Rulings:** the user's rulings below are final. Do not reopen one without the user's word, and do not ask the user a
> question this file answers.
>
> **Precedence:** when this file and an older document disagree, this file wins for anything dated 2026-11-11, and
> `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` wins for anything older.
>
> **No account-specific data:** this file names no session, environment, trigger or account. Where those matter, it
> says what to create.

## The program's design documents (start with these)

| order | document | what it is |
|---|---|---|
| 1 | `docs/GOALS_OF_RECORD_2026_11_07.md` | the goals, ratified by the user: G1-G11, and P1-P4, which this program builds |
| 2 | **`docs/DATA_DELIVERY_DESIGN_2026_11_10.md`** | **THE MAIN DESIGN AND PLAN of this program**: the approved design, phases A-F (§6), the D rulings (§6.1), "as built" for A and B, the later list (§9) |
| 3 | `docs/data-delivery-homework/*_BRIEF_2026_11_10.md` | the build briefs for B, C plus E, and D, with file:line maps |
| 4 | `docs/DATA_LOADING_TARGET_MODEL_2026_11_11.md` | the ruled target for every data load; its program runs after this PR, but B3 follows it now |
| 5 | `docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md` | the next program, built after this PR |
| earlier | `docs/PLAN_AS_DATA_2026_11_07.md`, `docs/DATA_IN_THE_PLAN_2026_11_09.md` | the plan-as-data design and data-in-the-plan (L1-L8) that came before; `DATA_DELIVERY_DESIGN` replaces the latter's delivery part |

This file is the state, the rulings and the runbook on top of those documents. It is not a replacement for them.

## 0. Read in this order

1. This file, all of it.
2. `AGENTS.md`, loaded automatically: the goals G1-G11, the layer table, and invariants 3, 3a and 4 (no fallbacks).
3. `docs/GOALS_OF_RECORD_2026_11_07.md`: the goals, ratified by the user.
4. `docs/DATA_DELIVERY_DESIGN_2026_11_10.md`, the approved design and plan:
   - §1, the model; §3, the failure rules; §4, one road;
   - §6, the plan in order, A to F; §6.1, the error and print-form rulings;
   - §8, how we stay on track; §9, the later list;
   - the "as built" sections: what A and B did and why.
5. The briefs in `docs/data-delivery-homework/`. Each has file:line maps; read the one for a step before building it.
   - `B_BRIEF_2026_11_10.md`: §6 B4, §7 B2d, §8 B3, §9 the generator rulings.
   - `TEST_DATA_GENERATOR_DESIGN_2026_11_10.md` and `TEST_DATA_GENERATOR_HOMEWORK_2026_11_10.md`: B2d.
   - `REMOTE_FILE_CREDENTIALS_HOMEWORK_2026_11_10.md`: B3.
   - `C_E_BRIEF_2026_11_10.md`: C1, C2, and E's eight items.
   - `D_BRIEF_2026_11_10.md`: D's slices D1.0-D1.7 and D2.
   - `PRINT_FORM_PROBE_2026_11_10.md` and `H2_DATA_TABLE_MEASURED_2026_11_11.md`: measurements.
   - `probes/`: every probe's source, each with its output.
6. Supporting documents:
   - `docs/COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md`: D0, the inventory D builds from.
   - `docs/LEAN_PRODUCT_P3_2026_11_10.md`: E's rulings, its §12.
   - `docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md`: the NEXT program, built after this PR. Read it so nothing in D
     contradicts it.
   - `docs/DATA_LOADING_TARGET_MODEL_2026_11_11.md`: the RULED target for every data load (CSV, JSON, any format;
     pasted or a file). Its program runs after this PR, but B3 and anything else here that touches loading follows
     it.
7. Background, read as needed:
   - `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md`: §3 the glossary at length, §7 the learnings, §8 how to run
     every check, §9 how lane sessions are run.
   - `docs/CLOUD_SESSIONS.md`: Bazel in a cloud container.
   - `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 and §6: cloud database lanes.
   - `docs/result-boundary-homework/README.md` §7: the engine runner.
   - `docs/SEMANTICS_REGISTER.md`: every deliberate difference from upstream.

## 1. What the program is, and how it ends

The goal is **total plan/exec separation (G4)**:
- The plan is SQL and data only.
- Exec runs only the plan's statements and edits no SQL at run. A keyed region is the one allowed exception.
- Data enters a database only through a load the plan carries.
- Every compatibility spelling lives at the edge, and the product depends on no edge.

The goals P1-P4 are built as phases A-F (`DATA_DELIVERY_DESIGN` §6):

| phase | goal | content | state |
|---|---|---|---|
| A | P1 | Data delivery rebuilt: model data as SQL in a content blob, files as pointers the database reads, failure rules, one road | CLOSED (5e9fcef8) |
| B | P1 | The rest of P1: the data-mechanism census to zero pending | in progress |
| C | P4 | Exec edits no SQL. C1: the trace comment (D3). C2: MariaDB's error read-back (D4) | |
| D | P2 | The edge: every compatibility spelling moved out of the product; the product's own errors and print forms | |
| E | P3 | Lean product: the eight E items | |
| F | | One full run on every database, then ONE PR | |

**The end.** The orchestrator opens ONE pull request, `claude/total-wall` into `main`, using
`.github/pull_request_template.md`, and **never merges it: the user merges.** No session pushes to `main`.

**The PR's timing is open.** The recommendation is to open it at B's checkpoint, so CI checks Windows and macOS before
C, D and E pile on; the user has not ruled. Ask once at B's checkpoint; without a yes, open it at F2.

## 2. A glossary (the words this program uses)

**Pipeline and roads**

- **Plan** (`ExecutionPlan`): data the planner makes and exec runs. It holds the statements with their SQL text,
  parameter slots, the target (database, session, setup steps) and the loads.
- **Test plan** (`testexecutionplan/TestPlan`): a test body's plan, with its verdict steps and edge data.
- **Product road / test road:**
  - The product road is a query (`Execution.run`).
  - The test road is a whole test body, asserts included, planned by `testplan/TestPlanner` and run by
    `testexec/TestRunner`.
  - The doors are `Execution`, `TestExecution` and `TypedQuery`.
- **The walk:** the deleted statement-by-statement interpreter. Never resurrect it.

**Data and its delivery**

- **Load / LoadStep:** how data enters a database. There are three forms:
  - model data as statements in a **content blob** named by digest;
  - a **file** by reference, which the database reads;
  - a **`data:` payload's** private table, or its rows inline.
- **Cadence:** when a load runs: `PER_ACQUISITION`, `AT_STEP`, `AT_RELEASE`, or `SOURCE` (a `data:` table, not a body
  write).
- **`data:` source / payload:** JSON (or text) written inline in a model, typically a JSON connection's data.
- **Frame:** a JSON connection's records as a relation: each record's text and its place (`u_frame_ord__`). Its form is
  `RECORDS`; a plain `data:` array is `ELEMENTS`.
- **Session table:** a database's private temporary table for a `data:` payload, made, filled and released by the plan.
- **`file:` source:** a file a query reads directly. `loadCsvToDbTable` loads a file into a table.
- **The fence:** which folders the server lets a `file:` source read. Set with `--sources DIR` or
  `LEGEND_LITE_SOURCE_DIRS`; with none named, no folder. On DuckDB the database itself enforces it.

**The edge and its machinery**

- **The edge** (`com.legend.edge`): the compatibility layer for upstream's exact texts and behaviours. The product never
  depends on it.
- **The referee:** legend-engine's own H2 (2.1.214, an isolated class loader). The corpus replays the engine's golden
  SQL on it beside lite's run, and it replays its own H2 copy of the plan's loads (`RefereeLoad`).
- **An appeal:** under the edge, when lite's SQL text differs from the engine's golden, the rows are compared instead,
  as compatibility only.

**Judges**

- **HOST:** Java compares the asserts' sides.
- **DATABASE:** the verdict is computed in SQL.
- **BOTH:** both run, and a disagreement fails.
- A **verdict step** is one assert's planned judgment.

**PCT**

- **PCT:** the Platform Compatibility Tests, upstream's per-function test suites. There are five suites: Essential,
  Grammar, Relation, Standard, Unclassified.
- **Channel A:** upstream's own runner sends each query to lite. It checks lite as a query engine.
- **Channel B:** lite runs each whole test body as its own plan under its own judges. It checks lite as a platform.
- **Expected-failure lists:**
  - Channel A: the `expectedFailures` field of `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_<DB>_<Suite>Functions_PCT.java`.
  - Channel B: `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv`.

**Other test suites**

- **The corpus (rcorpus):** upstream's relational tests, run on DuckDB and H2. Its rosters and registers are in
  `spec/src/test/resources/rcorpus/`. **Stress:** the service-suite stress tests, with floors in
  `StressServiceSuitesTest`.

**The program's guards**

- **The census** (`core/src/test/java/com/legend/DataMechanismCensusTest.java`): every way data reaches a database,
  each a row M1-M18, each one of:
  - DELETED (its symbols must read zero);
  - BOUNDED (held by a named guard);
  - STAYING (a ruled reason);
  - PENDING (the step that closes it).
  "Census pending" is the PENDING count.
- **The matrix:** `DataDeliveryMatrixTest` (in process) and `DataDeliveryMatrixPostgresTest`. Every data kind on every
  door and every database. Its `RED` lists the expected-red cells.
- **Exec's SQL edits:** the edits exec makes to SQL at run, pinned by `SentStatementsAreThePlansTest`. Two are left (D3,
  D4) besides the keyed region (`PlanRunner.expand`).

**Records and sessions**

- **Register row (S-number):** a ledgered, user-ruled divergence from upstream in `docs/SEMANTICS_REGISTER.md`.
- **Lane session:** a cloud session with the databases installed (embedded servers or cloud credentials). It measures a
  commit on request and changes nothing.

## 3. The user's standing words and the hard rules

- **The go:** "Keep going don't stop until done, make sure fast dev loop but don't skip the audits" (2026-11-11).
  - Also: "let's now keep going dont stop until all done".
  - And: "We still need to do audits at every important checkpoint, not just at the end".
- **The constraints:** "never push to main or open/merge a PR without my word; commit only the files you mean and push to
  the branch; never move or park my work without asking; no fallbacks; no hour-long waits or rabbit holes; when I ask
  what's going on, answer in a few plain lines. Lane sessions measure only."
- **Credentials:** never read, print or copy one. The plan never holds a secret.
- **"No second worker"** (2026-11-11). One worker builds. Do not start a second unless the user says so. A worker may
  use its own local subagents.
- **PR #81 (timeseries) is out of scope:** "Don't do the 81 trial. Focus on our work."
- **Deleting files:** a session never deletes a file it did not create. The creator deletes it, or the user gives the
  word.
- **Upstream fidelity (AGENTS.md):**
  - Ask "can we do exactly as pure/engine does?" first.
  - Upstream's answer is READ from the pinned sources or ASKED of the engine runner, never recalled.
  - A divergence is POSTED to the user, with the expression, upstream's answer and lite's, before it lands, and
    ledgered as a register row the user rules.
- **Speed** (2026-11-11): "We need a much much faster dev loop"; "We have warm sessions for all the databases to run in
  parallel, you haven't been leveraging those". See §7.
- **How the user wants to be talked to** (learned):
  - Plain words and short answers. A recommendation, not a survey.
  - No jargon without a definition.
  - The user dislikes refusals and narrowings: "We need to make json work everywhere". Prefer making a thing work on
    every database over refusing it.
  - The user pushes for pace, and wants audits kept.

## 4. The state at handoff (2026-11-11, about 10:30 UTC)

### 4.1 Commits on `claude/total-wall` since A closed

Run `git log --oneline 5e9fcef8..origin/claude/total-wall` for anything newer. Each commit message lists the checks it
ran.

```
(next)   the lanes' delivery rulings (Snowflake, BigQuery INLINE; Databricks SESSION_TABLE; the empty payload; S190
         withdrawn; the servers test in heavy) and the second audit's S13 (exact BIGINT floor division)
60e59850 this start-here file, rewritten for a session on any account
5a44e274 B2d.1: the test-data generator as ONE planned read-only statement (TestDataPlanner)
8b128599 this start-here file (first version) and three design docs copied in
fca77fec Trino's scalar TIMESTAMP keeps nine digits (the second audit's M1)
5fe98b92 Census: M3 and M15-referee DELETED (B2b)
2bae908f JSON on every database (SqlDialect.dataDelivery: SESSION_TABLE / INLINE / REFUSED)
00e67e38 B2b: delete edge/SeedTexts.java and base/CsvCells.java (by their creator)
57e2c445 B2b: the referee replays its own H2 copy of the plan's loads; no Java reads a file's cells
2f164ec4 B2a audit fixes (S1-S4) and Postgres's data: session table
3a3defa0 Trino: every date-time arm keeps nine digits, or refuses by name
862a4d02 B2a: the JSON frame onto data:, the judges' data: loads, the edge's live-source record (M10 DELETED)
69cb9a32 B2a, first piece: a program's seeds are the load planner's
5e9fcef8 A CLOSED
```

### 4.2 The census

| row | what | state |
|---|---|---|
| M1 | `###Data` into the server's project database | DELETED (L6c) |
| M2 | a connection's declared test data on the product path | DELETED (L2) |
| M3 | test data on the test road (seed loads, `SeedTexts`) | DELETED (B2b) |
| M4 | service-suite data through a synthetic connection | BOUNDED (`ServiceTestProvisionKeyTest`) |
| M5 | the system metamodel store on the test road | DELETED (B4) |
| M6 | the system metamodel store on the product road | DELETED (B4) |
| M7-seed | the seed forms (`setUpDataSQLs`) | STAYING (the author's statements) |
| M7-authored | `executeInDb`: SQL the user wrote | STAYING |
| M8 | `loadCsvToDbTable`'s path in a statement | DELETED (L1) |
| M9 | package setups and fixtures between plan and run | BOUNDED (L5) |
| M10 | JSON model data inlined | DELETED (B2a) |
| M11, M12, M16 | a query's literal rows; model-derived constants; parameters and keyed keys | STAYING (P9) |
| **M13-output** | the test-data generator's rows handed on as seed text | **PENDING: B2d** |
| **M14** | the browser, Python and DataCube load data outside any plan | **PENDING: B3** |
| M15-fixtures | the Java test fixtures' own JDBC writes | BOUNDED (`FixtureWriteCensusTest`) |
| M15-referee | the referee's mirror replaying seed texts | DELETED (B2b) |
| M17 | data parsed and never loaded | BOUNDED (B2e) |
| M18 | a `data:` payload in the statement: one bound parameter, or its own rows on H2 and MongoDB (§4.3) | BOUNDED (`DataDeliveryDeclaredTest`); its session table, release and edge record DELETED (the JSON pass, symbols pinned at zero) |

**The scoreboard,** reported by the worker on every push:

| measure | value | detail |
|---|---|---|
| census pending | **2** | M13-output, M14 (M5, M6 deleted by B4 on `claude/total-wall-b4`) |
| matrix | green in process | Postgres green too: its 3 catalog cells off the red list (B4) |
| exec's SQL edits | **2** | D3 goes in C1, D4 in C2 |
| fast loop | about 10 min | |
| wide local set | about 50 min | about 510 targets; no longer per push |
| unprivileged Postgres | about 170 s | |

### 4.3 JSON on every database (the JSON pass, 2026-11-11: no session table anywhere)

The delivery is per database: `SqlDialect.deliver` returns the source the statement reads, measured, chosen at
planning. There is no session table, no load, no release and no drop for a payload. The payload's text sits once in
the plan, as its statement's content keyed by SHA-256 (§5.9).

| delivery | database | the statement's source (measured at 100,000 records unless noted) |
|---|---|---|
| BOUND PARAMETER | Postgres 16 | `jsonb_array_elements(CAST(? AS JSONB)) WITH ORDINALITY AS legend_payload(value, ordinality)`, place = ordinality - 1. 259 ms |
| | MySQL 8.4 | `(SELECT ? AS legend_payload) AS legend_doc, JSON_TABLE(legend_doc.legend_payload, '$[*]' COLUMNS (legend_ord FOR ORDINALITY)) AS legend_rows`, element `JSON_EXTRACT(doc, CONCAT('$[', ord - 1, ']'))`. 342 ms (MariaDB's form took 586) |
| | MariaDB 11.4 | `JSON_TABLE(?, '$[*]' COLUMNS (legend_ord FOR ORDINALITY, legend_element JSON PATH '$'))`. 121 ms, keeps `1.50` as written |
| | DuckDB 1.5 | `json_each(CAST(? AS JSON))`, place = key. 83 ms |
| | SQLite 3.47 | `json_each(?)`, each atom put back as its JSON text (`CASE type WHEN 'text' THEN json_quote(value) WHEN 'null' THEN 'null' ...`), place = key. 45 ms |
| | Trino 483 | `UNNEST(CAST(json_parse(?) AS ARRAY(JSON))) WITH ORDINALITY`. The driver writes `?` into the text, so a payload whose literal passes 1,000,000 characters (`query.max-length`) is refused at planning by name |
| | ClickHouse 25.8 | `(SELECT JSONExtractArrayRaw(?) AS legend_elements) ARRAY JOIN arrayEnumerate(legend_elements) AS legend_index`. The driver writes `?` into the text: `max_query_size` (256 KiB by default; the lanes' URL sets 16 MiB) is a connection setting the database enforces. 390 ms at 16 MiB |
| | Databricks | `variant_explode(parse_json(?))`, place = pos. 557 ms at 10,000. Above the driver's 1 MiB parameter limit, INLINE: `SELECT parse_json(col1) AS data, col2 AS ord FROM VALUES ('...', 0), ...` (4.2 s at 100,000) |
| | Snowflake | `TABLE(FLATTEN(INPUT => PARSE_JSON(?)))`. 0.48 s |
| | BigQuery | `UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(?))) WITH OFFSET`. 7.6 s |
| INLINE | H2 2.5 | `VALUES (JSON '...', 0), ...`. 460 ms. H2 refuses an array parameter over 65,536 elements |
| | MongoDB | `$documents` of `{"$literal": doc}` |

- **Every form** returns no row for the empty array, and keeps a quote, a backslash, a line break, the order, nested values
  and typed values. `JsonPayloadFormsTest` reads each element back by its JSON value through the product door: in
  process, on Postgres (its matrix target) and on the servers (`data_source_servers_test`), plus a frame's records with
  their places.
- **A number's spelling is the database's.** DuckDB's `json_each` prints `1.50` as `1.5`, inside objects too. SQLite
  does the same for a top-level real: getting the element's own text back needs a path lookup per element, which is
  quadratic (19 s at 100,000). MySQL's JSON already did this. The values are JSON-equal, and member reads (`get`, cast)
  are unchanged. Register S194, ruled by the user (2026-11-11): "Is that the input JSON to the database? Then that
  makes sense bc it's the dialects plan for the dialects json".
- **Size limits:**
  - A limit that is the database's own property is refused at planning by name: Trino's 1,000,000 characters.
  - A limit that is a connection setting is left to the database, and its refusal reaches the caller: ClickHouse's
    `max_query_size`, MySQL's and MariaDB's `max_allowed_packet`.
  - Databricks goes INLINE above 1 MiB.
  - The INLINE statement is as large as the payload: H2 has no measured limit; MongoDB's command is 16 MB.
- **H2's string member read, FIXED (2026-11-11, the orchestrator's ask):** it kept a JSON string's escapes
  (`TRIM(BOTH '"' FROM CAST(x."t" AS VARCHAR))`: a backslash doubled, a line break as `\n`, a string's own edge quotes
  lost), on the session table as before. H2 2.5.252 has no `JSON_VALUE`. When it parses a string it normalises it: an
  escaped solidus becomes `/`, and a `\u` escape of any non-control character becomes the character. Its text then
  escapes only a quote, a backslash and the control characters, which `STRINGDECODE` reads back exactly (measured).
  The read is now `STRINGDECODE` of the quoted text. `JsonPayloadFormsTest` reads a quote, a backslash, a line break,
  a tab, `\u` escapes, an escaped backslash before `u`, an escaped solidus, the string `"null"` and edge quotes back
  exactly on every database it runs on.
- Two traps were measured earlier and still hold:
  - Databricks's `CAST(variant AS STRING)` sorts the keys.
  - Snowflake's empty form's place column types as `NUMBER(1,0)`.

### 4.4 In flight at the worker (verify each in the code: done only if its test exists and passes)

1. **DONE (the commit after 60e59850):** Snowflake INLINE, BigQuery INLINE and Databricks SESSION_TABLE (§4.3), plus
   the zero-row form on the INLINE databases (one typed NULL row and `WHERE FALSE`, the empty TDS literal's shape;
   `DataSourcesDeliveryTest`, and `DataSourceServersTest` on Trino, ClickHouse and MongoDB). `REFUSED` is gone from
   `DataDelivery`: each dialect declares its own (no default); the engine's text dialects refuse by name.
   - Test the dialect literal writers for a quote and a backslash.
   - Fix the two probe texts' quoting: `probes/BigQueryDataTable.java.txt` and `probes/DatabricksDataTable.java.txt`.
   - S190 then names no database: close it.
2. **H2's system catalog, DONE (the commit after c72fc876):** the catalog's ONCE load makes `LEGEND_SESSION`
   (`SqlDialect.dataSchema`), the refusal is deleted, and a body routed to the catalog sends neither the prepare nor a
   drop: a release on the catalog ran INSIDE the run's turn (measured: H2's DROP committed the registration, the next
   run collided), and the turn's rollback takes the rows anyway. `BodyRouterParityTest.dataSourcePrograms`. The
   worker had refused "a system-routed body reading a `data:` source on H2"; that refusal was VETOED. The ask was:
   - make `LEGEND_SESSION` part of the system catalog's once-made schema;
   - delete the `ProgramState.placeMade` refusal, and add a test;
   - confirm the catalog's release runs after its run transaction is rolled back (H2's DROP commits).
3. **The second audit's findings** (on 862a4d02..2f164ec4). **S6-S12 DONE (the commit after c72fc876)**, each with a
   test: S6 the tables held to one home database, a write's read too (`DataSourcesDeliveryTest`); S7 a frame run is
   its own planned step (`PlannedStep.FrameRun`), no place arithmetic; S8 one refusal on every renderer, MongoDB's
   too (`DataDeliveryDeclaredTest`); S9 a refused payload records nothing, a routed body's plan carries no release;
   S10 `pg_temp` on every statement (`DataDeliveryMatrixPostgresTest`, a search_path with pg_temp last); S11 and S12
   a writing query's drops are its plan's RELEASE (`ExecutionPlan.Release`, plan version 45, the test plan's the same
   record), sent after the run's rollback whatever its outcome (`DataReleaseTest`); the stale AT_STEP wording fixed.
   The original list:
   - **S5, DONE (the commit after dad2dc77):** `DateCtorRule`'s Float-second path writes its text carrier in six digits
     (ISO_MICRO) on every database, so a known second of more is refused at planning by name, and a computed one by the
     statement's own raise (its DECIMAL(38, 9) value against itself rounded to six); MongoDB, which keeps milliseconds,
     refuses a known second of more than three and any computed fractional second by name. `DateTimeFractionTest`
     (DuckDB, SQLite; MongoDB's render). Seen in passing: on SQLite a Float PARAMETER's second meets `date()`'s range
     guard first and raises "Invalid second: 1.5" for a legal value -- the range guard's, older than S5; not fixed here.
   - **S6:** `DataSources.Tables` must hold its home target and refuse a different `on`; pass `routed` through. If the
     refusal newly refuses any test, use per-target tables instead.
   - **S7:** track frame runs by node identity, not an integer shift.
   - **S8:** one typed refusal for an unread `data:` source (`AnsiSqlRenderer` vs MongoDbPipeline).
   - **S9:** tests for the payload cut ahead of the make and release, and for `release().on()` being System on a
     system-routed body.
   - **S10:** qualify Postgres's table with `pg_temp`.
   - **S11:** H2's release after a writing body's rollback, with a test.
   - **S12:** the query door's `data:` drops on every outcome.
   - **S13, DONE (the commit after 60e59850):** exact BIGINT floor division on Trino (`(a - mod(mod(a,b)+b,b)) / b`)
     and Databricks (`(a - pmod(a,b)) div b`); Trino's microsecond truncation through the epoch's micros (its
     `date_trunc('microsecond')` fails at run, measured); `fromMicros` through DECIMAL(38,0) past 2262. Measured on
     Trino 483: before 1970, in 2300, and past 2^53; `//pct:pct_trino_standard` green.
   - Also stale "AT_STEP" wording.
4. **The CI lane guard, DONE (the commit after 60e59850):** `//core:data_source_servers_test` is in `//gates:heavy`
   beside `mongodb_plan_test`; the guard's query is empty locally.
5. **B2d (the generator), in slices.**
   - **B2d.1, done:** `TestDataPlanner`, ONE read-only statement.
     - Each fetch is a `WITH` clause: starting rows by id, ordered by key, `LIMIT 20`, one clause per navigation step.
     - Its single cell is the whole test-data text: headers as constants, each table's distinct lines scrubbed or
       hashed in SQL, NULL as `---null---`, STRING_AGG in order, `-----` between tables.
     - It replaces the design's "one SELECT per table plus a writer". The orchestrator accepted that, as consistent by
       itself and the database doing the work.
     - Ruling 3's FAITHFUL output, DONE: a cell with a comma, double quote or line break (CR or LF) is quoted per
       RFC 4180 in SQL, nothing replaced (B2d.1 had scrubbed like upstream; fixed). `TestDataPlannerTest` on DuckDB, H2
       and SQLite: all three plus a single quote in one cell, and a NULL. The difference from upstream's lossy writer is
       register row S191.
     - Upstream's `loadAndTestExecution` (answered 2026-11-11): 30 tests call it, all in `testDataGeneration.pure`; 2
       have statements after it and 0 of those read a replaced table (one re-creates the database first, one replaces
       the same tables again). Its reload drops and re-creates EVERY table of the database (`setUpDataSQLs` ->
       `schemaAndTableSetup(dbConfig, [])`, toDDL.pure:155-177), so "the query over the slice" swaps every table: the
       slice's for their rows, every other for no row. No test asserts its Boolean.
   - **B2d.2, the test road:**
     - (1) `assertTestData` becomes a database set comparison per table (EXCEPT both ways, plus header equality, the
       expected side a planning constant). Accepted: it is upstream's comparison by other means.
     - (2) `loadAndTestExecution` becomes "the query over the slice": the query planned again, each store table
       swapped for its generated rows, read-only. **PENDING THE USER'S RULING** (§4.6). Build it, but land it only once
       ruled.
     - (3) The fetch texts differ: already ruled 2026-11-10, a register row; `assertSize` still holds.
     - Also: S143's skip lifted; `TestDataGeneratorRun` and the TDG appeal deleted.
   - **B2d.3:** the default starting rows, by upstream's rule read from the pinned sources.
5b. **B2d.2's read-only reload: RULED, land it** (§5.2), with its register row.
5c. **The INLINE size limits** (§4.3): a named refusal or a planned setting per database, each with a test at its limit.
5d. **A matrix cell for a CSV with an embedded JSON column.** The user asked whether a CSV can carry one. It can: types
    come from the model, so a `SEMISTRUCTURED`/`JSON` column's cell text is taken by the database's own JSON type.
    - **The cell:** CSV test data whose JSON column holds a comma, a doubled quote and a nested object, plus an empty
      cell and a NULL.
    - **Where:** every in-process database, and Postgres on its lane, then a field read by path.
    - **DONE (the commit after c72fc876):** `CsvJsonColumnTest` on DuckDB, H2, SQLite and Postgres: a comma, CSV's
      doubled quotes and a nested object read by path; an empty cell and `---null---` NULL. Green everywhere.
5e. **JSON delivery, round 2 (the user's question, 2026-11-11: "how to represent the data in the plan vs the database
    mechanism"; probes `docs/data-delivery-homework/probes/*Delivery2.java.txt`, answers recorded there).** At 100,000
    records: Snowflake inline 17.0 s, temp table 73 s, the payload as ONE bound parameter 0.48 s; BigQuery inline
    refused (over 1 MB), temp table 105 s, parameter 7.6 s; ClickHouse's driver writes a parameter into the text (no
    gain), its session temp table 2.6 s and unbounded. **RULED and BUILT (§5.9):** the dialect decides; Snowflake and
    BigQuery bind the payload. ClickHouse's session temp table was recommended and not yet asked again.
6. **B3's measurement, done:** DuckDB 1.5.6 binds credentials as parameters, so the credential mechanism is clean:
   - `CREATE TEMPORARY SECRET s (TYPE S3, KEY_ID ?, SECRET ?, REGION ...)` and `CREATE OR REPLACE TEMPORARY SECRET ...
     SCOPE ?` both prepare and bind;
   - `duckdb_secrets()` shows `persistent=false`;
   - `DROP TEMPORARY SECRET` works;
   - the probe is `probes/DuckDbBoundSecret.java.txt`.
7. **B4, done on `claude/total-wall-b4`** (census M5, M6 DELETED; every catalog statement on the in-process catalog, a
   server's on the platform's own engine's). Known residuals, dated 2026-11-11, not built:
   - An `assertError` read routed to the catalog beside a body on a user database sets its savepoint on the body's own
     session, guarding nothing there. Harmless today: H2's and SQLite's catalogs do not abort a transaction, and on
     DuckDB's the model's registration is placed ahead of every catalog step, so a rolled-back turn registers again
     (`CatalogTurnFailureTest`).
   - (Resolved by 560e99a: a store-free statement before a program's first catalog read is no longer written in the
     server's dialect first; the product door probes the program, resolution only, and plans it once.)

### 4.5 The last lane round (35)

All runs at 5fe98b92.

| lane | runs | result |
|---|---|---|
| 1 | Trino, ClickHouse and Postgres PCT (channel A; channel B under BOTH and under DATABASE); the two corpora; stress; the three server targets | the corpus, stress and server results were a fresh baseline |
| 2 | everything in `//core/...`; PCT DuckDB, H2, SQLite, MariaDB, MySQL, ClickHouse, Postgres, channel B; `//core:data_source_servers_test` | |
| 3 | Snowflake PCT (15 runs) | |
| 4 | BigQuery PCT | |
| 5 | Databricks PCT | |

- **Expected:** PCT tests that now pass but are still named in expected-failure lists, from the `data:` refusal's
  removal. The worker trims those lists forward.
- **Results:** this file was written before they arrived. A new session re-runs the round at the newest head (§7.3).

### 4.6 Open questions for the user (ask once, at the natural moment)

1. **The generator's reload check: RULED** (the user, 2026-11-11: "Read only reload is fine"). The ruling is in §5.2.
2. **The PR's timing** (§1).
3. **Native file loaders: SETTLED by the data-loading target model** (the user, 2026-11-11, "Yes write it";
   `docs/DATA_LOADING_TARGET_MODEL_2026_11_11.md`).
   - Native bulk loaders are part of the data-loading program after this PR.
   - Until then, a file load on a database that cannot read files is refused by name: the user's "option 1",
     2026-11-10.

## 5. Every ruling of this program (dated; each is final)

Older rulings are in `docs/GOALS_OF_RECORD_2026_11_07.md`, `docs/PLAN_AS_DATA_2026_11_07.md`,
`docs/DATA_IN_THE_PLAN_2026_11_09.md` §8, `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` (its header, §6.1 and §6's rows)
and the briefs' "RULED" items.

### 5.1 Data: where the data lives decides its road, not its format

| | **written in the model** (known at planning) | **a file** (outside the model) |
|---|---|---|
| examples | CSV test data, `###Data`, the catalog, `from()` seeds, a JSON connection's inline `data:` payload | `loadCsvToDbTable`, a `file:` source (CSV, JSON, Parquet), a remote file |
| the plan holds | the rows as SQL: DDL plus chunked `INSERT … VALUES` of typed literals, in a content blob named by digest (or, for a `data:` payload, its statement's bound parameter or inline rows, §4.3; the session table went with the JSON pass) | a pointer (a reference and a digest), never the contents |
| who reads it | the planner turns the model text into literals: allowed, because build-time model text is known at planning (`docs/JAVA_EVICTION_PLAN.md` §1) | the database, at run; exec binds the path, verified by digest |
| where it works | **every database** | DuckDB (CSV, JSON, Parquet) and H2 (CSV, `CSVREAD`); every other database refuses by name (the user's "option 1") |

- **Why files differ:** a file can change between planning and run, can be 500 MB (upstream's limit), and can sit behind
  a credential.
- **"No Java parses data"** means exec at run.
- **Why a JSON payload needs a table at all:** a JSON connection feeds a model-to-model mapping whose source is a Pure
  class, so no store declares a table for it. Upstream runs it in memory; lite runs it in the database, so lite makes
  its own table.
  - That table holds each record's text and its place.
  - It is not flattened, because records nest; the SQL reads fields by path.
- **JSON must work on every database** (the user, 2026-11-11: "We need to make json work everywhere"; "Mongo database was
  literally created to hold json"): §4.3.

### 5.2 B's rulings

- **B1:** the seed forms stay the effect script's. **B2c:** the fixtures are bounded, not converted. **B2e:** data no
  feature reads yet is "not implemented yet", refused by name.
- **B2d, the generator (the design APPROVED 2026-11-10).**
  - Read-only, with `WITH` clauses carrying the chain: starting rows ordered by key, `LIMIT 20`, one level per
    navigation step, no recursion. **NO temporary tables** ("We don't have to copy the impl of there is a better way").
  - The default starting rows are built by upstream's rule ("Great let's do it"). Upstream: `SELECT DISTINCT TOP 10`
    root keys from the query.
  - An empty seed is no filter ("1&2 yes").
  - Every level is ordered by its key; upstream is unordered, a register row.
  - FAITHFUL output: quoted cells, line breaks kept, and lite's test-data reader quote-aware ("Yes").
  - Reload checks planned as SQL ("4 yes").
  - **The reload is read-only** (the user, 2026-11-11: "Read only reload is fine").
    - **The question:** upstream's `loadAndTestExecution` rebuilds the WHOLE test database from the generated CSV, so
      every table the CSV does not name ends up empty. Then it runs the query again. Doing that literally means
      building SQL from a value known only at run, which breaks the wall.
    - **What lite does:** it plans the query again with every table of that database swapped: the slice's tables for
      their generated rows, every other table for no rows. The test's real tables are left untouched.
    - **Why the outcome is identical:** the worker counted 30 upstream tests that call it. 28 end with it. In the other 2,
      nothing afterwards reads a replaced table: one re-creates the database first, the other reloads the same tables
      again. None asserts the Boolean it returns.
    - **The register:** a row records the method difference.
    - **The CSV stays part of the user contract.** `generateTestData` returns a `TestDataGenResult` whose `dataCsvString`
      is the test-data text a user pastes into a test. `getRelationalCSVDataFromQuery` is the typed form.
  - Upstream's 46 SQL-text and 26 count checks: a register row.
  - `TestDataGeneratorRun` and the TDG appeal deleted.
- **B3:**
  - The hosts run the plan's loads.
  - The text plan (`generatePlan`/`arrowPlan`) renders from the execution plan: this lifts
    `PlanMaker.refuseLiveSourcesInText`, and each changed text is reviewed, never accepted in bulk.
  - Credentials by name: a planned `CREATE TEMPORARY SECRET` with values bound from exec's secret store. A guard keeps
    any secret value out of the plan.
- **The server's fence:**
  - `--sources DIR` or `LEGEND_LITE_SOURCE_DIRS`; none named means no folder. The library stays open.
  - **DuckDB enforces it** ("Okay", "Option 2"): `allowed_directories`, `enable_external_access=false` and
    `lock_configuration=true` as planned session steps, with the staging folder inside the fence.
  - H2 is left as a known gap.
- **B4:** every catalog statement runs on the process's in-process catalog ("B4 sounds good for now"). The server forms
  are deleted. A statement mixing catalog and user tables stays refused by name.
- **CSV:** S184 (DuckDB keeps a record wide by one empty cell) and S186 (H2 loads short records): "Csv findings are
  fine".
- **A2's two:** "Yes to both". A non-ISO date cell is text the database casts. Binary and array data are not
  implemented yet. SQLite's TIMESTAMP decodes like the others.
- **S187:** several runtimes in one query are refused on every door; cross-runtime queries go on the later list.
- **S189:** a NaN or Infinity Float cell in model data is refused by name. BIT accepts y/n.

### 5.3 Date-times

- **S181:** a date-time with more fraction digits than its target keeps is REFUSED, never cut: at planning for known
  values, by a SQL guard for parameters.
- Nine PCT tests on Postgres, MySQL, MariaDB, BigQuery and Databricks are ledgered "pending the nanosecond type" ("I guess
  refuse for now and put this on feature list for after total separation complete").
- ClickHouse keeps 9 digits with `DateTime64(9)`.
- Trino keeps 9 digits or refuses by name:
  - its `CAST(... AS TIMESTAMP(6))` ROUNDS;
  - microsecond arithmetic goes through epoch nanoseconds in `DECIMAL(38,0)`;
  - `STRPTIME` with micro or nano parts is refused.
- The nanosecond type itself is on the later list.

### 5.4 D: the product's errors and print forms (full text: `DATA_DELIVERY_DESIGN` §6.1)

**Errors**
- Strict and typed. A Float divided by zero raises.
- A product error is its kind plus where it happened: a fixed text known at planning, never the value.
- Safe versions are not built.

**Print forms**
- A value stays typed unless a string is asked for. Then:

| type | text |
|---|---|
| Integer | digits |
| Decimal | at its scale |
| Float | 6 decimals, trailing zeros and `.` trimmed |
| Boolean | `true` / `false` |
| StrictDate | `YYYY-MM-DD` |
| DateTime | ISO 8601 UTC, six digits and `Z`; nanoseconds only where stored |
| list | a JSON array |
| map | a JSON object |

- Objects and relations are refused, pointing to graph fetch.
- `format`: `%s %d %f %t{}` in the product; `%r` and padding at the edge.
- `makeString` skips empty elements.
- NaN and Infinity print as words.
- Lite's own CSV, JSON and Arrow writers are exact.

**Where compatibility goes**
- Pure's exact texts are the edge's own SQL render target.
- Compile messages use the product's own wording.
- Pivot names are bare.
- Engine-only SQL spellings go to an edge decorator.
- MongoDB needs its own print spelling (measured: `$toString` writes `1`, `1e+20`, dates in milliseconds with `Z`).

### 5.5 C

- **C1, RULED ("Agree with all"):**
  - The database receives exactly the plan's text: no per-run trace comment.
  - The execution id lives in lite's trace and in the edge's activity report.
  - A register row records that upstream puts it in the text.
- **C2, the brief's design (no open question):**
  - MariaDB/MySQL's read-back pair is declared by the plan's target, as `RaiseReadBack` from `SqlDialect.raiseReadBack()`
    (every dialect answers, no default), and sent through a counted, scoped path.
  - `RaiseRecovery` and the connector overrides are deleted.

### 5.6 pure/v1 and the native protocol (2026-11-11)

- **The user's words:** "lite clean sheet protocol that can work local or remote super fast and typed and always arrow no
  matter if local or remote. Then we should have remote compatibility mode that is drop in for engine or ours ...
  Basically make everything speak lite native / fast / shaped how we want. Then again at edge a purev1 compatibility
  mode that lite or engine could answer."
- **When:** "We can design now and then do it after this PR once we are 100% total separation".
- **So in D:** pure/v1 over HTTP becomes the edge's compatibility mode. Its upstream routes move to the edge; lite's own
  routes and the host protocol stay product. This settles `D_BRIEF` §6 item 1.
- **S125:** pure/v1 matches upstream's Arrow typing; the native protocol stays exact.
- **The native protocol** is built in the next program (`docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md`, six questions for
  the user's later review).

### 5.7 Process (2026-11-11)

- **The fast loop:**
  - per change: build, the touched packages' tests and the in-process matrix (about 10 minutes), then push;
  - the wide run is no longer a per-push gate;
  - red found later is fixed forward.
- **Lane sessions:** use them in parallel at every significant push, not only at checkpoints.
- **Audits:** they run beside the work and never block it. Their fixes fold into the next commits.

### 5.8 Data loading, the target model (2026-11-11)

The user asked how CSV, JSON and every other load should feel the same whatever the format, and whether pasted or a
file. The answer, ruled as the target ("Yes write it"), is `docs/DATA_LOADING_TARGET_MODEL_2026_11_11.md`:
- schema, format, location and delivery are kept apart;
- pasting is a file whose bytes live in the model;
- one typed format spec configures every reader;
- types come from the model;
- delivery is chosen per database: its own reader, then a bulk loader, then SQL values, then a refusal by name;
- a conformance matrix proves it.

Its program runs after this PR. Here, B3 and any loading work follow it, so they add no new path.

### 5.9 JSON delivery: the dialect decides (2026-11-11)

The user asked whether the payload's representation in the plan and the database's mechanism had to be coupled, and
why the planner branched on the database. Ruled, on round 2's probes ("Yes dialect and snow/bigquery params"):
- **The dialect decides.** The planner hands ONE payload (`SqlDialect.DataPayload`: the source, its elements cut once,
  the table name by digest) to `SqlDialect.deliver`, and places the answer: a session table (`Delivered.Table`) or a
  source the statement reads itself (`Delivered.Read`). No planner code holds a delivery decision
  (`DataDeliveryOwnerTest`).
- **Snowflake and BigQuery take the payload as ONE bound parameter** the database splits (`FLATTEN(PARSE_JSON(?))`,
  `UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(?)))`): a plan value (`Binding.Payload`, plan version 46), never the statement's
  text, no temp table, no drop. **Verified on the lanes at fba0a0f1:** a payload, an empty one and one holding a quote,
  a backslash and a line break through the product's own path on real Snowflake and BigQuery, the session sent the
  query alone; PCT channel A all green on both (Snowflake 1,249 tests in 584 s against 654 s at its round 22).
- **Databricks:** probe sent ("Check if we want to do same for Databricks ... so at least the cloud ones are
  consistent"); measured (probes/DatabricksDelivery2.java.txt): the bound parameter is the fastest form up to the
  driver's 1 MiB parameter limit (0.56 s at 10,000 records), inline rows take 100,000 in 4.2 s, today's session table
  is the slowest at every size (134 s at 100,000). Recommendation asked: the bound parameter, inline rows above 1 MiB.
- **Pasted CSV as one bound JSON parameter:** an option to investigate in the data-loading program
  (`DATA_LOADING_TARGET_MODEL` §5 item 6).

**The JSON pass, built (2026-11-11, branch `claude/total-wall-json`; the user's rulings: "Yes move everything off temp
table, for h2 should we also inline so we can get rid of all the temp table code completely?", "Yes on Databricks", "Yes,
include the digest reference in the JSON pass"):**
- **The digest reference (plan v48; 47 is B3's).**
  - A payload slot is `Binding.Payload(digest)`. The text is its statement's `content`, the same content-by-digest
    bundle a load uses: inline below `PlanJson.INLINE_LIMIT_BYTES`, a blob beside the plan above it, and verified
    against its digest where the plan is read.
  - The statement's typed tree names the payload by its digest too (`SourceUrl.byDigest`), so the bytes appear once in
    the plan. A render of such a tree is refused by name.
  - An inline payload too: the planner tags the rows a dialect writes (`SqlSource.Values.payload`), and the carried tree
    names them by digest. Before, Databricks' inline plan was 8.7 MB for a 1.09 MB payload (the lane's finding). Now a
    1.18 MB payload plans to 1.42 MB on H2, 1.36 MB on Databricks and 1.30 MB on DuckDB (bound).
    `DataSourcesDeliveryTest.aLargePayloadsPlanIsAboutTheSizeOfItsData` pins it: the payload once, the plan under twice
    its size.
  - The runner fetches the text by digest and binds it.
  - The TypeScript runner's `PLAN_VERSION` is 48. Test plans are version 17, with no release.
- **Every dialect's `deliver`** returns the statement's source (§4.3): bound wherever the database splits one
  parameter, inline on H2 and MongoDB, and on Databricks above 1 MiB.
- **Deleted:**
  - In the dialects: the session table (`SqlDialect.DataTable`, `dataTable`, `dataSchema`, `sessionTable`,
    `Delivered`, `dataSourceColumn`; H2's `LEGEND_SESSION`, Postgres's `pg_temp` spelling).
  - In the plan and exec: the plan's and the test plan's `release`, the `SOURCE` and `AT_RELEASE` cadences,
    `PlanSessions`' release, `ProductRun`/`TestRun`'s releasing, and `PlanRunner.release`.
  - In the planner: `DataSources.Tables` and its home tracking (S6), `ProgramState`'s data loads, prepares and releases,
    `SystemCatalog`'s schema in the ONCE load and `madeOnce`, and `BodyDriver.ExecEnv`'s shared tables (B4's
    `sharingTables`).
  - In the edge: `LiveSourceLoad`.
  - The census's M18 pins these symbols at zero.


## 6. What remains, in order

| step | what | done means | read |
|---|---|---|---|
| B (rest) | §4.4 items 1-4 | each test exists and passes; S190 closed; the CI lane guard passes | §4.4 |
| B2d | the generator as one plan (§4.4 item 5) | census M13-output DELETED; S143's tests run; the plan round-trips as JSON; the matrix has the generator's door | `TEST_DATA_GENERATOR_DESIGN`, `B_BRIEF` §7, §9 |
| B3 | the hosts run the plan's loads; the text plan from the execution plan; credentials by name; DuckDB's fence | census M14 DELETED; no secret in any plan (a sentinel guard); generatePlan/arrowPlan carry loads and bound paths, each changed text reviewed | `B_BRIEF` §8, `REMOTE_FILE_CREDENTIALS_HOMEWORK` |
| B4 | the catalog in process | census M5, M6 DELETED; Postgres's 3 red cells green | `B_BRIEF` §6 |
| S5, S13 | the date-time arms (§4.4 item 3) | tests; newly refused PCT tests ledgered under S181 | §4.4 |
| ✔ B | **checkpoint** (§7.4) | census pending 0; two audits with findings fixed; a lane round on every database; the timing; the PR question | |
| C1 | the trace comment out of the sent text | `JdbcStatements.stamped` and kin deleted | `C_E_BRIEF` §C1 |
| C2 | MariaDB's read-back declared by the plan | exec's SQL edits at run read 0 besides the keyed region | `C_E_BRIEF` §C2 |
| ✔ C | **checkpoint** | | |
| D1.0-D1.7, D2 | the edge, in this order: the spelling hook first; Raise/RaiseKind (after C2); the text nodes; lists, maps, format; the writers; the decorator; the compile messages; engine text, RawSqlBoundary, the referee and appeals; pure/v1's upstream routes to the edge; then D2's guard | no product SQL or MIR carries a compatibility spelling (D2's guard); PCT green through the hook; `D_BRIEF` §5's register rows | `D_BRIEF`, `COMPAT_IN_PRODUCT_INVENTORY`, `PRINT_FORM_PROBE` |
| ✔ D | **checkpoint** (print forms checked on every database) | | |
| E1, E2 | the eight E items | `C_E_BRIEF` §E's pins; register P3-R1 to R5 | `C_E_BRIEF` §E, `LEAN_PRODUCT_P3` §12 |
| ✔ E | **checkpoint** | | |
| F1 | one full run: every database, `//gates:local`, both corpora, both stress suites, H2's PCT, unprivileged Postgres | every break fixed | §8 |
| F2 | ONE PR into `main` | opened with the template; CI green on all OSes; **never merged by a session** | §1 |

E1's items 1, 2 and 4 come after D1.

## 7. How to work (the runbook)

### 7.1 Roles

- **The orchestrator:**
  - plans, relays and audits;
  - drives the lane sessions;
  - talks to the user;
  - keeps this file's §4 current.
  It builds nothing large. It may make small commits of its own (docs, deleting files it created), always rebased, never
  touching code the worker is changing.
- **ONE worker** builds §6 in order. After each push it sends the head and the scoreboard.
- **Lane sessions** measure only: targets at a head, and probes. They change nothing.
- **Auditors** are the orchestrator's local subagents, read-only on a detached worktree in a scratch folder. Remove the
  worktree after.

### 7.2 The loop

1. The worker builds one change, runs the fast loop (§8.1), commits with the checks it ran, pushes, and reports.
2. At every significant push (a step landing, or a change to dialects, delivery, dates or the edge), the orchestrator
   sends a lane round, at that head, to the sessions whose databases the change touches. All of them run in parallel.
3. Results return. A new failure, or an expected-failure entry that now passes, goes to the worker to fix forward.
4. When a step lands, an audit (§9.3) runs on its range, beside the work.
5. Re-check the worker on a timer about every 75 minutes, and nudge it only if it is idle with no job running.
6. **CI:**
   - CI runs on pull requests and on `main` only.
   - The Claude GitHub app could not start it by hand: `workflow_dispatch` returned 403.
   - So before the PR exists, the lane sessions are the wide run, and Windows and macOS are first checked by the PR.
   - A CI run takes about 50 minutes, on Linux, macOS and Windows in parallel.

### 7.3 Lane rounds

Each lane session runs one database group. A typical split:

| group | lanes |
|---|---|
| 1 | Trino, ClickHouse and Postgres PCT, both corpora, stress, the server targets |
| 2 | everything in `//core/...`; PCT DuckDB, H2, SQLite, MariaDB, MySQL, ClickHouse, Postgres; channel B; `//core:data_source_servers_test` |
| 3 | Snowflake |
| 4 | BigQuery |
| 5 | Databricks |

- **PCT runs:** each PCT lane runs channel A, channel B under BOTH (the default), and channel B under DATABASE
  (`--jvmopt=-Dpct.channelb.judge=DATABASE`; cloud lanes pass it through `JAVA_TOOL_OPTIONS` via `--test_env`).
- **Fit:** two or three embedded servers fit in one container. Run the PCT group with `--local_test_jobs=1 --jobs=2`.
- **Setup:** `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md` §9 says how to set lane sessions up and message them.

### 7.4 Checkpoints (the end of B, C, D, E)

1. Two independent audits of the phase's range, one after the other:
   - **(a) Tenets:** G1-G11 and the invariants. Look for wall breaks, fallbacks, SQL outside a dialect, MIR strings
     encoding SQL, and guard moves without a dated justification.
   - **(b) Correctness:** wrong answers, crashes, state leaking between bodies or sessions, and fixes that have no test
     which would fail if they were reverted.
2. A lane round on every group, at the phase's head.
3. The timing: the fast loop, the wide set and the lanes.
4. Every finding fixed. The one exception is a fidelity edge case outside the wall: it is refused by name, with a
   register row the user rules.

### 7.5 Triage (`DATA_DELIVERY_DESIGN` §8)

- Crashes, wrong answers and wall breaks are fixed.
- A narrowing (it worked, now it refuses) is a defect unless the user rules it. Make it work instead, as JSON showed.
- A fidelity edge case outside the wall is refused by name with a register row, POSTED to the user before it lands.
- A push that moves no scoreboard number and fixes no crash or wrong answer is out of scope.
- Spot-check an audit's MUST-FIX in the code yourself before relaying it.

### 7.6 Lessons that cost time before

These are from `PCT_CORE_CLOSEOUT` §7.8 and this program.

- **Commits:**
  - Commit only the files you mean: run `git status` and `git diff --stat --cached` before each commit. A `git add -A`
    once broke the branch on every OS.
  - Fetch and rebase before every push: another session may have pushed to the branch.
- **Builds:**
  - The local gate does not build everything. After an API change, `bazel build` the test targets that use it.
  - A manual target must be in a lane or in `heavy`, or CI's lane guard fails.
  - Foreground shell commands stop at 10 minutes: run long checks in the background and read their logs.
- **Disk:** the allowance is fixed. Delete big build outputs you no longer need. Never run `bazel query //...`. Never
  delete the LLVM tarball.
- **Probes:**
  - Hand-written probe SQL has its own bugs. BigQuery and Databricks quote `'` as `\'`, so `''` is wrong there.
  - A failing probe step may be the probe, not the database.
- **Process:**
  - Measure, never guess, and never recall upstream: read the pinned sources or ask the engine runner.
  - Never act on your own recommendation as if the user had chosen it.
  - Answer "what is going on" in a few plain lines: done, running, next, what needs the user.

## 8. How to run things

### 8.1 The fast loop (per change, about 10 minutes)

```
bazel test //core:census //core:guardrails //core:core_tests_root \
  //core:core_tests_sql //core:core_tests_planner \
  //core:core_tests_integration //core:core_tests_integration_h2 //core:core_tests_integration_sqlite \
  //core:core_tests_<each package touched> //docs:all --test_output=errors
```

- **Package tests:** they are `//core:core_tests_<pkg>`, made from `_CORE_TEST_PACKAGES` in `core/BUILD.bazel` (for
  example `core_tests_testplan`, `core_tests_setup`, `core_tests_testdatagen`).
- **The matrix:** `DataDeliveryMatrixTest` runs inside `core_tests_integration` and its lanes. The H2 and SQLite lane
  floors are `core/src/test/resources/lanes/{h2,sqlite}.txt`.
- **The embedded servers:** `bazel test //core:data_source_servers_test` (manual; Linux x86_64; ClickHouse, MariaDB,
  MongoDB, MySQL, Trino) when delivery or a dialect changes.
- **The judge matrix:** `//core:core_tests_<pkg>_judge_{database,both}`, suite `//core:core_tests_judges`.
- Also run `//spec:spec_tests` when spec changes.

### 8.2 The wide set, the gates and the corpora

- **The local gate:** `bazel test --lockfile_mode=error //gates:local`. It is `:core, :checks, :warehouse_jvm,
  :datacube, :ui, :sdlc`.
- **`//core/...`** (some messages call it `//core:all`) is every non-manual target in core, stress and Postgres
  included.
- **The wide set used before:** `//gates:local`, `//core/...`, `//pct:pct_duckdb`, both corpora, `//wasm/...`,
  `//engine-client/...`, `//parser-equivalence/...`, `//docs/...` and both stress suites. About 510 targets, about 50
  minutes.
- **Corpus:** `//gates:corpus_duckdb` and `//gates:corpus_h2` run `//spec:corpus_{duckdb,h2}`.
  - Rosters: `spec/src/test/resources/rcorpus/`.
  - Re-bless: `bazel run //spec:update_rcorpus_<lane>`.
  - One test: `bazel run //spec:corpus_one -- <lane> <host|database> <fqn>`.
- **Stress:** `//gates:stress` (`//core:stress_suites`, `//core:stress_suites_h2`; floors `MIN_PASS` in
  `StressServiceSuitesTest` and `StressServiceSuitesH2Test`).
- **Server:** `//core:core_tests_server`, `_server_judge_database`, `_server_judge_both`.
- **Unprivileged Postgres** (initdb refuses root):
  1. `chmod o+x /root` once.
  2. Copy `tools/postgres/as_unprivileged.sh` to a scratch folder.
  3. Run `bazel test <targets> --strategy=TestRunner=standalone --run_under="<scratch>/as_unprivileged.sh ubuntu"
     --local_test_jobs=1`.
  - Targets: core's Postgres tests (`postgres_arm`, `host_operations`, `data_loads`, `data_delivery_matrix_postgres`,
    `sql_island`, `sql_parity`, `iq_parity`, `cypher_parity`, `store_writes`, `relation_to_one`), plus
    `//pct:pct_postgres` and `//pct:pct_channel_b_postgres`.

### 8.3 PCT per database (`pct/BUILD.bazel`)

- **Channel A:** `//pct:pct_<db>` for `duckdb`, `h2` (Relation only), `postgres`, `sqlite`, `mariadb`, `mysql`, `trino`,
  `clickhouse`, `mongodb` (channel A only), `snowflake`, `bigquery`, `databricks`. Each suite is also its own target,
  `//pct:pct_<db>_<suite>`.
- **Channel B:** `//pct:pct_channel_b` (DuckDB) and `//pct:pct_channel_b_<lane>` for the others; MongoDB has none.
- **Manual targets:** all except DuckDB's, H2's and Postgres's.
- **In a cloud container,** first run `tools/cloud-distdir.sh <targets>`, which packs the archives GitHub refuses to
  serve, then `tools/cloud-maven-distdir.sh` for Maven gaps (`docs/CLOUD_SESSIONS.md`).

### 8.4 Embedded servers (no Docker: each test JVM starts its server on loopback)

The harnesses are `testing/src/main/java/com/legend/testing/Embedded*.java`; versions are pinned in `MODULE.bazel` and
`tools/<db>/<db>.bzl`.

| server | version | notes |
|---|---|---|
| Postgres | 16.15.0 | run unprivileged |
| MariaDB | 11.4.13 | Linux x86_64 |
| MySQL | 8.4.11 | Linux x86_64 |
| Trino | 483 | memory connector, 2 GB heap |
| ClickHouse | 25.8.2.29 | Linux x86_64 |
| MongoDB | 8.0.17 | Linux x86_64 |

### 8.5 Cloud databases (Snowflake, BigQuery, Databricks)

- **The properties file:** an environment variable, `LEGENDLITE_PCT_CLOUD_PROPERTIES`, names a properties file outside
  the repository. The targets read it through `env_inherit` (`pct/BUILD.bazel`, `CloudBackend.java`).
  - Its keys per cloud `c`: `c.url`, `c.property.<name>`, and the per-cloud keys listed in
    `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4.
  - For example `bigquery.system.property.EnableSession` for BigQuery's system connection, and
    `snowflake.system.property.schema`.
- **The credential files:** the credential properties name key files or tokens (Databricks a personal access token;
  Snowflake a key-pair file and passphrase or a password; BigQuery a service-account key file).
  - A new account gets them from the user, written into the session's environment by an environment setup script.
  - `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 and §6 have the format and the setup.
  - **Never paste a secret into chat, never print one, and never commit one.**
- **Proxy settings:** `~/.bazelrc` needs `test --test_env=JAVA_TOOL_OPTIONS --test_env=HTTPS_PROXY --test_env=https_proxy
  --test_env=NO_PROXY --test_env=no_proxy` (§6 of that doc).
- **Without the credentials:** run every other lane, and say plainly that the three cloud databases are unmeasured.

### 8.6 Upstream: the pinned sources and the engine runner

- **The pins:** legend-engine 4.145.0 and legend-pure 5.99.0, in `release.MODULE.bazel`. Tests get the roots
  automatically (`junit_test(upstream=True)`); never pass them by hand.
- **Reading a source:** `$(bazel info output_base)/external/+http_archive+legend_pure_src` (and
  `+http_archive+legend_engine_src`).
- **The engine runner**, to ask upstream for its answer: `//tools/engine-runner:testable`, the real engine jars (about
  600 MB). The recipe is in `docs/result-boundary-homework/README.md` §7:
  1. build it;
  2. put the classpath in a directory;
  3. compile a probe (parse, compile, plan, execute);
  4. run it with `--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx3g`;
  5. record the answer under `core/src/test/resources/upstream-api/`.

### 8.7 Bazel in a cloud container (`docs/CLOUD_SESSIONS.md`)

- Bazel 9.2.0 through bazelisk.
- Disk: about 8 GB for the output base and about 20 GB for the repository cache. Turn the disk cache off with
  `build --disk_cache=`.
- A first PCT build takes about 6 minutes.

## 9. Templates

### 9.1 The worker's first message

```
You are the WORKER of the total-wall program on neema2/legend-lite, branch claude/total-wall (never main). Read
docs/TOTAL_WALL_START_HERE_2026_11_11.md fully, then AGENTS.md, docs/DATA_DELIVERY_DESIGN_2026_11_10.md and the brief
for your current step in docs/data-delivery-homework/. Build §6's steps in order from the first not done; check §4.4's
in-flight items against the code first (done only if its test exists and passes).
Rules: no fallbacks (AGENTS.md invariant 4); SQL only in a dialect; exec runs only the plan's statements; every
divergence from upstream read from the pinned sources (or the engine runner), posted to the orchestrator before it
lands, and ledgered in docs/SEMANTICS_REGISTER.md (this program's block S171-S199; next free S191 unless taken); a
narrowing is a defect unless ruled; a guard or ratchet moves only with a dated justification comment; never delete a
file you did not create; commit only the files you mean (git status first); fetch and rebase before every push; never
push main; never open or merge a PR; never read or print a credential.
Loop per change: build, the fast loop of §8.1 plus the packages you touched (about 10 minutes), commit with the checks
you ran in the message, push. After each push send the orchestrator the head and the scoreboard: census rows pending;
matrix red; SQL edits exec makes at run; the timing. Fix lane and audit findings forward in your next commits. Keep §4
of the start-here file current when a step lands. Measure, never guess.
```

### 9.2 A lane round

```
Round <N>, measure only. Fetch claude/total-wall and check out <HEAD> (detached). Run <TARGETS> (for PCT: channel A,
channel B under BOTH, channel B under DATABASE). Report per target pass/fail against round <N-1>, each new failure's
test name and first error lines, any test that now passes but is still named in an expected-failure list, and each
target's wall time. Change nothing, commit nothing.
```

For a probe, say what to establish, give the repository's probe file, and ask for the exact SQL that worked and its
output.

### 9.3 An audit (a local, read-only subagent)

Make the worktree with `git worktree add --detach <scratch>/audit <head>`.

```
You are an independent, READ-ONLY auditor. Do not edit, commit, push, run Bazel, or message anyone. A detached
worktree is at <PATH> (HEAD = <head>); the delta is `git diff <base>..<head>`. Read AGENTS.md (G1-G11, invariants 3,
3a, 4), docs/DATA_DELIVERY_DESIGN_2026_11_10.md, and the step's brief. The worker claims: <claims>. Verify each.
Hunt for: wall breaks (exec deciding from row values, editing SQL, reaching planning packages; the plan holding code,
a machine path or a secret); fallbacks; SQL outside a dialect; MIR strings encoding SQL; state leaking between bodies
or sessions; drops skipped on failure paths; a drop that could touch a user's table; guard moves without a dated
justification; fixes with no test that fails if they revert. Report under ~700 words: MUST-FIX / SHOULD-FIX / NOTE,
each with file:line, a concrete failure scenario and a one-line fix; then each claim CLOSED (test name) or NOT CLOSED
(why). No preamble.
```

Spot-check a MUST-FIX before relaying it. Remove the worktree after.

## 10. The later list (outside this PR; start none without the user)

- **Protocol and data:**
  - the native protocol, the next program;
  - the data-loading program: the format spec, the conformance corpus, native bulk loaders, one reader per database
    (`docs/DATA_LOADING_TARGET_MODEL_2026_11_11.md` §5);
  - the nanosecond type;
  - safe versions of the raising functions (`safeDivide` first);
  - cross-runtime queries (S187);
  - time zones outside an `execute` frame (S183).
- **Features M17 names:** ModelStore, ExternalFormat, ServiceStore and Relation data; mapping and function test suites.
- **Model data for these column types:** BINARY, VARBINARY, OTHER, DISTINCT, ARRAY, OBJECT.
- **A statement over both the catalog and user tables.**
- **The catalog's turn lock, after the total wall** (B4's audit, finding 6, 2026-11-11). One lock with no timeout
  serializes every run reading an engine's catalog, process-wide, held while a product query streams its answer.
  Proposal: (A) a pool of connections to one in-memory catalog per engine, each run its own transaction (DuckDB
  `duplicate()`, H2's same in-memory URL; SQLite stays serialized), and (B) meanwhile a bounded wait refused by name
  ("the catalog is busy"). Measurement plan: N concurrent runs registering the same model's rows on one DuckDB and one
  H2 catalog through pooled connections, recording conflicts, blocking and throughput against today's single turn.
- **Programs:** P5 (judge parity on H2); P6 (no fallbacks, honest appeals); the corpus and stress burn-down.
- **PR #81** (timeseries), in its own session.

## 11. Where things are

| what | where |
|---|---|
| the branch | `claude/total-wall` |
| the design and plan | `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` |
| briefs, measurements, probes | `docs/data-delivery-homework/` |
| the census | `core/src/test/java/com/legend/DataMechanismCensusTest.java` |
| the matrix | `core/src/test/java/com/legend/integration/DataDeliveryMatrix{,Postgres}Test.java` |
| the delivery declaration | `SqlDialect.dataDelivery`, `core/src/test/java/com/legend/sql/dialect/DataDeliveryDeclaredTest.java` |
| the sent-statement guard | `core/src/test/java/com/legend/planner/SentStatementsAreThePlansTest.java` |
| the register | `docs/SEMANTICS_REGISTER.md` (this program: S171-S199) |
| the gates | `docs/GATES.md`, `gates/BUILD.bazel`, `.github/workflows/gate.yml`, `gates-run.yml` |
| PCT and its expected failures | `pct/BUILD.bazel`; `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_*_PCT.java`; `pct/.../channelb/ledger.tsv` |
| earlier program documents | `docs/PLAN_AS_DATA_2026_11_07.md`, `docs/DATA_IN_THE_PLAN_2026_11_09.md`, `docs/TENET_CHARTER.md`, `docs/JAVA_EVICTION_PLAN.md`, `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md` |
| the next program | `docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md` |
| the data-loading target | `docs/DATA_LOADING_TARGET_MODEL_2026_11_11.md` |
