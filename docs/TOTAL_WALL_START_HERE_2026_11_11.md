# The total-wall program: start here (the orchestrator's handoff, 2026-11-11)

> Written by the orchestrator session at about 10:00 UTC on 2026-11-11, so that a fresh session (on this account or
> another, with no memory of this one) can run the program from here to its PR without stopping and without asking the
> user anything this file already answers. **The user's rulings below are final. Do not reopen any of them without the
> user's word, and do not ask the user a question this file already answers.** When this file and an older document
> disagree, this file wins for anything dated 2026-11-11; the design (`docs/DATA_DELIVERY_DESIGN_2026_11_10.md`) wins
> for anything older.

## 0. Read in this order (about an hour)

1. This file, all of it.
2. `AGENTS.md` (loaded automatically): the goals G1-G11, the invariants, especially 3, 3a and 4 (no fallbacks).
3. `docs/GOALS_OF_RECORD_2026_11_07.md`: the goals, ratified by the user.
4. `docs/DATA_DELIVERY_DESIGN_2026_11_10.md`. This is the approved design and the plan.
   - §1: the model.
   - §3: the failure rules.
   - §4: one road.
   - §6: the plan in order, steps A to F.
   - §6.1: the rulings on errors and print forms.
   - §8: how we stay on track.
   - §9: the later list.
   - "as built" sections: what A and B did.
5. The briefs for the remaining steps, in `docs/data-delivery-homework/`. Each has file:line maps, read before building:
   - `B_BRIEF_2026_11_10.md`: §6 B4, §7 B2d, §8 B3, §9 rulings.
   - `TEST_DATA_GENERATOR_DESIGN_2026_11_10.md`: B2d, APPROVED.
   - `REMOTE_FILE_CREDENTIALS_HOMEWORK_2026_11_10.md`: B3.
   - `C_E_BRIEF_2026_11_10.md`: C1, C2 and E's eight items.
   - `D_BRIEF_2026_11_10.md`: slices D1.0-D1.7 and D2.
   - `PRINT_FORM_PROBE_2026_11_10.md`: D1's measurements.
6. `docs/COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md` (D0, the inventory D builds from) and
   `docs/LEAN_PRODUCT_P3_2026_11_10.md` (E's rulings, its §12). Both are copied onto this branch from
   `claude/p2-compat-inventory` and `claude/p3-lean-product`.
7. `docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md`. This is the NEXT program, built after this PR. It is here so that
   nothing in D contradicts it.
8. `docs/SEMANTICS_REGISTER.md`: every deliberate difference from upstream.
   - This program's numbering block is S171-S199. It has used up to S190, so the next free row is **S191**.
   - The timeseries line owns S145-S170 and S200-S299.

## 1. What the program is, and how it ends

- **Total plan/exec separation (G4).**
  - The plan is SQL and data only.
  - Exec runs only the plan's statements and edits no SQL at run. The one allowed exception is a keyed region.
  - Data enters a database only through a load the plan carries.
  - Every compatibility spelling lives at the edge, and the product depends on no edge.
- **The phases:** P1 to P4 of the goals, built as phases A to F (`DATA_DELIVERY_DESIGN` §6):

| phase | goal | what it is |
|---|---|---|
| A | P1 | data delivery rebuilt: model data as SQL in a content blob, files as pointers the database reads, failure rules, one road. **CLOSED at 5e9fcef8** |
| B | P1 | the rest of P1: the data-mechanism census (`core/src/test/java/com/legend/DataMechanismCensusTest.java`) to zero pending |
| C | P4 | exec edits no SQL: C1, the trace comment (D3); C2, MariaDB's error read-back (D4) |
| D | P2 | the edge: every compatibility spelling moved out of the product; the product's own errors and print forms |
| E | P3 | lean product: the eight E items |
| F | | one full run on every database, then ONE PR |

- **The end.** The orchestrator opens ONE pull request, `claude/total-wall` into `main`, with the repository's PR template
  (`.github/pull_request_template.md`), and **never merges it: the user merges.**
- **When to open it:** the orchestrator recommended opening it at B's checkpoint, so that CI checks Windows and macOS
  before C, D and E pile on. **The user has not ruled on that.** Ask once at B's checkpoint ("open the PR now so CI runs
  on all three OSes?"); without a yes, open it at F2.

## 2. The user's standing words and the hard rules (verbatim where quoted)

- **The go.** "Keep going don't stop until done, make sure fast dev loop but don't skip the audits" (2026-11-11). Also:
  "let's now keep going dont stop until all done"; "We still need to do audits at every important checkpoint, not just
  at the end".
- **The standing constraints.** "never push to main or open/merge a PR without my word; commit only the files you mean and
  push to the branch; never move or park my work without asking; no fallbacks; no hour-long waits or rabbit holes; when
  I ask what's going on, answer in a few plain lines. Lane sessions measure only."
- **Never read, print or copy a credential.** The plan never holds a secret.
- **"No second worker"** (2026-11-11). One worker builds. The orchestrator offered a second worker on B2d for speed and
  the user has not said yes. Do not start one unless the user says so.
- **PR #81 (timeseries) is OUT of scope:** "Don't do the 81 trial. Focus on our work."
- **The worker's own rule from the user:** never delete files it did not create. A file another session created is
  deleted by its creator or on the user's word.
- **"A divergence that is not a register row is a defect; a row the user did not rule is a defect too"** (AGENTS.md,
  upstream fidelity).
  - Ask "can we do exactly as pure/engine does?" first.
  - Upstream's answer is READ from the pinned sources or ASKED of the engine runner, never recalled.
- **Speed (the user, 2026-11-11: "Why are we going so slowly on B? ... We need a much much faster dev loop";
  "We have warm sessions for all the databases to run in parallel, you haven't been leveraging those").** See §6.

## 3. The state at handoff (2026-11-11, about 10:00 UTC)

### 3.1 Commits on `claude/total-wall` since A closed

```
fca77fec8 Trino's scalar TIMESTAMP keeps nine digits (the audit of 862a4d02..2f164ec4, M1)
5fe98b929 Census: M3 and M15-referee DELETED (B2b), their files gone with 00e67e38
db2558950 Merge remote-tracking branch 'origin/claude/total-wall' into claude/total-wall
2bae908f  JSON on every database (SqlDialect.dataDelivery: SESSION_TABLE / INLINE / REFUSED)
00e67e38  (orchestrator) B2b: delete edge/SeedTexts.java and base/CsvCells.java
57e2c445  B2b: the referee replays its own H2 copy of the plan's loads; no Java reads a file's cells
2f164ec4  B2a audit fixes (S1-S4) and Postgres's data: session table; the other databases measured
3a3defa0  Trino: every date-time arm keeps a nanosecond value's nine digits, or refuses by name
862a4d02  B2a: the JSON frame onto data:, the judges' data: loads, the edge's live-source record (M10 DELETED)
69cb9a32  B2a, first piece: a program's seeds are the load planner's
5e9fcef8  Round 34 ... (A CLOSED)
```
Run `git log --oneline 5e9fcef8..origin/claude/total-wall` for anything newer.

### 3.2 The scoreboard (reported by the worker on every push)

- **Census rows pending: 4.**

| row | what it is | closed by |
|---|---|---|
| M5, M6 | the system catalog's server forms | B4 |
| M13-output | the test-data generator's rows handed on as seed text | B2d |
| M14 | the browser, Python and DataCube load data outside any plan | B3 |

  - DELETED so far: M1, M2, M3, M8, M10, M15-referee.
  - BOUNDED: M9, M15-fixtures, M17, M18.
  - STAYING: M7-seed, M7-authored.
- **The data-delivery matrix:** green in process. On Postgres, 3 catalog cells are red, and B4 owns them.
- **SQL edits exec makes at run: 2 pending,** plus the allowed keyed region (`PlanRunner.expand`):
  - D3, the trace stamp: C1 removes it.
  - D4, MariaDB's read-back: C2 removes it.
- **The timing.**
  - The fast loop takes about 10 minutes per push: census, guardrails, core root, sql, planner, integration and its
    H2/SQLite lanes, the embedded servers, docs.
  - The wide local loop is 510 targets, about 50 minutes. It is no longer run per push.
  - The unprivileged Postgres lane is 11 of 11 in about 170 s.

### 3.3 Done in B

Each is described in `DATA_DELIVERY_DESIGN` §6 and its "as built" text.

- **B1:** seed forms STAYING.
- **B2c:** the fixtures are BOUNDED by `FixtureWriteCensusTest`.
- **B2e:** M17 BOUNDED.
- **B2a:** the JSON frame on `data:`, with session tables, plus its audit fixes.
- **B2b:** the referee replays its own H2 copy of the plan's loads. `SeedTexts` and `CsvCells` are deleted.
- **Trino nanoseconds:** each date-time arm keeps 9 digits or refuses by name.
  - Trino's `CAST(... AS TIMESTAMP(6))` ROUNDS.
  - Microsecond arithmetic goes through epoch nanoseconds in `DECIMAL(38,0)`.
  - `STRPTIME` with micro or nano parts is refused by name.
- **JSON (a `data:` payload) on every database:** see §4.2.

### 3.4 In flight at the worker (sent, not yet confirmed landed; verify each in the code)

1. **M1:** Trino's scalar TIMESTAMP is now (9). Landed in fca77fec8. Verify a test lowers a real `parseDate` and
   `cast(@DateTime)`.
2. **Snowflake INLINE.** A lane measured it at 5fe98b92:
   - CREATE TEMPORARY TABLE is private, but NO drop is safe. `DROP TABLE IF EXISTS` drops the user's PERMANENT table when
     no temp table exists (it happened), and `DROP TEMPORARY TABLE` is a syntax error.
   - So Snowflake's form is `SELECT PARSE_JSON(column1) AS data, column2 AS u_frame_ord__ FROM VALUES ('…', 0), ('…', 1)`.
   - Empty: `FROM VALUES (NULL::STRING, 0) WHERE FALSE`.
   - Or FLATTEN over `PARSE_JSON('[...]')`, whose empty case is `'[]'`.
3. **BigQuery INLINE.**
   - A TEMP table needs `EnableSession=1`. Lite's ordinary connection refuses one ("requires a script or session").
   - An unqualified temp name falls through to the URL's DefaultDataset, so only `_SESSION.<name>` is safe.
   - The form: `SELECT data, ord FROM UNNEST([JSON '…', JSON '…']) AS data WITH OFFSET AS ord`.
   - Empty: `UNNEST(CAST([] AS ARRAY<JSON>))`.
   - BigQuery quotes `'` as `\'`. The repository's `probes/BigQueryDataTable.java.txt` has two quoting bugs; fix that text.
4. **The empty payload on every INLINE dialect** (Trino, ClickHouse, MongoDB, and now Snowflake, BigQuery) gives zero
   rows, never a refusal:
   - Trino and ClickHouse: one typed NULL row with `WHERE 1 = 0`.
   - MongoDB: `$documents: []`.
   - Each is tested in `//core:data_source_servers_test`, a manual target that starts the embedded servers.
5. **H2's system catalog.** The orchestrator VETOED the worker's refusal of "a system-routed body reading a `data:`
   source on H2". Instead:
   - `LEGEND_SESSION` becomes part of the system catalog's own once-made schema, so nothing commits mid-run.
   - The `ProgramState.placeMade` refusal goes.
   - Add a test.
   - Confirm that the catalog's release runs after the catalog's run transaction is rolled back. H2's DROP commits.
6. **The second audit's findings** (on 862a4d02..2f164ec4). Fold them into the next commits:
   - **S5, MAKE_TIMESTAMP:** `Trino.java` formats seconds `%09.6f` into TIMESTAMP(6), and `DateCtorRule`'s ISO_MICRO path
     cuts a Float second to 6 digits on every dialect. Apply S181: keep 9 where the target keeps 9; elsewhere refuse a
     literal with more than 6 fraction digits (shortest repr) at planning, and a parameter by a SQL guard or by name.
     Report any newly refused PCT test, ledgered under S181.
   - **S6, the cross-database refusal:** `DataSources.Tables.read` (`:131-145`) only checks that the reads agree.
     - Make `Tables` hold its home target and refuse any `on` that differs.
     - Pass `routed` through (`ProgramState :138`; `PlanMaker :581/:618`), and test it.
     - If the refusal newly refuses ANY test, make per-target tables instead: one make, fill and release per target.
   - **S7, frame runs:** they are shifted by an integer `ahead` (`PlanMaker :453`, `TestDoor :131`, `:660`). Track them by
     node identity, as `byStep` does, and test H2 + `data:` + an eager frame run.
   - **S8, one typed refusal:** an unread `data:` source throws `IllegalStateException` in `AnsiSqlRenderer :323` but
     `DialectCapability` in MongoDbPipeline. Make them one typed refusal, and test it.
   - **S9, tests that fail if a fix reverts:** the payload cut ahead of the make and release (`DataSources :210-231`), and
     `plan.release().on()` is System in a system-routed test.
   - **S10, Postgres:** qualify the session table with `pg_temp` (`Postgres.java:231`; today its schema is null).
   - **S11, H2's release order:** the release must be sent AFTER a writing body's rollback, because H2's DROP commits.
     Test a writing body + `data:` on H2.
   - **S12, the query door's drops:** the `data:` drops are trailing steps, so a failure skips them. Run them on every
     outcome.
   - **S13, precision past 2255:** `epochBoundaries` and `timeBucket` divide microseconds as DOUBLE, exact only to about
     2255. Use `DECIMAL(38,0)` or BIGINT. Also check whether Trino's `date_trunc('microsecond')` (`intervalUnit`,
     `Trino.java:1143`) fails at run, and refuse it by name if so.
   - **NOTE:** stale "AT_STEP" wording at `ProgramState :131, :172` and the design doc's line 126.
7. **Then B2d** (the generator), then **B3**, with **B4** alongside.

### 3.5 Lane round 35 (in flight at handoff)

| session | head | running |
|---|---|---|
| **1a** | 00e67e38, then 5fe98b92 | Trino, ClickHouse and Postgres PCT, nine runs each time (channel A, channel B BOTH, channel B DATABASE); then `//gates:corpus_duckdb`, `//gates:corpus_h2`, `//gates:stress`, `//core:core_tests_server`, `_server_judge_both`, `_server_judge_database` at 5fe98b92 (a fresh baseline: round 34 had no corpora, stress or server targets) |
| **1b** | 00e67e38, then 5fe98b92 | `//core:all` and PCT DuckDB, H2, SQLite, MariaDB, MySQL, ClickHouse and Postgres, plus channel B; then MariaDB, MySQL, ClickHouse, Postgres (and MongoDB) PCT plus `//core:data_source_servers_test` |
| **Snowflake** | 5fe98b92 | the 15 PCT runs, against round 34. The data-table probe is DONE (§3.4 item 2) |
| **BigQuery** | 5fe98b92 | the PCT lanes. The probe is DONE (§3.4 item 3) |
| **Databricks** | 5fe98b92 | the probe first (temp view or temp table, private? safe drop? the inline form via VALUES or from_json/explode, and the empty case), then the PCT lanes. **Its probe result has not come back yet.** Its delivery follows the rule in §4.2. Until then S190 names Databricks alone |

**Expected:** some PCT tests now PASS that the lanes' expected-failure lists still name: tests that hit "sourceUrl
reached a dialect without ...". The worker trims those lists forward when a lane reports them.

### 3.6 Open questions for the user (ask once, at the natural moment)

1. **The PR's timing:** open it at B's checkpoint so CI covers Windows and macOS (recommended), or at F2 as planned (§1).
2. **Native file loaders:**
   - The recommendation: Postgres `COPY … FROM STDIN`, MariaDB/MySQL `LOAD DATA LOCAL INFILE` and ClickHouse
     `INSERT … FORMAT CSV`, built after this PR. Exec hands the file's bytes to the database's own reader, and no Java
     parses them.
   - Until then, a file load (`loadCsvToDbTable`, a `file:` source) on a database that cannot read files is refused by
     name. This is the user's "option 1" of 2026-11-10.
   - The user asked whether file loads need revisiting. The answer given: no, the idea holds (§4.1), and only coverage
     is missing.
   - Not ruled. Default: after the PR.

## 4. Every ruling of this program (dated; each is final)

The older rulings are in `docs/GOALS_OF_RECORD_2026_11_07.md`, `docs/PLAN_AS_DATA_2026_11_07.md`,
`docs/DATA_IN_THE_PLAN_2026_11_09.md` §8, `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` (header, §6.1, §6's B rows), and
the briefs' "RULED" items. Those of 2026-11-10 and 2026-11-11 are collected here.

### 4.1 Data: the holistic rule (2026-11-10 design; restated to the user 2026-11-11)

**Where the data lives decides how it is delivered, not its format.** CSV and JSON each appear on both roads.

| | **written in the model** (known at planning) | **a file** (outside the model) |
|---|---|---|
| examples | a connection's CSV test data, `###Data`, the catalog, `from()` seeds, a JSON connection's inline `data:` payload | `loadCsvToDbTable`, a `file:` source (CSV, JSON, Parquet), a remote file |
| the plan holds | the rows as SQL: DDL + chunked `INSERT … VALUES` of typed literals, in a content blob named by digest | a pointer (a reference + digest), never the contents |
| who reads it | the planner cuts the model text into literals (allowed: build-time model text is known at planning, like a literal; `docs/JAVA_EVICTION_PLAN.md` §1's rule) | the database, at run; exec binds the path, verified by digest |
| where it works | **every database** | DuckDB (CSV, JSON, Parquet), H2 (CSV, `CSVREAD`); every other database refuses a file load by name (option 1) |

- Why the two differ: a file can change between planning and run, can be 500 MB (upstream's limit), and can sit behind a
  credential.
- "No Java parses data" means **exec at run**. The planner reading model text is allowed.

### 4.2 JSON on every database (the user, 2026-11-11)

**The user's words:** "We need to make json work everywhere"; "Mongo database was literally created to hold json". So
S190's refusal of Trino and MongoDB is dropped.

**Why JSON needs a table at all.** A JSON connection feeds a model-to-model mapping. Its source is a Pure class, and no
store declares a table for it. Upstream parses the JSON in Java and runs the mapping in memory. Lite runs everything in
the database, so lite makes its own table for the records. That table holds each record's JSON text and its place, and
the SQL reads fields by path. Because records nest, the table is not flattened into typed columns.

The delivery is chosen per database at planning, as `SqlDialect.dataDelivery`:

| delivery | databases | form |
|---|---|---|
| SESSION_TABLE (a private temporary table) | DuckDB (`temp.`), H2 (`LEGEND_SESSION`, LOCAL TEMPORARY … TRANSACTIONAL), SQLite (`temp.`), Postgres (`pg_temp.`), MariaDB, MySQL (`DROP/CREATE TEMPORARY TABLE`) | the drop must be qualified so it can never touch a user's table |
| INLINE (the rows in the query) | Trino (`VALUES` + `json_parse`), ClickHouse (`values()`), MongoDB (`$documents` of `{"$literal": doc}`), Snowflake (`VALUES` + `PARSE_JSON`), BigQuery (`UNNEST` of JSON `WITH OFFSET`) | the empty payload gives zero rows |
| REFUSED by name | Databricks, until its probe reports | then SESSION_TABLE if a private table with a safe drop exists, else INLINE |

Census M18 (the inline form) is BOUNDED to the INLINE databases (`DataDeliveryDeclaredTest`).

### 4.3 B rulings (2026-11-10 and 2026-11-11)

- **B1:** the seed forms (`setUpDataSQLs`) stay the effect script's ("yes").
- **B2c:** the Java fixtures are bounded, not converted ("yes").
- **B2e:** data no feature reads yet is "not implemented yet", refused by name.
- **B2d, the generator** (APPROVED design, `TEST_DATA_GENERATOR_DESIGN`). One read-only query per table, its `WITH` clauses
  carrying the chain:
  - starting rows `ORDER BY` key `LIMIT 20`;
  - one level per navigation step, no recursion;
  - one read transaction;
  - **NO temporary tables** ("Do we need temp table for our implementation? We don't have to copy the impl of there is a
    better way").
- **The generator's other rulings:**
  - **The default starting rows ARE built** by upstream's rule, read from the pinned sources ("Is it much harder to
    support the default ... Great let's do it"). Upstream: `SELECT DISTINCT TOP 10` root keys from the query.
  - **An empty seed is no filter**, as upstream ("1&2 yes").
  - **Every level is ordered by its key**; upstream is unordered, a register row.
  - **The output is faithful** ("Yes"): a cell holding a comma, a quote or a line break is quoted (RFC 4180), and lite's
    test-data reader reads quoted line breaks.
  - **The reload checks are planned as SQL** ("4 yes").
  - **Upstream's SQL-text and count checks** (46 and 26) become a register row.
  - **`TestDataGeneratorRun` and the TDG appeal are deleted.** S143's skip is lifted when it passes.
- **B3:**
  - The hosts run the plan's loads.
  - The text plan (`generatePlan`/`arrowPlan`) renders from the execution plan, and each changed text is reviewed, never
    accepted in bulk. This lifts `PlanMaker.refuseLiveSourcesInText`.
  - Credentials by name: a planned `CREATE TEMPORARY SECRET` with bound values from exec's secret store; DuckDB 1.5.6
    measured to take bound parameters there. A guard keeps any secret value out of the plan.
- **B3, the server's file fence (2026-11-10):**
  - `--sources DIR` or `LEGEND_LITE_SOURCE_DIRS`; with none named, the server reads no `file:` source. The library stays
    open (any file).
  - **The database enforces the fence on DuckDB** ("Okay", then "Option 2"): `allowed_directories` +
    `enable_external_access=false` + `lock_configuration=true` as planned session steps, with the server's staging
    folder inside the fence.
  - **H2 stays as it is**, a known gap.
- **B4:** every catalog statement runs on the process's in-process catalog ("B4 sounds good for now"). The server forms are
  deleted. A statement mixing catalog and user tables stays refused by name; a product feature for that is later.
- **B2b's deletion:** the creator (the orchestrator) deleted `SeedTexts.java` and `CsvCells.java` (00e67e38).
- **CSV findings S184 and S186:** "Csv findings are fine".
  - S184: DuckDB keeps a record wide by one empty cell loaded.
  - S186: H2 loads short records.
- **A2's two questions:** "Yes to both". A non-ISO date cell is text the database casts. Binary and array data is not
  implemented yet. SQLite's TIMESTAMP decodes like the others.
- **S187:** several runtimes in one query are refused on every door. Cross-runtime queries are on the later list.
- **S189:** a NaN or Infinity Float cell in model data is refused by name. BIT accepts y/n.

### 4.4 Date-times (2026-11-10 and 2026-11-11)

- **S181:** a date-time with more fraction digits than its target keeps is REFUSED, never cut. Known values are refused at
  planning; a parameter is refused by its statement's own SQL guard.
- **Nine PCT tests are refused on Postgres, MySQL, MariaDB, BigQuery and Databricks.** They are ledgered "pending the
  nanosecond type" ("I guess refuse for now and put this on feature list for after total separation complete").
- **ClickHouse** keeps 9 digits through `DateTime64(9)`.
- **Trino** keeps 9 digits or refuses by name (§3.3).
- **The nanosecond type** goes on the later list (§9 of the design): a microsecond timestamp plus a sub-microsecond
  remainder, literals first.

### 4.5 D: the product's errors and print forms (2026-11-10; the full text is `DATA_DELIVERY_DESIGN` §6.1)

**Errors.**
- Strict and typed. A Float divided by zero raises.
- A product error is its kind and where it happened, a fixed text known at planning, never the value.
- Safe versions are NOT built (on the later list).

**Print forms.**
- A value is typed unless a string is asked for. Then:

| type | text |
|---|---|
| Integer | digits |
| Decimal | at its scale (use Decimal for exact amounts such as Bitcoin) |
| Float | 6 decimals, with trailing zeros and `.` trimmed |
| Boolean | `true` / `false` |
| StrictDate | `YYYY-MM-DD` |
| DateTime | ISO 8601 UTC, six digits + `Z`; nanoseconds only where stored |
| a list | a JSON array |
| a map | a JSON object |

- Objects and relations are refused, pointing to graph fetch.
- `format`: `%s %d %f %t{}` in the product; `%r` and the padding at the edge.
- `makeString` skips an empty element.
- NaN, Infinity and -Infinity print as those words.
- Lite's own writers (CSV, JSON, Arrow) are exact.
- Pure's exact texts are the edge's own SQL render target.
- Compile messages are in the product's own wording.
- Pivot names are bare.
- Engine-only SQL spellings go to an edge decorator.

**MongoDB's print spelling is its own** (D1). Measured: `$toString` prints `1`, `1e+20`, and dates in milliseconds with `Z`.

### 4.6 C (2026-11-10)

- **C1, RULED ("Agree with all"):**
  - The database receives exactly the plan's text: no per-run trace comment.
  - The execution id lives in lite's own trace and in the edge's activity report, where the corpus tests read it.
  - A register row records that upstream puts the id in the text.
  - Delete `JdbcStatements.stamped` and its kin (`C_E_BRIEF` §C1).
- **C2 (the brief's proposal; no user question is open):** MariaDB/MySQL's read-back pair is declared by the plan's
  target (`RaiseReadBack`, written by `SqlDialect.raiseReadBack()`, every dialect answering, no default) and sent through
  a counted, scoped path. `RaiseRecovery` and the connector overrides are deleted.

### 4.7 pure/v1 and the native protocol (2026-11-11)

- **The idea, the user's words:** "lite clean sheet protocol that can work local or remote super fast and typed and
  always arrow no matter if local or remote. Then we should have remote compatibility mode that is drop in for engine or
  ours ... Basically make everything speak lite native / fast / shaped how we want. Then again at edge a purev1
  compatibility mode that lite or engine could answer."
- **When:** "We can design now and then do it after this PR once we are 100% total separation".
- **So in D:** pure/v1 over HTTP becomes the edge's compatibility mode (this settles `D_BRIEF` §6 item 1, which was
  OPEN when that brief was written). Its upstream routes and
  engine-shaped answers move to the edge. Lite's own routes and the host protocol stay product.
  - **S125:** pure/v1 matches upstream's Arrow typing; the native protocol stays exact.
- **The native protocol** itself (`docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md`, six questions in its §6 for the user's
  later review) is BUILT in the next program, not this PR.

### 4.8 Process rulings (2026-11-11)

- **The fast dev loop.** Per change: build plus the touched packages' tests plus the in-process matrix (about 10
  minutes), then push. The wide run is no longer a per-push gate. Red found later is fixed forward.
- **Use the warm lane sessions in parallel at every significant push,** not only at checkpoints.
- **Audits run beside the work and never block it.** Their fixes fold into the next commits.

## 5. What remains, in order (each "done" is checkable)

| step | what | done means | read |
|---|---|---|---|
| B (rest) | §3.4 items 2-6 | each test exists and is green; S190 names no database except an unmeasured one | §3.4 |
| B2d | the generator as one plan | census M13-output DELETED; S143's tests run; the plan round-trips as JSON; the matrix has the generator's door | `TEST_DATA_GENERATOR_DESIGN`, `B_BRIEF` §7, §9 |
| B3 | the hosts run the plan's loads; text plan from the execution plan; credentials by name; DuckDB-enforced fence | census M14 DELETED; no secret in any plan (sentinel guard); generatePlan/arrowPlan carry loads and bound paths, each changed text reviewed | `B_BRIEF` §8, `REMOTE_FILE_CREDENTIALS_HOMEWORK` |
| B4 | the catalog in process | census M5, M6 DELETED; Postgres matrix's 3 red cells green | `B_BRIEF` §6 |
| ✔ B | **checkpoint** | census pending 0; two audits (tenets, then correctness) with every finding fixed; a lane round on all five lane sessions; the timing; the PR question (§3.6) | §6.3 |
| C1 | the trace comment out of the sent text | `SentStatementsAreThePlansTest` reads 1 (the keyed region only) once C2 lands | `C_E_BRIEF` §C1 |
| C2 | MariaDB's read-back declared by the plan | exec's SQL edits at run read 0 besides the keyed region | `C_E_BRIEF` §C2 |
| ✔ C | **checkpoint** | audits + fixes; lane round | |
| D1.0-D1.7, D2 | the edge: spelling hook first; Raise/RaiseKind; text nodes; lists/maps/format; writers; decorator; compile messages; engine text/RawSqlBoundary/referee/appeals; pure/v1's upstream routes to the edge; D2's guard | no product SQL or MIR carries a compatibility spelling (D2's guard); PCT green through the hook; the register rows of `D_BRIEF` §5 | `D_BRIEF`, `COMPAT_IN_PRODUCT_INVENTORY`, `PRINT_FORM_PROBE` |
| ✔ D | **checkpoint** | audits + fixes; lane round (print forms checked on every database) | |
| E1, E2 | the eight E items (REAL widened, BigQuery decimal scale in SQL, zoned raw refused, BC dates by DB text, kinds-from-text and positional reads deleted, pivot envelope typed by the plan, the writer guard over exec) | `C_E_BRIEF` §E's pins; register P3-R1..R5 | `C_E_BRIEF` §E, `LEAN_PRODUCT_P3` §12 |
| ✔ E | **checkpoint** | audits + fixes; lane round | |
| F1 | one full run: every database, `//gates:local`, both corpora, both stress suites, H2's PCT, unprivileged Postgres | every break fixed | §6.2 |
| F2 | ONE PR `claude/total-wall` → `main` | opened with the template, CI green on all OSes, **never merged by a session** | §1 |

Item order inside D and E: the briefs give it. C2 lands before D1.1. B2b, already done, came before D1.7. E1's items
1, 2 and 4 come after D1.

## 6. How we work (the runbook)

### 6.1 Roles

- **The orchestrator (you)** builds nothing big. It:
  - plans and relays;
  - runs audits;
  - drives the lane sessions;
  - answers the user in a few plain lines;
  - keeps this file current.
- It may make small commits of its own (docs, the deletion of files it created) on `claude/total-wall`. It always
  rebases first and never conflicts with the worker. It never edits code the worker is changing.
- **The worker** (one session, the user's effort setting) builds every step in order on `claude/total-wall`. After each
  push it sends the scoreboard (§3.2) and the head.
- **The lane sessions** measure only. They run targets at a head and probe databases, and change and commit nothing.
- **Audits** are local subagents of the orchestrator. They are read-only, on a detached worktree in the orchestrator's
  scratchpad. Remove the worktree after.

### 6.2 The loop

1. The worker builds one change, runs the fast loop (about 10 min), pushes, and sends the head and scoreboard.
2. At every significant push (a step landing, or a change to dialects, delivery, dates or the edge), the orchestrator
   sends a lane round to the sessions whose databases it touches, at that head. **Use all five in parallel; that is what
   they are for.**
3. Lane results come back. Any new failure, or an expected-failure entry that now passes, goes to the worker to fix
   forward in its next commit.
4. An audit runs on each landed step's range (e.g. `5fe98b92..<head>` once B2d lands), beside the work.
5. **CI:**
   - `gate.yml` runs on pull requests and on `main` only.
   - The Claude GitHub app CANNOT start it by hand: `workflow_dispatch` returned 403 "Resource not accessible by
     integration".
   - So until the PR is open, the lane sessions are the wide run on Linux, and Windows and macOS are first checked by
     the PR's CI.
   - A run takes about 50 minutes on Linux, macOS and Windows in parallel.
6. **Never** push to `main`. Never open or merge a PR except the one PR (§1). Never force-push the branch.

### 6.3 Checkpoints (at the end of B, C, D, E)

1. Two independent audits of the phase's whole range, one after the other, each a read-only subagent:
   - **(a) Tenets:** G1-G11 and AGENTS.md's invariants. Hunt for wall breaks, fallbacks, SQL outside a dialect, MIR
     strings that encode SQL, and guard moves without a dated justification.
   - **(b) Correctness:** wrong answers, crashes, leaks of state between bodies, missing tests that would fail if a fix
     reverted.
2. A lane round on all five lane sessions at the phase's head.
3. The timing check: fast loop, wide set, lanes.
4. Every finding fixed (or, for a fidelity edge case outside the wall, refused by name with a register row the user
   rules).

### 6.4 Triage (`DATA_DELIVERY_DESIGN` §8)

- Crashes, wrong answers and wall breaks are fixed.
- A fidelity edge case outside the wall is refused by name with a register row, and the divergence is POSTED to the user
  (the expression, upstream's answer, lite's) before it lands.
- A narrowing (something that worked and now refuses) is a defect unless the user rules it. Prefer making it work, as
  the JSON case showed.
- A push that moves no scoreboard number and fixes no crash or wrong answer is out of scope.

### 6.5 Talking to the user

- When asked "where are we", answer in a few plain lines: done, running, next, anything needing them.
- Post each new divergence, and any decision that is genuinely theirs, in one sitting with a recommendation.
- Do not ask about anything §4 rules.

## 7. Sessions, environment, credentials

### 7.1 The original account's sessions (reachable only from the account that made them)

| role | session id | runs |
|---|---|---|
| orchestrator | `session_0195rQEmsRT7otsv9tTmNxj5` | this file's author |
| worker | `session_0195rQEkmFr5eT7FgHFquBYX` | builds on `claude/total-wall` (xhigh effort) |
| lane 1a | `session_01FdBn7kuJDnnLMxLzVWqtJN` | corpora, stress, server lanes; PCT Postgres, ClickHouse, Trino |
| lane 1b | `session_01FSy7uuE3kKMYoJzWQWx3X6` | `//core:all`; PCT DuckDB, H2, SQLite, MariaDB, MySQL, ClickHouse, Postgres, channel B |
| Databricks | `session_017Ub2y2ECHqcMoxzLecjvDs` | Databricks PCT and probes |
| BigQuery | `session_01XJEeABN3XxCqvBhxsNSHwi` | BigQuery PCT and probes |
| Snowflake | `session_017WxTYpJVcaEYAYkubENkLj` | Snowflake PCT and probes |

- **Their environment:** `env_013JDMdMqhpKsXaJ4QXQCjdi`. The cloud databases' credentials live there, as saved network
  secrets and lane settings. **Never read or print them.**
- **The orchestrator's check-in:** trigger `trig_011XH7LL9BNvT5sF8ZWo1UQX`, re-armed about every 75 minutes with the state
  in its prompt.
- **The 7-day usage** was at "allowed_warning" on 2026-11-11. Five lane sessions at once spend it fast, so run rounds
  where they buy information.

### 7.2 From a different account

You cannot message the sessions above. To continue:

1. **Check out `claude/total-wall`.** The work is all on the branch. Run `git log` to see what landed after this file.
2. **Start ONE worker** with the prompt in §8.1, in an environment that can run Bazel (`docs/CLOUD_SESSIONS.md`:
   GitHub archives are refused in cloud sessions, so `tools/cloud-distdir.sh`; the disk allowance; root).
3. **Start lane sessions** with the prompt in §8.2.
   - The embedded servers (Postgres, MariaDB, MySQL, ClickHouse, Trino, MongoDB) run inside a session through the
     repository's harness.
   - **Snowflake, BigQuery and Databricks need credentials the new account does not have.** Ask the user to provide them
     to the new environment: in a cloud session, through the environment's secrets (`request_credential` asks for a key
     by host). Never paste a secret into chat.
   - Without those three, run every other lane and say plainly that the three cloud databases are unmeasured.
4. **Re-create a check-in** (`create_trigger` or `send_later`, about every 75 minutes) carrying the state.
5. **The original worker may have pushed past this file.** Read its last commits' messages: each carries its checks. Then
   continue from §5 at the first step not done.

## 8. Templates

### 8.1 The worker's first message

```
You are the WORKER of the total-wall program on neema2/legend-lite, branch claude/total-wall (never main). Read
docs/TOTAL_WALL_START_HERE_2026_11_11.md fully, then AGENTS.md, docs/DATA_DELIVERY_DESIGN_2026_11_10.md and the brief
for your current step in docs/data-delivery-homework/. Build §5's steps in order from the first not done (check §3.4's
in-flight items against the code first: each is done only if its test exists and passes).
Rules: no fallbacks (AGENTS.md invariant 4); SQL only in a dialect; exec runs only the plan's statements; every
divergence from upstream read from the pinned sources and ledgered in docs/SEMANTICS_REGISTER.md (next row S191), posted
to the orchestrator before it lands; a narrowing is a defect unless ruled; a guard or ratchet moves only with a dated
justification comment; never delete a file you did not create; commit only the files you mean; never push main; never
open or merge a PR; never read or print a credential.
Loop per change: build, the touched packages' tests plus the in-process data-delivery matrix (about 10 minutes), commit
with the checks you ran in the message, push. After each push send the orchestrator (session <ORCHESTRATOR_ID>) the
head and the scoreboard: census rows pending; matrix red; SQL edits exec makes at run; the timing. Fix lane and audit
findings forward in your next commits. Measure, never guess: a throwaway probe beats a recollection.
```

### 8.2 A lane round

```
Round <N>, measure only. Fetch claude/total-wall and check out <HEAD> (detached). Run <TARGETS>. Report per target
pass/fail against round <N-1>, each new failure's test name and first error lines, any test that now passes but is still
named in an expected-failure list, and each target's wall time. Change nothing, commit nothing.
```

- **Probes:** say what to establish, give the repository's probe file, and ask for the exact SQL that worked and its
  output.
- **What each session runs:** see §7.1. A new account names its own.

### 8.3 An audit (a local read-only subagent)

```
You are an independent, READ-ONLY auditor. Do not edit, commit, push, run Bazel, or message anyone. A detached
worktree is at <PATH> (HEAD = <head>); the delta is `git diff <base>..<head>`. Read AGENTS.md (G1-G11, invariants 3,
3a, 4), docs/DATA_DELIVERY_DESIGN_2026_11_10.md, and the brief of the step. The worker claims: <claims>. Verify each.
Hunt for: wall breaks (exec deciding from row values, editing SQL, reaching planning packages; the plan holding code,
a machine path or a secret); fallbacks; SQL outside a dialect; MIR strings encoding SQL; state leaking between bodies or
sessions; drops skipped on failure paths; guard moves without a dated justification; fixes with no test that fails if
they revert. Report under ~700 words: MUST-FIX / SHOULD-FIX / NOTE, each with file:line, a concrete failure scenario and
a one-line fix; then each claim CLOSED (test name) or NOT CLOSED (why). No preamble.
```

- Make the worktree with `git worktree add --detach <scratchpad>/audit-x <head>`.
- Spot-check a MUST-FIX yourself before relaying it.
- Remove the worktree after.

## 9. The later list (outside this PR; do not start any without the user)

- **The native protocol** (next program; design in `docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md`).
- **Native file loaders:** Postgres, MariaDB, MySQL, ClickHouse; cloud stages later (§3.6).
- **The nanosecond type** (§4.4).
- **Safe versions** of the raising functions (`safeDivide` first).
- **Cross-runtime queries** (S187).
- **Time zones outside an `execute` frame** (S183).
- **The features M17 names:** ModelStore, ExternalFormat, ServiceStore, Relation data; mapping and function test suites.
- **Model data for BINARY, VARBINARY, OTHER, DISTINCT, ARRAY and OBJECT columns.**
- **A statement over both the catalog and user tables.**
- **P5** (judge parity on H2), **P6** (no fallbacks, honest appeals), and the corpus and stress burn-down.
- **PR #81**, the timeseries line, in its own session.

## 10. Where things are

| what | where |
|---|---|
| the program's branch | `claude/total-wall` |
| the approved design and plan | `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` |
| briefs and measurements | `docs/data-delivery-homework/` (probes in `probes/`, each with its output) |
| the census | `core/src/test/java/com/legend/DataMechanismCensusTest.java` |
| the data-delivery matrix | the `DataDeliveryMatrix*Test` classes (in process; Postgres on its lane) |
| sent-statement guard | `SentStatementsAreThePlansTest` |
| the register | `docs/SEMANTICS_REGISTER.md` (next row S191) |
| the gates | `docs/GATES.md`, `gates/BUILD.bazel` (`//gates:local`, `corpus_duckdb`, `corpus_h2`, `stress`, `pct_duckdb`, `pct_h2`, `pct_postgres`, `pct_channel_b`, …) |
| Bazel in a cloud session | `docs/CLOUD_SESSIONS.md` |
| earlier program docs | `docs/PLAN_AS_DATA_2026_11_07.md`, `docs/DATA_IN_THE_PLAN_2026_11_09.md`, `docs/TENET_CHARTER.md`, `docs/JAVA_EVICTION_PLAN.md` |
| D's inventory, E's rulings, the next program | `docs/COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md`, `docs/LEAN_PRODUCT_P3_2026_11_10.md`, `docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md` |
