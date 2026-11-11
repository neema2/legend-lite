# P3, the lean product part 1: every site prepared for one sitting (2026-11-10)

> **Status: RESEARCH, nothing ruled.** This file prepares phase P3 of `docs/GOALS_OF_RECORD_2026_11_07.md` §4 ("no Java
> value computation on the product path", G6 and G3) so that the user can rule on every site at once. It changes no
> code. Each site gives where it is, what Java computes, who reaches it, how often it fires, upstream's answer read
> from the pinned sources, the exits that apply (the database does it, the edge, or a register row), a recommendation
> and the question for the user. Every divergence from upstream below is a DRAFT row for the user to rule (§10),
> never a decision.
>
> Branch `claude/p3-lean-product`, made from `origin/claude/total-wall` at `401b5fad`. Line numbers are that
> commit's. The probes and scripts are in `docs/lean-product-homework/` (§11).

## 0. The answer in one table

| # | site | fires on the lanes measured (§1) | upstream (read, §2) | recommended exit | question for the user |
|---|---|---|---|---|---|
| 1 | a REAL re-parsed through its shortest text | 0 on the corpus and PCT (§1.2); 1 unit test in core | the TDS JSON writes `Float.toString` (same digits as lite); the graph-fetch read widens on DuckDB/H2 and not on Postgres | **edge**: decode carries the driver's double; the writers spell a REAL slot's cell from its float | Q1 |
| 2 | BigQuery's scale padding | 0 here (no BigQuery lane in this container); 12 PCT Decimal tests on BigQuery (GATES, 2026-10-27) | no padding anywhere; the driver's `BigDecimal.toString`; upstream runs no BigQuery PCT | **database** (BigQuery writes the decimal at its declared scale as text), else **delete** (upstream-exact) | Q2 |
| 3 | the TIMESTAMPTZ shift to UTC | 0 everywhere measured | depends on the driver: UTC on DuckDB and Postgres, the original offset on H2 | **database** where the plan knows the column (lowering already does); for a raw grid, a **row** | Q3 |
| 4 | the BC-year repair | 0 repaired on the lanes; 1 PCT test on Databricks (GATES); 1 unit test | drops the era and takes the Julian calendar: 44 BC Mar 15 is `0044-03-13` | **database** (the database's own text, a TEMPORAL_TEXT slot) for the two drivers that re-read nothing; **delete** the array-leaf arm; a **row** for being right where upstream is wrong | Q4 |
| 5 | kinds chosen from a cell's text | 0 everywhere measured | never: a String cell is a String, quoted | **delete** (a planned slot, or a refusal by name) | Q5 |
| 6 | the positional read fallback | 0 outside its own unit test | none: upstream reads by label | **delete** (a name that matches nothing is refused) | none (G7 already rules it) |
| 7 | the dynamic-pivot envelope built at run | 30 on PCT (the 10 dynamic `testPivot_*` per lane, DuckDB, H2, SQLite: 144 generated columns); 0 elsewhere | built at run too, from the driver's reported column types | **edge** (it already is, in the engine's writer); one change: the plan marks a generated column, no name test | Q6 |

Plus §8 (the writer guard on `exec`: 65 hits today, and the two sites it would MISS) and §9 (the ledger's text: ten
claims that are false today).

## 1. How the firing was measured

A throwaway counter (`docs/lean-product-homework/p3count.patch.txt`, never committed: a class `P3Count` in `//base`
that counts each arm and prints the totals at JVM exit) was applied to `exec/JdbcDecode`, `result/RowDecode`,
`result/PureV1Writer` and `result/Output`, then the lanes were run one at a time, as `docs/CLOUD_SESSIONS.md` advises.
Each arm counts separately (for example "3.tz.shifted" against "3.tz.utc-already"). The raw counts are
`docs/lean-product-homework/counts-*.txt`.

### 1.1 What ran, and what did not

| lane | result with the patch | note |
|---|---|---|
| `//core:core_tests` (27 targets, about 4,000 tests) | 27 of 27 PASSED | |
| `//spec:corpus_duckdb`, `//spec:corpus_h2` (both passes each) | PASSED | |
| `//pct:pct_duckdb` (five suites + discipline) | 6 of 6 PASSED | |
| `//pct:pct_h2`, `//pct:pct_sqlite` (five suites) | 6 of 6 PASSED | |
| `//pct:pct_channel_b` (five suites) | 5 of 5 PASSED | |
| NOT run: Postgres, MariaDB, MySQL, Trino, ClickHouse, MongoDB, the cloud lanes (BigQuery, Snowflake, Databricks), both stress suites | | no credentials or no time; a cloud lane's count is quoted from `docs/GATES.md` where it exists |

### 1.2 The counts

The counter class registers its exit hook when it is first loaded, at its first hit, so a target with no counts file
reached no counted arm. Totals over every target run (`counts-core.txt`, `counts-corpus.txt`, `counts-pct.txt`):

| arm (site) | core (27 targets) | corpus DuckDB + H2 (4 passes) | PCT DuckDB, H2, SQLite, channel B (17 targets) |
|---|---|---|---|
| 1: a Float re-parsed (any of the six arms) | 1 (`ResultWritersTest`, the host twin's pin) | 0 | 0 |
| 2: BigQuery's padding | 1 (`JdbcDecodeH2Test`, simulated driver) | 0 | 0 |
| 3: an `OffsetDateTime` shifted, or already UTC | 0 | 0 | 0 |
| 4: a BC `Timestamp` or `Date` repaired | 2 (`JdbcDecodeH2Test.aTimestampKeepsItsEra`) | 0 | 0 |
| 4: the same arm with an AD value (nothing repaired) | 6 (`JdbcDecodeH2Test`) | 2 (the DuckDB host pass: array leaves) | 0 |
| 5: a kind from a cell's text (Number, Any, bytes) | 0 | 0 | 0 |
| 6: a host row read by position | 1 (`RowDecodeTest`, its own pin); by name 48 | 0 (no host rows) | 0 |
| 7: an engine-format envelope built at run | 0 | 0 | **30**: the 10 dynamic `testPivot_*` relation tests on each of DuckDB, H2 and SQLite |
| 7: a generated column typed by the name test | 0 | 0 | **144** (48 per lane) |
| (the decode's slotless arm, for scale) | 4 (`JdbcDecodeDuckDbTest`) | 0 | 0 |

So six of the seven sites fire nowhere on the lanes run here, outside their own unit tests: removing them is not
expected to move any of these lanes. Only site 7 fires, on the PCT's own route (the adapter reads a relation through
the engine's format, `docs/result-boundary-homework/README.md` §7). The cloud lanes are where sites 2 and 4 decide
tests (§4, §6).

## 2. Upstream, read from the pinned sources

legend-engine 4.145.0 and legend-pure 5.99.0 (the pins in `release.MODULE.bazel`), cloned at their tags. Paths are
shortened: `rel/` is `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-execution/legend-engine-xt-relationalStore-executionPlan/src/main/java/org/finos/legend/engine/plan/execution/stores/relational/`.

- **The relational JSON row (the `pure/v1` TDS answer).** `rel/serialization/RelationalResultToJsonDefaultSerializer.java:125-134`
  reads each cell with `RelationalResult.getValue` and writes it through `ValueTransformer.transformWrappedRelationalValueForJSON`
  and `ExecutionResultObjectMapperFactory.convertPurePrimitiveToJson`.
  - `rel/result/RelationalResult.java:469-546` (`getValue`): a column whose JDBC type is `TIMESTAMP` (93) is
    `getTimestamp(i, GregorianCalendar(GMT))`, a `DATE` (91) `getDate(i)`, any other `getObject(i)` (`:543`). The type
    is decided by the JDBC type code alone (`rel/result/SQLResultDBColumnsMetaData.java:52-60`): a `TIMESTAMP WITH TIME
    ZONE` (2014) is not a timestamp there.
  - `rel/serialization/ValueTransformer.java:24-49`: a `Timestamp` is `yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnnZ` of
    `toInstant().atOffset(UTC)`; a `java.sql.Date` is `toLocalDate()` as `yyyy-MM-dd`; anything else goes to the column's
    transformer.
  - `legend-engine-core/.../execution/result/serialization/ExecutionResultObjectMapperFactory.java:70-87`: a String
    quoted; null, NaN and the infinities `null`; anything else `value.toString()`.
- **The graph fetch's read** (`RelationalResult.getTransformedValue`, `:548-647`): `REAL`, `FLOAT`, `DOUBLE` are
  `getDouble` (`:588-597`); `TIMESTAMP` is `getTimestamp(GMT)` then `PureDate.fromSQLTimestamp`, which takes
  `Calendar.YEAR`, the year of the ERA (`legend-engine-core/.../dependencies/domain/date/PureDate.java:1446`).
- **legend-pure** reads a SQL timestamp with `DateWithSubsecond.fromSQLTimestamp` = `fromInstant(timestamp.toInstant(), 9)`
  (`legend-pure-core/legend-pure-m4/.../primitive/date/DateWithSubsecond.java:51-54`).
- **A dynamic result's columns** (`rel/result/SQLExecutionResult.java:95-104`): when the node is
  `isResultColumnsDynamic` (a dynamic pivot, `pureToSQLQuery.pure:6355`), the columns are read from the result set's
  metadata at run (`getColumnTypeName`), and `RelationalResult.java:238-257` builds the TDS builder from them with
  `SQLResultColumn.labelTypePair()` (`.../model/result/SQLResultColumn.java:59-90`: VARCHAR String; FLOAT, DOUBLE,
  DECIMAL, NUMERIC, REAL Float; the integers Integer; BIT Boolean; TIMESTAMP DateTime; DATE StrictDate).

**What upstream answers for each value, replayed.** `probes/UpstreamReplayProbe.java.txt` repeats those calls, one for
one, on today's drivers (DuckDB 1.5.6, H2 2.5.252, Postgres 16.15 with pgjdbc 42.7.13). It is a REPLAY of the source,
not the engine runner: where the runner is asked (`docs/result-boundary-homework/README.md` §7's recipe; not done
here, it needs about 600 MB of jars), the runner wins. Its output, `probes/upstream-replay-2026-11-10.txt`:

| value | DuckDB | H2 | Postgres |
|---|---|---|---|
| REAL 0.1, TDS JSON | `0.1` | `0.1` | `0.1` |
| REAL 0.1, graph-fetch read | `0.10000000149011612` | `0.10000000149011612` | `0.1` |
| REAL 1.0E-45 (the smallest subnormal), TDS JSON | `1.4E-45` | `1.4E-45` | `1.4E-45` |
| TIMESTAMPTZ `2024-01-02 03:04:05.123456+05:00`, TDS JSON | `2024-01-01T22:04:05.123456Z` (an `OffsetDateTime`'s `toString`, unquoted) | `2024-01-02T03:04:05.123456+05:00` (the original offset) | `"2024-01-01T22:04:05.123456000+0000"` (pgjdbc reports `timestamptz` as 93) |
| TIMESTAMP 44 BC March 15, TDS JSON | `"0044-03-13T10:00:00.000000000+0000"` | `"0044-03-15T10:00:00.000000000+0000"` | `"0044-03-13T10:00:00.000000000+0000"` |
| DATE 44 BC March 15, TDS JSON | `"0044-03-15"` | `"0044-03-17"` | `"0044-03-15"` |
| TIMESTAMP 1500-01-01 10:00, TDS JSON | `"1500-01-10T10:00:00…"` | `"1500-01-01T10:00:00…"` | `"1500-01-10T10:00:00…"` |
| DATE 1500-01-01, TDS JSON | `"1500-01-01"` | `"1499-12-23"` | `"1500-01-01"` |
| DECIMAL(10,2) 1.50, TDS JSON | `1.50` | `1.50` | `1.50` |

Two notes. First, upstream's answer for a REAL, a zoned timestamp and any date before 1582 depends on the driver. On a
BC date or a pre-Gregorian one it is wrong: the era is dropped, and the day moves by the Julian-Gregorian gap through
`java.sql.Timestamp` and `java.util.Date`. Second, upstream's TDS JSON for DuckDB's zoned cell would be an unquoted
token. That is read from the source and not seen running; if true, it is not valid JSON.

## 3. Site 1: a REAL re-parsed through its shortest text

**Where, and what Java computes.** `exec/JdbcDecode.java:386` (a FLOATING slot), `:404` (a DECIMAL slot), `:439` (a
TEXT slot), `:596` (a slotless Number), `:629` (a slotless Any or class), and the host twin `result/RowDecode.java:302`
(a REAL slot whose host widened it to a double: DuckDB-WASM, Python). The driver hands a four-byte float (`0.1f`, whose
exact value is 0.100000001490116119384765625). Java writes its shortest decimal text (`Float.toString`: `0.1`) and
parses that as a double (`0.1`, a DIFFERENT double from the float's widening, `0.10000000149011612`). That is a value
chosen in Java: the carrier holds a number the database never held.

**Reached by:** both paths. `JdbcDecode` is the one reader for product queries, test bodies and judges' sides alike;
`RowDecode`'s arm is the server's host rows (`server/PureV1Api.java:512`, `:713`).

**Fires:** in core, once (`ResultWritersTest.aHostsTextIsReadAsTheServerReadsTheValue`, the host twin's own pin). Zero
on the corpus lanes and on PCT (DuckDB, H2, SQLite, channel B; §1.2). A REAL slot arises only from a store column declared `REAL`/`FLOAT4` (or
DuckDB's `FLOAT`, which is four bytes): a Pure Float is a double, and no lowering produces a REAL. SQLite has no
four-byte float (its `REAL` is eight bytes, measured: the driver answers `Double`), so the arm never fires there.

**Upstream.** The TDS JSON writes the float's own `toString`, `0.1`, on every driver: lite's text today is upstream's.
The graph fetch reads `getDouble`, which widens on DuckDB and H2 and does not on Postgres (pgjdbc parses its text).
Upstream has no single answer for the VALUE.

**Exits.**
- **(a) The database does it.** Lowering casts a REAL column through the database's text to a double:
  `CAST(CAST(c AS VARCHAR) AS DOUBLE)` (Postgres `CAST(CAST(c AS TEXT) AS DOUBLE PRECISION)`). Measured
  (`probes/SqlExitProbe.java.txt`, `probes/sql-exit-2026-11-10.txt`) on 8 values: equal to lite's Java answer on DuckDB,
  H2 and Postgres for 7. The 8th is the smallest subnormal: DuckDB and Postgres write `1e-45`, the true shortest text,
  and Java's `Float.toString` writes `1.4E-45`, so the database's answer would differ from upstream's TDS JSON there.
  H2 agrees with Java. Cost: two casts on every REAL column a query projects.
- **(b) The edge.** The decode carries the driver's double unchanged, the widening, which is exact: no text, no
  re-parse; the decode is carriage. The writers spell a REAL slot's cell as the float it was: `Float.toString((float) d)`,
  exact, since a double widened from a float narrows back to it. The plan's column already says REAL (its slot), so
  the writer decides nothing from the value.
  - Where the spelling lives is the question. In the engine's writer only (`CellText.engine`), lite's own JSON and CSV
    would print `0.10000000149011612`. In both, the text matches today's everywhere.
  - The judges and Arrow then see the widened double: a judged REAL compares by the database's own value.
  - `RowDecode.java:302` goes too: a host and a server carry the same double without a rule of their own.
  - Dependents: `CellText` (one registered representation operation, `Float.toString` of a narrowed double);
    `WriterCarriageGuardTest`'s register (+1 there, -1 `Double.parseDouble` in `RowDecode`); `ResultWritersTest`'s
    host pin; the JdbcDecode REAL tests.
- **(c) A row** that names the re-parse as it is. Not recommended: it is the G6 violation the phase exists to remove.

**Recommendation: (b), with the spelling in every writer** (`CellText.pure` and `CellText.engine`), so no output
changes. The value the product carries becomes the database's; the text stays upstream's TDS JSON (`0.1`), and only
the edge chooses digits. Prefer upstream's exact text, which (b) gives and (a) does not at the subnormal.

**Q1.** "A REAL column (a store column declared REAL/FLOAT4): (b) the product carries the database's double, and every
writer spells a REAL cell from its float (`0.1`, as upstream's TDS JSON), or (b') only the engine's writer does and
lite's own outputs print the stored binary value (`0.10000000149011612`, a register row), or (a) the database casts it
through its text (`1e-45` where upstream writes `1.4E-45`, a register row)?"

## 4. Site 2: BigQuery's scale padding

**Where, and what Java computes.** `exec/JdbcDecode.java:283-286` with `declaredScale` at `:411-416`, switched on by
`exec/BigQueryConnector.java:52` (`Decoding.dropsDecimalZeros`). BigQuery's driver drops a NUMERIC's trailing zeros
(`3.0` arrives as `3`), so Java reads the scale out of the slot's text (`DECIMAL(p,s)`, an `Integer.parseInt`) and
calls `setScale` to put the zeros back. The value is equal under `compareTo`, but its scale is invented by Java.

**Reached by:** both paths, BigQuery only.

**Fires:** zero on every lane run here; in core, once, a unit test that simulates the driver
(`JdbcDecodeH2Test.aDecimalsDroppedZerosArePaddedToItsSlotsScale`). On BigQuery's own lane it decides 12 PCT Decimal
tests (`docs/GATES.md`, the 2026-10-27 entry, line 14980: they answered `3D` for `3.0D` without it).

**Upstream.** No padding: the engine's execution has no `setScale` on a read; the TDS JSON writes the driver's
`BigDecimal.toString` (`ExecutionResultObjectMapperFactory.java:86`), so upstream answers `3` where lite answers `3.0`.
Upstream 4.145.0 has no BigQuery PCT at all (no `relational-bigquery` manifest in the tree), so there is no upstream
verdict for those 12 tests. Measured on the in-process databases: DuckDB, H2 and Postgres keep the zeros (`1.50`);
SQLite holds a `NUMERIC` of `1.50` as the double `1.5`, but lite's SQLite carries decimals as text.

**Exits.**
- **(a) The database does it.** BigQuery's dialect writes a `DECIMAL(p,s)` output column as text at its scale under a
  `DECIMAL_TEXT` slot, the existing text carrier: `FORMAT('%.<s>f', x)` or `CAST(x AS STRING FORMAT 'FM…0.00')`.
  - Read from BigQuery's documentation, not run: there is no BigQuery in this container.
  - The exactness of `%f` on a 38-digit BIGNUMERIC must be measured on the BigQuery lane before this is chosen.
  - Cost: one function per decimal column on BigQuery alone; no other dialect changes.
- **(b) Delete it:** upstream-exact (the driver's value). BigQuery then answers `3` for `3.0D`, as upstream's reader
  would, and the 12 PCT tests fail on BigQuery by name. They become expected failures pinned by message, as every
  lane pins its own.
- **(c) A row** naming the padding. It stays a Java decision of a value's scale.

**Recommendation: (a)** if the BigQuery lane measures `FORMAT` exact on its NUMERIC and BIGNUMERIC. Otherwise **(b)**,
with the 12 tests pinned. Either way the Java padding goes. (a) keeps lite's answer (Pure's `3.0D`); (b) is upstream's.

**Q2.** "BigQuery's Decimal: (a) BigQuery writes it at its declared scale in SQL (one `FORMAT` per decimal column,
measured on its lane first), or (b) take the driver's value as upstream does and pin the 12 PCT tests as BigQuery's
expected failures?"

## 5. Site 3: the TIMESTAMPTZ shift to UTC

**Where, and what Java computes.** `exec/JdbcDecode.java:325-326`: an `OffsetDateTime` is moved to UTC
(`withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()`). That is a zone conversion in Java.

**Reached by:** both, but only from a column the plan does not type. Lowering never sends a zoned result: a zoned
parse is normalized to naive UTC in SQL (`lowering/Scalars.java:2361-2372`, `timezone('UTC', CAST(… AS TIMESTAMPTZ))`),
DuckDB's `now()` is cast to `TIMESTAMP` (`sql/dialect/DuckDb.java:485`), and the catalog types of Postgres and DuckDB
copy a zoned column as `CAST(timezone('UTC', %s) AS TIMESTAMP)` (`Postgres.java:152`, `DuckDb.java:385`). What remains
is a RAW GRID, a relation the plan declares no columns for, read as the database reports it at run (`PlanRunner.java:95-104`
→ `JdbcStatements.reportedRows` → `WireTypes.java:37`, TIMESTAMP_WITH_TIMEZONE → TIMESTAMPTZ).

**Fires:** zero on every lane measured (core, corpus, PCT; §1.2). Neither "3.tz.shifted" nor "3.tz.utc-already" was
reached.

**Measured, per driver** (`probes/sql-exit-2026-11-10.txt`):
- DuckDB hands an `OffsetDateTime` already in UTC: the Java shift is a no-op.
- H2 hands the original offset (`+05:00`): the shift is real.
- **pgjdbc hands a `java.sql.Timestamp` (it reports `timestamptz` as 93), and then REFUSES the reader's own re-read**
  (`getObject(i, LocalDateTime.class)`: "Cannot convert the column of type TIMESTAMPTZ to requested type
  java.time.LocalDateTime"). A zoned raw-grid column on Postgres fails today, by the driver's message. A latent
  defect, found here; not reached by any lane.

**Upstream.** By the driver (§2's table): UTC on DuckDB and Postgres, the original offset on H2.

**Exits.**
- **(a) The database does it**, for every column the plan types: already true. The equivalent SQL, measured equal to
  the Java answer on each database: DuckDB `CAST(timezone('UTC', x) AS TIMESTAMP)`, H2 `CAST(x AT TIME ZONE 'UTC' AS
  TIMESTAMP)`, Postgres `(x AT TIME ZONE 'UTC')`. A raw grid's columns are unknown at planning, so the planner cannot
  wrap them.
- **(b) The edge:** a raw grid is corpus-authored SQL in most of its uses (`RawSqlBoundary`, an edge item in §2 of the
  goals). If every raw grid is the edge's, the shift goes with it. The product's own `executeInDb` raw relation would
  then need the plan to type its columns, a planning change outside P3.
- **(c) Delete the arm, and refuse a zoned raw-grid cell by name** ("a raw grid's column '…' is TIMESTAMP WITH TIME
  ZONE: cast it to TIMESTAMP in the SQL"). Measured unreached on every lane. It also replaces Postgres's opaque driver
  error with lite's name.
- **(d) A row** keeping the shift: lite answers the UTC wall clock on every driver; upstream's answer depends on the
  driver.

**Recommendation: (c).** Delete the arm, refuse by name, and fix the Postgres re-read so that it too refuses by name.
The shift is the database's wherever the plan knows the type, and a raw grid's SQL is its author's to cast. If the user
prefers to keep raw grids lenient, (d) with its row.

**Q3.** "A raw grid's zoned column (SQL whose columns the database reports at run): (c) refused by name, its author
casting it, or (d) shifted to UTC in Java as today, a row (upstream: by the driver)?"

## 6. Site 4: the BC-year repair

**Where, and what Java computes.** `exec/JdbcDecode.java:324` (the `Timestamp` arm) → `wallClock` `:341-344`, and
`day` `:347-350`, both through `beforeChrist` `:352-356`. A `java.sql.Timestamp`'s `toLocalDateTime()` gives the year
of the era, so Java asks the calendar's ERA and computes `1 - year`: calendar arithmetic in Java.

**Reached by:** both, in two cases:
1. on Snowflake and Databricks at top level: their connectors re-read nothing as java.time
   (`SnowflakeConnector.java:50`, `DatabricksConnector.java:51`);
2. on every database for a STRUCTURE's leaf, an array element, which is never re-read (`reread` handles only a top
   cell; `array` decodes each element directly).

Everywhere else the cell is re-read as `LocalDateTime`/`LocalDate` (`reread`, `:206-221`), which is BC-faithful by the driver:
no repair, measured right on DuckDB, H2 and Postgres.

**Fires:** the repair proper (a BC value), zero on every lane run here; twice in core, a Timestamp and a Date (`JdbcDecodeH2Test.aTimestampKeepsItsEra`).
The arm with an AD value: 2 in the DuckDB corpus host pass (array leaves; nothing repaired) and 6 in core. On the cloud
lanes: Databricks' `testAdjustByMinutesBigNumber` (GATES 2026-10-27, line 14983). Snowflake's driver hands another
instant altogether, which the repair cannot fix (GATES, same entry).

**Measured** (`probes/sql-exit-2026-11-10.txt`). The repair is only as right as the driver's `Timestamp`:
- DuckDB and Postgres build an array element's `Timestamp` from its fields, so the repair gives the true day.
- **H2 builds it from the instant in the Julian calendar**: 44 BC March 15 arrives as a BC `Timestamp` whose fields
  read March 17, and the repair would answer `-0043-03-17`, two days wrong. Lite's H2 carries lists as JSON text, so
  this is not reached today. It shows the arm cannot be made right in Java across drivers.

**Upstream.** Wrong both ways (§2): the TDS JSON drops the era and takes the Julian day (`0044-03-13` on DuckDB and
Postgres); the graph fetch takes the year of the era. Lite is right where upstream is wrong.

**Exits.**
- **(a) The database does it.** Where the driver re-reads nothing (Snowflake, Databricks), the dialect writes the
  date-time as its own text under the existing `TEMPORAL_TEXT` slot, and lite parses the text. That is the reading
  `docs/GATES.md` already records for Snowflake's far-BC cells.
  - Each database writes its own BC spelling: DuckDB `0044-03-15 (BC) 10:00:00`, H2 `-0043-03-15 10:00:00`, Postgres
    `0044-03-15 10:00:00 BC`. The dialect needs a format that writes the astronomical year, or a spelling the parse reads.
  - For an array leaf: the array is written as text (the list-as-JSON carrier that H2 already uses), or its elements
    re-read through the driver's typed array getter where one exists.
- **(b) The edge:** none applies; the year is a value, not a presentation.
- **(c) A row** for the divergence itself, whichever exit is chosen. Lite answers the database's calendar date, with
  its era; upstream drops the era and moves pre-1582 dates to the Julian calendar.

**Recommendation: (a)** for Snowflake and Databricks (measured on their lanes first). Delete the array-leaf arm, with
an array of date-times carried as the database's text, or refused by name where the driver hands `Timestamp` leaves.
And **(c)**: the row that lite does not copy upstream's calendar defect. Do not copy upstream here: its answer is the
Julian shift, which no user wants.

**Q4.** "BC and pre-1582 date-times: is it ruled that lite answers the database's own calendar date (a row: upstream
drops the era and shifts to the Julian calendar), and that the Java repair is replaced by the database's text on
Snowflake and Databricks and for array elements?"

## 7. Sites 5, 6 and 7

### 7.1 Site 5: kinds chosen from a cell's text

**Where, and what Java computes.**
- `exec/JdbcDecode.java:597-598`: a slotless Number column whose driver hands a String becomes a `BigDecimal` if the
  text contains `.`, `e` or `E`, else an integer.
- `:619`: a slotless Any, class or enum column whose String starts with `{` or `[` is parsed as JSON, else kept as a
  String.
- `:621`: the same for bytes.

Each is a type decided from a value (charter Clause 2, G6). The twin on the test path is `testexec/AssertRuns.java:238`
(`t.startsWith("{") || t.startsWith("[")`), outside P3's product scope but the same smell.

**Reached by:** both; only for a column with no slot (a late-bound grid's column, a pivot's generated column), which
the slot-first decode leaves to the Pure type.

**Fires:** zero on every lane measured, every arm ("5.number-text.*", "5.any-text.*", "5.any-bytes.*").

**Upstream.** Never: a String cell is written as a quoted String (`convertPurePrimitiveToJson:72-75`), and a Variant is
recognized by the column's reported type name (JSON, MAP, VARIANT: `SQLResultDBColumnsMetaData.java:74-76`), never by
the text's first character.

**Exits.** (a) the planner gives each such column a slot: a Number's is not knowable before the value, so the column is
`DECIMAL` (exact), or the cell is refused by name; a structure's slot is JSON or STRUCT, known from the Pure type.
(b) the edge: none. (c) a row: not recommended, since upstream never does it.

**Recommendation: delete the three arms.** A slotless Number cell that arrives as text is refused by name; a slotless
Any/class text is the String it is (upstream's answer) or, where the Pure type is a class, the planner names the JSON
slot. Measured unreached, so nothing is expected to move. The lanes prove it when the deletion lands.

**Q5.** "Delete the kinds-from-text arms: a slotless Number given as text refused by name, an Any column's text a
String (upstream's answer)?"

### 7.2 Site 6: the positional read fallback

**Where, and what Java computes.** `result/RowDecode.java:51-60`: when NO member of a host's row object is named after
any plan column, and the counts agree, the cells are read by position. A name mismatch is guessed past.

**Reached by:** the server's host rows (`HostRows.stream`, `server/PureV1Api.java:512`, `:713`: DuckDB-WASM's and
Python's answers to a plan) and tests. A JDBC row never reaches it (`JdbcDecode` reads by index, the plan's order).

**Fires:** the positional branch once, in its own unit test (`RowDecodeTest.columnsAreReadByName_orByPositionWhenNoNameMatches`);
the by-name branch 48 times (core's server and root tests). Never on any lane.

**Upstream.** None: a TDS row is read by its label (`getColumnLabel`, `RelationalResult.java:174`, `:262`).

**Exit.** Delete. A host names each member after the SQL's alias, which is the plan's column name. A row whose names
match no column is refused with the existing message ("the row's object has no field for the plan's column …").
G7 names this exact fallback ("a positional read when a name is missing"), so it is already ruled. Dependents: the
`RowDecodeTest` case and its `{"_v":2}` sibling (`RowDecodeTest.java:130`) become refusals.

**No question.** One check before deleting: that no host (the engine-client, Python) renames a value root's column.
The engine-client hands the database's own keys (`engine-client/src/duckdb.ts:241-252`).

### 7.3 Site 7: the dynamic-pivot envelope built at run

**Where, and what Java computes.**
- `result/Output.java:57-58` (JSON) and `:73-74` (Arrow): when the plan states no columns (a dynamic pivot's, known only
  once its keys are read), the engine-format envelope is made from the columns the run announces.
- `result/PureV1Writer.java:72-106`, with `:80-87` and `primitive` at `:135-142`: each column's builder entry comes from
  the compiler's types read before the run (`PureV1Writer.Facts`, made by `server/PureV1Api.java:541-549`).
- A column the compiler did not type is recognized as a pivot's generated column by its NAME containing the pivot
  separator, and typed by its plan type through the primitive table.

Since 2026-11-09 the facts are data read before the run, no callback into the compiler. What remains at run is a
lookup by name and one name test.

**Reached by:** the engine's `pure/v1` and Arrow answers on the product path, and the PCT adapter, which reads through
that same writer. That is the edge already: it exists to match legend-engine's TDS builder (goals §2).

**Fires:** on PCT, 30 times: the 10 dynamic `testPivot_*` tests of the relation suite on each of DuckDB, H2 and SQLite,
whose adapter reads a relation through the engine's format (`PctPlanRoute.execute`). Their 144 generated columns
(48 per lane) are all typed through the name test. Zero in core and the corpus. The one other engine-format answer the
lanes produce is planned ("7.envelope-planned.arrow", once, `PureV1ApiTest.e8_executeArrow…`). So a change here is
proven by those 10 tests per lane.

**Upstream.** Built at run too, from the driver's reported column type names (`SQLExecutionResult.java:95-104`,
`RelationalResult.java:248-257`, `SQLResultColumn.labelTypePair`). Lite types a generated column by the plan's type
instead, which can differ: upstream's `labelTypePair` makes a DECIMAL column `Float` and an unknown type `String`.

**Exits.** (b) the edge: it already is in the engine's writer. It moves with P2's edge package and changes nothing in
the product. (a) the planner states the generated columns' template (the aggregate's type, the separator) as data on
the keyed node. The writer then reads "this column is generated" from the plan, never from a name. A user column whose
name contains the separator can no longer be misread. (c) a row: lite's builder types a generated column by its plan
type where upstream reads the database's report.

**Recommendation: (b) plus (a)'s small change**: P2 moves the envelope to the edge, and the plan marks generated
columns, so the name test goes. And the row (c), since the builder's type can differ from upstream's.

**Q6.** "The dynamic pivot's engine-format envelope: keep it at the edge, typed by the plan's type (a row: upstream
types a generated column from the driver's report, a DECIMAL as Float), with the generated columns marked by the
plan instead of by their name?"

## 8. The writer guard extended to `exec`, today

`WriterCarriageGuardTest`'s scan (its `VOCABULARY` and `stripped`, copied verbatim into
`docs/lean-product-homework/writer_guard_on_exec.py`) over `core/src/main/java/com/legend/exec` and
`core/src/main/mongodb/com/legend/exec`: **65 hits in 7 of 38 files**.

| file | hits | spellings (count) | what they are |
|---|---|---|---|
| `JdbcDecode.java` | 30 | `BigDecimal.valueOf` 4, `new BigDecimal` 4, `Double.parseDouble` 3, `bitLength` 3, `longValue` 3, `doubleValue` 2, `getYear` 2, `scale` 2, `Double.valueOf` 1, `Integer.parseInt` 1, `intValue` 1, `new BigInteger` 1, `setScale` 1, `stripTrailingZeros` 1, `toBigIntegerExact` 1 | sites 1 (the 3 `Double.parseDouble`, a `new BigDecimal`), 2 (`setScale`, `scale`, `Integer.parseInt`), 4 (`getYear` 2); the rest carrier reads like `RowDecode`'s registered ones (a long from a BigInteger, a decimal from a long) |
| `MongoDbDecode.java` | 21 | `longValue` 4, `BigDecimal.valueOf` 4, `scale` 2, `stripTrailingZeros` 2, `toBigIntegerExact` 2, `negate` 1, … | MongoDB's decimal and integer carriers; `negate` (`:204`) is worth a look |
| `PlanParameters.java` | 8 | `getYear` 4, `setScale` 1, `scale` 1, `Double.isFinite` 1, `Integer.valueOf` 1 | a parameter's checks: the year range refused (`:191`, `:197`), a decimal's plain digits (`:203`), a non-finite Float refused (`:180`) |
| `PlanRunner.java` | 2 | `getNano` 2 | `dateTimeText` (`:1002`): a date-time parameter's fraction cut to the target's digits (row S181, pending) |
| `RaisedErrors.java` | 2 | `Integer.parseInt` 2 | an error position read from the message |
| `WireTypes.java` | 1 | `Math.` 1 | `Math.max(scale, 0)` for a reported decimal |
| `MongoDbStatements.java` | 1 | `getNano` 1 | |

**What the guard would MISS** if extended unchanged:
- site 3 (`withOffsetSameInstant`);
- site 4's arithmetic (`withYear(1 - …)`, `Calendar.get(ERA)`);
- site 5 (`startsWith("{")`, `contains(".")`);
- site 1's `Float.toString` (only its `Double.parseDouble` is caught).

The vocabulary was written for writers, which receive typed values. For a decoder it needs: `withOffsetSameInstant`,
`withYear`, `withNano`, `atZone`, `Calendar.`, and a text test on a cell (`startsWith`, `contains`, `indexOf` over a
value's text). An honest extension adds those, then registers every remaining hit by name with its reason, as the guard
does for `result/` (G9: "a guard must scan where the code actually is").

**After P3's recommended exits**, `JdbcDecode`'s register would hold only carrier reads: about 22 of its 30 hits. Sites
1, 2 and 4 take 8 with them.

## 9. What the ledger's text gets wrong (`JavaEvalLedgerTest.java`)

`docs/lean-product-homework/ledger_names.py` lists every class-like name the file cites and checks it against the tree:
298 names, 79 not declared anywhere (many are JDK or upstream classes, which is fine). The claims that are false today:

1. **`EXEC_CLASSES`' entry for `JdbcDecode` (lines 1203-1208) says "nothing computed, nothing compared".** False: sites
   1, 2, 3 and 4 compute (a re-parsed double, a padded scale, a zone shift, a year's arithmetic), and site 5 decides a
   kind from text. This is the G9 defect the goals name: the decoder is registered by NAME only (a closed class list),
   with no count of what it does.
2. **The header's PERMANENT-ALLOWED (lines 49-55) names "the egress decode cluster
   (`Executor.fetch/unwrap/latticeKind/decodeAny`)".** `Executor.java` was deleted on 2026-10-27 (the ledger itself
   says so at line 1214). The cluster is `JdbcDecode` and `RowDecode` today.
3. **The header's METAMODEL CHANNEL (lines 35-48) names `MetamodelWalk`, `MetamodelSteps` and `GridReads`** as the
   channel's members. All are gone (`GridReads` "DIED with Phase 1", the same paragraph says); the walk was deleted on
   2026-11-06.
4. **`EXEC_CLASSES`' comment for `PlanRunner` (lines 1180-1187): "writing a value's type into a plan's type hole".**
   The type hole was deleted on 2026-11-09 (P4, D1; `4793298b`).
5. **The `WireTypes` comment (lines 1290-1295) lists `ROOT_CLASSES: ColumnProbe, GridProbe, DynamicPivot, and
   WireProbe`** as live. All four were deleted (ColumnProbe, WireProbe, GridProbe and DynamicPivot on 2026-10-31,
   S3a-S3c; the ledger's own lines 1633-1637 say so).
6. **The same comment lists `InstanceIds`, `SqlTypeCensus`, `PrimitiveCensus` in `TESTEXEC_CLASSES`**: none of the
   three exists.
7. **`EVICT_SIZE`'s history (line 80) and E4's row (line 1079) name `StatementExecutor`, `DynamicPivot`, `JsonCompare`,
   `StoreNav`** as the measured files. Gone, and their rows' text reads as live.
8. **The SIZE header's family `HostEval` (line 200)** and the assertion message at line 2019 ("HostEval.java, 2026-08-19
   audit"): `HostEval` was evicted on 2026-08-19, as the header itself says.
9. **The tenet's claim (lines 22-33): "the egress boundary is a small set of irreducible carriage sites plus the NAMED,
   SHRINKING residue registered below".** The decode's computation (item 1) is neither carriage nor registered: it is
   the unnamed residue that P3 exists to remove.
10. **Line 1061: "PLAN-RENDERED (`Compiler.executeWire` → `WireRender` → …)"** and `ResultJson` (line 1063),
    `CsvSerializer`/`JsonSerializer` (line 1066): none exists; the writers are `result/`'s.

Most of these are history written as present tense in comments, and the test still passes, because a comment is not
checked. The correction P3 owes (goals §4, "correct the ledger's text"):
- rewrite the header's three paragraphs to today's names (the decoders `JdbcDecode` and `RowDecode`; the deleted
  channel named as history);
- replace `JdbcDecode`'s "nothing computed" with what remains after P3's exits;
- move the dead names' comments under a dated "history" line;
- and, the mechanical part, give the decoder the extended writer guard's register (§8), so the claim is checked and
  not written.

## 10. Draft register rows (for the user to rule; ids provisional)

The plan-as-data block S171-S199 is in use up to S185 on `claude/total-wall`, whose worker may take the next numbers.
These rows carry provisional ids `P3-R1`… and are numbered when they land.

| id | kind | legend-pure / legend-engine | lite (proposed) | why | evidence | owner |
|---|---|---|---|---|---|---|
| P3-R1 (only under Q1 (b') or (a)) | outcome (output text) | the TDS JSON writes a REAL column's cell as the driver's `Float`'s `toString` (`0.1`); the graph fetch reads `getDouble` (widened on DuckDB and H2, not on Postgres) | (b') lite's own JSON and CSV print the database's double (`0.10000000149011612`), the engine's writer `0.1`; or (a) the database's shortest text read as a double (`1e-45` where upstream writes `1.4E-45`) | G6: the decode carries the database's value | `probes/upstream-replay-2026-11-10.txt`; `RelationalResult.java:543`, `:588-597`; `ExecutionResultObjectMapperFactory.java:86` | P3 |
| P3-R2 (only under Q2 (a)) | outcome | the driver's `BigDecimal` as it comes (BigQuery's drops trailing zeros: `3`) | BigQuery writes a `DECIMAL(p,s)` at its declared scale in SQL (`3.0`), Pure's literal | the declared scale is the type's; the database writes it | `JdbcDecode.java:283-286` (to be deleted); GATES line 14980 | P3 |
| P3-R3 (only under Q3 (d)) | outcome | a zoned cell by the driver: UTC on DuckDB and Postgres, its own offset on H2 (and DuckDB's TDS JSON token unquoted, read) | the UTC wall clock on every database | one answer per value | `probes/upstream-replay-2026-11-10.txt`; `SQLResultDBColumnsMetaData.java:52-60` | P3 |
| P3-R4 | outcome (engine defect, not copied) | a BC or pre-1582 date-time loses its era and moves to the Julian calendar (`0044-03-13` for 44 BC March 15 on DuckDB and Postgres; `1500-01-10` for 1500-01-01); a DATE likewise on H2 (`1499-12-23`) | the database's own calendar date, era kept (`-0043-03-15`) | upstream's answer depends on `java.util.Date`'s hybrid calendar, a defect | `ValueTransformer.java:37`, `PureDate.java:1446`, `DateWithSubsecond.java:53`; `probes/upstream-replay-2026-11-10.txt` | P3 |
| P3-R5 | outcome (typing) | a dynamic pivot's TDS builder types each generated column from the driver's reported type name (`labelTypePair`: DECIMAL and NUMERIC `Float`, an unknown `String`) | from the plan's type of the pivot's aggregate (a Decimal sum `Decimal`) | the type is the compiler's, known at planning | `SQLResultColumn.java:59-90`, `RelationalResult.java:248-257`; `PureV1Writer.java:80-87` | P2 (the edge) |

Sites 5 and 6 need no row: their exits are upstream's own answer and a refusal.

## 11. The homework (`docs/lean-product-homework/`)

| file | what |
|---|---|
| `probes/SqlExitProbe.java.txt` | each site's driver object today beside the SQL exit's answer, on DuckDB, H2, SQLite, Postgres; the header says how to run it |
| `probes/sql-exit-2026-11-10.txt` | its output (DuckDB 1.5.6, H2 2.5.252, SQLite 3.47.1, Postgres 16.15 / pgjdbc 42.7.13) |
| `probes/UpstreamReplayProbe.java.txt` | legend-engine's JSON-row and graph-fetch reads replayed call for call; NOT the engine runner |
| `probes/upstream-replay-2026-11-10.txt` | its output |
| `p3count.patch.txt` | the throwaway counter (never committed as code; applied, measured, reverted) |
| `collect_counts.py` | sums the counter's lines over a lane's test outputs |
| `counts-*.txt` | the counts per lane |
| `writer_guard_on_exec.py` | the writer guard's scan over any directory |
| `writer-guard-on-exec-2026-11-10.txt` | its output over `exec` |
| `ledger_names.py`, `ledger-names-2026-11-10.txt` | the ledger's cited names against the tree |

## 12. The questions, in one list

- **Q1 (site 1).** A REAL column: (b) the product carries the database's double and every writer spells a REAL cell
  from its float (`0.1`, upstream's TDS JSON); or (b') only the engine's writer does, and lite's own outputs print
  `0.10000000149011612` (row P3-R1); or (a) the database casts it through its text (`1e-45` where upstream writes
  `1.4E-45`, row P3-R1)?
- **Q2 (site 2).** BigQuery's Decimal: (a) written at its declared scale by BigQuery in SQL, measured on its lane first
  (row P3-R2); or (b) the driver's value as upstream takes it, the 12 PCT tests pinned as expected failures?
- **Q3 (site 3).** A raw grid's zoned column: (c) refused by name, the author casting it in SQL; or (d) shifted to UTC
  in Java as today (row P3-R3)?
- **Q4 (site 4).** BC and pre-1582 date-times: rule row P3-R4 (lite answers the database's calendar date; upstream's
  Julian shift not copied), and replace the Java repair with the database's text on Snowflake and Databricks and for
  array elements?
- **Q5 (site 5).** Delete the kinds-from-text arms: a slotless Number given as text refused by name, an Any column's
  text the String it is?
- **Q6 (site 7).** The dynamic pivot's engine envelope: at the edge, typed by the plan (row P3-R5), with the generated
  columns marked by the plan instead of by a name test?
- (Site 6 needs no question: G7 already rules the positional fallback out. Delete it.)
- **Q7 (§8).** Extend the writer guard to `exec` with the decoder's vocabulary added (zone and calendar calls, text
  tests on a cell), registering each remaining hit by name?
