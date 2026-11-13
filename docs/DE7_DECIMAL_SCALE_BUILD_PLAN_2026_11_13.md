# DE7: a decimal's scale is the database's, in every output — the build plan if ruled (a) (2026-11-13)

**A plan, not a build.** The orchestrator asked for it on 2026-11-13, while DE7 is with the user. The design and its
measurements are docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §5.10. The rule, as upstream answers it (legend-engine
4.145.0, read through the engine runner): every output carries the scale the DATABASE gave the column, from the JDBC
result metadata, never lite's slot and never the Pure type. Author: E.

## 1. Where the scale comes from (exec, at run)

- `JdbcStatements.rows` already reads `ResultSetMetaData` before the first row. It reads the column count to check the
  plan's columns.
- It will read each decimal column's `getPrecision` and `getScale` there too. Those become the ANSWERED scale: a
  run-time fact handed to the writer with the columns. Nothing new enters the plan.
- The writer still starts at the first row (`PlanRunner.Streamed`, `Batcher.start`), so the sink is never told columns
  it would have to take back.
- An empty answer starts at the end with the same metadata.
- **No stated scale** is a fact too. The metadata answers precision 0, scale 0, or a DECFLOAT. Each such column is
  marked "no scale" and handled per §3. It is never defaulted (invariant 4).

## 2. Each output, and what changes in it

| output | today | after |
|---|---|---|
| **decode** (`JdbcDecode`, DECIMAL arm) | the cell as the driver gave it; BigQuery pads to the slot's scale (`dropsDecimalZeros`) | every database pads to the answered scale, never rounding; a cell with more nonzero places is refused by name. BigQuery's flag is deleted: the metadata gives BIGNUMERIC's scale |
| **JSON rows** (`JsonRowsWriter`) | the BigDecimal as decoded (H2 `2.5`, DuckDB `2.50`) | the cell at the answered scale: `2.50` on both, as upstream's DEFAULT writes it |
| **CSV** (`CsvWriter`) | as JSON | as JSON (upstream's CSV writes the same digits) |
| **Arrow** (`ArrowWriter`) | `decimal128(p, s)` from the dialect SLOT, `DECIMAL(38,18)` for a bare Decimal: `2.500000000000000000` | `decimal128` at the ANSWERED precision and scale (upstream: `decimal128(3,2)`, `decimal128(38,16)`) |
| **pure/v1 envelope** (`PureV1Writer`) | the builder's types from the plan; cells as JSON | cells as JSON above; the builder unchanged (`relationalType` stays upstream's own label) |
| **TDS objects** (`TdsObjectsWriter`) | as JSON | as JSON |
| **host judge** (`testexec/Equality`) | Decimals compare by kind and value; scale per S127/S132 | unchanged rule. Its inputs are now at the answered scale, so `2.5` vs `2.50` no longer depends on the database |
| **database judge** (`VerdictSql`'s canon) | a decimal's text as the database casts it | unchanged: the canon is SQL and already the database's own text. A verdict that moved would be a finding |

## 3. Where Arrow has no stated scale (measured in §5.10, and to measure on the lanes)

| database | lite's statement today | metadata | the answer |
|---|---|---|---|
| DuckDB | `? AS p`, `ID * ?` | DECIMAL(3,2), DECIMAL(13,2) | `decimal128` at it |
| H2 | `CAST(? AS DECFLOAT)` for a bare Decimal parameter | DECFLOAT, precision 100000, scale 0: **no scale** | Arrow writes the column as TEXT (`DECIMAL_TEXT`, the cell's own digits, exact). JSON and CSV write the cell as the database gave it |
| SQLite | `printf('%.18f', ...)` text: lite's own carrier | TEXT: **no scale** | as H2. The 18 places are lite's; the carrier change is a separate item |
| Postgres | NUMERIC with no typmod | not measured (to watch) | measured before the build: a stated scale goes to `decimal128`; scale 0 with precision 0 is **no scale**, so text |
| Snowflake, Databricks, BigQuery, Trino, MariaDB/MySQL, ClickHouse | — | not measured | each lane measures metadata against cells on a fixed set before its decode changes |

The no-scale choice ((ii) of §5.10) is lite's own: upstream refuses the parameter form at planning. It is a register
row in the batch with DE7, worded "a bare Decimal the database answers without a scale: Arrow carries its digits as
text; JSON and CSV write the database's cell".

## 4. The tests that would pin it

- `DecimalScaleOutputsTest` (new, core). For the §5.10 query, its literal and `let` forms, and a stated `Decimal(10,2)`
  store column, each in-process database answers:
  - JSON, CSV and Arrow carry the same digits;
  - Arrow's type is the metadata's;
  - a no-scale column is text in Arrow.
  The expected answers are upstream's where upstream answers (§5.10's table), and the register row's where it refuses.
- `JdbcDecodeH2Test`: the pad-to-answered-scale rule. Fewer places are padded, never rounded; more nonzero places are
  refused by name. It replaces `aDecimalsDroppedZerosArePaddedToItsSlotsScale`.
- `ArrowWriterTest`: a column with an answered scale versus one with no scale.
- `ConnectorsTest.eachConnectorAnswersItsDriversDecodeFacts`: BigQuery's `dropsDecimalZeros` deleted.
- The IQ parity row that (a2) moved, `lite/m1d/cast_integer_column_to_numeric`, must NOT move. Postgres answers scale 0
  there, and the answered scale keeps it so. This is the reason the Pure type was rejected.

## 5. The lanes that must re-run

- Local:
  - core (all non-manual targets);
  - `//core:answer_snapshot` before and after, through `compare_answers.py.txt`. Every moved answer is expected to be a
    decimal's trailing zeros, and each is listed;
  - PCT channel A and channel B on DuckDB, H2 and SQLite;
  - Postgres unprivileged, with `iq_parity_postgres_test`;
  - `//:generated`;
  - `//gates:local`.
- External, a lane round on the commit: BigQuery (its 12 decimal tests, and the flag's deletion), Snowflake,
  Databricks, Trino, MariaDB/MySQL and ClickHouse. Each lane runs the metadata probe of §3 first.
- Engine-text (compatibility) outputs: their text must not move. A moved spelling is a finding, not absorbed.

## 6. Estimate

About 2 days: exec's metadata read, the decode, four writers, the tests and the local lanes. Then about 0.5 day a lane
to measure and confirm. The register row for §3's no-scale case goes with the build.
