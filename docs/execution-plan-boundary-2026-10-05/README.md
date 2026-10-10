# Evidence: the execution plan boundary

The measurements `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` cites. Nothing here is built or read by a test.

## `plans/` — real plans from legend-engine 4.145.0 (2026-10-05)

`gen.py` posts `model.pure` and each query of `queries.tsv` to a legend-engine 4.145.0 server on
`127.0.0.1:6300` (the release's own server, started from its distribution) and writes each plan to `out/<name>.json`;
`out/_types.json` counts the node kinds per plan. `fm/texts.json` is the template census of §6: every FreeMarker
template text in legend-engine's 22 fixture plans plus the 13 plans in `out/`.

## `probes/` — what the databases do with a bound value (2026-10-07)

Each probe is one Java file run straight from source with one JDBC driver on the class path, at the versions the
product pins (`tools/deps/jars_table.bzl`): DuckDB JDBC 1.4.4.0, H2 2.1.214, Postgres JDBC 42.7.13. Postgres is the
pinned Postgres 16 (`@embedded_postgres`), started with `initdb -U postgres -A trust` and `pg_ctl ... -p 55432`.

```
java -cp duckdb_jdbc-1.4.4.0.jar BindProbe.java jdbc:duckdb:
java -cp h2-2.1.214.jar          BindProbe.java jdbc:h2:mem:p
java -cp postgresql-42.7.13.jar  BindProbe.java jdbc:postgresql://localhost:55432/postgres postgres ""
java -cp <driver>                EnumIndex.java <url> <user> <password>
java -cp <driver>                TypingProbe.java <url> <user> <password>
java -cp <driver>                LiteralProbe.java <url> <user> <password>
java -cp <driver>                ListProbe.java <url> <user> <password>
```

- `BindProbe.java` → `bind-results.txt`: a value passed to the database as a value, not pasted into the SQL — a
  string with a quote, a list as one array (`ID = ANY(?)`), an empty list, a list of strings, a null. All three
  databases answer every case through `createArrayOf`; DuckDB refuses a bare Java array (`setObject(Integer[])`).
- `EnumIndex.java` → `enum-index-results.txt`: five ways to compare an enum parameter with a column that stores
  codes (400,000 rows, an index on the column; `ACTIVE` stored as `'A'` or `'X'`, `CLOSED` as `'C'`), read for
  whether the database uses the index. DuckDB reports its real scan only under `EXPLAIN ANALYZE` (plain `EXPLAIN`
  shows a sequential scan even for a key lookup), so the probe uses it there. H2's times are not comparable: H2
  reuses the result of a repeated identical query, so its index column is the measurement.

| Form | DuckDB | H2 | Postgres | a two-code value |
|---|---|---|---|---|
| F0 the runner translates (control; upstream's way) | index | index | index | right |
| F1 a value table: `STATUS IN (SELECT code FROM (VALUES …) m(code, name) WHERE name = ?)` | index | index | index | right |
| F2 `STATUS = (CASE ? WHEN … THEN 'A' … END)` | index | index | index | **wrong** (one code) |
| F3 `STATUS = ANY(CASE ? WHEN … THEN ARRAY[…] … END)` | index | **fails** | index | right |
| F4 the column decoded: `(CASE STATUS WHEN 'A' THEN 'ACTIVE' … END) = ?` | **scan** | **scan** | **scan** (~24 ms vs ~0.4 ms) | right |

F1 is the one form that uses the index on all three and is right for a value stored under two codes.

- `TypingProbe.java` → `typing-results.txt` (2026-10-09, step 2's landing 2): where a bare `?` is typed by the
  database — 26 positions: compared with a column, in arithmetic and functions over a column, alone in a projection,
  in arithmetic and functions alone, under `IS NULL` and `IS NOT DISTINCT FROM` (a value and a null), and the same
  positions with the placeholder cast. Every one answers on all three databases when the JDBC call carries the value's
  type (`setLong`, `setString`, `setBigDecimal`, `setObject(LocalDate)`, `setNull(i, Types.VARCHAR)`), so a plan's
  statement writes `?` without a cast and the runner binds each value by its declared type.
- `LiteralProbe.java` → `literal-results.txt` (2026-10-09, slice (b)): a bound value against the literal a `let`
  writes today, for each Pure primitive, in arithmetic with a column (a number) or compared with one, and projected in a
  subquery and read outside (the wire's cell): the literal, the value bound bare, the value bound in a cast; then a
  decimal beside an integer column, and H2's casts of a decimal. DuckDB and Postgres type a bare `?` by the bound value,
  every type answering as its literal (a Float bound as a decimal: as a double, `PRICE * ?` answers
  `1.6500000000000001`). H2 types a parameter when it prepares the statement — by its neighbour (`ID * ?` with 1.5
  answers `[2, 6]`) or not at all when it stands alone (the eight `FAIL` lines, `Unknown data type`) — so it takes a
  typed placeholder, exact for every type but a decimal, whose literal is typed by its own digits: no cast keeps a
  value's own scale (`NUMERIC` rounds, `NUMERIC(38,2)` pads, `DECFLOAT` drops trailing zeros). The last section is
  H2 2.4.240 (the PCT lane's pin, `-cp h2-2.4.240.jar`): the same answers in every case. PARK-19.
- `ListProbe.java` → `list-results.txt` (2026-10-09, slice (e)): a list parameter bound as one array (`col = ANY(?)`,
  the array from `createArrayOf` under each element type name a plan could carry) against the literal list a `let`
  writes (`col IN (...)`), for integers, strings, decimals, dates, timestamps and booleans, and the empty list. Every
  database answers as the literal under every name tried — but for decimals on DuckDB, whose driver makes a `DECIMAL`
  array of three places (`0.1234` read as `0.123`: the wrong row; PARK-20). On H2 the array must stay bare: a cast
  inside `ANY(...)` is read as H2's boolean `ANY` aggregate (the 15 `FAIL` lines).
- `ValueTypedCastProbe.java` → `value-typed-cast-results.txt` (2026-10-09, step 3): on H2 2.1.214 and 2.4.240 alike, a value bound in a cast to
  its own literal's type — the type the runner writes into a plan's type hole — against the literal: decimals of every
  shape, whole numbers, extreme magnitudes, dates and date-times to the nanosecond (passed as text), alone, in
  arithmetic, compared and inside the answer's JSON. The same type and text throughout, but a small whole number's type
  (BIGINT for the literal's INTEGER, the same text); the last case is the control: a plain `TIMESTAMP` rounds. Then an
  absent value of each kind, a null cast to its type by name alone: valid, and null, in every position. PARK-19's fix.
- `TimestampProbe.java` → `timestamp-results.txt` (2026-10-09, step 3): on DuckDB, a date-time bound bare and in each
  cast, as a `LocalDateTime`, a `Timestamp` and its text, against the `TIMESTAMP` and `TIMESTAMP_NS` literals. The
  driver cuts a bound date-time to the microsecond, even into a `TIMESTAMP_NS` cast; passed as text, the cast keeps
  every digit, as the literal does; and Postgres's driver rounding a bound value (measured by `PostgresArmTest`, its
  header). Why a date-time parameter is passed as its text on every database.
- `engine-reference/` (2026-10-10, step 3): the real reference. `record_parameters.py` and `record_literals.py` send
  the plan tests' queries (`model.pure`), with parameters and with the values written in, to legend-engine 4.145.0's
  execute and keep its answers; `compare_with_lite.py` sends the identical requests to lite's execute and compares
  every cell's exact text; `pure_text.py` runs plain Pure expressions on the engine's Pure, no database, printed by
  `toString` — Pure's own text. `results.txt`: where the engine answers, lite's values are its own but in four
  places (PARK-24); the engine cannot run a projected parameter, a Number parameter or a DateTime list; today's lite
  path refuses an absent optional value.
- `EngineValidationProbe.java` → `engine-validation-results.txt` (2026-10-09, step 3): legend-engine 4.145.0's own
  parameter validation, run on its released jars: its messages, its order of checks (missing, validation, its
  normalizer), its type list's order, and what it passes on (a null value, an empty list, NaN). The compatibility
  mode's reference (phase 2); the lite runner checks in its own words.

## `sharing/` — when two runs may share an in-memory database (2026-10-08)

`results.md`: the stress corpus with a shared session per test data against a fresh one per test, and how often a
run changes a shared database in the stress corpus and the relational corpus, counted by `count-probe.patch`
(applied, measured, reverted). The basis of decision A in the plan's §9.
