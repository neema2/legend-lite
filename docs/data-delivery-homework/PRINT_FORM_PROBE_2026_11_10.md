# Print forms on every database, measured (2026-11-10)

The user's question: which `toString` convention for a Float and a DateTime can every database produce cheaply, with
one format call, and give the same text everywhere? Five lane sessions measured it read-only on their live databases
(constant `SELECT`s, no repo change), 13:24-13:29 UTC. MongoDB was NOT measured: no live server in any lane session.

| session | databases, versions |
|---|---|
| 1b | DuckDB 1.5.6, SQLite 3.47.1, H2 2.5.252, MariaDB 11.4.13, MySQL 8.4.11 |
| 1a | Postgres 16.15, ClickHouse 25.8.2.29, Trino (the embedded coordinator) |
| Snowflake | Snowflake (the lanes' account, session time zone UTC) |
| BigQuery | BigQuery (JDBC driver 1.6.0) |
| Databricks | Databricks runtime 4.2.0 (session time zone UTC) |

The full per-database tables are in each session's report (the orchestrator session's transcript, 2026-11-10).

## 1. The plain cast of a Float (F0): one call, but neither the same digits nor the same spelling

| value | DuckDB | SQLite | H2 | MariaDB/MySQL | Postgres | ClickHouse | Trino | Snowflake | BigQuery | Databricks |
|---|---|---|---|---|---|---|---|---|---|---|
| 1.0 | `1.0` | `1.0` | `1.0` | `1` | `1` | `1` | `1.0E0` | `1` | `1` | `1.0` |
| 0.1+0.2 | `0.30000000000000004` | `0.3` | `0.30000000000000004` | `0.30000000000000004` | `0.30000000000000004` | `0.30000000000000004` | `3.0000000000000004E-1` | `0.3` | `0.30000000000000004` | `0.30000000000000004` |
| 1234567890.12 | `1234567890.12` | the same | `1.23456789012E9` | the same | the same | the same | `1.23456789012E9` | the same | the same | `1.23456789012E9` |
| 1e-7 | `1e-07` | `1.0e-07` | `1.0E-7` | `0.0000001` | `1e-07` | `1e-7` | `1.0E-7` | `1e-07` | `1e-07` | `1.0E-7` |

SQLite and Snowflake round to 15 significant digits; H2, Trino and Databricks print Java's forms (Trino always in
scientific form). **Correction:** the earlier claim, from three databases, that "the digits always agree" is false.

## 2. Fixed N decimals with the trailing zeros trimmed (F1): one call plus one trim on EVERY database

The call: DuckDB/SQLite/ClickHouse `printf('%.Nf', x)`; BigQuery/Trino `format('%.Nf', x)`; Databricks
`format_string('%.Nf', x)`; Postgres/H2/MariaDB/MySQL/Snowflake a cast through `DECIMAL(38,N)`/`NUMBER(38,N)`. The trim: one
`regexp_replace(s, '\.?0+$', '')` (SQLite: two `rtrim`s, it has no regexp).

The text is the same on every database while N decimals fit in about 15 significant digits. Beyond that, DuckDB,
ClickHouse and Snowflake print the double's exact binary value and the others print clean digits:

(The guarantee: a double's error is at most about 1.1e-16 of its size, so N decimals are clean while the size stays below
0.5·10^-N / 1.1e-16 ≈ 4.5·10^(15-N); above it, whether noise shows depends on the value. The measured splits are
above the guarantee. Every Bitcoin amount, at most 21 million with 8 decimals, is inside N = 8's guarantee.)

| N | the same text everywhere, guaranteed, below about | first value measured that splits | small values |
|---|---|---|---|
| 6 | 4,500,000,000 | `12345678901.12` → `12345678901.120001` (DuckDB, ClickHouse, Snowflake) | below 0.0000005 → `0` |
| 8 | 45,000,000 | `1234567890.12` → `1234567890.11999989` | below 0.000000005 → `0` |
| 12 | 4,500 | `1234567.89` → `1234567.889999999898` | |
| 15 | 4.5 | `123.45` → `123.450000000000003` | |

Two more differences, at any N:
- **Exact halfway values round differently.** At 2 places, `0.125` gives `0.12` on DuckDB `printf`, ClickHouse `printf` and
  Postgres `to_char`, and `0.13` elsewhere. `2.675` (stored as 2.67499…) gives `2.67` on DuckDB, SQLite, ClickHouse, Snowflake
  and BigQuery, and `2.68` on H2, MariaDB, MySQL, Postgres's numeric, Trino and Databricks.
- **Negative zero.** It is kept (`-0`) by printf/format (DuckDB, ClickHouse, Trino, BigQuery, Databricks) and lost (`0`)
  through a decimal cast (Postgres, H2, MariaDB, MySQL, Snowflake) and on SQLite.

## 3. 15 significant digits (F2): NOT one call everywhere, and not one spelling where it is

- One call, the C rule (an exponent below 1e-4 and from 1e15: `1e-07`, `1e+15`): DuckDB, SQLite, ClickHouse, BigQuery
  (`printf`/`FORMAT('%.15g')`), Snowflake (its plain cast).
- One call, a different spelling: Postgres (`CAST(x AS numeric)`, never an exponent: `100000000000000000000`), H2
  (`DECFLOAT(15)`, `1E+15`, `1E-7`).
- Not one call: MariaDB and MySQL (no printf, `%g` or DECFLOAT: a computed scale and several calls), Trino and Databricks
  (Java's `%g` keeps trailing zeros and pads: a regexp or a `CASE` on the magnitude).

## 4. Timestamps

| | stores by default | ISO UTC, 6 digits + `Z`, one call? | 9 digits (nanoseconds), one call? |
|---|---|---|---|
| DuckDB | microseconds (`TIMESTAMP_NS` for nanos) | yes: `strftime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')` | `strftime(ts_ns, '…%S.%n') \|\| 'Z'` (a 1.5.6 bug corrupts a literal after `%n`) |
| SQLite | text | **no**: `%f` is 3 digits; 6 needs string work | no (no time type) |
| H2 | `TIMESTAMP(6)` (rounds 9 digits) | yes: `FORMATDATETIME(ts, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''')` | yes, on `TIMESTAMP(9)` |
| MariaDB | `DATETIME(0)`; max 6 (truncates) | yes: `DATE_FORMAT(…, '%Y-%m-%dT%H:%i:%s.%fZ')` | no (no nanosecond type) |
| MySQL | as MariaDB; max 6 (rounds) | yes | no |
| Postgres | microseconds (rounds 9 digits) | yes: `to_char(ts, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')` | no (no nanosecond type; `FF9` is printed literally) |
| ClickHouse | `DateTime` whole seconds; `DateTime64(0..9)` | yes: `formatDateTime(ts, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC')` | yes, Joda pattern on `DateTime64(9)` |
| Trino | the literal's precision, up to 12 | yes: `to_iso8601` on `timestamp(6) with time zone` (`format_datetime` cuts to milliseconds) | yes, on `timestamp(9) with time zone` |
| Snowflake | `TIMESTAMP_NTZ(9)` (nanoseconds) | yes: `TO_VARCHAR(t, 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"')` (truncates) | yes, `FF9` |
| BigQuery | microseconds; no nanosecond type | yes: `FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', ts, 'UTC')` | the call works but always ends `000` |
| Databricks | microseconds (truncates 9 digits); no nanosecond type | yes: `date_format(ts, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")` with the session in UTC | the call works but always ends `000` |

A DATE prints `2024-01-02` by one call on every database.

## 5. What the measurements say

- **Float:** fixed 6 decimals with the trim is the only candidate that is one call (plus one trim) on every database
  measured, and it gives the same text everywhere below 4.5·10^(15-N) (N = 6: 4.5 billion; N = 8: 45 million). Its limits:
  values below 0.5·10^-N print `0`; above the guarantee DuckDB, ClickHouse and Snowflake can show binary noise in the last places; exact
  halfway values and `-0` differ by database. 15 significant digits does not meet "one call everywhere" (four databases
  need more, and the spellings differ where it is one call).
- **DateTime:** ISO 8601 in UTC, six fraction digits and `Z`, is one call on ten of the eleven; SQLite needs a small
  expression. Nanoseconds exist on five (DuckDB's own type, H2, ClickHouse, Trino, Snowflake); elsewhere there is no
  type to hold them.
- **Still to measure:** MongoDB.
