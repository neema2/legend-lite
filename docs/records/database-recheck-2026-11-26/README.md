# The databases where lite's SQL blows up, looked at again (2026-11-26)

*The user's instruction, with the fold rule's ruling that day: "relook at any database where the sql blows up - make
sure we are running latest versions and relook at sql/database docs don't trust our docs one what is and is not
supported". Three read-only research passes, one per group of databases; nothing here is built. Every fact about a
database in the three reports comes from the vendor's documentation read that day (cited there by URL) or from a
probe on the database (H2, SQLite, ClickHouse, Trino and Postgres were run; MySQL, MariaDB, BigQuery, Snowflake and
Databricks were read). Each pass re-planned the same queries (PCT's channel A) with the product's own planner and
matched the size baseline's totals exactly. Savings are estimates from those texts, not measured after a change.*

The reports: `clickhouse-trino-postgres.md`, `sqlite-mysql-mariadb.md`, `h2-bigquery-snowflake-databricks.md`.

## What the same queries cost today

| Database | Characters (thousands) | Against DuckDB |
|---|---|---|
| DuckDB | 530 | 1.0 |
| Databricks | 656 | 1.2 |
| Snowflake | 846 | 1.6 |
| Trino | 920 | 1.7 |
| BigQuery | 973 | 1.8 |
| Postgres | 957 | 1.8 |
| SQLite | 1,563 | 2.9 |
| MariaDB | 1,637 | 3.1 |
| ClickHouse | 1,774 | 3.3 |
| MySQL | 2,107 | 4.0 |
| H2 | 493, over fewer statements (it refuses many list operations) | |

## One cause explains most of it

**A list, or a computed value, is written more than once per operation.** A null guard writes it twice
(`CASE WHEN xs IS NULL THEN NULL ELSE f(xs) END`); turning a list into rows writes it once as the row source and once
per column to read each element. Operations nest, so a chain of N steps writes the first list 2^N times. It is the
largest item on MySQL, MariaDB, Postgres, Snowflake, BigQuery and SQLite. The fix has one shape everywhere: bind the
value once (a one-row derived table, a lateral, the row source's own value column) and read the name.

## The wins, ranked across the eleven

| # | Database | What | Estimated saving (characters) |
|---|---|---|---|
| 1 | ClickHouse | its settings ride the connection, not every statement (303 characters on each of 2,258 statements) | 684,000 (38.5% of its SQL) |
| 2 | MySQL, MariaDB | a list bound once per chain of list steps | 500,000 and 350,000 |
| 3 | MySQL, MariaDB, SQLite | a list turned into rows written once (overlaps 2) | 320,000, 230,000, 75,000 |
| 4 | SQLite | one fixed date-time text, nine fraction digits, made at each source; computed values bound once | 250,000 to 300,000 |
| 5 | MySQL | no `LATERAL` wrapper around a `JSON_TABLE` argument (1,901 of them) | up to 110,000 |
| 6 | ClickHouse, Trino | a literal typed once per column or array, not on every cell | 90,000 to 110,000 each |
| 7 | Snowflake | a list turned into rows bound once | 100,000 (12%) |
| 8 | BigQuery | the same, and the null guard dropped where a list cannot be NULL | 70,000 and 45,000 |
| 9 | Postgres | the null guard dropped over an array constructor; computed lists bound once | 56,000 and more |
| 10 | Trino | its own `date_diff` and `date_trunc` instead of cutting the timestamp's text | 55,000 |
| 11 | every database but DuckDB | identifiers quoted only where they need it | 30,000 to 50,000 each |
| 12 | H2 | a literal list read at a literal position, not a `CASE` per element | 38,000 (8%) |
| 13 | Snowflake | list statistics (`stdDev`, `variance`, `mode`) without a `REDUCE` written three times | 30,000 |
| 14 | MySQL | `SEPARATOR ','`, the default, not written (1,433 times) | 20,000, no risk |

Not a size item but the largest gap in coverage: **H2 can write most of the list operations its dialect refuses**
(probed on the shipped 2.5.252: filter, map, distinct, sort, reverse, slice, position, zip, the aggregates, over
`SYSTEM_RANGE` and the array read by index; fold and an exact product cannot be written).

## Claims of ours that were false or out of date

| Claim in lite's code or notes | What is true |
|---|---|
| ClickHouse: "a SET does not outlive its statement" | false: a setting rides the URL, a connection property or an HTTP session (probed, and documented) |
| ClickHouse's notes say 25.8 | we run 26.3.60.2 |
| Trino "has no PIVOT" | false on 483, the version we run |
| Trino "a nanosecond epoch has no spelling" | `from_unixtime_nanos` exists |
| H2 "no bit operations", "no ANY_VALUE", "no native lists", "no STRUCT" | false or outdated on 2.5.252: BITAND and its family, ANY_VALUE, ARRAY, ROW |
| H2's `LIST_FLATTEN` written `flatten()` | H2 has no such function: a wrong spelling |
| BigQuery "every list operation is a correlated subquery" | outdated: ARRAY_SLICE, ARRAY_FIRST, ARRAY_LAST |
| Databricks "no TIME type" | outdated: in DBR 18 LTS, as Beta |
| Snowflake "a list is reduced only with REDUCE" | too broad: an uncorrelated scalar subquery over FLATTEN is allowed anywhere |
| MariaDB "no VALUES alias list" | true on our 11.4, outdated from 11.7 |
| Postgres "never a native ARRAY", "IGNORE NULLS refused" | our dialect writes arrays now; IGNORE NULLS from Postgres 19 |

True and still standing: MariaDB has no LATERAL, no FULL JOIN and no outer reference in a derived table; H2 has no
LATERAL, no correlated UNNEST and no lambda; Snowflake refuses a correlation inside FLATTEN; ClickHouse's own
hyperbolic functions are wrong in the sixth digit and Postgres has no Float `mod` (our long forms there are right).

## Versions

| Database | We run | Latest |
|---|---|---|
| DuckDB | 1.5.6 | 2.0.1 (2.0.0 on 21 Oct 2026) |
| H2 | 2.5.252 | 2.5.252 |
| SQLite | 3.53.1 | 3.54.1 |
| Postgres | 16.15 | 19.0 (19 Nov 2026); 16.16 |
| MySQL | 8.4.11 | 8.4.13; 9.7 LTS |
| MariaDB | 11.4.13 | 11.4.14; 11.8 LTS; 12.3; 13.1 |
| Trino | 483 | 484 |
| ClickHouse | 26.3.60.2 LTS | 26.8 LTS; 26.11 |
| Snowflake, Databricks, BigQuery | services: 10.36, DBSQL 2026.39, none | 10.42, 2026.47, none |

## Defects found on the way (not size)

- **ClickHouse parses a text to a Float with its fast parser** on 26.3: a last digit off on some values, the smallest
  subnormal read as zero. Lite's Float literals no longer go through it (the next batch). A text parsed at run time
  (`parseFloat`, a cast of a string) still does: `precise_float_parsing = 1` makes it exact (the default from 26.7).
- **ClickHouse: one of our settings is obsolete from 26.5** (`query_plan_use_new_logical_join_step = 0`): on an
  upgrade the guarded raises it protects stop being protected, silently.
- **MariaDB returns a list of literal rows in no promised order** (37 statements are a `UNION ALL` with no `ORDER BY`):
  the likely cause of its lane's run-to-run list-order failures. Not reproduced here; it does not explain every one.
- **BigQuery refuses a sort of a list of mixed kinds:** lite orders by a STRUCT, which BigQuery does not order
  (confirmed on the account by the lane the same day: `|[1, 'a', true]->sort()` is refused, "ORDER BY does not
  support expressions of type STRUCT<...>"; a list of Numbers alone sorts correctly). The fix: order by the struct's
  fields.
- **H2:** `CARDINALITY(NULL)` in the subquery form above crashes ("Unexpected code path"): write
  `COALESCE(CARDINALITY(..), 0)`.

## How it would be done

Each row above is a slice of its own: built in a dialect, judged by the tests' answers and by the size baseline going
down, with its database's lane. None is started. Suggested order: the bind-once shape on MySQL and MariaDB first (the
largest, and the same shape then serves Postgres, Snowflake, BigQuery and SQLite); ClickHouse's settings; SQLite's
date-time text; the literal typing on ClickHouse and Trino; then the rest by size.
