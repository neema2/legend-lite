# SQL size check: SQLite, MySQL, MariaDB

## Summary: the biggest wins, ranked

1. **MySQL and MariaDB: write each list once, not twice per step.** `MariaDb.unlessNull` writes a list as `(CASE WHEN X IS NULL THEN NULL ELSE coalesce((SELECT .. FROM JSON_TABLE(X ..)), JSON_ARRAY()) END)`. X appears twice, so a chain of N list steps writes the first list 2^N times. 91 MySQL statements have this nesting. They come to 855K characters; DuckDB writes 42K for the same queries. On MariaDB the same queries come to 596K. Expected saving: roughly 500K on MySQL and 350K on MariaDB (an estimate, not a measurement).
2. **All three: a root explosion of one list writes the list twice.** It is written once as the row source and again to read each element by its position. Reading the row source's own value column writes it once. Saving: about 320K on MySQL, 230K on MariaDB and 75K on SQLite (this overlaps with win 1).
3. **SQLite: one fixed text form for a date-time, plus binding a computed value once.** The date families (timeBucket, adjust, dateDiff) come to 375K characters; DuckDB writes 33K. `micros(t)` is inlined up to 12 times, and output and judging re-parse the text. Expected saving: about 250-300K, and two texts of one instant become equal under `=`, `GROUP BY` and `DISTINCT`.
4. **MySQL: wrap `JSON_TABLE` in LATERAL only when its argument holds a subquery.** MySQL's own docs allow a function call there. There are 1,901 wrappers of about 60 characters each.
5. **MySQL: drop `SEPARATOR ','`.** A comma is GROUP_CONCAT's default. There are 1,433 of them, about 20K characters, and the change has no semantic risk.
6. **SQLite: named windows.** SQLite 3.53 accepts `WINDOW w AS (...)` (probed). Today 677 `OVER (...)` clauses are spelled out in full, often the same one several times.

**Claims found false or outdated:**
- MariaDB's "no VALUES alias list" is OUTDATED from MariaDB 11.7: column names after a derived table's alias are supported there. We pin 11.4.
- The SQLite class javadoc says hash, digest and Jaro-Winkler are "walls, by name". That is stale inside our own code: the dialect implements all three in SQL.
- SQLite's javadoc gives a 1,000,000-byte statement limit. That is the runtime limit as probed, but the build is compiled with `MAX_SQL_LENGTH=1073741824`, so the limit is a setting, not a property of the database.

Every other claim checked holds, by a probe or by vendor documentation (tables below).

Method: I planned the 2,451 `channel_a` queries on DuckDB, SQLite, MySQL and MariaDB with a throwaway Java program. It used the classpath of the built `lean_sql_baseline_test` in `/home/user/legend-lite` (commit 73e3a6c1; `ll-next` is at 52f62b34) and planned through the same path as `LeanSqlBaseline.measure`. The totals match `baseline.tsv` exactly: DuckDB 529,909, SQLite 1,562,673, MySQL 2,106,971, MariaDB 1,636,505. SQLite probes ran on the product's own driver jar (sqlite-jdbc 3.53.1.0). MySQL and MariaDB could not be reached from here, so their facts come from vendor docs, or from the dialect's own measured notes where marked.

---

## SQLite

### 1. Version

- **Lite runs:** xerial sqlite-jdbc **3.53.1.0**, which bundles SQLite 3.53.1. Source: `tools/deps/jars_table.bzl:128-133`. The probe `select sqlite_version()` returned `3.53.1`.
- **Latest:** SQLite **3.54.1** (2026-10-16), per https://sqlite.org/. sqlite-jdbc **3.54.1.0** (2026-10-29) bundles it, per https://github.com/xerial/sqlite-jdbc/releases.
- **Changes since 3.53 that matter for SQL size** (https://sqlite.org/releaselog/3_54_0.html):
  - `printf` gains `%J`/`%j` (JSON string literals).
  - The date functions gain a `weekday -N` modifier and an `end of` modifier.
  - 3.53.0 added `json_array_insert` (https://sqlite.org/releaselog/3_53_0.html).
  - None of these changes the list or date-time picture much. `weekday -N` could shorten the WEEK truncation `date(x, '-6 days', 'weekday 1')` slightly (not verified).
- **Compile options of the product build** (probe: `pragma_compile_options`): `ENABLE_MATH_FUNCTIONS`, `ENABLE_PERCENTILE`, FTS3/5, RTREE, STAT4, `MAX_SQL_LENGTH=1073741824`, `MAX_COMPOUND_SELECT=500`. There is no generate_series, carray, regexp, sha3 or md5 (all probed as "no such table/function").

### 2. Claims (SQLite.java javadoc, lines 33-66, and the LEXICON comment)

| Claim | Verdict | Evidence |
|---|---|---|
| No VALUES alias list `AS t(a,b)` | TRUE | Probe: `select * from (values (1,2)) as t(a,b)` gives a syntax error near "(". |
| No quantified comparison | TRUE | Probe: `1 = any (select 1)` gives a syntax error. |
| No QUALIFY | TRUE | Probe: a syntax error. |
| No LATERAL (only a table-valued function correlates) | TRUE | Probe: `, lateral (select 1)` gives a syntax error. `json_each(a.value)` correlates (probe). |
| No ORDER BY/LIMIT on a UNION branch | TRUE | Probe: "ORDER BY clause should come after UNION ALL not before". Wrapping the branch in a subquery works (probe). |
| No regular-expression function | TRUE for this build | Probe: `regexp()` and `x REGEXP y` both give "no such function". https://sqlite.org/lang_expr.html says REGEXP is user-defined. |
| No hash or digest function | TRUE for this build | Probe: `sha3('a')` gives "no such function". |
| "hash, digest, Jaro-Winkler: walls, by name" | **OUTDATED, internally** | The dialect now writes MD5/SHA-1 and Jaro-Winkler as recursive SQL (`SQLite.java` around line 2640; the `hash` family is 21K characters for 3 queries). |
| A statement is at most 1,000,000 bytes | TRUE at runtime; it is a setting | Probe: 999,000 bytes runs, 1,001,000 gives "statement too long". The compile maximum is 1,073,741,824 (`pragma_compile_options`), so `sqlite3_limit` could raise it (whether xerial exposes that is not verified). |
| Integer overflow becomes REAL silently | TRUE | Probe: `9223372036854775807 + 1` gives `9.22e18`, type `real`. |
| `x / 0` and `x % 0` are NULL; LIKE is case-insensitive | TRUE | Probe. |
| A bad JSON path raises with its text | TRUE | Probe: "bad JSON path: 'msg'". |
| FULL OUTER is native (3.39) | TRUE | Probe. |
| LEXICON comment: "KNOWN GAPS... typed DATE/TIMESTAMP literals, list lambdas, QUALIFY, ASOF" | OUTDATED as a description | The class now handles each of these by its own rewrite. |
| json_each correlates inside a scalar subquery | TRUE | Probe. |

SQLite features we could use, all probed on 3.53.1:
- `json_group_array(x ORDER BY ..)` and `string_agg(.. ORDER BY ..)` (since 3.44).
- `IS [NOT] DISTINCT FROM`, `FILTER (WHERE ..)` and `NULLS LAST`.
- Named `WINDOW w AS (..)`.
- `'[..]' ->> '$[#-1]'`.
- `percentile`/`median`.
- `jsonb()`. It shortens nothing, because the SQL text stays the same.
- A correlated derived table inside a scalar subquery works (`select (select b.m*2 from (select a.v+1 as m) b) from ..`), as does a correlated `WITH RECURSIVE` inside a scalar subquery.

`generate_series` and `carray` are not in the JDBC build. https://sqlite.org/series.html calls generate_series "a loadable extension included in the SQLite source tree"; it is not part of the core library.

### 3. Where the SQL blows up (channel_a, characters)

| Family | SQLite | DuckDB | Ratio | Excess |
|---|---|---|---|---|
| timeBucket (DateTime) | 171,193 | 13,309 | 12.9x | 157,884 |
| adjust | 94,978 | 5,327 | 17.8x | 89,651 |
| TypedExtendWindow | 193,249 | 112,474 | 1.7x | 80,775 |
| removeDuplicates (with function) | 87,680 | 8,644 | 10.1x | 79,036 |
| dateDiff | 66,153 | 11,104 | 6.0x | 55,049 |
| regexpExtract | 53,803 | 566 | 95x | 53,237 |
| let | 223,561 | 172,513 | 1.3x | 51,048 |
| hashCode | 43,123 | 939 | 46x | 42,184 |
| timeBucket (StrictDate) | 42,656 | 3,408 | 12.5x | 39,248 |
| hash | 21,002 | 112 | 188x | 20,890 |

**timeBucket, adjust, dateDiff (`SQLite.timeBucket`, `addInterval`, `boundaries`, `micros`, `fromMicros`)**

Why it is long:
- `micros(t)` writes `t` 4 times (its whole seconds, an `instr`, a `substr` and a second `instr`). This is because the fraction may be absent or of any length.
- `fromMicros(us)` writes `us` 4 times, and `timeBucket`'s floor writes its input twice.
- A literal week bucket therefore holds `micros(t)` 12 times (3,066 characters; DuckDB writes 107).
- The root's text output then re-reads the value twice more: `strftime('%Y-%m-%dT%H:%M:%S.', X) || substr(<fraction of X>)`.

Shorter form, which needs no new SQLite feature: bind once in a scalar subquery (a probe confirms this correlates) and read a fixed form.

```sql
-- today: 3,066 chars for timeBucket(18, WEEKS) of a literal
(SELECT datetime(f.u / 1000000, 'unixepoch') || '.' || printf('%06d', f.u % 1000000) || '000'
 FROM (SELECT m.m - ((m.m - o) % s + s) % s AS u
       FROM (SELECT unixepoch(substr(t, 1, 19)) * 1000000 + CAST(substr(t, 21, 6) AS INTEGER) AS m,
                    -259200000000 AS o, 18 * 604800000000 AS s) m) f)
```

The probe ran this with `t` = `'2024-01-31 17:15:59.249123456'`. About 300 characters plus the value once: an **85-90% cut** for this family. The micros read is short only once the text form is fixed (part (a) below).

**removeDuplicates with an equality function**

Why it is long: it is a fold, a correlated `WITH RECURSIVE`, and that is correct here. A non-recursive "no earlier equal element" form would differ for an equality that is not transitive. But the list is written 3 times inside the fold (the seed, the joined elements, the wrapping `json(json_array(..))` per element). The root explosion then writes the whole result twice more.

Shorter form: put the list in the fold's first CTE (`WITH RECURSIVE __in(xs) AS (SELECT <list>), ...`) and read it twice from there. Fix the root explosion as in win 2. Saving: roughly 60%.

**regexpExtract, hash, hashCode**

Why it is long: SQLite has no regular-expression or digest function (probed). The pattern is compiled to an automaton that a recursive query walks (`SqliteRegex`), and MD5/SHA-1 are recursive SQL. This is the best SQLite can do without registering a host function, which "the database executes" forbids. One part can shrink: `regexpExtract` also pays the root-explosion double read.

**TypedExtendWindow**

Why it is long:
- The window clause is spelled in full at every use. One query writes `LEAD(_tds0.fromDate) OVER (PARTITION BY .. ORDER BY ..)` 8 times inside date arithmetic.
- MySQL computes the window calls once in a select below (`WindowsComputedBelow`); SQLite does not.

Shorter form, both supported by the SQLite build we pin (probed):
- `.. OVER w .. WINDOW w AS (PARTITION BY _tds0.id, _tds0.fromDate ORDER BY ..)`;
- or SQLite also takes MySQL's pass (`WindowsComputedBelow`), so each window call is computed once in a select below.

**let, the root explosion (`SQLite.select` for a marker, `positions`, `element`)**

Why it is long: one exploded list is written as `SELECT json_extract(L, '$[' || (__u0.key) || ']') AS value FROM json_each(L) AS __u0`, so L appears twice. With several lists, `positions` writes a CASE that compares every pair's lengths, which is quadratic.

Shorter form:

```sql
SELECT __u0.value AS value FROM json_each(L) AS __u0 ORDER BY __u0.key
```

`json_each.value` is the same SQL value `json_extract` returns: a string unquoted, true as 1, a nested array as its JSON text (probed). An element that is itself a list or struct needs `json(__u0.value)` only where the JSON subtype matters. Saving: about 75K over 59 statements.

### (a) A fixed text form for a date-time

**Every place a date-time text is produced today** (SQLite.java):

| Source | Code | Form today |
|---|---|---|
| Literal | `timestampLit` (line 765), after `keptFraction` | as written: 0 to 9 fraction digits, `T` replaced by a space |
| Loaded cell, ISO shape | `RowLoad.cell` makes a `TimestampLit`, then as above | as written |
| Loaded cell, other shape | `Cast(StringLit, TIMESTAMP)` gives `padded(dateTimeText(..))` | as written, day fields padded |
| `data:` payload (`sourceUrl`) | `json_each` of the bound JSON | as written in the payload |
| parseDate / cast from text | `parsedDate`, `padded`, `dateTimeText` | as written |
| Zoned text to UTC | `utcOf`, `shift`: `datetime(substr(t,1,19), offset) \|\| substr(t,20)` | seconds by `datetime`, the fraction as written |
| Cast from a date | `x \|\| ' 00:00:00'` | no fraction |
| `NOW` | `strftime('%Y-%m-%d %H:%M:%f','now')` | 3 digits |
| `FROM_EPOCH_SECONDS` | `datetime(x,'unixepoch')` | no fraction |
| `FROM_EPOCH_MS` | `strftime('%Y-%m-%d %H:%M:%f', x/1000.0, 'unixepoch')` | 3 digits |
| `DATE_TRUNC` hour, minute, second | `strftime('%Y-%m-%d %H:00:00', t)` etc. | no fraction |
| `DATE_TRUNC` millisecond | `strftime(.. %f)` | 3 digits |
| `DATE_TRUNC` day | `date(t) \|\| ' 00:00:00'` | no fraction |
| `addInterval` years, months | `datetime(substr(t,1,19), mod, 'floor') \|\| substr(t,20)` | input's fraction as written |
| `addInterval` fixed units, `timeBucket` fixed units | `fromMicros` | none, or 1 to 6 digits with trailing zeros trimmed |
| `timeBucket` months, years | `date(..) \|\| ' 00:00:00'` | no fraction |
| `makeTimestamp` | `CASE .. printf('%02d') ELSE rtrim(printf('%09.6f'),'0')` | none, or 1 to 6 digits trimmed |
| A column of a user's own table | whatever wrote it | anything |

**What SQLite itself returns** (probed on 3.53.1):
- `datetime(x)` has no fraction.
- `datetime(x,'subsec')` and `strftime('%f')` always give exactly 3 digits, truncated: `'..00.9999'` with `'+1 second','subsec'` gives `'..01.999'`.
- `julianday` parses only milliseconds.

https://sqlite.org/lang_datefunc.html says %f is "fractional seconds: SS.SSS", that "only the first three digits are significant to the result", and that subsec "increases the resolution from seconds to milliseconds". The `subsec` modifier exists **since 3.42.0** (https://sqlite.org/releaselog/3_42_0.html: "Added the subsecond modifier to the date and time functions").

So no SQLite function can produce or normalize a 9-digit fraction. Any fixed form has to be made by text.

**Proposal: `'YYYY-MM-DD HH:MM:SS.fffffffff'`, always 9 fraction digits.**

A fixed width compares and sorts as the instant for years 0000-9999 (probed: `'..00.000000000' < '..00.500000000'`). It is also cheaper than a trimmed form: every SQLite function already writes a fixed width, so each source only appends a constant. A trimmed form would need a CASE at each source, because `rtrim(x,'0')` would eat the seconds' zeros when there is no `.`.

| Source | Change | SQL at run time? |
|---|---|---|
| Literal, ISO loaded cell, `data:` payload | Java pads to 9 digits when the plan is made: the literal in `timestampLit`; the cell and payload where the load writes them, whose column type is known | none |
| `DATE_TRUNC` hour, minute, second | put the zeros in the format: `strftime('%Y-%m-%d %H:00:00.000000000', t)` (probed) | none, the same length |
| `NOW`, `FROM_EPOCH_MS`, `DATE_TRUNC` millisecond | `strftime(.. %f ..) \|\| '000000'` (probed) | a constant appended |
| `FROM_EPOCH_SECONDS`, `DATE_TRUNC` day, cast from a date, months `timeBucket` | `\|\| '.000000000'` | a constant |
| `fromMicros` | `datetime(s,'unixepoch') \|\| '.' \|\| printf('%06d', f) \|\| '000'` (probed) | shorter than today's CASE |
| `makeTimestamp` | `printf('%012.9f', s)` (probed: `05.500000000`) | shorter than today's CASE |
| `addInterval` years and months, `utcOf` | none: they keep the input's digits, already fixed | none |
| parseDate, cast from text, a user's own column | must normalize by text: pad the fraction to 9 digits, and add `.000000000` where there is none | **yes**: the one source that needs SQL at run time, once per value read |

What this removes:
- The judge's arm in `variantAwareCast` (about 600 characters per judged date-time) becomes the value itself for every lite-made source.
- `micros` becomes `unixepoch(substr(t,1,19))*1000000 + CAST(substr(t,21,6) AS INTEGER)`, with `t` twice instead of 4 times.
- `fraction(t,9)` becomes `substr(t,21,9)`.
- The root's text output becomes `replace(t,' ','T')` cut to the type's digits, with `t` once.
- `=`, `GROUP BY`, `DISTINCT` and join keys become correct for two spellings of one instant. Today `'2014-01-01 00:00:00' = '2014-01-01 00:00:00.000'` is 0 (probed).

Risks:
1. A text read from a user's table is not canonical until normalized, so a column read keeps one normalization.
2. Years outside 0000-9999 still do not sort as text (`'10000-..'`; probed `datetime('10000-01-01')` is NULL). That is the case `WIDE_YEAR_SECOND` handles today.
3. Written precision is lost. `%2014-01-01T00:00:00.000` and `%2014-01-01T00:00:00` hold the same text, so a printed value can no longer show how many digits were written. DuckDB's TIMESTAMP already loses this, so the change matches "one product spelling per type". Whether Pure output relies on written precision anywhere must be checked against S399 and the result writer's rule before landing.
4. An interval RANGE window sorts by `julianday(k)`, which parses only milliseconds, so ties below 1 ms are not ordered (line 3255). With a fixed form, `unixepoch(substr(k,1,19)) * 1e6 + ..` or the text itself could replace it.

### (b) Lists as JSON: what the latest SQLite offers

- `json_each`/`json_tree`: already used.
- `json_group_array(.. ORDER BY ..)`: since 3.44, already used.
- `->`/`->>` with `#-N`: shorter than `json_extract(x, '$[' || .. || ']')` only for a literal index. The index-building CASE `CASE WHEN 1 < 0 THEN '#' || 1 ELSE 1 - 1 END` seen in the dumps is a constant: it could be folded to `0` when the plan is made, a fold of a constant (allowed).
- `jsonb`: a binary storage form; it does not shorten SQL text.
- `generate_series` and `carray`: **not in the JDBC build** (probed), so a range stays a recursive CTE (`WITH RECURSIVE __g(__v) ..`).
- No array-like extension ships in sqlite-jdbc 3.53.1.0 (the compile options above).

The list win for SQLite is not a new feature. It is writing each list once (win 2, and the CTE binding for folds).

### 4. SQLite's three biggest wins

1. **The fixed date-time form plus binding computed values once.**
   - What: the change in (a), and `timeBucket`, `addInterval` and `boundaries` written as one bound scalar subquery.
   - Overturns: nothing; it uses correlated derived tables (probed).
   - Saving: about 250-300K of the 375K date families, plus egress and judge text elsewhere.
   - Risks: the written-precision rule (risk 3 above); years outside 0000-9999; a user's own columns still need normalizing.
2. **The root explosion reads `json_each.value`** when the marker has one list, and binds the lists once when it has several.
   - Saving: about 75K.
   - Risk: an element that is a list or struct must keep its JSON subtype (`json(value)`), as `element` does today.
3. **Named windows, or the `WindowsComputedBelow` pass**, plus a fold's list in its first CTE.
   - Saving: a part of TypedExtendWindow's 81K and removeDuplicates' 79K. The count is not measured.
   - Risk: none semantic; named windows exist since 3.28 (probed on 3.53).

---

## MySQL

### 1. Version

- **Lite runs:** MySQL **8.4.11**. Sources: `tools/mysql/mysql.bzl:15` and the PCT class headers ("MySQL 8.4.11, the pinned server").
- **Latest:**
  - 8.4 LTS is at **8.4.13.1** (2026-11-24), per https://dev.mysql.com/doc/relnotes/mysql/8.4/en/.
  - The newest LTS is **9.7**: 9.7.0 LTS went GA on 2026-04-21 (https://blogs.oracle.com/mysql/mysql-9-7-0-lts-is-now-available-expanded-community-capabilities-and-dynamic-data-masking-for-enterprise), and 9.7.4.1 is dated 2026-11-24 (https://dev.mysql.com/doc/relnotes/mysql/9.7/en/).
  - Later innovation releases use calendar numbers (26.7, 26.10).
  - Lite is two patch releases behind on 8.4 and one LTS behind.
- **What 8.4 to 9.7 changed for SQL size:** nothing found. The 9.7 "what is new" page lists only `VERSION_WITH_UPDATE()` (https://dev.mysql.com/doc/refman/9.7/en/mysql-nutshell.html). 9.7's `JSON_ARRAYAGG` still takes no ORDER BY (below). I did not read 9.0-9.6 one by one: **not verified** beyond 9.7's page and the 9.7 manual pages cited here.
- **Features we use or could use in 8.4**, verified in the vendor manuals cited:
  - CTE and recursive CTE (8.0).
  - Window functions (8.0).
  - `JSON_TABLE`, implicitly lateral; "the expr ... can be a constant, a column, or a function call" (https://dev.mysql.com/doc/refman/9.7/en/json-table-functions.html).
  - LATERAL (8.0.14).
  - A derived table column list `(subquery) AS t (a, b)` (https://dev.mysql.com/doc/refman/8.4/en/derived-tables.html).
  - `JSON_ARRAYAGG(expr) [over_clause]`, with no ORDER BY or DISTINCT (https://dev.mysql.com/doc/refman/9.7/en/aggregate-functions.html).
  - `VALUES ROW(..)` (8.0.19; not re-fetched).

### 2. Claims (MySql.java javadoc, lines 9-22, and its methods)

| Claim | Verdict | Evidence |
|---|---|---|
| `JSON_ARRAYAGG` takes no ORDER BY or DISTINCT | TRUE (9.7 too) | The syntax is `JSON_ARRAYAGG(col_or_expr) [over_clause]`; "The order of elements in this array is undefined." https://dev.mysql.com/doc/refman/9.7/en/aggregate-functions.html |
| No `JSON_COMPACT` | TRUE (9.7) | It is not in the 9.7 JSON function table, https://dev.mysql.com/doc/refman/9.7/en/json-function-reference.html |
| No negative JSON array index (`$[last-n]` instead) | not re-verified | The JSON path syntax page was not fetched; this is the dialect's own measurement. |
| `JSON_VALUE` takes only a literal path | TRUE | "This must be a string literal value." https://dev.mysql.com/doc/refman/9.7/en/json-search-functions.html |
| A derived table and a recursive CTE may read an outer row; LATERAL exists | TRUE | The derived-tables page points same-SELECT references to LATERAL; LATERAL has a section of its own (https://dev.mysql.com/doc/refman/8.4/en/derived-tables.html). |
| A table function's argument may hold no subquery | TRUE as measured; the docs do not mention subqueries | The docs list constant, column and function call only. **But `MySql.jsonTable` wraps every argument in LATERAL, including function calls (`CONCAT(REPEAT(..))`), which the docs allow bare.** |
| No PERCENTILE_CONT, PERCENTILE_DISC or MEDIAN | not re-verified | It is not in the aggregate page I fetched (the page lists none); this is the dialect's measurement. |
| A string with `'` or `\` is spelled with `CHAR(39/92 USING utf8mb4)`, because MySQL re-serializes a CTE read by another CTE | not verified in 9.7 | It is applied to every literal, not only inside CTEs: 1,439 `CHAR(92 ..)` and 553 `CHAR(39 ..)`, about 22 characters each. |
| `group_concat_max_len` must be raised | TRUE | "a default value of 1024", https://dev.mysql.com/doc/refman/9.7/en/aggregate-functions.html |

### 3. Where the SQL blows up

| Family | MySQL | DuckDB | Ratio | Excess |
|---|---|---|---|---|
| let | 427,098 | 172,513 | 2.5x | 254,585 |
| removeDuplicates (with function) | 160,966 | 8,644 | 18.6x | 152,322 |
| maxBy (with a count) | 139,021 | 375 | 371x | 138,646 |
| minBy (with a count) | 137,421 | 363 | 379x | 137,058 |
| TypedSort | 144,700 | 18,064 | 8.0x | 126,636 |
| TypedExtendWindow | 191,594 | 112,474 | 1.7x | 79,120 |
| regexpExtract | 60,277 | 566 | 107x | 59,711 |
| range | 42,925 | 1,828 | 23x | 41,097 |
| sort (with key function) | 37,421 | 471 | 79x | 36,950 |
| contains | 32,300 | 2,589 | 12.5x | 29,711 |

Almost all of the top ten share three causes. (TypedExtendWindow is the exception: about 1.7x on all three databases.)

**Cause A: the NULL guard writes the list twice** (`MariaDb.unlessNull`, MariaDb.java:1232; also lines 2456, 2121 and 2074). This is the main cause.

```sql
(CASE WHEN X IS NULL THEN NULL ELSE coalesce((SELECT <agg> FROM JSON_TABLE(X, ..)), JSON_ARRAY()) END)
```

When X is the previous step's result, each step doubles it. `maxBy([..], 3)` is a range, then a map, then a sort, then a slice, then a map: one statement of 139,021 characters, where DuckDB writes 375.

MySQL already allows the form SQLite uses (`correlatesDerived()` is true):

```sql
(SELECT CASE WHEN l.j IS NULL THEN NULL
             ELSE coalesce((SELECT <agg> FROM JSON_TABLE(l.j, '$[*]' COLUMNS (..)) AS e), JSON_ARRAY()) END
 FROM (SELECT X AS j) AS l)
```

X is written once, and the depth becomes linear. Saving: most of the 855K in the 91 nested statements.

**Cause B: an ordered array is GROUP_CONCAT text** (`MySql.arrayAgg`):

```sql
JSON_EXTRACT(CAST(CONCAT('[', GROUP_CONCAT(JSON_ARRAY(v) ORDER BY o SEPARATOR ','), ']') AS JSON), '$[*][0]')
```

This is about 95 characters of wrapping, 1,421 times, and multiplied by cause A. JSON_ARRAYAGG cannot be ordered (TRUE above), so the GROUP_CONCAT stays. But `SEPARATOR ','` is the default ("The default separator between values in a group is comma"): drop it, saving about 20K with no risk. Wrapping each value in `JSON_ARRAY(v)` and reading `$[*][0]` back is needed so that a string is quoted correctly. A shorter equivalent is not verified.

**Cause C: the root explosion reads each element by index from a second copy of the list.** `JSON_UNQUOTE(NULLIF(JSON_EXTRACT(X, CONCAT('$[', ("__u0"."key" - 1), ']')), CAST('null' AS JSON)))` comes `FROM (LATERAL (SELECT X ..) .. JSON_TABLE(..))`. A typed `JSON_TABLE` column reads the element directly: `FROM JSON_TABLE(X, '$[*]' COLUMNS (k FOR ORDINALITY, v BIGINT PATH '$'))`, then `SELECT v`. Saving: about 320K over 59 statements (overlaps cause A).

**Cause D: LATERAL around every JSON_TABLE argument** (`MySql.jsonTable`). There are 1,901 wrappers, about 60 characters each, including around the row generator `CONCAT('[', REPEAT('0,', n - 1), '0]')`, which is a function call and needs no wrapper per the docs. Wrap only an argument that holds a subquery.

Other families:
- **TypedSort:** variant navigation `CASE WHEN JSON_TYPE(x) = 'ARRAY' THEN x END` writes x twice per level (`variantElements`; 984 uses). Nested flatten and get double each level. Bind x once (the derived-table form above). The guard itself is needed: MySQL paths auto-wrap a scalar, so `$[*]` over a non-array would yield one row.
- **range:** writes its row generator inside the doubled guard. Cause A fixes most of it.
- **regexpExtract:** `REGEXP_REPLACE` per match index over a generated row set, with cause A on top. MySQL has `REGEXP_SUBSTR(expr, pat, pos, occurrence)`, which takes the occurrence directly. But it returns the whole match, not a group, so the group still needs `REGEXP_REPLACE`. Fixing cause A is the safe part.
- **`CAST(JSON_OBJECT(..) AS JSON)` and `CAST(JSON_ARRAY(..) AS JSON)`:** 1,227 casts. `compact` is applied to values that are already JSON (a JSON function's result). Skipping it when the value is a direct `JSON_OBJECT`/`JSON_ARRAY` call saves about 20K. This is not verified by a probe: MySQL could not be reached from here.

### 4. MySQL's three biggest wins

1. **Bind the list once in the NULL guard**, with the derived-table form MySQL allows.
   - Overturns nothing: it uses `correlatesDerived() == true`, a fact the dialect itself measured.
   - Saving: about 500K (estimate).
   - Risk: MySQL's optimizer may handle a derived table in a scalar subquery differently (performance, not answers). NULL handling is kept, since the guard is the same CASE.
2. **The root explosion reads the typed JSON_TABLE column.**
   - Saving: about 320K, overlapping win 1.
   - Risk: a typed PATH column reports an error or default on a value of the wrong type (`ON ERROR`/`ON EMPTY`), where `JSON_EXTRACT` would answer. Keep the index read for a mixed (Variant) list.
3. **LATERAL only around a subquery argument, and no `SEPARATOR ','`.**
   - Saving: up to about 110K plus 20K.
   - Risk: nearly none. The JSON_TABLE manual says the argument may be a function call.

---

## MariaDB

### 1. Version

- **Lite runs:** MariaDB **11.4.13**. Sources: `tools/mariadb/mariadb.bzl:13` ("MariaDB 11.4, the older of the two long-term releases ... so nothing newer creeps into the dialect") and the PCT headers. The driver is Connector/J 3.5.10 (`tools/deps/jars_table.bzl:99`).
- **Latest** (https://mariadb.org/mariadb/all-releases/): 11.4.14 (2026-11-16), **11.8.10** (2026-11-16, the newer LTS), 12.3.4 (2026-11-16) and 13.1.2 (2026-11-23).
- **Changes after 11.4 that matter here:**
  - 11.7 added the **derived table column list** `AS t(a, b)`. https://mariadb.com/docs/server/reference/sql-statements/data-manipulation/selecting-data/subqueries/subqueries-in-a-from-clause-derived-tables: "MariaDB starting with 11.7.0 ... It is possible to assign column names in the derived table name syntax element."
  - 12.1 added Oracle's `(+)` outer join (search result on https://mariadb.com/docs/release-notes/community-server/release-notes-mariadb-12.1-rolling-releases/changes-and-improvements-in-mariadb-12.1). It is not useful here.
  - Nothing else found for LATERAL, FULL JOIN or correlated derived tables (below).
- **Features in 11.4:**
  - CTE and recursive CTE (10.2), window functions (10.2), `VALUES` table value constructor (10.3.3); versions not re-fetched.
  - `JSON_TABLE` ("available from MariaDB 10.6"; its expression "may have references to columns of other tables ... that precede this JSON_TABLE", https://mariadb.com/kb/en/json_table/).
  - `JSON_ARRAYAGG([DISTINCT] expr [ORDER BY ..] [LIMIT ..])` (https://mariadb.com/kb/en/json_arrayagg/), already used.
  - The SEQUENCE engine (`seq_1_to_N`, "installed by default", https://mariadb.com/kb/en/sequence-storage-engine/). Its bounds are part of the table's name, so it serves only constant ranges. Whether the trimmed bintar in `tools/mariadb` still carries it is **not verified**: the fetch deletes "storage-engine plugins".

### 2. Claims (MariaDb.java javadoc, lines 9-37, and its hooks)

| Claim | Verdict | Evidence |
|---|---|---|
| No LATERAL | TRUE (also in 13.x) | MDEV-19078 "Support lateral derived tables": Status Open, Fix Version 14.1, https://jira.mariadb.org/browse/MDEV-19078 |
| An outer reference in a derived table or recursive CTE fails ("Unknown table") | TRUE | MDEV-19077 "Support derived tables / CTE with external references": Open, no fix version, https://jira.mariadb.org/browse/MDEV-19077 |
| A table function's argument correlates | TRUE | "The expression may have references to columns of other tables" (JSON_TABLE page above) |
| No FULL OUTER JOIN | TRUE | MDEV-13648 "Add FULL OUTER JOIN": Stalled, Fix Version 14.1, https://jira.mariadb.org/browse/MDEV-13648 |
| No VALUES alias list | **OUTDATED from 11.7** | See the 11.7 column list above. TRUE on our pinned 11.4. |
| No QUALIFY, NULLS FIRST/LAST, FILTER, IS DISTINCT FROM | not verified | Not found in the 12.0-12.2 release notes searched; no vendor page states it either way. |
| No array, struct or map type | not verified by fetch | No vendor page read. |
| A SELECT raises no error of its own text; SIGNAL only in a stored program | not re-verified | This is the dialect's measurement. |
| Regular expressions are PCRE; base64, MD5, SHA-1 and SHA-2 are native | not re-verified | — |

### 3. Where the SQL blows up

| Family | MariaDB | DuckDB | Ratio | Excess |
|---|---|---|---|---|
| let | 347,582 | 172,513 | 2.0x | 175,069 |
| removeDuplicates (with function) | 108,902 | 8,644 | 12.6x | 100,258 |
| maxBy (with a count) | 94,598 | 375 | 252x | 94,223 |
| minBy (with a count) | 92,998 | 363 | 256x | 92,635 |
| TypedSort | 109,265 | 18,064 | 6.0x | 91,201 |
| TypedExtendWindow | 189,319 | 112,474 | 1.7x | 76,845 |
| regexpExtract | 32,274 | 566 | 57x | 31,708 |
| range | 32,122 | 1,828 | 17.6x | 30,294 |
| sort (with key function) | 26,630 | 471 | 57x | 26,159 |
| adjust | 29,042 | 5,327 | 5.5x | 23,715 |

MariaDB has the same causes A, C and variant doubling as MySQL. It is smaller only because `JSON_ARRAYAGG` takes ORDER BY, so there is no GROUP_CONCAT wrapping, and there is no LATERAL wrapper.

**Cause A on MariaDB.** A derived table cannot read an outer row (TRUE), so the SQLite and MySQL form works only when X reads no row. The dialect already knows that case (`closed`, used for folds). Three shorter forms, in order of safety:

1. **When X reads no row** (every literal-rooted chain in channel_a), bind it in a derived table:

   ```sql
   (SELECT CASE WHEN l.j IS NULL THEN NULL
                ELSE coalesce((SELECT JSON_ARRAYAGG(.. ORDER BY ..) FROM JSON_TABLE(l.j, ..) AS e), JSON_ARRAY()) END
    FROM (SELECT X AS j) AS l)
   ```

2. **When X reads a row**, bind it through a one-row table function, whose argument may correlate:

   ```sql
   (SELECT CASE WHEN b.j IS NULL THEN NULL ELSE coalesce((SELECT .. FROM JSON_TABLE(b.j, ..) e), JSON_ARRAY()) END
    FROM JSON_TABLE(JSON_ARRAY(X), '$[0]' COLUMNS (j JSON PATH '$')) AS b)
   ```

   This is **not verified**: MariaDB could not be reached. Whether a `JSON` PATH column returns a JSON null as SQL NULL must be measured on the lane.

3. **Drop the guard where X is provably not NULL.** A literal array, a range, and a list step over a non-NULL list are never NULL. This is a structural check on the MIR, not type inference.

Saving: most of the 596K in the 72 nested statements, roughly 350K (estimate).

**Cause C on MariaDB.** `JSON_VALUE(X, CONCAT('$[', ("__u0"."key" - 1), ']'))` comes `FROM JSON_TABLE(X, ..)`. Read the JSON_TABLE's typed column instead, as for MySQL. Saving: about 230K over 59 statements.

**Redundant `JSON_COMPACT` around `JSON_OBJECT`/`JSON_ARRAY`.** There are 1,191 such wraps. Inside another JSON function, MariaDB's own JSON function results are not re-quoted. **Not verified** by a probe here; measure it on the lane.

**adjust and parseDate** (21x). `utcOf` writes the text 6 times or more (`LIKE`, `REGEXP` twice, `RIGHT`, `LEFT`, CAST). Bind it once in a derived table where it reads no row, or test the shape with one `REGEXP` capturing the offset.

### 4. MariaDB's three biggest wins

1. **The NULL guard written once**: forms 1-3 above.
   - Uses `JSON_TABLE` correlation (vendor doc) and `closed` derived tables (the dialect's own measurement).
   - Saving: about 350K.
   - Risk: form 2 needs a lane probe for JSON null to SQL NULL. Form 3 must not drop the guard where an outer join or a JSON null can make a list NULL.
2. **The root explosion reads the typed column.**
   - Saving: about 230K.
   - Risk: a typed PATH column's behaviour on a value of the wrong type (`NULL ON ERROR` by default in MariaDB: not verified). Keep the index read for a Variant list.
3. **Move the pin to 11.8 LTS.** That gives the derived column list (11.7), so a TDS literal becomes `(VALUES (..),(..)) AS t(a,b)` instead of `SELECT .. AS a, .. UNION ALL SELECT ..`. There are several such sources in TypedExtendWindow, TypedSort and let.
   - This overturns the "no VALUES alias list" claim (OUTDATED from 11.7).
   - Saving: about 10 characters per row, small.
   - Risk: the pin's stated reason ("the older of the two long-term releases ... so nothing newer creeps into the dialect") is a policy choice for the owner, not a technical one.

### MariaDB: lists in the wrong order (the lane's flaky failures)

**What the lane saw.** On MariaDB, test bodies that return or compare lists fail in a set that changes from run to run on one commit (38, 23 and 14 failures on one commit; 17, 15 and 27 on another). Each failure is a list in rotated or swapped order: `match::testMatchWithFunctions` expected [1, 2] and got [2, 1]; `init::testInit` and `reverse::testReverse` fail the same way. MySQL does not show it.

**What I checked.** I scanned the channel_a SQL that MariaDB and MySQL receive for every construct whose order is assumed rather than asked for. I could not run MariaDB here, so the cause below is the strongest match between the SQL and the failing tests, not a reproduction.

| Construct in our SQL | MariaDB / MySQL count (channel_a) | What MariaDB's documentation guarantees |
|---|---|---|
| **A root `UNION ALL` of literal rows with no `ORDER BY`.** A Pure literal list, or a concatenate, is lowered as `SqlUnion` (`Lowerer.union` around Lowerer.java:1396 for concatenates; the root list literal takes the same shape). For example `SELECT 1 AS "value" UNION ALL SELECT 2 AS "value"`. | **37 / 37**, the same text on both | No order without an ORDER BY. The UNION page says only that the global ORDER BY "affect[s] the whole result set", and that ORDER BY inside one SELECT "does not affect the order of the UNION" (https://mariadb.com/kb/en/union/). The FAQ: "A 'table' (and subquery in the FROM clause too) is - according to the SQL standard - an unordered set of rows" (https://mariadb.com/kb/en/why-is-order-by-in-a-from-subquery-ignored/). |
| `ORDER BY` inside a derived table, with no LIMIT, assumed to hold outside it | not counted | Ignored by design: "This is not a bug"; the rows are "in some unspecified and undefined order" (the same FAQ). |
| `JSON_ARRAYAGG(x ORDER BY o)`: MariaDB's ordered list | 1,433 / 1 (MySQL uses `GROUP_CONCAT(.. ORDER BY ..)` instead) | Ordered: the syntax takes ORDER BY (https://mariadb.com/kb/en/json_arrayagg/), and "JSON_ARRAYAGG() is implemented on top of GROUP_CONCAT()", where "ORDER BY determines the order of returned values" (https://mariadb.com/docs/server/reference/sql-functions/aggregate-functions/group_concat). Known bugs: [MDEV-21914](https://jira.mariadb.org/browse/MDEV-21914) (ORDER BY ignored, fixed 10.5.4), [MDEV-22840](https://jira.mariadb.org/browse/MDEV-22840) (NULLs with ORDER BY, fixed 10.5.4), [MDEV-39817](https://jira.mariadb.org/browse/MDEV-39817) (an off-by-one gives garbage, fixed in 11.4.13, our pin). Open bugs [MDEV-40606](https://jira.mariadb.org/browse/MDEV-40606), [MDEV-40696](https://jira.mariadb.org/browse/MDEV-40696) and [MDEV-40700](https://jira.mariadb.org/browse/MDEV-40700) concern character set, length and validity, not order. No order bug found. |
| `JSON_ARRAYAGG(x)` with no ORDER BY | 4 / 4 (groupBy plus rowMapper, then maxBy) | No order is stated. |
| `GROUP_CONCAT(..)` with no ORDER BY | 12 / 12 (string joins in a groupBy) | No order is stated (same GROUP_CONCAT page). |
| `JSON_TABLE .. FOR ORDINALITY` as the element position | used everywhere, read back with `ORDER BY "key"` | "Counts the rows, starting from 1". "In the current code, evaluation of JSON_TABLE is deterministic, that is, for a given input string JSON_TABLE will always produce the same set of rows in the same order", but this is not promised for the future (https://mariadb.com/kb/en/json_table/). Safe today. |
| A recursive CTE's last row (`ORDER BY "__i" DESC LIMIT 1`) | used by folds | Ordered explicitly: safe. |
| `ROW_NUMBER() OVER ()` | 0 in product SQL (the judges use it, in `SideOrdinal`) | No order. |

**The likely cause is the first row.** The 37 statements that end in an unordered `UNION ALL` are exactly the failing kind of test. They include `'1'->match([.., a: String[1]|[1, 2], ..])`, which is `SELECT 1 .. UNION ALL SELECT 2`; `['a','b','c']->init()`; `tail`, `take`, `drop`, `slice`, `concatenate`, `zip` and `fold` over literals; and the other `match` tests. The full list is in the probe output below. The judges already know MariaDB reorders such unions: `SideOrdinal`'s javadoc says "MariaDB and Trino return a sorted CTE, and a literal's UNION ALL, in another order" (measured 2026-10-20). They number union branches (`SideOrdinal.branchOrder`, `inBranchOrder`) only where each branch can carry an ordinal, and the product statement itself carries none. MySQL sends the identical text and keeps the order, but it promises no more than MariaDB does.

Two things this does not explain:
- **The run-to-run variation.** The SQL text is the same on every run of a commit, so the variation must come from the server's plan or execution choices (statistics, memory, temporary-table handling). That is consistent with an order the SQL never asked for; it would not appear if the SQL had asked.
- **`testReverse` of a literal** is not a root union: its SQL orders by `"__u0"."key"`. Its failure may come from a body form outside channel_a, or from the judge's side read. **Not verified.**

**The ordered form, and its cost.** Give each branch its index and order by it:

```sql
SELECT 1 AS "value", 1 AS "__o" UNION ALL SELECT 2, 2 ORDER BY 2
```

That is about 25 characters per statement plus about 4 per element. The writer must skip `__o`; the lowering's `UnionSerialOrder` already adds such a column to unions under an envelope. Alternatively, write a literal list as one ordered explosion:

```sql
SELECT "v" AS "value" FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS ("o" FOR ORDINALITY, "v" BIGINT PATH '$')) AS t ORDER BY "o"
```

That is about 100 characters fixed, and shorter than the union from about four elements. Over the 37 statements either form costs about 1-2K characters on MariaDB, and the same on MySQL and SQLite, which send the same unordered union (SQLite's is in the `SQLite` rows of the dump).

Replacing `JSON_ARRAYAGG(.. ORDER BY ..)` with MySQL's GROUP_CONCAT form is not needed by the evidence: the documentation and the bug tracker give no order failure for it. That swap would cost about 80 characters per aggregate, about 115K in all.

Probe output, the 37 root unions with no ORDER BY, by first characters of the query (MariaDB):
`let branche ..`, `['Smith', 'Branche', 'Doe']->filter(..)` (3), `let roe ..` (2), `let source = #TDS ..`, two `#TDS val, str ..`, `#TDS id, name ..`, `[1, 2, 3]->concatenate([4, 5])`, `[^CO_Address ..]`, `[1, 2, 3]->drop(1)` and `drop(0)`, `let s = [^CO_Firm ..]`, three `fold`, `['a', 'b', 'c']->init()`, two `slice`, `tail`, two `take`, three `zip`, three `let x/y = [^Pair ..]`, and seven `match` (including `'1'->match([.., [1, 2], ..])`).

---

## MySql.java against MariaDb.java: every difference, and whether it is still real

| Difference (MySql.java) | Still real? |
|---|---|
| `sourceUrl`: the payload is read by index from one derived row, not by a `JSON PATH '$'` column | A speed choice (342 ms against 586 ms measured), not a capability. Either form works on both. |
| `passes()` adds `WindowsComputedBelow` | Measured MySQL behaviour (a subquery over a window call is evaluated on every row); not re-verified. SQLite would gain from the same pass for size. |
| `stringLit` writes `'` and `\` as `CHAR(..)` | Measured on 8.4; **not verified on 9.7**. It applies to every literal, where the bug is in a CTE another CTE reads. Limiting it to those would save about 40K. |
| `sessionSetup`: `group_concat_max_len`, `cte_max_recursion_depth` | Real: GROUP_CONCAT defaults to 1024 bytes (9.7 docs). |
| `compact` = `CAST(.. AS JSON)` | Real: there is no `JSON_COMPACT` (9.7 function list). Redundant on a direct JSON-function result. |
| `arrayAgg`: GROUP_CONCAT for ordered or distinct | Real: `JSON_ARRAYAGG` takes no ORDER BY or DISTINCT (9.7 docs). `SEPARATOR ','` is redundant. |
| `indexPath`: `$[last-n]` | Not re-verified (the dialect's measurement). |
| `jsonText`/`jsonMember`: `JSON_UNQUOTE(NULLIF(JSON_EXTRACT(..), CAST('null' AS JSON)))` | Real: `JSON_VALUE` needs a literal path (9.7 docs). There are 2,456 `CAST('null' AS JSON)`; no shorter JSON null spelling is verified. |
| `twoRows` (raise) as a UNION | The dialect's measurement. |
| `format` without SFORMAT | Real: MySQL has no SFORMAT (a MariaDB function); not re-fetched. |
| `hugeIntLiteral`, `yearOf`, `jsonAsText` (spaces after `,` and `:`), `booleanColumn` INT | The dialect's measurements; MySQL stores JSON parsed, consistent with the docs. Not re-fetched. |
| No PERCENTILE_CONT or MEDIAN (`windowQuantilesNative` false) | Not in MySQL's aggregate list (9.7 page). |
| `jsonTable` wraps every argument in LATERAL | Only needed for a subquery argument (9.7 JSON_TABLE docs allow a function call). |
| `jsonIsBinary`, `correlatesDerived` true, `groupRef` `$n`, `regexpReplace` with occurrence | Real (ICU regex; the derived-table docs). `correlatesDerived` is **true but not used by the NULL guard**: cause A. |
| `floatToText` above 1e60 | The dialect's measurement (FORMAT writes at most 65 digits). |

No difference was found that is false today. The waste lies in capabilities MySQL has and the dialect does not use: correlated derived tables in `unlessNull`, bare JSON_TABLE arguments, and the default separator.
