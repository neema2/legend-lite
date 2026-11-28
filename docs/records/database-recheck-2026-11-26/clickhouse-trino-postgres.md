# ClickHouse, Trino and Postgres: where the SQL blows up, checked against the vendors

Read-only research on `/home/user/ll-next` at `52f62b34`. No repository file was edited and Bazel was not run.

## Summary: the biggest wins, ranked

1. **ClickHouse: move the SETTINGS clause onto the connection.** It is 303 characters on each of 2,258 statements: 683,871 characters, which is 38.5% of ClickHouse's 1.77M. Over HTTP a setting can ride the URL or an HTTP session (probed on 26.3; the vendor documents both). Lite's own test harness already does this for `max_query_size` and `session_id`. The javadoc's "a SET does not outlive its statement" is **false**.
2. **ClickHouse and Trino: type each literal once, not each cell.** The numbers are measured; the coercions behind them were probed.
   - ClickHouse writes `toInt64(n)` 15,242 times (137K characters). 10,212 of them sit inside `values('… Nullable(Int64) …', …)`, whose declared structure already types the cell (probed), so about 92K characters go.
   - Trino writes `BIGINT 'n'` 15,379 times (138K characters). In a VALUES column or an ARRAY, one BIGINT element makes the whole column or array BIGINT (probed on 483). About 10,000 of those cells could be bare, which saves roughly 80-100K characters.
3. **Postgres: stop writing a list twice per operation.** Every list operation is `(CASE WHEN xs IS NULL THEN NULL ELSE ARRAY(SELECT … FROM unnest(xs) …) END)`, so `xs` appears twice and nesting doubles the text at each level (maxBy: 4,566 characters against DuckDB's 211). `ARRAY(subquery)` is never NULL (probed on 16.15), so the guard can go wherever `xs` is an ARRAY constructor or literal. A crude simulation of that one fold saves 56K characters (6%). Binding a computed list once, with a scalar subquery or a LATERAL, should save much more.
4. **ClickHouse: one setting is dead in the latest releases, and that is a correctness risk.** `query_plan_use_new_logical_join_step = 0` is OBSOLETE from 26.5: "the logical join step is now always used" (vendor source). On an upgrade to 26.8 LTS or 26.11 it silently stops protecting the guarded raises it was added for.
5. **Trino: the date families use far longer forms than Trino needs.**
   - dateDiff and timeBucket get epoch microseconds by cutting the timestamp's VARCHAR text, about 1,100 characters each.
   - `date_diff('millisecond', a, b)` counts boundaries the way DuckDB does (probed). For day and larger units, `date_diff(unit, date_trunc(unit, a), date_trunc(unit, b))` does the same.
   - About 50-60K characters.
6. **ClickHouse: `if(empty(L), NULL, arrayStringConcat(…L…))` writes the list twice.** `arrayReduce('groupConcatOrNull(sep)', L)` has the same answer (NULLs skipped, NULL over none, `['']` gives `''`) with the list written once (probed). joinStrings nests, so the saving compounds.
7. **Postgres: `contains` over a mixed list is unrolled one CASE per element** (1,756 characters against 259). A multi-argument `unnest(a, b)` in an `EXISTS` writes each array once.
8. **Trino 483 has PIVOT** (the version we run). The javadoc says "no PIVOT", so the claim is false as of the pinned version. Postgres 19 (released 2026-11-19) has IGNORE NULLS on lead, lag, first_value, last_value and nth_value.
9. **Quoting every identifier** costs 3.4-4.5% on all three (36-41K characters each). It is small and safe to cut only for names that need no quotes. It is low priority.
10. **Versions.**
    - ClickHouse: we run 26.3.60.2 LTS. The latest are 26.11.3.10-stable and 26.8.48.11-lts. The dialect javadoc still says 25.8.
    - Trino: we run 483. The latest is 484.
    - Postgres: we run 16.15. The latest are 19.0 and 16.16. Postgres 18 added `array_sort` and `array_reverse`, which would shorten every list sort and reverse.

How I measured: I planned the same `channel_a.jsonl` queries with the product's one planning entry, using the classes in the Bazel output tree's `lean_sql_baseline_test` runfiles (a small Java driver in `scratchpad/ctp-dump/`, no Bazel run). The totals match `baseline.tsv` exactly: ClickHouse 1,774,195, Trino 919,969, Postgres 957,199, DuckDB 529,909.

I probed real servers, started from the pinned binaries in the Bazel output tree: ClickHouse 26.3.60.2 (`clickhouse local` and its HTTP server), Trino 483 (its memory connector) and Postgres 16.15. All three are stopped now. Every "probed" below is one of these.

---

## 1. ClickHouse

### 1.1 Version

- **We run 26.3.60.2 (LTS).** Source: `tools/clickhouse/clickhouse.bzl:11` (`VERSION = "26.3.60.2"`) and `select version()` on the server.
- **The dialect javadoc is stale.** It says 25.8 (`ClickHouse.java:21`, "ClickHouse 25.8, its Memory engine").
- **JDBC driver:** `com.clickhouse:clickhouse-jdbc:0.10.0:all-dependencies` (`tools/deps/jars_table.bzl:119-121`).
- **Latest release:** v26.11.3.10-stable, v26.9.40.9-stable (marked "Latest") and v26.8.48.11-lts, all dated 26 Nov 2026 (https://github.com/ClickHouse/ClickHouse/releases).

What changed between 26.3 and 26.11 that matters here. Each item is from the vendor's `src/Core/Settings.cpp` at tag v26.11.3.10-stable (https://raw.githubusercontent.com/ClickHouse/ClickHouse/v26.11.3.10-stable/src/Core/Settings.cpp):

- `query_plan_use_new_logical_join_step` is OBSOLETE from 26.5: `{"26.5", true, true, "Obsolete setting, the logical join step is now always used."}`. Lite sets it to 0 to work around a 26.3 join planner that ran a guarded raise. On 26.5+ that line does nothing, so the workaround is gone. Re-measure those raise tests before any upgrade.
- `allow_experimental_correlated_subqueries` became an alias of `allow_correlated_subqueries` in 26.9 (Beta since 25.8). The old name still works.
- `precise_float_parsing` became default true in 26.7. See 1.6.

### 1.2 The SETTINGS clause (the known item, checked)

**Size.** `SETTINGS` plus `UNCORRELATED` is 303 characters. That is 683,871 characters over 2,258 statements, 38.5% of all ClickHouse SQL. Without it ClickHouse writes 1.09M characters (2.06x DuckDB).

**How lite connects.**
- The product opens no ClickHouse connection itself: `core/src/main/java/com/legend/exec/ClickHouseConnector.java` (`opening` returns `notOpened`), so sessions are the harness's.
- The harness URL is `jdbc:clickhouse://127.0.0.1:<port>/<db>?user=default&clickhouse_setting_max_query_size=16777216&clickhouse_setting_max_ast_elements=1000000&clickhouse_setting_max_expanded_ast_elements=10000000`. A session URL adds `clickhouse_setting_session_id=…&clickhouse_setting_session_timeout=3600` (`testing/src/main/java/com/legend/testing/EmbeddedClickHouse.java:73-96`).
- That javadoc itself says "The driver passes a `clickhouse_setting_` property as an HTTP parameter, a server setting."

**What the vendor says:**

| Mechanism | Vendor text | URL | Rides the connection once? |
|---|---|---|---|
| URL parameters | "You can also use the URL parameters to specify any settings for processing a single query or entire profiles of settings." | https://clickhouse.com/docs/interfaces/http | Yes: each HTTP request of the connection carries them, and no SQL text does |
| JDBC v2 server settings | "All server settings should be prefixed with `clickhouse_setting_`" (V2, 0.8+; helper `ClientConfigProperties.serverSetting(...)`) | https://clickhouse.com/docs/integrations/language-clients/java/jdbc | Yes. This is what lite's harness already uses. The page shows 0.9.8 in its snippets; 0.10.0 behaves the same per lite's own measurement (EmbeddedClickHouse javadoc). Not re-verified on the jar here. |
| `custom_settings` (V1 property) | Not mentioned for V2 on that page | same | Not verified for 0.10.0 |
| HTTP session | "you need to add the `session_id` GET parameter to the request"; "the session is terminated after 60 seconds of inactivity"; "Only one query at a time can be executed within a single session." | https://clickhouse.com/docs/interfaces/http | Yes, but stateful. A `SET` "Assigns value to the param setting for the current session" (https://clickhouse.com/docs/sql-reference/statements/set). |
| Settings profile | the `profile=` URL parameter | https://clickhouse.com/docs/interfaces/http | Yes, but it needs server configuration lite does not own on a customer's server |

**Probed on 26.3 over HTTP** (`curl` to the server's port):

```
plain:                        select r.b from (select 1 a) l left join (select 2 a, 5 b) r on l.a=r.a   -> 0
?join_use_nulls=1:            same query                                                              -> \N
?session_id=s1, SET first:    SET join_use_nulls=1, then the query in the same session               -> \N
SET of all eight, session s2: select 1 union select 1 -> 1;  select timezone(), getSetting('cast_keep_nullable') -> UTC, true
?union_default_mode=DISTINCT&session_timezone=UTC: select 1 union select 1 -> 1
?join_use_nulls=1 plus "SETTINGS join_use_nulls=0" in the statement: -> 0   (the statement's clause wins)
```

So the javadoc claim "The JDBC driver speaks HTTP with no session (a `SET` does not outlive its statement)" is **false**. It is true only of an HTTP request made with no `session_id`, and lite's own harness opens sessions.

**Keeping the plan "SQL and data only".** The machinery already exists for H2's settings:
- The plan's target declares `DatabaseSettings(required, readBack)` (`SqlDialect.java:88-99`).
- Exec's `JdbcStatements.check` writes them where the platform opened the session, and otherwise sends the target's own read-back statement and refuses the session on any row (`JdbcStatements.java:221-270`).
- `ClickHouse.java:190` returns `null` today ("none the database must have that a session statement cannot set").

The shape that keeps G4:
- The target declares the eight settings as data: names and values.
- Where exec opens the connection, it passes them as `clickhouse_setting_<name>` connection properties. That is not SQL and not an edit of any statement.
- Where the session is borrowed, the target's declared read-back runs. The database compares and exec carries the texts unread. Probed form:
  ```sql
  SELECT name FROM system.settings
  WHERE name IN ('join_use_nulls','session_timezone', …)
    AND (name, value) NOT IN (('join_use_nulls','1'),('session_timezone','UTC'), …)
  ```
  It answers no row when all hold. I probed it with two settings on the URL; it returned nothing.

Two cautions:
- The settings also apply to the session's DDL and INSERT seeds (for example `session_timezone` when a DateTime is parsed). That is probably what is wanted, but it is untested.
- `allow_experimental_correlated_subqueries` is chosen per statement: on only where every EXISTS is null-rejecting. Keep that one on the statement, and only where it is 1. Making the connection default 0 removes the 47-character `= 0` from nearly every statement.

**Which settings 26.3 and 26.11 still need.** Defaults read from `system.settings` on 26.3.60.2 and from `Settings.cpp` at v26.11.3.10:

| Setting lite writes | 26.3 default | 26.11 default | Still needed? |
|---|---|---|---|
| `join_use_nulls = 1` | 0 | false | Yes |
| `union_default_mode = 'DISTINCT'` | '' (a bare UNION is refused, probed: "Expected ALL or DISTINCT") | Unspecified | Yes, or write `UNION DISTINCT` (9 characters per UNION rather than 32 per statement) |
| `cast_keep_nullable = 1` | 0 | false | Yes |
| `session_timezone = 'UTC'` | '' | "" | Yes (or a server timezone of UTC) |
| `query_plan_merge_expressions = 0` | 1 | true | Yes, by lite's raise-guard measurement |
| `query_plan_filter_push_down = 0` | 1 | true | Yes, by lite's measurement |
| `optimize_functions_to_subcolumns = 0` | 1 | true | Yes, by lite's measurement |
| `query_plan_use_new_logical_join_step = 0` | 1 | **obsolete from 26.5, no effect** | Needed on 26.3; useless from 26.5 (see 1.1) |
| `allow_experimental_correlated_subqueries = 0/1` | 1 (Beta) | alias of `allow_correlated_subqueries`, true | Yes. The NULL bug is still there on 26.3, see the claims table. |

### 1.3 Claims in ClickHouse.java, checked

| Claim (line) | Verdict | Evidence |
|---|---|---|
| "The JDBC driver speaks HTTP with no session (a SET does not outlive its statement)" (31-32) | **FALSE** | Probed: `session_id` keeps a SET and URL parameters set settings. The vendor HTTP and SET pages are quoted above. |
| "ClickHouse 25.8" (21) | **OUTDATED** | 26.3.60.2 is pinned (`clickhouse.bzl:11`) |
| a list is never NULL: `Nullable(Array)` is refused (35) | TRUE | Probed: "Nested type Array(Int64) cannot be inside Nullable type" |
| no raise with a computed message: `throwIf` takes a constant one (43) | TRUE | Probed: "Argument at index 1 for function throwIf must be constant" |
| `aggregate_functions_null_for_empty` makes `groupArray` a nullable array, refused (65-66) | Not reproduced in the simple case | Probed: `groupArray` over no row with the setting gives `[]`. The refusal may need a grouped context; not verified. |
| correlated EXISTS is wrong for a NULL correlated column (86-91) | TRUE on 26.3 | Probed: `exists(… where (o.v IS NULL OR true) …)` gives 0 for `v = NULL`. 26.11 not verified. |
| no LATERAL join (255, 344, 417) | TRUE on 26.3 | Probed: `, lateral (…)` and `cross join lateral` are syntax errors |
| a correlated subquery refuses a sort; correlated IN is refused (344, 1114) | TRUE (IN) | Probed: "Correlated subqueries are not supported as IN function arguments yet". The sort case was not isolated. |
| no `VALUES … AS t(cols)` (540) | TRUE | Probed: a syntax error |
| a map's key is never nullable (947) | TRUE | Probed: "Map cannot have a key of type Nullable(…)" |
| `splitByString` takes no Nullable text (1660) | TRUE | Probed: "Nested type Array(String) cannot be inside Nullable type" |
| `printf` takes a native number or String, not a Decimal (2103) | TRUE | Probed: "Expected: Native number or String, got: Decimal(18, 2)" |
| `moduloOrZero` takes no Decimal (2027) | TRUE | Probed: "Illegal types Decimal(18, 1)…" |
| "an exact quantile takes no Decimal" (1465) | TRUE for `quantileExactInclusive`, FALSE for `quantileExact` | Probed: `quantileExactInclusive` refuses Decimal; `quantileExact(0.5)` over Decimal answers 1 |
| abs of an Int64 is a UInt64 (2000) | TRUE | Probed: `toTypeName(abs(toInt64(-1)))` gives UInt64 |
| "There is no supertype for types Decimal(2, 1), Float64" (1009, 1272) | **OUTDATED on 26.3** | Probed: `[toDecimal64(1.5,1), toFloat64(2)]` is `Array(Variant(Decimal(18, 1), Float64))` and `if()` gives a Variant, because `use_variant_as_common_type` is true. It no longer errors, but a Variant is not what lite wants, so the unify step is still right. |
| "ClickHouse's Time type is experimental (25.6, behind a setting)" (206) | **OUTDATED** | Probed: on 26.3 `enable_time_time64_type` defaults to 1, tier Production |
| "today() reached ClickHouse, which has no bare current_date" (1998) | **FALSE as worded** | Probed: `select current_date, today(), current_date()` all answer. If the refusal is about the session zone, the message should say so. |
| "ClickHouse has no JSON merge patch" (2139) | **FALSE as worded** | Probed: `jsonMergePatch` exists. It is not RFC 7396: `jsonMergePatch('{"a":1,…}','{"a":null,…}')` gives `{"a":null,…}`, so a null does not delete. The refusal is right; its reason is wrong. |
| "ClickHouse has no JSON pretty-printer" (2138) | TRUE on 26.3 | Probed: no function name containing "pretty" in `system.functions` |
| a TEMPORARY table lives only in an HTTP session, which the plan cannot name (148) | TRUE that temporary tables need a session (vendor HTTP page: sessions). "The plan cannot name it" is lite's design, not the database's. | |
| an integer literal is typed by magnitude: 1 is UInt8 (37-38) | TRUE | Probed: `toTypeName(1+1)` gives UInt16, `toTypeName(toInt64(1)+1)` gives Int64 |
| the driver refuses autocommit off (`ClickHouseConnector.java:47`) | Not verified (needs the JDBC jar) | |

### 1.4 Where the SQL blows up (SETTINGS removed)

With the SETTINGS clause stripped, ranked by excess characters over DuckDB:

| Family | ClickHouse | DuckDB | Ratio |
|---|---|---|---|
| let | 344,786 | 172,995 | 1.99 |
| TypedExtendWindow | 224,914 | 112,676 | 2.00 |
| date::adjust | 34,921 | 5,327 | 6.56 |
| date::timeBucket (DateTime) | 40,805 | 13,309 | 3.07 |
| TypedGroupBy | 38,463 | 13,617 | 2.82 |
| date::dateDiff | 30,503 | 11,104 | 2.75 |
| removeDuplicates | 18,395 | 8,684 | 2.12 |
| date::timeBucket (StrictDate) | 11,936 | 3,408 | 3.50 |
| TypedFilter | 18,160 | 9,902 | 1.83 |
| TypedPivot | 16,883 | 9,615 | 1.76 |

The highest ratios are tanh 13.4x, sinh and cosh 12x, and adjust 11x with SETTINGS.

**Why, by cause, each with a shorter form:**

1. **`toInt64(n)` around every integer literal.** It occurs 15,242 times (137K characters). Code: `ClickHouse.java` literal writing (`toInt64(1)`, javadoc 37-38). Inside `values('`p` Nullable(Int64), …', (toInt64(0), toInt64(1), …))` it is redundant, because the structure string types the cell (probed: `values('p Nullable(Int64), …', (0,'a'), (9223372036854775807,'b'))` gives Nullable(Int64) with the exact values). 10,212 of the wraps are in such rows, so about 92K characters go. Outside VALUES, a literal beside an Int64 operand also widens (`toTypeName(toInt64(1)+1)` is Int64), so the wrap is needed only where both operands are literals.
   ```sql
   -- now
   FROM values('`p` Nullable(Int64), `o` Nullable(Int64)', (toInt64(0), toInt64(1)), (toInt64(100), toInt64(2)))
   -- shorter, same types
   FROM values('`p` Nullable(Int64), `o` Nullable(Int64)', (0, 1), (100, 2))
   ```
2. **A Float cell is `CAST(CAST('1.0' AS Decimal(2, 1)) AS Nullable(Float64))`**, about 50 characters per cell. 412 such cells in values rows come to about 21K characters. In a Float64 column of `values()` the plain literal `1.0` is the correctly rounded double. A numeric literal parses exactly (the coordinator's lane found the text parse off by one ulp; probed in 1.6). Decimal-to-Float64 division is no more exact than that. The plain literal is shorter and at least as exact.
3. **A list written twice to get NULL over an empty list.**
   - `if(empty(present(L)), NULL, arrayStringConcat(arrayMap(toString, present(L)), sep))` is written by `ClickHouse.reduceCollection` (STRING_AGG, line 1516). The same shape is in `listBoolAnd`/`listBoolOr` (1415-1417), the quantile (1582), and lines 2805 and 2838.
   - joinStrings nests inside map and toString, so each level doubles: query 1764 is 3,697 characters against DuckDB's 430.
   - The STRING_AGG shorter form, probed:
     ```sql
     arrayReduce('groupConcatOrNull(\',\')', L)
     ```
     It gives `a,b` for `['a',NULL,'b']`, NULL for `[NULL]` and `[]`, and `''` for `['']`. Use it when the separator is a literal and the elements are String.
   - For the other shapes, bind L once: `arrayElement(arrayMap(f -> if(empty(f), NULL, g(f)), [L]), 1)` (probed: `arrayMap(s -> s[1]+length(s), [arraySort([3,1,2])])[1]` gives 4). A statement-level `WITH <expr> AS s` also works (probed).
   - Where the whole thing is wrapped in `coalesce(…, '')` (as here), the `if(empty…)` is dead, and the fold "fixed by the types" applies.
4. **Percentile writes the sorted list 9 times** (query 2320: 2,463 characters against 98). Same fix: bind it once with the `arrayMap(…, [L])[1]` trick. Note `quantileExactInclusive` interpolates linearly (probed: `arrayReduce('quantileExactInclusive(0.25)', [1,2,3,4])` gives 1.75, PERCENTILE.INC), but it refuses a Decimal.
5. **Date adjust and bucket.**
   - They read the instant several times (`toDateTime64(toDateTime64('…', 6, 'UTC'), 6, 'UTC')` written up to 4 times).
   - adjust adds a range guard that recomputes the sum, with a CASE that raises.
   - Bind the instant once (the arrayMap trick). Drop the double `toDateTime64(toDateTime64(…))`: the inner one already has the type. About 30-40% of these families.
6. **tanh, sinh and cosh as `pow(e(), …)`**: this is the best form. Probed: native `tanh(0.5)` gives 0.46211811616870957 (true value 0.46211715726…), and `exp(1.0)` gives 2.718281828460626 (true value 2.718281828459045). ClickHouse's native transcendental functions are imprecise, so the long form is justified.
7. **mode over a window writes `groupArray(…) OVER (…)` twice** (`modeOf`, 1476). Binding with arrayMap would halve it.
8. **Every identifier quoted:** 36,622 quote characters (3.4%). The `LEXICON` exists. Quote only reserved or case-unsafe names. Low priority.

### 1.5 The three biggest ClickHouse wins

1. **SETTINGS on the connection.**
   - What: declare the eight settings on the plan's target. Exec passes them as `clickhouse_setting_*` properties where it opens the connection, and reads them back where it borrows one. Keep `allow_experimental_correlated_subqueries = 1` on the statement only where needed.
   - Overturns: the javadoc's "SET does not outlive its statement".
   - Evidence: the probes and vendor pages in 1.2.
   - Saving: about 680K characters (38%).
   - Risks: a borrowed session the harness did not configure must be read back, never assumed. The settings also apply to DDL and INSERT. A settings profile with `readonly=1` refuses URL settings: the JDBC page warns about read-only profiles ("We deliberately avoid adding default settings to connection properties to avoid problems with read-only profiles").
2. **No `toInt64` or Decimal-cast wrapping inside a typed `values()`.**
   - Evidence: the probe in 1.4, causes 1 and 2.
   - Saving: about 110K characters.
   - Risk: a cell past Int64 range must still be refused at planning, as now.
3. **Bind a list once** (`arrayMap(f -> …, [L])[1]`) and use `groupConcatOrNull` for joinStrings.
   - Saving: tens of thousands of characters in `let`, joinStrings and percentile; it compounds when they nest.
   - Risk: ClickHouse evaluates every CASE branch inside a lambda (`ClickHouse.java:107-111`), so a raise moved into a lambda must keep its own guard (`raiseGuards`).

### 1.6 Float parsing from text (the coordinator's item)

**What the setting does.** `precise_float_parsing`: "Use the precise float parsing algorithm, which always returns the closest representable value to the input. When disabled, a faster but less accurate algorithm is used that may differ from the precise result by the least significant bits."

**Its default and history.** It became default true in 26.7. The history entry: `{"26.7", false, true, "Use the precise (closest-representable) float parsing algorithm by default, now that it is faster than the previous fast algorithm. Set to false to restore the pre-26.7 fast-but-less-accurate parsing in conversion functions."}`

Sources:
- Vendor docs: https://clickhouse.com/docs/reference/settings/formats/other
- Vendor source: https://raw.githubusercontent.com/ClickHouse/ClickHouse/v26.11.3.10-stable/src/Core/Settings.cpp, line about 1908.

**Its cost.** From 26.7 the vendor says precise is now faster than the old fast path. On 26.3 it is the slower algorithm; the size of the cost was not measured.

**When it was introduced:** before 26.3, since it exists there with default 0. Altinity says it contributed it (https://altinity.com/?p=21974). The exact version was not verified.

Probed on 26.3.60.2 with the text `1.3421E-8`:

| Expression | Setting 0 (the 26.3 default) | Setting 1 |
|---|---|---|
| `toFloat64('…')`, `toFloat64OrNull`, `toFloat64OrZero`, `CAST('…' AS Float64)`, `accurateCastOrNull('…','Float64')` | 1.3421000000000002e-8 (wrong) | 1.3421e-8 (right) |
| `toFloat64(trimBoth(' … '))` (lite's parseFloat form, `ClickHouse.java:1821`) | wrong | right |
| JSON number: `JSONExtract('{"a":1.3421E-8}','a','Nullable(Float64)')`, `JSONExtractFloat` of a number | right | right |
| JSON string holding the number: `JSONExtract('["1.3421E-8"]',1,'Nullable(Float64)')`, `JSONExtractFloat('{"a":"1.3421E-8"}','a')` | wrong | **still wrong** |
| input formats: `format(CSV\|TSV\|JSONEachRow, 'x Float64', '1.3421E-8')` | wrong | **still wrong** |
| the numeric literal `1.3421E-8`, `toFloat64(1.3421E-8)` | right | right |

What lite would have to write for a text parsed at run time to be the correctly rounded double:

1. Add `precise_float_parsing = 1` to the settings lite declares, on the connection (1.2) or in today's SETTINGS clause (+27 characters). That covers `toFloat64` and `CAST` of a String, the OrNull and OrZero forms, and accurateCast, which takes in lite's parseFloat (1821) and any text-to-Float cast. It is harmless on 26.7+, where it is the default.
2. For a JSON value that is a string (lite's `JSONExtract(…, 'Nullable(Float64)')` at 1883 and 1891 when the JSON holds a quoted number), extract the text and convert it under the setting. Probed exact: `toFloat64(JSONExtractString(j,'a'))`, and `toFloat64OrNull(JSONExtractRaw(j,'a'))` for a bare number.
3. For an input format (a CSV or TSV load): read the column as String and convert with `toFloat64` under the setting. Probed exact: `select toFloat64(x) from format(TSV, 'x String', '1.3421E-8')` with the setting gives 1.3421e-8. Whether 26.7+ also fixes the input formats was not verified; the history note says "conversion functions".

---

## 2. Trino

### 2.1 Version

- **We run 483.** Sources: `tools/trino/trino.bzl:13`, the server's `/v1/info` (`"version":"483"`), and the JDBC driver `io.trino:trino-jdbc:483` (`tools/deps/jars_table.bzl:108-110`).
- **Latest release:** 484, 15 Oct 2026 (https://trino.io/docs/current/release.html).

Changes that matter:
- **483 added PIVOT**: "Add support for the PIVOT clause to turn row values into columns" (https://trino.io/docs/current/release/release-483.html; syntax at https://trino.io/docs/current/sql/pivot.html). Probed: `… PIVOT (sum(v) FOR c IN ('x','y'))` answers.
- **484** adds `json_scalar()`, `JSON_SERIALIZE`, interval precision, and `EXCLUDE` in window frames (https://trino.io/docs/current/release/release-484.html). None of these shortens our forms much.
- **484 breaking change:** "Day-time interval maximum is reduced to about 292,000 years". Check the date guards on an upgrade.

### 2.2 Claims in Trino.java, checked (probed on 483 unless noted)

| Claim | Verdict | Evidence |
|---|---|---|
| no QUALIFY | TRUE | Probed: "mismatched input 'QUALIFY'" |
| no `SELECT * EXCEPT` | TRUE | Probed: a syntax error |
| **no PIVOT** (24) | **FALSE on 483** | Probed and in the 483 release notes. Note: in the probe, `SELECT *` over the PIVOT dropped the non-pivot column `k` (answered `[[4, 2]]`). Read https://trino.io/docs/current/sql/pivot.html for its grouping before using it. |
| a bare integer literal is a 32-bit INTEGER whose arithmetic raises at 2^31 (28-30) | TRUE | Probed: `2147483647 + 1` raises "integer addition overflow"; `BIGINT '2147483647' + 1` gives 2147483648 |
| … "so every integer literal is a BIGINT" | **Not needed everywhere** | Probed: `VALUES (BIGINT '0'), (1), (2)` makes the column bigint; `VALUES (CAST(NULL AS BIGINT)), (1)` gives bigint; `ARRAY[BIGINT '1', 2, 3]` is `array(bigint)`; `cardinality(…) - 1 + 2147483647` is bigint with no overflow. One typed operand suffices. |
| a lambda takes no element index (33) | TRUE | Probed: "Expected a lambda that takes 1 argument(s) but got 2" |
| no exact percentile, only `approx_percentile` (38) | TRUE | Probed: `SHOW FUNCTIONS LIKE '%percentile%'` lists only `approx_percentile` |
| no Jaro-Winkler (1377) | TRUE | Probed: no function like '%jaro%' |
| date_add and date_trunc have no microsecond unit (1109, 1203, 1505) | TRUE | Probed: "'microsecond' is not a valid TIMESTAMP field" |
| "a nanosecond epoch has no Trino spelling" (EPOCH_NS and FROM_EPOCH_NS, 1121) | **Half FALSE** | Probed: `from_unixtime_nanos(1600000000123456789)` is `timestamp(9) with time zone`, so FROM_EPOCH_NS has a spelling. EPOCH_NS (to nanoseconds) has none: `to_unixtime` is a double. |
| 'EST' is not a valid time zone (1429) | TRUE | Probed |
| no JSON pretty-printer, no JSON merge patch (1156-1157) | TRUE on 483 as far as probed | `SHOW FUNCTIONS LIKE '%merge%'` shows only digest merges |
| a WITH is written in place at each read (2036) | Not verified | |
| no temporary table (2043) | Not verified for the memory connector | |

### 2.3 Where the SQL blows up

| Family | Trino | DuckDB | Excess | Ratio |
|---|---|---|---|---|
| TypedExtendWindow | 189,007 | 112,676 | +76K | 1.68 |
| let | 229,802 | 172,995 | +57K | 1.33 |
| timeBucket (DateTime) | 69,254 | 13,309 | +56K | 5.20 |
| dateDiff | 30,399 | 11,104 | +19K | 2.74 |
| timeBucket (StrictDate) | 19,968 | 3,408 | +17K | 5.86 |
| TypedGroupBy | 27,888 | 13,617 | +14K | 2.05 |
| format | 17,340 | 5,315 | +12K | 3.26 |
| removeDuplicates | 19,862 | 8,684 | +11K | 2.29 |
| removeDuplicatesBy | 9,259 | 3,064 | +6K | 3.02 |
| jaroWinklerSimilarity | 6,086 | 134 | +6K | 45 |

Causes and shorter forms:

1. **`BIGINT 'n'` on every integer literal.** 15,379 times (138K characters, 15% of Trino's SQL). 10,033 are VALUES cells and about 1,600 more are array elements after the first. Code: the Trino integer-literal arm (javadoc 28-30).
   - Shorter: type one cell per VALUES column, and the first element of an ARRAY. Write a literal bare where its other operand is already BIGINT (a column, `cardinality`, `element_at`'s index).
   - Example, window query 721: 947 characters fall to about 560.
     ```sql
     FROM (VALUES (BIGINT '0', BIGINT '1', BIGINT '10'), (0, 2, 20), (0, 3, 30), …) AS "_tds0"("p", "o", "i")
     ```
   - Probed: the column is bigint.
   - Saving: about 90-110K characters.
   - Risk: a column whose every row is NULL needs `CAST(NULL AS BIGINT)` in one row (probed: it works). A literal-only expression (`1 + 2`) still needs one typed side.
2. **Date epoch from text.**
   - dateDiff and timeBucket compute epoch microseconds as `date_diff('second', epoch, date_trunc('second', ts)) * 1000000 + CAST(substr(CAST(CAST(ts AS TIMESTAMP(9)) AS VARCHAR), -9, 6) AS BIGINT)`, and write it 4 times per call. dateDiff with MILLISECONDS (query 1251) is 1,105 characters against DuckDB's 106. Code: `Trino.micros`/`nanos` (around 1570-1606) and the dateDiff arm.
   - Probes:
     - `date_diff('millisecond', TIMESTAMP '…00.0009', TIMESTAMP '…00.0011')` gives 1, and gives −1 for the reverse before 1970. That is a boundary count, as DuckDB's (exactness on every edge case was not verified).
     - `date_diff('day', '2015-07-07 23:00', '2015-07-08 01:00')` gives 0 and `date_diff('month', '2020-01-31', '2020-02-01')` gives 0. Elapsed units, not boundaries.
     - `date_trunc('millisecond', ts)` exists and keeps `timestamp(9)`.
   - Shorter forms:
     ```sql
     -- MILLISECONDS
     date_diff('millisecond', a, b)
     -- DAY..YEAR: boundaries
     date_diff('day', date_trunc('day', a), date_trunc('day', b))
     -- epoch milliseconds, exact BIGINT (probed: 1706661154999)
     date_diff('millisecond', TIMESTAMP '1970-01-01 00:00:00', ts)
     ```
   - Microsecond and nanosecond units still need the text route, because Trino has no microsecond field. For timeBucket, bind the epoch once with `element_at(transform(ARRAY[<epoch>], v -> <bucket arithmetic on v>), 1)` (probed: `element_at(transform(ARRAY[42], v -> v + v), 1)` gives 84).
   - Saving: about 50-60K characters.
   - Risk: week alignment (timeBucket's 1969-12-29 origin) must stay explicit; zone handling is unchanged.
3. **removeDuplicates.**
   - The `reduce` start state is written as `slice(transform(L, …), 2, greatest(cardinality(transform(L, …)) - 1, 0))` with `element_at(transform(L, …), 1)`, so L is written three times plus each per-element lookup (query 1107: 2,866 against 1,383).
   - Trino has `array_distinct` (in `SHOW FUNCTIONS LIKE 'array%'`) for the plain case. For the keyed case, `reduce(transform(L, x -> ARRAY[x]), CAST(ARRAY[] AS …), …)` starts from the empty array, so L appears once.
   - Also `(CASE WHEN BIGINT '1' = 0 THEN NULL ELSE element_at(x, BIGINT '1') END)` is a constant guard on a literal index. Fold it to `element_at(x, 1)`.
4. **jaroWinkler (45x):** written out because Trino has none (probed). Best possible, unless a UDF is allowed.
5. **format:** `concat_ws('', concat(format_datetime(…), substr(CAST(… AS VARCHAR), -9, 6)))` re-reads the timestamp. Bind once. Smaller gain.
6. **Quoting:** 41,482 quote characters (4.5%). Low priority.

### 2.4 The three biggest Trino wins

1. **One BIGINT per VALUES column and array, bare literals beside BIGINT operands.** About 100K characters (11%). Evidence: the probes in 2.2. Risk: an all-NULL column, and literal-only arithmetic.
2. **Native `date_diff('millisecond', …)` and `date_trunc` boundaries for dateDiff and timeBucket; bind the epoch once.** About 55K characters. Evidence: the probes in 2.3. Risk: run the existing edge-case suite (the 15 cases POSTGRES_BACKEND.md lists for date_diff) on Trino.
3. **PIVOT on 483** in place of the CASE-per-key carrier (TypedPivot is 15,193 against 9,615). It overturns "no PIVOT". Risk: Trino's grouping of the non-pivot columns behaved unexpectedly in my one probe. Read the doc and probe more before relying on it.

---

## 3. Postgres

### 3.1 Version

- **We run 16.15.** Sources: `tools/postgres/postgres.bzl:14` (`VERSION = "16.15.0"`, zonky embedded binaries) and `select version()`, which answered "PostgreSQL 16.15 on x86_64-pc-linux-gnu". JDBC driver: `org.postgresql:postgresql:42.7.13`.
- **Latest:** 19.0 (released November 19, 2026), 18.7, and 16.16 for our major (https://www.postgresql.org/support/versioning/).

What matters between 16 and 19:
- **18:** `array_sort()` ("sorts an array's first dimension"), `array_reverse()`, MIN and MAX on arrays and composites, and `jsonb` null castable to scalars as NULL (https://www.postgresql.org/docs/18/release-18.html).
  - `array_sort(arr [, descending [, nulls_first]])` (https://www.postgresql.org/docs/current/functions-array.html).
- **19:** `IGNORE NULLS`/`RESPECT NULLS` for `lead`, `lag`, `first_value`, `last_value` and `nth_value`, plus `error_on_null()` (https://www.postgresql.org/docs/19/release-19.html). There are still no lambdas or higher-order array functions (none on the array functions page).
- `any_value()` already exists in 16.

### 3.2 Claims, checked (Postgres.java and docs/POSTGRES_BACKEND.md)

| Claim | Verdict | Evidence |
|---|---|---|
| "Collections … are walls until the jsonb carrier lands … never Postgres' native ARRAY" (Postgres.java:32-33, CAPS comment 65) | **OUTDATED (contradicted by our own code)** | The dialect writes native `BIGINT[]`, `DATE[]` and `ARRAY(SELECT …)` (1,337 `ARRAY[` in the dump) and `JSONB[]` for structs and maps |
| arrays are rectangular: `ARRAY[ARRAY[1,2],ARRAY[3]]` is an error (BACKEND.md §5) | TRUE (as documented; not re-probed) | |
| no `%` or `mod()` for double precision (Postgres.java:898, BACKEND.md) | TRUE | "available for smallint, integer, bigint, and numeric" (https://www.postgresql.org/docs/current/functions-math.html) |
| no `round(double, int)` (909) | TRUE | Same page: only `round(numeric)`, `round(double precision)` and `round(numeric, integer)` |
| no max(boolean) (755) | Not re-verified (`bool_or` is used) | |
| no QUALIFY (317) | TRUE through 19 (not in the 19 release notes or our probes) | |
| IGNORE NULLS refused (BACKEND.md §7 P4) | **OUTDATED from 19** | 19 release notes quoted above |
| no stable row order (1466) | TRUE (SQL without ORDER BY) | |
| no core sha1 through 18 (984) | Not re-verified | |
| `ARRAY(subquery)` gives NULL for no row (implied by the guards) | **FALSE** | Probed on 16.15: `ARRAY(SELECT 1 WHERE false) IS NULL` gives f, and the value is `{}` |
| a call takes at most 100 arguments (74) | TRUE (FUNC_MAX_ARGS default; lite measured it) | not re-probed |
| "No STRUCT type here" (128) | Partly: Postgres has composite and ROW values, but named fields need a CREATE TYPE, which a plan does not run | not re-probed |

### 3.3 Where the SQL blows up

| Family | Postgres | DuckDB | Excess | Ratio |
|---|---|---|---|---|
| let | 270,572 | 172,995 | +98K | 1.56 |
| removeDuplicates | 37,877 | 8,684 | +29K | 4.36 |
| TypedExtendWindow | 131,606 | 112,676 | +19K | 1.17 |
| contains (Any) | 17,420 | 2,589 | +15K | 6.73 |
| Map keys | 14,589 | 369 | +14K | 39.5 |
| Map values | 14,595 | 375 | +14K | 38.9 |
| percentile | 14,181 | 1,642 | +13K | 8.64 |
| format | 15,931 | 5,315 | +11K | 3.00 |
| Map get | 10,608 | 194 | +10K | 54.7 |
| maxBy and minBy | about 10.4K each | about 500 | +10K each | 20.5-21.3 |
| Map put | 9,815 | 460 | +9K | 21.3 |

Causes and shorter forms:

1. **The null guard writes the list twice, and it nests.**
   - `overElements` (`Postgres.java:666-678`), `LIST_DISTINCT` (736), the map arms (405, 441) and `array_position` (1218) all write `(CASE WHEN xs IS NULL THEN NULL ELSE ARRAY(SELECT … FROM unnest(xs) …) END)`.
   - With list over list over list, the text doubles at each level. maxBy `[1,2]` by `[10,20]` (query 2261) is 4,566 characters against DuckDB's 211.
   - Shorter, in two steps:
     - (a) Drop the guard when `xs` is an `ARRAY[...]` literal, a `CAST(ARRAY[...] …)` or an `ARRAY(subquery)`: these are never NULL (probed). That is a fold on SQL structure and types, not on row data. My crude rewrite of exactly that one case over the dump: 959,161 falls to 903,043 (−56K).
     - (b) For a computed list that can be NULL, bind it once: `(SELECT CASE WHEN l IS NULL THEN NULL ELSE ARRAY(SELECT … FROM unnest(l) WITH ORDINALITY …) END FROM (SELECT <xs> AS l) AS b)`, or a LATERAL in the FROM. Not probed as a whole statement.
2. **Constant guards that are written out.**
   - `LIST_GET` writes `CASE WHEN <index> < 0 THEN xs[cardinality(xs)+1+i] ELSE xs[i] END`, so the list appears three times even when the index is the literal 1 (`CASE WHEN 1 < 0`).
   - range writes `CASE WHEN 1 > 0 THEN g < 2 + 1 ELSE g > 2 + 1 END` for a literal step.
   - Fold both on the literal (a constant fact, allowed by the "Folds" ruling): use `LIST_AT` for a non-negative literal index, and the one branch for a literal step.
3. **No lambda: `contains` over a mixed list is unrolled per element.**
   - Query 1013: `CASE WHEN tags[1] = 4 AND dates[1] = … THEN 1 ELSE 0 END + …` eight times, each writing both arrays (1,756 characters against 259).
   - Shorter. Postgres's multi-argument `unnest` zips arrays in FROM (documented: "Expands multiple arrays side by side into rows, padding shorter arrays with NULLs. Allowed only in the FROM clause", https://www.postgresql.org/docs/current/functions-array.html):
     ```sql
     SELECT coalesce(EXISTS (SELECT 1 FROM unnest(CAST(ARRAY[1,1,1,1,2,3,4,2] AS BIGINT[]),
                                                  CAST(ARRAY[NULL,NULL,NULL,NULL,NULL,NULL,DATE '2014-02-01',NULL] AS DATE[])) AS u(t, d)
                             WHERE t = 4 AND d = DATE '2014-02-01'), FALSE) AS "value"
     ```
     That is about 230 characters, a saving of about 85% on the family.
4. **Maps as `JSONB[]` with key checks re-built at every use.**
   - `keysChecked` (`Postgres.java:461-467`) writes the entry array three times (count of NULL keys, count of distinct keys, and the result), and every later read re-builds the whole map. Map get (query 1067) is 9,708 characters against 104.
   - Bind the built map once: a scalar subquery `(SELECT … FROM (SELECT <entries> AS m) b)`. Or build `jsonb_object_agg(key, value)` once and read it with `->`. Ordering and duplicate detection would need care, because jsonb objects do not keep insertion order; that is the reason lite keeps an array.
5. **Sort and reverse as `ARRAY(SELECT x FROM unnest(xs) WITH ORDINALITY … ORDER BY …)`.** Postgres 18's `array_sort(xs)`, `array_sort(xs, true)` and `array_reverse(xs)` do it in one call. Their NULL placement is documented: `nulls_first` defaults to `descending`. That matches DuckDB only if lite's spec is NULLS LAST ascending; check against `sortKey`. Structs held as jsonb still need the subquery, because jsonb order compares keys by storage order (`Postgres.java:680-683`). This requires a version bump to 18 or later.
6. **percentile** writes the sorted list 9 times. Same binding fix. Postgres has `percentile_cont` as an ordered-set aggregate over rows: `(SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY x) FROM unnest(xs) x)`, which is one read of the list. This is documented Postgres; it was not probed here.
7. **rem over Float (10x):** the long exact construction is the best the database can do. There is no double-precision mod (docs above).
8. **Quoting:** 37,341 quote characters (3.9%). Postgres folds unquoted names to lowercase, so quoting is needed for mixed-case names only. Low priority.

### 3.4 The three biggest Postgres wins

1. **Drop the null guard over never-NULL lists, and bind other lists once.**
   - Overturns the implicit "ARRAY(subquery) may be NULL".
   - Evidence: the probe in 3.2.
   - Saving: at least 56K characters from (a) alone, likely 100K+ with binding, because it removes the doubling in `let`, removeDuplicates, maxBy and minBy, and percentile.
   - Risk: a list column or an outer-join side can be NULL, so keep the guard there.
2. **Zip with multi-argument `unnest` in an EXISTS or an aggregate subquery** for contains, indexOf and other per-element unrolls. It uses a documented feature. Saving: about 15K on contains alone, more where the same unroll appears. Risk: unnest pads the shorter array with NULL, which is the same as the unrolled form's out-of-range NULL.
3. **Move to Postgres 18 or 19 and use `array_sort`/`array_reverse` for scalar lists.** In 19, IGNORE NULLS unblocks the window rows refused today (BACKEND.md P4). Evidence: the release notes cited above. Risk: NULL ordering (above), and the embedded binaries (zonky) must ship 18 or 19, which was not checked.

---

## Probe files (scratchpad)

- `ctp-dump/`: the SQL dump of the four databases (`sqlf.tsv`, with family), `fam.py` (ranking), `show.sh`, and `pgsim.py` (the guard-drop estimate)
- `chprobe/`: the ClickHouse server config, and the vendor `Settings.cpp`, `SettingsChangesHistory.cpp` and `FormatFactorySettings.h` at v26.11.3.10
- `trtool/`: the Trino probe statements and their runner
- `pgtool/`, `pgprobe/`: the Postgres probe runner and its data directory
