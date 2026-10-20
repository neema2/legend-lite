# `#IQ{...}#` showcase: one SAP IQ query, SQL Anywhere's answer, and legend-lite's Snowflake and DuckDB SQL

This page follows one non-trivial SAP IQ query through legend-lite's `#IQ{...}#` island
(`docs/IQ_SQL_STUDY_2026_10_13.md`). It was verified on 2026-10-14 against the translator at commit `918a9ccc6`
(branch `claude/iq-m1`); the commit that added this page, its cases and its pinned SQL changed no translator code.

- **Accepted.** lite translates the query; it does not refuse it.
- **Correct.** lite's answer matches SQL Anywhere 17's cell for cell, in the same row order, on both parity lanes
  (DuckDB and Postgres). The judge is the IQ parity harness's own (`IqParity.outcome`), and the harness's guards are
  clean. SQL Anywhere's rows were snapped by the repo's oracle, `tools/iq-oracle`, under the `iq16-default` profile.
- **Pinned.** The SQL the query plans for Snowflake and for DuckDB is pinned byte for byte, so a change to either is
  seen and reviewed.
- **Snowflake.** The same query, compiled against a Snowflake connection, plans to the SQL in §7. That SQL is rendered
  only: nothing ran on Snowflake.

**Where it lives.**

- `core/src/test/resources/iq-parity/lite/showcase.yaml`: the query (`showcase_main`) and the two cases that support
  it (§3), run by both IQ parity lanes, `//core:iq_parity_test` and `//core:iq_parity_postgres_test`.
- `core/src/test/resources/iq-parity/iq-answers.jsonl`, keys `lite/showcase/*`: SQL Anywhere's answers, snapped by
  `tools/iq-oracle/snap.sh` (its `README.md` says how); `ledger.tsv` holds each case's status (PASS on both lanes).
- `core/src/test/resources/iq-parity/showcase/snowflake.sql` and `duckdb.sql`: the pinned SQL, checked by
  `IqParityTest.theShowcasePlansItsPinnedSql` in `//core:iq_parity_test`. If the SQL changes on purpose, rewrite them
  from the workspace root and update §5 to §8 here:

  ```sh
  bazel test //core:iq_parity_test --strategy=TestRunner=local \
    --test_env=JDK_JAVA_OPTIONS=-Diq.showcase.update=$PWD/core/src/test/resources/iq-parity/showcase
  ```

## 1. The query

The island, as a Pure query:

```
|let skip = 'hello';
#IQ{
SELECT TOP 5 START AT 2
       [Row Id] = t.id,
       LEFT(t.vc, 3) + ':' + a.labels   AS [Tag],
       a.n                              'Labels',
       a.last_u                         AS [Last u],
       'Is x' = IF t.vc = 'x' THEN 'Y' ELSE 'N' ENDIF,
       ISNULL(LOCATE(t.vc, 'l'), -1)    AS [First l],
       DATENAME(dw, t.dt)               AS [Day]
FROM tb('e2e::DB.t') AS t
JOIN (SELECT t.id AS tid,
             COUNT(v.id) AS n,
             MAX(v.id)   AS last_u,
             LIST(v.lbl, ',' ORDER BY v.lbl) AS labels
      FROM tb('e2e::DB.t') AS t
      LEFT JOIN (SELECT id, t_id, RTRIM(label) AS lbl FROM DBA.u) v ON v.t_id = t.id
      GROUP BY t.id) a ON a.tid = t.id
WHERE t.vc <> @skip OR t.vc IS NULL
ORDER BY [Row Id] DESC
}#;
```

The case in `showcase.yaml` writes bare table names (`FROM t`); the harness rewrites each to its Legend source,
`tb('e2e::DB.t') AS t`, and the text above is the island after that rewrite. `DBA.u` is not rewritten: it is an
owner-qualified name, which the island binds to the model's own table `u` (an owner is a schema, and a table declared
outside any schema is `DBA`'s: the study's O2). `@skip` is a host variable, read as the enclosing Pure query's
variable `skip` (the study's O3); the case binds it with `params: "skip = 'hello'"`, a `let` before the island.

The Pure model is the harness's own (`IqParity.model` over `iq-parity/schema.yaml`). For the Snowflake SQL only the
connection differs:

```
###Relational
Database e2e::DB
(
    Table t ( id INTEGER, sm SMALLINT, it INTEGER, bg BIGINT, nm NUMERIC(10,2), dbl DOUBLE,
              ch CHAR(5), vc VARCHAR(20), dt DATE, ts TIMESTAMP, bt BIT )
    Table u ( id INTEGER, t_id INTEGER, label VARCHAR(10), qty NUMERIC(5,1) )
    ...                                   (r, g, o as in schema.yaml)
)
###Connection
RelationalDatabaseConnection e2e::Conn
{
    type: Snowflake;
    specification: Snowflake { name: 'IQ_SHOWCASE'; account: 'acct'; warehouse: 'wh'; region: 'us-east-1'; };
    auth: SnowflakePublic { publicUserName: 'u'; privateKeyVaultReference: 'vault/k'; passPhraseVaultReference: 'vault/pp'; };
}
###Runtime
Runtime e2e::RT
{
    mappings: [];
    connections: [ e2e::DB: [ environment: e2e::Conn ] ];
}
```

The account and the vault references are placeholders: planning never connects. The DuckDB lane uses the same model
with `type: DuckDB; specification: DuckDB { }; auth: Test;`.

## 2. What it exercises

| IQ feature | Where in the query | Visible in the answer |
|---|---|---|
| Bracketed identifiers with spaces | `[Row Id]`, `[Tag]`, `[Last u]`, `[First l]`, `[Day]`; `ORDER BY [Row Id]` sorts by a select-list alias | the column names |
| T-SQL alias forms | `[Row Id] = t.id` and `'Is x' = IF ...` (`alias = expr`); `a.n 'Labels'` (a string as an alias) | the column names |
| `TOP n START AT m` | `TOP 5 START AT 2` over `ORDER BY [Row Id] DESC` | row 7, the first in order, is skipped |
| `IF ... THEN ... ELSE ... ENDIF` | `'Is x'` | row 5 shows **NULL**, not `'N'`: IQ's `IF` is NULL when its condition is UNKNOWN, unlike `CASE ... ELSE` |
| `+` as string concatenation | `LEFT(t.vc, 3) + ':' + a.labels` | row 5's Tag is `':'`: next to a string, a NULL operand counts as `''` |
| Case-sensitive comparison (the `iq16-default` database is case-sensitive) | `WHERE t.vc <> @skip` and `LOCATE(t.vc, 'l')` | `'Hello'` (row 1) is kept and `'hello'` (row 2) dropped; `LOCATE` finds no lowercase `l` in `'ÉCOLE'` and gives 0 |
| Blank-padded comparison | `IF t.vc = 'x'` | row 6 holds `'x  '`, which equals `'x'`, so it shows `Y` |
| `ISNULL` | `ISNULL(LOCATE(t.vc, 'l'), -1)` | row 5 shows `-1` |
| IQ's functions | `LEFT`, `LOCATE` (IQ's argument order: the string first), `RTRIM`, `DATENAME(dw, ...)` | `ÉCO`, `Rés`; the day names |
| A join to a grouped derived table | an inner `JOIN (...) a`, which LEFT JOINs a nested derived table `v` | |
| `GROUP BY` with IQ's aggregates | `COUNT(v.id)`: 0 for an empty group. `MAX(v.id)`: NULL for an empty group. `LIST(v.lbl, ',' ORDER BY v.lbl)`: IQ's string aggregate, skipping NULL and `''`, and `''` for an empty group | `one,uno`; Labels 0; Last u NULL |
| An owner-qualified table | `FROM DBA.u` | bound to the model's `u`: the Pure reads `#>{e2e::DB.u}#` |
| A host variable | `@skip`, the enclosing query's `let skip = 'hello'` | compared as IQ compares a VARCHAR: case-sensitive and blank-padded |
| Latin-1 text | `'Résumé'`, `'ÉCOLE'` in `t.vc` | `Rés`, `ÉCO` |

The rows this query reads (`schema.yaml`):

| t.id | t.vc | t.dt |  | u.id | u.t_id | u.label |
|---|---|---|---|---|---|---|
| 1 | `Hello` | 2026-03-04 | | 1 | 1 | `one` |
| 2 | `hello` | 1999-12-31 | | 2 | 1 | `uno` |
| 3 | `Résumé` | 2000-02-29 | | 3 | 3 | `three` |
| 4 | `ÉCOLE` | 1900-01-01 | | 4 | 99 | `orphan` |
| 5 | NULL | NULL | | 5 | NULL | `none` |
| 6 | `x␠␠` (two trailing blanks) | 2026-12-31 | | | | |
| 7 | `apple` | 2026-03-04 | | | | |

## 3. SQL Anywhere 17's answer

SQL Anywhere 17.0.11.7254, profile `iq16-default`: the oracle's IQ-like database, case-sensitive, blank-padded, with a
binary ISO-8859-1 collation (`tools/iq-oracle/README.md`). The variable is a real SQL Anywhere variable on the case's
connection, a LONG VARCHAR set to `'hello'` before the query runs (since review 5's M5; the answer is the one snapped
with the value written into the SQL before).

| Row Id `integer` | Tag `long varchar` | Labels `integer` | Last u `integer` | Is x `char(1)` | First l `integer` | Day `varchar(9)` |
|---|---|---|---|---|---|---|
| 6 | `x␠␠:` | 0 | NULL | `Y` | 0 | `Thursday` |
| 5 | `:` | 0 | NULL | NULL | -1 | NULL |
| 4 | `ÉCO:` | 0 | NULL | `N` | 0 | `Monday` |
| 3 | `Rés:three` | 1 | 3 | `N` | 0 | `Tuesday` |
| 1 | `Hel:one,uno` | 2 | 2 | `N` | 3 | `Wednesday` |

(`␠` marks a blank. The snapped row is `["6","x  :","0",null,"Y","0","Thursday"]`.)

Two more cases support it, also snapped and also PASS on both lanes:

- `showcase_all_rows`: the same select list without `TOP`/`START AT` and without the `WHERE`. It answers rows 7 to 1:
  row 7 is `app: | 0 | NULL | N | 4 | Wednesday` and row 2 is `hel: | 0 | NULL | N | 3 | Friday`. So `START AT 2`
  skips row 7, and the case-sensitive `<> @skip` drops row 2 only.
- `showcase_inner_aggregate`: the derived table on its own, with the `SUM(v.id)` the main query cannot hold (§9).
  Groups 2, 4, 5, 6 and 7 answer `n = 0`, `last_u = NULL`, `su = NULL`, `labels = ''`: IQ's `LIST` over a group whose
  only value is NULL is the empty string. Here the SUM is the statement's own and lite accepts it, with IQ's overflow
  check (D-E3).

## 4. legend-lite's answer: identical

On DuckDB (the Postgres lane answers the same):

```
COLS [Row Id, Tag, Labels, Last u, Is x, First l, Day]   (Integer, String, Integer, Integer, String, Integer, String)
ROW  [6, "x  :",        0, null, "Y",  0,  "Thursday"]
ROW  [5, ":",           0, null, null, -1, null]
ROW  [4, "ÉCO:",        0, null, "N",  0,  "Monday"]
ROW  [3, "Rés:three",   1, 3,    "N",  0,  "Tuesday"]
ROW  [1, "Hel:one,uno", 2, 2,    "N",  3,  "Wednesday"]
```

`ledger.tsv`:

```
lite/showcase/showcase_main@iq16-default	PASS	PASS
lite/showcase/showcase_all_rows@iq16-default	PASS	PASS
lite/showcase/showcase_inner_aggregate@iq16-default	PASS	PASS
```

The lanes' guards are clean for the three cases: the answers file (no UNSNAPPED, STALE or GONE answer); engine
neutrality (the translation is the same under the IQ engine's rules and under SQL Anywhere's, so SQL Anywhere is a
fair judge); D-E2's evaluation order (the WHERE puts no operation that can raise beside its `OR`).

## 5. The Pure the island desugars to

The translator's output (`SqlToPure.translate`, printed by `PureComposer`), formatted for reading: it is equal to the
printed text but for whitespace. Compiled directly as a query, without the island, it plans byte-identical Snowflake
and DuckDB SQL and answers the same five rows: the island is sugar for this Pure.

```
|let skip = 'hello';
#>{e2e::DB.t}#
  ->join(
      #>{e2e::DB.t}#
        ->join(
            #>{e2e::DB.u}#
              ->extend(~[lbl:r|if($r.label->isEmpty(), |[], |$r.label->toOne()->regexpReplace('(\\n|[\\t-\\r ])+$', '', false))])
              ->select(~[id, t_id, lbl])
              ->rename(~id, ~v_id),
            meta::pure::functions::relation::JoinKind.LEFT,
            {a, b|($b.t_id->isNotEmpty() && $a.id->isNotEmpty()) && ($b.t_id->toOne() == $a.id->toOne())})
        ->groupBy(~[id], ~[
            n:r|$r.v_id:y|$y->count(),
            last_u:r|$r.v_id:y|$y->max(),
            _sql_1:y|$y->joinStrings(r|if($r.lbl->isEmpty(), |[], |if($r.lbl->toOne() == '', |[]->cast(@String), |$r.lbl->toOne())), ',', ~lbl->ascending())])
        ->extend(~[labels:r|if($r._sql_1->isEmpty(), |'', |$r._sql_1->toOne())])
        ->extend(~[tid:r|$r.id])
        ->select(~[tid, n, last_u, labels]),
      meta::pure::functions::relation::JoinKind.INNER,
      {a, b|($b.tid->isNotEmpty() && $a.id->isNotEmpty()) && ($b.tid->toOne() == $a.id->toOne())})
  ->filter(r|($r.vc->isNotEmpty()
              && !(($r.vc->toOne()->startsWith($skip) || $skip->startsWith($r.vc->toOne()))
                   && ($r.vc->toOne()->replace(' ', '') == $skip->replace(' ', ''))))
             || $r.vc->isEmpty())
  ->extend(~['Row Id':r|$r.id])
  ->sort([~'Row Id'->descending(meta::pure::functions::relation::NullOrder.LAST)])
  ->limit((2 - 1) + 5)
  ->extend(~[
      Tag:r|if($r.vc->isEmpty(), |[], |$r.vc->toOne()->left(if(3 < 0, |0, |3)))->coalesce('') + ':' + $r.labels,
      Labels:r|$r.n,
      'Last u':r|$r.last_u,
      'Is x':r|if($r.vc->isNotEmpty() && (($r.vc->toOne()->startsWith('x') || 'x'->startsWith($r.vc->toOne()))
                                          && ($r.vc->toOne()->replace(' ', '') == 'x'->replace(' ', ''))),
                 |'Y',
                 |if($r.vc->isNotEmpty() && !(($r.vc->toOne()->startsWith('x') || 'x'->startsWith($r.vc->toOne()))
                                              && ($r.vc->toOne()->replace(' ', '') == 'x'->replace(' ', ''))),
                     |'N',
                     |[]->cast(@String))),
      'First l':r|if($r.vc->isEmpty(), |[], |$r.vc->toOne()->indexOf('l'))->coalesce(-1),
      Day:r|if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->isEmpty(), |[],
          |if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->toOne() == 1, |'Sunday',
          |if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->toOne() == 2, |'Monday',
          |if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->toOne() == 3, |'Tuesday',
          |if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->toOne() == 4, |'Wednesday',
          |if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->toOne() == 5, |'Thursday',
          |if(if($r.dt->isEmpty(), |[], |$r.dt->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Sunday))->toOne() == 6, |'Friday',
          |'Saturday')))))))])
  ->select(~['Row Id', Tag, Labels, 'Last u', 'Is x', 'First l', Day])
  ->drop(2 - 1);
```

## 6. How the IQ semantics land in the SQL

Each row names an IQ rule, then the SQL for Snowflake (§7) and for DuckDB (§8).

| IQ rule | Snowflake SQL | DuckDB SQL |
|---|---|---|
| **Blank-padded `=` / `<>`.** `'x  ' = 'x'` is true. Neither Snowflake nor DuckDB pads, so lite writes padding as: one string starts with the other, and the two are equal once blanks are removed, which holds exactly when the longer one's tail is blanks. The comparison stays binary and case-sensitive, as `iq16-default` is. | `(startswith(a,b) OR startswith(b,a)) AND replace(a,' ','') = replace(b,' ','')` | the same, with `starts_with` |
| **A host variable** is the enclosing query's value. | `'hello'`, the `let`'s constant, inlined | the same |
| **An owner-qualified table** is the model's table. | `FROM "u"` | `FROM u` |
| **`IF ... ENDIF` is three-valued**: NULL on an UNKNOWN condition. | `CASE WHEN c THEN 'Y' ELSE CASE WHEN NOT c (with an explicit vc IS NOT NULL) THEN 'N' ELSE NULL END END` | the same |
| **String `+` treats a NULL operand as `''`.** | `COALESCE(...,'') \|\| ':' \|\| COALESCE(labels,'')`: Snowflake's `\|\|` is NULL when an operand is, so each is coalesced | `concat(coalesce(...,''), ':', labels)`: DuckDB's `concat` skips NULLs |
| **`TOP 5 START AT 2`**, its items computed for the rows up to the last it keeps (IQ's engine computes the items its ORDER BY does not read for those rows only, START AT's skipped one too: since 2026-10-16) | the sorted rows cut to `LIMIT 6` in a subquery, the items computed over it, re-sorted, then `OFFSET 1` | the same |
| **NULL sorts lowest** | `ORDER BY "t0"."id" DESC NULLS LAST` | the same |
| **`LIST(x, ',' ORDER BY x)`** skips NULL and `''`, is `''` for an empty group, and orders by the binary collation | `CASE WHEN COUNT(x') = 0 THEN NULL ELSE LISTAGG(x', ',') WITHIN GROUP (ORDER BY lbl ASC) END`: the dialect guards `LISTAGG` with a COUNT so that a group of NULLs is NULL (Pure's empty `joinStrings`), which the island maps to `''` one level up | `STRING_AGG(x', ',' ORDER BY lbl ASC)`, mapped to `''` one level up |
| **`MAX` of an INTEGER** is NULL for an empty group | `MAX("t4"."v_id")` | `MAX(t4.v_id)` |
| **`RTRIM`** drops trailing blanks and the controls `\t` to `\r` (the M1d trim cases pin this) | `REGEXP_REPLACE(label, '(\\n\|[\\t-\\r ])+$', '', 1, 1, 'c')`: the backslashes doubled, as a Snowflake string literal reads its escapes | `regexp_replace(label, '(\n\|[\t-\r ])+$', '', '')` |
| **`DATENAME(dw, d)`**: English day names, the week numbered from Sunday whatever the session's settings | `MOD(MOD(DAYOFWEEKISO(dt), 7) + 7, 7) + 1`, then a CASE mapping 1 to 7 to `'Sunday'` ... `'Saturday'`: it does not depend on Snowflake's `WEEK_START` or on `DAYNAME`'s abbreviations | `date_part('isodow', dt)` in the same arithmetic |
| **`LOCATE(s, 'l')`**, case-sensitive here; `ISNULL(..., -1)` | `coalesce(CASE WHEN vc IS NULL THEN NULL ELSE CHARINDEX('l', vc) END, -1)` | `strpos(vc, 'l')` |
| **`LEFT(s, n)`**: a negative n counts as 0 | `left(vc, CASE WHEN FALSE THEN 0 ELSE 3 END)`: the guard folded only as far as `FALSE` | the same |
| **Join equality** (Pure's `==` over values that may be NULL) | `ON x IS NOT NULL AND y IS NOT NULL AND x = y` | the same |
| **Identifiers** | each double-quoted (`"t"`, `"vc"`, `"Row Id"`), so Snowflake keeps the model's lower-case names rather than folding them to upper case | quoted only where needed (`"Row Id"`) |

## 7. The SQL for Snowflake (rendered, not run)

`Compiler.query(Compiler.compileModel(model), query).plan("e2e::RT").sql()`, pinned in
`core/src/test/resources/iq-parity/showcase/snowflake.sql`. Formatted here for reading: equal to the pinned file but
for whitespace, its string and identifier literals byte for byte.

```sql
SELECT
  "t7"."Row Id" AS "Row Id",
  (COALESCE(coalesce(CASE WHEN "t7"."vc" IS NULL THEN NULL ELSE left("t7"."vc", CASE WHEN FALSE THEN 0 ELSE 3 END) END, ''), '') || ':' || COALESCE(coalesce("t7"."labels", ''), '')) AS "Tag",
  "t7"."n" AS "Labels",
  "t7"."last_u" AS "Last u",
  CASE WHEN "t7"."vc" IS NOT NULL AND (startswith("t7"."vc", 'x') OR startswith('x', "t7"."vc")) AND replace("t7"."vc", ' ', '') = replace('x', ' ', '') THEN 'Y' WHEN "t7"."vc" IS NOT NULL AND NOT ((startswith("t7"."vc", 'x') OR startswith('x', "t7"."vc")) AND replace("t7"."vc", ' ', '') = replace('x', ' ', '')) THEN 'N' ELSE NULL END AS "Is x",
  coalesce(CASE WHEN "t7"."vc" IS NULL THEN NULL ELSE CHARINDEX('l', "t7"."vc") END, -1) AS "First l",
  CASE WHEN "t7"."dt" IS NULL THEN NULL WHEN MOD(MOD(DAYOFWEEKISO("t7"."dt"), 7) + 7, 7) + 1 = 1 THEN 'Sunday' WHEN MOD(MOD(DAYOFWEEKISO("t7"."dt"), 7) + 7, 7) + 1 = 2 THEN 'Monday' WHEN MOD(MOD(DAYOFWEEKISO("t7"."dt"), 7) + 7, 7) + 1 = 3 THEN 'Tuesday' WHEN MOD(MOD(DAYOFWEEKISO("t7"."dt"), 7) + 7, 7) + 1 = 4 THEN 'Wednesday' WHEN MOD(MOD(DAYOFWEEKISO("t7"."dt"), 7) + 7, 7) + 1 = 5 THEN 'Thursday' WHEN MOD(MOD(DAYOFWEEKISO("t7"."dt"), 7) + 7, 7) + 1 = 6 THEN 'Friday' ELSE 'Saturday' END AS "Day"
FROM (
  SELECT *, "t0"."id" AS "Row Id"
  FROM "t" AS "t0"
  JOIN (
    SELECT "t5"."id" AS "tid", "t5"."n" AS "n", "t5"."last_u" AS "last_u", "t5"."_sql_1" AS "labels"
    FROM (
      SELECT
        "t1"."id" AS "id",
        COUNT("t4"."v_id") AS "n",
        MAX("t4"."v_id") AS "last_u",
        (CASE WHEN COUNT(CASE WHEN "t4"."lbl" IS NULL THEN NULL ELSE REGEXP_REPLACE("t4"."lbl", ' +$', '', 1, 1, 'c') END) = 0 THEN NULL ELSE LISTAGG(CASE WHEN "t4"."lbl" IS NULL THEN NULL ELSE REGEXP_REPLACE("t4"."lbl", ' +$', '', 1, 1, 'c') END, ',') WITHIN GROUP (ORDER BY "t4"."lbl" ASC) END) AS "_sql_1"
      FROM "t" AS "t1"
      LEFT OUTER JOIN (
        SELECT "t3"."id" AS "v_id", "t3"."t_id" AS "t_id", "t3"."lbl" AS "lbl"
        FROM (
          SELECT
            "t2"."id" AS "id",
            "t2"."t_id" AS "t_id",
            CASE WHEN "t2"."label" IS NULL THEN NULL ELSE REGEXP_REPLACE("t2"."label", '(\\n|[\\t-\\r ])+$', '', 1, 1, 'c') END AS "lbl"
          FROM "u" AS "t2"
        ) AS "t3"
      ) AS "t4" ON "t4"."t_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t4"."t_id" = "t1"."id"
      GROUP BY "t1"."id"
    ) AS "t5"
  ) AS "t6" ON "t6"."tid" IS NOT NULL AND "t0"."id" IS NOT NULL AND "t6"."tid" = "t0"."id"
  WHERE "t0"."vc" IS NOT NULL AND NOT ((startswith("t0"."vc", 'hello') OR startswith('hello', "t0"."vc")) AND replace("t0"."vc", ' ', '') = replace('hello', ' ', '')) OR "t0"."vc" IS NULL
  ORDER BY "t0"."id" DESC NULLS LAST
  LIMIT 6
) AS "t7"
ORDER BY "t7"."Row Id" DESC NULLS LAST
LIMIT NULL
OFFSET 1
```

## 8. The SQL for DuckDB (rendered and run)

The same query with the DuckDB connection, pinned in `core/src/test/resources/iq-parity/showcase/duckdb.sql`: the
statement the DuckDB lane runs. Formatted here likewise.

```sql
SELECT
  t7."Row Id",
  concat(coalesce(CASE WHEN t7.vc IS NULL THEN NULL ELSE left(t7.vc, CASE WHEN FALSE THEN 0 ELSE 3 END) END, ''), ':', coalesce(t7.labels, '')) AS Tag,
  t7.n AS Labels,
  t7.last_u AS "Last u",
  CASE WHEN t7.vc IS NOT NULL AND (starts_with(t7.vc, 'x') OR starts_with('x', t7.vc)) AND replace(t7.vc, ' ', '') = replace('x', ' ', '') THEN 'Y' WHEN t7.vc IS NOT NULL AND NOT ((starts_with(t7.vc, 'x') OR starts_with('x', t7.vc)) AND replace(t7.vc, ' ', '') = replace('x', ' ', '')) THEN 'N' ELSE NULL END AS "Is x",
  coalesce(CASE WHEN t7.vc IS NULL THEN NULL ELSE strpos(t7.vc, 'l') END, -1) AS "First l",
  CASE WHEN t7.dt IS NULL THEN NULL WHEN MOD(MOD(date_part('isodow', t7.dt), 7) + 7, 7) + 1 = 1 THEN 'Sunday' WHEN MOD(MOD(date_part('isodow', t7.dt), 7) + 7, 7) + 1 = 2 THEN 'Monday' WHEN MOD(MOD(date_part('isodow', t7.dt), 7) + 7, 7) + 1 = 3 THEN 'Tuesday' WHEN MOD(MOD(date_part('isodow', t7.dt), 7) + 7, 7) + 1 = 4 THEN 'Wednesday' WHEN MOD(MOD(date_part('isodow', t7.dt), 7) + 7, 7) + 1 = 5 THEN 'Thursday' WHEN MOD(MOD(date_part('isodow', t7.dt), 7) + 7, 7) + 1 = 6 THEN 'Friday' ELSE 'Saturday' END AS Day
FROM (
  SELECT *, t0.id AS "Row Id"
  FROM t AS t0
  JOIN (
    SELECT t5.id AS tid, t5.n, t5.last_u, t5._sql_1 AS labels
    FROM (
      SELECT
        t1.id,
        COUNT(t4.v_id) AS n,
        MAX(t4.v_id) AS last_u,
        STRING_AGG(CASE WHEN t4.lbl IS NULL THEN NULL ELSE regexp_replace(t4.lbl, ' +$', '', '') END, ',' ORDER BY t4.lbl ASC) AS _sql_1
      FROM t AS t1
      LEFT OUTER JOIN (
        SELECT t3.id AS v_id, t3.t_id, t3.lbl
        FROM (
          SELECT
            t2.id,
            t2.t_id,
            CASE WHEN t2.label IS NULL THEN NULL ELSE regexp_replace(t2.label, '(\n|[\t-\r ])+$', '', '') END AS lbl
          FROM u AS t2
        ) AS t3
      ) AS t4 ON t4.t_id = t1.id
      GROUP BY t1.id
    ) AS t5
  ) AS t6 ON t6.tid IS NOT NULL AND t0.id IS NOT NULL AND t6.tid = t0.id
  WHERE t0.vc IS NOT NULL AND NOT ((starts_with(t0.vc, 'hello') OR starts_with('hello', t0.vc)) AND replace(t0.vc, ' ', '') = replace('hello', ' ', '')) OR t0.vc IS NULL
  ORDER BY t0.id DESC NULLS LAST
  LIMIT 6
) AS t7
ORDER BY t7."Row Id" DESC NULLS LAST
OFFSET 1
```

## 9. What lite refused on the way

The query was shaped around refusals. Each names what lite cannot answer exactly yet, rather than guess.

| Tried | lite's refusal (abridged) |
|---|---|
| `SUM(v.id)` in the derived table `a` | "SUM read only for some groups (... under a row limit, ... a derived table ...) is not supported: IQ checks its running total for every group (D-E3), lite's databases only where they read it". SQL Anywhere checks an INTEGER sum's overflow in the aggregation, for every group; lite's check sits where the value is read, and the outer `TOP` reads some rows only. Hence `MAX(v.id)` in the query, and the SUM in `showcase_inner_aggregate`, where it is the statement's own. A SUM at the top level is refused too: the `TOP` reads it for some groups only. |
| `SUM(u.qty)`, a NUMERIC(5,1) | "SUM of a NUMERIC is not supported under IQ's engine yet: its result type there is the research's D-N1, not yet verified" |
| `MIN(u.label)` | "MIN of strings is supported only where every value is its own collation key (RTRIM, TRIM, a constant): IQ compares 'a' and 'a ' as one value and shows the first it meets" |
| `LIST(v.lbl, ',' ORDER BY v.id)` | "LIST ordered by anything but its value, a column, is not supported yet: the order of tied rows is the plan's" |
| `LIST(u.label, ',' ORDER BY u.label)` over the raw column | "LIST of strings is supported only where every value is its own collation key (a derived table's RTRIM, TRIM or constant)": why the query has the nested `v` with `RTRIM(label)` |
| `IF ... THEN 'blank-padded =' ELSE 'no' ENDIF` | "CASE of strings of different lengths is IQ's CHAR(14): whether the IQ engine pads it ... is not decided": why the branches are `'Y'`/`'N'` |
| `ISNULL(a.labels, '-')`, where LIST gives a LONG VARCHAR | "ISNULL of a LONG VARCHAR is not supported yet" |
| `SELECT DBA.u.id ... FROM DBA.u` | "schema-qualified column references are not supported yet": the owner qualifies the table only |
| `TOP @n` | "a variable or a parameter as TOP's count is not supported yet" |
| `FROM dbo.u` | "the table dbo.u is not supported: 'dbo' is one of IQ's own users, whose tables are its catalog" |
| the same query as `#IQ:sqla17-default{...}#` | "LIST of strings is supported only where every value is its own collation key (... LOWER): IQ orders 'A' and 'a' as one value": SQL Anywhere's default database is case-insensitive |

The query reads no `CHAR(n)` column either: the IQ engine pads `CHAR(n)` (provisional decision D-S1) and SQL Anywhere
does not, so a comparison over `t.ch` would trip the engine-neutrality guard.

## 10. How it was checked, and the limits

- **lite.** `//core:iq_parity_test` and `//core:iq_parity_postgres_test` run the three cases as islands over the
  schema seeded into DuckDB and an embedded Postgres, and judge them against the snapped answers: PASS on both, the
  guards clean. A negative control: with one cell of the snapped answer changed (`'x  :'` to `'x:'`), the judge
  reports `FAIL Row 4, column 'Tag': expected 'x:' but got 'x  :'`.
- **The pinned SQL.** `IqParityTest.theShowcasePlansItsPinnedSql` compiles `showcase_main`'s island, its `let`
  before it, with both connections and compares `plan(...).sql()` with the two files byte for byte; with a line of
  `snowflake.sql` changed it names the line. The formatted island of §1 and the formatted Pure of §5, each compiled as
  a query, plan the pinned SQL byte for byte (checked once, at `918a9ccc6`; the test pins the case's one-line text).
- **SQL Anywhere.** `tools/iq-oracle/snap.sh --write --only 'lite/showcase/*'`, then `snap.sh --check`, clean.
- **Snowflake.** Rendered only: no Snowflake account was used, so the Snowflake text has not run anywhere. Its
  semantics rest on legend-lite's Snowflake dialect.
- **IQ itself.** No IQ server is available. SQL Anywhere 17 on an IQ-like database (case-sensitive, blank-padded,
  binary ISO-8859-1) is the oracle, as everywhere in the island, and the query avoids rules only the IQ engine shows,
  such as D-S1's CHAR padding.
