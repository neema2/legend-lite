# The `#IQ{...}#` island's census checklist: what to build, in order

Created 2026-10-15 at the user's direction ("how do we stop going off track and focus on these tiers").

**The rule.** Work goes top-down through this list:
- take the first item marked **open**;
- no item from a lower tier, no divergence-allowlist entry (`core/src/test/resources/iq-parity/iq17-divergences.tsv`) and no scale tail while an item above it is open, unless the user directs otherwise;
- every commit names the item it closes (`checklist 0.7`) and updates its status here in the same commit.

**The source** is the world SQL usage census, compiled 2026-10-14 from:
- SF25: the Snowflake VLDB 2025 study of 667M production BI queries;
- RS24: the Amazon Redshift fleet, VLDB 2024;
- GR18: Tableau Public, DBTest 2018;
- SQLShare and the other studies it cites.

The tiers are that study's: usage weighted by the chance that two dialects disagree silently. Shares are from SF25 unless the item says otherwise.

**Each item is judged by the live SAP IQ 17.1** (`iq17-*` profiles). Where IQ's engine differs from SQL Anywhere 17, which judges the `iq16-*` profiles, the decision is a row of `docs/SEMANTICS_REGISTER.md` (S52).

Status words:
- **done**: built and measured;
- **open**: not built;
- **partial**: built for the common forms, the rest refused by name;
- **out**: deliberately not built, with the reason.

## Tier 0: in most real queries, and silently different across dialects

| # | Item | Status |
|---|---|---|
| 0.1 | String comparison: case, collation, trailing blanks; equality and IN filters, join keys, GROUP BY, DISTINCT, MIN/MAX, ORDER BY on text | done. Allowlisted remainders: a tab in blank padding (D-S5); UTF-8 CHAR(n) counting bytes. |
| 0.2 | NULL semantics: IS [NOT] NULL, outer-join NULLs, COALESCE/ISNULL typing, NULL sort position, NOT IN with a NULL member, NULL in concatenation, `''` versus NULL, aggregates over empty and all-NULL input | done. Allowlisted remainder: IS UNKNOWN over NOT IN of a subquery holding NULL. |
| 0.3 | GROUP BY with SUM/COUNT/MIN/MAX: result type and overflow of SUM, AVG of integers, COUNT(*) versus COUNT(col), an aggregate over no rows, GROUP BY on text by its collation | done for every value within a BIGINT. Deferred (the user, 2026-10-15: features before overflow edges): IQ's engine widens an integer result past a BIGINT to a NUMERIC (D-N3, 15 allowlisted runs), where lite's database raises; it matters only past 9.2 × 10^18. |
| 0.4 | ORDER BY plus TOP/START AT: NULL placement, collation order, ties | done. IQ has no LIMIT/OFFSET, and TOP 0 is IQ's error. `ORDER BY (1)`, a position under IQ's engine and a constant on SQL Anywhere's front end, is refused by name under IQ's engine (2026-10-15). |
| 0.5 | CASE/IF, CAST and coercion: branch unification, rounding versus truncation, string-to-number and string-to-date, DECIMAL scale, integer division, a zero divisor | partial. String to date reads the year-first spellings and, measured on IQ 17.1 (2026-10-16), blanks around the separators, a month's English name or a start of it in either order, AM and PM, and under IQ's engine a time alone on 1900-01-01: a computed string passes through linear replacements into one ISO spelling, then is parsed once. Done (2026-10-15, measured on IQ 17.1): a CAST to a shorter VARCHAR or CHAR is cut to n bytes, never inside a character, and CHAR(n) padded to n bytes (D-C2); a narrower NUMERIC, and a DOUBLE to a NUMERIC of at most 18 digits, truncate toward zero (D-N6); a number or a date to too short a text raises for that row (D-C1); a NUMERIC as text, in CAST, `\|\|` and STRING, keeps its 0 before the point (D-C8); SIGN and TRUNCNUM keep their argument's type (D-F19). A string read as an INTEGER or a NUMERIC by IQ's engine's own reading (white space around, a sign, digits with one point; no exponent; '' its error), cut toward zero. CASE, IF and COALESCE of a string and a number are IQ's error -1009145, refused. Refused by name: `CAST(x AS NUMERIC)` without a precision (a NUMERIC(126,38) under IQ's engine), a DOUBLE to a NUMERIC of more than 18 digits, a NUMERIC constant or a DOUBLE as text, a string that is no constant read as a DOUBLE (IQ's reading is not correctly rounded: '1.555' is 1.5550000000000002). Nothing open. |
| 0.6 | Joins, inner and outer: ON versus WHERE under outer joins, text and NULL keys, legacy outer-join syntax | done (2026-10-15): the T-SQL `*=` / `=*` outer joins as IQ 17.1 reads them (its PUBLIC tsql_outer_joins is On; Off, IQ's -1099): a WHERE condition that reads the NULL-extended table joins, the rest filters; a chain (-680), a cycle (-136) and an ANSI join beside them (-681) are IQ's errors, refused; under OR refused. Out: two output columns of one name (S73). KEY JOIN (IQ's foreign-key join) is IQ's own, and comes after Tier 0. |
| 0.7 | Dates: EXTRACT/DATEPART, DATEDIFF's boundaries, DATEADD at month end, week and weekday numbering, CURRENT DATE, fractional seconds, date arithmetic | done (2026-10-15): EXTRACT, DATEPART/DATENAME with IQ's calendar weeks (from Sunday, D-T10), DATEDIFF, DATEADD, date ± days and date − date, strings read as dates, DATEFORMAT (its tokens as measured; another refused by name), DAYNAME, MONTHNAME, DATETIME, YMD. LAST_DAY is not IQ's (-265). |

## Tier 1: common, with moderate risk

| # | Item | Status |
|---|---|---|
| 1.8 | CTEs and derived tables: scoping; a scalar subquery of more than one row raises | done. WITH RECURSIVE (2026-10-17, measured on IQ 17.1, 91 shapes): IQ's error as IQ runs it (-706 a recursion read beside an IQ table, -922 no column list, -1001030 a WITH RECURSIVE that recurses nowhere over IQ's tables alone; plain CTEs where a DUMMY or a query without FROM makes SQL Anywhere run part of it). A recursion IQ answers (over no IQ table, and in SQL Anywhere's front end) waits for the Cypher island's recursion native (`meta::legend::lite::recurse`, branch `claude/keen-noether-v5mis0`) to reach main: the island will lower to it, not build a second one. |
| 1.9 | IN lists: NOT IN and NULL, long lists | done (20,000 values). A column in the list is IQ's -1001030, refused. |
| 1.10 | String functions: LIKE (case, ESCAPE, blanks), SUBSTRING with a start of 0 or a negative start, LEN/LENGTH and blanks, CHARINDEX/LOCATE, `+` versus `\|\|`, TRIM, UPPER/LOWER | done (2026-10-15, measured on IQ 17.1): REVERSE, INSERTSTR, BYTE_LENGTH (UTF-8 bytes), GREATER/LESSER and ARGN (typed as CASE; the second of equals), POWER under IQ's engine (NULL where it has no DOUBLE); LENGTH and BYTE_LENGTH of an integer column, its storage width (D-S7). Refused by name: LENGTH of a date or a timestamp (a storage length varying with the value), SOUNDEX and DIFFERENCE (SQL Anywhere's own numeric code). NUMBER(*): the result's rows numbered in its order, before TOP and START AT (as ROW_NUMBER over the ORDER BY); IQ's -864 and -1001030 placements refused. REGEXP_SUBSTR: the first match from a start, for patterns of single-character atoms with greedy quantifiers; refused by name: alternation and groups (IQ matches leftmost-longest, DuckDB leftmost-first), the backslash and POSIX classes (IQ's are Unicode's), an occurrence past 1 (IQ counts it from one character past the last match's start), an escape character. |
| 1.11 | Window functions: ranking ties, the default RANGE frame, LAG/LEAD defaults, NTILE | done |
| 1.12 | UNION / UNION ALL: type and length unification, deduplication by the collation | done |
| 1.13 | OR, NOT, `<>`, ranges, BETWEEN with reversed bounds | done |

## Tier 2: rarer, or of less risk

| # | Item | Status |
|---|---|---|
| 2.14 | EXISTS and IN subqueries, semi and anti joins, correlated subqueries | done, with IQ's engine's positions refused as IQ refuses them |
| 2.15 | COUNT(DISTINCT) and DISTINCT, by the collation | done |
| 2.16 | Statistical and list aggregates: STDDEV and VARIANCE (sample, population), MEDIAN, PERCENTILE_CONT/DISC, LIST's order (GROUP_CONCAT) | done, the rest refused by name. Done (2026-10-15, measured on IQ 17.1): PERCENTILE_CONT and PERCENTILE_DISC (WITHIN GROUP; IQ's errors for them raised before translating), MEDIAN of a NUMERIC (its type, the midpoint cut toward zero: D-F20) and of a DOUBLE. Refused by name: STDDEV and VARIANCE (IQ's engine computes them by a lossy formula of its own: VARIANCE of 1000000.1, .2, .3 is 0.0100 there, 0.01 exactly), PERCENTILE_DISC of a BIGINT, a NUMERIC of more than 15 digits, a string or a date, MEDIAN of a BIGINT (IQ's midpoint wraps). LIST ordered by another column, IQ's NULL first ascending and last descending; two keys, and two LISTs of one query ordered differently (D-A1: IQ may order both alike), refused. |
| 2.16b | Subtotals: ROLLUP, CUBE, GROUPING SETS, GROUPING() (BI tools' totals) | done (2026-10-16, measured on IQ 17.1). GROUP BY ROLLUP(...), CUBE(...), WITH ROLLUP, WITH CUBE and ( ) are read as a UNION ALL of their groupings. A key not grouped by is NULL of its type, and GROUPING(k) is 0 or 1. Keys may be select-list aliases (an alias before a column of its name). A grouping by nothing is one row even over no rows. TOP, DISTINCT and HAVING apply to the whole. IQ's errors, raised: GROUPING SETS, a key beside ROLLUP or CUBE, PERCENTILE beside CUBE, MEDIAN and LIST beside either (-1001030), GROUPING of no key (-149). Refused by name: ROLLUP or CUBE in a subquery, a derived table or a set operation's branch; CUBE of more than 6 keys; SELECT *; a window function beside it; an ORDER BY key that is none of the items. |
| 2.16c | CROSS APPLY, OUTER APPLY, LATERAL | done (2026-10-16, measured on IQ 17.1). IQ's engine answers APPLY of a query that reads nothing before it (CROSS APPLY a join, OUTER APPLY a LEFT JOIN on a true condition), of a table, of a FROM function, and of computed columns: a query of one row without FROM that reads the row before it, `CROSS APPLY (SELECT t.id * 2 AS d) x`, chained or joined after, OUTER as CROSS. Lite translates those as an extend of the left relation, in `SqlToPure`, so the Postgres island's FROM-less LATERAL is answered too. IQ's errors, raised: a query with FROM that reads the row before it is -1001030 (a lateral derived table, a correlated UNION) or the internal error -1006000 (an aggregate, DISTINCT, a window); LATERAL is -1001030; VALUES is -131; an APPLY without a name is -131. Refused by name: a FROM function's argument read from the row (sa_rowgenerator(1, t.id)). |
| 2.17 | Session and metadata statements (31% of BI statements) | out: the island reads one SELECT and refuses IQ's catalog by name (standing rule) |
| 2.18 | DML and CTAS | out: the island is the read path |

## Scale (the census's §3 budgets)

| Shape | Census budget | Status |
|---|---|---|
| Joins | 0-50 routine, 100-350 stress (outlier p99 352; 247 outer joins in one Tableau query) | done: 352 joins plan in 1.3 s, 500 in 3.7 s. Above 1,000, DuckDB's parser depth (0.0003% of joining queries). |
| SQL text | p99 16 KB; 1 MB, 16 MB and 27 MB stress | done: 1 MB in 2.3 s, 16 MB in 21 s (translate + compile + plan) |
| Projection expressions | 1,000 routine; 8,000 and 290,000 stress | done: 16,000 items in 3-7 s |
| Expression depth | past 100; recursive descent survives 1,000 | done for CASE (one flat CASE) and measure sums. Parentheses nested 1,000 deep are DuckDB's own limit. |
| GROUP BY keys | more than 10 | done (50) |
| UNION ALL | a chain of 127; over 1,000 inputs as the extreme | done to 500. Over 1,000 inputs, DuckDB's parser depth. |
| IN lists | 1 to 10,000+, with NULL and NOT IN | done (20,000) |
| Measure sums | 10 to 50 terms | done for integers and DOUBLEs (100 terms). Open: NUMERIC(18,2) past ~20 terms, where IQ's typing outgrows lite's 38 digits. |

## The real-world corpus lane (2026-10-15)

The census ranks features; the corpus lane measures the island against real queries. `IqCorpusTest`
(`//core:iq_corpus_test`, manual) reads every query of each suite under `-Diq.corpus.root` as an island over the suite's
tables (empty, on DuckDB) and ranks what it refuses. The corpora stay private (licences, size): SEDE (11,708 Stack
Exchange Data Explorer queries, T-SQL), SQLStorm v1.0's SQL Server set (18,251), the Public BI benchmark (646 Tableau
queries), BIRD Mini-Dev (500). A query is ANSWERED, REFUSED (by name), IQ_ERROR (the island says IQ raises an error,
its syntax error or an error it names), DB_ERROR or CRASH.

| Suite | Answered before | Answered now | IQ's own error now | Refused by name now |
|---|---|---|---|---|
| SEDE | 40.6% | 56.8% | 15.5% | 27.4% |
| SQLStorm | 21.7% | 30.7% | 50.7% | 18.6% |
| Public BI | 93.0% | 94.4% | 2.6% | 2.9% |
| BIRD | 58.2% | 69.2% | 18.4% | 12.4% |

(2026-10-16. Answered fell a little where former wrong answers are now refused (the windowed derived tables, below).
IQ's own error now counts each refusal that names IQ's code, measured. In SQLStorm, 5,724 queries end in T-SQL's
OFFSET ... FETCH, IQ's syntax error -131. What lite does not answer is mostly SQL IQ itself rejects.)

Measured on the live IQ 17.1 and built (parity cases: `lite/corpus_gaps.yaml`):
- a statement's trailing separator (1,691 of SEDE's queries);
- COUNT's UNSIGNED BIGINT beside other numbers. With a signed integer IQ shows a NUMERIC(20,0) that it computes as a whole number (its `/` divides whole); with a NUMERIC, a NUMERIC of its 20 digits; with a DOUBLE, a DOUBLE. This covers COALESCE, ISNULL, CASE, NULLIF, CAST, SUM and AVG of counts, and NUMERIC and DOUBLE arithmetic (about 1,300 queries);
- NULLIF of numbers, by COALESCE's type;
- a DATE compared with a TIMESTAMP, at its midnight (227);
- a reading of the clock moved by DATEADD (225);
- ROUND of a NUMERIC past 18 digits;
- a quotient or a ROUND whose IQ type passes 38 digits (`COUNT(id) * 100.0 / COUNT(*)`, IQ's NUMERIC(44,21)): computed where the value fits, refused at run time where it does not;
- national strings (`N'...'`);
- GROUP BY expressions that read no select-list alias;
- REAL and FLOAT, IQ's single precision (`lite/iq_reals.yaml`), rounded by Veltkamp's split in double arithmetic: FLOAT and FLOAT(1..24) are REAL, FLOAT(25..53) DOUBLE; a value past the largest single is IQ's error, one below the smallest normal 0. Two REALs give a REAL in SQL Anywhere's front end, a DOUBLE (unrounded) under IQ's engine. A REAL with another number is a DOUBLE, in COALESCE, CASE and UNION too. SUM and AVG are DOUBLEs, MIN and MAX REALs;
- an aggregate's DISTINCT over an expression, and one in a SELECT DISTINCT, under IQ's engine (the research's D-R1 held that IQ restricts them; IQ 17.1 reads them);
- ROUND of a DOUBLE or a REAL. IQ's steps are the sign times floor(|x| * 10^n + 0.5) / 10^n, each step a DOUBLE: ROUND(2.675e0, 2) is 2.68 and ROUND(1.005e0, 2) 1.0. This holds for its front end and SQL Anywhere 17 alike;
- several clock readings in one statement under IQ's engine, which reads one clock. D-T13 is SQL Anywhere's front end's;
- the names IQ gives unaliased CASTs and CONVERTs. A whole item is named as its operand (`CAST(t.ts AS DATE)` is `ts`). One inside an item is written out in lower case: `cast(t.sm as numeric(10,2))`, `convert(varchar(10),t.sm)`. The type keeps its written spelling, except that INT is `integer`, DOUBLE PRECISION is `double` and FLOAT is `real`. This fixed 34 of the research's naming cases, under both judges;
- LIKE's `[set]` classes of ASCII members, ranges and a leading `^`;
- DATALENGTH: a string's bytes, and a column's storage width under IQ's engine;
- a table's `WITH (NOLOCK)` and `WITH (READUNCOMMITTED)`, read and dropped. After a derived table or a FROM function a hint is IQ's -131;
- a constant date string beside a DATE or a TIMESTAMP in COALESCE, ISNULL or CASE, read as one. `COALESCE(ts, '1900-01-01')` is a TIMESTAMP.
- a string function's count or position computed in 64 bits or a BIGINT (2026-10-17, 23 queries, `SUBSTRING(s, 2, LEN(s) - 2)`): read. Postgres's dialect narrows it to the 32-bit integer its substr, left, right, repeat, lpad and rpad take (`StringCountsAsInt`). A value past 32 bits is refused by name where the row computes it: IQ raises for a computed one and gives `''` for a BIGINT column's extremes. CHAR of a computed code stays refused (DuckDB's chr).
- a year alone, or a year and a month, as a date (2026-10-17): the month's first day. `'2010'`, `'2010-05'`, `'2010/5'`, `'2010 May'` and `'May 2010'` are read in comparisons, CAST and CONVERT, with a time after them or not.

Measured to be IQ's own errors, raised or named by the island:
- `OFFSET ... FETCH`, `LIMIT`, `INTERVAL` and `FILTER` are -131;
- `STRING_AGG`, `CONCAT`, `EOMONTH`, `FORMAT` and `STRING_SPLIT` are -265, though a user function could hold the name, so these stay refused by name;
- the T-SQL date parts `yyyy`, `d`, `m`, `ww`, `n`, `s` and `q`, `x.Comment`, `x.Date`, `x.Key`, `x.Order` and `x.User` (reserved words after a dot), and `Date`, `Comment`, `time` and `Key` standing alone are all -131;
- under IQ's engine:
  - TOP in any subquery, derived table or common table expression is -1001030 (the research's D-R1 had held that IQ may ignore it);
  - a subquery inside a conditional expression is -1001030;
  - a subquery in an outer join's ON is -1001047 unless its conjunct reads the null-supplying side alone (directly and through correlation; in a FULL JOIN always): 21 shapes measured. It is -1001047 too where no conjunct without a subquery reads a column beside it (2026-10-17: `ON u.id = (SELECT MAX(id) FROM u)` alone, or beside `1 = 1`; SQLStorm's 6 such queries over a common table expression);
  - an integer where DATEADD or DATEDIFF reads a date is -1001011;
  - CAST to NVARCHAR or NCHAR is -1001030;
  - a number compared with a string (`PostTypeId = '1'`, `YEAR(ts) = '2026'`, in IN, BETWEEN and CASE alike), and a date or timestamp compared with a number, is -1001013. A string that is no number is the front end's -157 first.
- numbers ending in a four-digit year (`'01/01/2018'`, `'8-16-2011'`, `'05/2010'`) are no date under DATE_ORDER YMD (2026-10-17, 55 of SEDE's queries). Compared, IQ's engine raises -157 beside a DATE column and -1009145 beside a TIMESTAMP or a computed DATE; SQL Anywhere's front end raises -157. In CAST and CONVERT, where IQ's engine raises -1001006, they stay refused by name, as every constant IQ cannot convert is.

Open, ranked by the corpora:
- CAST to DECIMAL without a precision, IQ's NUMERIC(126,38) (70);
- a subquery in a derived table's select list (192): built 2026-10-16, measured on IQ 17.1 over 40 shapes. IQ's engine reads such a subquery as if it were written where its column is read. Answered: unqualified reads in the select list (in arithmetic and functions too), in ORDER BY, under DISTINCT and TOP, and the derived table's other columns anywhere. IQ's errors, refused naming them: a qualified read of any of its columns (-1005004); the subquery's column in WHERE, GROUP BY or HAVING, or inside a conditional or an aggregate (-1001030). Still refused by name: a join beside it, a window over the column, and a nested derived table;
- a statement with TOP whose select list can raise (114): built 2026-10-16 (the user's decision), measured on IQ 17.1,
  SQL Anywhere 17 and Postgres 16 (`lite/corpus_gaps.yaml`'s TOP cases, `sql-parity/lite/math.yaml`).
  - IQ computes the items its ORDER BY does not read only for the rows it keeps: under its engine the rows up to the
    limit's last (START AT's skipped rows too), in SQL Anywhere (its front end, and SQL Anywhere 17) the rows it
    returns. ORDER BY's items, and all of a DISTINCT's, are computed for every row. Grouped queries and windows alike.
  - `SqlToPure.assemble` computes those items after the sort and the limit (`InputDialect.rowLimitItems`), and the
    Lowerer no longer merges an extend after a limit into the truncated select (`Fold.extendFolds`), which made the
    database compute it for every row first. Both lanes answer every measured case.
  - Postgres differs: it computes a cheap item (a division) for every row before its sort, so the Postgres island
    keeps that order;
- the outer join ON shapes IQ reads but DuckDB cannot plan: a subquery in a RIGHT JOIN's ON (refused by name). Built 2026-10-17: a LEFT JOIN's ON conjunct holding a scalar subquery and reading the null-supplying side alone (`ON u.t_id > 0 AND u.id = (SELECT MAX(id) FROM u)`) filters that side before the join in DuckDB's dialect (`OuterJoinSubqueryFilters`), which DuckDB cannot plan otherwise. SQLStorm's 6 queries once listed here are IQ's -1001047 (above);
- CROSS APPLY (676), which this round measured only as IQ's internal error on one correlated shape (2.16c).

The refusals the live IQ answers (2026-10-17): every query the island refused, or that lite's database failed, over
the dba.stackexchange sample was sent to the live IQ (`refused_suite`, 1,599 queries; IQ answers 582). By the
island's reason, those IQ answers, and what became of them:
- built this round: a second statement without a separator, named (85 of 108; out: the island reads one SELECT);
  month-first dates, IQ's error (55 of 61) and a year and a month, read (6); a string function's 64-bit count (23);
  LIKE's sets of any member (13 of 17); `*` in a grouped query showing its group keys (7); a GROUP BY of constants
  only, NULL too (4: IQ 17.1 reads it, where the research's D-R1 held that its engine restricts it); LOG and LOG10
  of a COUNT and STR of NTILE, IQ's unsigned results (13); DATEADD by a computed count, refused at run time past
  SQL Anywhere's bound only (10); NULLIF of a date and a date string (3); an IN subquery that is a UNION (5 of 8:
  INTERSECT and EXCEPT there are IQ's -1001030). Judged against IQ 17.1's stored answers: 110 PASS where 0 did;
  the rest are ties of ROW_NUMBER. A CONVERT style to a shorter string stays refused: IQ's engine cuts it in some
  places and raises -1009145 in others (a styled result compared with a string, CONVERT(CHAR(6), d, 112) ranked);
- measured once the live IQ returned (2026-10-17): IQ 17.1 reserves four words SQL Anywhere 17 does not, as no name
  anywhere (`aes_decrypt`, `clear`, `iq`, `timeline`: SEDE's `AS Timeline`), now the iq17 profiles' keywords; its other
  extra words of sa_reserved_words() are names (true and false only as values). A CONVERT style's text longer than
  its string is IQ's error for each row computing it (-1009145 under its engine, -638 in its front end; none over no
  rows), raised so, where the corpus's earlier PASS were queries no row reached. IQ's engine reads a DOUBLE from a
  17-digit string one unit off in 6 of 267 Tableau-style constants (its front end exactly), so such a CAST stays
  refused: an equality filter on it may match no row in IQ. A DOUBLE sum's overflow is its engine's -1001010 in every
  group, those TOP drops too (the paused AVG decision, measured);
- DATE, YEAR, DATEPART and DATEADD of a computed string (BIRD's `DATE(SUBSTR(s, 1, 10))`): read as CAST to
  TIMESTAMP reads it under IQ's engine (measured: the same values, its -1001006 alike);
- TRUE and FALSE (BIRD's `THEN TRUE ELSE FALSE`): IQ 17.1's constants 1 and 0, SMALLINTs as the literals are
  (`TRUE + 1` is 2), named `true` unaliased, no alias; the iq17 profiles read them so, and the listed true-keyword
  divergence is gone. NULLIF of a date and a string that is no date is its engine's -1001014 whether or not a row
  reaches it (it converts the constant as it plans; its front end's NULLIF(d, '') is d);
- a date string IQ cannot convert names both codes rather than one: which IQ reports is its evaluation order's (a
  DATEPART of a string's -1001011 may come first; a BETWEEN over a computed DATE in a grouped query is -157);
- lite's database failing on SEDE's empty sample tables (27 of 40): each answers over the data, PASS; the other 13
  are IQ's errors too (division by zero, the outer join ON rule above);
- AVG or SUM read for some groups only (39, Tableau's TOP n ... AVG(CAST(x AS double)) among them): built
  2026-10-17 where the statement's own SELECT reads it so (a CASE, a COALESCE, its row limit). IQ checks the running
  total for overflow in every group (D-E3; measured on IQ 17.1, its engine's -1001010 for a DOUBLE in a group TOP
  drops): the check is now also a filter of every group after the aggregation (`Aggregation.guard`), which no read
  pattern skips. Still refused by name: an aggregate of a derived table, a set operation's branch or a subquery, where
  a WHERE on a group key around it may reach below its aggregation;
- out, the 38 digits of lite's DuckDB decimals: CAST to DECIMAL without a precision (64, IQ's NUMERIC(126,38)), AVG
  of a wide NUMERIC (53, IQ's NUMERIC(127,59): '9.83333...' to 59 places), a NUMERIC quotient or product past 38
  digits (46), CAST of a NUMERIC to more places (8). IQ answers them exactly; DuckDB holds no decimal past 38 digits
  (Postgres's numeric would), so lite refuses by name rather than answer other digits. Decided, the user (2026-10-17):
  the targets' ceiling decides it. Per their documentation: Snowflake's NUMBER holds 38 digits (scale at most 37);
  its DECFLOAT 38 significant digits exactly, rounded half up past them; DuckDB's DECIMAL a width of 38 (an operation
  needing more is an error, a DECIMAL division a DOUBLE); Postgres's unconstrained numeric 131,072 digits before the
  point and 16,383 after. IQ's averages and quotients here run past 38 significant digits (CAST(32767 AS DECIMAL) / 3
  is 10922.333... to 40 places), so no target but Postgres holds IQ's answer, and a rounded one is a wrong one:
  refused by name, not built for Postgres alone;
- an expression nesting too many checked operations (39 of 44, a scale bound): an integer quotient is lite's own
  native now (`meta::legend::lite::quotient`, the database's integer division), where the islands' Float estimate read
  each operand about fourteen times (`CONVERT(int, LEN(x) * (1 + y) / 1000)`: 30 KB of SQL to 600 bytes); 5 more
  answer. A scalar subquery a select item computes on every path is computed once now, a column before the item
  (`SqlToPure.output`, both SQL islands): a correlated COUNT under a DOUBLE quotient was written 504 times (187 KB,
  1,520 SELECTs; now 86 KB, 10), and one more answers; the SE judge is unchanged but ties (PASS 9,932 -> 9,935). The
  rest repeated a long operand through checked operations' NULL and zero tests. Built 2026-10-17: an item past the
  size bound has its long operands (64 Pure nodes or more, read on every path) each computed once, bottom up, a
  column before it (`SqlToPure.output`), and the lowering writes a large computed column (48 SQL nodes or more) that
  later columns read twice in its own select rather than into each read (`Fold.rereadsLarge`; 2026-10-18, landing
  5's review: removed, main's fold budget, `Fold.Budget`, writes such a column once since the merge). 32 of the 44 answer
  (sede-13007: 9.6 KB of SQL, where the bound lifted wrote 1.5 MB); 8 pass the bound still; the SE judge unchanged
  but ties;
- measured and built in the landing's later round (2026-10-17, each snapped from IQ 17.1): PATINDEX of LIKE's `_`,
  `[set]`s and a `%` inside (`'%[^a-z]%'`, `'[^?.]'`): a pattern not starting with `%` is LIKE's test, one starting
  with it the first place from which the rest matches, held to the end without a trailing `%` (a regular
  expression's first match, `regexpIndexOf`); ASCII in a UTF-8 database, under IQ's engine the first UTF-8 byte
  (`ASCII('é')` is 195), in the front end NULL past 127; SQUARE, which is POWER(x, 2) (NULL past the largest DOUBLE
  and where it underflows; SQL Anywhere 17 answers Infinity there, so its front end is read in the iq17 profiles
  only); a plus before a string, the string; CONVERT to DATE or TIMESTAMP with a style: a constant string read by
  the style's order (103 day first, 101 month first, 102 year first; a style past 100 reads the year as written, one
  below it two digits by NEAREST_CENTURY), a computed one by 20, 21, 120 and 121 under IQ's engine (`yyyy-mm-dd` and
  `hh:mm[:ss]` only: IQ rejects `2009-11-7` and blanks there, and reads seconds past 59 by its own rules), a date
  with a style IQ's engine's -1009145, a national type its -1001030; a date compared with a string computed of
  constants (`'2100-12-31' + ' 23:59:59'`), as CAST reads it, where one reading a column is IQ's -1001013. Beside a
  DATE, IQ's engine reads a compared string as a DATE, its time dropped (`dt = '2026-03-04 12:00'` holds); SQL
  Anywhere's front end and SQL Anywhere 17 read it as a TIMESTAMP;
- out: the name IQ gives an unaliased expression (42): IQ prints it after its own rewrite, derived tables merged,
  an inner alias folded in as `x as alias`, generated names (`u_2__`) and spacing (`COUNT( u.id)`), none
  of which the query's text holds. Not built yet: RAND (8, in ORDER BY: no answer to judge). CONVERT's styles 6,
  7, 8, 9, 109, 13, 113, 14 and 114 are read now (`:mmm` milliseconds, the hour of twelve and AM or PM for 9 and
  109); 22 to 25, 126, 127, 130 and 131 are IQ's engine's -1009145 ("CONVERT format style ... is invalid"),
  refused by name;
- the lane's IQ_ERROR claims verified (2026-10-17): lite names an IQ error for 14,275 of the corpora's queries,
  none of them sent to IQ before. A sample of 906, up to 12 for each of the 138 reasons, was sent to IQ 17.1 over
  the dba.stackexchange sample. IQ raised the code lite names for 730; another error first for 163 (both reject:
  a GROUP BY error before STDEV's -265, a reserved word elsewhere in the text); and answered 12, lite's false
  claims, built now except where noted:
  - a window's PARTITION BY or ORDER BY over a GROUP BY expression (`PARTITION BY YEAR(CreationDate)` beside
    `GROUP BY YEAR(CreationDate)`, 3), which IQ's correlated-reference check of a window read as -149;
  - `SELECT DISTINCT u.* ... ORDER BY u.displayname` (2), a column the `*` shows;
  - `AS row` (1): IQ 17.1 reads six words SQL Anywhere 17 reserves as names everywhere (array, json, row, rowtype,
    unnest, varray; `IqLexer.IQ_NAMES`, the iq17 profiles);
  - an outer join's ON subquery under a WHERE that rejects the null-supplying side's NULLs (2): IQ reads the inner
    join that it then is (`u.id > 0`, IS NOT NULL, IN, BETWEEN, LIKE of its column; not IS NULL, an OR or a
    COALESCE). Built for the WHERE of the join's own query, read as an INNER JOIN; not built where an enclosing
    query's join rejects them through a grouped common table expression (SQLStorm's 11558): IQ's error here;
  - not built: FORMAT in a derived table's column the outer query never reads (4). Measured on IQ 17.1: IQ drops
    a derived table's or a common table expression's columns no one reads before it plans them, so nothing in them
    raises (an unknown function's -265, a subquery's -1001030, a CAST's -157, a division by zero), but names still
    bind there (an unknown column is -143), and a DISTINCT or a `*` reads every column. Lite refuses an unknown
    function as it parses, and translates every column: pruning the unread ones first (where every column they
    read is known) would read these, the parser's function checks moved after it;
- a derived table's select-list subquery, reworked (2026-10-17): the lane's 154 such refusals sent to IQ 17.1, which
  answered 5 and raised -1001030 or -1005004 for the rest, then 20 shapes more measured. A column holding a subquery
  that no one reads is dropped by IQ, through nested derived tables, so it raises nothing. All 5 answers were such
  columns.
  - A read column is -1001030 in a grouped, aggregating or DISTINCT derived table (`*` reads every column), and in
    WHERE, GROUP BY, HAVING or a join's ON.
  - It is -1005004 read qualified (`d.n`; another column qualified is fine), or ordered by with no select item
    reading it.
  - Read in the select list it answers: beside a join, beside the outer query's own subquery, and through nested
    derived tables (`n + 1` shown and read above).
  - A derived table computing a window function is planned whole (9 of the 154): a column holding a subquery is
    its -1001030 there, read or not, through a `*` too. Held columns pass up through stars and qualified reads.
  - Judged: all 154 now match IQ, 149 naming its error and 5 answering.
  - Refused by name: one read inside the outer query's own subquery (IQ's -1001030 in the case measured), or in a
    window;
- other dialects' syntax, IQ's syntax error -131 now (2026-10-17, measured; the syntax research's `reject` cases):
  CAST to DATETIME2 or INTERVAL (other dialects' type names, unless a user-defined domain holds the name; 54
  queries), an aggregate's `FILTER (WHERE ...)` (12), TRIM's ANSI forms `TRIM(BOTH '<>' FROM s)` (14). FOR XML
  PATH and FOR JSON are -131 too, but the research reads FOR XML's other forms: still refused by name;
- a name of no function of IQ's (STRING_AGG 2,205 queries, CONCAT 240, STDEV 30, ...; 2026-10-17, measured): read
  as a call. A syntax error in its arguments comes first (-131, `NVARCHAR(MAX)`), then a grouped query's column
  outside the GROUP BY (-149; IQ reads the name as no aggregate), then its -265, raised as it is translated, where
  every one was -265 before (its sample: 1 of 12 STRING_AGG claims matched IQ's code). The aggregates' forms after
  the name (DISTINCT, an ORDER BY inside, WITHIN GROUP, OVER) stay refused by name, as -265;
- IQ's own codes for three more errors (2026-10-17, from the IQ_ERROR verification, measured):
  - nested aggregates are -156 (`InputDialect.Problem.NESTED_AGGREGATE`, where the shared translator wrote
    Postgres's text);
  - LIKE over a date or a timestamp converts the pattern first, so one that is no date (`'%2015%'`) is -1009145,
    and one that is a date -1001013;
  - a subquery's reference to a column that is inside a GROUP BY expression but no GROUP BY item itself
    (`month(p1.CreationDate)` beside `GROUP BY month(CreationDate)`) is -1005028, where a column in no GROUP BY
    expression stays -149;
- TOP n PERCENT (9 queries; 2026-10-17, measured): read as IQ reads it, TOP n and then a column named PERCENT. A
  table of no such column is its -143, and `TOP n PERCENT *` its syntax error, where lite refused it by name;
- the IQ_ERROR verification again after these rounds (the same 906 queries): IQ raises the code lite names for 813
  (from 730), another for 69 (from 163), answers 5 (from 12; FORMAT in unread columns, 4, and SQLStorm's 11558,
  whose join is made inner by an enclosing query's join). SQLStorm's coverage: IQ_ERROR 12,078, REFUSED 358 (from
  601);
- FROM items IQ reads no other way (2026-10-17, measured): VALUES as a FROM item, and a column list after a
  table's, a CTE's or a function's correlation name (`STRING_SPLIT(...) AS t(name)`, `[u] (NOLOCK)`), are its
  syntax error -131, where only a derived table takes a column list. An index hint there (`r (INDEX i)`) is SQL
  Anywhere's, refused by name. A call of a name that is no system procedure in FROM (`FROM u (NOLOCK)`) is its
  -265;
- IQ's order among its errors (2026-10-17, from the IQ_ERROR verification): a row limit in a subquery or a derived
  table (-1001030) before an outer join's ON subquery (-1001047; 7 of 7), and a held item's own subquery inside a
  conditional expression or an aggregate (-1001030) before a qualified read of it (-1005004; 4 of 4);
- TO_CHAR and LIMIT (BIRD's TO_CHAR(date, 'YYYY'), 19, and LIMIT, 13 beside SEDE's and SQLStorm's 18; 2026-10-17,
  measured): IQ's TO_CHAR is SQL Anywhere's conversion of a binary string by a character set, so of a date it is its
  -157 as it plans, and of a string the string. LIMIT is a name under the reserved_keywords option's default, so a
  row limit spelled so is its syntax error -131, in a subquery and after a table too. With these, BIRD's errors
  are named as IQ names them, except a few: SQLite's `Match` table (IQ's reserved word, which the answers driver
  delimits, 11), a Postgres `::` cast, a 38-digit CAST and a table of no island (one each);
- the landing's review (2026-10-17; each measured on IQ 17.1 and pinned in `corpus_gaps.yaml`): a derived table's
  subquery column read under another subquery, or held by a DISTINCT derived table, is refused or IQ's -1001030 (not
  read as read once); a NATURAL or USING join over one is refused; a column list beside `*` names no item; a grouped
  outer query's qualified key is matched by its qualifier; CONVERT styles 20 and 21 read a four-digit year; TRIM's
  ANSI form needs a string or FROM after BOTH, LEADING or TRAILING (a column named so is a column). The review's
  claim that a string `+` keeps an outer join outer was measured false: under WHERE, `b.label + 'x' = 'x'` drops the
  rows the join supplies NULLs for, though a stored NULL concatenates as ''; `||` keeps them (both pinned);
- a subquery in GROUP BY (SQLStorm's 9; measured on IQ 17.1, 2026-10-17): IQ's -1001030 under its engine,
  correlated or not; the front end's reading is not built;
- a function of no name of IQ's (FORMAT, STRING_AGG, ...) in a derived table's column no one reads (SEDE's 4, found
  by verifying the claims, and 23 more; measured on IQ 17.1, 2026-10-17): IQ drops the column before it resolves its
  functions or computes its values (1 / 0, CAST('x' AS INT) raise nothing) and answers; its names still bind (-143)
  and a grouped table's columns are still checked (-149); read, or under DISTINCT, its -265. Where the column is the
  call alone, of a string's result, read nowhere, the island reads the call as a NULL string (its arguments
  translated, so -143 and -149 stand); otherwise refused by name. Open: dropping any unread column, values and all;
- other dialects' argument forms (CHARINDEX's start, ROUND's third argument, LOG's base; the corpus lane's 21;
  measured on IQ 17.1, 2026-10-17): IQ's -154 under the iq17 presets, before a missing column's -143, a conversion's
  -157 and a grouping's -149; the iq16 presets keep the refusal (the SA research holds some accepted);
- more names of no function of IQ's (VAR, VARP, STDEVP, UNNEST, SUBSTRING_INDEX, SPLITPART, PARSENAME,
  DATEDIFF_BIG; the corpus lane's 19; measured on IQ 17.1, 2026-10-17): its -265 (DATEDIFF_BIG's part a column, -143);
  DB_NAME() answers the server's own name and COUNT_BIG is its -1001030 under its engine (both left refused);
- a column of a GROUP BY expression read outside it (SEDE's 20; measured on IQ 17.1, 2026-10-17): a column
  qualified or not is one there (CAST(d.ts AS DATE) under GROUP BY CAST(ts AS DATE): answered, 4 of the 20); where
  no expression around the column is a key even written otherwise, IQ's -149 (the other 15); one that is a key with
  its + or * operands in another order (1 + k under GROUP BY k + 1, which IQ answers) stays refused by name;
- a reserved word qualifying a column (SQLStorm's top.n) and a reserved type's name standing as a value
  (SEDE's TRY_CONVERT(int, ...); measured on IQ 17.1, 2026-10-17): IQ's -131 (datetime, timestamp, money and text are
  names there, a column's -143); CONTAINS's correlation name (contains.score) stays refused;
- CONVERT's styles under IQ's engine (the corpus lane's style 126, 8; every style from -1 to 400 and 1000 measured
  on IQ 17.1, 2026-10-17): it writes a date or a timestamp in 0-14, 20, 21, 36-40, 100-114, 120, 121, 136-140 and
  365 (36-40 and 136-140 to the microsecond, 365 yyyyjjj), now all read; every other style is its -1009145 as it
  plans, over no rows too. Its front end reads 126 and 127 as 121 (not built: the island refuses its styles);
- known, D-N3 (the judge's NUMERIC places, 9 of the tiebreak's remaining FAILs): IQ's engine types arithmetic on a
  column by its data's range (n + 1.0 is a NUMERIC(3,0) over the sample, an INTEGER column); over tables
  holding each type's extremes, as the parity tables do, its types are the declared ones', which the island's are;
  under its engine an integer column enters + - * with its own type's digits (100.0 * sm a NUMERIC(8,0), sm + 1.0
  a NUMERIC(6,0)), now read so, and so the quotient over it (it / (sm + 1.0) a NUMERIC(17,7));
- a subquery in the statement's ORDER BY (the corpus lane's 4; measured on IQ 17.1, 2026-10-17): IQ's -1005004 under
  its engine, correlated or not, shown as an item or not, in arithmetic too;
- other dialects' forms that are IQ's -131 (measured on IQ 17.1, 2026-10-17): FOR XML PATH (its FOR XML reads RAW,
  AUTO, EXPLICIT; the lane's 15), a date part after a value (ts - 1 YEAR, - (2) DAY, DAYS too), a type's name before a
  string (DATETIME2 '...', DATETIME, SMALLDATETIME, TIMESTAMP; DATE and TIME are reserved words there already), AS
  after a function's argument (but an XML function's or TREAT's), T-SQL's query hints in OPTION (MAXRECURSION 0,
  RECOMPILE: IQ's OPTION sets options, name = value); SPLIT_STRING is its -265;
- a LIKE pattern whose [ no ] closes (the lane's 5; measured on IQ 17.1): its engine's -1001047, over no rows too;
  its front end matches nothing by one (not built: refused);
- the landing's second review (2026-10-17; each measured on IQ 17.1): a rejected outer join is read as inner
  always (IQ's plan: a window over it, or in a derived table between, numbers the inner rows; COUNT(*) OVER through a
  windowed middle table measured, by a WHERE or a join), but the rejection enters no grouped derived table a window
  above it reads (its NULL group stays: SQLStorm's grouped CTE under a ROW_NUMBER, measured); a named WINDOW's keys read a derived column; NOT LIKE NULL and BETWEEN NULL
  reject nothing; -157, TO_CHAR's -157 and CONVERT's -1009145 are held behind the statement's names (-143 first),
  -265 before a held error; ORDER BY's -1005004 for a scalar subquery outside a conditional only (CASE WHEN EXISTS
  is -1001030: refused); an enclosing query's column beside a GROUP BY expression is no -149; TO_CHAR of a string is
  a LONG VARCHAR;
- batches (the corpus lane's SEDE, 155 queries; measured on IQ 17.1 and SQL Anywhere 17, 2026-10-17, with the
  user's approval to measure ';'): a client reading one result gets the first statement's answer, or its error,
  separated by ';' or not; a later statement that does not parse fails the batch, -131 (an argument count, IQ's -154
  alone, too), and nothing else a later statement holds is raised; WITH begins a later statement only after a ';'.
  Read so now (refused before without a measurement: the island reads one result). A SELECT with no FROM reads
  SYS.DUMMY's row, so * shows dummy_col. The 155 judged: 114 IQ's rows, 37 both errors, 4 refused otherwise;
- LATERAL (SELECT ...) in FROM (the lane's 4; measured on IQ 17.1): its engine's -1001030 (the front end reads it:
  refused by name). A closing parenthesis no opening one matches is -131. Not built, recorded: CAST of a timestamp to FLOAT (IQ's engine a REAL count of microseconds, a
  date's of days; the front end's days; SEDE's 4, T-SQL's date truncation through a float, which means nothing
  there);
- open, error order (the claim verification's residue, 2026-10-17: 34 of 879 claims name another code than IQ's):
  where a query holds two errors, IQ raises the one of its earlier phase -- syntax, then -154, names (-143, -142,
  -144), grouping (-149, -150), function names (-265), then conversions and type checks (-157, -1001013, -1009145)
  -- and lite raises the first it meets as it translates. Built for -157 before -1001013 (held, InputDialect
  .translated) and for a syntax error after a CTE's recursion or a decorated unknown call; the general form would
  hold each claim with its phase and raise the earliest's, one or two queries per pair. Built since (2026-10-17, the
  second verification, 1,110 of 1,173 claims IQ's code, none an answer): a subquery's placement error (-1001030,
  -1005004, -1001047) is held behind the statement's names, grouping and functions (InputDialect.failed); a call of
  no function of IQ's is -265 before any later error; a date conversion under its engine is held as -1001013 is;
  a windowed derived table's -1001030 before an outer qualified read's -1005004; a subquery in a window's key is
  -156; STRING_AGG's DISTINCT, ORDER BY and WITHIN GROUP read as its arguments under the iq17 presets (-149 for an
  ungrouped column, else -265);
- LIKE over a date of a pattern IQ's engine converts to one, its trailing %s dropped ('2019-06%', '2019%', '2019_06%';
  measured on IQ 17.1, 2026-10-17): its -1001013; one it cannot ('%2019%', '2019-06-%'), -1009145. An aggregate of
  the outer query's columns in a subquery's WHERE is the outer query's, not -150 (IQ's engine raises -1005009 for
  it, or its subquery's TOP's -1001030 first; refused by name). Third verification: 1,135 of 1,169 claims IQ's code;
- IQ carries a derived table's column's NULL rejection into it (SQLStorm's CTE whose LEFT JOIN's ON holds a
  subquery, joined on its column by an inner join; measured on IQ 17.1, 2026-10-17): its outer query's WHERE or inner
  join over it, through arithmetic, unqualified where it is the one FROM item, grouped or not, nested: the derived
  LEFT JOIN is read as inner, before a window there numbers its rows (IQ numbers the inner join's rows, where SQL's
  order numbers the outer join's). An inner join over an outer one rejects as a WHERE does, one under it does not.
  The three derived-rank FAILs pinned since landing 5 were this, and pass;
- IQ raises a CTE's recursion (-938, -914) and a decorated call of no function of its (STRING_AGG(...) WITHIN GROUP,
  DISTINCT, OVER; -265) once it has read the whole statement (SQLStorm's 6, found by verifying the claims; measured on
  IQ 17.1, 2026-10-17): a syntax error after either is its first. The reader reads on past them for one;
- a string constant against an integer column (SEDE's n >= 'x', n an INTEGER, found by verifying the claims; measured on
  IQ 17.1 over 40 spellings and every number type, 2026-10-17): IQ's engine converts it first, so one that is no number
  by its grammar (spaces, a sign, digits and a point, an exponent: '', '+', '.', 'e5' are numbers; '5e', '- 5', a tab,
  a comma are not) is its -157, before any -1001013 of the statement and after a missing column's -143. Against a
  NUMERIC or DOUBLE column, or an expression, -1001013. LIKE over an integer converts a pattern without a wildcard;
  the -1001013 is held until the statement is translated (InputDialect.translated);
- not built yet: PIVOT (IQ 17.1 reads it, both engines; an IN list of literals, COUNT shown as a NUMERIC(20,0)).
  The corpus's 10 are SQL Server's `IN ([Question], [Answer])`, IQ's -143 (a name) or -1816 (a number);
- open, a decision: ODBC escapes (`{d '2010-03-01'}`, `{ts ...}`, `{fn NOW()}`; 19 of the corpus's queries). IQ 17.1's
  server rejects every one (-131, measured), while an ODBC client that processes escapes translates them before it
  sends (the syntax research's `accept-with-option:client_escapes`). Whether an island reads them as such a client
  would, a typed constant and the function, is the user's call: refused by name until then.

### Phase 2: the answers judged (2026-10-16)

The data is a sample of dba.stackexchange's (29 tables), loaded into the live IQ as `ll_*` tables and into each lane's
database. The live IQ answered all 12,445 SEDE and SQLStorm queries the island translates. The snapper's machinery
collected the answers (the scratchpad's `corpus_answers.py`; the data and the answers stay private). `IqCorpusTest`
judges them as parity runs (`-Diq.corpus.answers`), and `-Diq.corpus.rows` writes lite's rows of each FAIL. A FAIL
whose rows are IQ's as a multiset is PASS_UNORDERED: a real-world ORDER BY ties. Each remaining FAIL with an ORDER BY or
a TOP was judged again with every output position appended to its ORDER BY. IQ answered these tie-broken twins too.

| Verdict | First judge | Now |
|---|---|---|
| PASS | 10,471 | 10,433 |
| PASS_UNORDERED | 1,454 | 1,434 |
| BOTH_ERROR | 46 | 62 |
| REFUSED | 1 | 182 |
| FAIL | 437 | 319 |
| BUG (lite answers where IQ raises) | 20 | 5 |
| ERROR (lite raises where IQ answers) | 5 | 0 |

Every remaining FAIL is accounted for. Of the tie-broken twins:
- 148 PASS: their FAIL was ties only;
- 135 are ROW_NUMBER or NTILE numbering tied rows, each engine its own way;
- 10 read the clock;
- 7 are D-N3: IQ types a column by its data's range (`ROUND(1.0 * Score / ViewCount, 8)` is a NUMERIC(12,7) over this
  data);
- 2 are an IQ defect, listed under iq-defect: a WHERE or an outer join's condition on the null-supplying side of a
  windowed derived table's LEFT JOIN, applied below its windows;
- 1 is a name: a window ordered by aggregates in a grouped query.

The BUGs left are IQ's optimizer limit -1005015 on three Cartesian products, which no translation can know, and a
FROM-rewrite artifact of the answers driver.

Wrong answers found and fixed, each measured on the live IQ (parity cases: `lite/corpus_gaps.yaml`):
- a correlated read of an enclosing row's window column (a Lowerer fix);
- an ORDER BY name reads a select item only by its alias; otherwise it is a FROM column, an ambiguous one -144, DISTINCT
  too;
- a computed date-and-time string cast to DATE did not check its time;
- a WHERE over a windowed derived table or CTE that reads none of its window columns. Lite's lowering folded it into
  the window's WHERE, as Pure composes an extend and a filter (`Fold.filterSlot`), so the window ranked the filtered
  rows. Also, window functions over a WHERE that reads a derived window column (21 corpus queries). Fixed 2026-10-16
  in both SQL islands by inline views: `SqlToPure` marks a derived table or CTE whose own query computes windows
  (`meta::legend::lite::inlineView`), and `InlineViewChecker` stamps each window inside as a boundary, so the lowerer
  computes them in their own SELECT. The Postgres island had the same wrong answers: 4 Postgres-judged cases in
  `sql-parity/lite/windows.yaml`, now PASS.

Built, measured on the live IQ:
- a time alone read on 1900-01-01 under IQ's engine;
- a grouped query's unaliased items named by their own spelling under IQ's engine (56 queries);
- a CONVERT with a style named as IQ names it;
- computed date strings with month names, blanks and AM/PM.

IQ's errors raised by name, measured: a window function in a condition's subquery (-1001030); two subqueries compared
and a subquery as BETWEEN's operand (-1001030); an alias of a select-list subquery read in WHERE, GROUP BY or HAVING
(-1001030); a subquery inside a condition's expression, now naming IQ's -1001030.

Judged again on 2026-10-17, the lane rebuilt and the live IQ asked afresh (12,536 SEDE and SQLStorm queries
answered): PASS 10,643, PASS_UNORDERED 1,504, FAIL 357, BUG 8. The tie-broken twins of the FAILs: 166 PASS, and
every other FAIL is a numbering of tied rows (ROW_NUMBER, RANK: 166), a clock (12) or IQ's data-dependent NUMERIC
scale (D-N3, 12); none unexplained. The BUGs are IQ's defects and limits (its internal error -1006001 in an ORDER
BY, three times; its Cartesian-product limit -1005015, four) and one table alias `limit` IQ reads as its clause.
One more IQ defect found: rows tied on the ORDER BY can carry each other's columns in IQ's answer (sqlstorm-10142:
user 508's latest post is 2011-01-27 in the data, lite's answer; IQ's row gives user 50's 2011-01-06).

Judged again later on 2026-10-17 (the follow-on branch, after this file's later rounds), coverage rerun first:
- SEDE's answered queries rose from 6,756 to 6,889, SQLStorm's from 5,780 to 5,807.
- The regrown SE suite (12,696 queries, IQ asked for its 161 new ones): PASS 10,763, PASS_UNORDERED 1,526, FAIL 366,
  BOTH_ERROR 18, BUG 11. The moves into FAIL are tied rows again (ROW_NUMBER or RANK over equal keys, a
  `Rank <= 5` filter keeping other tied rows: 61 rows against 60).
- Of the 3 new BUGs, 1 is IQ's internal error -1006001. The other 2 are a string of literals compared with a date,
  which IQ folds and raises over no rows: read as the literal it spells now.
- The refused suite: PASS 158 and PASS_UNORDERED 17 (from 145 and 16), REFUSED 404 (from 418).
- BIRD unchanged: 349 PASS, 132 BOTH_ERROR.

BIRD's answers judged too (2026-10-16). BIRD's 11 databases were sampled consistently by their foreign keys: parents
first, a child keeping the rows that reach kept parents, then filled to 600 rows a table (32,206 rows; the scratchpad's
`bird_sample.py`). They were loaded into the live IQ as `ll_b_*` and judged as the dba.stackexchange sample is:
- 342 PASS and 132 BOTH_ERROR: 474 of 500, 94.8%;
- 21 refused by name;
- 4 FAIL: 3 ties, and 1 name, fixed: a column named by a reserved word shows in double quotes, t."date";
- 1 ERROR: FALSE, a keyword in IQ 17.1 (the listed true-keyword divergence).

The landing's review (2026-10-16): three independent reviewers; the fixes are listed in `docs/IN_FLIGHT.md`, each
measured on IQ 17.1 and SQL Anywhere 17 where IQ decides (`lite/corpus_gaps.yaml`'s review cases). Known, recorded:
- ROLLUP or CUBE with TOP computes its items before the limit (the union's), so an item that raises for a dropped
  group raises where IQ answers;
- an APPLY's query that reads the left side by an unqualified name its own FROM lacks may bind an enclosing query's
  column, where IQ's engine raises -1001030;
- a rename's frame (the SQL islands' last step where an output is named apart from its column) carries no ORDER BY:
  the subselect's order rides alone, which DuckDB and Postgres keep, other databases need not;
- the listed iq17 divergences (`iq17-divergences.tsv`) answer where IQ answers otherwise or raises; refusing them by
  name under the iq17 profiles is the cheaper honest form (the review's finding 3), not done here.

Decisions for the user: none open. (Decided 2026-10-16 by the user and built, above: the window boundary, as inline
views; TOP's select list, computed after the limit.)

### The benchmark corpora and the differential fuzzer (2026-10-17)

The user asked for real queries that push the island hard. Their corpora, data and answers stay private
(`~/legend-lite-iq-private`); here are only the findings.

- **Suites:** SQLStorm's TPC-H (17,036), TPC-DS (15,242) and JOB (11,714) queries, in PostgreSQL's dialect; TPC-H's 22
  and TPC-DS's 99 canonical queries; BIRD's full dev set (1,534, SQLite's dialect). Each is kept as written, and also
  with recorded rewrites: a trailing LIMIT as TOP; `DATE 'x'` as `CAST('x' AS DATE)`; BIRD's backticks as double
  quotes.
- **Data:** TPC-H at scale factor 0.01 (86,805 rows) is loaded into the live IQ as `ll_h_*`. TPC-DS at 0.01, its large
  dimensions cut to the rows the facts read (117,631 rows), is prepared as `ll_d_*`.
- **Coverage on the rewritten suites:**
  - TPC-H answers 9,427 of 17,036. Before today's two fixes, 4,668 of the LIMIT-only suite.
  - TPC-DS answers 8,568 of 15,242, and refuses 441.
  - JOB answers 4,811 of 11,714. 3,961 of the rest are IQ's -149 (PostgreSQL's functional dependence on a grouped
    key), to be verified against IQ on empty tables.
- **Built from them, each measured on IQ 17.1:**
  - the IQ engine's AVG of a NUMERIC(15,2) or (16,2) (2,500 queries: the sum made whole before it is scaled);
  - WITH RECURSIVE as IQ raises it (-706, -922, -1001030, or plain CTEs; 1,805 queries);
  - `SUBSTRING(s FROM a FOR n)` and `::` as -131;
  - fifteen more other dialects' functions as -265.
- **A recursion IQ answers** (over no IQ table, or in SQL Anywhere's front end) waits for the Cypher island's
  recursion native to reach main (the island will lower to it).
- **The differential fuzzer** (`fuzz.py`, private) makes random queries over the parity tables, each ordered by a
  unique key, and judges them against the live IQ. Three rounds (2,000, 3,000 and 3,000 queries) passed 1,892, 2,743
  and 2,775. Fixed from them, each measured:
  - BETWEEN beside a NULL column bound (three-valued); an empty range of constants (FALSE, a NULL x too);
    `x BETWEEN x AND c` (FALSE for a NULL x);
  - an aggregate nested in a select item's aggregate before one in WHERE (-156; -150 where an IN reads it);
  - a ranking function's constant key beside a column key (answered; -1005028 only for constants alone);
  - a union of a decimal and an integer branch (the compiler's concatenate check);
  - constants' arithmetic past 32 bits (a BIGINT);
  - SIGN of a NUMERIC with no digit before its point (-1001014).
- **From the judged corpora:**
  - BIRD's full dev set (1,450 judged): 1,047 PASS, 5 PASS_UNORDERED and 389 matching error codes. One fix: a
    qualified ORDER BY column naming a column item, -149 not -854.
  - TPC-H over its data (9,427 judged): 9,131 match IQ once ties are broken. The rest are ROW_NUMBER over ties,
    IQ's Cartesian-product limit, D-N3's places and the 38-digit decision. One fix: a derived table IQ merges into an
    outer join's ON, -1001047 for its WHERE's subquery.
  - JOB's error claims: 422 of 423 sampled matched IQ's code over empty tables.
- **Built from the benchmarks:** ARRAY_AGG under IQ's engine (-1001030, in IQ's order; dropped with a column no one
  reads), LPAD and RPAD (IQ's engine's own; the front end's -134), and a conversion's -157 before a subquery's
  placement (a row limit's in a derived table still first).
- **Built later the same day:**
  - IQ engine types past 38 digits, where IQ's value is still 38 digits at its scale:
    - an AVG of NUMERIC(39,23) and wider, by long division (at most three chunks; an AVG of an AVG is refused);
    - `+ - *` and SUM at IQ's scale, a value that may not fit refused at run time (TPC-H's Q1 expression);
    - an ordering of two decimals whose common type passes 38 digits, compared at the fewer places (floor or ceil
      by the operator). Values past 38 digits stay refused, per the user's decision.
  - A stale refusal deleted: a set operation whose first branch shows a NULL constant (fixed by the compiler's union
    stamps, d619c5cd6); 131 JOB queries.
  - A window function in a subquery after an outer join's or ORDER BY's subquery (the order of -1001030 and
    -1001047/-1005004).
  - ILIKE and PostgreSQL's arrays as IQ's -131.
  - The DuckDB dialect writes an outer join's ON without the IS NOT NULL tests its comparisons make. DuckDB planned
    them as a nested loop: one TPC-H query took 55 s against 0.1 s.
- **Built from the fuzzer's fourth round (2,877 of 3,000 PASS), measured on IQ 17.1:**
  - BETWEEN beside two column bounds: a NULL bound leaves the row UNKNOWN unless the other bound reads no column
    (`3 + 0` and `ABS(3)` count as constant; `j - 5` does not).
  - An expression compared with itself, written alike (`v = v`, `ABS(v) >= ABS(v)`), is two-valued outside a search
    condition: `= <= >=` hold where it is not NULL, and `<> < >` never hold. It stays three-valued through AND, OR and
    NOT from a WHERE, and in SQL Anywhere's front end.
  - `0 + x`, `x + 0` and `x - 0` are `x` for a SMALLINT, an INTEGER or a DOUBLE (typed so), and not in GROUP BY. A key
    `0 + x` is no key of a select item or a HAVING written the same way, so IQ raises -149.
  - REPLACE of a string typed with no width is NULL (`''`, `LEFT(vc, 0)`, `SPACE(0)`, `TRIM('')`, a derived table's
    `''` column). REPLACE of a wider empty string stays `''`.
  - In fuzz round 5 (2,549 of 3,000 PASS before these fixes): a simple CASE's operand and WHEN of different kinds are
    -1009145; GREATER and LESSER in a window's keys are -1001025; REPLACE's width follows CHAR and VARCHAR (a CHAR
    keeps its width under LEFT); and the SQL renderer keeps a trailing `+` or `*` operand's parentheses (DuckDB
    overflowed `w + 2000 - 2020`).
  - IQ's own defect, not copied: its window SUM over a ROWS frame of an INTEGER wraps at 32 bits and shows the value
    unsigned (18446744069414584319 where the frame's sum is -1).
  - Built after round 5, measured on IQ 17.1 (re-judged: 2,584 PASS, refusals 60 to 27, FAILs 22 to 13):
    - STUFF under IQ's engine: it appends at a start just past the string, as the front end does, where its manual
      says NULL (D-F3); a length past 32,767 is NULL.
    - DAYS, WEEKS, YEARS, MONTHS, HOURS, MINUTES and SECONDS of one argument count from 0000-01-01, proleptic
      Gregorian (the front end counts from another epoch; refused there).
    - CHAR of a computed code: the DuckDB and Postgres dialects cast `chr`'s code to INTEGER. Under IQ's engine in a
      UTF-8 database CHAR is the code's byte modulo 256, refused for a row whose byte is 0 or above 127.
    - LIST of a string column ordered by itself in IQ 17.1's UTF8BIN database: the list drops trailing spaces, so
      values of one collation key read alike. A row holding a tab or a line break is refused at run time.
    - A column string read as a REAL under IQ's engine: its float spelling rounded to single precision, `''` or
      blanks 0.0, any other spelling -1009145. As a DOUBLE it stays refused: IQ's reading is not correctly rounded
      (`'1.555'` is 1.5550000000000002), and an equality test could tell.
    - DATEADD's type, checked (2026-10-17): under IQ's engine a DATE stays a DATE (an hour added keeps its day) and
      a string constant is a TIMESTAMP; in the front end a DATE is a TIMESTAMP. Lite already read all three; the
      earlier note that "a constant date is a TIMESTAMP under IQ's engine" was about the string constant.
    - NULLIF of a CHAR under IQ's engine: compared blank-padded as `=` compares, the second argument not cut (the
      front end cuts it; still refused there).
  - Measured, not built: IQ's engine writes a DOUBLE as text (`||`, CAST, STRING) with 18 significant digits and
    trailing zeros kept, C's `%#.18g` (`-0.100000000000000006`, `1.00000000000000005e+300`), zero as `'0'`. DuckDB's
    printf could write it; Postgres has no such format.
    - GREATER and LESSER: a statement over IQ tables that reads either one anywhere (select list, WHERE, ORDER BY, a
      subquery) runs in SQL Anywhere's front end, whole: beside one `CEILING(nm)` is a DOUBLE, `it + 1` past 2^31 - 1
      and an INTEGER SUM past its range are -158, `ROUND(nm, -1)` past NUMERIC(10,2) is -158. A derived table's
      column no one reads does not count (IQ drops it first); in a window's keys they are -1001025. Read so, made
      possible by the front end's NUMERIC unification, measured on SQL Anywhere 17 (D-N1 there: integers below a
      BIGINT are NUMERIC(10,0), a BIGINT NUMERIC(20,0)).
    - ROLLUP or CUBE beside a DISTINCT aggregate: -1001030, raised after the names' and the conversions' errors.
    - From the benchmark coverage pass (45,647 queries, 618 refused): a function of no name of IQ's nested in an
      unread derived column (COALESCE(STRING_AGG(...), ...)); a row value `(a, b) IN ...` is -131 (a predicate's
      selectivity `(c, 40)` is read); FILTER inside a call is -131; LAG's default for a COUNT; a LIST beside strings;
      a wide NUMERIC CAST to fewer places (placesExact).
    - The SQL renderer for DuckDB: Pure's half-even round of a DECIMAL to a constant scale is now exact (ROUND_EVEN
      of a DECIMAL is a DOUBLE there; a wide value came back wrong in its last digits).
    - A regression of the `x + 0` fold, fixed: a zero written with a point folds only in a shown item's type
      (`AVG(it + 0.0)` is of scale 21, `(it + 0.0) / (sm + 0.0)` a NUMERIC quotient).
  - Snowflake's integer division (2026-10-18, landing 5's review; not built, another area's): its INT_DIVIDE arm
    writes `CAST(TRUNC(a / NULLIF(b, 0)) AS BIGINT)`, and Snowflake rounds `a / b` to six places before it cuts
    (999999999 / 1000000000 would be 1), so lite's `quotient` is inexact there. The Postgres front end keeps its exact
    estimate until the arm is exact (`(a - MOD(a, b)) / b`, to be measured on a Snowflake); the IQ island's quotients
    on a Snowflake runtime share the arm.
  - Round 8 (2026-10-18, NULL logic and mixed kinds: 1,311 PASS, 1,571 matching errors, 22 FAIL, 15 BUG of 3,000),
    found, not built yet, for the next landing:
    - A BETWEEN or a comparison across kinds IQ's engine cannot compare, beside a NULL bound or an untyped NULL
      (`sm BETWEEN dbl AND NULL`, `vc BETWEEN sm AND NULL`, `NULL BETWEEN NULL AND NULL`, `it >= ts`), is its
      "Invalid data type comparison in predicate" error; lite answers.
    - COALESCE, ISNULL and NULLIF of a NUMERIC constant beside a DOUBLE or an untyped NULL type otherwise there
      (`COALESCE(2.5, dbl)`, `ISNULL(1e0, 0, dbl)`, `NULLIF(NULL, '1')`).
    - A string constant read as a date beside another kind names IQ's code otherwise (6 runs).
    - Its 3 ERRORs are D-N3's overflow, deferred (row 0.3): `it * bg`, `-1 + bg` and `bg % -1` over a BIGINT at
      its limits are a NUMERIC(29) or NUMERIC(20) in IQ, and DuckDB's BIGINT raises.
  - IQ's own quirk, not copied: `ROW_NUMBER() OVER (ORDER BY bg - bg, id)` over BIGINT extremes does not honor the
    `id` tiebreak (RANK sees the rows equal).
  - Measured, not built: `1 + v = v + 1` is two-valued too (IQ reads the sum in either order), and lite keeps it
    three-valued.
- **Judged with ties broken:** TPC-H 9,131 of 9,427 match IQ; TPC-DS 8,403 of 8,647; JOB 4,554 of 4,811. What is
  left is ROW_NUMBER over ties inside CTEs (both answers valid), D-N3's places, IQ's internal errors and its
  Cartesian limit.
- **Measured, not built:**
  - BIGINT arithmetic past 64 bits. IQ's engine answers a NUMERIC(20,0) (`bg + 7` over 2^63 - 1). Lite types from the
    column, so it keeps a BIGINT and the database's overflow raises. That is D-N3: IQ sizes from the data, and
    widening every BIGINT operation would mistype every realistic one.
  - A sum far past its declared type (a triple join's fan-out, 2e20) defeats lite's type-sized division steps, and the
    database raises; IQ answers. A ROUND of a quotient whose declared type is narrower than IQ's data-sized one raises
    lite's range check, where IQ answers (both D-N3).
  - An uncorrelated scalar subquery of many rows over an empty outer side: IQ never computes it, DuckDB raises.
  - CAST of a column string to REAL or DOUBLE (26 JOB queries): IQ's string-to-float rules, not measured.
  - A / IQ's engine types at a scale no one step holds: TPC-H's Q8 and Q14 (SUM / SUM of NUMERIC(32,4)s, scale 33),
    and about 50 corpus queries. The long division works, with the divisor bound to 18 digits and refused past them
    at run time, but its plan repeats each chunk's rest. It is 91,571 nodes even with one-estimate chunks, past the
    20,000-node limit. It needs intermediate values bound once (columns computed before the projection, as
    `Windowing.computed` does for windows), which the translator's select items do not have yet. Checked 2026-10-17:
    that alone would not do, since the Lowerer merges an `extend` into its consumer by substitution (a window's
    computed key is written as its column and again inside PARTITION BY). The Lowerer would also have to keep a
    SELECT level for a complex column read more than once.
- **The canonical queries, judged over the data:** TPC-H's 22 give 19 PASS and Q22's -131. Q8 and Q14 are refused
  (above). TPC-DS's 99 give 62 PASS and 19 matching error codes; 4 differ only in decimal places IQ derives from the
  data (D-N3: its window SUM is NUMERIC(10,2) over the parity tables, wider over TPC-DS's), and 14 are refused.
