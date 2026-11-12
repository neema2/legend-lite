# Lean product SQL: measured, and a design (E3, 2026-11-12)

> **Status: a DESIGN for the orchestrator's ruling.** No product lowering or dialect code changes until it rules
> (the E brief, 2026-11-12). The measurement tools are committed; the numbers below are reproducible with them (§2).
>
> **The governing ruling (the user, 2026-11-12):** "we really want the most expert lean and fast SQL on the product
> path and push ALL compatibility to the edge ... just the best fastest leanest best expert plan/sql with typed
> output". Fidelity governs WHAT the product answers (values by type and value, errors by kind and where), never HOW:
> plan shape, SQL, statement count and a value's text are lite's own. Nothing here is shaped to resemble upstream's SQL.

## 1. The answer in five lines

1. **Lite's product SQL is one statement per query**, measured on 2,708 channel-A queries and 11 dialects: 1.00
   statements per query (the dynamic pivot's keyed plan, 10 queries, is the one with two). Fewer statements per
   query is already done. Snowflake's per-statement cost comes from the number of queries a test makes, not from
   the plan.
2. **26% to 74% of the product's SQL characters are repeats**, depending on the dialect. The same subexpression is
   written again at every reference. The worst statements are 85% to 99% repeats: 160,235 characters on MySQL for a
   JSON path, 14,334 on Postgres for `range()->map()->percentile()`.
3. **The cause is one missing construct.** The MIR (lowering's typed SQL tree, `com.legend.sql`) has no way to bind
   a value once. Several things are therefore inlined at each reference, and the copies compound when nested:
   - a Pure `let`, and a user function's argument;
   - an object read once per property;
   - a list operand read by each arm of a guard;
   - an exploded list read at each column.
4. **Binding once works and is faster.** A mechanical bind-once rewrite of the constant-rooted statements answered
   the same rows on every pair it applied to (§5.1). It cut characters by 54% on DuckDB, 59% on SQLite and 63% on H2.
   Execution time fell 34% on DuckDB (2.7× faster on statements over 3,000 characters), fell 49% on SQLite, and was
   unchanged on H2.
5. **The proposal:**
   - P1, bind once: a root binding for constants, an expression `Let` for per-row repeats, and an exploded list read
     through a column.
   - P2 and P3: no self-casts and no text round trips.
   - P4: each dialect's own best spelling, its multipliers bound.
   - P5: guard the result with a shrink-only baseline per dialect and family, over the plans' bytes, with a planted
     violator.

## 2. How it was measured (reproducible)

- **The cases.** PCT channel A's queries on DuckDB, all five suites, were captured (first by a capture since folded
  into the switchboard's `pct-cases`, which records each (model, expression) pair the lane executes; the two were
  not compared). That gave 2,708
  queries, 2,451 of them unique: each one's model and expression, 860 KB in all. The suites passed with the capture
  on (essential 38 s, grammar 31 s, relation 57 s, standard 38 s, unclassified 21 s).
- **The plans.** `//core:product_sql_measure measure` (`com.legend.integration.ProductSqlMeasure`, a measuring
  tool, never a test) planned every case on 11
  relational dialects in one JVM, with no database: planning never touches one. That took 110 s. It writes each
  statement's size and text, taken from the plan's nodes (result, keys, keyed body, update, effect, check; never a
  target's loads or session statements: those are data delivery, not the query).
  - **Cross-check:** Snowflake's largest statement is 12,180 characters. That is exactly the figure the Snowflake
    lane measured at 970f0a992 for `removeDuplicates` over a mixed list.
- **The repeats.** `docs/lean-sql-homework/analyze.py.txt` factors each statement greedily. It repeatedly replaces the
  repeated group (a bracketed expression of at least 60 characters) that saves the most with a 6-character
  reference, until no repeat is left. "Repeated" means the characters this saves. It is an upper bound on what
  binding can save, since a binding costs its own wrapper.
- **Before and after.** `docs/lean-sql-homework/bindonce.py.txt` rewrites each constant-rooted statement (a PCT
  expression's `SELECT <expr> AS value`, with no FROM at the top level) to bind each repeated expression once, in
  a one-row derived table at the root. Rules applied:
  - an expression that raises is never bound (§5.1's laziness rule);
  - a binding that reads another is nested inside it.

  `//core:product_sql_measure time` runs before and after on lite's own in-process drivers (DuckDB 1.5, SQLite,
  H2 2.5). It compares the rows, then times each side as the median of 7 runs after a warm one. A rewrite that
  fails or answers differently is "not applicable", never counted.
- Commands: §8.

## 3. The baseline: PCT channel A's product statements, per dialect

| dialect | queries planned | refused at planning | statements | per query | median chars | p90 | max | total chars | repeats |
|---|---|---|---|---|---|---|---|---|---|
| DuckDB | 2,492 | 9 | 2,502 | 1.00 | 83 | 616 | 9,142 | 808,551 | 32% |
| H2 | 1,885 | 616 | 1,895 | 1.01 | 78 | 710 | 14,333 | 631,264 | 26% |
| SQLite | 2,488 | 13 | 2,498 | 1.00 | 222 | 1,898 | 24,352 | 2,003,401 | 59% |
| Postgres | 2,416 | 85 | 2,426 | 1.00 | 112 | 1,079 | 18,005 | 1,424,216 | 57% |
| MariaDB | 2,416 | 85 | 2,426 | 1.00 | 191 | 1,734 | 120,331 | 2,431,678 | 68% |
| MySQL | 2,417 | 84 | 2,427 | 1.00 | 222 | 2,114 | 160,235 | 3,228,949 | 74% |
| Trino | 2,492 | 9 | 2,502 | 1.00 | 146 | 1,109 | 11,047 | 1,312,912 | 45% |
| ClickHouse | 2,484 | 17 | 2,494 | 1.00 | 358 | 1,433 | 28,591 | 2,227,798 | 41% |
| Snowflake | 2,492 | 9 | 2,502 | 1.00 | 124 | 1,038 | 12,180 | 1,168,137 | 42% |
| BigQuery | 2,407 | 94 | 2,417 | 1.00 | 124 | 1,266 | 12,648 | 1,537,292 | 49% |
| Databricks | 2,419 | 82 | 2,429 | 1.00 | 100 | 791 | 7,669 | 933,830 | 33% |

Reading the table:

- **Queries planned** counts queries that send a statement. The rest of the 2,708 answer a constant the plan holds
  (no statement), or are refused at planning by name. The refusals were not checked against each dialect's
  expected-failure lists: H2's 616 are the largest group, most of them lists (H2 runs only the Relation suite in
  PCT).
**Core's own product queries, on their lanes** (`//core:core_tests_integration{,_h2,_sqlite}` with
`-Dlegend.diagnostics=dump-sql`, the statements sent under the unmarked origin `other` that read: queries, not
seeds, session settings or the catalog's statements; no code change):

| lane | statements | median chars | p90 | max | total chars | repeats |
|---|---|---|---|---|---|---|
| DuckDB | 2,603 | 138 | 490 | 18,893 | 653,394 | 7% |
| H2 | 2,416 | 165 | 645 | 18,888 | 754,976 | 6% |
| SQLite | 2,475 | 183 | 824 | 40,469 | 1,106,211 | 31% |

Core's queries are mostly relational: a store's tables, projected, filtered and joined. They repeat little. The
repeats are concentrated in expression-heavy queries (lets, objects, lists, JSON), which PCT exercises and which
SQLite spells with correlated subqueries. The `other` origin is the census's residual mark, so a few non-query
statements may be in it.

- **The relational corpus** is not in either table yet. The corpus's bodies are
  planned by the test road, not by `TypedQuery.executionPlan`. It is named as a next measurement (§7, item 1),
  with the same tools.

## 4. The causes, measured

**By query family.** Each family's share of a dialect's product characters, and how much of that share is repeats.
"let" is any query with a Pure `let`; the other families are classified by the expression's functions
(`classify.py.txt`).

| dialect | let | list operations | mixed lists | JSON | dates | relations |
|---|---|---|---|---|---|---|
| DuckDB | 49% of chars, 57% repeats | 10%, 1% | 2%, 10% | 3%, 55% | 8%, 2% | 11%, 1% |
| Postgres | 44%, 78% | 13%, 50% | 4%, 49% | 3%, 59% | 5%, 10% | 7%, 7% |
| MySQL | 22%, 78% | 25%, 83% | 7%, 77% | 7%, 89% | 4%, 24% | 5%, 13% |
| Snowflake | 38%, 68% | 10%, 22% | 8%, 36% | 1%, 16% | 7%, 8% | 9%, 2% |
| BigQuery | 38%, 58% | 13%, 48% | 4%, 61% | 2%, 42% | 6%, 25% | 9%, 6% |
| SQLite | 22%, 62% | 11%, 58% | 7%, 76% | 3%, 69% | 23%, 70% | 7%, 21% |

**By mechanism.** These are the worst statements' largest repeated groups (`analyze.py`'s report). Each is traced
to code by a read-only survey of lowering and the dialects, with file:line:

1. **A Pure `let`, or a function argument, inlined at each reference.**
   - Code: `Lowerer.withQueryLets` puts each let's lowered scalar in a table, and every `$x` reads that SqlExpr back.
     G½'s β-inlining (`UserCallInliner`) substitutes an argument the same way.
   - Example, case 217 (a `let` of an object, compared): the object literal is written 11 times; 14,328 characters
     on Postgres, 87% repeats. It compounds through nested user functions (k^depth).
2. **An object read once per property** (`MixedEncoding.declaredOnly`, and the object's JSON writer):
   - `CASE WHEN o IS NULL ... struct_extract(o, 'name') ... struct_extract(o, 'left') ...`, F+1 copies per level.
   - With (1), the object's whole literal is written F+1 times for every reference.
3. **A list operand read by every arm of a guard.**
   - Percentile (case 2557, `range()->map()->percentile(0.75)`): the list is written 10 times on DuckDB (652
     characters), 30 times on Postgres (14,334 characters, 99% repeats), and 82 times on MySQL.
   - The same shape appears in `at` (op 3x, index 4x), slice, `DateCtorRule` (each component 4x), division and
     remainder guards, and `sqrt`.
4. **An exploded list re-written at every read.** The mixed-list lead: Snowflake's `explodedColumn` (`GET(list, i)`,
   `Snowflake.java` ~:517) and the FLATTEN's `ARRAY_SIZE(list)`.
   - Chain: `TaggedLists.sort` writes the positions 3 times, `Dedup.keptDedup` writes them 3 times, and Snowflake's
     `LIST_REDUCE` 3 times more.
   - `TaggedLists.root` then reads the result K+4 times, which gives the 8 copies of 6,241 characters the lane
     found. MariaDB, MySQL, SQLite and BigQuery read an exploded column the same way.
5. **JSON navigation guards.**
   - The pattern is `CASE WHEN json_type(path) = 'ARRAY' THEN ... path ...` over the same path at each step of a
     chain (case 1027).
   - Size: 9,142 characters on DuckDB (85% repeats), 18,005 on Postgres, 160,235 on MySQL (99%).
6. **Dialect spellings that multiply an operand:**
   - Snowflake `LIST_REDUCE` (3x), `listGet` with a computed index (the list 4x, the index 5x);
   - Trino `roundHalfEven` (6x-7x), ClickHouse `JSON_TYPE` (7x), H2 `jsonType` (~7x);
   - SQLite and MariaDB `split`, `AnsiSqlRenderer.modFloored` (3x).

**Other findings:**

- **Stacked casts.** `CAST(CAST(` appears 191 times on DuckDB, 896 on Postgres, 874 on Snowflake and 681 on Trino.
  On Postgres the most common are:
  - the raise's carrier, `CAST(CAST(CAST(chr(31) || ... AS TIMESTAMPTZ)`, a planned error typed into its slot (93);
  - `CAST(CAST(x ->> 'k' AS BIGINT) AS VARCHAR)`, a text read as an integer and then written as text (60);
  - `CAST(DATE '...' AS DATE)`, a typed literal cast to its own type (52).
- **Pass-through wrapper selects** (`SELECT * FROM (`) occur 0 to 9 times per dialect, so they are not a cost.
- **The judge's own SQL is outside this measurement.** Snowflake's shortest-digits float text (`Snowflake.java`
  ~:1911) is written 7 times by `VerdictSql`'s canon in `exp::testSimple` (37,013 characters). It is noted here for
  the test road. On the product path the same printer is reached by `toString`, `format` and a guard's message over
  a Float: 1x per use, about 3-4 KB each.

### 4.1 Mixed lists, measured (facts only, the orchestrator's question, 2026-11-12)

The question: which queries of the fixed sets hold a collection LITERAL whose elements' types differ, and what share
of each database's product SQL they make. Measured with `bazel run //core:product_sql_measure -- mixed
core/src/test/resources/lean-sql <out>`; a query counts once, under its worst literal. The kinds:
- **numbers:** Integer, Float, Decimal, Number;
- **dates:** StrictDate, DateTime, Date;
- **truly mixed:** anything else.

A list mixed by an expression (a concatenation, a map) rather than a literal is not seen. Each cell gives the queries,
then that kind's share of the database's product characters.

| set | database | numbers: queries, % of chars | dates | truly mixed | all queries |
|---|---|---|---|---|---|
| channel_a | BigQuery | 32, 1.6% | 12, 0.7% | 68, 7.4% | 2,357 (1,235,651 chars) |
| channel_a | ClickHouse | 32, 1.5% | 12, 0.7% | 74, 3.6% | 2,434 (1,819,026 chars) |
| channel_a | Databricks | 32, 1.6% | 12, 1.0% | 74, 4.9% | 2,369 (777,755 chars) |
| channel_a | DuckDB | 32, 1.4% | 12, 1.0% | 74, 3.6% | 2,442 (655,187 chars) |
| channel_a | H2 | 14, 0.2% | 0, 0.0% | 43, 10.6% | 1,887 (550,036 chars) |
| channel_a | MariaDB | 32, 1.4% | 12, 0.5% | 74, 9.2% | 2,366 (2,060,278 chars) |
| channel_a | MySQL | 32, 1.6% | 12, 0.4% | 74, 9.4% | 2,367 (2,779,588 chars) |
| channel_a | Postgres | 32, 1.2% | 12, 0.7% | 74, 6.7% | 2,366 (1,160,996 chars) |
| channel_a | SQLite | 32, 1.2% | 12, 0.5% | 74, 9.2% | 2,438 (1,778,534 chars) |
| channel_a | Snowflake | 32, 2.8% | 12, 0.8% | 74, 10.9% | 2,442 (995,481 chars) |
| channel_a | Trino | 32, 1.6% | 12, 1.1% | 74, 4.6% | 2,442 (1,098,766 chars) |
| core | BigQuery | 0, 0.0% | 0, 0.0% | 0, 0.0% | 39 (3,769 chars) |
| core | ClickHouse | 0, 0.0% | 0, 0.0% | 4, 68.4% | 44 (40,245 chars) |
| core | Databricks | 0, 0.0% | 0, 0.0% | 4, 78.8% | 44 (18,992 chars) |
| core | DuckDB | 33, 1.3% | 3, 0.3% | 21, 2.4% | 2,570 (665,815 chars) |
| core | H2 | 13, 1.6% | 0, 0.0% | 4, 0.3% | 306 (70,404 chars) |
| core | MariaDB | 0, 0.0% | 0, 0.0% | 4, 98.3% | 42 (191,390 chars) |
| core | MySQL | 0, 0.0% | 0, 0.0% | 4, 98.9% | 40 (278,123 chars) |
| core | Postgres | 0, 0.0% | 0, 0.0% | 4, 75.5% | 78 (40,155 chars) |
| core | SQLite | 5, 0.3% | 0, 0.0% | 5, 49.1% | 244 (230,139 chars) |
| core | Snowflake | 0, 0.0% | 0, 0.0% | 4, 97.4% | 44 (123,838 chars) |
| core | Trino | 0, 0.0% | 0, 0.0% | 4, 89.6% | 44 (33,028 chars) |

- In channel A, the truly mixed literals are 74 queries, about 3% of the queries. They make 3.6% (DuckDB) to 10.9%
  (Snowflake) of the product characters.
- In core's set, 4 truly mixed queries make 98% of MariaDB's and MySQL's product characters (188,118 of 191,390), 97%
  of Snowflake's and 76% of Postgres's. On those databases core plans few queries (40-78), so the 4 dominate.

## 5. The principles

### 5.1 P1: bind a repeated expression once

**The rule:** an expression the SQL needs more than once is computed once and read by name. The MIR gets one
binding construct, and each dialect renders it in its best form. There are three placements, from cheapest to
richest.

**P1a, a root binding (a constant: no row, no lambda parameter).** A query-level `let`, a literal object or list, a
constant list operand. Its form is a one-row derived table at the root, read as a column:

```
SELECT <expr reading b.x> AS value FROM (SELECT <x> AS x) b
```

This is the shape `Lowerer.taggedRoot`'s "once" select already uses. It works on every dialect. A reference costs
the column's name, so it beats every per-expression form. Measured, on the statements where the mechanical rewrite
applied, with the same rows before and after:

| dialect | statements | characters before -> after | time before -> after | median time ratio | statements over 3,000 chars |
|---|---|---|---|---|---|
| DuckDB 1.5 | 108 | 229,843 -> 105,415 (-54%) | 134.7 -> 89.5 ms (-34%) | 0.80 | 14: 42.0 -> 15.6 ms (2.7x faster) |
| SQLite (lite's driver) | 333 | 714,359 -> 291,398 (-59%) | 35.7 -> 18.3 ms (-49%) | 0.63 | 64: 16.7 -> 6.8 ms |
| H2 2.5 | 50 | 160,531 -> 59,212 (-63%) | 11.6 -> 11.4 ms (0%) | 1.00 | 18: 11.1 -> 10.9 ms |

- **Examples:**
  - Case 226 on DuckDB (two `let` objects compared): 8,309 -> 2,313 characters, 3.3 -> 1.2 ms.
  - Case 2557 on SQLite (the percentile): 16,419 -> 1,559 characters, 1.29 -> 0.19 ms.
- **Not applicable to:** the mechanical rewrite did not apply to 25 DuckDB, 90 SQLite and 29 H2 statements. Their
  repeats read a correlated alias or a lambda's parameter. They need P1b.
- **H2's time does not move:** H2 parses and optimises fast and evaluates these constants once already. Its gain is
  size only.

> **P1a, BUILT (2026-11-12): `lowering/LetBinding.bindRepeats`, run on every root that takes the scalar road.**
>
> **What the pass does.** After lowering, it finds every expression the root's FROM-less select writes more than once.
> Expressions are compared by structure, so a let's or an argument's value at each read, an object read per
> property, and a list read by each arm of a guard all count. Each repeat that pays for its binding is computed
> once, in a one-row derived table (`FROM (SELECT <value> AS __l0) AS __lets`), and read by column. The pass picks the
> most saving repeat first and stops when none saves. A repeat inside two bound values is bound beneath them.
>
> **When it binds.** A binding pays when its node estimate is positive: `(k-1)*w - 2k - 6`, with k copies of w
> nodes. A reference costs about two nodes; a value and its select cost one each. A repeat is bound only where:
> - it is closed: no column, and no lambda parameter it does not bind itself;
> - it can raise nowhere, judged by structure: no raise, division, parse, domain function or narrowing cast;
> - it aggregates nothing, reads no subquery, is not tagged or checked, and draws nothing afresh;
> - its type is plain.
>
> The statement clock (`now()`, `today()`) is bound whenever it repeats, so a let reads it once.
>
> **Where it never binds.** Never inside a derived table: H2 reads no outer column there (measured, its lane). Never
> where the select unnests in its projections: the dialect places that by its FROM (H2's lane).
>
> **Measured on the fixed sets, at the guard's pins** (`baseline.tsv` re-pinned; no family grew):
>
> | channel A | characters | plan JSON |
> |---|---|---|
> | DuckDB | -13.6% | -7.8% |
> | Postgres | -15.5% | -8.3% |
> | MySQL | -13.9% | -8.3% |
> | Trino | -12.6% | -7.9% |
> | ClickHouse | -12.6% | -8.2% |
> | BigQuery | -12.4% | -8.2% |
> | Databricks | -11.7% | -7.7% |
> | Snowflake | -10.0% | -7.2% |
> | H2 | -8.7% | -3.9% |
> | MariaDB | -8.3% | -7.0% |
> | SQLite | -7.6% | -7.5% |
>
> Core's set: DuckDB -0.2%, SQLite -1.7%, every other database unchanged.
>
> **Timed on the statements it changed** (`//core:product_sql_measure time`, the same rows before and after on
> every pair):
>
> | database | statements | characters | time | over 3,000 characters |
> |---|---|---|---|---|
> | DuckDB | 65 | -57% | 141 -> 78 ms | 110 -> 55 ms |
> | SQLite | 65 | -60% | 19.8 -> 10.1 ms | |
> | H2 | 25 | -64% | 10.5 -> 7.8 ms | |
>
> **Answers:** PCT channel A and channel B on DuckDB, PCT on H2, and core's three lanes all green, unchanged.
> `LetBindingTest` pins the clock read once, the bound repeats with their answers, and a raising or single-read let
> left inline.
>
> **What is left for P1b and P1a.2.** A repeat that reads a row, a lambda's parameter or a correlated alias is
> P1b's, as is a repeat inside a guard's raising branch. An eager let is P1a.2's.

> **P1a.2, eager lets (RULED 2026-11-12: "lets bind eagerly, as Pure does"), the homework, measured in process**
> (`bazel run //core:product_sql_measure -- probe <file>`, each database's own raise: DuckDB `error(...)`, SQLite a bad
> JSON path, H2 `SIGNAL`):
>
> | database | an unreferenced column of the one-row derived table | read only in an untaken branch | forced: `WHERE l IS NULL OR l IS NOT NULL` | a `MATERIALIZED` CTE |
> |---|---|---|---|---|
> | DuckDB 1.5 | not evaluated: `let x = 1/0; 2` would answer 2 | not evaluated | raises | raises, even unreferenced |
> | SQLite | not evaluated | raises | raises | raises |
> | H2 2.5 | raises: a constant raise is folded at prepare time (a non-constant one is to be measured) | raises | raises | no such syntax |
>
> - So an eager let needs FORCING wherever a raising value is bound. The `WHERE` reference raises on all three, so it
>   is the portable candidate.
> - Next: the server and cloud dialects measured on their lanes (to ask) with the same probe file.
> - Then a raising let is bound with its forcing `WHERE` on the root select, and an unreferenced one raises as Pure's
>   would.
> - A database with no forcing form refuses an unreferenced raising let by name, as ruled.

**P1b, an expression binding (`SqlExpr.Let(name, value, body)`).** For a repeat that reads a row or a lambda's
parameter: a guard's operand, a percentile's list inside a `map`, an object's properties per row. Each dialect
renders it in its best form, decided by measurement before it lands:

| dialect | form | measured? |
|---|---|---|
| DuckDB | `list_transform([v], x -> body)[1]`, valid inside a lambda; or a LATERAL where not in a lambda | to measure |
| Snowflake | a singleton `TRANSFORM`, as its shortest-digits printer already writes; a lambda cannot read an enclosing lambda's parameter, so there it stays inline | to measure on the lane |
| Postgres, MySQL 8, Trino, BigQuery | `(SELECT body FROM (SELECT v AS x) l)` (BigQuery: `FROM UNNEST([v]) x`); a LATERAL where the binding feeds a FROM | to measure |
| ClickHouse | `arrayMap`, or a `WITH` alias | to measure |
| SQLite | a correlated subselect | to measure |
| H2, MariaDB | inline until measured: H2 gains nothing on time (P1a), MariaDB has no LATERAL | — |

**When not to bind (each one measured or found by the rewrite):**

- **Never bind an expression that raises ahead of its condition.**
  - A binding is evaluated unconditionally. Lifting a guarded raise (the planned error, `error(chr(31)...)`) out of
    a `CASE` branch makes it raise where the original does not: the first rewrite did exactly that, and its
    statements failed.
  - The rule: bind a guard's OPERAND (evaluated anyway), never its guarded RESULT. A `Let` whose value can raise is
    placed only where every reference is evaluated.
- **SQLite's JSON subtype does not survive a column.** `json(...)` bound in a derived table is read back as text:
  2 of 335 pairs answered differently (`json_array(b1, ...)` quoting the bound JSON). On SQLite a bound JSON value
  is re-applied with `json()` at its reference, or not bound.
- **Trivial operands** (a column, a literal under ~16 characters) are never bound: the binding costs more than it
  saves.
- **Order and determinism:** a binding changes no ORDER BY. A Pure `let` and a function's argument are call by
  value, evaluated once: binding once IS Pure's semantics, and inlining was the deviation. Measured (2026-11-12):
  - `|let t = now(); $t == $t;` plans today as two reads of the clock on every dialect (DuckDB `CAST(now() AS
    TIMESTAMP) = CAST(now() AS TIMESTAMP)`, SQLite `strftime(..., 'now') = strftime(..., 'now')`, MySQL
    `UTC_TIMESTAMP(6) = UTC_TIMESTAMP(6)`, ...).
  - It is NOT observable today: each of these databases holds its clock fixed within one statement. `random()` is
    refused by name on the product path, so it cannot be read twice.
  - In channel A's 2,708 queries, no statement reads a user-visible non-deterministic value twice. The hits are a
    planned raise's anti-folding `random()` (Postgres), `generateGuid`'s intended independent draws (SQLite), and one
    false match (`toDayOfMonth`).
  - So P1a fixes a latent deviation, not an observed bug. Its test pins `now()` written ONCE for the let.

**P1c, an exploded list read through a column (the mixed-list lead).**
- Today `TaggedLists.sort` and `root` explode a list expression. Snowflake, BigQuery, MariaDB, MySQL and SQLite then
  write `GET(<the list>, i)` at every read.
- Instead: the list becomes a column of a preceding one-row derived table (P1a, or P1b at a row), and the
  explosion reads the column. Each read is then `GET(b.positions, i)`.
- Expected: the lead's 8 copies of 6,241 characters (90% of a 55,389-character statement under channel B; the same
  lowering makes channel A's 12,180-character product statement) become one. Measured after the change on the
  Snowflake lane (to ask).

### 5.2 P2: no wrappers on the product path

- A typed literal is never cast to its own type (`CAST(DATE '2015-07-03' AS DATE)`: 52 on Postgres's PCT set).
- A planned raise is typed into its slot by ONE cast, not three: `CAST(CAST(CAST(chr(31) || ... AS ...)` (93 on
  Postgres).
- The renderer's cast arm skips a cast whose operand already has the target type. The type is on the MIR node
  (`SqlExpr.type()`), so this needs no inference.
- No judge, verdict or canon wrapper is ever in a product statement. That is true today (measured: the product's
  statements are the plan's nodes, and none carries the canon). The guard (§6) keeps it true.
- Pass-through `SELECT * FROM (...)` layers: 0 to 9 per dialect. No action.

### 5.3 P3: typed values, never text

- A value read as text and then written back as text through another type
  (`CAST(CAST(x ->> 'k' AS BIGINT) AS VARCHAR)`, 60 on Postgres) is read once at its target type.
- A temporal is built from typed parts, never from a `strftime(...) || '.000'` string (`timeBucket`, `adjust` on
  DuckDB: literal-gated today).
- A guard's message is the only text a guard builds. Its operand's text (`str(x)`) is bound by P1, so a Float's
  printer is written once per message, not once per reference (on Snowflake that printer is about 3-4 KB).

### 5.4 P4: the best form per dialect

Each dialect's own spelling is chosen for that database. None is shaped to resemble another database's or
upstream's. The multipliers named in §4 item 6 are rewritten with P1b's binding in each dialect:

- Snowflake `LIST_REDUCE` and `listGet`;
- Trino `roundHalfEven`;
- ClickHouse `JSON_TYPE` (a `multiIf` over one `JSONType(x)`);
- H2 `jsonType`;
- SQLite and MariaDB `split`;
- `modFloored`.

The float printers bind their operand on every dialect that can, as BigQuery and SQLite already do
(`bindsFloatText`, `bindsFloatPrint`).

### 5.5 P5: fewer statements per query

Measured: 1.00 per query already (§1). The Snowflake lane's 827 statements for 345 essential tests are about 2.4 per
test: the queries a test makes (several `execute`s per test) plus the suite's one session statement. They are not a
plan's extra statements. No product change is proposed. The keyed (dynamic pivot) plan's two statements are its
design: the keys, then the body.

### 5.6 Answer or SQL: what each item changes

The orchestrator's rule (2026-11-12, the user's words: "we are allowed to change error message and change sql and
change results even change types ... as long as they are typed and sane"): a change to the product's ANSWER is
written up here first and ruled, then becomes a register row with scope "product". A change to the SQL alone needs
no row. "Sane" means:
- typed;
- principled;
- the same answer on every database;
- no silent loss;
- deterministic.

| item | changes | why |
|---|---|---|
| P1a, root binding | **SQL only** | a let and an argument are call by value: binding once is Pure's own semantics (the `now()` read once is a latent deviation fixed, not observable today, §5.1) |
| P1b, `SqlExpr.Let` | **SQL only** | the same; the laziness rule keeps a raise where it was |
| P1c, explosion read through a column | **SQL only** | the same list, read once |
| P2, no self-cast | **SQL only** | a cast to the operand's own type is the identity |
| P2, one cast on a raise | **SQL only** | the raise's message and kind are unchanged |
| P3, text round trips | **SQL only where the value is typed**; to check where a JSON member is read as text, cast to an integer, then cast back to text (`CAST(CAST(x ->> 'k' AS BIGINT) AS VARCHAR)`): the round trip normalises `"007"` to `7`, so dropping it could change an answer | measured per site before it lands; any answer change comes back here first |
| P4, dialect multipliers bound | **SQL only** | the same expression, written once |

**Answer changes that would shrink the SQL, both already ruled for D (DATA_DELIVERY_DESIGN §6.1, the start-here
§5.4).** E builds neither; it coordinates with D.

- **Errors are their kind plus where they happened, never the value.**
  - Today a guard's message re-reads its operand to print it ("offset 5 where the collection is of size 3"): `str(x)`
    in the message, so the operand is written once more.
  - Measured: the raises are 4% of DuckDB's product characters (35,120 of 808,551).
  - Under D's error form the message is a fixed planning-time text, and the operand's extra copy goes. On Snowflake,
    a Float operand's text is the ~3-4 KB shortest-digits printer per message.
- **A Float's text is "6 decimals, trailing zeros and `.` trimmed".**
  - Today `toString` and `format` of a Float build the shortest round-trip digits in SQL. On Snowflake that is the
    ~3-4 KB printer per use; `FloatPrintText` reads its operand about 9 times on the other dialects.
  - D's print form is a fixed-scale render that each database spells in one call. The shortest digits move to the
    edge, as the orchestrator's example says.

### 5.7 Mixed lists, the hybrid (RULED by the user, 2026-11-12: "hybrid mixed list is good, lets go with that")

It replaces P1c. **This section is the write-up the orchestrator asked for before the build.** It covers the
answer changes, each a draft register row with scope "product" and ids to be assigned, and the before and after SQL
per dialect.

**The rule, on the product path.** The TYPER makes every promotion explicit in the typed tree; lowering never infers
one.
1. **Number lists** promote by a fixed rule:
   - Integer + Decimal -> Decimal;
   - Integer + Float -> Float;
   - an Integer literal above 2^53 in a Float-promoted list is refused at compile time, by name (no silent loss).
2. **Date lists:** StrictDate + DateTime -> DateTime, a date taken as its midnight at UTC.
3. **Truly mixed (Any) lists** are an implicit Variant: one JSON array, kinds kept.
   - Its elements are strings, numbers (integer and float told apart per dialect) and booleans.
   - A date inside one is a small lite tag, `{"date": "2025-02-10"}`, never a silent string.
   - Sort, min, max, greatest and least across kinds are refused by name.
4. **Today's tagged lists** (a tag plus typed slots) leave the product. The compatibility road keeps them; where is
   being decided with the user.

**The answer changes (draft register rows, scope "product"; upstream read from legend-pure 5.99.0 before each lands):**

| draft row | expression | upstream (to be read and recorded) | lite after | why it is sane |
|---|---|---|---|---|
| E-R1 | `[1, 1.0]->removeDuplicates()` | two elements, `1` and `1.0`: Pure's equality tells Integer from Float | one element, `1.0` | one promoted type, IEEE-equal values; the same on every database |
| E-R2 | `[1, 2.0]->at(0)->type()` (and `toString()`) | `Integer`, `'1'` | `Float`, `'1.0'` | the list's one declared element type, the value matching it |
| E-R3 | `[%2025-02-10, %2025-02-10T00:00:00]->removeDuplicates()` (and `==` across them) | two elements: a StrictDate is not a DateTime | one element, the DateTime | a date is its midnight at UTC in a DateTime list (ISO 8601) |
| E-R4 | `['b', 1, 'a']->sort()`, `->min()`, `->max()`, `greatest`, `least` across kinds | ordered by upstream's cross-kind comparison | refused by name ("a list of mixed kinds has no order: sort a list of one kind") | no principled order across kinds; refusal is never a silent answer |
| E-R5 | a date inside a truly mixed list read back (`[1, %2025-02-10]->at(1)`) | the StrictDate | the StrictDate, carried in the Variant as `{"date": "..."}` | typed, never a string; listed because the CARRIER changes (a JSON array, not tag+slots), the answer does not |

Every channel-A PCT test that reads a changed answer moves to the compatibility road (which keeps today's tagged
lists), or becomes an expected failure on the product door with its row. Never a test made green by ledgering it.
The count is to be measured at the build: a query whose answer moves shows up in channel A's diff.

**Before and after SQL, per dialect** (characters of the product statement; "after" for numbers and dates is the
plan of the query's already-homogeneous twin, which a promoted list lowers like, measured on 2026-11-12; truly mixed
is a design target until built):

| family (case) | DuckDB | Postgres | Snowflake | MySQL | SQLite | Trino | BigQuery |
|---|---|---|---|---|---|---|---|
| numbers, `[1.0, 2, 3.0]->stdDevSample()` (2569): before | 60 | 175 | 1,673 | 239 | 138 | 102 | 145 |
| after: the twin `[1.0, 2.0, 3.0]` | 62 | 176 | 1,742 | 242 | 140 | 95 | 160 |
| dates, `[%2025-02-10, %2025-02-10T20:10:20+0000]->greatest()` (2075): before | 451 | 588 | 570 | 904 | 579 | 856 | 651 |
| after: the twin `[DateTime, DateTime]` | **92** | **166** | **133** | **300** | **155** | **136** | **160** |
| truly mixed, `['1', 2, '1', '3', 1, 3, '3', 2]->removeDuplicates(...)` (1267): before | 1,486 | 4,577 | 12,180 | 28,110 | 16,084 | 3,013 | -- |
| after (design target) | a JSON array's distinct elements in order (DuckDB `list_distinct` over a `JSON[]`, Postgres `jsonb_array_elements` + first `ORDINALITY` per value, Snowflake `ARRAY_DISTINCT`, MySQL `JSON_TABLE` + `ROW_NUMBER`) | | | | | | |

- **Numbers:** today's product already lowers a mixed number list as one native list (DuckDB `[1.0, 2, 3.0]`), so
  the SQL barely moves. The change is the elements' TYPE (E-R1, E-R2): their kinds stop being tracked.
- **Dates:** the tag-and-slots root (`CASE WHEN __tag = 1 ...`, a slot column per kind) becomes one typed list,
  about 4x smaller on every dialect.
- **Truly mixed:** the tagged list's slot arrays and their positions, read per element and per channel (the
  mixed-list lead, §4 item 4), become one JSON array the database reads natively. This is where the size lands.
  §4.1's measurement puts these 74 channel-A queries at 3.6% (DuckDB) to 10.9% (Snowflake) of product characters,
  and core's 4 at up to 98% of a server database's.

**The build, after this write-up is ruled:**
1. The typer's promotion, with typed nodes for the promotions and for the Variant list.
2. Lowering of the Variant list per dialect, each form measured on its lane.
3. The tagged lists' product uses deleted; the compatibility road keeps its own.
4. The rows above landed with the user's batch.
5. The guard's pins for the mixed families fall: a re-pin, dated.

### 5.8 The cast-and-case rule's census (the user's heuristic, ruled 2026-11-12): first counts

**The rule.** Every CAST, CASE and text function in product SQL traces to one of two sources:
1. the user's own Pure call;
2. a named product rule that keeps the answer typed and the same on every database.

Anything else is a defect.

**First measurement:** channel A after P1a, counted in the SQL text, string literals blanked. "Text" counts `||`,
concat, replace, trim, pad, printf/format, strftime, substr, upper/lower, chr, json_quote.

| database | statements | CAST | CASE | text | characters | commonest cast targets |
|---|---|---|---|---|---|---|
| DuckDB | 2,502 | 3,161 | 994 | 1,868 | 645,793 | DOUBLE 800, VARCHAR 793, BIGINT 574, JSON 395 |
| H2 | 1,895 | 2,025 | 958 | 939 | 548,415 | VARCHAR 818, BIGINT 310 |
| Postgres | 2,426 | 7,892 | 2,552 | 2,698 | 1,111,608 | VARCHAR 1,791, BIGINT 1,742, TIMESTAMPTZ 245 |
| Snowflake | 2,502 | 7,576 | 1,362 | 2,489 | 993,604 | DOUBLE 1,840, VARCHAR 1,602, BIGINT 1,350 |
| SQLite | 2,498 | 4,809 | 6,032 | 14,784 | 1,796,764 | REAL 1,933, INTEGER 1,710, TEXT 1,009 |
| MySQL | 2,427 | 13,315 | 8,739 | 12,204 | 2,751,100 | JSON 6,285, SIGNED 2,362, DATETIME(6) 1,619 |

**The method proposed for the classification.** Classifying from the text alone would be guessing. Each CAST and
CASE should be counted where lowering MAKES it, by a typed reason the MIR node carries, and the census pins the
counts per reason:
- the user's call (`toString`, `cast`, `parse*`, `if`);
- the declared type (a literal or a value the plan declares otherwise);
- a raise's carrier (D's);
- a Pure operator's semantics (a floored mod, an integer division);
- the print form (D's).

A node with no reason fails the guard by name. That is the second, shrink-only pin.

Two things to settle first:
- **Where the reason lives.** A field on `Cast` and `Case` needs every constructor site to name its reason (the MIR's
  records are rebuilt in about 18 switch sites).
- **D's share.** The print forms and the raise sentences are D's; they are counted, marked D and left to D.

> **BUILT (E-4, 2026-11-12, on D's RaiseText):**
> - **The reason lives on the node.** Every `SqlExpr.Cast` and `SqlExpr.Case` carries a `SqlExpr.Reason`, and neither
>   has a constructor without one: a null reason is refused by name ("a CAST without a reason (E-4)").
>   - The reasons are a closed enum: `user`, `declared`, `conform`, `operator`, `carrier`, `print`, `raise`,
>     `dialect`, `judge`.
>   - `conform` replaces `Cast`'s boolean, and `conform()` reads it.
>   - A rewriter that rebuilds a node keeps its reason. Some rebuilds (`Windows`, `SqlPostProcessors`,
>     `FoldToListReduce`) used to drop `conform`; they now carry it.
>   - The plan carries it: `MirJson` writes `"reason"` and refuses an unknown code by name.
> - **How reasons were assigned.** About 420 constructor sites were set by their file's role, and the mixed files
>   (`Scalars`, `Lowerer`, `CastPolicy`) site by site:
>   - the verdict SQL is `judge`;
>   - the canon, the literal spelling, the render and `toRepresentation` are `print`;
>   - the list, JSON, tagged and graph carriers are `carrier`;
>   - `CastPolicy` and `match` are `user`;
>   - the natives' semantics are `operator`;
>   - the declared types of literals, NULLs, loads and keys are `declared`;
>   - a raise's condition is `raise`;
>   - a dialect's own pass is `dialect`, except the Float spellings, which are `print`.
> - **The census.** `leansql.CastCaseCensus` lowers channel A on every relational database, runs each dialect's
>   passes (`sql/dialect/PassedTrees`, test code) and counts the nodes per reason.
>   - Pins: `lean-sql/cast-case-census.tsv`, held by `//core:cast_case_census_test` (in `//gates:checks`). A count
>     that grows fails; a shrink past 1% asks for a re-pin (`bazel run //core:update_cast_case_census`).
>   - First counts, summed over the 11 databases:
>
>     | Reason | Casts | Cases |
>     |---|---|---|
>     | declared | 6,461 | 0 |
>     | carrier | 5,329 | 1,750 |
>     | print | 4,384 | 3,943 |
>     | operator | 3,675 | 946 |
>     | dialect | 1,301 | 1,300 |
>     | user | 1,325 | 33 |
>     | raise | 0 | 495 |
>     | conform, judge | 0 | 0 |
>
>     Conform and judge are 0 because channel A has no mapping reads and no verdicts.
>   - Not counted: a CAST a dialect writes as text inside a render method, never as a node. The text counts above
>     include those; the node census does not.
> - **Its cost.** No statement and no SQL character moved. Plan JSON grew about 0.2% (about 15.6K characters per
>   database on channel A), which is the reason carried in each plan's MIR. The sets were re-pinned with that date.
> - **What comes next on this census.** `print` and `carrier` are the shrink targets: E-5a, the Float canon staged, and
>   E-2, the hybrid mixed lists. `declared` is the next one to read case by case: a literal's cast is the plan's
>   declared type, but a dialect that types a literal by itself does not need one.

### 5.9 E-5, the test road's speed: measured, and the design (the user's ruling of 2026-11-12; BUILD after the A/B/C merge)

> **The ruling:**
> - The judges reuse P1a's bind-once construct.
> - A value's printed text is written once per verdict.
> - Fewer statements per test: BOTH reads the database judge's fused program (§7.6 items 1-2 of
>   `docs/PCT_CORE_CLOSEOUT_START_HERE_2026_11_06.md`).
> - The judge's SQL is pinned as a second shrink-only set.

**Measured** (2026-11-12, PCT channel B essential on DuckDB, in process, 343 tests, all passing; `-Dlegend.diagnostics=sql-sizes,dump-sql`,
`-Dpct.channelb.judge=BOTH|DATABASE`; analysed by `docs/lean-sql-homework/judge_sql.py.txt`):

| Judge | Statements | Per test | Characters | Per test |
|---|---|---|---|---|
| DATABASE | 701 (343 session, 358 verdict) | 2.0 | 2,483,655 | 7,241 |
| BOTH | 2,762 (343 session, 2,419 other) | 8.1 | 2,814,871 | 8,207 |

BOTH's 2,419 other statements, by kind:

| Kind | Statements | Characters |
|---|---|---|
| a verdict statement identical to the DATABASE judge's | 182 | 492,798 |
| a verdict wrapper of BOTH's own (not the DATABASE judge's text) | 676 | 2,125,086 |
| a host side that is a literal (`SELECT 'x' AS value`, `SELECT 1 AS value`, ...) | 654 | 18,572 |
| a host side that computes | 907 | 172,241 |

- **The same statement sent twice in one test:** 354 times, 170,715 characters.
- **A side's query run twice:** 1,525 host-side statements whose query runs again inside a later verdict statement of
  the same test (153,864 characters).

So BOTH does every side's work twice: once for the host and once inside the database's verdict.

**Where the characters are: the Float canon.**
- The verdict statements' repeated text was found by greedy bind-once of every repeated bracketed group: 52% of the
  DATABASE judge's characters (31.6% of BOTH's wrappers).
- Nearly all of it is ONE expression, `SqlFn.FLOAT_CANON` spelled by `sql/dialect/FloatCanonText`.
  - That is Pure's literal spelling of a Float, the exponent unfolded as text.
  - Each canon is 4,124 characters.
  - It reads the double's text (`CAST(value AS VARCHAR)`) 43 times, the mantissa's `regexp_extract` 24 times, the
    exponent 6 times and the point's position 6 times.
- 171 canons in 34 of the 358 verdict statements are **28% of the DATABASE judge's characters** (25% of BOTH's).
- The same expression prints a Float on the product path (`toString`, a Float in a string).

**E-5a, the Float canon staged (a dialect change, not testplan's):**
- The text, the mantissa, the exponent and the point are bound once, as columns of nested one-row derived tables under
  the scalar subquery `FloatCanonText.Expansion` already writes for SQLite and BigQuery (`bindsFloatText`, which binds
  the text alone today).
- The canon becomes 665 characters over its four columns, plus about 300 characters of shells: 4,124 -> about 1,000.
- Probed (`product_sql_measure probe`, 2026-11-12):
  - **DuckDB:** the staged form answers every row as the inline form does. That held over 301,001 doubles (tiny,
    1e290-scale, thirds and NULL): 0 rows differ. Over 200,000 rows it ran **166 ms -> 52 ms** (3.2x).
  - **SQLite:** the correlated nesting works at depth 2, as its depth-1 binding does today.
  - **H2:** refuses an outer column inside a derived table at any depth (`Column "V" not found`, as P1a met). H2 keeps
    the inline form.
- Expected: about -21% of the essential suite's DATABASE-judge characters on DuckDB, and the product's Float printing
  smaller and faster on every dialect that binds. The server dialects are probed on `//core:data_source_servers_test`
  and the lanes.

**E-5b, a value's printed text once per verdict** (testplan/VerdictSql; after the merge):
- A side's CTE writes its canon three times: `__text`, `__nulls`, and `__trees`.
- It becomes one inner select computing `__c`, with the three read off `__c`. This is P1a's bind-once, applied at the
  side's CTE.
- Measured saving: 5.8% of the DATABASE judge's characters, 6.5% of BOTH's wrappers (smaller than E-5a, because the
  canon's own inside dominates).

**E-5c, BOTH reads the fused program** (testplan; after the merge):
- BOTH plans the DATABASE judge's fused verdict and nothing of its own.
- The host judge reads each side's typed value from it: per side, a typed column of the fused statement beside
  `__expected`/`__actual`.
- That brings BOTH from 8.1 to about 2 statements per test, and its characters to the DATABASE judge's plus the typed
  columns.
- The literal sides (654 statements) go with it.
- Until then, a literal side is a `Side.Constant` (no statement), if the host judge may read a planned literal. That is
  a question for the orchestrator: G6 says the database does the work, and a literal's printed text is the database's
  today.

**E-5d, the judge's pin** (after the merge):
- The corpus's `judge-sql.tsv` (today written unguarded) becomes `rcorpus/lean-sql-judge-baseline.tsv`.
- It is held by the same rule as the product's (growth fails; a shrink past 1% re-pins).
- PCT channel B gets the same pin per suite, under the judge mode the lane runs.
- It lands after the merge, so the merge does not re-pin it.

**The order:**
1. E-5a: a dialect, not testplan. It can be built now if the orchestrator agrees; its pins move down on the product
   set too.
2. E-5d's pin, at the merge.
3. E-5b.
4. E-5c.

Each one is measured by this section's commands before and after.

## 6. The guard: a product-SQL size baseline, shrink-only (G3)

> **BUILT (2026-11-12, ruled by the orchestrator: its own target in `//gates:local`, all 11 dialects).**
> - Target: `//core:lean_sql_baseline_test` (`com.legend.leansql.LeanSqlBaseline`), in the gate's `checks` suite.
>   It takes 99 s.
> - Fixed set: `core/src/test/resources/lean-sql/channel_a.jsonl`, 2,451 unique cases.
> - Pins: `core/src/test/resources/lean-sql/baseline.tsv`, 3,352 rows (set x database x family). A family is the
>   typed root's function id, "let", "refused", or the root's kind. Typed, never matched in the text.
> - Re-pin with `bazel run //core:update_lean_sql_baseline`.
> - The planted violators are `LeanSqlBaselineTest.theRuleCatchesGrowthAndBytesMovedIntoThePlan`:
>   - growth in one family fails, though another shrank;
>   - text moved into the plan fails on the plan's JSON;
>   - a second statement fails on the count;
>   - a shrink past 1% asks for a re-pin.
> - Re-measuring reproduces every pin exactly: the plans are deterministic (G10).
> - **Core's set, added the same day:** `lean-sql/core.jsonl` holds 3,527 product queries over 872 models, each
>   planned on its own runtime.
>   - Captured from `//core:core_tests_{integration,planner,server,root}`, first by a diagnostics switch in the
>     planner (ruled (a)); since E-1 (the user's ruling of 2026-11-12, the capture OUT of the product) by a test
>     harness's observer: the planner keeps one typed hook, `planner.PlanningObserver` (no file I/O, no switch), and
>     `leansql.QueryCaptureListener` (`//core:query_capture`, on core's, the corpus's and PCT's test classpaths,
>     loaded by the JUnit launcher) installs the capturing observer when `-Dlegend.capture.queries=FILE` is set.
>     `ArchitectureTest.thePlannerWritesNoFile` holds the planner's library (WebAssembly included) to writing no
>     file. The sets were not re-pinned.
>   - Left out: 22 queries naming a test's temporary file, and 3 queries carrying a 1.2 MB JSON payload (data size,
>     not SQL shape).
>   - `//core:query_capture_test` runs with the switch on. It shows the capture records the model's text, the query
>     and the runtime's NAME only (a connection naming vault references is recorded as written). It also shows the
>     capture changes no plan: replanning the recorded query gives the same plan byte for byte, and channel A's
>     DuckDB plans with the switch on measure exactly the pins made with it off.
> - **The corpus's set, added the same day** (ruled: `//spec:lean_sql_corpus_{duckdb,h2}`, in `//gates:local`, about
>   100 s each):
>   - Every discovered corpus body is planned whole (`TestExecution.planBody`, the database judge, planning only).
>   - Its PRODUCT statements -- the body's own statement nodes: results, frame runs, writes -- are pinned per database
>     and test package in `spec/src/test/resources/rcorpus/lean-sql-baseline.tsv`, by the same rule.
>   - Excluded, as ruled: verdict and canon steps, and a fixture's setup, seeds and loads.
>   - Re-pin with `bazel run //spec:update_lean_sql_corpus_<lane>`.
>   - Measured: DuckDB 325 product statements, 1,297,677 characters; H2 324, 1,246,855.
>   - The JUDGE's statements are recorded UNGUARDED (`judge-sql.tsv` in the test's outputs): DuckDB 2,523 statements,
>     20,327,235 characters; H2 2,378, 21,591,361. On the corpus the test road's SQL is about 16 times the product's:
>     E3's last item, the test road's speed.
>
> Totals at the pins (channel A, 2,451 cases):
>
> | database | statements | chars | plan JSON chars |
> |---|---|---|---|
> | DuckDB | 2,309 | 655,187 | 10,521,488 |
> | H2 | 1,754 | 550,036 | 8,348,129 |
> | SQLite | 2,305 | 1,778,534 | 11,651,948 |
> | Postgres | 2,233 | 1,160,996 | 10,983,992 |
> | MariaDB | 2,233 | 2,060,278 | 12,984,728 |
> | MySQL | 2,234 | 2,779,588 | 13,631,967 |
> | Trino | 2,309 | 1,098,766 | 11,141,683 |
> | ClickHouse | 2,301 | 1,819,026 | 11,810,341 |
> | Snowflake | 2,309 | 995,481 | 11,522,108 |
> | BigQuery | 2,224 | 1,235,651 | 10,806,797 |
> | Databricks | 2,236 | 777,755 | 10,661,332 |

- **The fixed set:**
  - PCT channel A's 2,451 unique cases (`core/src/test/resources/leansql/channel_a.jsonl`, 0.8 MB), planned on the
    11 dialects;
  - later, core's product queries and the corpus's (§7 item 1), as their own sets.
- **The bound, by family and by plan:**
  - Per dialect and per FAMILY, two pins: the total characters of the product statements, and the total bytes of
    the plans' JSON (`PlanJson`). A family is the expression's outermost Pure function, or "let", or "literal": 215
    families.
  - Each must not grow. Because the plan's JSON is pinned beside the statements' text, moving bytes from the text
    into a bound parameter, a CTE or plan data does not pass.
  - Because the pins are per family, a gain in one family cannot pay for a loss in another.
  - The statements-per-query count is pinned per dialect (today 1.00, the keyed plans' 2).
- **Shrink-only, house style:**
  - A pin that grows fails, naming the dialect, the family, the old and new figures and the worst case.
  - A pin that shrinks by more than 1% asks for a re-pin, dated, made by a generator target
    (`bazel run //core:update_lean_sql_baseline`).
  - The 1% slack keeps an unrelated lowering change from forcing a re-pin of 2,365 numbers (215 families x 11).
    Growth has no slack.
- **The planted violator:** a unit test hands the guard's comparison a planted measurement (a family's characters
  one over its pin, and a plan JSON byte moved out of the text) and asserts that both fail by name.
- **Its cost and its place:** 110 s to plan the whole set on 11 dialects, measured in one JVM. Proposed: its own
  target in `//gates:local`, not in the fast loop. Or the fast loop runs DuckDB plus one other dialect (about 20 s)
  and the gate runs all 11. **For the orchestrator to choose.**

## 7. The order of work, and the questions

1. **Measure the corpus (no product change; core's queries are measured, §3):** the relational corpus's product
   statements, per lane dialect, by the same tools: a `product-sql` diagnostic beside `sql-sizes` or a test-side
   capture. Then pin the baseline (§6) with the guard and its planted violator. The guard lands before any lowering
   change, so every change below moves its pins down.
2. **P1a, the root binding:**
   - query lets and constant repeats bound in a one-row derived table at the root;
   - the laziness rule as a typed fact on the MIR (a value that can raise is never bound ahead of its condition);
   - SQLite's JSON rule.

   Expected, from §5.1: -54% to -63% characters on the constant-rooted statements, -34% DuckDB time.
3. **P1c, explosions read through a column.** Snowflake's lead first; the lane measures it.
4. **P1b, `SqlExpr.Let`:**
   - one MIR record and one render arm per dialect, no `default ->`;
   - each dialect's form measured before it lands;
   - guards, `at`, percentile, `DateCtorRule`, the object reader.
5. **P2, P3, P4** in the dialects.
6. Each step re-pins the baseline down, with its date and its measured before/after per dialect.

**Questions for the orchestrator:**

- Q1: the guard's place (its own gate target, or part of the fast loop).
- Q2: P1a first, as the largest measured gain for the least code (root bindings only, no new MIR expression).
- Q3: whether P1b's per-dialect forms may land dialect by dialect (DuckDB and Postgres first, then the lanes).

None of this changes an answer. A change that would is a defect, caught by the existing lanes, and the rewrite
measurement's "same rows" check stands beside it. No register row is expected.

## 8. Commands

```
# 1. capture channel A's cases (DuckDB, every suite): the switchboard's pct-cases (pct-cases.tsv in each suite's
#    test outputs), then each distinct case once as the set's JSON lines
bazel test //pct:pct_duckdb_{essential,grammar,relation,standard,unclassified} --local_test_jobs=1 \
  --jvmopt=-Dlegend.diagnostics=pct-cases
python3 docs/lean-sql-homework/pct_cases_to_jsonl.py.txt channela.jsonl bazel-testlogs/pct/pct_duckdb_*/test.outputs/pct-cases.tsv
#    core's product queries: the test harness's capture (E-1) on core's suites, e.g.
#    bazel test //core:core_tests_planner --nocache_test_results --strategy=TestRunner=standalone \
#      --test_env=JAVA_TOOL_OPTIONS=-Dlegend.capture.queries=$PWD/core-capture.jsonl
# 2. plan them on every database the platform implements (planning only): sizes.tsv, sql-<database>.jsonl
bazel run //core:product_sql_measure -- measure $PWD/channela.jsonl $PWD/m0
# 3. analyse; the bind-once rewrite; time it on the in-process databases
python3 docs/lean-sql-homework/analyze.py.txt m0 channela.jsonl 5
python3 docs/lean-sql-homework/classify.py.txt m0 channela.jsonl DuckDB Postgres MySQL Snowflake BigQuery SQLite
python3 docs/lean-sql-homework/bindonce.py.txt m0/sql-DuckDB.jsonl pairs-DuckDB.jsonl   # and SQLite, H2
bazel run //core:product_sql_measure -- time $PWD $PWD/t1
python3 docs/lean-sql-homework/timing.py.txt t1/timing.tsv
```

MongoDB is not in the tables: its storeless runtime is a MongoDB connection (`StorelessRuntime.declaringConnection`),
not the relational one the tool declares, and its statement is an aggregation pipeline. It is measured separately if
the orchestrator asks.
