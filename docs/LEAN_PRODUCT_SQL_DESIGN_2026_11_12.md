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
>   - Captured by `-Dlegend.diagnostics=capture-queries=FILE` (`QueryCapture`, the switchboard's; ruled (a)) from
>     `//core:core_tests_{integration,planner,server,root}`.
>   - Left out: 22 queries naming a test's temporary file, and 3 queries carrying a 1.2 MB JSON payload (data size,
>     not SQL shape).
>   - `//core:query_capture_test` runs with the switch on. It shows the capture records the model's text, the query
>     and the runtime's NAME only (a connection naming vault references is recorded as written). It also shows the
>     capture changes no plan: replanning the recorded query gives the same plan byte for byte, and channel A's
>     DuckDB plans with the switch on measure exactly the pins made with it off.
> - The corpus's set is next: it is one module built from upstream's files, so its baseline lives in `spec`, planning
>   each body there (§7 item 1).
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
#    core's product queries: -Dlegend.diagnostics=capture-queries=$PWD/core-capture.jsonl on core's suites
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
