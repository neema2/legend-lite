# One plan, one result, renderers: where output formatting belongs

**Status: PROPOSED, 2026-10-22. Awaiting the user's ratification. Nothing here is built.** If ratified, it amends
`docs/PLAN_AND_RESULT_MODEL_2026_10_18.md` §5 (F1 to F4) and needs a new clause in `docs/TENET_CHARTER.md` (§3, D3).
The rest of that document stands: the one plan record (P1 to P6), the result as a stream with sinks (S1 to S6), the
decode by the plan's facts (§4) and the fast path (§6).

**Its execution plan is `docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md`** (proposed 2026-10-26, at the user's ask): the
tenets T1 to T8 with their guards, measurement first (M0), six phases each ending in a deletion, the gates, and the
rulings it needs (its §8: D1 to D5, D3's clause, D7, Arrow, LEAN JSON's numbers, the materialized result).

It comes out of a review with the user on 2026-10-21/22 (branch `claude/upbeat-johnson-l4hogx`). That review started
from one number: Snowflake's PCT statement for `1.5 + 2.25` was 48 KB, its served statement 6 MB.

## 0. What we found

### 0.1 The same query, three statements

`|1.5 + 2.25`, characters of SQL, measured 2026-10-22 at `eee1e560`:

| Path | DuckDB | BigQuery | Snowflake |
|---|---|---|---|
| Typed (`Execution.execute`, the base query) | 42 | 71 | 44 |
| PCT text (`TypedQuery.Output.TDS_TEXT`) | 1,687 | 11,029 | 48,288 |
| Served JSON (`pure/v1/execution/execute`, legend-engine's text) | 78,959 | 320,970 | 5,973,345 |

MySQL's served statement is 2.6 MB and MariaDB's 2.4 MB, both since before the branch (their long double text,
`MariaDb.floatLayout`, 2026-10-15). The query is one addition. Everything past the first row is formatting: the
database writes each consumer's text.

A short-term fix landed beside this note (the Float print forms spelled once on Snowflake and BigQuery: Snowflake's
PCT statement is now 6.8 KB and its served statement 5.9 KB). It treats the symptom on two databases. This note is
about the cause.

### 0.2 Four things were all called "formatting"

| Kind | Examples | Who needs it | Where it belongs |
|---|---|---|---|
| A. Text that is a query's answer | `toString`, `+` on strings, `joinStrings`, `format`, `toCSV`, a TDS's `toString` | real users: the string is the result | the database, inside the query. Unchanged. |
| B. Values SQL has no type for | year-only and year-month dates, mixed lists, class instances, graph fetch and model-to-model JSON | real users | the database, as data encodings. Unchanged. |
| C. The judges' text | `FLOAT_CANON`, `CanonicalRenderSql`, `VerdictSql` | channel B, the corpus's database judge | the database. Unchanged. |
| D. Output formats | PCT's TDS text (`TdsTextWrap`), served JSON (`WireRender`, `Render.jsonWire`), served CSV, streamed JSON | test harnesses and legend-engine's API | **this note** |

Only D is at issue. Clients of a product route need values: right, typed, exact. They do not need a spelling. Every
JSON parser reads `1.0E7` and `10000000.0` as the same number.

### 0.3 How legend-engine and BASE did it

- **legend-engine** runs the plain SQL and builds results in Java. Its served Float text *is* Java's
  `Double.toString` of the double its driver read. Lite emulates that function in SQL (`floatPrintJava`), which is
  the 79 KB on DuckDB.
- **PCT at BASE (59b547a5e)** carried values (`cos(1.0)`, a list) back typed, and `ValueBridge` built legend-pure's
  values from them in Java. Its header calls it "THE OUTBOUND BIJECTION (the permanent piece of the derived minimum)".
  Relations came back as text the database wrote. The plan route (`PctPlanRoute`, every lane since 2026-10-18/19)
  moved values onto database-written text as well. That move lost tests: "22 PCT tests passing at the merge-base
  failed on the plan route, which has the database print a Float" (`Snowflake.floatText`'s comment). Winning them back
  took a 5 KB exact-digits expression per Float.
- **The measurement of 2026-10-18** (`PLAN_AND_RESULT_MODEL` §8.1), DuckDB, 10 million rows: typed columns 10 to 12 s,
  a database-written CSV line per row 14 to 18 s, the JSON rows wire 50 to 61 s.

## 1. The design

```
                     plan renderers: lite JSON · legend-engine's plan JSON · SQL dump
                    ╱
Pure query ──▶ PLAN ── the base query + column facts (one per query; no output format in it)
                    │
                    ▼  execute: the DATABASE computes everything that has meaning (A, B, C)
               RESULT ── typed rows, lossless
                    │
                    ╲  result renderers, on the host, carriage only:
                     · Arrow IPC · JSON · CSV          ← lite-native clients
                     · legend-engine pure/v1 JSON      ← legacy clients (Studio, Query, DataCube, services)
                     · ValueBridge                     ← PCT, the corpus: legend-pure's values, no text at all
```

This is P1's move, already ratified for plans (legend-engine's plan format is a rendering of the lite plan, not a
second planner), made for results.

**The ruling (the user, 2026-10-24): two renderers, first class, over the one plan.**

| | LEAN (lite-native) | LEGACY (legend-engine compatible) |
|---|---|---|
| For | lite's own clients: Python, the tab, DataCube's lite route, services written against lite | Studio, Query, DataCube's engine route, services, and the PCT and corpus harnesses |
| SQL | the leanest and fastest possible: the base typed query, nothing in it for output (the 44 characters of §0.1, not the 6 MB) | the same statement: execution is never forked |
| Result | typed rows, faithful to Pure's types by the plan's facts (the kind, the Decimal's scale, the DateTime's precision, the tagged winner's slot), the values as the database holds them | the same typed rows |
| Text | the renderer's lossless writer (Arrow, JSON, CSV): no Pure spelling rule is applied on the host, and none is asked of the database | legend-engine's bytes: `Double.toString`, the nine-digit `+0000` DateTime, the Decimal at its scale; PCT through `ValueBridge`, no text at all |
| Judge | the round-trip gate (§6.1): the value survives | `OutputMatrixTest` byte for byte; PCT and the corpus test by test |

"Faithful to Pure's types" is the plan's facts carried to the reader, never a spelling: a `3` stays an Integer and a
`3.0D` a Decimal at scale 1 because the slot says so, not because a text said so. Where the two renderers differ is the
writer alone; what they read is one result from one statement.

## 2. Decisions to ratify

**D1 to D5 ratified by the user on 2026-10-26** ("everything else is okay"), with the program's rulings beside them
(`docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md` §8): the database judge is never deleted; no Arrow library enters the
product (the batch is our own typed arrays behind one accessor; the warehouse's Arrow IPC framing is shared, its
buffers are not); D7 is the
program's R5, its expert version waiting on the user's yes (results carry a Float's bits; a Float printed as the last
thing a column does is spelled by the writer, Pure's own implementation; a String built inside a query keeps the
exact-digits expression; no JavaScript or other non-SQL code in the database).

- **D1. One plan per query.** The plan is the base query and its column facts. The output format is never chosen
  at plan time: `TypedQuery.Output` stops being a planning input, and the `TextResult` node and its `Format` go.
- **D2. One result: typed rows, lossless.** Every Pure kind has one declared representation in the result (§5),
  each from the plan's facts or the data, never inferred.
- **D3. Every output format is a host renderer, and a renderer is carriage only** (§3). It is registered as a
  carriage site in `JavaEvalLedgerTest`. This is the charter clause.
- **D4. Legacy compatibility is a renderer, not an executor.** legend-engine's API is one renderer, judged byte for
  byte against measured legend-engine output (`OutputMatrixTest`); a deliberate difference is a row in
  `SEMANTICS_REGISTER.md`, as S104 is today. PCT and the corpus go through `ValueBridge`. Execution is never forked.
- **D5. The database keeps all meaning:** kinds A, B and C of §0.2. The SQL that exists only to write output text is
  retired (§7).
- **D6. Two renderers, named** (the user, 2026-10-24; the table in §1): LEAN and LEGACY, both over the one plan and
  the one typed result. LEAN's SQL is the base typed query and its text is a lossless writer's; LEGACY's text is
  legend-engine's bytes. A third output format is one of the two, or a new renderer under the same rule; never a
  change to the statement.

## 3. The carriage rule (D3)

> **Serialization is carriage.** A renderer is correct exactly when every value of every Pure type survives the round
> trip: written, read back with the plan's schema, the same value. Bytes are not the contract, except for two named
> byte formats: legend-engine's `pure/v1` JSON (parity) and the text a test harness reads.

It is the mirror of the read the tenet already accepts: the driver turning the database's bytes into a Java value is
"a carrier conversion, never an evaluation" (S4, `TENET_CHARTER.md` Clause 2c). Writing that value back out without
loss is the same kind of thing.

A renderer **may** lay values out: order them, quote and escape them, write names and headers, call a library's
lossless writer (Java's own `Double.toString` is legend-engine's spelling). It **may not**, and stops and hands the
work back to the database if it ever would:

- choose a spelling by a Pure rule (the plain-against-scientific threshold, the `.0` rule, a date's precision);
- convert a type, default a null, round, or shift a time zone;
- decide a Pure-only fact. A DateTime's precision is part of a Pure value: it comes from the plan or the data.

The test that separates carriage from evaluation: if Java and the database disagree, who is right? For meaning,
there must be one implementation, the database's. For carriage there is nothing to referee: the round trip decides.

## 4. The legacy renderers

**legend-engine's API.** Its envelope (`builder`, `activities`, `result`) is already Java (`PureV1Api`). The rows
become Java too: a Float by `Double.toString`, a DateTime by legend-engine's nine-digit `+0000` form, a Decimal at its
own scale (S104). It is judged by bytes: `OutputMatrixTest` stays the gate, and must pass unchanged when the route
switches. The SQL emulations go: `LiteralSpelling.floatPrintJava` (now `SqlFn.FLOAT_PRINT_ENGINE`),
`dateTimeTextEngine`, `WireRender`'s JSON and CSV wires. So do MySQL's and MariaDB's megabyte statements.

**PCT and the corpus.** Channel A becomes plan → typed rows → `ValueBridge` → legend-pure. `ValueBridge` already
builds values, lists and class instances; it gains a relation built from typed rows. No text crosses, so there is no
spelling to get wrong: legend-pure prints whatever an assertion prints, with its own code (the reference). Channel B
(the database judge) is untouched. This is how BASE carried values, now through the plan.

Neither renderer can lower PCT's score through a deliberate difference. PCT never passes through legend-engine's
renderer. `ValueBridge`'s contract is lossless, so a difference there is a bug, not a register row.

## 5. The result's representation of each kind (D2)

To be completed in the first step, one row per kind, each with its round-trip test:

| Pure kind | SQL slot today | Driver read | Fact it needs | Known risk |
|---|---|---|---|---|
| Integer | BIGINT; HUGEINT past 64 bits | `getLong` / `getBigDecimal` | the slot | an Integer past 64 bits has no Arrow type |
| Float | DOUBLE | `getDouble` (exact bits) | none | none: the 22 Snowflake tests came from text, not the read |
| Decimal | DECIMAL(p, s) | `getBigDecimal` | the value's scale (S104) | BigQuery's BIGNUMERIC drops a value's scale |
| String, Boolean | VARCHAR, BOOLEAN | exact | none | none |
| StrictDate, DateTime | DATE, TIMESTAMP | `getObject` per driver | the precision | time zones; Snowflake refuses java.time re-fetch |
| year-only, year-month dates | text | text | the precision | carried as data today (kind B) |
| Enumeration | VARCHAR | exact | the enum's path | none |
| class instance, Pair, List, Map | JSON, ARRAY, STRUCT | per driver | the class layout | nested values per driver; layouts must travel in the plan |
| a mixed selection's winner (`max`, `min`, `greatest`, `least`, `mode` over mixed Number or Date) | a tag plus one typed slot per kind (§5a) | the tag, then the slot | the slots' kinds | none: every slot is its own type |
| `Any`, mixed lists (`sort`, the Any-kind carrier) | the identity channel | per driver | the tag | carried as data today (kind B); step B of §5a |

### 5a. A mixed selection: the kind as a tag, never a spelling (RATIFIED 2026-10-23)

The user, 2026-10-23, on `[1, 2.5]->max()`: "is there really no way to do mixed lists without casting to string?"
and "do the right design and fixes now towards the correct architecture". There is, and this is it.

**The fact.** Every candidate's kind is static: in `[1, 2.5, 3.0D]` the first is an Integer, the second a Float, the
third a Decimal, known at plan time. Only the choice is dynamic. So the database returns the *choice* -- a tag -- and
each kind present has a slot of its own type; nothing is spelled unless a query asks for text.

```sql
-- |[1, 2.5, 3.0D]->max()
SELECT CASE _t.__tag WHEN 1 THEN 0 WHEN 2 THEN 1 WHEN 3 THEN 2 END AS value,   -- the slot index
       CASE _t.__tag WHEN 1 THEN _t.__c0 END AS value__0,                        -- BIGINT
       CASE _t.__tag WHEN 2 THEN _t.__c1 END AS value__1,                        -- DOUBLE
       CASE _t.__tag WHEN 3 THEN _t.__c2 END AS value__2                         -- DECIMAL(2,1)
FROM (SELECT list_position(vals, list_max(vals)) AS __tag, 1 AS __c0, 2.5 AS __c1, 3.0 AS __c2) AS _t
```

- **MIR:** `SqlExpr.Tagged(tag, comparable, candidates)`, each candidate its Pure kind, its slot type and its value;
  its type is `SqlType.Tagged(slots)`, the distinct (kind, SQL type) pairs. No dialect renders it: a query's root
  projects it (above), and a consuming rule reads the channel it needs.
- **Consumers:** returned as a result, the tag and the slots; a comparison, an equality or a sort key reads the
  comparable; `toString`, `format`, the TDS text and the JSON wires spell `CASE tag WHEN k THEN text(candidate k)`,
  each arm its own static kind, so a Float literal's text is the database's own plain text and only a computed Float
  pays the exact-digits form, once; any other consumer refuses by name until it is designed (arithmetic over a tagged
  value is one: Pure's kind of `[1, 2.5]->max() + 1` is dynamic too, and the spelling carrier read it through an
  implicit text cast).
- **Plan:** the column's slot is `TAGGED<Integer BIGINT, Float DOUBLE, Decimal DECIMAL(2,1)>`; the physical columns
  are `<name>__<i>` (`SqlType.Tagged.slotColumn`, the one owner of the name). The decoders read the tag, then that
  slot's column with its own type: `3.0D` stays a Decimal at scale 1, `3` an Integer. The judges' canon is the same
  CASE over the tag.
- **The tenet:** the unroll compares positions and rearranges shapes (C6.2); the database picks the winner and holds
  it in its own type; the host reads by the plan's facts. Nothing is folded in Java (Clause 4) and no text is
  manufactured to carry a kind.
- **What it replaces:** the F10 spelling carrier for selections -- `ids[list_position(vals, winner)]` over Pure
  literal spellings labelled `LITERAL`, whose Float spelling was ~4 KB on DuckDB and ~5 KB written four times on
  Snowflake (`[1, 2.5]->max()` 4,184 and 20,385 characters; `mode` over two Float literals 8,380 and 53,427). Step A
  (this landing) takes the five selection rules. Step B takes the lists -- mixed `sort`, the Any-kind carrier, a
  mixed collection at a root -- onto parallel typed arrays and retires the LITERAL slot, `LiteralText.parse` and the
  dialects' physical types for it.
- **Not a mode.** Both doors (§1) get the same SQL: the kind survives as a type in the product too (the user,
  2026-10-23: "worth keeping this for mixed lists in the real product/platform").

## 6. Gates

1. **The round-trip gate (built first):** each Pure kind through each carrier (every database's driver read, Arrow,
   JSON), compared by value. Nothing switches to host rendering until its kinds pass here.
2. **`OutputMatrixTest`, byte for byte**, for legend-engine's API before and after its switch.
3. **PCT and the corpus, test by test, every lane, no lane below its previous count** (the merge gate's rule), with
   a full re-baseline when channel A switches.
4. **The deletion step** is part of the program, not "later": the output-text SQL is deleted in the same program
   that replaces it.

## 7. What it deletes

`WireRender`'s JSON and CSV wires, `TdsTextWrap`, `Render`'s whole-text and per-row text wires (`jsonWire`, `csvWire`,
`tdsText`, `jsonWireRows`), `LiteralSpelling.floatPrintJava` and `dateTimeTextEngine` (and `SqlFn.FLOAT_PRINT_ENGINE`
with them), the `TextResult` node and `ExecutionPlan.Format`, `TypedQuery.Output` as a planning input, and
`PctPlanRoute`'s text path. `KeyedPivot` keeps its probe (a query concern) and loses its text outputs. Snowflake's
exact-digits code stays: in-query `toString` (kind A) and the judges (kind C) still use it.

## 8. Order of work

0. Land the current branch under today's design (the narrow Float fix, BigQuery's open tests, merge main, re-measure).
   Mixing this redesign in would make the comparison with BASE meaningless.
1. Ratify this note; complete §5's table.
2. The round-trip gate, local databases first, then the clouds.
3. legend-engine's API renderer; switch `pure/v1` execute; `OutputMatrixTest` unchanged; delete its SQL.
4. `ValueBridge` from typed rows; switch PCT channel A, then the corpus; re-baseline every lane.
5. D1: the plan stops choosing a format; delete §7's list.
6. Arrow out for lite-native clients when one needs it (Python's ARROW_IPC route already runs the plain query).

## 9. Risks

- **Driver reads become load-bearing.** Timestamps and zones, nested values, BigQuery's BIGNUMERIC scale. The
  round-trip gate finds them; a fix belongs in the plan (for example: read that column as text on that database, a
  representation chosen from the dialect's facts), never in the renderer.
- **Host rendering speed is not measured.** §8.1 measured typed columns (10 to 12 s) and database-written text; typed
  rows plus a Java writer were not. Measure in step 2.
- **A Rust (or browser) runner ports the renderers.** They are small and judged by the same fixtures. The browser
  tab already runs lite's Java compiled to WebAssembly (the comment in `PureV1Api.jsonAnswer`), so the same renderers
  can run there.
- **Two implementations during migration.** Database-written text and host renderers coexist until step 5, cross-
  checked where both run. The deletion is a committed step.

## 10. Open decisions for the user

1. **D3's clause:** is lossless carriage on the way out acceptable in Java, under §3's line?
2. **Lite-native JSON:** strictly lossless (64-bit integers and Decimals as strings, Arrow the primary format), or
   legend-engine's conventions?
3. **Arrow in memory, or only on the wire?** The 2026-10-18 rule stands: built only on a measured gap.
4. **A database-text cross-check during migration:** keep it on the local lanes until step 5, or not at all?
5. **Kind A on a database whose own text is lossy** (the one place the LEAN renderer's SQL could still differ from
   LEGACY's): an in-query `toString` of a Float on Snowflake, BigQuery, MySQL or MariaDB pays the exact-digits
   expression (about 5 KB) for Pure's text. LEAN could instead take the database's own text there, as a register row
   per database -- a query's answer would then differ by database, which LEGACY never allows. Not decided; built
   only on a measured product cost (2026-10-23's view: rare in product queries, and no fact in the plan for it yet).

   **Measured 2026-10-24** (the user asked for a cheap, widely available printf-style standard for LEAN, and doubted
   that Snowflake has no 17-digit formatter; the Snowflake lane ran a probe, not committed, over 1,946 doubles):
   Snowflake's format models accept any picture width, but a FLOAT reaches them as a SIXTEEN-digit decimal.
   `TO_VARCHAR(x, '9.9999999999999999EEEE')`, the 18- and 20-position pictures, `TM9`, `TME` and `TO_JSON` all print
   the same sixteen digits (a wider picture pads blanks, or a literal 0 under a `0` picture), correctly rounded at
   sixteen for every normal double (8 ties half up; the subnormals flushed), and 566 of the 1,946 do not read back as
   the same double (0.30000000000000004 prints 0.3; 185.46332835263038 prints 185.4633283526304). A fifteen-position
   picture is double-rounded from those sixteen digits (91 differ from a correctly rounded `%.15g`), and an `EEEE`
   picture's exponent has two digits (`E+##` from 1e100). A seventeenth digit exists only through a DECFLOAT cast:
   seventeen real digits, and every text that parsed read back (1,438 of 1,438), but the run-time cast pre-rounds to
   eighteen digits and double-rounds 80 of them (1948878528.8944077 prints ...078, 1/3 prints ...332), it prints
   seventeen digits always (0.1 as 1.0000000000000001E-01: a shortest search over 15, 16 and 17 is still needed), and
   it throws on NaN and the infinities, which kills the statement. So no one-call formatter on Snowflake writes a
   double's shortest or seventeen digits; the exact-digits expression is what losslessness costs there. The other
   printf-class formatters measured earlier: SQLite's `printf('%!.17g')` is correctly rounded and is the dialect's
   shortest search today; BigQuery's `FORMAT('%.17g')` is not (630 of 1,837 wrong, 2026-10-21), so BigQuery also
   takes the exact decimal (BIGNUMERIC). MariaDB's and DuckDB's casts, Postgres's and H2's text, ClickHouse's and
   Trino's are the shortest digits already.

   **Proposed D7 (for the user):** LEAN's Float text, where a query computes it (kind A), is the shortest round-trip
   decimal in Pure's layout: the database's own text where its cast writes it, the exact-digits expression where it
   does not (Snowflake and BigQuery, measured). The same SQL for both renderers, so the plan needs no renderer fact
   for it. No 15- or 16-digit standard: it would make the lossless engines lossy by design, and Snowflake's own
   fifteen is double-rounded. The lean-at-all-costs alternative -- Snowflake's sixteen digits (`TME`, laid out) as a
   register row for that one database -- stays available if a measured product cost ever asks for it.
