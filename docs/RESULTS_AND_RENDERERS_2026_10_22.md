# One plan, one result, renderers: where output formatting belongs

**Status: PROPOSED, 2026-10-22. Awaiting the user's ratification. Nothing here is built.** If ratified, it amends
`docs/PLAN_AND_RESULT_MODEL_2026_10_18.md` §5 (F1 to F4) and needs a new clause in `docs/TENET_CHARTER.md` (§3, D3).
The rest of that document stands: the one plan record (P1 to P6), the result as a stream with sinks (S1 to S6), the
decode by the plan's facts (§4) and the fast path (§6).

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

## 2. Decisions to ratify

- **D1. One plan per query.** The plan is the base query and its column facts. The output format is never chosen
  at plan time: `TypedQuery.Output` stops being a planning input, and the `TextResult` node and its `Format` go.
- **D2. One result: typed rows, lossless.** Every Pure kind has one declared representation in the result (§5),
  each from the plan's facts or the data, never inferred.
- **D3. Every output format is a host renderer, and a renderer is carriage only** (§3). It is registered as a
  carriage site in `JavaEvalLedgerTest`. This is the charter clause.
- **D4. Legacy compatibility is a renderer, not an executor.** legend-engine's API is one renderer, judged byte for
  byte against measured legend-engine output (`OutputMatrixTest`); a deliberate difference is a row in
  `SEMANTICS_REGISTER.md`, as S97 is today. PCT and the corpus go through `ValueBridge`. Execution is never forked.
- **D5. The database keeps all meaning:** kinds A, B and C of §0.2. The SQL that exists only to write output text is
  retired (§7).

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
own scale (S97). It is judged by bytes: `OutputMatrixTest` stays the gate, and must pass unchanged when the route
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
| Decimal | DECIMAL(p, s) | `getBigDecimal` | the value's scale (S97) | BigQuery's BIGNUMERIC drops a value's scale |
| String, Boolean | VARCHAR, BOOLEAN | exact | none | none |
| StrictDate, DateTime | DATE, TIMESTAMP | `getObject` per driver | the precision | time zones; Snowflake refuses java.time re-fetch |
| year-only, year-month dates | text | text | the precision | carried as data today (kind B) |
| Enumeration | VARCHAR | exact | the enum's path | none |
| class instance, Pair, List, Map | JSON, ARRAY, STRUCT | per driver | the class layout | nested values per driver; layouts must travel in the plan |
| `Any`, mixed lists | the identity channel | per driver | the tag | carried as data today (kind B) |

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
