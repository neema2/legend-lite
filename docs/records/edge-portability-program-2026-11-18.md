# The edge as portable plans: one plan, two presentations (2026-11-18)

**Status: APPROVED by the user as the next program. It starts once the first total separation has landed and
everything is clean (the edge program's count at 0, its final audit and CI green, the user's merge word).** Nobody
starts it before then.

Plan of record for the current program: `docs/records/total-wall-edge-program-2026-11-16.md` (this program is its
Phase 10's next step and its §12 log, 05:00 to 05:25 on 2026-11-18).

## 1. The user's words (verbatim)

- 05:00, asking about the edge: "Didn't we say we originally wanted most if not all of this to be done in java after
  results came back as type product instead of parallel plan? So that mostly everything that could be would really
  be at the edge edge mostly flexing the product and then trying to fix up later in java instead of parallel tracks
  for product and compatibility? Did we kind of move away from that idea?"
- 05:15: "Yes we can do the edge as follow up after everything is clean, remember that a big part of this is also
  that we want to import upstream plans to run on our platform and export plans from us to to be able to run in
  upstream as next step, for that to happen properly our edge should really be plans that don't do a lot of the
  stuff in SQL (as opposed to lite where we want almost everything in SQL?)"
- 05:25, on the plan below: "Love that plan".

The design this restores was already the plan of record (2026-11-16): "product and test code being super slim and
fast, then compatibility at hopefully mostly at java edge on top of clean typed results on platform"; §2.2 "The edge
is Java over typed results"; §4 item 1 "No SQL that reproduces upstream's printed text, in the product, the judges or
the edge"; Phase 2 "done when no sentence is built in SQL"; Phase 4b "no canon or print text is built in SQL".

## 2. Why: where the edge drifted (measured on d-stage `4e8702016`)

The answers the edge sends to clients follow the design. The product returns typed rows, and the edge formats them in
Java: pure/v1's JSON, TDS objects, the service-test runner, and the presenter (`edge.Presenter`). The compatibility
judge (HOST, the user's ruling of 2026-11-17 01:30) decides in Java.

For test runs, about 2,000 lines of edge code still build legend-pure's text inside SQL at planning:

| Class | Lines | What it builds in SQL |
|---|---|---|
| `RelationTextLowering` | 780 | `toCSV`, a TDS's `csv`, a relation's `toString`, composed over the rows |
| `PureMessages` | 479 | a raise's sentence, by `CONCAT` (for example "Cannot divide " + x + " by zero") |
| `UpstreamVerdictWords` | 191 | the database judge's legend-pure failure words |
| `PurePrintForms` (+ `TdsNullCells`) | 184 | a Float's and a DateTime's legend-pure print; `TDSNull` for an empty cell |
| `EdgePureOnlyTexts` | 136 | texts only legend-pure writes |
| `EngineResultEnvelopes` | 88 | legend-engine's result envelope, built in SQL (check against `PureV1Writer`, which writes it in Java) |
| `PureListTexts` | 49 | a list's `[a, b]` |
| `PureDateTimePrint` | 43 | legend-pure's DateTime print |
| `EdgeDateTimeTexts` | 28 | a DateTime literal's text |
| **Total** | **1,978** | |

Phase 2 landed only on the product's side: a raise now carries its kind and typed operands. The edge's half, writing
the sentence in Java, never happened.

**How it happened:**
1. The separation count measured where code lives (product or edge), not how it works. Each census row closed when its
   code moved, and the fastest move was byte for byte. The SQL-building code changed house without being redesigned.
2. The safety net (the edge golden net) demanded byte-identical edge SQL. It rewarded moving code and punished turning
   it into Java, even when the answer came out the same.
3. Older rulings pointed the other way: D1.1 and Q15 (2026-11-10) made the edge "its own SQL render target". The
   2026-11-16 plan replaced that idea, but the code built under it stayed.

## 3. The framing

The split is not "less SQL at the edge, more SQL in lite". It is computing versus presenting:

- **Computation goes to the database, for both.** Filters, joins, aggregations and the query's real work run in SQL.
  legend-engine's own plans do the same.
- **Presentation goes where upstream puts it.** That covers how a value prints, an error's sentence, the result's JSON
  and a failed assert's message. Upstream presents in Java, in legend-engine's result serializers and legend-pure's
  runtime. Lite already presents a result's cells in its writers, after the SQL returns typed values.

So the target is **one plan, two presentations**.
- For most queries the edge's SQL is exactly the product's SQL.
- The SQL differs only on the short listed set where upstream itself computes differently inside a query (kind B of
  the plan of record's §6): a Float's `toString` inside an expression as legend-engine's cast, a parse as a plain
  cast, the Float literal's kind, H2's settings, and the corpus's scan order, raw SQL and renames. Each is listed and
  re-ruled under this program's test.
- Everything else is a different writer over the same typed rows.

This is what the user first described: flex the product, then fix up in Java at the edge.

## 4. Why plan import and export need it

- **Export, lite to legend-engine.** Our SQL goes into legend-engine's plan format. Its SQL returns typed values, so
  legend-engine's own serializer produces legend-engine's own bytes. An edge that built legend-pure's text inside the
  SQL would export plans that carry text instead of typed values: wrong, or formatted twice.
- **Import, legend-engine to lite.** Upstream's SQL goes into our plan. Our edge writer formats the typed rows as
  legend-engine would.

## 5. Where each piece lives, by the tenets

The runner gains no decisions. Formatting moves to the writers, and the planner gets simpler because it stops building
text in SQL.

| Piece | Where | Tenet |
|---|---|---|
| Which SQL runs, and which output form applies ("write as legend-engine's JSON", "word this error legend-pure's way") | **The planner**, written into the plan as data | G4: plan only plans; the plan is SQL and data only |
| Sending the statements, streaming typed rows | **The runner**, unchanged: one runner | G1: one road; exec runs only the plan's statements |
| Turning typed values into upstream's text | **The writers**, at the boundary, following the form the plan names; one presenter family (exists: `edge.Presenter`) | G6: small Java formatting at the edge; a writer decides nothing (`WriterCarriageGuardTest`) |
| Turning an upstream plan into ours (import) | **The edge's planner**, at import time | G11: the plan as portable data |
| Turning our plan into upstream's format (export) | **The edge's planner**: the ~9,400 lines of legend-engine plan text (`EnginePlans`, `PlanText`, `EngineStyleH2` and family) are most of this already | G11 |

## 6. What import must respect

- **Upstream's SQL templates (FreeMarker) are resolved at import, never at run.** Our runner never edits SQL text
  (AGENTS.md invariant 3; the charter's A3). So:
  - a template variable becomes a bound parameter;
  - a conditional node becomes a planned branch;
  - the temp-table IN protocol becomes a load.
  Anything that can't be translated is refused by name (G7).
- **Upstream plan nodes that run Java or Pure** (generated Java for graph fetches and model-to-model mappings,
  Pure-execution nodes) can't be imported as they are: our plans never hold code (G4), and we have one evaluator (G1).
  The likely answer is to plan that query again from its Pure source rather than import the node. **Open decision for
  the user when the program starts.**

## 7. The work

1. **Raise sentences.** The product's raise already carries its kind and typed operands (Phase 2). The edge writes
   legend-pure's sentence in Java from them, after the error comes back. `PureMessages`' SQL goes.
2. **Failure messages.** The host judge (the compatibility judge) already words them in Java (`EdgeHostWords`). The
   database judge's compatibility words (`UpstreamVerdictWords`) are retired.
3. **`toString`, `toCSV`, lists, dates and `TDSNull`.** Where the text is the final value an assert compares, the
   product returns typed rows and the edge prints them in Java. Upstream-specific SQL stays only where the text feeds
   further computation inside the query, each such case listed as a kind-B row and ruled.
4. **The safety net** compares outcomes (verdicts, answer bytes and messages), not the edge's SQL text.
5. **A guard:** no edge class builds upstream text as SQL, except the listed kind-B rows. Shrink-only, like every
   ledger.
6. **Export and import,** on top of 1 to 5: export from the existing plan-text code; import as an edge planner, with
   the limits of §6.

## 8. Done when

1. The edge's SQL equals the product's SQL, apart from the listed kind-B rows (measured over PCT, the corpus and
   pure/v1).
2. A lite plan exported to legend-engine gives legend-engine's answers.
3. An upstream plan imported into lite gives the same answers it gives in legend-engine.
4. The guard of step 5 reads only kind-B rows, each ruled by the user.

## 9. Open decisions, for when it starts

- Imported nodes that need Java or Pure evaluation: refuse by name, or plan the query again from its Pure source (§6).
- Each kind-B row re-ruled under the "one plan" test.
- `EngineResultEnvelopes` (SQL) beside `PureV1Writer` (Java): which callers still need the SQL one.
