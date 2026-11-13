# Compatibility, end to end (2026-11-12)

> **The user's question, 2026-11-12:** "now if we step back - and start thinking about backwards compatibility
> holistically with all of these decisions we made - compat at the edge + java for most things + import/export transform
> - does that simplify our compatibility story or make it more complex? i think we have to think of the whole thing now
> end to end with all the pieces we want? bc i hope this also gets us out of formating floats mostly and all the other
> crazy sql we were doing"
>
> **Who writes what** (the user: "perfect do it"):
> - §1-§4 and §7: the frame, by the orchestrator.
> - §5: the body (the classification and the measurement), by D.
> - §6: the product half, by E.
> - The orchestrator then reviews the whole against the user's rulings and brings it to the user.
>
> Nothing here is built before the user reads it.

## 1. The principle: computation is the database's, presentation is Java's

- **Computation** decides WHICH rows and values come out: which rows, which values, in what order, which error is
  raised. It is the database's (G6), in the plan's SQL.
  - A sort is computation. It decides which rows survive a `take`, `first`, `slice` or a join.
  - So it can never be repaired after the fact in Java.
- **Presentation** decides HOW a value is written once it is out: a Float's text, a date's text, a Decimal's scale, an
  error's sentence, a TDS JSON envelope.
  - Lite's own writers spell the product's forms.
  - The edge's Java presenter spells upstream's.
  - G6 allows exactly this: "Small Java formatting ... only at the edge".
  - Presentation is never SQL, with one exception (§4.1).
- **Formats** decide how a plan or an API looks on the wire: engine plan JSON in and out
  (`ENGINE_PLAN_INTEROP_2026_11_12.md`), the `pure/v1` routes, and the browser's compatibility module. They are
  translations at planning, at the edge, and exec never sees them.

Lite's "crazy SQL" came from doing PRESENTATION inside SQL:
- the Float canon (`FloatCanonText`, 4,124 characters, `CAST(... AS VARCHAR)` 43 times);
- legend-pure's error sentences built in SQL guards;
- tagged mixed lists printed in the query;
- BigQuery's scale padding;
- date repairs.

The split moves each one to its home.

## 2. Three layers, one home each

| Layer | Decides | Home | SQL? |
|---|---|---|---|
| **1. Semantics** | sort order, equality, division, null order, which error | PRODUCT: lite's semantics (§3: upstream's wherever sane). UPSTREAM: a `PlanStyle` lowering strategy, one per deliberate difference, each a register row | yes: the product's own, plus the few swaps |
| **2. Presentation** | the text of a Float, date or Decimal; error sentences; TDS JSON | product writers (§5.4's forms); the edge's Java presenter (upstream's forms) | no (§4.1's exception) |
| **3. Formats** | engine plans in and out, `pure/v1`, the browser | edge translators at planning; `EdgeServer`; the lazily loaded TypeScript module | no |

```
Pure query
  → planner (PlanStyle)   SQL COMPUTES: rows, values, order, errors (as a typed kind plus operands)
                          PRODUCT: lite's semantics      UPSTREAM: + the registered swaps
  → exec                  typed rows (a double is a double, a date is a date)
  → presentation (Java)   product writers: lite's forms   |   edge presenter: upstream's forms
  → formats (edge)        pure/v1, engine plan JSON in and out, the browser module
```

## 3. The rulings this rests on (dated; the user's words)

- **The product is lite's own** (2026-11-12): "we really want the most expert lean and fast SQL on the product path and
  push ALL compatibility to the edge".
  - Answers may change if they are typed and sane: the same on every database, no silent loss, deterministic. Each
    change is a register row.
- **The cast-and-case rule** (2026-11-12): a platform CAST or CASE in product SQL that the user did not ask for is
  almost certainly wrong. E-4's census (`//core:cast_case_census_test`) counts them by typed reason.
- **The product's print forms** (§5.4 of the start-here): a value stays typed unless a string is asked for. A Float
  prints with 6 decimals, trailing zeros and `.` trimmed. Pure's exact texts are the edge's.
- **Mixed lists, the hybrid** (2026-11-12), and **the sort like upstream** (2026-11-12: "can we do the sorting like
  upstream?"): a mixed list sorts by kind rank (number, date, boolean, string), then value, in the database.
- **The layered edge** (D-2, 2026-11-12): "measure PCT and RCorpus and then hopefully switch to Layered".
- **The browser's middle option** (D-1, 2026-11-12): "D-1 is if we want our UIs to talk to real legend-engine, i like
  the middle compatibility option".
- **Engine plans in and out** (2026-11-12): `ENGINE_PLAN_INTEROP_2026_11_12.md`.
- **PROPOSED, for the user to rule:** in layer 1, the product adopts upstream's COMPUTATION wherever it is sane (the sort
  is the precedent, and the upstream-fidelity tenet asks for it). In layer 2, the product keeps its own simple forms.
  - This reconciles "the product is lite's own" with the fidelity tenet: we differ in how a value is written, rarely in
    which value it is.
  - The register's semantic rows then become a short list.

## 4. What leaves SQL, and what honestly stays

**Leaves SQL:**
- The Float canon leaves the product. The product prints by §5.4's simple form; results are re-spelled in Java at the
  edge.
- Error sentences become a typed kind plus operands (D's `RaiseText`/`ErrorKind`), spelled in Java.
- Tagged lists become typed lists (the hybrid).
- Exec's decode repairs go: BigQuery's scale padding, the BC-date repair, kinds read from text, the positional read.
  The database returns typed values (E's P3 items).
- Engine-only SQL spellings move to edge dialect subclasses.
- The test judges' canon comparisons become typed-value comparisons (E-5c).

**Stays:**
1. **Printing inside a query, in UPSTREAM.** A `toString`, `format` or `makeString` whose text is filtered, joined on,
   grouped or concatenated must be computed by the database, in upstream's exact text. This is the one place the canon
   survives, in the edge's SQL only. D measures how often PCT and both corpora hit it (§5).
2. **Each deliberate semantic difference** needs its UPSTREAM strategy. §3's proposal keeps this list short.
3. **Two styles to test.** PCT runs UPSTREAM; product tests run PRODUCT.
4. **Creep:** compatibility drifting back into the product. §7's guards stop it.

## 5. The body (D)

One table, every known item, with these columns:
- the item and its site;
- today's behaviour;
- its LAYER (1, 2 or 3);
- what it becomes;
- the SQL it removes from the product (characters or statements where measured);
- its register row (new, changed or deleted);
- its owner (D or E).

The items to classify:
- K1-K9 (`COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md`);
- P3's sites 1-7 (`LEAN_PRODUCT_P3_2026_11_10.md`);
- E-R1 to E-R5;
- every semantic row of `SEMANTICS_REGISTER.md`;
- the judge's canon;
- D-1's four local routes;
- import and export.

**The measurement** (PCT, both corpora):
- how many queries print INSIDE the query (§4.1);
- for each register row, whether it is semantics or presentation;
- how much of D's planned edge SQL the layered presenter removes.

Answer the user's question with numbers: how much compatibility code, and how much SQL, before and after.

## 6. The product half (E)

- **Product semantics:** every place lite's computation differs from upstream's today, each with:
  - adopt upstream's (and say why that is sane), or keep lite's (and say why upstream's is not sane);
  - the register row.
- **Product print forms:** §5.4's table checked against E-4's census. Every CAST or CASE whose reason is `print`
  should vanish from the product, except §4.1's in-query printing. List what remains.

## 7. Guards (proposed; each with a planted violator)

- **Same rows in both styles.**
  - PRODUCT and UPSTREAM compute the same rows in the same order for every PCT and corpus query, except the registered
    semantic rows. Only presentation differs.
  - This is the guard that proves layer 1 is small.
- **No presentation in product SQL:** E-4's `print` reason at zero on the product, except in-query printing.
- **The edge depends on the product, never the reverse** (G5): no product class imports `com.legend.edge`. The existing
  D guards cover the strategies, `@EdgeMayOverride` and the browser bundle.

## 8. Questions for the user (compiled from §5 and §6 at review)
