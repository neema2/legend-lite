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

Written by D, 2026-11-13, at `claude/total-wall-d` d2c8ea7, PlanStyle not yet built. Every number below was measured
that day on this branch (§5.2 says how); nothing is recalled.

> **Ruled by the user, 2026-11-13** (start-here §6.1 item 3, O1-O9), applied to this section on 2026-11-13:
> - **O1:** the product adopts upstream's COMPUTATION wherever it is sane; a borderline case goes to the user.
> - **O2:** mixed lists keep kinds (E-R1 to E-R3 are no longer divergences), so the edge needs no list form of its own:
>   the tagged-list print work planned for D1.3 is dropped.
> - **O3:** legend-engine's SQL text stays at the edge. The EngineStyle printers are KEPT, as edge dialects: option (a)
>   of §5.4's conflict 1; D's recommendation (b) was not taken.
> - **O4:** `indexOf` and `average([])` answer legend-pure's value in the product, and the engine's at the edge.
> - **O6, O7:** engine plans in and out per `ENGINE_PLAN_INTEROP_2026_11_12.md` (§3.3, §5); the browser's
>   compatibility module is one more engine behind DataCube's `Engine` interface (D-1's row below).
> - The raise row this section names is **S308** (S307 went to `executeInDb`, 77e6ab7); D's rows take S308-S339.

### 5.0 The answer, first

**For the product: simpler, and measurably so. For compatibility as a whole: about the same amount of code, all of it
in one place, and a small part of the SQL it writes today.**

- **The product loses all of its print SQL.** That means `FLOAT_PRINT`, the date and decimal print forms, the raise
  sentences, the engine-text printers and the dialects' P14 repairs. Today they sit in 289 registered compatibility
  spellings across 85 product files (`CompatSpellingGuardTest`). After D2 that register is empty except `JUDGE` and
  `STAYS`.
- **The edge does NOT lose its print SQL. It keeps a smaller part of it.**
  - The layered presenter takes the cases where a print IS the answer.
  - Those cases are rare on PCT and the corpus: 20 of PCT's 1,910 channel-A statements, and 541 of the DuckDB
    corpus's 6,151.
  - Printing INSIDE a query is ten times more common. 121 PCT statements and 488 corpus statements do it, and
    upstream's text must be computed there by the database (§4.1).
  - So `FloatPrintText` and the date, Decimal and Boolean print forms move to the edge's dialects. They are not
    deleted.
- **The big SQL win is the judge, not the presenter.**
  - PCT channel B, the database judge, writes 10.4 MB of SQL for 5,069 statements. 9.8 MB of it sits in statements
    that print inside the query.
  - Mostly that is the judge's canonical text: 306 statements use `FLOAT_CANON`, plus thousands of `CAST ... AS
    VARCHAR`s concatenated into the canon.
  - E-5c's typed comparisons are where "the crazy SQL" goes away.
- **What a Java presenter removes from the edge's SQL is small.**
  - PCT channel A: 7.8 KB of 542 KB (1.4%).
  - DuckDB corpus: 10.5 KB of 9.3 MB (0.1%). H2 corpus: 10.9 KB of 8.6 MB (0.1%).
  - The presenter is right for the reasons §1 gives: presentation belongs in Java, and the export and import plans
    need no printing at all. It is not right for the size of the SQL on these suites.

### 5.1 The classification: every known item, one row each

Layers: **1** semantics (computation), **2** presentation, **3** formats; **0** for an item that is not part of a
query's run-time answer (compile time, tooling). "Removes from the product" is the product SQL or code the item takes
out of the product; where it is SQL, the measurement is E-4's census (`//core:cast_case_census_test`) for the product
style, and §5.2's for the upstream style.

#### K1-K9 (`COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md`)

| item, site | today | layer | becomes | removes from the product | register | owner |
|---|---|---|---|---|---|---|
| K1: SQL guards raising legend-pure's sentence (13 kinds; `Scalars`, `DecimalKindRules`, `ToOneGuard`, `Coercions`, `CastPolicy`, `ListRules`, `ListEncodings`, `DateCtorRule`, `CheckedEnvelope`) | the database raises Pure's exact text | **1** (that it raises, and which kind) + **2** (the sentence) | **built, local** (D1.1, families 1-19): the plan raises a `RaiseKind` with typed operands. The product writes the kind's name; the edge's `PureMessages` spells Pure's sentence | every Pure sentence literal in product SQL (the guard's D1.1 rows: 2 left, the resolver's two casts) | **new S308** (a product raise is its kind and place, in lite's words). It reopens S138 for the product and keeps S138 for the edge | D |
| K2: `sqrt`'s operand spelled through `FLOAT_PRINT` | Pure's Float text inside a raise | **2** | a typed operand; the edge spells `Double.toString` in Java (`PureMessages`) | one `FLOAT_PRINT` per `sqrt` guard | under S308 | D |
| K3: DuckDB's words on Postgres, MariaDB, MySQL and SQLite | the reference database's sentence | **2** (the kind is 1) | **built, local** (families 18-19, K3): typed kinds | 4 sentence families | under S308 | D |
| K4: engine SQL spellings (quoting, upper-casing, the lexicon, the flag-gated scan order, DDL details, the bare Float literal) | engine's text conventions in product dialects | **2** (SQL text, no answer). The NULLS order is **1** and stays (ruled 2026-11-12) | the scan order and the lexicon's engine half go to the edge's dialects (D1.5). The quoting stays the product's one spelling unless Q5 rules otherwise | the scan order's 5 hits and the lexicon (D1.5 rows) | none: no answer changes | D |
| K5: engine-TEXT printers reached from the planner (`toSQLString`, `planToString`, `toDDL`, the allocation and activity text) | `EngineStyleH2/DB2/Composite`, `EngineText`, `EnginePlans`' text half, `PlanAllocations`, `RawSqlBoundary`: 4,854 lines, 111 register hits (D1.7) | **3** (a plan's text is a format) | edge translators at planning, reached through `PlanStyle` (parked as local WIP, 2026-11-12). **See §5.4, conflict 1** | the whole family from the product (4,854 lines) | **new**: the product's answer for these natives (refused, or the product dialect's own SQL), in the parked question to the orchestrator | D |
| K6: the engine writers (`PureV1Writer`, `TdsObjectsWriter`, `CellText`'s engine half, `presentedName`) | Java at the result boundary, engine's TDS JSON and forms | **2** (cells) + **3** (TDS JSON) | the edge's presenter (D1.4); `Output.Engine` registered by the edge | 389 writer lines, plus `CellText`'s engine half, from the product's result library | Q8's pivot-name row, if the product's own writers stop quoting | D |
| K7: the trace comment, MariaDB's read-back, the legacy decode rules | gone (C1, C2), or P3's | **3** (the trace text) / **1** (decode) | done by C. The decode is P3, below | (done) | S304 (exists) | C / E |
| K8: compile and server sentences in legend-engine's words (~15) | engine's text | **0** with a **2** sentence | the product's own sentences keyed by phase and kind; the edge (`pure/v1`) spells engine's (D1.6) | 19 register hits (D1.6) | new row with D1.6 (Q6) | D |
| K9 P1, P2, P5 (`toString`, `pureToString`, `format`) | Pure's print forms in SQL | **2** where the print IS the answer; **1** where the text feeds computation (§4.1) | the product prints by §5.4's forms. The edge keeps Pure's forms in SQL for in-query use only, and its Java presenter re-spells an answer | product: E-4's `print` reason. Upstream: §5.2's table | S308 family, with §5.4 of the start-here (exists as the ruling) | D (edge) / E (product forms) |
| K9 P3: `FloatPrintText` and the twelve `FLOAT_PRINT` arms | Pure's Float text in SQL (183 lines; the heaviest single form) | **2**; **1** in-query | leaves the product. Moves to the edge's dialects **for in-query use only**: 2 PCT and 36 DuckDB-corpus statements need it there | the twelve product arms | under the print-forms row | D |
| K9 P4: a Float over an integral store column printed as an integer | engine's form | **2** | the edge's presenter | one `LiteralSpelling` arm | Q9 | D |
| K9 P6: `makeString`'s `TDSNull` | engine's TDS null cell | **2** | `''` in the product, `TDSNull` at the edge (Q14) | the D1.3 rows (`TDS_NULL_CELL`, 5 hits) | Q14 | D |
| K9 P9, P10: a relation's `toString` (TDS text), `toCSV` | Pure's and engine's text, written in SQL by `Render` (834 lines) | **2** | refused in the product (an answer is rows). The edge's presenter in Java: the rows come back typed, then are written | `Render`'s text half from the product | Q12 | D |
| K9 P11: `CellText.pure` (lite's CSV, JSON and Arrow text) | Pure's forms in lite's own outputs | **2** | the product's writers write §5.4's forms | the Pure forms in `CellText.pure` | §5.4 (ruled) | E (product writers) |
| K9 P12: `presentedName` | engine's pivot quoting in every writer | **2** | the edge only (Q8) | one call per writer | Q8 | D |
| K9 P13: MongoDB's pipeline prints | Pure's Float, date and `format` texts in a pipeline | **2** | the product's forms. The edge's presenter for answers; no in-query printing on MongoDB at the edge until measured | MongoDB's print arms | under the print-forms row | D |
| K9 P14: each dialect's repairs to match Pure's or DuckDB's text | per-database text repairs | **2** | deleted from the product: §5.4's forms are the database's own | the P14 sites in 8 dialects | under the print-forms row | E (product) |

#### P3's sites 1-7 (`LEAN_PRODUCT_P3_2026_11_10.md`), E's

| site | layer | becomes | register | owner |
|---|---|---|---|---|
| 1 a REAL re-parsed through its shortest text | **2** | decode carries the driver's double; the writers spell the cell | P3's draft row | E |
| 2 BigQuery's scale padding | **2** | the database writes the scale, else deleted | P3's draft row | E |
| 3 the TIMESTAMPTZ shift to UTC | **1** (which instant is read) | the database, where the plan knows the column | P3's draft row | E |
| 4 the BC-year repair | **1** (which date) | the database's text in a temporal slot; the array-leaf arm deleted | P3's draft row (lite right where upstream is wrong) | E |
| 5 kinds chosen from a cell's text | **1** (which type) | deleted: a planned slot or a refusal | none | E |
| 6 the positional read fallback | **1** (which column) | deleted | none (G7) | E |
| 7 the dynamic-pivot envelope | **3** | the edge's writer; the plan marks a generated column | Q6 of P3 | E |

#### E-R1 to E-R5 (E's design doc §5.7, from E's reading of pinned legend-pure)

| item | upstream | the hybrid | layer | cost in SQL, each way |
|---|---|---|---|---|
| E-R1 `[1, 1.0]->removeDuplicates()` | 2 elements (`CompiledSupport.eq` is false across classes) | 1 (promoted to Decimal) | **1** | **Upstream's way:** the distinct key carries the element's kind as well as its value: one more column in the `DISTINCT`/`GROUP BY`, the tag channel `TaggedLists` already writes (562 lines, today's machinery). **The hybrid's way:** one promoted column, no tag |
| E-R2 `[1, 2.0]->at(0)` | an Integer (each element keeps its class) | a promoted Float | **1** | **Upstream's way:** the element's kind travels with it (a tag per element, read at the answer). **The hybrid's way:** none. The hybrid is leaner in SQL by one channel per mixed list |
| E-R3 StrictDate vs its midnight DateTime | unequal, and the StrictDate sorts first (`DateFunctions.compare`) | promoted, so equal | **1** | **Upstream's way:** a kind rank in the comparison key, the same rank E-R4 uses. **The hybrid's way:** none |
| E-R4 a cross-kind sort | kind rank, then value | RULED tonight: like upstream, in the database | **1** | one rank column in the sort key (already ruled) |
| E-R5 | no answer change | | **0** | none |

**Ruled (O2, 2026-11-13): mixed lists keep kinds, as upstream; E-R1 to E-R3 are no longer divergences.** E-R1 to E-R3 are exactly §3's PROPOSED principle: whether the product adopts upstream's computation where sane. All
three upstream answers are typed and deterministic, so they are sane by §3's test. Their cost is the one tag or rank
column that E-R4's ruling already pays for. **D's recommendation: adopt upstream's for all three.** Then one
mechanism, kind rank plus value, serves the sort, equality and distinct, and the hybrid keeps only its typed
carriage. That is E's to bring: E owns the hybrid.

#### The register (`SEMANTICS_REGISTER.md`, all 179 rows; the per-row file is §5.3's homework)

| layer | rows | live (not withdrawn, closed or fixed) | live, outside the islands (`#Cypher`, `#IQ`, `#SQL`) |
|---|---|---|---|
| 0 not run time (typing, compile, scope, tooling, the judges' harness) | 67 | 67 | 53 |
| **1 semantics** | 74 | 66 | **37** |
| 2 presentation | 21 | 21 | 18 |
| 3 formats | 17 | 16 | 16 |

- **The 37 live layer-1 rows** against legend-pure and legend-engine: S9 S16 S17 S18 S19 S20 S22 S26 S27 S63 S94 S97
  S105 S108 S109 S110 S111 S119 S120 S123 S124 S127 S129 S134 S139 S140 S173 S175 S176 S177 S179 S180 S181 S183 S184
  S186 S187.
- 12 of the 37 read as sane to adopt (typed, deterministic, the same on every database). The rest are upstream
  defects, upstream disagreeing with itself, or engine refusals lite answers. Each is E's §6 to rule.
- These 37 rows are what the UPSTREAM style's lowering strategies would have to cover, IF each one became a strategy.
  §3's proposal shrinks that list.
- **Layer 2 (21 rows):** S2 S4 S69 S85 S87 S101 S104 S107 S115 S117 S118 S121 S138 S174 S189 S193 S194 S195 S300 S304
  S306. All are presentation; none needs SQL at the edge once the presenter exists, except where §4.1 applies.
- **Layer 3 (17 rows):** S28-S39 (engine input), S89 S125 S188 S199 S301. All are translators at planning.

#### The rest

| item | today | layer | becomes | removes from the product | register | owner |
|---|---|---|---|---|---|---|
| the judge's canon (`FloatCanonText` 206 lines, `CanonicalRenderSql` 1,100, `VerdictSql` 2,718; the host judge's Java canon) | the database judge compares canonical TEXT in SQL | **2** (it compares presentations) | E-5c: typed-value comparisons. PCT channel B's 306 `FLOAT_CANON` statements and its 8,880 concatenated casts go | test road only. It is the largest SQL in the measurement: 9.8 of channel B's 10.4 MB | the judge's rows (S131, S132, S135, S137, S142, S144, L0 here) | E |
| D-1's four local routes (`grammarToJson/model`, `grammarToJson/lambda`, `jsonToGrammar/lambda`, `compilation/lambdaRelationType`) | served in the tab and in Python through `Boundary.pureV1` → `PureV1Routes` | **3** | lite-owned exports (`modelElementsOrError`, `parseLambdaOrError`, `printLambdaOrError`, a JSON-lambda relation type). The engine clients move to a lazily loaded TypeScript module. `//wasm:boundary` drops `//core:edge`. **O7 (ruled 2026-11-13): that lazily loaded TypeScript compatibility module IS a real legend-engine, as one more engine behind DataCube's `Engine` interface, built from D's translations (the plan import, the edge presenter); DataCube's code never knows** | the edge from the browser bundle | none (no answer changes) | D, after A/B/C merges |
| import and export of engine plans (`ENGINE_PLAN_INTEROP_2026_11_12.md`) | export half there (`generatePlan`, the product statement); import TypeScript-only, one shape | **3** | translators at planning at the edge (D3.1-D3.4) | `generatePlan`'s writer from `server/` (D3.1) | S199 (changes: export plans in UPSTREAM) | D |

### 5.2 The measurement

**How.**
- A probe (`docs/compat-e2e-homework/print-site-probe.patch.txt`; measurement only, never on a branch) walks every
  statement a dialect renders, and records each print it contains:
  - **the print forms:** `CAST(... AS VARCHAR)`, `FLOAT_PRINT`, `FLOAT_CANON`, `DECIMAL_TO_TEXT`, `BOOL_TO_TEXT`,
    `STRFTIME`;
  - **each print's position:**
    - **OUTPUT:** a root projection IS the print of one value.
    - **NESTED:** an inner select's projection is exactly a print.
    - **IN-QUERY:** anything else, together with the node that consumes the print and the print's operand;
  - **the statement's SQL**, rendered again with its OUTPUT prints replaced by the value they print, which gives the
    characters a presenter removes.
- A raise's operands are excluded, since they are D1.1's typed operands.
- **Runs:** PCT channel A on DuckDB (`//pct:pct_duckdb`, 5 suites), PCT channel B (`//pct:pct_channel_b`, 5 suites),
  and the corpus on DuckDB and H2 (`//spec:corpus_duckdb`, `//spec:corpus_h2`). All are green, all plan in the
  UPSTREAM style, all at d2c8ea7, on 2026-11-13. Statements are counted once by their text.
- **Classifying IN-QUERY prints**, by the node that consumes them:
  - `CAST(NULL AS VARCHAR)` is a **typed NULL**, not a print.
  - A cast under `=`, `IS NULL`, `COALESCE` or `CASE` with a column operand is a **typing cast**. Those belong to the
    cast-and-case rule (E-4), not to printing.
  - A print under a string function (`CONCAT`, `STRPOS`, `REPLACE`, `SPLIT`, `LPAD`, `FORMAT`, `RTRIM`, ...), or any
    Pure print form (`FLOAT_PRINT`, `DECIMAL_TO_TEXT`, `BOOL_TO_TEXT`, `STRFTIME`, `FLOAT_CANON`) anywhere, is
    **genuine in-query printing**.

**Statements, by where they print (the UPSTREAM style).**

| run | statements | no print | output print only | genuine in-query print | of which a Pure print form in-query | typing or NULL casts only | nested only | SQL total |
|---|---|---|---|---|---|---|---|---|
| PCT channel A (DuckDB) | 1,910 | 1,716 | 20 | **121** (6.3%) | 98 (`STRFTIME` 96, `FLOAT_PRINT` 2) | 50 | 3 | 542 KB |
| PCT channel B (the database judge) | 5,069 | 2,399 | 19 | **2,011** (39.7%) | 810 (`STRFTIME` 363, `FLOAT_CANON` 306, `FLOAT_PRINT` 92, `BOOL_TO_TEXT` 47, `DECIMAL_TO_TEXT` 30) | 391 | 249 | 10.4 MB |
| corpus, DuckDB | 6,151 | 4,489 | 541 | **488** (7.9%) | 232 (`STRFTIME` 142, `DECIMAL_TO_TEXT` 45, `FLOAT_PRINT` 36, `BOOL_TO_TEXT` 18) | 604 | 29 | 9.3 MB |
| corpus, H2 (first pass; no consumer split) | 5,896 | 4,439 | 534 | 905 (in-query, typing casts included) | `FLOAT_PRINT` and `DECIMAL_TO_TEXT` as DuckDB | — | 18 | 8.6 MB |

**Print sites in-query, by consumer** (corpus DuckDB; PCT channel A in brackets):
- typing casts: 6,046 [163];
- typed NULLs: 1,861 [8];
- a plain cast under a string function: 2,966 [60];
- a Pure print form: 800 [102].

**What the layered presenter removes from the edge's SQL** (the OUTPUT prints, rendered away):

| run | SQL in statements with an OUTPUT print | after | removed | share of all the run's SQL |
|---|---|---|---|---|
| PCT channel A | 9,116 | 1,336 | 7,780 chars | 1.4% |
| PCT channel B | 2,491,478 | 2,483,750 | 7,728 chars | 0.1% |
| corpus, DuckDB | 597,345 | 586,823 | 10,522 chars | 0.1% |
| corpus, H2 | 547,400 | 536,531 | 10,869 chars | 0.1% |

**What the numbers say.**
1. **§4.1 is not rare on the corpus.**
   - 7.9% of the DuckDB corpus's statements print inside the query: a `toString`, `format` or date text that is then
     concatenated, searched (`STRPOS`, i.e. `contains`), replaced or split.
   - 3.8% (232) need a Pure print form there.
   - On PCT channel A it is 6.3%, almost all `STRFTIME` under `format`'s `%t{}`.
   - These must stay SQL in UPSTREAM.
2. **A print as the answer is rare.** 1% of PCT channel A's statements, 8.8% of the corpus's. Most of the corpus's are
   plain casts, which already read the same in both styles.
3. **The judge prints most.** Channel B's 2,011 printing statements are the database judge's canonical text: 306 with
   `FLOAT_CANON`, and 8,880 casts concatenated under string functions. E-5c removes them.
4. **Typing casts outnumber prints.** 6,046 typing casts and 1,861 typed NULLs in the corpus. They are not
   presentation, and the layered design does not touch them. They are E-4's cast-and-case census, and the frame
   should not count them as "crazy SQL from printing".

**Register rows, semantics or presentation:** §5.1's register table. 37 live layer-1 rows outside the islands, 21
layer-2 rows, 17 layer-3 rows.

### 5.3 Before and after: compatibility code and product SQL

| | before (today, d2c8ea7) | after (the frame, D's share built) |
|---|---|---|
| compatibility spellings in product files (`CompatSpellingGuardTest`) | 289 hits in 85 files (D1.1 2, D1.2 44, D1.3 16, D1.4 36, D1.5 5, D1.6 19, D1.7 111, D1.8 27, JUDGE 28, STAYS 1) | JUDGE 28 → 0 with E-5c, STAYS 1. **0 in the product** |
| engine text family (K5) | 4,854 lines, reached by the product planner | the same lines at the edge (O3, ruled 2026-11-13: option (a) of §5.4's conflict 1) |
| Pure print forms in SQL (`FloatPrintText` 183, the 12 `FLOAT_PRINT` arms, `LiteralSpelling`'s print forms, `Repr` 99, `TaggedLists.print`, P14's repairs in 8 dialects) | in the product, written on every database | **leave the product.** At the edge, kept only for in-query printing: `FloatPrintText` and the date, Decimal and Boolean forms stay, the P14 repairs go |
| TDS text and `toCSV` (`Render`, 834 lines) | SQL in the product | Java at the edge's presenter (smaller: written once over typed rows, not per database) |
| writers (`PureV1Writer` 298, `TdsObjectsWriter` 91, `CellText`'s engine half) | the product's result library | the edge's presenter |
| the judge's canon (`FloatCanonText` 206, `CanonicalRenderSql` 1,100, `VerdictSql` 2,718) | SQL on the test road | E-5c's typed comparisons: most of these lines, and 9.8 MB of channel B's SQL |
| raise sentences (K1, K3) | Pure's text in product SQL | **done locally:** kinds in the product, `PureMessages` (139 lines) at the edge |
| upstream-style SQL that a presenter removes | — | 0.1-1.4% of each run's SQL (§5.2) |

**In one line:**
- **The product:** simpler. No compatibility spelling, no print SQL beyond §5.4's forms, no engine text.
- **Compatibility:** relocated more than reduced. The presenter adds a Java class family and takes little SQL. The
  real reduction is the judge (E-5c) and the raise sentences (done).

### 5.4 Where D's current plan conflicts with the frame

1. **K5, the EngineStyle printers (parked). Counted 2026-11-13 at 9525fe6.**
   - The frame puts plan text in layer 3, as translators at planning. The interop design exports a plan in the
     UPSTREAM style with lite's own SQL text, not engine's spelling (§3.2 there).
   - If `toSQLString` and `planToString` answered the UPSTREAM dialect's text, the engine-text family could go, and
     the parked move would be wasted. What decides it is how many asserts compare engine's exact SQL or plan text.
   - **Method.**
     - For one run, every engine-text printer refused (`EngineText.engineText` and `enginePlanText`), except where
       the caller was DDL or seed setup. Each call was logged with its caller.
     - PCT channel A (5 suites) and channel B (5 suites) ran, and both corpora's host passes.
     - Every corpus assert was compared with the green baseline. Each lost test's body was read in the pinned
       legend-engine sources for the engine text it names.
     - Measurement only: the probe is not in the code.
   - **PCT: 0 tests depend on engine text.** All 10 suites stay green with the printers refusing.
   - **The corpus (DuckDB: 2,517 tests; H2: 2,404, the same shape).** The printer calls, by caller:

     | caller | calls |
     |---|---|
     | `PlanAllocations.activitySql` (an execute frame's activity SQL) | 1,606 |
     | `EnginePlans.planModel` | 325 |
     | `EnginePlans.toSqlString` | 120 |
     | `EnginePlans.sequencePlan` | 60 |
     | `EnginePlans.planToString` | 57 |
     | `EnginePlans.engineTestSql` | 32 |
     | `PlanAllocations.node` | 8 |
     | the generator's plan text (`BodyDriver`) | 6 |
     | `EnginePlans.crossDbTdsPlan` | 5 |
     | DDL and seed setup (`ddlText`, `SeedSqlForms`; allowed) | 65 |

     1,641 passing DuckDB tests (1,615 on H2) lose an assert. The 1,612 DuckDB bodies found in the pinned sources:

     | what the test's body names | tests |
     |---|---|
     | the executed statement's SQL (`activities`, `RelationalActivity`, `sqlRemoveFormatting`) | 965 |
     | plan text (`planToString`, `executionPlan`, `generatePlan`) | 111 |
     | `toSQLString` | 73 |
     | none of these | 463 |

     The 463 are collateral: lite writes the activity SQL for every execute frame, whether or not the test reads it.
     **So about 1,150 corpus tests (46% of 2,517) assert legend-engine's exact SQL or plan text**, almost all of it
     the executed statement's text.
   - **The choice (the user rules).**
     - **(a) Keep the engine printers, moved to the edge** (the parked move). The corpus keeps checking engine's SQL
       text: fidelity of HOW, in the corpus only.
       - Cost: the engine-text family stays, about 4,850 lines (`EngineStyle*` 2,892, `EnginePlans` 932,
         `PlanAllocations` 553, `RawSqlBoundary` 339, `EngineText` 82, `PlanEnvelope` 56), relocated to the edge.
     - **(b) Engine's SQL text is a non-goal**, by the user's own "fidelity is WHAT, not HOW".
       - The ~1,150 SQL-text asserts become one register row ("lite does not reproduce legend-engine's SQL or plan
         text"). The corpus judges those tests by their answers alone.
       - The engine-text family is deleted, about 4,850 lines.
       - `toSQLString` and `planToString` answer the UPSTREAM dialect's text, as export writes it.
       - Cost: the corpus loses its SQL-text checks (about 1,150 tests' text asserts); its answers are still checked.
       - PCT is unaffected either way.
   - **D's reading: (b).**
     - The product owns its SQL, and fidelity is answers: the user's ruling of 2026-11-12.
     - (b) makes the edge smaller instead of moving it.
     - It gives up a check the corpus has today, which is why it is the user's call.
   - **RULED (O3, 2026-11-13): (a).** The printers are kept, as edge dialects; the parked move resumes.
2. **D1.2 and D1.3 (Pure's print forms at the edge).**
   - My plan was edge dialect arms for every print form.
   - The frame says Java presents an answer. §5.2 says that removes only the OUTPUT positions, and those are rare.
   - **The plan shrinks** to the in-query arms (`FLOAT_PRINT`, `STRFTIME`, `DECIMAL_TO_TEXT`, `BOOL_TO_TEXT`, `Repr`
     and the list forms where in-query), plus a Java presenter for answers.
   - Both are needed. Neither replaces the other.
3. **D1.4 (the writers) is unchanged.** It is the presenter's first half.
4. **The edge's raise sentences (D1.1)** are already Java: presentation, as the frame wants.

### 5.5 The homework

- `docs/compat-e2e-homework/print-site-probe.patch.txt`: the probe, a temporary class and a two-line hook in
  `AnsiSqlRenderer`, never committed to code.
- `docs/compat-e2e-homework/register-layers.tsv`: all 179 register rows with their layer, the adopt reading for layer
  1, and a note.
- `docs/compat-e2e-homework/analyze.py`, `analyze-consumers.py`: the two readers of the probe's output.

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
