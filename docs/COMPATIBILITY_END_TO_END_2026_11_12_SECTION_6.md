# Compatibility, end to end — §6, the product half (E, 2026-11-12)

> This is the text of §6 of `docs/COMPATIBILITY_END_TO_END_2026_11_12.md`, which lives on `claude/total-wall-docs`.
> E wrote it on `claude/total-wall-e`; the orchestrator merges it in. It is docs only: nothing here is built before the
> user reads it.
>
> **Evidence:**
> - Upstream's answers were READ from the pinned legend-pure (`@legend_pure_src`). Each item names the file.
> - The product's SQL was measured by E-4's census (`//core:cast_case_census_test`) and its print-placement mode
>   (`bazel run //core:update_cast_case_census -- placement`), on channel A's 2,451 cases.

## 6.1 Product semantics: where lite's computation differs from upstream's

The frame's proposal (§3): **in layer 1, the product adopts upstream's computation wherever it is sane, with the sort
as the precedent.** Tested item by item below.
- "Sane" means the same answer on every database, no silent loss, and deterministic.
- "Upstream" means compiled legend-pure, the PCT's reference, unless named otherwise.
- Where legend-pure and legend-engine disagree, both are named.

| # | Item | Upstream (read) | Lite today, or as drafted | Proposal | Why | Register |
|---|---|---|---|---|---|---|
| 1 | **E-R4**: sort, min, max, greatest and least across kinds | a total order: numbers by value, then dates, then booleans, then strings (`CompiledSupport.compareInt`, `PRIMITIVE_CLASS_COMPARISON_ORDER`) | drafted: refused by name | **ADOPT, ruled by the user 2026-11-12** ("can we do the sorting like upstream?") | two sort keys the database computes; upstream's PCT asserts it | no row; strings follow the database's collation (§5.7) |
| 2 | **E-R1**: `[1, 1.0]->removeDuplicates()` | two elements: `eq` is false across classes (`CompiledSupport.eq`) | drafted: one, `1.0` (the hybrid's number promotion) | **ADOPT upstream: keep each number's kind** | sane: the kind is part of the value, which the user already ruled for lite's own runs (S127, 2026-11-04: "two numbers of different kinds are never equal"). The drafted promotion would contradict S127 | drafted row E-R1 withdrawn; the hybrid's rule 1 changes |
| 3 | **E-R2**: `[1, 2.0]->at(0)` | the Integer | drafted: `1.0`, a Float | **ADOPT upstream** | the same reason as 2 | drafted row E-R2 withdrawn |
| 4 | **E-R3**: a StrictDate beside its midnight DateTime | unequal; the StrictDate sorts first (`DateFunctions.compare`: a coarser date before a finer one; `testEqualDateStrictYear`: `%2014 != %2014-01-01`) | drafted: equal, the date promoted to its UTC midnight | **ADOPT upstream** | sane, and simpler: a StrictDate has no zone, so promoting it to a UTC midnight invents one. Carried as its ISO text in the Variant's date tag, upstream's order IS the texts' order (`'2025-02-10' < '2025-02-10T00:00:00'`), and their inequality is the texts' | drafted row E-R3 withdrawn; the hybrid's rule 2 changes |
| 5 | **E-R5**: a date inside a mixed list read back | the StrictDate | the StrictDate | same | — | none |
| 6 | **S123**: a Decimal in a mixed sort | compared exactly by value (`compareUnmatchedNumbers`, BigDecimal) | ranked with the numbers by its value as a DOUBLE | **ADOPT upstream** with item 1 | a DOUBLE can tie two distinct Decimals; the database compares DECIMAL exactly | S123 deleted when item 1 builds |
| 7 | **S127**: `1 == 1.0`, `1D == 1` | false: `eq` compares classes | false: the kind is part of the value | already upstream (compiled); keep | — | the row stays as the record of the user's ruling between upstream's runtimes |
| 8 | **S139**: two Decimals of one value and different scales (`0.3D == 0.30D`) | unequal: `BigDecimal.equals` | equal: compared by value | **KEEP lite's** | upstream's is not sane in a database: a `DECIMAL(10,2)` column holds `0.30` for `0.3D`, so a stored value would never equal its own literal. Scale belongs to the column, not the value | S139 stays |
| 9 | **S134**: `indexOf` | legend-pure: 0-based; legend-engine's lowering: 1-based (its own PCT fails) | 1-based, as the engine; the PCT tests are known failures | **ADOPT legend-pure's 0-based** in the product; the engine's 1-based at the edge | sane: Pure's `at`, `substring` and `indexOf` are 0-based together, and a 1-based `indexOf` breaks `substring($s, $s->indexOf(...))`. The engine's answer is its own defect | S134 becomes an edge-only row; the PCT failures leave the ledger |
| 10 | **S19**: `average([])` | legend-pure raises (`Float[1]` promises a value); legend-engine answers NULL | NULL, as the engine | **ADOPT legend-pure's raise** in the product (a `RaiseText` kind); NULL at the edge | sane: a NULL under `[1]` breaks the multiplicity the typer relied on, a silent wrong answer downstream | S19 becomes an edge-only row |
| 11 | **S140**: a precise primitive's constraints (`Numeric(4, 2)`, `Varchar(10)`) | checked | parsed, not checked (the user, 2026-11-05: "leave the guards off for precision for now") | **ADOPT upstream later**, as the user ruled "for now" | sane: a value outside its declared type is the silent loss the product forbids. It costs one guarded CAST (reason `raise`) per write and cast | S140 stays until built |
| 12 | **S120 / S119**: a Float is an IEEE double; NaN and infinities are values | compiled legend-pure: a Java double, which is the same; the interpreter: a BigDecimal | a double | already upstream (compiled); keep | the interpreter's BigDecimal is the outlier | rows stay, naming the interpreter |
| 13 | **S124**: a TDS's cells in value position | the rows' arrival order (unspecified) | numbered once, deterministically | keep: lite's order is one of upstream's valid answers | determinism (G10) | the row stays as a note, not a divergence |
| 14 | **S22**: `count()` over an empty association | legend-pure: 0; legend-engine: 1 (its outer join's defect) | 0 | already upstream (legend-pure) | — | edge-only row |

**What the proposal does to the hybrid (§5.7 of the lean-SQL design).** Items 2-4 reverse rules 1 and 2 of the ruled
hybrid ("number lists promote", "date lists promote"). They go back to the user. The honest cost is below.

| Choice | Number lists | Date lists |
|---|---|---|
| **Promote (as ruled)** | one typed list: the SQL barely moves (DuckDB 60 -> 62 characters on the stdDev case) | one typed DateTime list, about 4x smaller than today (DuckDB 451 -> 92) |
| **Keep kinds (upstream)** | a literal list's kinds are known at planning, so most reads (`at`, `type`, `toString` of a literal element) cost nothing. A COMPUTED mixed list (`removeDuplicates`, `sort`) is the Variant JSON array, integers and floats told apart per dialect | the Variant with ISO-text date tags: smaller than today's tag-and-slots, larger than the promoted list (not yet measured; it lands with E-2's build) |

E's recommendation:
- **Numbers:** keep kinds, as upstream does. S127 already commits lite to it, and a literal's kind is free at planning.
- **Dates:** keep kinds too, because the text order makes it cheap. The 4x is the promotion's alone, and it is paid for
  with an invented zone.

Both choices are measurable at E-2's build; the user rules.

**The test of the frame's proposal:**
- Of the 14 items, 9 adopt or already are upstream: 1-5, 6, 7, 12 and 14.
- 2 adopt upstream in the product and keep the engine's at the edge: 9 and 10.
- 1 keeps lite's, because upstream's is not sane in a database: 8.
- 1 is deferred by an earlier ruling: 11.
- 1 is a note, not a divergence: 13.
- So the product's semantic divergences fall to **S139** (item 8), plus S140 until it is built.

The proposal holds. Two caveats:
- This table covers the rows E knows that reach PRODUCT computation. D's §5 classifies every register row.
- Items 9 and 10 change answers for current lite users. Each is a register row the user rules, as the tenet requires.

## 6.2 Product print forms: §5.4 checked against E-4's census

The rule (frame §1 and §4): a CAST or CASE whose reason is `print` leaves product SQL, except printing INSIDE a query
(§4.1).

**Measured, channel A, with E-4's typed reasons.** The counts are DuckDB's. Nine of the 11 databases have the same
counts. BigQuery has 209 inside casts. Snowflake spells its own canon: 125 and 0 at the result, 91 and 293 inside.

| | `print` casts | `print` cases | |
|---|---|---|---|
| **at the result** (the top of a root projection, or a chain of `print` nodes from it) | 179 | 12 | presentation: **all leave SQL**, and the writers spell the value (§5.4's forms) |
| **inside the query** | 241 | 353 | counted per chain below; most is still presentation, routed through a carrier |

What holds each inside chain (DuckDB; 470 chains):

| Held by | Chains | What it is | Verdict |
|---|---|---|---|
| `COALESCE` | 179 | an element printed into a list or string carrier (`makeString`, `joinStrings`, a tagged mixed list's text slot) | **leaves.** The hybrid (E-2) carries typed elements, and the result's writer prints them. Where the text is the user's string (`makeString`), it stays: in-query printing |
| `LOWER` | 72 | a Boolean printed `true`/`false` (the database's `TRUE` lowered) | leaves at the result. Stays only when the text is computed with |
| `cast(operator)` | 63 | a printed value cast back for an operator (a number re-read from its text) | **leaves**: a round trip through text, which the typed value replaces |
| `EQUAL` | 46 | printed texts compared: instance equality by canon text (`InstanceEquality` over `CanonicalRenderSql`) | **leaves**: equality is computation, compared on the typed values (the frame's §4 "typed-value comparisons") |
| `RTRIM` | 36 | a Float's trailing zeros trimmed (`FloatPrintText`) | leaves at the result (§5.4's 6-decimal form is the writer's) |
| `CONCAT` | 31 | `'a' + $x->toString()` | **stays**: in-query printing (§4.1) |
| `Lambda` | 26 | an element printed inside a list function's lambda (`map(x \| $x->toString())`) | **stays** when the list's text is the answer's value; leaves when only the result's presentation reads it |
| `STRPOS`, `FORMAT`, `REPLACE` | 17 | the Float print's tests; `format` | `format` stays (the user asked for a string); the Float tests leave with the Float print |

**What remains after the moves:** the `CONCAT`, `FORMAT` and in-lambda chains whose text the query computes with, and
`makeString` and `joinStrings` of the user's own strings. On DuckDB that is **about 60 to 90 chains of 470 (13-19%)**.
The upper end counts all `Lambda` chains and the `makeString` part of `COALESCE`; that split is measured at the build,
not estimated now.

The guard: frame §7's "no presentation in product SQL" becomes E-4's census row `print` with a pin at that remainder,
shrink-only.

**Not counted:** a CAST a dialect writes as text inside a render method (not a node). The SQL-text census (§5.8 of the
lean-SQL design) bounds those.

**The judge's canon is not product SQL.** E-5a staged it, -19.7% of the database judge's characters. E-5c replaces it
with typed-value comparison in the fused verdict statement.

## 6.3 E's rows for D's §5 table

| Item (site) | Today | Layer | Becomes | SQL removed from the product | Register | Owner |
|---|---|---|---|---|---|---|
| E-R1 `[1, 1.0]->removeDuplicates()` (`MixedEncoding`, `TaggedLists`) | two elements, tag-and-slots | 1 | two elements, kinds kept (literal: at planning; computed: Variant) | the tagged list's slots (§4.1 of the lean-SQL design: 3.6% DuckDB to 10.9% Snowflake of channel A's characters, for the 74 mixed queries) | drafted row withdrawn (proposal) | E |
| E-R2 `[1, 2.0]->at(0)` | the Integer | 1 | the Integer, its kind from the typed tree | as E-R1 | drafted row withdrawn (proposal) | E |
| E-R3 StrictDate beside DateTime | two elements, tag-and-slots | 1 | two elements, a Variant with ISO-text date tags; upstream's order is the texts' order | the date slots (to be measured at the build; the promoted form was about 4x smaller) | drafted row withdrawn (proposal) | E |
| E-R4 a cross-kind sort | tag-and-slots order | 1 | kind rank, then value, in the database | — | none (ruled 2026-11-12) | E |
| E-R5 a date in a mixed list | the StrictDate | 1 | the same, carried in the Variant | — | none | E |
| S123 a Decimal in a mixed sort | ranked as a DOUBLE | 1 | compared as DECIMAL | — | deleted with E-R4 | E |
| S134 `indexOf` | 1-based | 1 | 0-based in the product, 1-based at the edge (`PlanStyle.UPSTREAM`) | — | edge-only (proposal) | D (the edge's swap), E (the product's lowering) |
| S19 `average([])` | NULL | 1 | raises in the product, NULL at the edge | — | edge-only (proposal) | D, E |
| `print` at the result (179 casts, 12 cases per database) | printed in SQL | 2 | the writers spell §5.4's forms | about 24% of the product's `print` nodes | — | E |
| `print` inside, as presentation (`COALESCE`-carrier, `LOWER`, `cast(operator)`, `EQUAL`, `RTRIM`) | printed in SQL | 2 | typed carriers and typed comparison | about 400 of 470 chains on DuckDB | — | E (the hybrid, typed equality) |
| `print` inside, as computation (`CONCAT`, `format`, the user's `makeString`) | printed in SQL | 1 (§4.1) | stays: in the product in lite's form, at the edge in upstream's | — | — | — |
| The judge's Float canon (`FloatCanonText` via `LiteralSpelling.leaf`) | 4,124 characters a canon | test road | staged (E-5a, built), then typed comparison (E-5c) | test SQL: -19.7% (E-5a) | — | E |

## 6.4 How it was measured

```
bazel run //core:update_cast_case_census -- placement                 # 6.2's placement and holders (manual; nothing written)
bazel test //core:cast_case_census_test                               # the census, pinned per reason and database
python3 docs/lean-sql-homework/judge_sql.py.txt <channel-B test.log>  # the judge's statements, E-5a's before and after
```
