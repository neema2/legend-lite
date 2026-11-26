# Re-check of the step 6.1 batch's fixes

Scope: `git diff 52f6b5aa 50bf872b -- core/src/main core/src/test` (e6d96694, cd093721, 57c67db0, 50bf872b), the guard
edit 609d8933 (`SqlFnSortedTest`, IN gains `lowering.TaggedLists`), and adfc89bd5 (`inFold`'s class test becomes
`spelledAlike`). The final state read is adfc89bd5 for `LiteralUnroll.java` and 609d8933 for the other files, which
adfc89bd5 does not touch. Read-only: nothing was built or run. Paths are under `core/src/main/java/com/legend/` unless
stated otherwise.

**Verdict: 0 BLOCKERS. SHOULD-FIX: 3 open (N1, N2, N3), plus 5 earlier items deferred with a record (S2, S3, S4, S5,
S6).**

## Earlier findings

| # | Status | Evidence |
|---|---|---|
| B1 String needle matched a date's text key | **FIXED** | `compiler/spec/LiteralUnroll.java:476`: an element matches only when `spelledAlike(value, e)`. The four kinds are records, so the switch at l.454-461 gives the same answer as the `getClass()` test it replaced (adfc89bd5 changes no answer). Test: `exec/TaggedListTest.java:167`. |
| B2 recursion unroll lost its `size() == 0` stop | **FIXED** | `LiteralUnroll.java:446-449`: `comparedHere` admits `TypedCInteger` again, and `equalityFold` folds Integer `==` (l.423-426). The ruling "Whole numbers fine" is recorded in AGENTS.md (e1588334c). `ExactIntegerFoldTest` asserts the folds again. There is still no core test of a literal recursion; the landing page says five corpus tests cover it. |
| B3 a one-value IN over a parameter read as a LIST parameter | **FIXED** | `lowering/TaggedLists.java:606-607`: one candidate is written `=`. Every dialect's list-parameter arm is `a.size() == 2 && a.get(1) instanceof PlanParam` (Ansi l.1369, BigQuery 1893, MariaDb 2151, SQLite 1928, Snowflake 1639), so an IN of three or more arguments is never misread. The test (`TaggedListTest.java:156`) only plans the parameter case (it is on the variant lane) and never runs it. |
| S1 bare `=` chain answered NULL | **FIXED IN PART** (see N1) | `TaggedLists.java:106-108`: `NOT (x IS DISTINCT FROM y)` unless both sides are `neverNull`. That check trusts the TypeFact, so a column the fact wrongly calls non-null still gets the bare `=`. Computed elements are covered (test l.245-246). |
| S2 SQLite compares date-time literals by their text | **NOT FIXED** (deferred) | Covered by slice 6.5 in `docs/records/database-recheck-2026-11-26/START_HERE.md`. The landing page's table does not name it. DuckDB and SQLite still disagree on `%…T10:00:00 == %…T10:00:00.000` and on the written-list IN. |
| S3 order of two date-times at one instant | **RECORDED, pending the user's ruling** | The S399 row was extended by cd093721 ("put to the user"). A row the user has not ruled is still a defect under AGENTS.md. The point about `TaggedLists.sort` tie stability was not addressed. |
| S4 `get` and `groupBy` folds compare Decimal and date by Java equals | **NOT FIXED** (deferred to step 10, recorded) | `LiteralUnroll.java:606` still compares `ps.value().equals(k.value())` for any kind. `newMap([pair(1.0D,'a')])->get(1.00D)` still folds to `[]`, which breaks the fold rule (rule 1) today. The comment at l.91-92 now says so honestly. The Integer ordering and NormalizeFolds parts are resolved by the ruling. |
| S5 the crossing-number answer depends on the list's shape | **NOT FIXED** (deferred to slice 6.2, recorded) | The B2 fix makes this spread wider; see N2. |
| S6 lists not written out use the lambda `sameList` | **NOT FIXED** (deferred to slice 6.4, recorded; not measured) | |
| S7 ClickHouse IN truncating a Decimal | **CLOSED by the author's probe** (I cannot verify it) | Landing page: "probed on the lane at three scales, correct". |
| Notes: NumberLiterals bound | **correct, as before** | `compiler/spec/NumberLiterals.java:149-153`. A 38-digit whole number gives (38,0) and passes; 39 digits is refused; `1.0e40` (unscaled 10, scale -39) gives whole 41 and is refused; `1e3` gives whole 4, scale 0, read as 1000 at (38,0); `0.` followed by 38 digits gives (38,38) and passes; `1.` followed by 38 digits gives 39 and is refused. Still missing: a test that the 38-digit case passes. Open as before: a 38-digit Integer literal against a Decimal of scale > 0 needs 38+s digits in the database. |

## Hand traces

Each input was traced by hand through the code at its final state.

- `'a'->in(['a', 1])` → folded true.
- `1->in(['1', 1.0])` and `1->in([1.0, 'a'])` → folded false.
- `true->in(['true', 1])` → folded false.
- An enumeration value against `['<its name>', 1]` → false. `spelledAlike` fails, and `neverEqual` would also exclude it.
- `E1.a->in([E2.a])` → false: `neverEqual` (the classes differ).
- `%2014-01-01->in(['2014-01-01', %2014-01-01])` → not folded. In lowering the StrictDate family holds one element, so it is written `DATE … = DATE …` and answers true.
- `%2014-01-01->in([%2014-01-01T00:00:00,'a'])` → no channel of the family → FALSE. StrictDate and DateTime are separate families: `MixedEncoding.family` merges only `DateTime(n)` into `DateTime`.
- `2->in([2D,'a'])` → folded false. This is right under the equality rule (two types), but see N2.
- `[1,'a'] == [1,'a']` → `1 = 1 AND 'a' = 'a'`.
- `[1.5D,'a'] == [1.50D,'a']` → `1.5 = 1.50 AND …`, true (S139).
- A nullable column as the needle → `COALESCE(col = x, FALSE)` → false.
- Two family elements → `IN`; one → `=`; none → `FALSE`.
- **No path folds two values of different classes to true.**
  - `equalityFold` and the `contains` fold require equal `cls`.
  - `inFold` requires `spelledAlike`.
  - `neverEqual` only ever answers false.
  - `get` requires equal `cls`.
- **No path still compares a Float, a Decimal or a date in Java for `==`, `in` or `contains`.** It remains only in `get` and `groupBy` (S4).

## New findings

### N1 (SHOULD-FIX). `neverNull` trusts the TypeFact, not "a literal", so S1 survives for columns
`TaggedLists.java:120-123`. The javadoc says "a literal's". The code accepts any `TypeFact.Typed` with
`nullable == false`.

A column's fact is "today the pure-multiplicity echo" (`sql/SqlExpr.java` `Column.of(table, OutputCol)`). So a `[1]`
property mapped to a NULLABLE column is claimed non-null. `MixedEncoding.taggedList` admits a column element by its
static kind, and `literalElements` keeps it, so `sameWrittenLists` is reached.

- Input: `filter(p | !([$p.name, 1] == ['x', 1]))`, with `name:String[1]` mapped to a nullable column, over a row
  whose name is NULL.
- Answer: `name = 'x' AND 1 = 1` is NULL, `NOT` keeps it NULL, and the row is dropped. That is the exact S1 input.
- Join pads are weakened (`asNullable`), except where the WHERE is judged null-rejecting (§E3-S). I did not trace
  whether `NOT(a = b AND …)` is judged null-rejecting.
- Fix: decide by the node, not the fact. Use the plain `=` only for literal nodes (`IntLit`, `StringLit`,
  `DecimalLit`, …, or a `Cast` of one) and use the null-safe form otherwise.
- Fix: also use `SqlFn.NULL_SAFE_EQUAL` rather than `NOT(IS_DISTINCT_FROM)`. That saves a `NOT (…)` per position on
  every dialect: MariaDB currently writes `NOT (NOT (a <=> b))` and ClickHouse `NOT NOT coalesce(…)`.

### N2 (SHOULD-FIX, consistency). The Integer `in` fold now answers what the lowering refuses or answers otherwise
Restoring Integer to `comparedHere` turns on `inFold` for whole numbers (l.471-477). `inFold` treats an element of
another kind as no match. The `contains` fold (l.527-536) instead gives up when any element is of another kind and
leaves the question to lowering.

- **Mixed list:** `2->in([2D, 'a'])` folds to **false**. `[2D, 'a']->contains(2)` and `$intCol->in([2D, 'a'])` are
  **refused** (`TaggedLists.refuseCrossingNumbers`, Integer × Decimal(·,0)). At 52f6b5aa all three were refused.
- **Homogeneous number list, reachable through an inlined generic body (`f(x:Number[1]) {$x->in([0.0, 1.0])}`):**
  - `f(1)` folds **false**. That is the equality rule's answer for run-time types.
  - The mirror `1.0->in([0, 1])` goes to the database as `1.0 IN (0, 1)` and answers **true**. `inFold`'s javadoc
    endorses that via NumberLiterals, but NumberLiterals does not reach two literals or a `Number` context.
  - At 52f6b5aa both went to the database (true and true). Now the same pair answers by which side is the Integer.
- Fix: make the `contains` fold match `inFold` (an element of another kind is no match). Then close S5 in slice 6.2 so
  lowering answers false too. Record or fix the database's coercion of a Float needle against an Integer list in a
  `Number` context (register S127's gap). It needs a register row if it stands.

### N3 (SHOULD-FIX, PLAUSIBLE). `sameWrittenLists` answers a constant `false` without evaluating the elements
`TaggedLists.java:90-92` and l.110-111. A length mismatch, or any position whose families differ, returns
`BoolLit(false)` and drops every element's SQL, including a computed element that would raise.

- Input: `[$p.name->toOne(), 1] == ['x', 'y']` over a row with no name. Pure raises; lite answers false.
- The `sameListByKinds` this replaces evaluated every array, so the raise reached the database. AGENTS.md: never
  accepted for cost is "a NULL where Pure raises", and a constant false is the same loss.
- I did not confirm that a raising `[1]` element reaches here (it depends on the Typer and the carrier).
- The same constant `FALSE` in `contains` when no channel is of the family (l.582-588) is older than the batch.
- Fix: return the constant `false` only where every element is a literal. Otherwise return null, so the caller falls
  back to the array compare, which evaluates every element.

### Nit
`LiteralUnroll.java:460`: `spelledAlike`'s `default -> false` is unreachable behind `comparedHere`. Under "a missing
case throws", it should throw `IllegalStateException`. It does not change any answer today.

## What I could not determine without running
- Whether every dialect's `IN` and `=` agree over a family of mixed SQL types: DECIMAL(2,1) with DECIMAL(3,2), or
  TIMESTAMP literals at two written precisions on SQLite (S2).
- Whether §E3-S's null-rejection keeps the weakening for a join-padded column used under `NOT(… = …)` (N1).
- Whether a raising `[1]` element reaches `sameWrittenLists` (N3).
- Whether `f(x:Number[1]) {$x->in([0.0, 1.0])}` compiles and inlines as traced (N2). The earlier steps-4/5 audit
  reached the mirror shape.
- That the full run is green with 609d8933's guard entry and adfc89bd5 (`CodeShapeGuardrailTest`). The commit messages
  say so; I did not run them.
