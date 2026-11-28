# Step 6.0: the equality measurement (measured 2026-11-26)

*Measured by a separate session (measure only) at `9c751659`, for step 6 of
`docs/records/edition-plan-to-finish-stage-b-2026-11-25.md` ("the rest of the one equality": asserts planned as plain
comparisons, one equality, both judges agreeing). This is the session's report, set out. Its harness
(`tools/equality-measure/`, the manual target `//core:equality_measure`) is on that session's own branch; a second
run on the literal rule's commit follows.*

## What was run

- **Bodies:** 1,943 collected, 1,697 run: core's tests 456 (610 lambda literals holding asserts, in 39 test files),
  PCT's legend-pure tests 469, PCT's legend-engine tests 772 (1,357 `<<PCT.test>>` functions cloned at the pinned
  tags, each `$f->eval(|e)` replaced by `e`).
- **Left out:** `assertTdsEquivalent` 79, `assertError` 58, literals joined to code 40, `assertInstanceOf` 17,
  `assertIs` 17, `assertJsonStringsEqual` 11, `assertIsNot` 6, `eval` with parameters 6, no model 14.
- **Two forms of each body:** as written, and each assert rewritten to its plain comparison (`assertEquals(a, b)` to
  `assert(a == b)`, and so on). 135 bodies are already plain. The rewrite works on the text; about 30 pairs were read
  by hand and no difference left was a parse error.
- **Each form** under the host judge and the database judge, on DuckDB, H2 and SQLite: 20,616 rows, about 4 minutes.

## 4a. Bodies whose two forms differ: 206 of 1,697 on at least one database

DuckDB 116, H2 167, SQLite 134 (core 92/132/88, PCT 24/35/46).

| Group | DuckDB | H2 | SQLite | Example |
|---|---|---|---|---|
| a compile refusal by the typing rule, rewritten only | 34 | 33 | 34 | `forAll(i \| assertEquals(1D, $i))`; `assertEq(1, <Decimal>)` answers, `eq(1, <Decimal>)` is a compile error |
| dates and written precision, pass as written, fail rewritten | 22 | 19 | 34 | `%2020-01-01T10:00:00.5 == ...00.500` |
| H2's missing list encodings | 0 | 64 | 0 | `assertEquals([], [1,2]->filter(...))` meets LIST_FILTER |
| H2's mixed-list syntax error | 0 | 24 | 0 | `[1,'a',true,2.5] == ...` |
| SQLite's list and map forms | 0 | 0 | 21 | unnest in a select list; malformed JSON |
| DuckDB's list shapes | 16 | 0 | 0 | a scalar against a list: `assertSameElements([true,false],[true])` |
| empties, pass to fail | 7 | 0 | 8 | `[] == [1,2]->filter(...)` |
| `==` over an empty side answers nothing | 3 | 1 | 4 | "assert condition must be one value, got 0" |
| the database judge's unjudged forms, as written only | 6 | 2 | 6 | the kind gate; membership of a store Decimal |
| grids, fail to pass | 4 | 3 | 4 | the product's `==` ignores column names and row order |
| a body not planned | 4 | 1 | 4 | an element's message inside `forAll` |
| the mixed-days refusal | 3 | 1 | 3 | `assertContains([%2020-01-01T00:00, %2020-01-02], ...)` |
| number types | 1 | 3 | 3 | `[1.0D,10.125D] == [1.000D,10.125D]` is false |
| hour-precision date-time literals unreadable (new) | 4 | 4 | 0 | `%2015-04-16T14 == date(2015,4,16,14)` |

## 4b. The two judges against each other

| | DuckDB | H2 | SQLite |
|---|---|---|---|
| any outcome differs, as written | 12 | 23 | 27 |
| any outcome differs, rewritten | 5 | 1 | 5 |
| **PASS against FAIL** | **0** | **0** | **0** |

Every disagreement is the database judge declining (unjudged 10/8/8 as written, 0 rewritten), refusing a form (lists,
a map's `eq`) or erroring. **No body gets a pass from one judge and a fail from the other.**

## 4c. "compares X with Y" compile refusals

- As written 24 distinct bodies; rewritten 57.
- **Answered as written, a compile error once rewritten: 34**: Integer and Decimal 12, Integer and Float 5, Integer
  and Variant 6, an enumeration and a String 3, Float and Decimal 2, Boolean and Integer 2, a day and a date-time 1.
  (How many of these the literal rule of 2026-11-26 reads is the second run's question.)

## 4d. Censuses

- An assert inside a helper function: core 8 of 610 bodies; PCT 0 of 1,357 (26 PCT tests hold no assert at all).
- A comparison with a side typed only at run time (Any, Number, the abstract Date): core 32 Any, 22 Date, 13 Number of
  399 bodies with a typed comparison; PCT 38 Date, 31 Number, 7 Any of 1,090. No type variable appeared.

## 4e. The product against itself (DuckDB / H2 / SQLite)

- With both sides' types known, these are compile errors on all three now: `[1,2]->contains(1.0)`, `1.0->in([1,2])`,
  `['A','B']->indexOf(t::E.A)`, `[1,2] == [1.0,2.0]`, `1 == 1.0`, a date against its midnight.
- **With one side cast to `Any`, the old disagreements remain:**
  - `contains(1.0)` true / true / true, but `in` false / false / false;
  - `indexOf(E.A)` 0 / an error / 0, but `contains` false everywhere;
  - `[1,2] == [1.0,2.0]` true / false / false;
  - `1 == 1.0` true everywhere;
  - a date against its midnight true / true / false.
- `[] == [1,2]->filter(x|$x>5)`: false / an error / false.
- `pair(1, 1D) == pair(1, 1)`: true everywhere. `1.0D == 1.00D`: true everywhere.
- `...00.000 == ...00` (date-times): false everywhere.

## What this says for step 6

1. The judges never contradict each other on a verdict; the database judge's gaps are forms it declines or cannot
   write, which shrink from 62 to 11 when asserts are plain comparisons.
2. The run-time-typed comparisons (a side typed `Any`) are where the product still disagrees with itself across
   databases and across `contains` and `in`: that is the run-time type tag's work, the core of step 6.
3. Plain comparisons expose 34 bodies that compare two types on purpose through an assert; each needs the literal
   rule, a conversion, or a ruling.
4. H2 (88 bodies) and SQLite (21) fail rewritten forms for want of list encodings, a gap of those dialects, not of
   the equality.

**Not measured:** bodies unmeasured in both forms (140 refused and 34 errors as written on DuckDB under the host
judge: whole-Variant equality 34, a missing runtime 20, PCT models lite cannot parse); `assertIs` (left out).
