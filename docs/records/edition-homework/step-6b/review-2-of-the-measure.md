# Review: the position matrix as step 6B's acceptance test

Reviewed: `core/src/test/java/com/legend/equalitymeasure/EqualityMeasure.java` (positionList, sidesList, positions,
storeSidesList, storePositionList, storePositions, answer, run, outcomeOf), `tools/equality-measure/positions.py`, the
two model files, and the output in `.../scratchpad/positions-s0/`. Spec: AGENTS.md "The equality rule", PLAN §1 and §3,
plus SEMANTICS_REGISTER S139, S399, S430 where AGENTS.md points to them.

## Conclusions first

The expected answers are mostly right. The tool runs and its classes are mostly read correctly. As an acceptance test,
though, it can read zero while the goal is missed, in five ways:

1. **A NULL answer in a store row's kept value is read as `false`.** This hides a real defect today, on all three
   databases (finding R1).
2. **`N` accepts any non-answer**, including `NotImplementedException`, a run-time `DataError`, a timeout and a NULL.
   It also accepts `false` where the rule demands a compile error. A real gap is hidden today (R2, R3).
3. **"Refused everywhere" is not part of the zero** (PLAN §3.1). The places a database "cannot answer" are taken from
   whichever comparison runs first. So the count can go down by refusing more, or by breaking the simplest comparison
   (R4, R5).
4. **It covers `==` and two asserts only.** The rule names `!=`, `in`, `contains`, `indexOf`, the deduplications,
   joins and ordering, and none of them is asked. The `!`-under-an-assert forms, `hasSameElements` /
   `assertSameElements`, and the relation API over a store are not asked either (C1-C6).
5. **Most kinds have only a "true" row**: Decimal, Integer against Decimal, day, date-time, Float, Boolean, pair. An
   implementation that answers `true` for every Decimal or date comparison passes (C7).

## 1. Expected answers

Checked all 72 value rows and all 13 store rows against the rule. Most are right; these are the exceptions.

**E1. `N` is two different expectations, merged.** READ. Affects `int and float`, `day and date-time`,
`integer and float columns` and `day and date-time columns`.
- The rule says: "comparing two different types is a compile error naming the fix", and "Still a compile error: two
  literals (`1 == 1.0`)".
- Inside a function whose parameter is declared `Any`, it says the comparison "is compared when the query runs", and
  "at run time the two are never equal". It also says "a comparison between ... a type variable's values is that
  call's compile error".
- So the expected answer depends on the place:
  - REFUSED-when-planned at every static place and at function-generic;
  - `false` at function-any.
- With `N` meaning "refused or false", a statement place that answers `false` for `1 == 1.0` passes. That is
  legend-pure's door behaviour ("legend-pure's door rewrites a cross-type `==` to `false`") leaking into lite.
- Fix: add an expectation `R`, meaning a planning refusal whose exception is `ModelException` or
  `TypeInferenceException` (today's data has these). Give each `N` row a per-place override: function-any expects `F`
  (or `''` in the store), and every other place expects `R`.

**E2. `pairs int and decimal` (`?`) is determined: T.** READ.
- The rule for classes: "the run-time class is part of the value -- the same class, then the keys of a keyed class".
  For numbers: "Within a family a comparison is by value: `1 == 1D` is true".
- `pair(1, 1D)` and `pair(1, 1)` have the same class, and their elements are equal within a family.
- With `T`, the database judge's `UNJUDGED` becomes a counted gap, not a question for the user.

**E3. `Number int and decimal` and `Number lists int and decimal` (`T`): the rule's own words split.** READ.
- For `T`: the families bullet says an Integer and a Decimal of one value are "one element (a duplicate, a member, the
  same position)".
- Against it: the run-time bullet says "equal means the same type and the same value".
- `T` is the likelier reading. It should be posted to the user once, not decided by the tool.

**E4. `nested lists` (`?`) is answered `T` almost everywhere.** INFERRED.
- If lite treats `List<T>` as a keyless class, the rule ("the identity of a keyless one") gives `F`.
- Keep `?`, but list it with the answers the databases gave, so the user rules on data.

**E5. `year and year-month` (`F`) is right.** READ. S399: "a year, a year-month, a day and a date-time are kinds never
equal across". Not a defect; noted because AGENTS.md alone does not say it.

**E6. Rows that are right and the reason.** READ.
- `date-time hour and minute` and `built to the hour and literal` are `T` (S399: "equal whatever their written
  precision (an hour, a minute ...)").
- `tables a column's name differs` is `F` (S430: "two tables whose columns differ are never equal"; not a compile
  error).
- `instances written alike` is `F`: `t::K` declares no key, so the comparison is by identity.

**Store rows.** READ: I recomputed all 13 from the five rows in the model's CSV. All are correct, apart from the `N`
merge in E1.

## 2. How the report reads an outcome

**R1. A NULL kept value reads as `false` (HIGH; it hides a live defect).** READ: `positions.py:62-63`.
- Only `same == 'true'` counts as true, so `"same":null` silently becomes "not true".
- Data: `computed column value and literal | value` answers
  `{"id":3,"same":null} ... {"id":5,"same":null}` on DuckDB, H2 and SQLite.
- The rule ("everywhere else -- a kept value ... the equality answers for its empty side where it is written") says
  `false`, and AGENTS.md never accepts "a NULL where Pure raises".
- The report calls the comparison "one answer, the expected one" on all three databases.
- Fix: if any `same` is `null`, return `'other answer'`. (The value matrix already reads `null` as `other answer`,
  `positions.py:39-44`.)

**R2. `N` accepts every kind of non-answer (HIGH; it hides a live gap).** READ: `positions.py:108-111`.
- `answers and refusals and expected != 'N'` exempts `N`, and `not answers` with `N` counts as expected.
- Data: `integer and float columns` and `day and date-time columns` at `filter-function-any` are `REFUSED
  NotImplementedException: object-space expression node TypedRunTimeComparison is not substitutable yet`, on all three
  databases.
- The rule wants `false` (no rows) there: the parameter is `Any`, so the comparison runs when the query does. The
  report counts it as right.
- A `DataError` at run, a `TIMEOUT`, a NULL or an NPE would be accepted the same way.
- Fix: with E1, `N` and `R` accept only a typed planning refusal at R places, and `F` (or `''`) at function-any.

**R3. Any exception counts as a refusal.** READ: `outcomeOf`, `EqualityMeasure.java:598-604`; `no_answer`.
- `REFUSED` means any throwable during planning. A `NullPointerException` or `ClassCastException` would read as
  "refused NullPointerException", which is a crash, not a refusal by name.
- Fix: classify only `ModelException`, `TypeInferenceException`, `DialectCapability` and `NotImplementedException`
  with a message as refusals. Anything else is `CRASH`, always a failure.

**R4. "Refused everywhere" is outside the zero.** READ: PLAN §3.1 lists contradictions, partial answers and wrong
answers; `KINDS`.
- A slice that makes DuckDB refuse, say, every map or every list-in-lambda comparison moves it from "partly answered"
  to "refused everywhere", and the pinned count goes down.
- PLAN §1 accepts "a database that cannot do a comparison refuses it by name", but DuckDB is the first-class target.
- Fix: count "refused everywhere" in the pin for DuckDB, or keep a dated allowlist per database of comparisons allowed
  to be refused (H2's 12 today).

**R5. The excluded places come from the data, from one comparison.** READ: `positions.py:95-96`.
- `missing` is the set of places where the first comparison (`int same`, or `two integer columns` for the store) has
  no answer. Those places are then dropped for every comparison on that database, whether the others answered or not
  (`:102`).
- So a regression that breaks `int same` at, for example, `list-filter` on DuckDB removes `list-filter` from all 72
  rows, and the count goes DOWN.
- Today it hides nothing: H2 answered in none of its four excluded places (I checked all of H2's kept-in-map,
  list-exists, list-forAll and reads-element rows: 278 DialectCapability, 7 ModelException, 3 NotImplemented).
- Fix: hard-code the excluded places per database with a date and a reason (H2: the four list-lambda places, PLAN
  §2.7), drop only refusals at them, and fail if one ever answers wrongly.

**R6. A wrong single answer is hidden behind "partly answered".** READ: the order in `classify`.
- When one answer is wrong and other places refused, the kind is "partly answered", never "NOT the expected one". Its
  count is 0 for every database.
- Example: SQLite `list date-times two precisions` is refused in 18 places and answers `F` at reads-element, where `T`
  is expected.
- This is harmless to a zero, but the report understates wrong answers. Fix: count `wrong` as its own flag beside the
  kind.

**R7. An assert run that passes counts a pass, whatever the verdicts said.** READ: `run`, `:575-587`.
- `run` returns `PASS` without checking `verdicts[1]` or whether `verdicts[0]==0`.
- Today every PASS says "1 verdicts" (514 of 514) and failures throw, so nothing is hidden.
- But an assert planned away would read `T`. Fix: return FAIL if `verdicts[1] > 0`, and NO-VERDICT if
  `verdicts[0] != 1`.

**R8. Minor regex points.** READ.
- `'true' in text` also matches a String answer `"true"`.
- A multi-row answer `[{"value":true},{"value":true}]` reads `T`.
- Fix: parse the JSON (`json.loads`) and require exactly `[{"value": <bool>}]`, or `[{"same": <bool>}]` for
  table-column.

### Rows checked by hand

Value matrix, each against the report's line:
- `int differ`, DuckDB: all 19 places `F` (`not` gives `true`, which the negation turns to `F`); both asserts `FAIL`.
  The report correctly omits it.
- `empty and value`, DuckDB: reads-element `{"value":null}` is `other answer`, and the report correctly says "partly
  answered". Really it is a NULL defect; see R8 and E1.
- `year and year-month`, all databases: `F` everywhere answered, H2's four places refused. Correctly omitted.
- `date-time hour and minute`: `T` everywhere answered. Correctly omitted.
- `tables a column's name differs`, DuckDB: `F` in 13 places, `DataError` in 5, `TypeInferenceException` in 1. The
  report's "13 / 5 / 1" is right.
- `instances written alike`, H2: `F` in 11 places, the 4 assert places refused, out of 15 counted (19 minus 4
  excluded). Right.

Store matrix:
- `two integer columns`, all three databases: filter `1,5`; filter-not `{2,3,4}`, whose complement is `1,5`; value
  and value-if `1,5`; value-not's complement `1,5`. Right.
- `column and the empty list`, DuckDB: `3,5` everywhere. Right.
- `decimal column and literal at another scale`, DuckDB: `1,3` everywhere. Right.
- `day column and literal`, DuckDB: `1,2`. Right.
- `computed column value and literal`, DuckDB/H2/SQLite: the value place holds `null` for rows 3 and 5, which the
  report reads as false and calls expected. **Wrong (R1).**
- `integer and float columns` at filter-function-any: `NotImplementedException`, which the report calls expected.
  **Wrong (R2).**

## 3. What the goal covers and the tool does not ask

Ranked by how often real queries hit them. "Cheap" means a row of the existing lists.

**C1. `!=` (very common; cheap).**
```java
new Position("not-equal", true, false, (l, r) -> "{|(" + l + ") != (" + r + ")}"),
new StorePosition("filter-ne", true, (l, r) -> "{|" + all + "->filter(r | (" + l + ") != (" + r + "))->project([r | $r.id], ['id'])}"),
```
The empty-side rule makes `!=` over a row's empty value the classic NULL trap. The store's `filter-not` covers `!(...)`
but not `!=`.

**C2. Literal adaptation against a store column (very common; cheap; absent).**
The rule: "`$x.qty > 0` over a Float is `> 0.0`", and "a written day does not read as a date-time".
```java
new StoreSides("float column and integer literal", "1,4", "$r.f", "1"),
new StoreSides("decimal column and integer literal", "1,3", "$r.d", "1"),
new StoreSides("integer column and inexact literal", "N", "$r.i", "1.5"),
new StoreSides("date-time column and day literal", "N", "$r.at", "%2020-01-01"),
new StoreSides("day column and date-time literal", "N", "$r.day", "%2020-01-01T00:00:00"),
new StoreSides("date-time column and hour literal", "2", "$r.at", "%2020-01-01T10"),
new StoreSides("integer column and decimal literal", "1,2,4", "$r.i", "1D"),
```
The value list has the equivalent too: `"[1.0, 2.0]->max()" == "2"` is `T`.

**C3. Membership: `in`, `contains`, `indexOf` (very common; cheap as sides whose answer is compared with `true` or an
integer, so every place still applies).**
```java
out.add(new Sides("contains int and decimal", "T", "[1, 2]->contains(1D)", "true"));
out.add(new Sides("contains int and float", "N", "[1, 2]->contains(1.0)", "true"));
out.add(new Sides("contains date-time precisions", "T", "[%2020-01-01T10:00:00.000]->contains(%2020-01-01T10:00:00)", "true"));
out.add(new Sides("mixed contains float", "F", "[1, 'a']->contains(1.0)", "true"));
out.add(new Sides("in decimal scales", "T", "1.0D->in([1.00D, 2D])", "true"));
out.add(new Sides("indexOf mixed int decimal", "T", "[2, 1D]->indexOf(1)", "1"));
out.add(new Sides("mixed list day read out", "F", "[%2020-01-02, %2020-01-01T00:00]->at(0)", "%2020-01-02T00:00"));  // S399's named known gap
```
On the store:
```java
new StoreSides("integer column in a list", "1,2,4", "$r.i->in([1, 3])", "true"),
new StoreSides("decimal column in an integer list", "1,3", "$r.d->in([1, 3])", "true"),
```
Rows with no value must come out false here: the empty-side rule.

**C4. The deduplications and ordering (common; cheap the same way).**
```java
out.add(new Sides("dedup int and decimal", "T", "[1, 1D, 2]->removeDuplicates()->size()", "2"));
out.add(new Sides("dedup decimal scales", "T", "[1.0D, 1.00D]->removeDuplicates()->size()", "1"));
out.add(new Sides("dedup date-time precisions", "T", "[%2020-01-01T10, %2020-01-01T10:00:00]->removeDuplicates()->size()", "1"));
out.add(new Sides("dedup int and float", "T", "[1, 1.0]->removeDuplicates()->size()", "2"));
out.add(new Sides("order exact against float", "N", "1 < 1.5", "true"));
out.add(new Sides("order int and decimal", "T", "2 < 2.5D", "true"));
out.add(new Sides("order day and date-time", "N", "%2020-01-01 < %2020-01-01T10:00:00", "true"));
out.add(new Sides("sort tie exact before float", "T", "[1.0, 1]->cast(@Number)->sort()->at(0)->instanceOf(Integer)", "true"));
out.add(new Sides("sort tie keeps written order", "T", "[1D, 1]->cast(@Number)->sort()->at(0)->instanceOf(Decimal)", "true"));
```
On the store, a relation `distinct` over `D` and `groupBy` on `I` are worth one row each.

**C5. The asserts the plan names, and negated asserts (common in tests; cheap).** PLAN §2.4 lists them.
```java
new Position("assertNotEquals", true, true, (l, r) -> "{|assertNotEquals(" + l + ", " + r + ")}"),
new Position("assertFalse", true, true, (l, r) -> "{|assertFalse(" + eq.apply(l, r) + ")}"),
new Position("assert-not", true, true, (l, r) -> "{|assert(!(" + eq.apply(l, r) + "))}"),
```
Plus a small separate list for `hasSameElements` / `assertSameElements` / `assertContains` / `assertSize`, each with
its own expected answer:
```java
"[1, 2]" vs "[2, 1D]"        -> T
"[1, 1, 2]" vs "[1, 2, 2]"   -> F
```
plus two tables in another row order (`T`).

**C6. A comparison over a store in the relation API, a join, and the test checker (common; moderate cost).**
- Every store place is the legacy `.all()->filter->project`. The modern path is not asked:
  ```
  #>{t::DB.R}#->filter(x | $x.I == $x.I2)->select(~ID)
  #>{t::DB.R}#->extend(~same: x | $x.I == $x.I2)->select(~[ID, same])
  ```
- Neither is a join condition:
  ```
  #>{t::DB.R}#->join(#>{t::DB.R}#->select(~[ID, D])->rename(~ID, ~ID2)->rename(~D, ~D2), JoinKind.INNER, {a, b | $a.I == $b.D2})
  ```
- Neither is an assert over a store's value: `assertEquals([1, 5], t::R.all()->filter(r | $r.i == $r.i2)->map(r | $r.id)->sort())`.
- The store matrix reads zero today. As it stands it can only catch regressions; it measures nothing B is meant to fix.

**C7. False and difference rows for kinds that only have `T` (cheap; important).**
```java
out.add(new Sides("decimal differ", "F", "1.0D", "1.01D"));
out.add(new Sides("int and decimal differ", "F", "1", "1.5D"));
out.add(new Sides("float differ", "F", "1.5", "2.5"));
out.add(new Sides("float computed inexact", "F", "0.1 + 0.2", "0.3"));
out.add(new Sides("decimal computed exact", "T", "0.1D + 0.2D", "0.3D"));
out.add(new Sides("boolean differ", "F", "true", "false"));
out.add(new Sides("day differ", "F", "%2020-01-01", "%2020-01-02"));
out.add(new Sides("date-time a millisecond", "F", "%2020-01-01T10:00:00.001", "%2020-01-01T10:00:00"));
out.add(new Sides("big integers", "F", "9007199254740993", "9007199254740992"));   // a double carrier collapses these
out.add(new Sides("big integer and decimal", "T", "9007199254740993", "9007199254740993D"));
out.add(new Sides("decimal past double", "F", "1.00000000000000000001D", "1D"));
out.add(new Sides("string case", "F", "'a'", "'A'"));
out.add(new Sides("string trailing space", "F", "'a'", "'a '"));           // MySQL/MariaDB PAD SPACE
out.add(new Sides("list int and decimal differ", "F", "[1, 2]->filter(x | $x > 0)", "[1D, 3D]"));
out.add(new Sides("pairs differ", "F", "pair(1, 'a')", "pair(1, 'b')"));
out.add(new Sides("enum and string", "N", "t::E.A", "'A'"));
out.add(new Sides("Any int and decimal", "T", "1->cast(@Any)", "1D->cast(@Any)"));
```
Keyed instances need a model change, so they are less cheap:
```
Class t::KK { <<equality.Key>> id: Integer[1]; name: String[0..1]; }
^t::KK(id=1, name='a') == ^t::KK(id=1, name='b')  -> T
```
Use whatever key marker lite uses; INFERRED. Also an unrelated-class case, `^t::K(id=1) == ^t::KK(id=1)`, which is
`N`. Today `instances written alike` is `F`, and a comparison that is always `false` passes it.

**C8. Mapping code, derived properties, constraints, and concretely typed functions (less common; moderate).**
- The rule says these hold too ("a function's, a derived property's, a constraint's and a mapping's Pure").
- A function with concrete parameters, the commonest user shape, is never asked. Only `Any[*]` and `T[*]` are.
- One derived property on `t::R`, `same(){$this.i == $this.i2}:Boolean[1]`, asked at the store places, is cheap.

**C9. Outside the matrix entirely.**
- Postgres (slice 3) is not measured.
- PLAN §2.6's "both ways on DuckDB" (lists against rows) is not checked.
- Goals 2 and 5 (computed once, lean SQL) are not measured here; PLAN §3.4's pins are a separate check.
- The measure says nothing about "nothing computed twice".

## 4. Do the probe bodies put the comparison where they claim?

**Sound** (READ):
- statement, not, if, lets, kept-in-map, list-filter, list-exists, list-forAll;
- function-any and function-generic: the body is in a user function, and the rule's "function" cases are what they
  test;
- table-filter and table-column (a one-row `#TDS`, with a row lambda that does not read the row);
- assertEquals and assert;
- the eight store places.

Weak points:

**P1. reads-element's wrapper is itself an equality.** READ: `:269-270`.
- `(L)->filter(e | $x == 0)` routes the side through an Integer `==`, the very code being rebuilt. If that inner
  comparison breaks, L becomes empty and the outer answer flips. The contradiction is then blamed on the wrong
  comparison.
- The wrapper also turns every single L into `[*]`, and applies a collection `filter` to a map, a pair or a table (a
  relation `filter`, another function).
- Fix: `(L)->drop($x)`, with `x = 0`. It reads the element, keeps every value, and holds no comparison. For a table,
  `->limit(1 - $x)`, or leave tables out of this place.

**P2. The list-lambda places do not read their element.** INFERRED.
- kept-in-map, list-filter, list-exists and list-forAll put a constant comparison in the lambda. A planner may hoist
  it. Today DuckDB's `DataError` shows it is not hoisted, but after B a hoist would make those places pass trivially.
- reads-element is the only place where a side depends on the element, and only the left side does.
- Fix: also add `[1]->filter(x | (L)->drop($x - 1) == (R)->drop($x - 1))->isNotEmpty()`, which reads it on both sides.

**P3. `and`/`or` with a literal operand can be folded away.** INFERRED. `(eq) && true` folds to `eq` under the fold
rule, so the place may not exist after folding. Use an operand that is not a constant but is fixed by the data, such as
`([1]->size() == 1)`, or on the store `$r.id > 0`.

**P4. table-filter and table-column use a one-row table whose row lambda ignores the row.** READ. This is the same
hoisting risk as P2. It is not a row-reading comparison; that is the store matrix's job.

**P5. The asserts.** READ. `assertEquals(L, R)` is not `(L) == (R)`: the sides are not parenthesised, though each is
one argument, so this is fine. PLAN §2.4 equates the two; good.

## 5. Other reasons to distrust a zero

- **The model comes from the workspace, not the build.** INFERRED. `BUILD_WORKSPACE_DIRECTORY` is read through
  `positionModel` and `storePositions`. Edit the txt files and the run measures something else, with no record of
  which model produced the counts. Write the model's digest into the counts tsv.
- **The expectations live in Java and the reading lives in Python.** READ. Nothing checks that every comparison
  appears in the tsv. A comparison that times out or crashes the run before its row is written simply disappears from
  the count. The `only` regex can also drop rows. Fix: the report should require the full set of ids (72 + 13, or
  whatever the lists declare) and fail otherwise.
- **The pin's number.** READ, from PLAN §3.1. The pin is "may only go down"; with R4 and R5 the easiest way to lower
  it is to refuse more, or to break the first comparison. Pin per kind and per database, including "refused
  everywhere", against a dated allowlist.
- **SQLite is measured but not in the zero.** PLAN §3.1 says DuckDB and H2. The counts file mixes all three; the pin
  should name its databases.
