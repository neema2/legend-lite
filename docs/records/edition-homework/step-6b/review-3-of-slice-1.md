# Review: step 6B, slice 1 (`git diff c81297490 HEAD -- core/src/main`, branch claude/step-6-numbers)

Reviewer: independent, read-only. I ran nothing: no Bazel, no database. READ = seen in the code; INFERRED = reasoned
from the code, not run.

## Verdict: FIX FIRST

There are two blockers. (1) BigQuery answers the two-argument instruction inverted. (2) The new `sideRows` lets a
value drawn afresh in a let be drawn once per side, and the planner's own check used to catch that. Three should-fix
items change answers or SQL on databases other than DuckDB, which goes against the half page's "no database but
DuckDB changes its answers".

## BLOCKER

### B1. BigQuery writes the two-value `SAME_IN_ORDER` as NOT EQUAL (READ)
- `BigQuery.java:1579` routes `SAME_IN_ORDER, SAME_IN_ANY_ORDER` to `nullSafeArm`.
- `nullSafeArm` (`BigQuery.java:1880-1887`) sets `equal = c.fn() == SqlFn.NULL_SAFE_EQUAL`, which is false for
  `SAME_IN_ORDER`. When `a.size() == 2` and a side is composite (an Array), it writes
  `(TO_JSON_STRING(a) <> TO_JSON_STRING(b))`.
- So every two-argument instruction over arrays gives the opposite answer. Two arguments is what you get when a side
  reads the row, when the planner is null, or when a side draws afresh.
- Before this slice the same comparison was a plain `EQUAL`, which `equalArm` wrote as `compositeEquality(..., true)`.
- Trigger (INFERRED: the side reads the row, so two arguments):
  `T.all()->filter(r | [$r.a, $r.b] == [1, 2])` keeps exactly the rows that differ.
- Four-argument calls are unaffected: they fail the size check and go on to `nullSafe` → `sameValues`.
- Fix: in `nullSafeArm`, send `SAME_IN_ORDER`/`SAME_IN_ANY_ORDER` straight to `nullSafe(writer, c)` before the
  composite test. Even better, have `sameValues` take `parentPrec` (see S2) and keep this arm out of the path.

### B2. `sideRows` does not see a draw in a let the side reads (READ, consequence INFERRED)
- `RootComparison.plan` refuses to plan two sides when the comparison, or any let it reads (transitively, seeded
  lets included), draws afresh (`drawsAfresh(equal, lets, seeded)`, lines ~221-243). Its comment says why: "a let
  both sides read would be drawn twice".
- The new `sideRows` (`RootComparison.java` ~206-210) checks only `UserCallInliner.drawsAfresh(side)`, the side's own
  tree. It then lowers `with(lets, side)` as a statement of its own, and does that once for each side.
- Trigger, on any rows-writing database (Postgres, SQLite, H2, MariaDB, ...):
  `|let g = generateGuid(); [$g, 'a'] == [$g, 'a'];` (Strings, so in scope).
  1. `plan` sees the draw and returns null.
  2. The lowering builds `EQUAL`, and `asSameValues` turns it into `SAME_IN_ORDER(..., rows(left), rows(right))`.
  3. Each row sub-query draws its own guid, so the answer is false. Pure answers true.
  - Before this slice: the plain comparison, which reads the let once.
- The same hole exists for `insideAnExpression` callers: an `if` or a lambda whose condition reads such a let.
- Fix: share the transitive check. Make `drawsAfresh(TypedSpec side, lets, seeded)` (the body of the current
  `drawsAfresh` with `side` in place of `equal`) and call it from `sideRows`.

## SHOULD-FIX

### S1. Under `!`, the two-value form loses its null-safe negation on rows databases (READ, the NULL-array case INFERRED)
- `Scalars` `not` (`Scalars.java:200-211`) calls `NullSemantics.negate`. Its switch (`NullSemantics.java:162-174`)
  turns `EQUAL` into `notEqualNullArms`, which is `NULL_SAFE_NOT_EQUAL`, `IS DISTINCT FROM`. `SAME_IN_ORDER` hits
  `default -> null`, so the caller writes a bare `NOT`.
- With two arguments the base dialect then writes `NOT (a = b)` where it wrote `(a IS DISTINCT FROM b)` before.
- The two differ whenever one array value is SQL NULL. On Postgres, Snowflake and Trino, an aggregated list of no
  rows (`array_agg`) is NULL.
- Trigger (INFERRED, assuming a to-many navigation in a filter is lowered as a correlated array value):
  `P.all()->filter(p | !($p.kids.age == [1]))`.
  - Pure: a parent with no kids is kept.
  - Now: `NOT (NULL = ARRAY[1])` is NULL, so that parent is dropped.
- DuckDB is unaffected: `NOT (x IS NOT DISTINCT FROM y)` means the same as `IS DISTINCT FROM`.
- The four-argument form is unaffected: its sub-query is never NULL.
- Fix: add an arm to `NullSemantics.negate`:
  `case SAME_IN_ORDER -> c.args().size() == 2 ? (enumInvolved ? notEqualExpandedArms(c.args()) : notEqualNullArms(c.args())) : null;`
  That is exactly what the `EQUAL` negated to before.

### S2. `sameValues` ignores the parent's precedence: `a = b` is written bare (READ)
- `AnsiSqlRenderer.java:1398-1401` writes `writer.expr(new EQUAL(args), 4)`. It has no `parentPrec`, and `nullSafe`
  has none to pass it.
- An operand of a comparison is written at precedence 5 (`infix`, `nonAssociative`). Before the slice the inner
  `EQUAL` was parenthesized there; now it is not.
- Trigger: `([1,2]->map(x | $x + $r.k) == [2,3]) == true`, or `($r.xs == ['a']) == $flag`, where a side reads the
  row. Postgres renders `a = b = TRUE`, a syntax error, because `=` does not associate in Postgres. H2 likely does
  the same. MySQL and MariaDB happen to parse it.
- Fix: give `sameValues` and `nullSafe` a `parentPrec` parameter and pass it to `writer.expr`. Or write the two-value
  form in parentheses, as DuckDB's `sameLists` does.

### S3. MongoDB: a list comparison it used to send as `$eq` is now refused (READ: the arm; INFERRED: that it was reached)
- `MongoDb.java:1045` throws `DialectCapability` for every `SAME_IN_ORDER`/`SAME_IN_ANY_ORDER` ("MongoDB's pipeline
  judges no assert").
- Before the slice, a list equality in a `$match` or a projection reached Mongo as `EQUAL`.
- Fix: for a two-argument `SAME_IN_ORDER`, write it as the dialect's `EQUAL`.

### S4. At a query's answer, the typed pair machinery is gone for these kinds (READ; differences INFERRED)
`RootComparison.plan` now returns null before `SideEqualities.pair(...)`. What that path did, and what replaces it:
- **The draw check.** It still runs before the stand-aside. Its job is undone by `sideRows`: see B2.
- **`refuseIdentityOnly` / `refuseMixedDays`.** These are Variant and date refusals, not reachable for Integer,
  Decimal or String. Nothing is lost.
- **The maps refusal.** Not reachable for these kinds. Nothing is lost.
- **`kind(...)` reading a Number side's column type.** The stand-aside is keyed on the declared types, so Number,
  Any and abstract-Date sides still take the old path. Nothing is lost.
- **`besideDecimals` and `oneCarrier`.** These are lost.
  - The rows form now pairs an INTEGER `__o` against a DECIMAL `__o` with `IS NOT DISTINCT FROM`. That is right on
    Postgres, H2, MariaDB, ClickHouse, Trino and BigQuery (INFERRED).
  - Where a non-relation side's typed rows come out as `DECIMAL_TEXT`, text meets a number. That is false on SQLite,
    and an error elsewhere. Before, it was converted to one carrier or refused by name.
  - Store-read sides are relation-typed and are excluded by the `Type.isRelation` test, which narrows this a lot
    (READ). I could not find a non-relation side whose rows are `DECIMAL_TEXT`. The guard is gone, though.
  - Fix: run `besideDecimals` and `oneCarrier` in `sideRows`'s pair, or refuse a `DECIMAL_TEXT` side in
    `asSameValues`.
- **HUGEINT.** Left as it is in both paths. Nothing is lost.
- **`answerOnly`.** Not relevant here.

### S5. DuckDB at the root: values that used to be compared as typed rows are now compared as list values (INFERRED)
- Before, DuckDB's root comparison was `SAME_IN_ORDER(listed(valueRows(e)), listed(valueRows(a)), ...)`.
  `valueRows` drops NULL elements ("a NULL value dropped (Pure has no null value)").
- Now DuckDB compares the raw list values with `IS NOT DISTINCT FROM`. Any lowering that leaves a NULL element in a
  list value now answers differently on DuckDB (false) and on the rows databases (true, the NULL dropped). An
  example is an element that may be empty, which `Equalities.everyElementWritten`'s comment says the run decides.
- Probe: `|let o = [1,2]->filter(x | $x > 5)->first(); [$o, 1] == [1];` on DuckDB and on Postgres. Pure says true.
- If it splits, the fix is in the list lowering, not here. But this slice makes the query's answer depend on it for
  the first time.

## NOTE

- **N1. Nullability claim is now false for the two-value form (READ).** `SqlTyping.java:354` marks
  `SAME_IN_ORDER`/`SAME_IN_ANY_ORDER` never-NULL ("a Boolean on every form"). The two-value base form `a = b` (and
  BigQuery's `compositeEquality`) can be NULL. `VerdictSql.typedRow` (`:574-581`) projects such a node as a non-null
  `__h` when `comparesSides` matches, and `comparesSides` now matches product comparisons too. Either make the
  nullability depend on the argument count, or make the two-value form null-safe.
- **N2. Fallback shape (invariant 4).** When rows are missing, `asSameValues` silently drops to two arguments, and the
  base dialect writes the database's own `=`. The half page itself calls that comparison wrong where a list is JSON
  text (it is guarded only by `refuseExactNumbersOfTwoScales`). This behaviour is not new, but it is now an explicit
  silent fallback branch, not a refusal. Name it in the register, or refuse it on JSON-carrier dialects.
- **N3. Unreachable refusal.** `AnsiSqlRenderer.java:1403-1406` refuses fewer than four arguments with a message about
  "in any order". The only path that builds fewer than four is `SAME_IN_ORDER`. The message is wrong for a
  three-argument call. Harmless.
- **N4. Cost on DuckDB, and recursion risk (INFERRED).** `sideRows` runs on every dialect. On DuckDB it lowers each
  side twice more as whole statements, which nobody renders.
  - It now does this for WRITTEN lists too: the old `insideAnExpression` stopped at `!wholeOrTable`.
  - Each side statement carries all the body's lets. If lets are lowered eagerly, a let holding a list `==` re-lowers
    itself inside its own side statement.
  - The old path had the same shape for computed lists only, so it is probably bounded. Still, I would add one test:
    `|let a = [1,2] == [1,2]; let b = [3] == [3]; $a && $b;` on DuckDB and Postgres.
  - A side that lowers as an expression but throws as a statement now fails on DuckDB too.
- **N5. `boundOnce` (READ).**
  - `__se` and `__sa` are built at render time, after CTE extraction, so they never collide at top level.
  - A comparison nested in a side lands inside the outer CTE's body, where the inner WITH shadows the outer one
    legally.
  - Sides are never correlated, because `sideRows` requires the side to read nothing of the row.
  - A WITH inside a scalar sub-query already existed before this slice (`wholeSides` wraps an `answer` that has
    CTEs), so no new dialect risk. I did not confirm H2's support independently.
  - `named()` does not check LIMIT without ORDER. Reading such a side twice was already the old `of` behaviour.
- **N6. MariaDB and MySQL rows form, case and pad collations (INFERRED).** `['A'] == ['a']` in a lambda used to be a
  JSON text compare (binary). Where a side now has rows, it is `<=>` under the session collation. The root path already
  had this. Check the collation lite sets.
- **N7. Architecture (READ).**
  - No SQL is named in the lowering. It reads declared types and SQL type facts only, which is allowed structural
    reading.
  - `sql/dialect/SameValuesForm` imports only `com.legend.sql`.
  - No `default ->` was added.
  - Exhaustive switches are untouched.
  - `planner` uses the new `SideEqualities.typedRows` (public), which is fine under the planner-entry rules as far as
    I can see. I did not run ArchUnit.
- **N8. Coverage of the condition (READ).**
  - `comparedAsValues` reads `info().type()`, the declared type.
  - Exact-number lists declared `Number` (`[1, 2D]`) or `Any` are not covered and keep the old paths. That is
    consistent with `RootComparison`'s `valueType` test, because relations are excluded on both sides.
  - Against an Integer, Decimal or String declaration, `aList` is enough in practice: TaggedLists are answered
    earlier by `taggedEquality`, and JSON-carried lists are Array-typed, so the JSON refusal still applies through
    the `EQUAL` arm.
  - A one-value side boxed by `PureSql.asList` is covered correctly.
  - `emptySidesAsPure` after `asSameValues`: for two list values, `mayHoldNoValue` was already false (`oneValue` is
    false for an Array, and boxing requires `[1]`), so nothing is lost there (READ).

## Every reader of SAME_IN_ORDER / SAME_IN_ANY_ORDER, checked for two arguments

| Reader | Two arguments |
|---|---|
| `DuckDb.java:617` → `sameLists` | Uses args 0 and 1 only: OK |
| `Postgres:807`, `SQLite:1702`, `H2:698`, `Snowflake:1400`, `Databricks:1078`, `Trino:937` → `nullSafe` → `sameValues` | OK, except S1 and S2 |
| `MariaDb:1950`, `ClickHouse:2012` → `sameValues` | OK. MySQL: `MySql.java:124` delegates to `super`, i.e. MariaDB |
| `BigQuery:1579` → `nullSafeArm` | **Inverted (B1)** |
| `MongoDb:1045` | Throws (S3) |
| `SqlTyping:354` | Nullability wrong for two arguments (N1) |
| `SqlTyping:401` | Type: OK |
| `SqlTyping:795` | `nullStrict`: OK, conservative |
| `VerdictSql:461`, `:695` | Builds four arguments; `comparesSides` matches two (N1) |
| `SideEqualities:130/233/327/383/1662/1876/1895` | Build four arguments: OK |

No reader indexes argument 2 or 3 without a size check. `AnsiSqlRenderer.sameValues` now checks the size first.

## Not checked

- I ran nothing, so none of the measured numbers are confirmed.
- Whether lets are lowered lazily (N4).
- Whether to-many navigations lower to NULL-able array values on Postgres (S1's trigger).
- H2's support for a WITH inside a scalar sub-query.
- The `LetBinding.bindRepeats` and `SubselectPrune` passes, over unrendered arguments.
- The edge's `PureText.dialect` subclasses.
- Tests under `core/src/test`.
- ArchUnit rules.
