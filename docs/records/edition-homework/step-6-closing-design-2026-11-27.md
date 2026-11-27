# Step 6 closing batch: the design, before any code (2026-11-27)

For the rows marked "design" in `step-6-closing-batch-2026-11-27.md`. To be read by an independent reviewer before
anything is built. Nothing here is built. Every "today" below is the scratch branch `claude/step-6-numbers` at
`42f62cc5c`.

## 0. The rule this design is held to

- The edition (lite, legend-pure, legend-engine) is read by the type-checker. After it, the ONE place that may depend
  on the door is the lowering's lookup of the door's table for a call: `EditionTable.lowersAs(call, features)`, asked
  in `lowering/Scalars.lower` (step 5's ruled design, `edition-plan-to-finish-stage-b-2026-11-25.md` line 45: "the
  lowering's side is one lookup"). It answers "a rule the door named for this call", or nothing, meaning the product's
  own rule. The edition program's closing guards replace even that with the type-checker's rewrite; that is not this
  batch.
- So: **the planner asks nothing about the door, and the lowering asks only that one question.** Anything that is
  the product's own behaviour hangs off "the door named no rule for this call".

## 1. What breaks that rule today (all added in step 6)

| Where | What it does |
|---|---|
| `planner/RootComparison.plan` (and `.inside`) | Takes `numbersOf`, a function the planner builds from a lowerer (`ProductStatements.lower`: `lowerer(...).numbersOf(comparison)`), and returns "not planned" when the answer is not `TWO_FAMILIES`. The planner is inferring "a door admitted this `==`" from which number rule the door's table named. |
| `lowering/Lowerer.numbersOf`, `lowering/NamedRules.numbers` | The accessor that turns the table's answer into that signal. |
| `planner/ProductStatements.lower` | Calls `RootComparison.plan` BEFORE lowering: a second entry for the same comparison the lowering reaches through `Lowerer.withWholeComparisons` when it stands inside an expression. Two paths, kept equal by `RootComparisonTest.aComparisonAnswersTheSameInsideAnExpression`. |
| `lowering/EditionTable.emptySideIsFalse` (default true; `edge/EngineTable` false) | A second kind of question to the door's table, asked by `Lowerer.markKept`. |
| `lowering/HeldKinds.refuseAcrossKinds` | Runs in `Scalars.lower` after the lookup returned nothing. Correct by position; nothing states it. |

Not broken, and unchanged by this batch: `EditionTable.lowersAs` and `.checks` themselves (step 5); the per-door
forms object the Java checker is handed (`testexec/HostForms`, to which step 6 added `exactNumbersByValue`).

## 2. The design

### 2.1 One entry, in the lowering (rows A1, A2, A6)

1. The planner hands the lowering ONE thing, with no condition on the door: how to plan a comparison of two whole
   sides.
   `interface WholeComparisons { @Nullable SqlQuery answer(TypedNativeCall comparison, boolean negated); }` in
   `lowering`, implemented by `planner/RootComparison` (today's `plan` and `inside`, merged, with `numbersOf` gone:
   the product's rule is `TWO_FAMILIES`, a constant).
2. The lowering decides, at the call, by its one lookup:
   - `Lowerer.wholeSides(n)` asks `styled().edition().lowersAs(n, features)` first. A rule the door named: this is
     not the product's comparison, the general arm lowers it by that rule, as today. Nothing named: ask the planner.
   - The same check, once more, for the query's last statement: `Lowerer.lower(List<TypedSpec> body)` begins with
     "is the last statement `==` or `hasSameElements` (or `!` of one) that the product's own rule lowers, and does the
     planner answer it?" If so the planner's statement IS the statement (not a wrapper around it), exactly today's
     root SQL. This replaces the pre-pass in `ProductStatements.lower`.
   - `hasSameElements` is the product's own function: no door's table has a row for it, so the lookup answers
     nothing for it at every door, with no special case.
3. `HeldKinds.refuseAcrossKinds` is documented as what it is: part of the product's own rule for a comparison,
   reached only when the lookup named no rule. No code change beyond the comment and a test that a door's comparison
   over two column types is not refused.
4. Deleted: `Lowerer.numbersOf`, `NamedRules.numbers`, the `numbersOf` parameters, the pre-pass in
   `ProductStatements.lower`. `BodyPlanner` (the test body's road) gets the same root behaviour for free: one path.

Guard: an ArchitectureTest rule that no class of `com.legend.planner` calls `EditionTable`'s methods or
`Styled.edition()` for a comparison (the three existing reads, `env.edition().stampsTestBodies()` at the inliner, are
the stamp's and are listed by name), and that `Lowerer` has no public method returning a fact of the door's table.

**Risk to check by probe before building:** a body whose `let` binds a table and whose last statement compares it.
Today the pre-pass never lowers the lets as values; the new entry must keep that (it runs before
`Lowerer.withQueryLets`).

### 2.2 The empty side, with no question to the door (row A3, needs the user's ruling)

- `EditionTable.emptySideIsFalse` and `EngineTable`'s override are deleted. `Lowerer.markKept` marks every equality
  a kept value is made of; `Equalities.falseWhereASideIsEmpty` applies to the lowered comparison whichever rule
  lowered it (it reads only "is this a plain `=` of two operands, exactly one of which may be empty").
- Effect: at legend-engine's door a comparison shown as a column, one side empty, answers false where it answered no
  value. Pure's answer. legend-engine's own answer there is to be RUN first (asked of the upstream worker); if it is
  the database's NULL, this is a new row of the differences page, the user's to rule before it is built.
- If the user rules against it: the guard is applied only where the lookup named no rule (lite's own code), and
  legend-pure's door keeps the gap it had.

**Upstream's own answers, RUN 2026-11-27** (a worker session; legend-engine 4.145.0 on its H2 2.2.224, each
statement's column read over JDBC; interpreted and compiled legend-pure 5.99.0; a table `P (ID, NICK, N)` with rows
`(1, 'a', 1)` and `(2, NULL, NULL)`):

| Written | legend-engine inside its store query (SQL it wrote; the NULL row) | Store-less: interpreted legend-pure, compiled legend-pure, legend-engine's own Java |
|---|---|---|
| `$r.NICK == 'a'`, `'a' == $r.NICK`, `$r.N == $r.ID` as a column | `NICK = 'a'`: **NULL** (no value) | an empty side against a value: **false** on all three |
| `!($r.NICK == 'a')`, `$r.NICK != 'a'` as a column | `NICK is distinct from 'a'`: **true** (its `not(equal)` is written null-safe, `dbExtension.pure:1133-1161`) | **true** on all three |
| `($r.NICK == 'a') \|\| ($r.ID == 7)` as a column | `NICK = 'a' or ID = 7`: **NULL** | false |
| the same as a FILTER | `==` drops the NULL row; `!(==)` and `!=` keep it | -- |
| `[] == []` | -- | true on all three |

So Pure's answer is two-valued everywhere, and legend-engine's store query is three-valued for `==` alone: its `==`
and its `!(==)` are not each other's negation there. Under 2.2 lite answers Pure's way at every door; the recorded
difference at legend-engine's door is exactly the first and third rows (a column of `==`, or of `==` under `||`,
with one side empty: false where legend-engine's database answers no value). Under `!`, as `!=`, and in every
filter, the two agree.

### 2.3 The order of a side (row A4)

Read again before designing: the miss that started this row was NOT the order rule. `OrderView` answered INCIDENTAL
correctly on the resolved tree (`TypedProject > TypedTableReference`); the class query mapped to a list was read as a
TABLE by `RootComparison` and never reached the check (fixed by `valueType`). So the row is narrowed:

- The judgement stays where the comparison is planned, with the function the assert uses
  (`OrderedAsserts.unorderedSide`).
- The hole that remains is `OrderView`'s default: a native call it does not know is DEFINED. After the store is
  resolved, the lite-internal natives that read a store (`Pure.INTERNAL_DESUGAR`: `route`, `legacyNavigate`,
  `groupByOverInstances`, `inlineView`, `recurse`, `stage`, ...) fall into that default. Each is classified in
  `OrderView` (reads a store: INCIDENTAL; passes its source through: its source's), and a test walks every member of
  `INTERNAL_DESUGAR` and fails for one with no classification.
- A matrix test: for each way a side can be made from a store (class `all()`, `all()->map`, `project`, a navigation,
  a graph fetch root, `executeInDb`, a relation accessor, a milestoned read), unsorted is refused under `==` and
  sorted is compared, as a query's answer and inside an `if`.

### 2.4 `hasSameElements` in the type rule (row A5)

- `EqualityClasses.COMPARISONS` gains `hasSameElements` as an EQUALITY (operator text `hasSameElements`), so the
  type-checker asks the door's comparison setting about it as it does for `==`: lite's own setting refuses two known
  types when the query is compiled, and a model's compile check reports it.
- `RootComparison`'s own check (`EqualityClasses.neverEqual`, thrown at planning) is deleted.
- To verify on reading the code: what `edge/PureComparisons` and `edge/EngineComparisons` return for an EQUALITY
  whose callee is not `equal` (they were written for `==`). If either builds a call of `equal`, the row is split:
  a new kind, answered by each door as "the function as written".

### 2.5 Sides that read the row they stand in (row B1)

Today such a comparison is the plain `=` of two list values (the planner declines it). Proposed, never a wrong
answer:

| Sides | Outcome |
|---|---|
| a map, or a table, on either side | refused by name at the lowering (a map's entries have no order a database's own comparison keeps; a table is no value) |
| typed lists of one known kind, on a database that holds lists | the database's own list comparison, as today: it is the rule (in order, each element by value) |
| the same on a database that carries a list as JSON text | Integer, String, Boolean, enumeration elements: text equality is the rule, as today. Decimal, Float, date elements: refused by the DIALECT (the text is not the value: `1.0` against `1.00`), by the words `SameValuesForm` already uses |
| lists typed only when the query runs (mixed) | the tagged-list comparison, as today |

`hasSameElements` over such sides stays refused by name.

### 2.6 Two types that may both be empty (row B3)

`LiteCrossTypeRule` returns the plain comparison when neither side is known to hold a value. Pure: equal only where
both are empty. Proposed: the marker answered as `COALESCE(('l' || text(l)) = ('r' || text(r)), l IS NULL AND r IS
NULL)`: false when both hold a value (two types), true when both are empty, false when one is; both sides evaluated.
Single values only; many-valued sides are the planned form's already.

## 3. Order of the build

1. 2.1 (the entry), with the probe for table-valued lets first. Run: `RootComparisonTest`, `NumberFamiliesTest`,
   `DatesTypedByTheirColumnsTest`, `EmptySideEqualityTest`, the edge tests, the guard.
2. 2.4, 2.3 (type rule, order classification), their tests.
3. 2.2 after the user's ruling and legend-engine's answer.
4. 2.5, 2.6.
5. The remaining "build" rows of the batch list (B4 probe, B5, B8, B11's run).
6. Regenerate once. Then the one verification.

## 4. What the reviewer is asked

- Does 2.1 leave ANY place after the type-checker, other than `EditionTable.lowersAs`/`checks`, that depends on the
  door? Name it.
- Is "the door named no rule" a sound stand-in for "the product's own comparison" for every call 2.1 routes by it
  (`equal`, `eq`, `hasSameElements`, the orderings)? Which door rows exist for those calls today
  (`edge/UpstreamTies`, `edge/EngineTable`, `edge/PureTable`), and does any call reach the lowering at a door WITHOUT
  a row (so it would silently take lite's behaviour)?
- 2.2: any position where a comparison's value is kept that `markKept` does not reach, or reaches wrongly.
- 2.3: store-reading shapes missing from the matrix.
- 2.5: is refusing at the dialect by the elements' type fact reliable (are the facts present on both operands)?
- Anything in the batch list that should be "build" and is marked "record", or the reverse.

## 5. After the independent review (2026-11-27, read-only, of `4d98b6709`; nothing built)

The reviewer was given this note and the rules, not the author's conclusions. Its verdict: 2.1's direction is sound
(the planner implementing an interface of the lowering; `numbersOf` deleted; the column-type refusal behind the
lookup; every compatibility door has a row for `==`, the orderings, `contains`, `in`, `removeDuplicates`, `sort`,
`max`/`min`). Four things would have shipped a wrong answer or a bad design as written. Each section below REPLACES
the matching part above.

### 5.1 Section 0, restated honestly (the user's to rule)

- What this batch achieves: **no decision about a comparison is made by door anywhere but the lowering's lookup of
  the door's table.**
- What is still there after it, all older than step 6 (steps 2 and 3 of the edition program) and removed only by the
  program's closing guards (E7): the planner builds the door's table and reads its policy for stamping a test body's
  code (`planner/PlanEnv.edition()` and `editionTable()`; `ProductStatements`' written flags and stamps; `BodyPlanner`;
  `FramePlanner` -- at least nine read sites); the lowering reads the door's forms object (`Styled.forms()`) and its
  text facets.
- So the user's sentence "We shouldn't land with the planner knowing editions", read literally, is NOT met by step 6
  alone. Put to the user as it is.
- The guard, reworded so it can be checked: every read of `Edition` or `EditionTable` from `com.legend.planner` is
  listed by class and method; a new one fails. `Lowerer` has no public method returning a fact of the door's table.

### 5.2 Amendments to 2.1

1. **One helper for the lookup** (`Lowerer.doorsRule(call)`), called by `Scalars.lower`, `wholeSides` and the root
   check, so "one lookup" is one piece of code, asked in three places.
2. **Written down where it lives** (`EditionTable.lowersAs`, `NamedRule.NUMBERS_OF_THREE_TYPES`): a row REPLACES the
   call's own rule, and the product's own rule for `==` includes the whole-side plan and the column-type refusal.
   Without that sentence the lowering would be inferring "no whole-side plan" from a number row -- A1's side signal
   one layer down. A test: every compatibility table returns a row for every function `HeldKinds.READ` lists, and for
   `equal` and `eq`.
3. **The merged function's readable names** are the statement's lets, the lets the environment seeds (this settles
   B8), AND the query's parameters; a name a lambda binds inside the comparison is not readable. Today the root path
   has no such check and the inside path has, so `{names: String[*] | $names == ['a','b']}` is planned at the root and
   left to the database inside an `if`. Probe both before building.
4. **The test road.** A judged side `$a == $b` becomes the planned statement itself (a top-level `WITH`), where today
   it is a scalar wrapping a subquery that then goes through let binding and pruning. Probe the database checker's
   reading of that shape, and a `WITH` nested as a derived table on H2, MySQL and MariaDB. The entry returns before
   the shared lowering and before pruning; say so in the code.
5. The number-rule parameters threaded through `SideEqualities` only ever receive the product's rule: deleted.
6. Lowerers made with no planner (`Compiler.lowerResolved`, `edge/EnginePlans`, the seedable-lets probe) take the
   database's plain list comparison silently today. Refused by name instead (invariant 4).

### 5.3 The empty side (replaces 2.2)

- **The fallback written in 2.2 is struck.** "Guard only where the lookup named no rule" keys the empty-side meaning
  on whether a number row exists (A1's side signal again) and takes the fix away from legend-pure's door, which has
  it today.
- **Faithful to both doors** (recommended to the user): the question stays on the door's table, the same kind as the
  existing `checks`. `EngineTable.emptySideIsFalse` answers `comparison.outsideStore()` (false outside a database
  query, as legend-engine's own Java was measured to answer; the database's no-value inside one). Cost: that line
  and two test cases.
- **The rule itself has holes, the same under either ruling.** Today the lowering marks the places a value is KEPT,
  from two call sites (the query's answer, a computed column) and through `&&`, `||`, `!` and an `if`'s branches. Not
  reached: an aggregate's input (`count` of the comparison drops the row Pure counts as false); a window column; a
  list lambda's result (`->map(x | $x.nick == 'a')`: the element can be lost); a comparison used as an operand
  (`($p.nick == 'a') == false`); one inside `toString`, a collection, a pair; a query-level let's value; the
  never-equal marker's wrapper.
- **Proposed: turn the mark around.** Kept values are an open list, and a miss is a wrong answer. Conditions are a
  closed list: a filter's predicate, a join's condition, an `if`'s or CASE's condition, an assert's, and `&&`, `||`,
  `!` beneath those. The lowering marks the equalities standing as CONDITIONS and guards every other. A miss then
  costs a few characters of SQL, which the size pins catch, never a wrong answer. Version 1 (the guard inside every
  comparison, conditions included) broke five targets; those five are the probe for this form.
- **Which sides.** Exactly one side that may be empty: as today. Both sides `[0..1]` and not both plain columns
  (`$p.nick == $p.alias->toUpper()`: today a bare `=`, no value where Pure says false, or true when both are empty):
  the null-safe equality, each side written once, which IS Pure's answer for all three cases. To settle on reading
  each dialect's render arm: whether one form (null-safe) serves both cases. A `[*]` side that is one value per row
  (a to-many navigation): probe.
- At legend-engine's door inside a database query its own rule is read first in the pinned source
  (`pureToSQLQuery.pure`, `nullSafeEqualsOperation`: both lower bounds 0 is null-safe there), never recalled.

### 5.4 The order of a side (replaces 2.3; REVISED 2026-11-27 after the user's questions)

The user: "We don't know list of sorted stuff?  Weren't we checking this for compatibility already?", then "shouldn't
it match what assertEqual means for same thing? ... maybe okay if sometimes says \"use hasSameElements\" when it
doesn't know?"

Read again in the code (`compiler/spec/OrderView.java`): the rule already names the sorts, the store reads, the
order-destroying steps (a grouping, a join, a table's concatenate, a pivot) and about forty order-keeping functions,
and it is ONE rule for `==` and for `assertEquals` on lite's door: both ask `OrderedAsserts.unorderedSide`. It stays
one rule. The first version of this section (classify every node and native, no default) is withdrawn as far larger
than the hole.

The hole is the fallback alone: a function or node the rule does not name is answered DEFINED without reading what
it was applied to, so store rows passed through `removeDuplicates`, `reverse`, `tail`, an `if` are called ordered.
Harmless enough for a test (a strict assert over rows whose order varies fails loudly, and the list was grown that
way); not for a query's answer, which is returned quietly.

**The fix: where the rule does not know, it does not say "ordered".**

- An unnamed function or node with one value: DEFINED (one value has no order to compare).
- An unnamed function or node with many values: as ordered as the LEAST ordered of its many-valued inputs, the rule
  a list's `concatenate` already has. Over store rows: INCIDENTAL, so `==` and lite's `assertEquals` refuse it,
  naming the two fixes (sort both sides, or `hasSameElements`), and say that lite cannot tell the order is fixed.
  Over a written or a sorted list: as today.
- Three corrections found on the way: a query's PARAMETER is DEFINED (its order is the caller's; today a variable
  no `let` binds is taken for an execution frame's rows and refused); a to-many property read after a sort is not
  SORTED by that sort; `sort()` then `distinct()` is RUN first, and `distinct` leaves the order-keeping list if its
  SQL does not keep the order.
- A function found to be refused wrongly is added to the named lists, with its test: the list grows by evidence, as
  it always has.

What moves, to be listed for the user from the corpus and PCT runs: on lite's door, an `assertEquals` or `==` over
store rows passed through an unnamed function becomes a refusal; at the two compatibility doors the same assert
becomes the any-order check (weaker, never a new failure). Nothing else.

Tests: the matrix of 2.3 (each way a side is made from a store, unsorted refused and sorted compared, as a query's
answer, inside an `if`, and under `assertEquals`), plus `reverse`, `removeDuplicates`, `tail`, an `if` of two lists,
a parameter, a to-many read after a sort, `sort()->distinct()`.

### 5.5 `hasSameElements` in the type rule (replaces 2.4; the reviewer's blocker)

Routing it through each door's comparison setting would make lite's own function mean different things by door
(`hasSameElements([1,2],[1D,2D])` true at lite's door, false at legend-pure's), against 2.1 and register S430. It is
lite's function with lite's meaning at EVERY door: the type-checker applies lite's rule to it whatever the door's
setting, and the planner's fallback words name the two types.

### 5.6 Per-row comparisons (amends 2.5)

Probe each database before building: how a JSON-text database spells a Boolean and a whole number in a list;
BigQuery (no `=` over arrays); Snowflake (arrays of variants). The refusal sits in `sql/dialect` beside
`SameValuesForm`, covers equal, not-equal, both null-safe forms and membership over lists, and an operand with no
type fact is refused, not passed.

### 5.7 Two types that may both be empty (amends 2.6)

Each side written once (a CASE over the two emptiness tests), never the doubled text form; never a cast to text of an
`Any` side that may hold an instance.

### 5.8 What the review adds to the batch list

Rows A8, A9 and B16 to B20 of `step-6-closing-batch-2026-11-27.md`; B2, B9 and B10 change standing.

### 5.9 What upstream's answers of 2026-11-27 add (`upstream-step6/answers-request-3-2026-11-27.md`, all RUN)

- **A date built to the hour or the minute against a date-time** is FALSE on all three upstream runtimes (the
  precision is part of the value). Lite's own rule is register S399 (by the instant). Step 6's shared equality code
  now reads such a text by its instant at every door: what lite's two compatibility doors answer for
  `date(2015, 4, 16, 14) == %2015-04-16T14:00:00` is to be RUN in the build; if it is true there, the doors keep
  upstream's answer (a row of the doors' tables, or the door's rewrite), or it is a new recorded difference the
  user rules. Batch row B19.
- **An empty computed list against `[]`, and one value against a list of one**, answer at upstream as lite answers
  since step 6: the doors moved TOWARD upstream there.
- **Inside legend-engine's store query:** two plain nullable columns under `==` are null-safe (two empties equal);
  with a computed side `==` is a plain `=` (no value for an empty side) and its negation is null-safe. Lite's
  legend-engine door inside a store query must answer exactly that: three cases in the door's tests (5.3).
- **A tie of an Integer and a Decimal** in `max`, `min`, `greatest`, `least`: register S428's pattern (a two-argument
  call keeps its first, a list's fold its last, `least` its first). Row B9's register row is written from it.
