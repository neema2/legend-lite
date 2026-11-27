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
