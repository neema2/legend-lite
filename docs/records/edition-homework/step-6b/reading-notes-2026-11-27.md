# My own reading of the comparison code (for step 6B), at scratch head 601d442e4 -- read myself, line by line

## 1. lowering/Equalities.java (478 lines) -- the VALUE form (`==`/`eq` where it stands in a row)
`equal(n, args, numbers)` is a LADDER, in this order:
 1. a written `[]` on a side: `[]==[]` true; else emptiness of the other (list size, or IS NULL). (126-150)
 2. a tagged value / tagged list on a side -> `taggedEquality` -> TaggedLists.sameElement / sameWrittenLists /
    sameListByKinds / sameList / sameListOfKind / variant-list `=`; else NotImplemented. (151-156, 331-392)
 3. partial dates (year, year-month): same precision -> `=` on the texts; computed Date carried TEMPORAL_TEXT -> `=`;
    else constant FALSE (no read!) for a full-precision other / non-Any other. (157-186)
 4. TEMPORAL_TEXT against TIMESTAMP -> SideEqualities.instantOfText / instantOfHeld (S399). (190-197)
 5. CastPolicy.comparisonWireOperand each side; VariantShapes.alignLiteralToJson; EnumSourceValues.decodeInvert. (198-209)
 6. one value against an array-typed SQL side -> both as lists. (210-218)
 7. nullArms: NULL literal / two optional columns / optional PlanParam -> NULL_SAFE_EQUAL; VariantEquality.between
    refusal; else plain EQUAL. (427-477)
Then OUTSIDE equal: `emptySidesAsPure(n, compared, positive)` rewrites only a plain 2-arg EQUAL (62-76) and
`guardedSides` lets NullSemantics.negate pattern-match the COALESCE form (104-109).
Also here: neverEqualButEvaluated, sameOnlyWhereBothEmpty, neverAmongButEvaluated (text marks), and the DOORS'
`upstreamsPartialDateTime` (written precision).
OBSERVATIONS (mine):
 - Decisions are made by SQL SHAPE (instanceof Tagged, ArrayLit, Column, NullLit, PlanParam; `aList`, `carriedAs`)
   and by TYPED-TREE shape (TypedCollection, TypedCDate, AT_DATE_DATE args size), i.e. by how a side was written
   and carried -- not by one (kind, carrier) table.
 - Step 3 still returns constant FALSE without reading either side for partial vs full date (a refusal lost if a
   side raises) -- unlike neverEqualButEvaluated used elsewhere.
 - Empty-side handling is spread over three places: the `[]` ladder (1), nullArms (7), emptySidesAsPure (after).
 - The year/year-month form here is TEXT `=`; SideEqualities uses numbers for the same kinds (to check below).

## 2. planner/RootComparison.java (420) -- the WHOLE-SIDE entry
`plan(lets, equal, negated, seeded, parameters, lowered, ctx)`:
 - only `==` (AT_BOOLEAN_EQUAL) and hasSameElements; `eq` never.
 - drawsAfresh -> unplanned (plain comparison) -- BECAUSE each side is lowered as its OWN statement with the lets
   copied in front (`lowered.apply(with(lets, left))`, `...right`): a shared let is computed TWICE. This is the
   "runs twice" the user asked about, in the PRODUCT path too, not only in the test record.
 - tables -> `tables(...)`: columnsDiffer => constant (no read); else SideEqualities.tables(leftPlan, wireDecidedKinds..)
 - two KNOWN different types -> unplanned (falls to the value form / door's rule).
 - `!anyOrder && !whole(left) && !whole(right)` -> null (value form). whole = map carrier or multiplicity many,
   and NOT `written` (a literal/collection of literals).
 - refuseUnordered (compile error, OrderedAsserts.unorderedSide).
 - SideEqualities.pair(left, kind, leftPlan, right, kind, rightPlan) -> pair.compared(leftPlan, rightPlan, anyOrder)
   -> null => maps refused by name, else unplanned.
 `over(...)` gives WholeComparisons {asTheStatement, insideAnExpression, everyValueRead}:
 - insideAnExpression: only where FreeVars(comparison) subset of readable names (lets, seeded, parameters minus
   lambda-bound names) => else returns null => the VALUE FORM answers (Equalities / TaggedLists / the database's
   list `=`), except maps: refused.
 => THE SPLIT: "whole sides that read nothing of the row" use SideEqualities; "anything reading the row" uses
    Equalities. Which code answers is decided by WHERE the comparison stands + how the side was written
    (`written`), exactly what AGENTS.md's "Wherever it stands" forbids in meaning.
 - `kind(side, plan)`: Number side's kind from the plan's output column type; Any -> VariantEquality.sideType.

## 3. lowering/SideEqualities.java (2156) -- the WHOLE-SIDE code (was the database judge's VerdictSql typed members)
Shape of every member (values, collections, tagged, instances, grids/tables, contains*, subset, perElement,
instanceTexts): take each side's PLAN (a SqlQuery "wrapped"), turn it into typed ROWS (`valueRows`: __o value, __rn
order), NORMALISE the rows per kind, put each side in CTEs, then ONE node compares the two: SqlFn.SAME_IN_ORDER /
SAME_IN_ANY_ORDER (args: the two listed lists + two row subqueries; the dialect's SameValuesForm picks list `=` or the
portable row form). Returns `Compared(ctes, from, same, held, alone)`; `held` = each side's values for a FAILURE RECORD
(test checker only); `answerOnly()` drops them for a query.
THE PER-VALUE NORMALISERS ARE ALREADY EXPRESSIONS OVER ONE VALUE (they are just embedded in row-building):
 - DateTime: `instantOfHeld(v)` = CAST(v AS TIMESTAMP, JUDGE); `instantOfText(text)` (hour text gets ":00") -- these
   two ARE already shared with Equalities.equal (step 4 of its ladder).
 - written Date text (TEMPORAL_TEXT/VARCHAR typed abstract Date): `writtenKindIs(kind, text)` + `writtenDateValue(kind,
   text)`: Year -> BIGINT, YearMonth -> y*12+m BIGINT, StrictDate -> DATE, DateTime -> TIMESTAMP. (DATE_KINDS)
   !! Equalities compares year/year-month as TEXT `=` (its ladder step 3). Two forms for one kind. On equal
   precision the answers agree unless the texts differ for one value (e.g. sign/padding "+2014" vs "2014"; to check).
 - JSON-carried Any: `jsonKindIs(kind, JSON_TYPE(v))` + `jsonValue(kind, v)` over String/Integer/Float/Boolean only;
   object/array => RAISE JUDGED_TREE_NOT_TYPED. (JSON_KINDS)
 - exact numbers: `stated` (one compare column for Integer+Decimal: `wideEnough(d)`), `fitted(value, own, column)`
   CAST of an Integer to that Decimal; `besideDecimals`: vs DECIMAL_TEXT carrier => both as canonical decimal TEXT
   (LiteralSpelling.decimalText); too-wide => text. PAIR-DEPENDENT: the common type is a function of BOTH sides.
 - tagged (SqlType.Tagged: tag + slots): `kindedRows`: k = index of the kind key, v0..vn one column per kind key
   (kindKey: DateTime of any written digits = one kind; Integer+Decimal = "Exact number").
 - instances: `pairedLeaves(es, as, keys)` -> leaves (path, type), `leafOf` = CAST(field, leaf type, JUDGE);
   `sameInstance(e, es, a, as, keys)` is ALREADY the value-form expression (AND of NULL_SAFE_EQUAL per leaf) and is
   what InstanceEquality uses inside expressions (to confirm).
 - maps: `entryRows` -> entries sorted by key as [{k,v}] (CanonicalRenderSql.sortedEntries).
MY CONCLUSION SO FAR: the whole-side code = (a) per-value normalisers + pair alignment, (b) row framing + one
list/rows node. (a) is the true "element rule" and it already exists here as expressions; Equalities has a SECOND
copy of (a) as a hand ladder (text for partial dates, TaggedLists.sameElement for tagged, wire operands, enum
decodeInvert), and TaggedLists has a THIRD copy for lists inside a row. Unifying = Equalities.equal and
TaggedLists.* call (a); (b) stays the statement form. That is concrete and checkable, not a new table invented.
 - THE DISPATCH is `SideEqualities.pair(eSpec, eKind, ePlan, aSpec, aKind, aPlan, ctx)` -> `Pair(e, a, Form, keys)`,
   Form in {VALUES, DATES, EMPTY_ONLY, TAGGED, INSTANCES, COLLECTIONS, INSTANCE_TEXTS} (lines 1948-2060). Order:
   refuseIdentityOnly (Variant S401; refuseMixedDays S399) -> Variant => EMPTY_ONLY -> mixed (Tagged carrier /
   written Date text / JSON carrier) => TAGGED -> Nil => EMPTY_ONLY -> dateKind both => DATES -> typedClass both =>
   VALUES -> same class carried as VARCHAR => INSTANCE_TEXTS / path VALUES -> Array/Map carrier => COLLECTIONS ->
   same class => INSTANCES(keys) -> else null.
   IT READS THE PLAN ONLY FOR `plan.outputs().get(0).type()` (the carrier) -- so the dispatch is really a function
   of (typed spec, Pure kind, SQL carrier) per side: a VALUE-LEVEL signature. A pair of expressions has exactly
   those three facts (TypedSpec, its type, SqlExpr.type()). => The one dispatch can serve both forms.
 - Members the PRODUCT never calls today (only the test checker does): contains, containsTagged, containsInstances,
   subset, perElement, grids (grid against list), instanceTexts?, and `held`. They are already in lowering/ (product
   code), public. "Move that code into product so both can do it" = route the product's `contains`/`in`/forAll-
   contains over whole sides to them from RootComparison; no code needs to move packages.
 - `values(...)`: bothEmptyOnly when kinds differ (not exactPair) -- "two kinds never equal" is decided HERE by
   `!eKind.equals(aKind)`; in the value form it is decided by the type-checker (compile error) / HeldKinds / the
   doors' NamedRule / TypedRunTimeComparison. Third place: JudgmentFacts.kindsDiffer (test checker). To read.

## 4. lowering/TaggedLists.java (974) -- mixed values/lists INSIDE a row (value form's list layer)
 - `Tagged` = tag + candidates(kind, value); `TaggedList` = positions + channels (one array per kind/slot).
 - sameElement(x, y, numbers): OR over candidate pairs with numbers.canEqual(kindA, kindB) of
   (tagX=i AND tagY=j AND valueA = valueB) -- PLAIN `=` between channel values; NO per-kind normaliser
   (SideEqualities.kindedRows applies writtenDateValue / instant / fitted before comparing).
 - sameList / sameListOfKind: LIST_FILTER + Lambda over positions (needs higher-order list functions);
   sameWrittenLists (no list at all, position by position); sameListByKinds (array by array).
 - contains / containsElement: IN over written elements of the kind, or LIST_FILTER lambda.
 - `NumberFamilies numbers` (TWO_FAMILIES, or a door's three types) is threaded as an ARGUMENT -- "policy as data"
   already exists here for the doors (the thing the user wants replaced by typed calls from the type-checker).
 => a second implementation of "tagged element equality" and of "two lists in order", different SQL shapes.

## 5. Who calls what (grep, verified)
 - SideEqualities.pair: planner/RootComparison (product) AND testplan/JudgmentFacts.typedPair (test checker).
   => the pair comparison of two whole sides IS shared today.
 - Product-only extra: SideEqualities.tables (RootComparison.tables).
 - Test-checker-only (via lowering/VerdictSql): contains, containsTagged, containsInstances, subset, perElement,
   grids (typedGridPair), documentTexts/jsonDocuments, kindedRows/kindsOf (direct), + record plumbing (listed, held).
 - Value form reaches SideEqualities only for: instantOfHeld/instantOfText (Equalities), sameInstance
   (InstanceEquality), typedClass (Scalars).
 - testplan/DatabaseJudge.databaseVerdict (67-208) makes ITS OWN decisions BEFORE the shared pair:
   kind gate (AssertVerdicts.kindKey -> KindClass; non-primitive differing kinds => UNJUDGED), enum frame for an
   untyped wire, TDSNull sentinel rewrite of the expected side, sides planned by BodyDriver.planValue with a
   CanonRider (kinds, literalIndex, tdsWrapped, declined) -- NOT by the planner's `lowered.apply(with(lets, side))`,
   host-constant side bound as VALUES (constantSide), grid-vs-list (gridColumnPair), grid pair
   (VerdictSql.typedGridPair), "no literal channel" / "unrefined-number: multi-candidate side" => UNJUDGED.
   So: same pair code, DIFFERENT side planning and DIFFERENT gatekeeping => an assert and a query can still differ
   in (a) whether the comparison is made at all, (b) what plan each side has (carrier!), since the carrier picks
   the Form. This is the precise sense in which "merged for real" is false today.

## 6. testplan/AssertVerdicts.adjudicate (349-640) -- the test checker's ROUTER (read)
Assert vocabulary (NativeFn.Verdict, a closed switch): ASSERT_EQUALS / NOT_EQUALS, ASSERT_SAME_ELEMENTS,
ASSERT_CONTAINS / NOT_CONTAINS, ASSERT_SIZE / NOT_SIZE, ASSERT_EMPTY / NOT_EMPTY, ASSERT / ASSERT_FALSE (condition),
ASSERT_EQ / NOT_EQ, ASSERT_IS / IS_NOT, ASSERT_INSTANCE_OF, ASSERT_EQ_WITHIN_TOLERANCE, ASSERT_TDS_EQUIVALENT,
ASSERT_JSON_STRINGS_EQUAL, TO_CSV (operand only). Before the switch: lineage-tree lines; quantified forms
(map / forAll over an assert -> unrolled or vector `quantified`); dual golden -> assertEquals; a caller's message
(split, planned as its own row, read only on failure).
Inside ASSERT_EQUALS: renderedArm (one side a DB-rendered grid text) -> grid pair (sqlGridPair -> arm.gridPair) ->
arm.equals(..., gridPair flag). VerdictArm = the judge mode's arm (database / host).
What each assert MEANS as a plain Boolean (the original 6.4/6.5 idea):
  assertEquals(e,a) = e == a | assertSameElements = hasSameElements(e,a) | assertContains(c,v) = c->contains(v)
  assertSize(c,n) = c->size() == n | assertEmpty(x) = x->isEmpty() | assert(c) = c | assertEq = eq | assertIs = is
  assertInstanceOf = instanceOf | tolerance = abs(e-a) <= t  -- tdsEquivalent and jsonStringsEqual have meanings of
  their own (not equality).
=> For "an assert IS the comparison it means" the PRODUCT must plan, as a query's answer: whole-side contains /
   not-contains, size, isEmpty, grid-vs-list, eq/is over whole sides -- today only `==`/hasSameElements of two
   sides are planned whole (RootComparison); the others are lowered as plain expressions (value form).

## READ SO FAR (myself, 2026-11-27 evening): Equalities (all), RootComparison (all), SideEqualities (values,
collections, tagged, instances, sameInstance, contains*, subset, perElement, listed, valueRows, kindsOf, kindedRows,
date/JSON normalisers, instants, exact-number alignment, Pair + pair, date kinds, refusals; NOT grids/tables/
instanceTexts bodies), TaggedLists (600-812: sameElement, sameList, sameListOfKind, contains), DatabaseJudge
(1-260: databaseVerdict, gridColumnPair), AssertVerdicts.adjudicate.
## NOT YET READ: VerdictSql (1560), DatabaseAsserts (717), HostAsserts, DatabaseJudge 260-1088 (size, empty,
contains, condition, tolerance, ARM), Scalars' contains/in/indexOf/removeDuplicates/ordering rules,
InstanceEquality, NullSemantics.negate, Lowerer.asCondition/written/withWholeComparisons, compiler.spec
(EqualityClasses, LiteCrossTypeRule, TypedRunTimeComparison, OrderedAsserts), HeldKinds, the doors' tables
(UpstreamTies, PureTable, EngineTable, EditionTable), sql/dialect/SameValuesForm and the dialects' equalArm.

## 7. lowering/Lowerer.java 425-515 + Scalars.java 2385-2440 -- WHERE THE CODE IS CHOSEN (read)
 - `wholeAnswer(body)`: body's last statement is ==/hasSameElements (or ! of one) and `Scalars.doorsRule(...) == null`
   -> `wholeComparisons.asTheStatement` (planner's statement IS the statement).
 - `wholeSides(n)`: inside an expression, doorsRule == null -> `ScalarSubquery(wholeComparisons.insideAnExpression(n))`.
 - else `written(call, args)` -> `Scalars.lower`: `doorsRule` = `styled.edition().lowersAs(call, features)` -> a
   `NamedRule` -> `NamedRules.lower` (door's rule REPLACES the product's, and so a door never gets the whole-side
   statement nor HeldKinds); else `HeldKinds.refuseAcrossKinds`, then RULES.get(FunctionId) (Equalities.equal).
 - then `styled().edition().emptySideIsFalse(call)` -> `Equalities.emptySidesAsPure(call, e, positiveConditions...)`.
 => THE LOWERING READS THE EDITION (through `Styled.edition()`: lowersAs, emptySideIsFalse) -- tenet 6's exception
    says only the type-checker may ("nothing after the typer can see it"). The user's direction ("doors' comparing
    functions become their own typed calls via the type-checker") removes exactly these two lookups: the door's
    `==` would arrive as a different FunctionId with its own RULES entry.

## 8. Scalars containsRule / inRule (3690-3835) -- the value form's MEMBERSHIP (read)
 Another hand ladder: TDSNull needle -> IS NULL scan; mixed list -> TaggedLists.contains/containsElement; Any list
 -> Membership(TO_VARIANT(needle), list) (JSON against JSON); class-vs-primitive -> constant FALSE (no read);
 kindMismatch -> constant FALSE (no read); else `SqlExpr.Membership(needle, list)` / flat IN = the DATABASE'S OWN
 membership with NO per-kind normaliser. SideEqualities.contains (test checker only) = instants() + join on
 NULL_SAFE_EQUAL. Two membership codes; the product uses only the first.
