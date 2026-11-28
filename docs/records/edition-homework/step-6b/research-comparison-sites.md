> **Research for step 6B, 2026-11-27.** Read from the code at `3bee40e22` by a read-only research pass; nothing was
> run, and what it could not trace it says so. Its sizes are estimates from where methods start. Input to step 6B's
> design (the map of every comparison code, and the candidates for deletion with what each still does that the
> product's `==` does not): not a ruling and not a design.

# Step 6B homework: every place in core's main sources that decides how two values compare (read, not run)

Worktree `/home/user/ll-final`, detached at `3bee40e22`. All paths are under `core/src/main/java/com/legend/` unless
written otherwise. Line numbers are at `3bee40e22`. The architecture audit (review 4) was written at `8b84e6a62`, and
some of its line numbers have moved since then. One rename has landed since that audit: its
`SameValuesForm.refuseTextOnlyLists` is now `SameValuesForm.refuseExactNumbersOfTwoScales`. Nothing here was run.
"Not verified" marks anything I did not trace to the end.

Rule letters used throughout:
- **(a)** number families: Integer with Decimal by value, Float apart.
- **(b)** a Decimal by its number, not its scale.
- **(c)** dates: a date-time by its instant; a day is no date-time; partial dates by precision.
- **(d)** an empty side: false, and two empty sides are equal.
- **(e)** different types are never equal.
- **(f)** order counts for `==` and not for `hasSameElements`.
- **(g)** a class instance by its run-time class and its keys or identity.
- **(h)** enumerations.
- **(i)** maps by their entries, in any order.
- **(j)** Variant and JSON values.

---

## 1. The sites

### 1A. Product, value form: an expression compares two values where it stands

| # | Site (file:line) | Shapes | Operations | Callers | Rules it encodes itself |
|---|---|---|---|---|---|
| V1 | `lowering/Equalities.equal(n,args,numbers)` :114-220 (the 2-arg overload :41 uses TWO_FAMILIES) | value/value, value/list (boxes the value :208-216), tagged value or list (delegates :148), partial date | `==`, `eq` | `Scalars` RULES :105-108 (lite); `Scalars.threeNumberTypesAndWrittenDatePrecision` :3875 (doors); reached from `Scalars.lower` :2409, from `Lowerer.written` :437 | (d): the literal `[]` ladder :126-144, `[]==[]` true, otherwise IS NULL or `Scalars.emptiness`. (c): partial precision :152-182 (same precision `=`; a computed TEMPORAL_TEXT compared as text :166-169; a full-precision date false :173; a non-date false :179). (c): a TEMPORAL_TEXT against a TIMESTAMP by instant via `SideEqualities.instantOfText/instantOfHeld` :187-193. (e): partly (:179). (h): via `EnumSourceValues.decodeInvert` :202. (j): via `VariantShapes.alignLiteralToJson` :201 and `VariantEquality.between` in `nullArms` :457 |
| V2 | `Equalities.nullArms` :434-477 | value/value | `==`, `eq` | V1 :219 | (d): NULL-literal operand null-safe :449; two optional columns null-safe :459-465; an optional PlanParam null-safe unless an enum is involved :472. (j): `VariantEquality.between` refuses Variant identity :457 |
| V3 | `Equalities.emptySidesAsPure` :62-76, `mayHoldNoValue` :83-89, `oneValue` :94-99, `guardedSides` :104-109 | one value per side | `==` | `Lowerer.written` :442-444; `NullSemantics.negate` :158; `Lowerer.whereLambda` :5144 | (d): both may be empty gives NULL_SAFE_EQUAL (not for enums, :70); one side may be empty and not a positive condition gives `COALESCE(l=r,false)` :75. Multiplicity is read from the SQL type fact (:94-99) |
| V4 | `Equalities.taggedEquality` :327-385, `sameTaggedLists` :391-401, `variantList` :417-425 | tagged/tagged, tagged/static kind, tagged list/tagged list, tagged list/list of one kind, tagged list/variant list | `==`, `eq` | V1 :148 | (a)/(e) through `NumberFamilies`; (j): falls to the database's `=` on the variant carrier :374-379; refuses a non-static kind against a non-variant list :381 |
| V5 | `Equalities.neverEqualButEvaluated` :229-235, `sameOnlyWhereBothEmpty` :244-247, `noneOrSome` :250, `isNone` :256, `neverAmongButEvaluated` :263-273 | value/value, list folds | `==`/`eq`/`in`/`contains`/`indexOf` answered "never equal" by types | `Lowerer.runTimeComparison` :3820, :3833-3835; `SideEqualities.sameOnlyWhereBothEmpty` :174; `Equalities.upstreamsPartialDateTime` :306 | (e) and (d): writes `'l'||CAST(x AS VARCHAR) = 'r'||...` under COALESCE, or `'nn'` text over CASEs. These are SQL decompositions chosen in the lowering |
| V6 | `Equalities.upstreamsPartialDateTime` :291-307, `partsWritten` :310 | date built to hour or minute / TIMESTAMP | `==`, `eq` (doors only) | `Scalars.threeNumberTypesAndWrittenDatePrecision` :3874 | (c), upstream's variant: written precision is part of the value; equal only to a literal of the same precision, by text |
| V7 | `lowering/TaggedLists`: `sameWrittenLists` :87-125, `sameListByKinds` :160-182, `sameElement(list,p,q)` :618-643, `sameElement(tagged,kind,value)` :649-666, `sameList` :669-682, `sameListOfKind` :686-703, `sameElement(tagged,tagged)` :719-736, `contains` :747-797, `containsElement` :801-810, `oneChannelPerFamily` :63-76, `dedup` :611 | tagged value or list | `==`, `contains`, `in`, `removeDuplicates` | V4; `Scalars.containsRule` :3736-3739; `inRule` :3803-3806; `removeDuplicatesRule` :3598 | (a) via `NumberFamilies.canEqual`; (b) and (c) left to the database's `=` per channel, with each family's channels compared together (Decimals at two scales, date-times at two digit counts); (e): a kind with no channel is false or a member of nothing; refuses a kind known only at run time :651-654, :749-751 |
| V8 | `TaggedLists.sort(tl,alias,Ties)` :319-360, `Ties` :316, `numberTypeOrderAt` :438, `refuseInexactRanks` :487 | tagged list | `sort`, ties; ends of `max`/`min`/`greatest`/`least` | `Scalars` sort rule :1045-1047; `sortTiesAsWritten` :3895; `leastOf`/`greatestOf`/`greatestElement`/`leastElement` :3989-4117 | Ordering by value, then by type place (exact numbers before a Float) under BY_TYPE; AS_WRITTEN and LAST_WRITTEN_FIRST for the doors |
| V9 | `lowering/MixedEncoding`: `family` :443, `exactNumber` :448, `numberTypePlace` :102, `channelsOf` :456, `kindOf` :468, `kindOfValue` :518, `kindMismatch` :876-883, `TiePick` :96, `rankedComparableOf` :339, `comparables` :635 | kind vocabulary for tagged values | every tagged comparison; `in` | V4, V7, V8, `HeldKinds` :118-123, `Scalars.inRule` :3777, `Scalars.lower` :2453 | (c): a date-time at any written digits is the DateTime family :443. (a): exactNumber. (e)/(g): `kindMismatch`, primitive against a class is never equal |
| V10 | `lowering/NumberFamilies` :20-30 (`TWO_FAMILIES`, `THREE_TYPES`, `canEqual`) | kinds | all tagged comparisons, HeldKinds | V7, V9, HeldKinds :120, Scalars rules | (a) for the product; three types for the doors |
| V11 | `lowering/HeldKinds.refuseAcrossKinds` :89-144, `readsAs` :170-183, `READ` :62-78 | a value typed only by its column | `==`, `eq`, `in`, `contains(Any[*],Any)`, `<`, `<=`, `>`, `>=` | `Scalars.lower` :2421 (product only: a door's row returns at :2415 before it) | (a)/(e): refuses an exact-number column against a Float column; (c): a day against a date-time column. It has its own literal rule `readsAs` (a whole Float only), separate from `NumberLiterals`. It calls `SideEqualities.refuseMixedDays` :104-107. It reads "no source position" as "not the query's" :90 |
| V12 | `lowering/NullSemantics`: `negate` :151-175, `notEqualNullArms` :68-85, `notEqualExpandedArms` :122-143 (enums), `optionalOperandGuards` :36-55, `verbatim` :94-98, `enumInvolved` :104-115 | value/value | `!=` (`not(==)`), `not(in)`, orderings (guards) | `Scalars` NOT rule :200-212; `Lowerer.whereLambda` :5144; ordering rule :128; `string contains` :3725; `Lowerer.written` :440 | (d) for the negation (null-safe not-equal; NOT IN gets `OR IS NULL`); (h): enum comparisons keep the expanded OR arms and never null-safe :160-165 |
| V13 | `lowering/Scalars` ordering rule :115-131 (+ `dateArg` :2945-2958) | value/value | `<`, `<=`, `>`, `>=` (and the lite shims) | `Scalars.lower`; `MixedEncoding.comparables` for a Tagged operand :2453-2455 | (c): a partial-date literal is padded to its first day; (d): optional-operand guards |
| V14 | `Scalars.containsRule(numbers)` :3643-3763 | list/value, tagged list, Any list, relation | `contains` (collection, and String contains) | RULES :2001; doors :3878 | (j): Variant DISJOINT false :3657-3662; TDSNull membership :3706-3718; tagged via V7 :3733-3740; (j): an Any list is variant-wrapped (`TO_VARIANT`) and compared JSON to JSON :3746-3750; (g)/(e): class against primitive false :3754; otherwise the database's `Membership` under COALESCE :3759 |
| V15 | `Scalars.inRule(numbers)` :3767-3844, `inOfOptionalRule` :3847-3850 | value/list | `in` | RULES :2326-2327; doors :3860, :3881 | (j): Variant DISJOINT :3771; (e): `kindMismatch` :3777; (d): `in(x,[])` false, COALESCE; wire-operand unwrap (CastPolicy); tagged via V7; Any list via TO_VARIANT; otherwise the database's IN or Membership |
| V16 | `Scalars.removeDuplicatesRule(numbers)` :3576-3639 | list, tagged list | `removeDuplicates` (and `distinct` through InstanceEquality for objects) | RULES :1468; doors :3884 | (j): refuses Variants `VariantEquality.refuseAmong` :3587; tagged via V7 :3598; otherwise `ListEncodings.orderedDedup` :3611 (the database's list position equality) or `Dedup.keptDedup` with a comparator |
| V17 | `Scalars` indexOf rule :1323-1355 | list/value | `indexOf` (collection) | RULES | (j): Variant DISJOINT gives -1 :1336-1339; otherwise the database's `LIST_POSITION`. No family rule here; tagged lists via `TaggedLists.at` (:1404) are positional only, not compared |
| V18 | `Scalars` sort rule :1028-1095; `Comparators.select` (lowering/Comparators.java :36, 124 lines) | list | `sort`, comparator `max`/`min` | RULES | Tagged via V8; otherwise the database's LIST_SORT. A comparator must be a bare compare |
| V19 | `Scalars.leastOf`/`greatestOf`/`greatestElement`/`leastElement` :3989-4117, `refuseRealTies` :4130-4140+, `extremeKeepingTheFoldsTie` :3903, `extremeOfTwoInTheDatabasesType` :3924-3952 | list, args, tagged | `max`, `min`, `greatest`, `least` | RULES; NamedRules :~50-52 | Tie picks (TiePick, Ties); refuses date or enum ties of two kinds; `extremeOfTwo...` calls `SideEqualities.typedClass` :3928 |
| V20 | `Scalars.threeNumberTypesAndWrittenDatePrecision` :3856-3887, `sortTiesAsWritten` :3891, `doorsRule` :2402-2404, `lower` :2409-2457 | dispatch | `==`, `eq`, `in`, `contains`, `removeDuplicates`, orderings, `sort`, extremes | `NamedRules.lower` (via the `THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION` arm) | The doors' (a) (three types) and (c) (written precision); the orderings at a door skip HeldKinds |
| V21 | `lowering/InstanceEquality`: `claims` :62-93, `lower` :96-125, `equality` :127-187, `contains` :189-233, `removeDuplicates` :267-288, `isDistinct` :291-306, `indexOf` :309-335, `collectionsEqual` :339-362, `staticallyDisjoint` :424, `disjointAnswer` :458 | instance/instance, instance list | `==`, `equal`, `eq`, `contains`, `in`, `indexOf`, `removeDuplicates`, `distinct`, `isDistinct` | `Lowerer` arm :4596-4608 (only when `instanceKeysOf != null`); `staticallyDisjoint` arm :4399-4401 | (g): eq is identity `__id`; == is a class mismatch false (identity lane only) :151-158, then typed leaves (`SideEqualities.sameInstance` :172), else VARCHAR canon text (`CanonicalRenderSql.instanceEqualityCanon`) :177-186. It returns null for an "unclaimable" shape (:46-48, :145, :164), and the Lowerer then throws (:4601-4606). Membership, indexOf and dedup over canon TEXT |
| V22 | `lowering/CanonicalRenderSql.instanceEqualityCanon`, `trackedId`, `emptyArray` (file 1,119 lines; also `wrapWithCanon`, `wrapTdsCanon` for the judge) | instance as canonical text | (g) | V21; `SideEqualities` :sortedEntries, trackedId, valueAtKey, wrapTdsCanon; `Scalars` trackedId; `CanonRider` | (b): the Decimal scale is normalised in the canon (carriers doc §"Not carriers"). (c): written digits are kept for tagged slots, `CanonicalRenderSql.java:1043-1046` (the carriers doc says this conflicts with S399; not verified whether product equality reaches it) |
| V23 | `lowering/EnumSourceValues.decodeInvert` :28-94 (114 lines) | DECODE chain against a string or enum literal | `==` | V1 :202 | (h): an enum literal is inverted to the source condition; a plain String compares in raw space |
| V24 | `lowering/VariantShapes.alignLiteralToJson` :31-41 | JSON against a literal | `==` | V1 :201 | (j): the literal is wrapped TO_VARIANT, and the database's JSON `=` decides |
| V25 | `lowering/CastPolicy.comparisonWireOperand` :273-289 | wire cast against a literal | `==`, `in` | V1 :195-198; `inRule` :3798 | Unwraps a mapping wire cast when the literal is in the source type (raw-space compare) |
| V26 | `Lowerer.runTimeComparison` :3810-3843, `none` :3849-3863, `readWhereItStands` :3867, `everyValueRead` :3873-3876, `computes` :3880-3897; arm :3913-3919 | marked comparison (`TypedRunTimeComparison`, `TypedCrossTypeComparison`) | `==`, `eq`, `in`, `contains`, `indexOf` answered "never equal" | `scalarInner` :3913-3919 | (e) and (d): both may be empty gives `sameOnlyWhereBothEmpty`; otherwise V5. A computed list is read by the planner's `everyValueRead` statement. `computes` duplicates `LiteCrossTypeRule.harmless` |
| V27 | `Lowerer.asCondition` :365-400, `countPositive` :404, `countPlaces` :415, `predicate` :426, `written` :437-445, `nativeArgs` :454-461 | position marks | `==` (positive condition or not) | filter, join, exists/find, if, window, reducer sites (:427, :457, :2457, :2541, :3413, :3683, :4346, :4414, :4421) | (d): decides whether V3 guards the equality. Asks `EditionTable.emptySideIsFalse` :443. `verbatimEquality`/`legacyNullUnsafeEquals` keep a plain `=` :439-440 |
| V28 | Lowerer arms in `scalarValueTailArms`: hasSameElements :4579-4585; `==` with `wholeSides` :4590-4591; InstanceEquality :4596-4608; generic `written` :4619 | dispatch | `==`, `hasSameElements` | `scalar` | Order of the switch decides which equality runs: CrossType marker, then RunTime marker (:3913-3919), then `staticallyDisjoint` (:4399), then hasSameElements, then whole sides, then instances, then the scalar rule |
| V29 | `Lowerer.whereLambda` :5141-5146 | negated exists or forAll predicate | `forAll` over `==` | relation exists/forAll | (d): a guarded equality negates to null-safe not-equal |
| V30 | `lowering/Comparators` (124) | comparator lambdas | `sort`, `max`/`min` by comparator | Scalars | The direction from a bare compare; no equality rule |

### 1B. Product, whole-side form: two sides each planned as a statement

| # | Site | Shapes | Operations | Callers | Rules |
|---|---|---|---|---|---|
| W1 | `planner/RootComparison.plan` :59-124 | list/list, map/map, table/table, mixed | `==`, `hasSameElements` | `over(...).asTheStatement` :166 and `.insideAnExpression` :171-197 | (e): two known types differ gives "unplanned" (null for `==`, a refusal for hasSameElements) :88-100, with an exact-number pair allowed :92. (f): `refuseUnordered` :349-359 (via `OrderedAsserts.unorderedSide`). Draws afresh :72. (i): two maps with no typed comparison refused :113-121. Two single values return null (the plain comparison) :101-104 |
| W2 | `RootComparison.over` :147-205 (`insideAnExpression` :171, `everyValueRead` :200), `asking` :253-258 | dispatcher | the same | `Lowerer.withWholeComparisons` :472 from `ProductStatements.lowerer` :460 (the product road) and `BodyPlanner.lowererFor` :177 (the test road) | Inside an expression a side that reads the row or the element (free variables not "readable") gives null, so the value form applies (:179-195); a map read from the row is refused :187-193 |
| W3 | `RootComparison.tables` :310-337, `valueType` :267, `table` :274, `whole` :371-385, `written` :387, `kind` :404-413, `known` :362 | table/table | `==`, `hasSameElements` | W1 :85-87 | (f): ordered or multiset. Columns that differ give a constant :317-320. Kinds per column via `VerdictQueries.wireDecidedKinds` (compiler/spec :728). A Number side's kind is read from the plan's SQL type :404-413 |
| W4 | `lowering/SideEqualities.pair` :1982-2054, `Pair` :1948-1976 (`compared` :1967-1975, `kindsDiffer` :1959) | chooses the form: VALUES, DATES, EMPTY_ONLY, TAGGED, INSTANCES, COLLECTIONS, INSTANCE_TEXTS | `==`, `hasSameElements`, `assertEquals`, `assertSameElements`, `assertEq`, `assertIs` (enum) | W1 :110; `testplan/JudgmentFacts.typedPair` :93-98 (database judge) | (j): `refuseIdentityOnly` :1986, a Variant against another type is EMPTY_ONLY :1992-1999. Tagged, written-date or JSON sides go TAGGED :2000-2008. Nil gives EMPTY_ONLY :2009-2017. Dates go DATES via `dateKind` :2105-2121. Instance texts :2031-2036; a packageable element by its path :2037-2043; collections :2044-2047; INSTANCES with keys :2050-2053; otherwise null |
| W5 | `SideEqualities.values` :93-158 (+ `sameOnlyWhereBothEmpty` :171-179, `everyValueRead` :185-204, `besideDecimals` :1196-1234, `oneCarrier` :1145, `instants` :1072-1114, `instantOfHeld` :1115, `instantOfText` :1122, `noRows` :1134) | list/list (ordered or multiset) | VALUES, DATES, EMPTY_ONLY | W4 | (a): an exact pair read as one type :104-113. (b): `besideDecimals` canonical text or widened DECIMAL :1196-1234. (c): instants via `readOnce`/`instants`. (e)/(d): two kinds lite never calls equal give "the same only where both are empty" :106, :128-131. (f): SAME_IN_ORDER or SAME_IN_ANY_ORDER :130. A DECIMAL_TEXT carrier mismatch returns null :115 |
| W6 | `SideEqualities.collections` :214-247 (+ `entryRows` :256, `scalar` :250) | list of lists, maps | COLLECTIONS | W4 | (i): a map as its entries sorted by key; needs both sides of one SQL type :218 (the audit's disagreement 3) |
| W7 | `SideEqualities.tagged` :306-360, `kindedRows` :895-1055, `kindsOf` :695-745, `kindKey` :881-887, `EXACT` :890, `DATE_KINDS` :751, `JSON_KINDS` :755, `jsonKindIs` :759, `jsonValue` :775, `writtenDate` :783, `dateKindOfText` :793, `writtenKindIs` :802, `unsignedYear` :824, `writtenDateValue` :835, `heldType` :863, `instant` :869, `writtenKey` :875, `stated` :1181, `fitted` :1174, `wideEnough` :1168 | mixed, Any (JSON), written dates | TAGGED | W4 | (a): EXACT key, always TWO_FAMILIES. (c): Year as BIGINT, YearMonth as y*12+m, StrictDate as DATE, DateTime as TIMESTAMP, kind read from the text's structure. (j): JSON kinds String/Integer/Float/Boolean. (h): an enumeration slot gives `false` (no kinded reading) :703-705 |
| W8 | `SideEqualities.instances` :361-411, `sameInstance` :412-428, `leafOf`, `pairedLeaves` :443, `restrictedRows`, `restricted`, `leaves` :551, `leafClass`, `fieldType`, `tracked` :620, `leafRows` :666 | instances of one class | INSTANCES | W4; `InstanceEquality.equality` :172 (sameInstance) | (g): the run-time class leaf, then the keys, or the identity (`__id`) |
| W9 | `SideEqualities.instanceTexts` :1845-1892, `jsonDocuments`, `rootElements`, `elementTexts`, `documentTexts` :1893-1947 | instances carried as JSON documents | INSTANCE_TEXTS | W4 | (g) by document text |
| W10 | `SideEqualities.tables` :1712-1720, `grids` :1601-1663, `tableSide` :1725, `typedGrid` :1749, `typedCells` :1544-1600, `cellType`, `besideExact` :1673, `holdsEveryInteger`, `columnsDiffer` :1695 | tables / grids | `==` and `hasSameElements` over tables; `assertEquals`/`assertSameElements` of grids | W3 :332; `VerdictSql.typedGridPair` :145 | (f); per-cell (a)/(b) via `besideExact`; a `Compared.alone` form for the query :1651-1660 |
| W11 | `SideEqualities.refuseIdentityOnly` :2127, `refuseMixedDays` :2138-2147, `variantKeysRefused` :2150 | refusals | all of the above, and HeldKinds | W4; `HeldKinds` :104-107; `JudgmentFacts` :102, :107 | (j) S401; (c) S399, a list holding days and date-times |
| W12 | `SideEqualities.Compared` :53-73, `Held` :77, `answerOnly` :70 | record plumbing for the judge | — | `RootComparison.answer` :292-298; `VerdictSql.typed` :98-109 | Carries the judge's failure-record values inside the product's type (the audit's "debt") |
| W13 | `lowering/WholeComparisons` (interface, 44 lines) | — | — | `Lowerer`, `RootComparison.over` | — |

### 1C. Compile time (the typer): decides before lowering whether, and how, a comparison is planned

| # | Site | Operations | Callers | Rules |
|---|---|---|---|---|
| T1 | `compiler/spec/EqualityClasses` :1-277: `COMPARISONS` :70-101, `crossing` :126, `heldCrossing` :144, `differing` :175-201 (pairs, lists and maps compared by their type arguments), `neverEqual` :211, `atRunTime` :219-232, `declaredOpen` :236, `of` :257-275, `litesOwn` :108-115, `notTheQuerys` :52-61 | `==`, `eq`, `hasSameElements`, `in`, `contains`, `indexOf`, orderings, `compare`, `max`/`min`(2-arg), every assert* that compares | `Typer.compared` :291-307, `comparedWhereItStands` :315-348; `LiteCrossTypeRule`; `PureComparisons` | (e) at compile time; (a): the exact pair exception :185-190, :211-213; (g): related classes are not different :197, :248-254; (h): an enum is its own type :264. Removes the source position to mark "not the query's" :52-61 |
| T2 | `compiler/spec/Typer.compared` :291, `ruleFor` :311-313, `comparedWhereItStands` :315-348, `atTheCall` :351-354 | the same | the typer, for every call | Picks the door's `CrossTypeRule`, or lite's for hasSameElements :312 |
| T3 | `compiler/spec/LiteCrossTypeRule` (276): `inQuery` :40-46, `exact` :53-68, `inBody` :81, `inGenericBody` :95-104, `atTheCall` :136-191, `harmless` :199-218, `neverEmpty` :221, `byValueThenType` :236-262 | the same | T2 (lite's door, and lite's own hasSameElements at every door) | (a): Integer against Decimal is the comparison itself. A crossing left as a marker is a refusal by the planner. (d): `neverEmpty`. Orderings of exact against Float folded into `<`/`<=` (D3) :236-262 |
| T4 | `compiler/spec/NumberLiterals` (298): `read` :53, `readWritten` :72, `literal` :242, `inExactArithmetic` :134 | `==` and orderings with a number literal | T3 :44, :100, :157 | (a): a literal reads as the other side's type when exact. Duplicated in `HeldKinds.readsAs` |
| T5 | `compiler/spec/CrossTypeComparisons.refuse` (74) and `typed/TypedCrossTypeComparison` (102: `refusal`, `refusalAtTheCall`, `refusalOverColumns`) | the marker | `PlanMaker` :96, `ProductStatements` :519, `SidePlanner` :92, `TestPlanner` :251, `SpecCompiler` :121; `Lowerer` :3913 | (e): a refusal by name |
| T6 | `compiler/spec/typed/TypedRunTimeComparison` (86: `neverEqual`, `answeredNeverEqual`, `fixedByTheCall`, `leftWritten`/`rightWritten`) | marker | T1 :229, T3, `PureComparisons.atTheCall`; `Lowerer.runTimeComparison` | — |
| T7 | `compiler/spec/typed/VariantEquality` (261): `between` :174, `refuseAmong` :245, `refuseVariantKeys` :231, `sideType` :149, `dateKinds` :56, `seen` :43 | Variant `==`, `in`, `contains`, `indexOf`, `removeDuplicates`, keys | `Equalities.nullArms` :457; Scalars :1336, :3587, :3657, :3771; `SideEqualities.pair`; `RootComparison.kind` :412; `InstanceEquality` :105 | (j) S401: identity only is refused; a Variant against other types is DISJOINT. (c): `dateKinds` feeds refuseMixedDays |
| T8 | `compiler/spec/LiteralUnroll` equality fold :~395-428, `comparedHere` :446-449, `spelledAlike` :454, `inFold` :482-498, `neverEqual` :506-510, contains fold :544-557, integer ordering fold :524-532 | `==`, `in`, `contains`, `<`... over spelled literals | the unroll pass | The fold rule: String, Boolean, enum and Integer only; (h): an enum against another kind is false :506 |
| T9 | `compiler/spec/OrderedAsserts` (181): `refuse` :48, `rewrite` :58, `unorderedSide` :117, `scope` :130; `compiler/spec/OrderView` (354) | (f) | `TestPlanner` :250; `BodyDriver` :326; `RootComparison.refuseUnordered` :350; `edge/UpstreamTestBodies` :78 | (f): whether a side has a defined order |
| T10 | `compiler/element/EqualityKeys` (245) | (g): key resolution | `SideEqualities.pair` :2051; BodyPlanner :~166; DatabaseJudge :534; JudgmentFacts | — |

### 1D. The database judge's own comparison code (test plan only)

| # | Site | Operations | Callers | What it does beyond W4 |
|---|---|---|---|---|
| J1 | `testplan/AssertVerdicts.adjudicate` :349-679 (331 lines; the switch :~473-670), `planDatabase` :67, `databaseGate` :115, `gateOf` :167, `sqlGridPair` :251, `wholeTables` :261, `gridSchemasDiffer` :267-283, `primitivePair` :882, `kindKey` :1880, `forAllContains` :1835, `envelopeValuesRead` :1803, `effectiveSchema` :1591 | every assert under the DATABASE judge | `DatabaseAsserts.plan` (via the verdict batch) | Dispatch by assert kind to the `VerdictArm` (`DatabaseJudge.ARM`) |
| J2 | `testplan/DatabaseJudge.databaseVerdict` :67-206 (144 lines) | `assertEquals`, `assertNotEquals`, `assertSameElements`, `assertEq` (primitives, keyless), `assertIs` (enum), `assertNotEq`/`assertIsNot` | ARM.equals :776, gridPair :783, sameElements :860, is :892, eq :956; AssertVerdicts :~656 | Its own kind gate :83-90 (`KindClass` inequality gives "unjudged"); an enum frame for an untyped wire :101-107; a TDSNull sentinel rewrite :114; `CanonRider` literal channel :108-109, :170-183; a grid against a list `gridColumnPair` :211-244; grid against grid `typedGridPair` :146-166; otherwise `JudgmentFacts.typedPair` and `SideEqualities.pair` :191-202; refuses a pair with no typed form :204 |
| J3 | `DatabaseJudge.databaseContains` :499-554 | `assertContains`, `assertNotContains` | ARM.contains; AssertVerdicts :~664 | `SideEqualities.contains`/`containsInstances`/`containsTagged` (JUDGE-ONLY members, see 1B gap) |
| J4 | `DatabaseJudge.databaseCondition` :600-647 | `assert`, `assertFalse`, `assertIs` (elements) | ARM.condition :995, ARM.is :905 | The forAll-contains fold is `SideEqualities.subset` :616-626. Otherwise the condition is lowered by the PRODUCT (`planSide` gives `BodyDriver.planValue`, which uses BodyPlanner's lowerer with `RootComparison.asking`; traced to BodyDriver :747, :758 and BodyPlanner :177, not line by line inside `planStaged`) and judged `IS NOT DISTINCT FROM true` (`VerdictSql.typedCondition` :157-166) |
| J5 | `DatabaseJudge.databaseSize` :397-438 | `assertSize`, `assertNotSize` | ARM.size | The product's `size()` and `==` via `VerdictQueries.sizeOf`/`sameValue` and `SidePlanner.over` (the one assert already planned "as the comparison it means") |
| J6 | `DatabaseJudge.databaseEmpty` :440-489 | `assertEmpty`, `assertNotEmpty` | ARM.empty | `VerdictSql.typedEmpty`/`typedEmptyTagged`/`typedEmptyGraph`/`typedNotEmptyGraph` (its own count) |
| J7 | `DatabaseJudge.databaseTolerance` :649-675, `toleranceArithmetic` :680-690 | `assertEqWithinTolerance` | ARM.tolerance | `VerdictSql.typedTolerance` :511-539, the judge's own formula |
| J8 | `DatabaseJudge.databaseTdsEquivalent` :560-596 | `assertTdsEquivalent` | ARM.tdsEquivalent | `VerdictSql.typedTdsEquivalent` :796-861, `toleranceCells` :862-907 |
| J9 | `DatabaseJudge.jsonStringsEqual` :1011-1087 | `assertJsonStringsEqual` | ARM.jsonStringsEqual | `VerdictSql.typedJson` :420-475 (JSON text via VARCHAR casts, canonical golden) |
| J10 | `DatabaseJudge.ARM` :763-1005: quantified :792, quantifiedVector :817 (returns null for ASSERT_EQUALS), perElement :865-891, is :892, instanceOf :910-955, eq :956-978 | quantified asserts, per element, instanceOf, eq | AssertVerdicts | perElement is `SideEqualities.perElement` :1351-1403 (JUDGE-ONLY); instanceOf is `VerdictSql.typedInstanceOf`/`typedInstanceOfHeld`; quantified is `typedAllOf` |
| J11 | `testplan/JudgmentFacts.TypedPair` :34-87, `typedPair` :93-98, `computed`, `variantDisjoint`, `variantMembership` :101-115, `sideFacts`, `kindOf`, `keyTree`, `keys`, `enumsOf` :119-324 | facts for the record and the family | DatabaseJudge | Picks `oneScalar` :39-48 and `family` :61-67 for the record; no comparison of its own |
| J12 | `lowering/VerdictSql` (1,560): `typed` :98, `typedContains*` :112-128, `typedSubset` :132, `typedPerElement` :139, `typedGridPair` :145, `typedCondition` :157, `typedOverValues` :191, `typedEmpty*` :221-337, `typedInstanceOf*` :338, :476, `typedJson` :420, `typedTolerance` :511, `typedAllOf` :541, `typedRow` :573-620 (record columns), `whereFails` :711, `typedTdsEquivalent` :796, `toleranceCells` :862, `batch` :1022-1146, `assertError` :1189, the canon machinery `Side` :44-63 / `canonRows` :1293-1392 / `gridRowCanons` :1393-1420 / `sideRows` :934, `decimalValue` :53 | the judge's statements | DatabaseJudge, DatabaseAsserts, VerdictBatch | Records, the judge's own tolerance, JSON, empty and instanceOf forms |
| J13 | `lowering/CanonRider` (191) | literal channel / kinds of a side | DatabaseJudge, BodyDriver, SidePlanner | Chooses the literal channel and the candidate kinds the judge compares by |
| J14 | `testplan/DatabaseAsserts` (717), `VerdictBatch` (822), `VerdictArm` (117), `TestPlanner.planIsolated` :96-114 (BOTH) | orchestration | the test door | — |

### 1E. The host judge (the one Java comparison tenet 5 allows)

| # | Site | Operations | Rules |
|---|---|---|---|
| H1 | `testexec/Equality.same` :139-215, `structural` :217, `ordered` :225-235, `sameElements` :242-248, `grid` :252, `rowMultiset` :263-281, `sorted` :291-306, `typeRank` :308, `withinRank` :322, `firstDiff` :382-412, `pureJson*` :339-380, `carrierKind`, `effectiveKind` :86-131 | every host assert | (a): exact by value only when `rules.forms().exactNumbersByValue()` :170-172. (b): a Decimal by value :180-187. Floats exact :188-203. (c): `LiteHostForms.equalDates` :214. (e): a kind mismatch is false :165. (d): null only equals null :147-148. (h): `LiteWords.EnumName` equality :148. (j): a `Kinded` Variant equals nothing :147. (i): maps by key set, then values :385-396. Order-free sort puts exact numbers before a Float only in lite's forms :300-304 |
| H2 | `testexec/LiteHostForms` (47): `equalDates` :22-28, `exactNumbersByValue` :38-41 (true); `HostForms` (30), `HostRules` (20); edge's `core/src/edge_harness/java/com/legend/edge/CompiledPureHostForms.java:46` (false) | — | (a) and (c) per door |
| H3 | `testexec/HostJudgments` (889): `judge` :286-315, `equalsOf` :785-820 (`Equality.ordered`), `sameElementsOf` :607, `containsOf` :436, `eqOf` :558, `subsetOf` :525, `gridPairOf` :388, `tdsRowValuesVerdict` :88, `tdsEquivalentOf` :362, `toleranceOf` :455, `jsonOf` :822, `asEqualSees` :333 (keys restriction), `restrictToKeys`/`restrictOne`/`restrictNested` :158-218 | dispatch per HostJudgment | (g): instances restricted to keys :333-343 |
| H4 | `testexec/TdsCompare.grids` :42, `tdsEquivalent` :121, `ordLeniency` :172 (179 lines) | tables | (f) and the TDS tolerance |
| H5 | `testexec/PureAsserts.withinTolerance` :32, `eqHolds` :58, `holds` :140 (160 lines) | tolerance, `assertEq` | — |
| H6 | `testexec/IsolatedRun` (206) | BOTH: runs both plans and compares the verdicts | — |
| H7 | `testplan/HostAsserts.planAssert` :309, `AssertVerdicts.planHost` :770-881, `planArm` :890-1000, `testplan/HostJudge.plan*` :58-353, `HostSides` | host planning: each side planned by the product as a value, the judgment as data | — |

### 1F. Door (edition) sites

| # | Site | What it changes |
|---|---|---|
| D1 | `edge/UpstreamTies.RULES` :50-74 | Names `THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION` for `==`, `eq`, `contains`, `removeDuplicates`, `in`, the four orderings :68-73; `SORT_TIES_AS_WRITTEN` :53; `EXTREME_KEEPS_THE_FOLDS_TIE` :59-63 |
| D2 | `edge/PureTable.lowersAs` :40-41; `edge/EngineTable.lowersAs` :202-217, `emptySideIsFalse` :267-269 (`outsideStore()`) | Legend-pure's and legend-engine's lowering tables |
| D3 | `edge/PureComparisons` (153): `inQuery`/`inBody`/`atTheCall`/`answer` :~60-140 (EQUALITY false if never empty; MEMBERSHIP false; INDEX -1; ORDERING and ASSERT the database's; inside a container the database's) | Legend-pure's typer setting |
| D4 | `edge/EngineComparisons` (66): every crossing is the comparison itself; `readsBodies` false | Legend-engine's typer setting |
| D5 | `edge/UpstreamTestBodies` :65-115 | Rewrites `assertEquals` over an unordered side, and `assert(a == b)`, to `assertSameElements` at a door |
| D6 | `lowering/EditionTable` (61, `emptySideIsFalse` default true :58-60); `LiteEditionTable` (none) | — |
| D7 | `lowering/NamedRule` :113-135 / `NamedRules` :~50 | The doors' rule, dispatching to Scalars V20 |
| D8 | `testplan/TestPlanner.refuseTheDatabaseJudgeInCompatibility` :73-80; `TestDoor` :74 | A door judges with the host alone |

### 1G. Dialect sites (EQUAL, NULL_SAFE_EQUAL, IS DISTINCT, the list forms)

| # | Site | Behaviour |
|---|---|---|
| S1 | `sql/dialect/SameValuesForm` (182): `of` :~41-51 (refuses an Array or Map value column), `inOrder`, `anyOrder`, `refuseExactNumbersOfTwoScales` :~130-150 | The portable SAME_IN_ORDER and SAME_IN_ANY_ORDER (row join with IS NOT DISTINCT FROM; signed UNION grouped) |
| S2 | `AnsiSqlRenderer.nullSafe` :1382-1393, `sameValues` :1397-1404, `sameLists` :1408-1425, `notDistinctFrom`, `distinctFrom` :1376-1378 | ANSI IS [NOT] DISTINCT FROM; DuckDB uses `sameLists(..,"list_sort")` (`DuckDb.java:617`) |
| S3 | `BigQuery.equalArm` :1871-1877, `compositeEquality` :1984-1988, `nullSafeArm` :~1879-1887 | A struct or array compared as TO_JSON_STRING text |
| S4 | `ClickHouse.equalArm` :2146-2153 (structs inside an array); NULL_SAFE via `coalesce(a = b, ...)` :2009-2014; sameValues :2012 | Tuple equality |
| S5 | `H2.equalArm` :844-866 | A Boolean against a String literal coerced ('N' gives false) |
| S6 | `MariaDb.equalArm` :2138-2147 (a composite as compacted JSON; `refuseExactNumbersOfTwoScales`); `<=>` :1951-1953 | Lists compared as JSON text |
| S7 | `SQLite.equalArm` :1916-1924 (`json(a) = json(b)`; `refuseExactNumbersOfTwoScales`) | Lists compared as JSON text |
| S8 | `Trino` :927 (a ROW pair as NOT DISTINCT), `rows` :543 | — |
| S9 | `MongoDb` :1038-1046 (`$eq`, `$ne`; SAME_* refused) | — |
| S10 | `membership(...)` per dialect: AnsiSqlRenderer :1109, BigQuery :1279, ClickHouse :1502, Databricks :734, DuckDb :836, MariaDb :1408; `CarrierStrategies.membershipRule` :~1834-1907 | The database's own `IN` and list containment |
| S11 | Databricks :530, :1078; Postgres :807; Snowflake :1400 | Plain or null-safe arms; no type-specific branch read |

---

## 2. Per rule: the sites that encode it

| Rule | Encoded at | Agree? |
|---|---|---|
| (a) number families | V10 `NumberFamilies.canEqual`; V9 `MixedEncoding.exactNumber`/`numberTypePlace`; V11 HeldKinds :120; V7 `oneChannelPerFamily` :68; W5 `values` :104-113; W7 `kindKey`/`EXACT` :881-890; W4 `typedClass` (via RootComparison :88-93); `SideEqualities.exactNumber`/`oneValueKind` :1151-1160 (DatabaseJudge :528, :617, :881); T1 `EqualityClasses.differing`/`neverEqual` :185-213; T3 `LiteCrossTypeRule.exact` :53-68; T4 NumberLiterals; HeldKinds.readsAs :170; H1 Equality :170-172, :300-304; H2 forms; doors D1 to V20 (THREE_TYPES) | **Known to disagree**. (1) The audit's case 1: `n: Number` on a DECIMAL column, `== 0.5` refused (readsAs accepts only whole Floats) where `d: Decimal == 0.5` compiles (NumberLiterals). (2) The audit's case 2: `Number[*]` lists from an exact column and a Float column. `SideEqualities.values` gives false (`bothEmptyOnly` :106), where single values are refused by HeldKinds. (3) The closing-batch record (`docs/records/edition-homework/step-6-closing-batch-2026-11-27.md`): `assertEquals([1, 2], [1D, 2.00D])` passes as an assert and fails as `==` on H2 and SQLite (3+3 bodies). The whole-side form (W5/W7) and `TaggedLists` use TWO_FAMILIES unconditionally, but only lite's door reaches them; the doors' tagged rules take THREE_TYPES |
| (b) Decimal by number | W5 `besideDecimals` :1196-1234 (canonical text or widened DECIMAL), `oneCarrier` :1145; W7 `stated`/`wideEnough`; V22 the canon normalises scale; V7 delegates to the database's `=` per channel; V1 leaves it to the database's `=`; S6/S7 compare list JSON TEXT (refused only for two declared scales); W6 needs the same SQL type on both sides :218; H1 `decimal().compareTo` :186; legacy `VerdictSql.decimalValue` :53 (the audit says it is dead; `decimalByValue` is never true) | **Known to disagree**. The audit's case 3: maps of Decimals at two scales. `collections` returns null and the RootComparison map refusal now refuses them (:113-121). That refusal exists only at the root or with readable sides; a row-reading map is refused at :187. List values per row on SQLite and MariaDB compare as JSON text when the types state one scale, or when an operand states no type (SameValuesForm javadoc; record D8) |
| (c) dates | V1 :152-193 (partial precision and instant); V6 upstream precision (doors); V13 `dateArg` padding for orderings; V9 `family` (DateTime(n) is one family); V7 per-channel `=`; W4 `dateKind` :2105; W5 `instants` :1072; W7 DATE_KINDS and `writtenDateValue` :751-859; W11 `refuseMixedDays`; V11 HeldKinds day against date-time; T7 `VariantEquality.dateKinds`; H2 `LiteHostForms.equalDates` (kind, then instant floor); V22 CanonicalRenderSql :1043-1046 keeps written digits | **Known to disagree**. Closing-batch record: "dates and written precision" verdicts move between an assert and `==` (DuckDB 7, H2 7, SQLite 14), e.g. `assertEquals(%2020-01-02T00:00, [%2020-01-02, %2020-01-01T00:00]->at(0))`; one SQLite "JSON date-time" case. The value form compares a partial computed against a literal by text (V1 :166); the whole-side form by components (W7). The two agree on each case only if they read the same text; not verified |
| (d) empty side | V1 :126-144 (literal `[]`); V2 `nullArms`; V3 `emptySidesAsPure`; V5 `sameOnlyWhereBothEmpty`; V26 `runTimeComparison` / `none`; V27 positive-condition marks; V12 negation; V14/V15 COALESCE to false; W5 EMPTY_ONLY / `noRows` / `sameOnlyWhereBothEmpty` :171; T3 `neverEmpty` :221; D3 `PureComparisons.neverEmpty`; D2 `EngineTable.emptySideIsFalse`; H1 :147-148 | **Known to disagree**. The closing-batch record: `0 == ''->ascii()` as a plain comparison returns no value on H2 and SQLite. The audit: shared inlined nodes defeat the positive mark (partly addressed by `countPlaces` :386-394 since). Enumerations are left out of the both-empty rule (V3 :70; V2 :472) |
| (e) different types never equal | T1 (compile time); T3; T5 marker refusal; V1 :173-181; V9 `kindMismatch`; V14 :3754; V15 :3777; V7 `canEqual` false; V11 refusal over columns; W1 :88-100 "unplanned"; W5 EMPTY_ONLY; J2 kind gate :83-90; H1 :165 | Different behaviours by design: compile error, planning refusal, false, or "same only where both are empty". **Disagree** on lists, per the audit's case 2 |
| (f) order | W1 `refuseUnordered` via T9 `OrderedAsserts.unorderedSide`; W5/W10 SAME_IN_ORDER or SAME_IN_ANY_ORDER; S1/S2 dialect forms; V4/V7 tagged lists in order; the database's array `=` for per-row lists; J2 `canonicalOrder`; H1 `ordered`/`sameElements`/`rowMultiset`; D5 the doors rewrite to assertSameElements | Lite: `hasSameElements` refuses where `assertSameElements` answers in one family (a frame's table against a written list; DuckDB 2, from the closing-batch record) |
| (g) instances | V21 InstanceEquality (identity `__id`, class fold, `sameInstance`, canon text); V22 canon; W8 `instances`/`sameInstance`; W9 instance texts; W4 INSTANCES / packageable by path; T1 `related` classes; T10 keys; J10 eq; H3 `asEqualSees`/`restrictToKeys` | V21 shares `sameInstance` with W8 but falls back to canon text :177-186 and to a "generic rule" (null gives the Lowerer's refusal :4601). Whether the canon fallback and W8 always agree: not verified |
| (h) enumerations | V23 decodeInvert; V12 `enumInvolved` (expanded NOT arms, no null-safe); V3 :70 (skipped); V2 :472; T1 `of` :264; T8 `neverEqual` :506; W7 `kindsOf` refuses enum slots :703; W4 `typedClass` accepts EnumType (VALUES); J2 enum frame :101-107; ARM.is :896-901; H1 `LiteWords.EnumName` | Cannot be told from reading (no measured case found) |
| (i) maps | W6 `collections`/`entryRows` (sorted entries); W1 :113-121 (refuse a no-typed map); W2 :187-193 (refuse a row-reading map); the database's map `=` elsewhere; H1 `firstDiff` key set | **Known to disagree**, per the audit's case 4: `assert($m1 == $m2)` at a door goes through the value form (the database's map `=`), where `assertEquals` goes to the host's maps by entries. At lite's door the RootComparison refusals now cover the cases that read the row |
| (j) Variant/JSON | T7 `VariantEquality.between`/`refuseAmong`/`refuseVariantKeys`; V2 :457; V14 :3657, :3746 (TO_VARIANT JSON compare); V15 :3771, :3788-3809; V16 :3587; V17 :1336; V24 alignLiteralToJson; V4 `variantList` JSON `=` :374; W4 EMPTY_ONLY :1992; W7 JSON_KINDS; J3 `variantMembership`; H1 `Kinded` | An Any list's membership in the value form is JSON-to-JSON database equality (V14 :3746), where the whole-side form reads JSON kinds (W7). Whether they agree for `1` against `1.0` in JSON: not verified |

---

## 3. The call graph

### (i) Two single values, `$x.a == 'k'`
1. `Lowerer.scalar` → `scalarInner` (:3899).
2. If the typer left a `TypedCrossTypeComparison`, it throws (:3913). If it left a `TypedRunTimeComparison`, it goes to `runTimeComparison` (:3919, V26).
3. Otherwise `scalarRelationalArms`: `InstanceEquality.staticallyDisjoint` gives `disjointAnswer` (:4399).
4. Otherwise `scalarValueTailArms`:
   - `wholeSides(n)` (:4590). It is null here because `RootComparison.insideAnExpression` returns null when neither side is whole or a table (:176).
   - Then `InstanceEquality.claims` (:4596).
   - Otherwise `written(n, nativeArgs)` (:4619).
5. `written`:
   1. `Scalars.lower` (:2409).
   2. `doorsRule` (:2414). A door's row goes to `NamedRules`, then to `threeNumberTypes...` (V20), then to `upstreamsPartialDateTime` or `Equalities.equal(THREE_TYPES)`.
   3. Otherwise `HeldKinds.refuseAcrossKinds` (:2421), then the RULES entry `Equalities.equal` (V1), then `nullArms` (V2).
   4. Back in `written`: verbatim or legacy flag → plain `=` (:439). Otherwise `emptySideIsFalse` (door) → `emptySidesAsPure` with the positive mark (:442-444).
6. The dialect renders EQUAL, NULL_SAFE_EQUAL or COALESCE.

Branch points:
- the typer's marker (decided by `EqualityClasses` and the door's `CrossTypeRule`);
- `staticallyDisjoint`;
- whether `instanceKeysOf` is set (`InstanceEquality`);
- `doorsRule` (the edition table);
- `HeldKinds` (SQL type facts);
- inside `equal`: the literal `[]`, Tagged, partial precision, TEMPORAL_TEXT against TIMESTAMP, `decodeInvert`, value against list;
- in `nullArms`: the operand shapes and multiplicities;
- in `written`: the flags, `emptySideIsFalse`, the positive mark.

### (ii) Two whole sides at the query's root
1. `Lowerer` :681 → `wholeAnswer(body)` (:493-510). It requires `wholeComparisons != null`, the last statement to be `==` or `hasSameElements` (or `!` of one), and `doorsRule == null`.
2. `WholeComparisons.asTheStatement` → `RootComparison.plan` (:59). The branches:
   - draws afresh → unplanned;
   - a table → `tables` → `SideEqualities.tables` → `grids`;
   - two known types that differ → unplanned;
   - neither side whole → null (the value form, as (i));
   - otherwise `refuseUnordered`, each side lowered as its own statement, `SideEqualities.pair` → `Pair.compared` → `values`, `tagged`, `instances`, `collections` or `instanceTexts`;
   - a null map → refused.
3. `answer` → `answerOnly` → one-row SELECT.
4. The dialect renders SAME_IN_ORDER or SAME_IN_ANY_ORDER (DuckDB's list form, or the portable `SameValuesForm`).

### (iii) Two whole sides inside an expression (an `if`, `&&`)
1. `scalarValueTailArms` :4590 → `wholeSides` (:480). It is null where `doorsRule != null`.
2. `insideAnExpression` (:171). It returns null when no side is whole or a table, and null when the free variables are not all readable. A map read from the row is refused.
3. Otherwise `plan(..., negated=false)`, as in (ii), read as a `ScalarSubquery`.
4. For `hasSameElements` a null throws `hasSameElementsInsideAnExpression` (:4581-4583).

### (iv) A side that reads the row it stands in
`insideAnExpression` returns null at :179-195, so the comparison falls through to (i). The arguments are lowered per row: a list as the database's array or JSON. The comparison is `Equalities.equal`, often the database's array `=`, or a `taggedEquality`. On SQLite and MariaDB a composite goes to `equalArm` → JSON text (`refuseExactNumbersOfTwoScales`). A map is refused (:187). `hasSameElements` is refused.

### assertEquals → verdict

**HOST judge**
1. `TestPlanner.plan` (:122), then `HostAsserts.plan`, then `planAssert` (:309).
2. `AssertVerdicts.planHost` (:770), then `planArm` (:890), then `HostJudge.planEquals` (:319). That yields a `HostSides.Check` whose sides are planned as product values (each side lowered by the product, comparisons inside a side included), plus a `HostJudgment.Equals`.
3. At run: `TestRunner` → `HostJudgments.judge` (:286) → `equalsOf` (:785). Grids go to `tdsRowValuesVerdict`; otherwise `asEqualSees` (keys), then `Equality.ordered` → `same` (H1) with `HostRules` from `run.hearing().hostForms()` (:288; the door's forms).

**DATABASE judge**
1. `DatabaseAsserts.plan` → `AssertVerdicts.planDatabase` / `adjudicate` (:349). The case `ASSERT_EQUALS`/`ASSERT_NOT_EQUALS` (:~486-540) branches:
   - a relation pair: `sqlGridPair` → `arm.gridPair`, or unjudged;
   - otherwise `arm.equals`.
2. `DatabaseJudge.databaseVerdict` (:67) branches in order:
   1. the kind gate;
   2. the CanonRider plans of each side (`BodyDriver.planValue`, the product's lowering);
   3. a grid against a list (`gridColumnPair`), or grids (`VerdictSql.typedGridPair` → `SideEqualities.grids`);
   4. the literal channel check;
   5. `JudgmentFacts.typedPair` → `SideEqualities.pair` → `Pair.compared`, then `VerdictSql.typed` (adds the record columns);
   6. `runTyped` → a deferred verdict row in `VerdictBatch`.
3. At run: the database answers `__verdict`. A failure's record is built by `AssertRecords.ofTypedRow` (testexec/AssertRecords :88).

**BOTH**: `TestPlanner.planIsolated` (:96-114) builds two plans, a host one and a database one, held as `TestPlan.Isolated`. `IsolatedRun` (testexec) runs each and compares the verdicts. If the database plan's judge is not DATABASE, the host plan alone is used (:103-107). At a door only HOST is allowed (TestPlanner :73).

**`assert(a == b)`**:
- Database judge: `databaseCondition` (J4) → product `==` (paths i to iii).
- Host judge: `planCondition` (HostJudge :109); the condition is planned by the product too.
- Door: may be rewritten to `assertSameElements` (D5).

---

## 4. Lines of code

File totals (`wc -l`):

| Group | Files (lines) | Total |
|---|---|---|
| Value form (lowering) | Equalities 478; TaggedLists 974; MixedEncoding 1,183; NumberFamilies 30; HeldKinds 184; NullSemantics 185; InstanceEquality 496; CanonicalRenderSql 1,119; EnumSourceValues 114; VariantShapes 118; Comparators 124; CastPolicy 321; Scalars 4,252; Lowerer 5,214 | — |
| Whole-side form | SideEqualities 2,156; RootComparison 420; WholeComparisons 44 | 2,620 |
| Door plumbing (lowering) | NamedRule 135; NamedRules 59; EditionTable 61 | 255 |
| Compiler | EqualityClasses 277; LiteCrossTypeRule 276; CrossTypeComparisons 74; CrossTypeRule 93; NumberLiterals 298; TypedRunTimeComparison 86; TypedCrossTypeComparison 102; VariantEquality 261; OrderedAsserts 181; OrderView 354; LiteralUnroll 776; VerdictQueries 779; EqualityKeys 245 | — |
| Database judge | VerdictSql 1,560; DatabaseJudge 1,088; JudgmentFacts 324; AssertVerdicts 1,925; CanonRider 191; DatabaseAsserts 717; VerdictBatch 822; VerdictArm 117; BodyDriver 878 | — |
| Host judge | testexec/Equality 455; HostJudgments 889; TdsCompare 179; LiteHostForms 47; HostForms 30; HostRules 20; PureAsserts 160; AssertRecords 411; LiteWords 384; IsolatedRun 206 | — |
| Edge | PureComparisons 153; EngineComparisons 66; UpstreamTies 86; PureTable 86; EngineTable 340; UpstreamTestBodies 539; edge_harness CompiledPureHostForms 57 | — |
| Dialect | SameValuesForm 182; plus the arms listed in 1G | — |

Comparison-specific spans inside the large files, estimated from method start lines:

| File | Spans | Estimate |
|---|---|---|
| Scalars | containsRule ~121; inRule ~78; removeDuplicatesRule ~64; threeNumber… 32; extremes and ties ~215 (:3889-4140); indexOf ~33; sort ~67; ordering ~17; not ~14; lower/doorsRule ~55 | ~700 |
| Lowerer | asCondition group ~95 (:346-461); wholeSides/wholeAnswer ~50; runTimeComparison group ~92 (:3806-3897); the arms ~45; whereLambda 6 | ~290 |
| TaggedLists | equality, membership, sort and dedup members (:63-190, :296-613, :618-810) | ~600 |
| MixedEncoding | kind vocabulary and comparables (:86-130, :339-470, :518-660, :876-883) | ~300 |
| SideEqualities, product-reached | Compared/Held 40; values family ~165; collections ~70; tagged and kinded ~400 (:306-360, :695-1055); instances ~260 (:361-686 less the contains members); instance texts ~100; grids and tables ~240 (:1502-1840); pair and kinds ~210 (:1940-2155); decimal and instant helpers ~150 | ~1,600 |
| SideEqualities, judge-only | `containsInstances` 43; `containsTagged` 44; `contains` 38; `subset` 33; `perElement` 53; `emptyKind` 12; `oneValueKind` 4 | ~227 |
| VerdictSql | — | all 1,560 is judge code (no product caller except `classExtentRows` from `FramePlanner` and `graphCount` from `SnapshotEnvelope`, ~110 lines) |
| DatabaseJudge | databaseVerdict 144; gridColumnPair 36; databaseContains 61; databaseCondition 49; databaseTolerance 48; databaseTdsEquivalent 40; databaseEmpty 51; databaseSize 43; jsonStringsEqual 77; ARM 248; rest ~290 | 1,088 |
| AssertVerdicts | judge-side: adjudicate 331 + planDatabase/gate 172 | ~500 |
| AssertVerdicts | host-side: planHost + planArm 223 | — |

**What a merge would touch.**
- The product's value form and the whole-side form: about 6,000 lines (Equalities 478, HeldKinds 184, NullSemantics 185, NumberFamilies 30, InstanceEquality 496, the Scalars spans ~700, the Lowerer spans ~290, TaggedLists ~600, MixedEncoding ~300, SideEqualities 2,156, RootComparison 420, VariantShapes/EnumSourceValues/CastPolicy spans ~150).
- The database judge's comparison and verdict code: about 5,500 lines (VerdictSql 1,560, DatabaseJudge 1,088, JudgmentFacts 324, AssertVerdicts ~500, CanonRider 191, DatabaseAsserts/VerdictBatch parts not estimated).
- The compile-time rule (~1,600) and the host judge (~1,900) are legitimately separate. They would be touched only where they duplicate the product's kind vocabulary.

---

## 5. Where the path depends on WHO IS ASKING

| Site | What switches |
|---|---|
| `compiler/spec/Typer.ruleFor` :311-313 and `atTheCall` :351-353 | The door's `CrossTypeRule` (`LiteCrossTypeRule`, `edge/PureComparisons` :36, `edge/EngineComparisons` :21) decides the marker, a constant or the plain call. Set by `TypedQuery.comparing` (PureComparisons :50, EngineComparisons :28, :43, :49) |
| `lowering/Scalars.doorsRule` :2402-2404 → `EditionTable.lowersAs` (`edge/PureTable` :40, `edge/EngineTable` :202-217 → `UpstreamTies.rule` :79-86) | Replaces the rule for `==`, `eq`, `in`, `contains`, `removeDuplicates`, orderings, `sort`, the extremes (V20) |
| `Scalars.lower` :2414-2421 | A door's row skips `HeldKinds` |
| `Lowerer.wholeSides` :481 and `wholeAnswer` :506 | `doorsRule != null` turns the whole-side plan off: lists, maps and tables at a door take the value form |
| `Lowerer.written` :443 → `EditionTable.emptySideIsFalse` (EngineTable :267-269 by `outsideStore()`) | The empty-side guard |
| `Lowerer.written` :439 (`legacyNullUnsafeEquals`, a legend-engine Feature flag, `platform/Feature.java:~33`) | A plain `=` |
| `NumberFamilies.THREE_TYPES` (from V20) | Tagged comparisons and ties |
| `Equalities.upstreamsPartialDateTime` (V6) | Door only |
| `TaggedLists.Ties` / `MixedEncoding.TiePick` via `NamedRule.SORT_TIES_AS_WRITTEN` and `EXTREME_*` (Scalars :3891-3952, :3989-4117 `upstreamTies`) | Ties |
| `testexec/HostJudgments.judge` :288 | `HostRules` from `run.hearing().hostForms()`; `Equality.same` :170 and `sorted` :302 read `exactNumbersByValue` (lite true: `LiteHostForms` :38; edge false: `core/src/edge_harness/java/com/legend/edge/CompiledPureHostForms.java:46`) |
| `testplan/TestPlanner.refuseTheDatabaseJudgeInCompatibility` :73-80 | The judge at a door is the host's alone |
| `edge/UpstreamTestBodies` :65-115 | Rewrites the door's asserts (assertEquals over an unordered side, and `assert(a == b)`) |
| `planner/PlanEnv.edition` :187; `ProductStatements` :140-146, :194; `BodyPlanner` :202, :215; `FramePlanner` :132, :173 | The `outsideStore` and test-body stamps, which `EngineTable.emptySideIsFalse` and `checks` then read |
| `compiler/spec/EqualityClasses.litesOwn` :108 | hasSameElements always uses lite's rule |

---

## 6. Deletion candidates (factual) and the gap each leaves

A candidate here is code that exists only to plan or judge an assert's comparison in the database, apart from the product's `==`. The "gap" column says what the product's `==` path does NOT do today. Closing that gap is the merge's work.

| Candidate (file:line, lines) | What it does | Gap vs product `==` today |
|---|---|---|
| `testplan/AssertVerdicts.adjudicate` :349-679 (331) + `planDatabase` :67-114 (48) + `databaseGate`/`gateOf` :115-238 (124) + `sqlGridPair`/`wholeTables`/`gridSchemasDiffer`/`kindKey`/`forAllContains`/`envelopeValuesRead`/`effectiveSchema` (~150) | The database judge's dispatch per assert kind | The product has no notion of an assert's polarity, its message, or "unjudged" |
| `testplan/DatabaseJudge.databaseVerdict` :67-206 (144) | assertEquals, assertNotEquals, assertSameElements, assertEq, assertIs | (1) A kind gate that returns "unjudged" for non-primitive kinds that differ (:83-90), where the product refuses or plans. (2) An enum frame for an untyped wire (:101-107). (3) The TDSNull sentinel cells of a golden (:114). (4) The CanonRider literal channel for Any or mixed sides (:170-183); the product uses Tagged MIR instead. (5) Sides on two different databases: refused (:132). (6) A host constant bound as VALUES on the other side's database (`constantSide`). (7) **The failure record: each side's values as typed columns** (`VerdictSql.typed`/`typedRow`), which `==` never writes. (8) **A grid against a list** (`gridColumnPair` :211-244: one column's values, or a wide grid's cells row-major); the product's `hasSameElements` refuses "a table against a side that is no table" (closing-batch record) |
| `DatabaseJudge.gridColumnPair` :211-244 (36) | The flattened cells of a frame against a list | **A frame's cells compared as a flat list**: not in RootComparison (it refuses a table against a non-table, :312-316) |
| `DatabaseJudge.databaseContains` :499-554 (61) + `SideEqualities.containsInstances` :469-511 (43), `containsTagged` :1236-1279 (44), `contains` :1280-1317 (38) + `VerdictSql.typedContains*` :112-128 | assertContains, assertNotContains over whole sides | **Membership of a whole side**: RootComparison plans only `==`/`hasSameElements` (:66-69). The product's `contains`/`in` are value-form only (V14, V15, V21) |
| `DatabaseJudge.databaseCondition` forAll-contains branch :603-629 + `SideEqualities.subset` :1318-1350 (33) + `VerdictSql.typedSubset` :132 | `assert($need->forAll(n | $have->contains($n)))` as a subset statement | A subset of two whole sides; the product's form is a per-row lambda |
| `DatabaseJudge.ARM.perElement` :865-891 + `SideEqualities.perElement` :1351-1403 (53) + `VerdictSql.typedPerElement` :139 | Quantified `assertEquals` element by element (`forAll(x | assertEquals(...))`), recording the first failing index | **The first failing element's index and operands in the record**; `quantifiedVector` declines the product's vector for ASSERT_EQUALS (:822-826) because the product's `==` would "compare across kinds and drop a Decimal's scale" |
| `DatabaseJudge.databaseTolerance` :649-690 (48) + `VerdictSql.typedTolerance` :511-540 (30) | assertEqWithinTolerance | **Tolerance**: the judge's own formula and arithmetic type (`toleranceArithmetic`). `VerdictQueries.assertAsPredicate` :490-496 already has an `abs(a-b) <= t` spelling used for the quantified vector |
| `DatabaseJudge.databaseTdsEquivalent` :560-596 (40) + `VerdictSql.typedTdsEquivalent` :796-861 (66) + `toleranceCells` :862-907 (46) + `carriedTemporal` | assertTdsEquivalent | **TDS equivalence with numeric and temporal tolerance**; no product function exists |
| `DatabaseJudge.jsonStringsEqual` :1011-1087 (77) + `VerdictSql.typedJson` :420-475 (56) | assertJsonStringsEqual | **JSON-document equality** (canonical golden, an unordered root as a multiset; `VerdictQueries.canonicalJsonGolden`/`jsonRootElements`) |
| `DatabaseJudge.databaseEmpty` :440-489 (51) + `VerdictSql.typedEmpty` :301, `typedEmptyTagged` :322, `typedEmptyGraph` :221, `typedNotEmptyGraph` :236, `graphCount` :243-293 (shared with `SnapshotEnvelope`) | assertEmpty, assertNotEmpty | Not an equality; the record lists the values. Could be `isEmpty()` planned as the product does `size` (J5's pattern) |
| `ARM.instanceOf` :910-955 + `VerdictSql.typedInstanceOf` :338, `typedInstanceOfHeld` :476 | assertInstanceOf | Not an equality; the record names the type |
| `ARM.quantified`/`quantifiedVector` :792-859 + `VerdictSql.typedAllOf` :541 | quantified assert/assertFalse | Already the product's vector of Booleans; the judge adds only the record |
| `ARM.eq` :956-978, `ARM.is` :892-909 | assertEq, assertIs | Identity: a keyed pair is refused :977; elements go to `ChainNormalizer.identityCondition` |
| `testplan/JudgmentFacts` :34-115 (TypedPair, `oneScalar`, `family`) (~80 of 324) | The record family and the oneScalar gate | Record only |
| `lowering/CanonRider` (191) | The literal channel and candidate kinds of a judged side | The product uses `SqlExpr.Tagged`/`TaggedList`; two representations of "a kind beside the value" |
| `lowering/VerdictSql.Side` :44-49, `decimalValue` :53-60, `canonOf` :62, `canonRows` :1293-1392, `canonRowsLayered`, `gridRowCanons` :1393-1420, `gridCell`, `rowsOf` :1435-1472, `unpivot` :1473, `sideRows` :934 (one caller, DatabaseJudge) | The canon-text machinery | The audit says `Side`'s flags and `decimalValue` are dead (`decimalByValue` is never true); not re-verified |
| `SideEqualities.Compared.alone`/`answerOnly`, `Held` :53-83 | The record values carried in the product's type | The product needs only `same` |
| `testplan/DatabaseAsserts` (717), `VerdictBatch` (822), `VerdictArm` (117), `DatabaseGate` (17) | Orchestration and fusion of verdict rows | These would carry a plan of `==` statements instead. Their own logic is fusion, budget and raising runs (VerdictBatch :720-813), not equality |

Already merged (no gap): `assertSize` and `assertNotSize` (J5, `VerdictSql.typedOverValues` :191-215 with `SidePlanner.over`); `assert`/`assertFalse` over a condition (J4's non-forAll branch: the product lowers the condition); the whole-side `assertEquals` pair (J2 → `SideEqualities.pair`, the same leaf as RootComparison).

**Gap summary.** Before an assert can be "the plain comparison it means, planned like any query", the product's `==` path needs:
1. the failure record (each side's values, typed, and the polarity), written apart from `same`;
2. a frame or grid against a flat list (`gridColumnPair`), where RootComparison refuses;
3. membership and subset of whole sides (assertContains, assertNotContains, forAll-contains);
4. the first failing element of a quantified assertEquals;
5. tolerance (assertEqWithinTolerance, assertTdsEquivalent);
6. JSON-document equality (assertJsonStringsEqual);
7. the TDSNull golden sentinel and the enum frame for untyped wires;
8. sides on two databases and host constants bound as VALUES;
9. the "unjudged" kind gate's cases (non-primitive kinds that differ), which the product refuses or answers differently;
10. agreement on the measured movers: dates and written precision (DuckDB 7, H2 7, SQLite 14), number types `[1,2]` against `[1D,2.00D]` (H2 3, SQLite 3), one SQLite JSON date-time, `0 == ''->ascii()` (H2 1, SQLite 1);
11. at the doors, the whole-side form is turned off today (§5), so a door's `assert(a == b)` and `assertEquals` take different codes.
