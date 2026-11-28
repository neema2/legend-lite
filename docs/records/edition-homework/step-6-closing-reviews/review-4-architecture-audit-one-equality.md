# The independent ARCHITECTURE audit of step 6, "the one equality" (at `8b84e6a62`; the user: "make sure the code is adhering to our tenets and actually doing the real one equality work for real as real architecture/design")

Read-only, by an independent reviewer (a subagent given the code, the design and the rules, never the author's
conclusions), 2026-11-27. Kept verbatim as evidence. Nothing in it was run by the reviewer: each finding is from
reading. Paths under `/home/user/ll-audit...` are a read-only worktree of this repository.

---

# Architecture audit: step 6, "the one equality", at 8b84e6a62 (read-only)

## Verdict
"One equality" is real only for one case: a whole-side `==` or `hasSameElements` (computed lists, maps, tables) at lite's own door, written where it reads nothing of the row. There the query's answer, the same comparison inside an `if`, and the database judge's `assertEquals` all reach `SideEqualities.pair` → `Pair.compared`.

Everywhere else the product still has two equalities:
- **The value form** (`Equalities`, `TaggedLists`, `MixedEncoding`, `InstanceEquality`, `NullSemantics`, the `Scalars` membership rules).
- **The whole-side form** (`SideEqualities`).

They use different representations, and only tests keep them in step. Asserts are not planned as `==`. The judge's code was moved, not deleted: `core/src/main` grew by 3,511 lines net, where the plan promised "about 2,500 lines gone". Door-dependence after the type-checker is wider than the ruled single lookup.

## A. Where "equal" is decided

| Shape | Query answer / inside an expression (lite) | Database judge | Host judge | legend-pure / legend-engine doors |
|---|---|---|---|---|
| 1 one known type | `Equalities.equal`→`nullArms` (Equalities.java:106,400); empty sides `emptySidesAsPure` via `Lowerer.written` (Lowerer:399-407); `!` via `NullSemantics.negate` (:138) | `DatabaseJudge.databaseVerdict`→`JudgmentFacts.typedPair`→`SideEqualities.pair/values` (SideEqualities:1987,93) | `testexec/Equality` (:~150-240) | `Scalars.numbersOfThreeTypes`→`Equalities.equal(…,THREE_TYPES)`, `upstreamsPartialDateTime` (Scalars:3854; Equalities:261); `EngineTable.emptySideIsFalse` (:267) |
| 2 typed at run time | written in a function: `LiteCrossTypeRule.atTheCall` (:135), which gives a constant or `TypedRunTimeComparison`; that marker goes to `Lowerer.runTimeComparison` (:3771). Column-typed: `HeldKinds.refuseAcrossKinds` (:88) | `SideEqualities.pair` (kinds from CanonRider) | Equality | `PureComparisons.atTheCall` (:81), `EngineComparisons` |
| 3 whole lists | root: `Lowerer.wholeAnswer`(:454)→`RootComparison.plan`(:59)→`SideEqualities.pair`. Inside: `Lowerer.wholeSides`(:441)→`insideAnExpression`(:163), as a ScalarSubquery | SideEqualities | Equality.sameElements/ordered | value form only: `doorsRule`≠null switches the plan off (Lowerer:442,467) |
| 4 lists reading the row | value form, i.e. the database's array `=`; `SameValuesForm.refuseTextOnlyLists` (SameValuesForm:61; MariaDb:2141, SQLite:1919) | — | — | same |
| 5 maps | `SideEqualities.collections` (:219); a row-reading map is refused (RootComparison:179) | same | Equality | database map `=` |
| 6 tables | `RootComparison.tables`(:278)→`SideEqualities.tables`(:1717) | `VerdictSql.typedGridPair`→`SideEqualities.grids`(:1606); `typedTdsEquivalent` (VerdictSql:796) is judge-only | TdsCompare | database's own |
| 7 instances | `InstanceEquality.equality` (:127) tries three forms in turn: `SideEqualities.sameInstance`, else canon text (:180-185), else "generic rule". Whole sides: `SideEqualities.instances`(:366) / `instanceTexts`(:1850) | same | HostJudgments | same as lite |
| 8 Variant | `VariantEquality.between` (Equalities:423); `refuseIdentityOnly` (SideEqualities:2132) | Form EMPTY_ONLY | `LiteWords.asVariants` | same |
| 9 mixed/tagged | `Equalities.taggedEquality`(:293)→`TaggedLists.sameElement/sameList`(:618-719) | `SideEqualities.tagged`/`kindedRows` (:311,900) | Equality | TaggedLists with THREE_TYPES |
| 10 membership, order | `Scalars.containsRule/inRule/removeDuplicatesRule(NumberFamilies)` (:1468,2001,2326), `InstanceEquality.contains/indexOf`; orderings `LiteCrossTypeRule.byValueThenType`, HeldKinds | `SideEqualities.contains*/subset` (:1241-1356): judge-only, since RootComparison plans only `==` and `hasSameElements` | Equality | `UpstreamTies` rows (:52-74) |

**How many implementations.** There are two complete SQL-level product equalities: the value form and the whole-side form. Beside them sit two legitimately separate rules:
- the type-checker's compile-time rule (`EqualityClasses`, `LiteCrossTypeRule`, `NumberLiterals`);
- the host judge (tenet 5).

**Duplicated fragments**, each of which can drift:
- the literal rule: `NumberLiterals.literal` (:242) vs `HeldKinds.readsAs` (:160);
- "may raise": `LiteCrossTypeRule.harmless` (:200) vs `Lowerer.computes` (:3825);
- the list of comparison functions, kept three times: `EqualityClasses.COMPARISONS`, `HeldKinds.READ`, `UpstreamTies`;
- the kind a column gives: `HeldKinds` vs `RootComparison.kind` (:372) and `wireDecidedKinds` (:301);
- the tagged vocabularies: `TaggedLists` with `NumberFamilies` vs `SideEqualities.kindKey`/`EXACT`.

**Concrete disagreements, found by reading (none run):**
1. With `n: Number` on a DECIMAL column, `$p.n == 0.5` is refused: `readsAs` accepts only whole Floats. With `d: Decimal`, `$p.d == 0.5` compiles, because `NumberLiterals` reads 0.5D as exact.
2. Two whole `Number[*]` lists, one from an exact column and one from a Float column: `SideEqualities.values` answers **false** (`bothEmptyOnly`, :106). The same pair as single values or per row is **refused** by HeldKinds. AGENTS.md says it is refused.
3. `newMap([pair('a',1.5D),pair('b',2.0D)]) == newMap([pair('b',2.00D),pair('a',1.50D)])`:
   - `collections` needs both sides to have the same SQL type (:222); a Decimal literal's scale rides its type, so it returns null;
   - `plan` then answers `unplanned` and null (:114), which leaves the database's order-sensitive map `=`;
   - the result is likely false, where Pure says true.
4. At a compatibility door, `assert($m1 == $m2)` goes through the value form while `assertEquals($m1,$m2)` goes to the host's maps-by-entries. The two can disagree on entry order.
5. Shared nodes defeat the positive-condition mark; see C.

## B. The three promises
1. **"A value typed at run time carries its type": PARTLY.**
   - Truly carried only for mixed values: `SqlExpr.Tagged`/`TaggedList`, `kindedRows`.
   - Otherwise the type is recovered statically, either after inlining (`EqualityClasses.heldCrossing` behind upcasts) or from SQL column facts (HeldKinds, `RootComparison.kind`).
   - Where neither applies, the comparison is the database's value comparison, with no type.
   - `TypedRunTimeComparison` is a type-checker marker, not a carried type.
2. **"Asserts planned as plain comparisons": FALSE (PARTLY for whole sides at lite's door).**
   - `assertEquals` goes `AssertVerdicts` (1,925 lines; arms at :510-560) → `DatabaseJudge.databaseVerdict` (CanonRider, literal channel, :146-208) → `typedPair`.
   - It shares only the `SideEqualities.pair` leaf with `==`, and only for whole sides.
   - For single values, `==` uses Equalities and the assert uses `SideEqualities.values`.
   - Only `assert(a == b)` and `assertSize` (`typedOverValues`) really use the product's `==`.
3. **"The judges' separate comparison code is deleted": FALSE.**
   - VerdictSql went from 3,034 to 1,560 lines and JudgmentFacts from 513 to 324, but SideEqualities is new at 2,161, so the code moved.
   - Judge-only comparison code still in VerdictSql: `typedJson` (:420-475, JSON text via VARCHAR casts), `typedTolerance` (:511-539, the judge's own formula), `typedTdsEquivalent` and `toleranceCells` (:796-907), and the canon-text machinery (`Side`, `canonRows`, `gridRowCanons`, :44-63, 1293-1440).
   - Also remaining: CanonRider (191 lines), DatabaseJudge (1,088), CanonicalRenderSql (1,119, shared with InstanceEquality).
   - SideEqualities itself still carries the judge's record parts (`Held`, `Compared.alone`, :53-79).

## C. Tenets and invariants in the equality code

**Tenet 6 (behaviour by door after the type-checker):**
- Ruled: `Scalars.doorsRule` → `EditionTable.lowersAs`/`checks` (Scalars:2400).
- Extra:
  - `EditionTable.emptySideIsFalse` (EditionTable:58; Lowerer:405; EngineTable:267) is a third question to the door. I found the recommendation in the design's §5.3 but no recorded ruling.
  - `wholeSides`/`wholeAnswer` read "the door named a rule" as "no whole-side plan" (Lowerer:442,467). So the doors' list, map and table meaning is keyed on a numbers row.
  - `NUMBERS_OF_THREE_TYPES` also carries upstream's date-precision rule (NamedRule:122-124). Its method name, `Equalities.upstreamsPartialDateTime`, names who wants it, against NamedRule's own contract (:6-8).
- Older and not equality-specific: `legacyNullUnsafeEquals` (Lowerer:219,232,401); `Styled.forms()`; the planner's stamps (PlanEnv:187; ProductStatements:140; BodyPlanner:202; FramePlanner:132).
- `HostForms.exactNumbersByValue` (Equality.java ~169, ~301) is the host judge's own and acceptable under tenet 5.
- The type-checker tells the lowering "not the query's comparison" by **erasing the source position** (EqualityClasses:52-61, read at HeldKinds:90). A diagnostic field is being used as a semantic flag.
- The design's guard (every planner read of the edition listed; no table fact public on Lowerer) is not in `ArchitectureTest`.

**Tenets 1 and 2:**
- No row value is computed in Java.
- The constant answers in `atTheCall` (:185-193) are facts of the types, which the fold rule allows.
- `PureComparisons` answers without evaluating the sides (its javadoc says so).
- `RootComparison.kind`/`wireDecidedKinds` choose the comparison from the plan's output SQL types. That is a type decided downstream of the frontend, which strains invariant 1.

**Invariant 2 (the lowering infers no types):**
- `HeldKinds` validates types in the lowering (ruled by the user, but still a strain).
- `Equalities.mayHoldNoValue` (:82-91) and `aList` (:190-198) take multiplicity from SQL facts.
- The `positiveConditions` identity marks (Lowerer:346-385).

**Invariant 4 (no fallbacks):** each of these silently takes another path.
- `plan`'s null on no pair (:114);
- the lambda-bound-name check that leaves the database's list comparison (:171-187);
- lowerers made with no planner (Lowerer:442,455; recorded);
- `refuseTextOnlyLists` passes an operand with no type fact (SameValuesForm:61-69, against the design's §5.6) and covers only EQUAL/NOT_EQUAL on SQLite and MariaDB/MySQL;
- InstanceEquality's "generic rule" (:46-48,163-166);
- `runTimeComparison`'s "any other is its comparison" (Lowerer:~3796);
- enumerations left out of the both-empty rule (Equalities:69);
- `besideDecimals` "as it is" (:1210-1217).

**Invariants 3/3a (SQL decompositions chosen in the lowering):**
- `neverEqualButEvaluated` writes `'l'||CAST(x AS VARCHAR) = 'r'||CAST(y AS VARCHAR)` under COALESCE (Equalities:211-217). It casts an `Any` side to text, against the design's §5.7.
- `neverAmongButEvaluated` (:239), `sameOnlyWhereBothEmpty` (:226), `everyValueRead` (SideEqualities:186-205).
- `besideDecimals` compares decimal canonical text (:1201-1240); `instantOfText` (:1127); InstanceEquality's VARCHAR canon.
- The correct pattern already exists: `SAME_IN_ORDER` is rendered by the dialect (`SameValuesForm`).

## D. Patchwork versus design
- **`TypedRunTimeComparison.neverEqual` + the marked-text forms + `everyValueRead`:** debt. They compensate for "no type is carried". If the values carried a tag, the database would evaluate both sides and answer false by itself, and the marker and the text tricks would go.
- **Positive-condition marks + `guardedSides` shape-matching** (`NullSemantics.negate`:144-148): a sound idea in a fragile mechanism.
  - The inliner places one node at every read (UserCallInliner:458), so a node shared between a positive and a kept position is left unguarded.
  - Example: `filter(r | k::f($r.nick=='a'))` with `f(b) = $b || ($b == false)` drops the row with an empty nick, where Pure keeps it (by reading).
  - `guardedSides` also matches `neverEqualButEvaluated`'s COALESCE; it is right today only by coincidence.
  - Better: pass the position explicitly, or use one typed MIR node whose negation is structural.
- **`Compared.alone`:** debt from moving judge code into the product. The comparison should produce only `same`; the judge should add its own record.
- **`RootComparison.valueType`** (:235-239) reads a one-column many-row relation as a list. It compensates for the store resolver erasing the list/table distinction; fix that in the resolver.
- **`mayHoldNoValue` / `aList` by SQL fact:** the same root cause, multiplicity after store resolution.
- **`oneCarrier`, `besideDecimals`, `INSTANCE_TEXTS`, `instantOfText`, `jsonCarried`:** the core debt. SideEqualities compares egress-shaped side plans (texts, JSON documents) inherited from the judge, not typed values.
- **`OrderView`'s fallback** (:289-337) is a sound conservative default, not debt.

## E. Verdict and the short list
"One equality" is real for whole-side `==`/`hasSameElements` at lite's door (root and inside an `if`, and the database judge's `assertEquals` of the same sides). It is not real for single values, per-row lists, membership, instances inside expressions, or any compatibility door. Two product implementations remain.

1. **Two product equalities with different representations.**
   - Wrong: Tagged MIR vs kinded rows; canon text vs typed leaves; refuse vs false (disagreements 1 and 2 above).
   - Consequence: every rule change must be made twice.
   - Right design: one typed comparison IR over values that carry a kind tag, with semantics defined once; the planner decides only *where* it is evaluated (an expression or a statement).
   - Size: 1-2 weeks.
   - Before landing: state it honestly in the record and the register. The rebuild itself can follow.
2. **Door meaning keyed on the numbers row.**
   - Wrong: the doors lose the whole-side plan, maps and tables because `NUMBERS_OF_THREE_TYPES` exists, and that row also carries dates.
   - Hazard: retire the number row and the doors' list, map and table answers change silently.
   - Right design: split it into rows named by what they write, decide the whole-side plan independently, and fold `emptySideIsFalse` into the ruled lookup or get the user's ruling on it.
   - Size: about a day. **Fix before landing.**
3. **Identity-set positive marks.**
   - Wrong: a wrong answer with shared nodes (example in D).
   - Right design: an explicit polarity or position passed down.
   - Size: 1-2 days. **Fix before landing**: it is a wrong answer on ordinary code.
4. **Silent fallbacks to the database's own comparison** (C, invariant 4).
   - Consequence: the map example in A answers false where Pure says true.
   - Right design: refuse by name wherever the planner declines a whole side; make `SameValuesForm` refuse operands with no type fact and cover the null-safe forms and membership.
   - Size: hours. **Fix the wrong-answer paths before landing**; the no-planner lowerers can stay a recorded follow-up.
5. **The whole-side comparison works on egress carriers and SQL tricks chosen in the lowering.**
   - Right design: compare typed side values, with the "evaluated, false" form as a MIR node the dialect writes.
   - Size: days. A recorded follow-up, but the lines-deleted claim should be corrected now.

**Delete rather than fix:**
- VerdictSql.`Side`'s dead flags and `decimalValue` (VerdictSql:44-63; `decimalByValue` is never true).
- The duplicate `HeldKinds.readsAs` (call NumberLiterals) and `Lowerer.computes` (call the type-checker's `harmless`).
- The judge-record API (`Held`, `alone`) from `SideEqualities`' public surface.
- `TypedRunTimeComparison.neverEqual` and the three text-trick helpers, once types are carried.

**Could not determine by reading:**
- Whether disagreements 1-4 and the shared-node case actually reproduce (nothing was run).
- Whether the user ruled on `emptySideIsFalse`.
- Whether tests already pin `UpstreamTies` against `HeldKinds.READ`.
