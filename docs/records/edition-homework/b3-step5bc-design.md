B3 step 5b/5c design. I read /home/user/s1 (50b33970 plus the uncommitted 5a) and the 5a-2 draft in /home/user/s4 (Compiler.modelCheck, SpecCompiler.crossingsOf, and the deletion of TaggedLists.refuseCrossingNumbers). I edited nothing and ran nothing.

Path prefixes used below: M = /home/user/s1/core/src/main/java/com/legend/ and T = /home/user/s1/core/src/test/java/com/legend/.

The 2026-11-23 measurement can be re-run. Its data and tools are in /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/eq-measure/:
- results: runs/class_table.txt, runs/direction_table.txt, pq.table.txt
- harness: tools/run.py, tools/rewrite.py, tools/classify.py

## 1. Where Pure equality is implemented today

### 1a. The product: typer, folds and lowering

**Typer (5a).**
- M/compiler/spec/EqualityClasses.java:46-66 names the comparisons the rule reads; :109-125 decides which types are known at compile time. Static: Integer, Float, Decimal, String, Boolean, Byte, StrictDate, DateTime, StrictTime, each enumeration, Variant, a class. Run-time: Number, Date, LatestDate, Any, the Enum class, type variables, every GenericType (so Pair and List too), functions, relations.
- Overloads (5a diff, about :137-149) wraps a crossing in TypedCrossTypeComparison only while Typer.markingCrossings is on, which means the body a door receives.
- Function bodies (Typer.inFunctionScope), normalized inlines, islands, execute frames and mapping operations are typed unmarked. They get the database's comparison, for every audience.

**Compile-time folds over spelled operands.**
- LiteralUnroll.java:390-426 `equalityFold`: element references by path, TDSNull carriers, same-class literals by Java `equals`.
- LiteralUnroll.java:434 `neverEqual`: an enumeration against another spelled scalar (S169). The contains fold is at :472-485, the `in` fold at :596.
- StaticFold.java:877 `staticEquals`.

**Lowering: `equal` and `eq`** (lowering/Scalars.java:115-191).
- A literal `[]` becomes IS NULL, or tagged size 0; `[] == []` is true (:128-145).
- Tagged values go to `taggedEquality` (:3822-3883).
- Year and year-month literals are equal only at the same precision (`partialPrecision`, :3734; the rule at :153-173).
- Then CastPolicy.java:277, the Variant JSON alignment, EnumSourceValues (an enumeration column against a String compares the stored value), and NullSemantics.equalNullArms.
- Everything else is the database's `=`. Consequences:
  - `1 = 1.0` is true.
  - A day equals its midnight on DuckDB and H2, not on SQLite.
  - Date-time literals compare as their written text, so `T10:00:00 != T10:00:00.000`.

**Lowering: orderings and negation.**
- Orderings :199-216 pad partial dates (`dateArg`) and add NullSemantics' optional guards.
- `not(...)` :284-297 goes to NullSemantics.negate :203-221.
- NullSemantics.java:107-150 makes equality null-safe only for a NULL literal, two optional columns, or an optional parameter (never for an enumeration). `verbatim` :101 keeps a mapping's own conditions as plain `=`.

**Lowering: membership.**
- `contains` :2236-2355:
  - a Variant on one side folds false;
  - a tagged list goes to TaggedLists.contains;
  - an Any list compares as JSON through TO_VARIANT (:2338);
  - a class against a primitive folds false;
  - otherwise the database's membership.
- `in` :2670-2754: `kindMismatch` folds false (:2688), Any goes to the variant carrier (:2699), then tagged, relation, or an IN list.
- `indexOf` :1535-1567 is LIST_POSITION, the database's `=`.
- `removeDuplicates` :1679-1742 and `distinct` :1766-1778 are the database's DISTINCT; a tagged list deduplicates by `sameElement`.

**Instances.**
- Lowerer.java:4153-4155 folds a statically disjoint pair; :4330-4342 hands claimed shapes to InstanceEquality and refuses by name what it cannot lower.
- lowering/InstanceEquality.java:127-176:
  - A keyed class compares by key canon *text* (CanonicalRenderSql.instanceCanon :707, kind-tagged leaf texts). A Decimal key's scale and a date-time key's written digits are therefore part of the value.
  - A keyless class compares by `__id`, on the identity lane only.
  - Two different static classes are false only on the identity lane.
- The rest of InstanceEquality:
  - contains/in :178-222, removeDuplicates :256, isDistinct :280, indexOf :298;
  - collections in order :328-351;
  - `staticallyDisjoint` :413-442: Variant, two enumerations, an enumeration against a non-String, an instance literal against a primitive.

**Mixed values (track L).**
- MixedEncoding.java:112-239 `taggedList` makes one channel per (kind, SQL slot): Integer BIGINT, Float DOUBLE, Decimal its own DECIMAL(p,s), String, Boolean, StrictDate DATE, DateTime TIMESTAMP. A DateTime literal with written subsecond digits gets the kind "DateTime(n)" (:157). A partial date rides as its text; an enumeration as its name.
- The rank (:287-341) is a struct {g, n DOUBLE, d TIMESTAMP, s, p}, where p is the written precision. There is no type tie-break, so 1 and 1.0 tie.
- `kindOf` :397-435.
- TaggedLists.java:
  - `sameListByKinds` :68-96 is per slot, so a Decimal at another scale or a DateTime(n) is another slot.
  - `refuseCrossingNumbers` :107-121 is deleted by 5a-2.
  - `sameElement` :381, :397, :465; `sameList` :417 uses LIST_FILTER.
  - `contains` :491-515: `channelOf` :385 finds only the first channel of a kind.
  - `sort` :226 and `refuseInexactRanks` :258.

**Variants (shared by the product and both judges).** compiler/spec/typed/VariantEquality.java: `between` :158 (S401), `refuseAmong` :229, `dateKinds` :56.

### 1b. The database judge

**testplan/DatabaseJudge.java.**
- `databaseVerdict` :67-206: the kind gate :83-90, the riders, grids :137-166 (`typedGridPair`), then `JudgmentFacts.typedPair` :191-205, else UNJUDGED by name.
- `databaseContains` :495.
- `databaseCondition` :603-650 plans `assert(cond)`: the product lowers the condition and `typedCondition` reads its one Boolean. This is the shape 5c generalizes.

**testplan/JudgmentFacts.java.** `TypedPair`/`typedPair` :31-180 has these forms:
- VALUES (`typedClass`);
- DATES (`dateKind` :239: a literal's kind, or the column's SQL type);
- EMPTY_ONLY (a Nil side; a Variant against other values; two number kinds, equal only when both are empty, S127);
- TAGGED (a tagged plan, a written Date's text, a JSON carrier);
- INSTANCES, COLLECTIONS, INSTANCE_TEXTS.

`refuseMixedDays` is at :272.

**lowering/VerdictSql.java** (3,059 lines; its pin of 2,286 is frozen).
- `typedEquals` :96-148: each side becomes one typed list, compared by SAME_IN_ORDER or SAME_IN_ANY_ORDER. DuckDb.java:613 renders these as lists; H2 and SQLite use the portable row form (sql/dialect/SameValuesForm.java:40). Emptiness is read from the rows (`noRows` :1002).
- `typedCollections` :158: nested lists in order; maps as key-sorted entries (:196).
- `typedTagged` :245, `kindsOf` :587 and `kindedRows` :781-930: each value is a row {k, v0..vn}.
  - A Decimal is compared by value.
  - A DateTime of any written precision is one kind.
  - A written Date's kind is read from its text (:685-751).
  - A JSON value is compared by its JSON kind (:646-672).
- `instants` :954-998 reads date-times as TIMESTAMP; an hour literal is read at its minute.
- `typedInstances` :295 and `pairedLeaves`/`leaves`/`leafRows` :334-583 compare typed leaves: `__type`, then the keys (nested keyed leaves in place), or `__id` for a keyless class.
- Membership: `typedContains`, `typedContainsTagged`, `typedContainsInstances` (:360, :1494-1573). Also `typedSubset` :1626 and `typedPerElement` :1659.
- `nanos` :1774-1845: DuckDB's per-assert nanosecond reading (S399's interim clause).
- `typedInstanceTexts` :1241, `typedJson` :1399, `typedTdsEquivalent` :2057, `typedGridPair` :2194.

By my count of non-blank, non-comment lines, about 1,400 of the 2,286 pinned lines are this comparison layer.

### 1c. The host judge (kept, as tenet 5's second implementation)

testexec/Equality.java `same()` :139-212:
- a Variant or an enumeration value is equal only to the same enumeration's same value (`Kinded`, :147); a map is never a list (:150);
- the kind comes from a numeric declaration, else from the carrier (:122);
- different kinds are never equal (:165, S127);
- Integer exact, Decimal by `compareTo` (S139), Float exact, String and Boolean exact;
- dates by LiteHostForms.java:22-34 (kind below the hour, instant floor; S399).

Collections:
- `ordered` :222.
- `sameElements` :239-245 sorts by `typeRank`/`withinRank` :288-319. There is no type tie-break, so `[1, 1.0]` against `[1.0, 1]` fails.
- `rowMultiset` :260, `pureJson` :326.

testexec/HostJudgments.java:158-214 `restrictOne`: the classifier first, then the keys; a nested value by its own `__type`. Grids go through TdsCompare.

## 2. The gap: what `assert(e == a)` lacks to answer as the judges do

The measurement's categories (class_table.txt; number of bodies affected on any database):

| Category | Bodies | Category | Bodies |
|---|---|---|---|
| number kinds | 52 | grids | 11 |
| H2 has no list encoding | 42 (67 on H2) | assert planning | 8 |
| instances and keys | 32 | cross-type primitives | 8 |
| dates: written precision vs instant | 29 | nanoseconds | 7 |
| database judge UNJUDGED | 21 | nulls and empties | 6 |
| H2 mixed-list SQL error | 17 | enumeration vs String | 6 |
| `is` is not a native | 15 | SQLite Decimal arithmetic | 4 |
| day vs date-time | 15 | Variants | 4 |
| mixed-day list refused | 14 | hour literal | 3 |
| DuckDB list shapes | 13 | mixed-list side refused | 3 |

5a turns the static share of these into compile errors in both forms: the static part of number kinds, day vs date-time, enumeration vs String, cross-type primitives, and a Variant against a value. That needs re-measuring. What remains, by mechanism (VerdictSql has it, the product does not):

- **G1 — one value within one type.**
  - `%2020-01-01T10:00:00 == ...T10:00:00.000` and `%2020-01-01T10 == ...T10:00` are false today.
  - `^KD(d=1.0D) == ^KD(d=1.00D)` is false.
  - A mixed `[1.0D]` against `[1.00D]` is false (two slots).
  - An hour literal errors on DuckDB.
- **G2 — run-time kinds.**
  - A tagged value against a value of no static kind is refused (TaggedLists :400, :494).
  - Any lists ride as JSON (Scalars :2338, :2699), where a Decimal or a date cannot be told apart.
  - 1 and 1.0 tie in the order, so exactly-one-of `<`, `==`, `>` fails; the host's `sorted` has the same flaw.
- **G3 — lists.**
  - H2 lacks LIST_POSITION, LIST_FILTER, LIST_REDUCE and LIST_SORT.
  - `[1, 2.5] == [1.0, 2.5]` is a syntax error on H2.
  - A computed `[]` is not a literal `[]`: `[] == [1, 2]->filter(x | $x > 5)` is false on DuckDB and SQLite.
  - There is no multiset form; maps and nested lists compare as a struct `=`.
- **G4 — instances.**
  - They compare as canon text, not typed leaves.
  - On the value lane the shape is unclaimed and refused.
  - The class is decided only on the identity lane.
  - Keyless `$xs->at(0) == $xs->at(0)` is false.
  - Generic arguments are not typed: `pair(1, 1D) == pair(1, 1)` is true.
- **G5 — grids.** `==` over relations ignores column names.
- **G6 — `is` and `forAll`.** `is` is not a product native (assertIs and assertIsNot bodies need it); `assert(cond, msg)` inside `forAll` is not planned.
- **G7 — DuckDB nanoseconds** (`VerdictSql.nanos`).
- **G8 — not equality; these stay judge members.** JSON (`typedJson`/`pureJson`), instance documents, `assertTdsEquivalent`. Tolerance is already a Pure body (VerdictQueries.assertAsPredicate :468-490).

## 3. The slices (each lands green alone; cheapest and safest first)

Every slice runs the cadence:
- the fast check (guards plus the touched packages);
- one full round: PCT channel A LITE, channel B DuckDB and SQLite under LITE (both judges) and under compat, H2's relation suite, both corpora on the host judge, lean SQL, the edge net;
- a review run beside the next slice;
- the records and register rows.

The lowering files have no EVICT_SIZE pins. Their growth (+1.4k to 2.3k lines, per the plan) is bounded only by CodeShapeGuardrailTest's per-method limit.

### 5b-0 — measure (no code, on the 5a-2 head)
- Re-run the harness on DuckDB, H2 and SQLite and reclassify the results by G1–G8.
- Census every comparison whose side `EqualityClasses.of` leaves to run time, split two ways:
  - by carrier: Tagged/TaggedList, JSON, or plain SQL (a Number or Date column, or a function's result);
  - by place: the received body or a function body;
  - across PCT, both corpora, and core's tests.
- This decides Q2 and Q3 and gives each slice its expected moves.

### 5b-1 — one value within one type (S139 and S399's product line; closes G1)
- **Changes:**
  - Scalars' `equal` reads date-time operands as instants, an hour at its minute (VerdictSql.instants' rule). `partialPrecision` stays for year and year-month.
  - MixedEncoding drops "DateTime(n)" from the kind (:157, :425, :620). The written digits stay only as a print fact for CanonicalRenderSql.java:1046, until B2a's nine-digit print lets them go.
  - The rank's p becomes the date kind (Year < YearMonth < StrictDate < DateTime), not the written precision.
  - `sameListByKinds` compares by kind: Decimal channels cast to one DECIMAL that holds both scales.
  - TaggedLists.contains checks every channel of the needle's kind.
  - `instanceCanon`: a Decimal leaf trimmed (VerdictSql.decimalValue's rule), a date-time leaf at its instant.
- **Tests:** TaggedListTest and InstanceIdentityTest pins flip; DecimalKindsTest:142 and EqualityWorldsConformanceTest:127 stay.
- **Records:** S399's product line and S139 amended.
- **Expected moves:** PCT compat about 0 (S399's row read no such pair in legend-pure's PCT; measure the scale cases). The corpus moves none or toward passing: these are same-type comparisons, and legend-engine's answer is the database's instant or value.
- **Risks:** SQLite's date-time texts; DuckDB nanoseconds (refused per S181).

### 5b-2 — run-time kinds complete (closes G2)
- **Changes:**
  - TaggedLists.sameElement and contains give a value of no static kind its kind from its SQL type: BIGINT Integer, DOUBLE Float, DECIMAL Decimal, DATE StrictDate, TIMESTAMP DateTime, VARCHAR String, BOOLEAN Boolean.
  - A JSON-carried Any compares by its JSON kind (String, Integer, Float, Boolean). Where it may hold a Decimal or a date, it is refused by name (design item 3: no JSON boxing).
  - Scalars' Any arms (:2338, :2699) read a literal Any list as a tagged list, not TO_VARIANT.
  - The rank gets a type tie-break after n and d (Q4). testexec/Equality.sorted :288 gets the same tie-break (tenet 5).
- **Deletes:** the "kind only its runtime value names: not lowered" refusals (TaggedLists :400, :494).
- **New guard:** design item 9's property test — exactly one of `<`, `==`, `>` holds for every comparable pair — on DuckDB, SQLite and H2; the warm lanes once.
- **Pin:** Equality.java's pin of 319 grows by about 3. That needs your word, as on 2026-11-23.
- **Expected moves:**
  - PCT LITE: mixed-list refusals become answers (TaggedListTest :279-291).
  - PCT compat: moves toward Pure's run-time type rule.
  - Corpus: none expected (tagged lists come from literal mixes) — measure both lanes.
- **Risks:** DOUBLE ties past 2^53 (`refuseInexactRanks` stays); lean-SQL text pins.

### 5b-3 — generics and classes (closes G4's typing)
- `EqualityClasses.of(GenericType)` reads the arguments of Pair, List and Map:
  - a crossing where an argument pair crosses, so `pair(1, 1.0) == pair(1, 1)` is the 5a marker, and both doors already answer markers;
  - run time where an argument is run-time.
- InstanceEquality (:151-158, :191-197) compares the run-time class first, on every lane: `__type` where the struct carries it, refused by name where it does not.
- **Expected moves:** PCT compat — legend-pure's door answers false (rows shaped like 5a's ledger rows). Corpus: none (legend-engine's door unwraps markers).
- **Risk:** a polymorphic store row with no `__type`.

### 5b-4 — only if Q2 says refuse
In the received body, the typer marks a comparison with a run-time-typed side as a node, the same mechanism as 5a's marker:
- legend-engine's door unwraps it; legend-pure's door keeps it;
- the lowering refuses it (TYPE) where the two SQL kinds differ.

New EdgeOnlyMembersTest entries come with the node.

### 5c-0 — measure
Run the harness with `assert(<Pure body>)` on the 5b head, both judges, three databases. Three conditions must hold:
1. No body whose database-judge verdict today is PASS or FAIL becomes REFUSED or ERR, except ones named.
2. Host versus database-judge disagreements are at most the measured 3, 1 and 3 (DuckDB, H2, SQLite), each named.
3. Every failure record is byte-identical.

### 5c-1 — the product's `==` complete, in one owner (closes G3–G6)
- **New M/lowering/Equalities.java** takes:
  - Scalars :115-191, the equality decisions inside the contains/in/indexOf/dedup arms, NullSemantics.equalNullArms, and InstanceEquality;
  - Scalars keeps one dispatch line each (this also keeps the per-method limit).
- **Lists:** two lists, or a scalar and a list, compare as SAME_IN_ORDER over their rows. The SqlFn and SameValuesForm already render on every dialect, H2 included. Emptiness is read from the rows (`noRows`' rule).
- **H2:** row forms for indexOf, contains, dedup and sort.
- **Instances:** typed leaves, moved from VerdictSql :334-583, replace `instanceCanon`'s text; keyless by `__id` where it is tracked.
- **Grids:** `==` by column name and kind, rows in order.
- **New:** an `is` native; `assert(cond, msg)` inside `forAll` planned (AssertVerdicts.quantified).
- **Deletes:** the canon-text equality and the unclaimed refusal at Lowerer :4330-4342.
- **New guard:** an ArchitectureTest rule that only Equalities (and VerdictSql, until 5c-2) mints SAME_IN_*.
- **Expected moves:** product answers only. H2's 67 list bodies answer; core pins on `[] == computed []` and on grids flip.
- **Engine door:** the planned refusal of list `==` (pureToSQLQuery processEquals :8363-8445) is measured first and lands only if neutral.
- **Risks:** H2 SQL size (lean SQL); UNTRACKED `__id` raises (CanonicalRenderSql :671).

### 5c-2 — the asserts as their Pure bodies; VerdictSql's comparison layer deleted (this is the slice that deletes it)
- **Bodies:**
  - assertEquals → `e == a`; assertNotEquals → `!=`;
  - assertSameElements → `sort(e) == sort(a)`, with the multiset rows for values that have no order;
  - assertContains → `contains`; assertEq → `eq`; assertIs/IsNot → `is`;
  - assertEqWithinTolerance → `abs(e - a) <= d`.
- **How:** VerdictQueries.assertAsPredicate :468 is generalized to the root forms and planned through databaseCondition :603.
  - Each side is bound once, as a CTE (readOnce's lesson, VerdictSql :212-222), so the values compared are the values recorded.
  - The record keeps both sides: valueRows, listed, listValue and typedRow stay as the record layer.
  - The record family is chosen in AssertVerdicts from the sides' carriers — what is left of TypedPair.family :74.
- **Deletes:**
  - VerdictSql :96-1040: everything there except typedCondition, typedSize, typedEmpty and noRows.
  - VerdictSql's membership and per-element members: typedContains, typedContainsTagged, typedContainsInstances, typedSubset, typedPerElement.
  - VerdictSql: Side, decimalValue and canonOf, if sideRows goes.
  - About −1,100 to −1,300 lines from VerdictSql.
  - JudgmentFacts :31-180 plus `dateKind` and its helpers (about −130).
  - DatabaseJudge's kind gate :83-90, the dispatch :187-205, `gridColumnPair` :211, and the forAll-contains typed arm :606-632 (about −170).
- **Pins** (each a dated shrink):
  - EVICT_SIZE: VerdictSql 2,286 → measured (about 1,100); JudgmentFacts 355 → about 220; DatabaseJudge 838 → about 650; AssertVerdicts 1,335, which likely shrinks.
  - TestPlanJson.VERSION 42 → 43.
  - A new ArchitectureTest rule: VerdictSql mints no comparison (SAME_IN_*, NULL_SAFE_EQUAL, IS_DISTINCT_FROM).
  - AGENTS.md's equality section's `[CONVENTION until B3 step 3]` amended.
- **Tests that move:** AssertBarTest (:116-121, :185-190, :236-238, :1033-1040), BodyRouterParityTest, JudgesOnPlansTest, VerdictRowOrderTest, LiteralChannelTest, NumberKindsEdgeTest :52-53, EqualityCompatEdgeTest :44-45.
- **Expected moves:**
  - PCT LITE (both judges): UNJUDGED becomes judged (21 bodies); disagreements fall from 47/51/54 as written to about 3/1/3.
  - Compat PCT and both corpora: none (the host judge alone, rule 4); byte-identical lists required.
  - The warm lanes run.
- **Risks:** a side computed twice (raise order, `now()`) — mitigated by the single binding.

**What must be true before 5c-2:**
1. 5c-0's three conditions hold.
2. 5c-1 has landed and covers every shape the layer types: lists in order and as multisets, empties, typed instance leaves, maps and nested lists, grids, tagged and JSON kinds.
3. 5b-1 and 5b-2 have landed; otherwise S139, S399 and S127 are lost.
4. The host's tie-break matches and both-judge agreement is green on PCT LITE and core.
5. Q5 is ruled.

### 5c-3 — the rest
- `VerdictSql.nanos` moves into the product's `==` (per Q5).
- `refuseMixedDays` :272 is deleted where tagged dates already keep days and date-times apart; this closes S399's `let`-bound gap.
- Tolerance becomes its Pure body; SQLite's Decimal arithmetic stays refused, recorded.
- `typedGridPair` is replaced by 5c-1's grid `==`; `typedTdsEquivalent` stays a member.
- Pins shrink again.

## 4. Open questions (yours to rule)

**Q1. Asserts across known types.** Is `assertEquals(1, 1.0)`, and its assertNotEquals, assertSameElements and assertContains forms, a compile error like `==`?
- *Recommendation: yes.* Add the assert ids to EqualityClasses. legend-pure's door answers false; legend-engine's door unwraps the marker, so the host's S127 false holds and nothing moves.

**Q2. A Number- or Date-declared value whose SQL type fixes its kind** (for example a Number property over a DOUBLE column). The strict rule, item 1, says this is a compile error. Doing that needs 5b-4's node.
- *Recommendation:* measure in 5b-0. Build 5b-4 only if lite's own code or PCT has cases; otherwise record a register row.

**Q3. The function-body residue.** 5a leaves comparisons inside function bodies, execute frames and mapping operations unmarked, so they get the database's comparison for everyone at run time. 5b and 5c inherit this.
- *Recommendation:* keep it through 5c as a recorded register row. Revisit at step 6, when the harness composes the doors and can rewrite after inlining. At 5a, marking these broke about 32 corpus tests a lane.

**Q4. "Then by type" order for equal comparables.**
- *Recommendation:* Integer < Float < Decimal, and StrictDate < DateTime (as O2's D1 already orders days first).

**Q5. DuckDB's nanosecond clause** (S399's interim clause, "to be asked again when the product's == decides every assert").
- *Recommendation:* the product's `==` reads a date-time with a written digit past the microsecond as TIMESTAMP_NS (1677 to 2262; outside that range, refused by name), and every other at microsecond fidelity. This is a fact of the plan.

### Critical files for implementation
- /home/user/s1/core/src/main/java/com/legend/lowering/Scalars.java
- /home/user/s1/core/src/main/java/com/legend/lowering/TaggedLists.java
- /home/user/s1/core/src/main/java/com/legend/lowering/InstanceEquality.java
- /home/user/s1/core/src/main/java/com/legend/lowering/VerdictSql.java
- /home/user/s1/core/src/main/java/com/legend/testplan/DatabaseJudge.java

Close behind: /home/user/s1/core/src/main/java/com/legend/testplan/JudgmentFacts.java, /home/user/s1/core/src/main/java/com/legend/lowering/MixedEncoding.java, /home/user/s1/core/src/main/java/com/legend/compiler/spec/EqualityClasses.java, /home/user/s1/core/src/main/java/com/legend/testexec/Equality.java, and /home/user/s1/core/src/test/java/com/legend/JavaEvalLedgerTest.java (pins at :314, :603-608, :1069).