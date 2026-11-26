# Step 6 of the edition program, "the rest of the one equality": implementation sheet

Read-only research on the worktree `/home/user/ll-edit`, branch `claude/edit-ahead`, at `46f943fe`. Nothing was edited
or run there; no Bazel. Paths are relative to the repository; `M/` means `core/src/main/java/com/legend/`, `T/` means
`core/src/test/java/com/legend/`.

**How lines are counted.** Two numbers per file: *raw* (every line, `wc -l`), and *code*, which is
`JavaEvalLedgerTest`'s own rule (`T/JavaEvalLedgerTest.java:2497-2500`): block and line comments stripped, blank lines
dropped. The ledger's pins are code counts. Line ranges were counted with the same rule over the range.

**The step, as planned** (`docs/records/edition-plan-to-finish-stage-b-2026-11-25.md` §5 row 6): "A value whose type
is known only at run time carries its type; asserts are planned as plain comparisons; the judges' separate comparison
code is deleted. Done when: both judges agree; about 2,500 lines gone." It is the earlier program's B3 step 3 (later
renumbered B3 step 5, sub-slices 5b and 5c; `docs/records/compat-b3-progress.md:157`, `:500-501`).

**The one thing to know first.** A detailed design for this work already exists:
`docs/records/edition-homework/b3-step5bc-design.md` (313 lines, written 2026-11-25 against another tree, `/home/user/s1`).
Its map of the code (§1), its gap list G1-G8 (§2) and its slices 5b-0 to 5c-3 (§3) are still the best plan. This sheet
re-reads the code at `46f943fe`, corrects what has changed since, and adds what that design does not cover (the
comparison setting of step 2b, the edition rows, step 4's dependency, the guards). Where the records and the code
disagree, the code is stated and the record named.

Records that disagree with the code:
- The 5b/5c design says `TaggedLists.refuseCrossingNumbers` "is deleted by 5a-2". It is not: it is at
  `M/lowering/TaggedLists.java:107-121`, called from `:80`, `:472`, `:501`. The 5a-2 draft (the function-body path) was
  withdrawn on 2026-11-25 (`compat-b3-progress.md:542-544`); `Compiler.modelCheck` and `SpecCompiler.crossingsOf` do not
  exist.
- Register S127 names `DatabaseJudge.crossNumberKinds`. No such method exists. Two numbers of different known kinds are
  now the typed pair's `VALUES` form with two kinds (`M/testplan/JudgmentFacts.java:151-155`), and
  `VerdictSql.typedEquals` turns that into "the same only where both are empty" (`M/lowering/VerdictSql.java:104`,
  `:119-120`).
- The stage-B plan's design says a comparison across classes is "a constant false with no SQL"
  (`compat-stage-b-plan-2026-11-24.md:131-133`). The ruling that followed (option A, kept 2026-11-25) makes it a compile
  error in lite. The doors' rewrites in that plan became comparison settings in the type-checker in step 2b (below, §3).

---

## 1. How an assert is planned and judged today

### 1.1 The road, end to end

1. **The door.** `TestExecution.planBody` calls `testplan/TestPlanner.plan` (`M/testplan/TestPlanner.java:34`).
   - With judge mode `BOTH`, the body is planned twice, each judge alone, into one `TestPlan.Isolated`
     (`planIsolated`, `:96-115`).
   - A compatibility style refuses any judge but the host's (`refuseTheDatabaseJudgeInCompatibility`, `:73-80`;
     rule 4 of the rules of record).
2. **The lift and the refusals** (`lifted`, `:247-263`): `AssertLift.lift`, then three product refusals:
   `RelationCountAsserts.refuse` (S137), `OrderedAsserts.refuse` (B3 step 4), `CrossTypeComparisons.refuse` (the marker
   a comparison across types leaves; `M/compiler/spec/CrossTypeComparisons.java:22-29`).
3. **The class.** `BodyClass.of` (`M/testplan/BodyClass.java`, 1,316 raw lines). Then one judge's planner
   (`TestPlanner.java:176-182`):
   - `HostAsserts.plan` (`M/testplan/HostAsserts.java:95-114`);
   - or `DatabaseAsserts.plan` (`M/testplan/DatabaseAsserts.java:76-100`).
4. **The router.** Both planners go through `testplan/AssertVerdicts`: `planHost` (`:771`) for the host and the
   database gate (`planDatabase` `:67`, `databaseGate` `:115`); `adjudicate` (`:350-716`) classifies each assert.
   Each judge answers through its `VerdictArm` (`M/testplan/VerdictArm.java`, 117 raw lines), chosen by
   `AssertVerdicts.arm` (`:1580`). The arm methods are the assert families the planner knows: `tdsEquivalent`, `size`,
   `contains`, `tolerance`, `condition`, `empty`, `eq`, `instanceOf`, `is`, `sameElements`, `quantified`,
   `quantifiedVector`, `perElement`, `gridPair`, `equals`, `jsonStringsEqual`, `rendered`, `staticallyDecided`
   (`VerdictArm.java:24-116`).
5. **The plan.** Assembled by `TestDoor.planned` and carried as data: `testexecutionplan/TestPlan`, its JSON
   `TestPlanJson` (format `VERSION = 42`, `M/testexecutionplan/TestPlanJson.java:91`), the host facts' codec `HostJson`
   (1,239 raw lines).
6. **The run.** `testexec/TestRunner`.
   - The host judge's verdicts run through `HostJudgments` and `AssertRuns`.
   - The database judge's verdict rows are read by `DatabaseVerdicts` and `RootLoops`.
   - Every failure is a record built by `AssertRecords`, whichever judge found it.
   - Under `BOTH`, `IsolatedRun.run` (`M/testexec/IsolatedRun.java:99`) runs both plans and compares them: the count of
     asserts, their order, each verdict, each message and the ending. A difference fails the body by name
     (`JudgesDisagree`, `:201`).

**The assert natives** (`M/builtin/Pure.java:2677-2696`, plus `AT_RELATION_ASSERT_TDS_EQUIVALENT` and
`AT_LEGEND_LITE_ASSERT_RAISE`):
- `assert`, `assertFalse`;
- `assertEquals`, `assertNotEquals`, `assertSameElements`;
- `assertEq`, `assertNotEq`, `assertIs`, `assertIsNot`;
- `assertContains`, `assertNotContains`;
- `assertSize`, `assertNotSize`, `assertEmpty`, `assertNotEmpty`;
- `assertInstanceOf`, `assertEqWithinTolerance`, `assertJsonStringsEqual`, `assertTdsEquivalent`;
- `assertError`, `fail`.

**The failure records:** `M/error/AssertKind.java:42-97`. **The database judge's row families:** `RootLoop.Family`
(`M/testexecutionplan/RootLoop.java:168-236`, 26 families, 8 of them for tagged values or documents).

### 1.2 The host judge (the Java comparison; tenet 5's independent second implementation)

**Planning.** These read the facts the run will need, at planning: a side's kind class, the key trees of an instance
pair, its static facts (`JudgmentFacts.sideFacts`, `keys`, `keyTree`; `M/testplan/JudgmentFacts.java:300-513`).
- `M/testplan/HostJudge.java`: 353 raw lines, 243 code.
- `M/testplan/HostSides.java`: 166 raw lines.
- `M/testplan/HostSideBatching.java`: 195 raw lines.

The judgment is a `HostJudgment` variant (`M/testexecutionplan/HostJudgment.java:25-313`): `Equals`,
`SameElements`, `Eq`, `Contains`, `Is`, `Rows`, `GridPair`, `TdsEquivalent`, `Json`, `Rendered`, `Size`, `Empty`,
`Condition`, `Tolerance`, `InstanceOf`, `AssertError`, and others.

**Running.** `M/testexec/HostJudgments.java` (886 raw, 649 code) reads each side's typed rows and decides through:
- **`M/testexec/Equality.java`** (442 raw, 319 code). `same()` (`:139-212`) is the rulebook:
  - a Variant or an enumeration value equals only the same enumeration's same value (`:147-149`);
  - a map is never a list (`:150-153`);
  - the kind is the numeric declaration's, else the carrier's (`effectiveKind`, `:122-130`; `carrierKind`, `:84-108`);
  - different kinds are never equal (`:165-170`, S127);
  - an Integer is compared exactly, a Decimal by `compareTo` (S139), a Float exactly (S15 deleted), a String and a
    Boolean exactly;
  - dates by `LiteHostForms.STRICT.equalDates` (`M/testexec/LiteHostForms.java:22-34`): the kind below the hour, then
    the instant (S399).

  Its collection rules:
  - `ordered` (`:222`);
  - `sameElements` (`:239-245`): sort, then ordered; instances and maps as a multiset;
  - `sorted`/`typeRank`/`withinRank` (`:288-319`): numbers by `BigDecimal` value, **with no type tie-break**;
  - `rowMultiset` (`:260`).
- `M/testexec/PureAsserts.java` (160 raw): the decisions of the assert family.
- `M/testexec/TdsCompare.java` (179 raw): headers and grid row order.
- Instances: `HostJudgments.restrictOne` (the classifier, then the keys).
- **The measuring instruments**, which are not a verdict path: `M/testexec/CanonicalForm.java` (122 raw, 60 code),
  the host's reference render of the byte channel, and `M/testexec/CanonicalDivergence.java` (488 raw, 289 code), the
  census that compares it with the host's verdict. They are called from `HostJudgments.java:253`, `:567`, `:627`, `:807`
  and `AssertRuns.java:172`.

### 1.3 The database judge (its own comparison in SQL)

**Planning.** `M/testplan/DatabaseJudge.java` (1,098 raw, 838 code).
- **`databaseVerdict`** (`:67-210`) handles `equals`, `sameElements`, `eq`, `is`, `gridPair` and `rendered`:
  - a kind gate (`:83-90`): two sides of different non-primitive kinds are "UNJUDGED";
  - the riders (`lowering/CanonRider`, canon texts riding the side's query);
  - grids (`typedGridPair`);
  - then `JudgmentFacts.typedPair` (`M/testplan/JudgmentFacts.java:108-180`), which chooses a form (`:40`): `VALUES`,
    `DATES`, `EMPTY_ONLY`, `TAGGED`, `INSTANCES`, `COLLECTIONS`, `INSTANCE_TEXTS`;
  - `TypedPair.row` (`:88-103`) hands each form to its `VerdictSql` member.
- `databaseContains` (`:495-562`), `databaseSize`/`databaseEmpty` (`:396-494`), `databaseTolerance` (`:652-699`),
  `databaseTdsEquivalent` (`:563-602`), `jsonStringsEqual` (`:1021`).
- **`databaseCondition`** (`:603-650`) plans `assert(cond)`: the product lowers the Boolean and `VerdictSql.typedCondition`
  reads it. **This is the "plain comparison" shape the step generalizes.**

**The statements.** `M/lowering/VerdictSql.java` (3,059 raw, 2,286 code; pinned at 2,286 in
`T/JavaEvalLedgerTest.java:607`, "frozen" by the stage-B plan):
- `typedEquals` (`:96-148`): each side becomes one typed list, compared by `SqlFn.SAME_IN_ORDER` or
  `SAME_IN_ANY_ORDER`. Decimals are compared by value (`decimalValue`/`canonOf`, `:53-64`), date-times as instants
  (`instants`, `:954-1001`), and two kinds compare as "both empty" only (`:104`, `:119`).
- `typedCollections` (`:158`): nested lists, and maps as entries sorted by key (`:196`).
- `typedTagged` (`:245`) with `kindsOf`/`kindedRows` (`:587-937`): each value is a row `{k, v0..vn}`. A Decimal is
  compared by value, a DateTime of any written precision is one kind, a written Date's kind is read from its text
  (`:675-751`), and a JSON value is compared by its JSON kind (`:651-672`).
- `typedInstances` (`:295`), with `pairedLeaves`/`leaves`/`leafRows` (`:334-586`): typed leaves are compared, first
  `__type`, then the keys, or `__id` for a keyless class.
- Membership and the rest:
  - `typedContainsInstances` (`:360`), `typedContainsTagged` (`:1494`), `typedContains` (`:1537`);
  - `typedSubset` (`:1626`), `typedPerElement` (`:1659`), `typedAllOf` (`:1604`), `typedTolerance` (`:1574`);
  - `nanos` (`:1774-1850`): DuckDB's per-assert nanosecond reading;
  - `typedGridPair` (`:2194`), `typedTdsEquivalent` (`:2057`), `typedJson` (`:1399`), `typedInstanceTexts` (`:1241`).
- The record layer, which stays: `valueRows`, `listed`, `listValue`, `typedRow` (`:1717-1773`, `:1851-1966`).

**Rendering.** `SameValuesForm` (`M/sql/dialect/SameValuesForm.java`, 138 raw) is the portable row form, used on H2 and
SQLite. DuckDB writes its own list form. **Only VerdictSql mints `SAME_IN_*`** (grep).

**Batching.** `M/testplan/VerdictBatch.java` (822 raw) fuses one statement per segment.

**Running.** `M/testexec/DatabaseVerdicts.java` (43 raw), `RootLoops.java` (377 raw), and `AssertRecords.java` (407 raw)
builds the records by family (`:109-128`).

### 1.4 The same rule, three mechanisms

| Rule | Host judge (Java) | Database judge (VerdictSql) | The product's `==` (lowering) |
|---|---|---|---|
| Two number types differ | false (`Equality.java:165`) | "both empty" only (`VerdictSql.java:104,119`) | compile error in a received body (5a); in a function body the database's `=` (`1 = 1.0` true) |
| Decimal by value (S139) | `compareTo` (`:183`) | `decimalValue` (`:53-64`) | SQL `=` on plain DECIMALs; in a tagged list each scale is its own channel, so `[1.0D,'a'] == [1.00D,'a']` is false (`TaggedLists.java:71-83`); a keyed instance compares by canon text, which includes the scale |
| Date-time by instant (S399) | `LiteHostForms.equalDates` | `instants` (`:954`), `DATES` form | a mixed list's written-digit kind `DateTime(n)` (`MixedEncoding.java:419-423`) is another kind; per the 2026-11-23 measurement, date-time literals compare as written (not re-verified) |
| Day vs date-time | kind differs: false | `dateKind` (`JudgmentFacts.java:239`) | compile error in a received body; in a body, true on DuckDB and H2 |
| Order of mixed numbers | `BigDecimal` value, no type tie-break (`Equality.java:290-319`) | multiset over typed rows (SameValuesForm) | the rank struct `{g, n DOUBLE, d, s, p}`, no type tie-break (`MixedEncoding.java:287-341`) |
| Instances | classifier, then keys (`restrictOne`) | typed leaves | canon text (`InstanceEquality.java:127-176`, `CanonicalRenderSql.instanceEqualityCanon`) |
| Lists on H2 | Java | the portable row form | missing list functions (LIST_FILTER, LIST_POSITION, ...) |

This is why an assert cannot yet be "a plain comparison": both judges already implement S127, S139 and S399 over typed
values, and the product's `==` does not, in every shape. Every finding of item 3's long tail was two of these three
disagreeing (`compat-stage-b-plan-2026-11-24.md:89-94`).

The database judge already refuses the product's `==` where that would differ. Its vector arm says so:
`DatabaseJudge.java:832-837` notes "never the database's SQL equal over a predicate vector, which compares across kinds
and drops a Decimal's scale".

### 1.5 "The judges' separate comparison code": what it is, and its size

**The reading taken here.** The code the database judge composes on its own instead of the product's `==`. This reading
follows the stage-B plan's estimate, "-2.5k to 3k (VerdictSql's comparison layer, `JudgmentFacts.typedPair`,
DatabaseJudge's equality planning)" (`compat-stage-b-plan-2026-11-24.md:97-98`). The host judge is not in it: tenet 5
keeps it as the independent second implementation in the product, and it is the only judge in compatibility.

| Class (part) | What goes | Raw | Code |
|---|---|---|---|
| `lowering/VerdictSql` `:44-65`, `:88-953` | Side, decimalValue, typedEquals, typedCollections, typedTagged, typedInstances and leaves, kindsOf/kindedRows, typedClass | 888 | 688 |
| `lowering/VerdictSql` `:954-1041` | instants, noRows, oneCarrier (instants move into the product's `==`) | 88 | 62 |
| `lowering/VerdictSql` `:1455-1716` | typedInstanceOfHeld, typedContainsTagged, typedContains, typedTolerance, typedAllOf, typedSubset, typedPerElement | 262 | 223 |
| `lowering/VerdictSql` `:1774-1850` | nanos (moves into the product per Q5) | 77 | 63 |
| `lowering/VerdictSql` `:2194-2322` | typedGridPair and helpers (replaced by a grid `==`) | 129 | 109 |
| `lowering/VerdictSql` `:1056-1077`, `:1179-1215` | typedSize, typedEmpty, typedEmptyTagged (become `size()==n` and `isEmpty()`) | 59 | 46 |
| `testplan/JudgmentFacts` `:31-260` | TypedPair, typedPair, emptyKind, dateKind and helpers | 230 | 144 |
| `testplan/DatabaseJudge` | databaseVerdict `:43-210`, gridColumnPair `:211-263`, databaseContains `:495-562`, forAll-contains `:606-632`, databaseTolerance `:652-699` | 364 | 277 |
| `testexecutionplan/RootLoop.Family`, `testexec/AssertRecords` (the tagged and document families), `HostJson`/`TestPlanJson` codecs | families made redundant; not measured | ~100-200 | ? |
| **Subtotal, database judge** | | **~2,200-2,300** | **~1,600-1,650** |
| `testexec/CanonicalForm`, `testexec/CanonicalDivergence` | the measuring instruments of the byte channel (optional, decision D10) | 610 | 349 |

What stays in VerdictSql: `typedCondition`, the size-of-graph members, `typedInstanceOf`, `typedJson` and its document
helpers, `typedInstanceTexts`, `typedTdsEquivalent`, the record layer, `batch`, `assertError`, and the canon-row and
grid helpers (`:2323-3059`, 737 raw, 519 code). That is about 1,150 to 1,250 raw lines.

**Is "about 2,500 lines" plausible?**
- As a *gross raw* count of the database judge's layer, yes at the low end: about 2,200 to 2,300. It reaches 2,500 to
  2,900 only if the instruments go too (D10), or if `CanonRider`/`CanonicalRenderSql` (191 and 1,119 raw) shrink once the
  database judge's riders go. That was not traced.
- As a *code* count it is about 1,600.
- As a *net* count across core it is far smaller. The product's `==` must grow to cover what VerdictSql covers today:
  - typed instance leaves (about 250 raw of VerdictSql `:334-586`) **move**, they are not deleted;
  - lists on H2, the multiset form, generics, grid `==` and an `is` native are new.

  The stage-B plan estimated that growth at +1.4k to 2.3k lines in lowering (`compat-stage-b-plan-2026-11-24.md:97`), so
  the net is roughly 0 to -1,000 lines.

The "Done when" should say gross deletion from the judges, and the user should know the net (D1).

---

## 2. How a value whose type is known only at run time is carried today

### 2.1 The carriers

1. **A tagged value** (`SqlExpr.Tagged`, `M/sql/SqlExpr.java:1304-1333`; `SqlType.Tagged`, `M/sql/SqlType.java:84`).
   It is the winner of a selection among candidates whose kinds are known statically: `max`, `min`, `greatest`, `least`
   and `mode` over a mixed Number or Date literal (`MixedEncoding.MixedElems.tagged`, `M/lowering/MixedEncoding.java:37-56`).
   - `tag` is the 1-based candidate; `comparable` is the winner on the comparison channel; each candidate is
     `(kind, slot SQL type, value)`.
   - At the root it is written as the tag plus one column per slot (`result/TaggedCell.java`, `model/TaggedValue.java`).
2. **A tagged list** (`SqlExpr.TaggedList`, `SqlExpr.java:1349-1420`): a mixed collection (`[1, 'a', 2.0]`).
   - It is carried as parallel arrays by position: `positions`, `tags`, `comparable`, and one `Channel` per distinct
     `(kind, slot)`. Over a relation's cells it is a `rows` subquery instead.
   - It is built by `MixedEncoding.taggedList` (`:112-239`). The kinds are read from each element's static type
     (`kindOf`, `:397-435`):
     - an Integer is a BIGINT, a Float a DOUBLE, a Decimal its own `DECIMAL(p,s)`;
     - a DateTime literal with written subsecond digits is the kind `DateTime(n)` (`:419-423`);
     - a partial date is its text; an enumeration is its name (`columnChannel`, `:445-485`).
   - The rank for ordering is the struct `{g, n, d, s, p}` (`rankedComparableOf`, `:287-341`): numbers as a DOUBLE,
     dates as a TIMESTAMP with the date's precision as `p`. **Two numbers of equal value tie**, whatever their types.
3. **The VARIANT (JSON) lane**: an `Any` list that is not a literal mix (a row's cells, an `Any` variable or property).
   - It is wrapped element by element with `TO_VARIANT` (`MixedEncoding.variantElement`, `:790-813`).
   - Membership compares JSON with JSON (`Scalars.contains`, `M/lowering/Scalars.java:2318-2327`; `in`, `:2705-2706`).
   - Equality against a mixed list moves the mixed list onto that lane (`Scalars.taggedEquality`, `:3866-3871`).
4. **A plain SQL value whose Pure type is known only at run time**: a `Number`, `Date` or generic position (a property
   declared `Number` mapped to a DOUBLE column; a function result typed `Number`). The SQL type fixes the kind
   physically, but nothing carries it. `EqualityClasses.of(NUMBER)` is empty (`M/compiler/spec/EqualityClasses.java:113-114`),
   so there is no marker, and `equal` falls through to the database's `=` (`Scalars.java:174-190`).
5. **Instances**: a struct carries `__type` where the layout has it, and `__id` for an identity the product tracks
   (`InstanceEquality`, `M/lowering/InstanceEquality.java`, 485 raw).

### 2.2 Where equality reads a tagged value

| Operation | Lowering site | What it does with a tag |
|---|---|---|
| `==`, `eq` | `Scalars.java:115-191` → `taggedEquality` `:3817-3878` | `sameElement` (`TaggedLists.java:381`, `:397`, `:465`), `sameList` `:417`, `sameListByKinds` `:68`, `sameListOfKind` `:434` |
| `contains` | `Scalars.java:2309-2317` | `TaggedLists.contains` `:491-515` / `containsElement` `:519` |
| `in` | `Scalars.java:2697-2704` | the same |
| `removeDuplicates` | `Scalars.java:1681-1690` | `dedup` with `sameElement` |
| `removeDuplicatesBy`, `contains` with a comparator | `Scalars.java:2122`, `:2254` | `dedupBy`, `containsBy` |
| `sort`, `max`, `min` | `Scalars.java:1139`, `:1283`, `:1322`, `:1382`, `:1400` | `TaggedLists.sort` `:226` (by the rank), `refuseInexactRanks` `:258-272` |
| `indexOf` | `Scalars.java:1515-1560` (LIST_POSITION) | no tagged arm found (Not verified) |
| joins, filters | relation columns are of one type; no tag | |

### 2.3 What the rule requires and is not yet true, with Pure expressions

The rule: "Types known only at run time carry their type beside the value; equal means the same type and the same
value, and a mixed list orders by value, then by type." The rulings of 2026-11-25 add Q4 ("among equal magnitudes
Integer < Float < Decimal, and StrictDate < DateTime") and Q5 (DuckDB reads a date-time with a digit past the
microsecond as TIMESTAMP_NS) (`compat-b3-progress.md:528-535`).

**G1. One value within one type** (S139, S399), in the product's `==`:
- `[1.0D, 'a'] == [1.00D, 'a']` is false: each Decimal scale is its own slot (`TaggedLists.java:71-83`). By S139 it is
  true.
- `[%2020-01-01T10:00:00.000, 'a']->contains(%2020-01-01T10:00:00)` is false: the needle's kind is `DateTime`, the
  channel's `DateTime(3)` (`MixedEncoding.java:419-423`, `channelOf` `:385-395`). By S399 it is true.
- `[1.0D, 2.00D, 'a']->contains(2.00D)` is false: `channelOf` returns the first Decimal channel only. Wrong under any
  rule; a bug, by reading.
- `^KD(d=1.0D) == ^KD(d=1.00D)` (a keyed class) is false: keys compare as canon text (`InstanceEquality.java:127-176`).
  By S139 it is true.
- Two date-time literals at different written precision compare as written (the 2026-11-23 measurement,
  `compat-assert-records-progress.md:1000-1002`; not re-verified).

**G2. Run-time types, not carried or not compared:**
- A tagged value against a value of no static kind is refused by name (`TaggedLists.java:397-402`, `:491-496`). Example:
  `[1, 'a']->contains($p.weight)` with `weight: Number[1]`, or `[1, 2.0]->max() == $p.weight`.
- Numbers of different types in mixed lists are refused, not answered (`refuseCrossingNumbers`, `TaggedLists.java:107-121`):
  `[1, 'a'] == [1D, 'a']`, `[1.5, 'a']->contains(1.5D)`, `[3, 'x']->removeDuplicates()` over a list holding `3` and `3D`.
  The rule answers each one (false; `removeDuplicates` keeps both).
- A plain `Number` value compares by the database's `=`: `$p.weight == 1`, where `weight: Number` is mapped to a DOUBLE
  column holding `1.0`, is true. The rule says false (a Float is not an Integer), or a compile error if the column's type
  is taken as known (Q2, decision D2).
- The JSON lane ignores Pure types:
  - over `Any` cells, a Decimal and a Float of the same digits are one JSON number;
  - a date is a JSON string, so `$t.rows->at(0).values->contains('2020-01-01')` can hold when the cell is the StrictDate
    `%2020-01-01`.

  By reading the carrier; not run.
- The order has no type tie-break:
  - `[1.0, 1, 'a']->sort()` keeps `[1.0, 1, 'a']` (stable on a tie); Q4 wants `[1, 1.0, 'a']`;
  - for `1` and `1.0` in one mixed list, none of `<`, `==`, `>` holds, so the rule's "exactly one" fails;
  - the host's `Equality.sorted` has the same flaw (`Equality.java:290-319`), so
    `assertSameElements([1, 1.0], [1.0, 1])` fails on the host. Pure: it holds.
- Past 2^53 a DOUBLE comparable ties two different numbers. `refuseInexactRanks` refuses such literal orders by name
  (`TaggedLists.java:252-343`); a computed Decimal is refused too.

**G3. Lists:**
- H2 has no `LIST_FILTER`, `LIST_POSITION`, `LIST_REDUCE` or `LIST_SORT`, so `[1, 2.5] == [1.0, 2.5]` and mixed-list
  equality fail on H2 (42 to 67 bodies in the 2026-11-23 measurement).
- `[] == [1, 2]->filter(x | $x > 5)` is false on DuckDB and SQLite: a computed empty is not the literal `[]`
  (`Scalars.java:128-145` handles only the literal).
- There is no multiset `==` in the product.

**G4. Generics and classes:**
- `pair(1, 1D) == pair(1, 1)` is true: `GenericType` is "run time" in `EqualityClasses.of` (`:120`), and the struct
  `=` compares values.
- The run-time class is compared only on the identity lane: an `Employee` and a `Person` with the same keys are equal
  in the product. The ruling says not equal.
- Keyless `$xs->at(0) == $xs->at(0)` is false.

**G5. Grids:** `==` over relations ignores column names (the measurement).

**G6.** `is` is not a product native (`assertIs` and `assertIsNot` need it). `assert(cond, msg)` inside `forAll` is not
planned.

**G7.** DuckDB nanoseconds live in `VerdictSql.nanos` only (Q5: into the product's `==`).

**G8. Not equality.** These stay judge members: JSON (`typedJson`), instance documents (`typedInstanceTexts`),
`assertTdsEquivalent`.

---

## 3. How comparisons across types are decided today, after step 2b

**The rule's types.** `M/compiler/spec/EqualityClasses.java` (126 raw).
- Known at compile time: Integer, Float, Decimal (any precision), String, Boolean, Byte, StrictDate, DateTime,
  StrictTime, each enumeration, a class.
- Known only at run time: Number, Date, LatestDate, Any, Nil, TDSNull, the Enum class, a type variable, any
  `GenericType`, a function, a relation (`:109-125`).
- The comparisons, by declaration identity (`:46-66`): `==`, `eq`, `in`, `contains`, `indexOf`, `<`, `<=`, `>`, `>=`,
  `compare`, and the two-argument `max`/`min`.
- **No assert is in the list.**
- Two classes cross only if neither is a subtype of the other and they share no subclass (`:91-98`).

**The setting.** `M/compiler/spec/CrossTypeRule.java` (42 raw) is the interface: `inQuery`, `readsBodies`, `inBody`. The
type-checker asks it in `Typer.compared` (`M/compiler/spec/Typer.java:215-234`), called from `Overloads.applyGeneric`
(`M/compiler/spec/Overloads.java:141`). The scope decides:
- `QUERY`, the body a door receives (`SpecCompiler.typeQueryBodyMarked`): the setting answers;
- `BODY`, a function body (`Typer.inFunctionScope`, `:153-161`): asked only if `readsBodies()` is true, and no setting
  says true today;
- `UNREAD`, islands and planner-typed code: never asked.

The three settings:
- **Lite** (`M/compiler/spec/LiteCrossTypeRule.java`): in the query, the marker `TypedCrossTypeComparison`
  (`M/compiler/spec/typed/TypedCrossTypeComparison.java`), which `CrossTypeComparisons.refuse` turns into a TYPE-phase
  compile error. The test planner refuses it at `TestPlanner.java:251`, a product query at `PlanMaker.plan`. In a body,
  the call itself (the database's comparison).
- **legend-engine** (`M/edge/EngineComparisons.java`): the call, everywhere.
- **legend-pure** (`M/edge/PureComparisons.java`):
  - an equality whose sides are never empty answers `false`;
  - a membership answers `false`, an `indexOf` `-1`;
  - an ordering is the call;
  - an equality whose two sides may both be empty is left as a marker and refused by name (`asPures`, `:35-42`).

**The test-body door.** `M/edge/UpstreamTestBodies.java` (519 raw).
- `admittedForPure` (`:47-49`) types under `PureComparisons`; `admitted` (`:53-55`) under `EngineComparisons`.
- Then the body rewrites (`:59-62`): S137 `countedAsOne`, S144 `jsonGoldensCleaned`, `joinsOrderFree`, `rowsOrderFree`
  (unordered `assertEquals` becomes `assertSameElements`; TDS flattened cells become rows, `asRows`).

**How an assert's two sides meet the rule, or bypass it:**
- `assert($a == $b)`, `assert($c->contains($v))` and the like: the `==` inside is an ordinary call in the received body,
  so the rule applies (a marker in lite, the door's answer elsewhere).
- `assertEquals(e, a)`, `assertNotEquals`, `assertSameElements`, `assertEq`, `assertNotEq`, `assertIs`, `assertContains`
  and the rest: **bypass the rule.** EqualityClasses lists no assert. `assertEquals(1, 1.0)` types cleanly and is judged
  false by both judges (S127: host `Equality.java:165`; database "both empty only"). The user ruled on 2026-11-25 (Q1)
  that such an assert is a compile error, like `==` (`compat-b3-progress.md:530-532`). **That ruling is not built.**
- An assert inside a helper function: the helper's body is typed as a function body (`BodyDriver.helperAssertRoot`,
  `M/testplan/BodyDriver.java:277-307`, through `specs.compile(callee)`). Any `==` in it is unmarked: the database's
  comparison.
- A quantified assert (`forAll(x | assertEquals(...))`): `VerdictQueries.assertAsPredicate`
  (`M/compiler/spec/VerdictQueries.java:468-498`) **mints `equal`, `not`, `abs` and `lessThanEqual` as `TypedNativeCall`s
  directly, without the typer**, so no crossing is ever seen. The database arm refuses to judge an equality that way
  (`DatabaseJudge.java:832-837`); it uses the per-element form.
- TDS asserts (`assertTdsEquivalent`, a grid `assertEquals`) and JSON asserts (`assertJsonStringsEqual`) compare grids
  or texts, not through `==`.
- A grid pair's column kinds are spelled from the expected side's schema (`DatabaseJudge.databaseVerdict`, `:62-66`).

---

## 4. What "asserts planned as plain comparisons" means concretely

The database judge plans the assert as the Boolean it means, in the product's own vocabulary. The product's lowering
computes it, exactly as it would in a product query. The statement also carries both sides' values for the failure
record, each side bound once (as `VerdictSql.readOnce` already does, `:212-222`), so the values compared are the values
recorded. `databaseCondition` (`DatabaseJudge.java:603-650`) is the seam. `assertAsPredicate` (`VerdictQueries.java:468`)
is the pattern, but it must type its calls **through the typer**, so that the comparison setting applies.

| Assert | Plain comparison | Notes |
|---|---|---|
| `assertEquals(e, a)` / `assertNotEquals` | `e == a` / `!(e == a)` | needs the product's `==` to cover G1 to G5 |
| `assertSameElements(e, a)` | Pure's body is `sort(e) == sort(a)` (`assertSameElements.pure:11`, cited in `Equality.java:234-235`) | instances and maps have no order: needs a multiset equality (D6) |
| `assertEq` / `assertNotEq` | `eq(e, a)` | instances: identity or keys |
| `assertIs` / `assertIsNot` | `is(e, a)` | **no product native** (G6) |
| `assertContains` / `assertNotContains` | `contains(c, v)` | the same rule as `==`, including tagged lists |
| `assertSize(c, n)` / `assertNotSize` | `size(c) == n` | a relation is refused (S137); a graph side counts JSON documents (stays a member) |
| `assertEmpty` / `assertNotEmpty` | `isEmpty(c)` / `isNotEmpty(c)` | the same; graph sides stay |
| `assertEqWithinTolerance(e, a, d)` | `abs(e - a) <= d` (already minted by `assertAsPredicate`) | SQLite's Decimal arithmetic stays refused (5c-3) |
| `assertInstanceOf(v, T)` | an instance-of test, typed | today `typedInstanceOf` reads the static type or a tag; a product native may be needed (Not verified) |
| `assert(c)` / `assertFalse(c)` | `c` / `!c` | already this (`typedCondition`) |
| `assertJsonStringsEqual(g, d)` | **not plain**: JSON equality is semantic (key order, number kinds) | a judge member (G8) |
| `assertTdsEquivalent(e, a)` | **not plain**: a per-cell tolerance over grids | a judge member |
| `assertEquals` of two grids (relations) | a grid `==` by column name and kind, rows in order | new in the product (G5); until then a member |
| `assertEquals` of printed tables (`toString`, `toCSV` sides) | String `==` | already Strings after step 2b's lift at the doors; lite's route prints in the database |
| `assertError`, `fail`, `assertRaise` | **not comparisons**: a failure caught at run | stays |

**The verdict.** It is the product's `==` in the database. The record is built by the host's builders from the bound
side values (`AssertRecords`), so messages stay identical across judges.

**What each judge does after the step:**
- **The database judge** (product, lite's rules only): plans each assert as a product Boolean plus the record's values;
  its own comparison layer is gone; it keeps its members for JSON, TDS tolerance, graph counts and instance documents.
- **The host judge** (product and compatibility): unchanged in mechanism. It reads each side's typed values and decides
  in Java (`Equality`), which tenet 5 keeps as the independent second implementation. Its rulebook must match the
  product's `==` rule for rule, including the Q4 tie-break.
- **Under `BOTH`:** an agreement is now "the product's `==` equals the host's Java rule". That is G2 (judge parity)
  measured directly, which is the point.
- **In compatibility:** the host judge alone. Its answers do not change, but the product `==` changes do reach the
  compatibility doors' SQL inside queries (§6.4).

---

## 5. The guards that pin this area

| Guard | Kind | What moves |
|---|---|---|
| `T/JavaEvalLedgerTest.java` `EVICT_SIZE`, exact both ways (`:2486-2525`: growth fails, a shrink demands a re-pin) | size pins, **shrink-only** | `testexec/Equality.java` 319 (`:314`), `PureAsserts` 99 (`:321`), `testplan/HostJudge` 243 (`:596`), `JudgmentFacts` 355 (`:603`), `lowering/VerdictSql` 2,286 (`:607`), `testplan/DatabaseJudge` 838 (`:608`), `AssertVerdicts` 1,335 (`:611`), `BodyDriver` 544 (`:863`), `testexec/HostJudgments` 649 (`:1069`), `AssertRuns` 128, `SideShapes` 285, `DatabaseVerdicts` 23, `Verdicts` 77, `TdsCompare` 107 (`:1069-1078`). The host's tie-break grows `Equality` (about +3; the 5b/5c design asked the user's word); everything else shrinks, each a dated re-pin |
| `JavaEvalLedgerTest` file lists (`:1454-1506`, `:1714-1764`, `:2260`) and `verdictFilesJudgeOnly` (`:2281`) | allowlists of files | rows leave with deleted files (`CanonicalForm`, `CanonicalDivergence`, if D10) |
| `T/ArchitectureTest.java` `hostVerdictIsReachableOnlyFromTheVerdictSeam` (`:4041-4075`), with its bite test `theRepointedJudgeRulesBite` (`:4029`) | allowlist of classes | names `AssertVerdicts`, `HostJudge`, `DatabaseJudge`, `PureAsserts`, `TdsCompare`, `CanonicalForm`, `CanonicalDivergence`, `Equality$*`, `HostJudgments`, `AssertRuns`, `DatabaseVerdicts`, `HostForms`, `LiteHostForms`, `AssertRecords`; deleted classes leave |
| `ArchitectureTest.theDatabaseJudgeReachesNoEdge` (`:5232-5262`) | rule over named classes (`VerdictSql`, `DatabaseJudge`, `DatabaseAsserts`, `VerdictBatch`, `DatabaseVerdicts`, `RootLoops`) | must hold. If the database judge's verdict becomes the product's lowering, the product lowering must reach no edge class either (it does not today: `lowering` has the edition table as an interface) |
| New rule (5b/5c design): only the product's equality owner and VerdictSql mint `SAME_IN_*`; later VerdictSql mints no comparison (`SAME_IN_*`, `IS_DISTINCT_FROM`) | rule to add | |
| `T/VerdictChannelRegisterTest.java` `JUDGE_CALLERS` (`:32-60`) | closed register of `Equality`'s callers | test files are listed; a deleted caller leaves (it failed on exactly this at B3 1a) |
| `T/ValueTextCensusTest.java:55,61,62` (Equality 8, DatabaseJudge 1, JudgmentFacts 1) | exact counts | shrink as code goes |
| `T/LiteralJoinCensusTest.java:58` (DatabaseJudge 1) | exact count | |
| `T/ParkedWorkLedgerTest.java:67` (`new SqlWith(` anchors, incl. `VerdictSql.java`) | anchor list | |
| `T/RaiseTextGuardTest.java:44` (VerdictSql's raise sites) | count | |
| `T/CodeShapeGuardrailTest.java:37` `METHOD_LIMIT = 250`, empty allowlist (`:41`) | per-method ceiling | the product's `==` rules in `Scalars` (4,179 raw) must stay under it; the 5b/5c design moves them into a new owner for that reason |
| `testexecutionplan/TestPlanJson.VERSION = 42` (`:91`) | format version | goes up with any plan-shape change (RootLoop families, `HostJudgment`) |
| `core/src/test/resources/lean-sql/baseline.tsv` (3,747 rows), `LeanSqlBaselineTest` | **shrink-only** size pins of product queries planned by `TypedQuery.executionPlan` (`T/leansql/LeanSqlBaseline.java`), per set, database and family | verdict statements are not measured. Product queries using `==`, `contains` or `in` on dates, Decimals, tagged lists, instances or H2 lists can grow: a grow needs the user's word (as with `acos`, 6b item 7) |
| `spec/src/test/resources/rcorpus/lean-sql-baseline.tsv`, `//spec:lean_sql_corpus_<lane>` | shrink-only | the corpus's queries go through the same `==` lowering unless an edition row maps it |
| `T/testplan/AssertBarTest.java` (1,393 raw; 385 bodies × DuckDB, H2, SQLite × every judge) | behaviour pins | the 5b/5c design names `:116-121`, `:185-190`, `:236-238`, `:1033-1040` (line numbers from the other tree; re-find by name) |
| `T/exec/TaggedListTest.java:279-291` (refusals become answers), `T/testexec/InstanceIdentityTest.java`, `T/DecimalKindsTest.java` (`:142` stays), `T/testexec/EqualityWorldsConformanceTest.java`, `T/testexec/EqualityCompatEdgeTest.java`, `T/testplan/NumberKindsEdgeTest.java`, `T/JudgesOnPlansTest.java`, `T/testplan/BodyRouterParityTest.java` (2,372 raw), `T/lowering/VerdictRowOrderTest.java`, `T/lowering/SqlCanonConformanceTest.java` and its H2 twin, `T/exec/LiteralChannelTest.java`, `T/testexec/V7DualChannelCensusTest.java`, `T/testexec/CanonicalFormTest.java`, `T/compiler/spec/CrossTypeRuleTest.java`, `CrossTypeComparisonsTest.java` | behaviour pins | as each slice lands |
| `core/src/test/resources/lanes/h2.txt` (210 lines), `sqlite.txt` (41) | floors, **shrink-only** (a listed test that passes fails the run) | H2 list fixes should remove lines |
| PCT channel B ledger (`pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv`), lane `duckdb-lite` (64 rows, lite's rules, both judges; e.g. `ModelException: min compares Float with Integer`) and the compatibility lanes | exact-match ledger (every failure a row, every row failing) | lite lane rows move as answers move; compatibility lanes must not move |
| `pct/engine_lane/*.tsv` | recorded scoreboards | only if the engine door's SQL changes |
| Corpus rosters (`spec/src/test/resources/rcorpus/*-roster.txt`, registers) and the edge's recorded corpus answers (`spec/src/test/resources/edge/corpus_*_host_*.tsv`) | recorded | must not move (host judge) unless the product's `==` changes the SQL inside the corpus's queries |

---

## 6. Dependencies, order, measurement, and the first slice

### 6.1 Step 4 (strict typing inside functions and mapping code)

Step 6 can begin before step 4, but **its central slice cannot finish safely before step 4 or without a stopgap.**
- Today a cross-type comparison in a function body is the database's `=` for lite (`LiteCrossTypeRule.inBody`).
  - An assert in a helper (`BodyDriver.helperAssertRoot`) is typed as a body. Today both judges call
    `assertEquals(1, 1.0)` there false (S127).
  - Once the database judge's verdict is the product's `==`, it would say `1 = 1.0` true while the host still says
    false: a new disagreement.
  - So either step 4 lands first (the helper's comparison is a compile error), or the plain-comparison slice keeps a
    static gate deciding known-different types at planning, as `EMPTY_ONLY` does today. That gate is about 20 lines,
    deleted with step 4 (decision D3).
- Q1 (an assert across known types is a compile error) depends on **6b item 1**, the literal ruling. Under option (a),
  `assertEquals(0, $x.floatCol)` becomes an error; under option (b) it is legal. The count of lite's own tests that
  break, and whether Q1 is built in step 6 or step 4, depends on it.
- Steps 6.0 to 6.4 below (the product's `==`) do not depend on step 4.

### 6.2 Step 5 and the edition rows

Changing lite's `==` changes the SQL of every door's queries unless a row of the door's table keeps that door's meaning.
- v3 S-11 (`edition-design-v3-2026-11-25.md:213`): run-time-type comparisons under `engine` take the database's
  promotion (ruled), under `pure` take Pure's.
- Today all three doors share the tagged list's refusal (`refuseCrossingNumbers`). When lite answers it (false), the
  doors need:
  - legend-engine's table: a row (a `NamedRule`) for "the database's promotion";
  - legend-pure's: compiled Pure's, by text (`1D == 1` true, `1 == 1.0` false; S127).
- G1's changes (a date-time by its instant, a Decimal by value) are recorded differences under `pure` (S-17, S-18:
  "recorded (expensive or ruled)"), so PCT's legend-pure run should move toward lite's answer, each move listed. Under
  `engine`, legend-engine's answer is the database's, which mostly already is by instant and by value.
- The rows need step 3's table mechanism, which exists (`lowering/EditionTable`, `NamedRule`); they do not need step 5's
  other rows.

### 6.3 Step 9 (test machinery out of the product)

There is no semantic dependency: step 9 is the compatibility side (the SQL-text appeals and the referee, host judge
only). Step 6 is lite's database judge and the product's `==`.

They edit the same files: `AssertVerdicts` (step 9 deletes `gateOf`'s branch `:180-190` and the dual-golden rewrite),
`VerdictQueries` (step 9 deletes `assertEqualsOf`, `:47`; step 6 generalizes `assertAsPredicate`, `:468`),
`HostAsserts`, `DatabaseAsserts` (`:312`), `TestPlanner` (`:130-215`) and `JudgePlans` (`:163`)
(`edition-homework/b3-step6-design.md:136`).

Recommended: whichever runs second rebases. Do not run them in parallel sessions on these files.

### 6.4 What judges it, and what must be measured first

**Measure first.** The 2026-11-23 harness (`tools/run.py`, `rewrite.py`, `classify.py`) lived in another session's
scratchpad, which no longer exists. Only its results are in `compat-assert-records-progress.md:985-1010`. It must be
rebuilt: every assert body of `AssertBarTest`, PCT-shaped cases and the final audit's batches, rewritten to its Pure
body and run under both judges on DuckDB, H2 and SQLite.

The measure:
- by gap G1 to G8;
- how many lite bodies and PCT tests have an assert inside a helper;
- how many compare a `Number` or `Date` position (Q2).

It was taken before 5a. 5a turned the static share of number types, a day against a date-time, an enumeration
against a String and Variants into compile errors, so the numbers must be taken again.

**The judges of the change:**
- `//core:core_tests_judges`: the `DATABASE` and `BOTH` lanes over twelve packages and the root (`core/BUILD.bazel:1021-1058`,
  `:1101`). Fastest single loop: `//core:core_tests_testplan_judge_both --test_filter=AssertBarTest`, then the host's
  `//core:core_tests_testplan`.
- `//core:core_tests_lanes`: the H2 and SQLite floors.
- PCT, lite's rules, both judges isolated: `//pct:pct_channel_b_lite_<suite>` for Essential, Grammar, Relation and the
  other two suites (`pct/BUILD.bazel:499-521`). This is the step's "both judges agree" gate on PCT.
- PCT, compatibility (host alone): `//pct:pct_channel_b_<suite>`. Must not move, except moves the product's `==` brings
  into a door's queries, each listed.
- The corpus (host alone): `//spec:corpus_duckdb`, `//spec:corpus_h2`, and `bazel run //spec:corpus_one -- <duckdb|h2> host <fqn>`
  for one test. The program's measure is "both corpus result lists byte-identical".
- The size pins: `//core:lean_sql_baseline_test`, `//spec:lean_sql_corpus_<lane>`.
- The guards and the census (the fast check in AGENTS.md).
- The warm database lanes: needed because the product's `==` SQL changes (dates, Decimals, lists on each database).
  The database judge itself never runs on them: they run channel B under the compatibility rules, the host's.

### 6.5 The order inside step 6

This follows the 5b/5c design, updated.

| # | Slice | Deletes | Verdicts expected to move |
|---|---|---|---|
| 6.0 | Rebuild the harness and measure on the current head; census of run-time positions (Q2), asserts in helpers, and edition exposure | nothing | none |
| 6.1 | **One value within one type** (5b-1, G1): `==` reads date-times as instants; `DateTime(n)` leaves the kind (`MixedEncoding.java:419-423`); the rank's `p` becomes the date's kind; `sameListByKinds` compares Decimals by value; `contains` checks every channel of a kind; the instance canon trims Decimals and reads instants | little | product answers only (S139, S399's product line); PCT under `pure` moves per S-17/S-18 |
| 6.2 | **Run-time types** (5b-2, G2): a value of no static kind gets its type from its SQL type; `refuseCrossingNumbers` deleted (strict false); the Q4 tie-break in the rank and in `Equality.sorted`; the JSON lane compares by JSON kind and refuses a Decimal or a date there; S-11's two edition rows; a property test that exactly one of `<`, `==`, `>` holds for every comparable pair | refusals become answers | TaggedListTest's pins; PCT lite; the compatibility doors held by the rows |
| 6.3 | **Generics and classes** (5b-3, G4): `EqualityClasses.of(GenericType)` reads Pair/List/Map arguments; the run-time class first | — | PCT compatibility per the doors' settings |
| 6.4 | **The product's `==` complete in one owner** (5c-1): a new equality owner out of `Scalars` (`:115-191`) and `InstanceEquality`; `SAME_IN_*` over rows for lists on every dialect (H2 included); typed instance leaves moved from VerdictSql `:334-586`; grid `==`; an `is` native; `assert(cond, msg)` in `forAll` | the canon-text instance equality | product answers on H2 (67 list bodies), `[] == computed []`, grids |
| 6.5 | **Asserts as plain comparisons** (5c-2): the families of §4 planned through `databaseCondition`, sides bound once, records unchanged; **needs step 4 or the static gate (D3)** | VerdictSql `:44-953`, `:1455-1716` and the rest of §1.5; JudgmentFacts `:31-260`; DatabaseJudge's equality planning | database-judge UNJUDGED become judged (21 bodies in the old measure); disagreements 47/51/54 down to about 3/1/3 on DuckDB/H2/SQLite |
| 6.6 | The rest (5c-3): `nanos` into the product; `refuseMixedDays` (`JudgmentFacts.java:272`) deleted where tags keep days apart; tolerance as its body; `typedGridPair` replaced by the grid `==` | VerdictSql `:1774-1850`, `:2194-2322` | S399's let-bound gap closes |

### 6.6 The smallest first slice that deletes code and moves no verdict

No slice of the comparison layer itself is verdict-neutral before the product's `==` covers that form: every typed
member exists because the product's `==` differs there. The candidates:

1. **The measuring instruments** `CanonicalForm` and `CanonicalDivergence` (610 raw, 349 code), with their probe calls
   in `HostJudgments` (5 sites) and `AssertRuns`, and the tests that read them (`V7DualChannelCensusTest`,
   `CanonicalFormTest`, part of `LiteralChannelTest`). They are "not a verdict path" by their own contract
   (`CanonicalForm.java` header; `hostVerdictRule`), so no verdict can move. The guard rows leave. This is cleanup, not
   the step's mechanism (D10).
2. **`assertSize`, `assertNotSize`, `assertEmpty`, `assertNotEmpty` over non-graph, non-relation sides** as product
   conditions (`size(c) == n`, `isEmpty(c)`) through `databaseCondition`, with the record's values bound once.
   - It deletes `VerdictSql.typedSize`, `typedEmpty` and `typedEmptyTagged` and their DatabaseJudge arms: about 150 raw
     lines.
   - Counting and emptiness carry no type rule, so the verdicts should be identical. To confirm: AssertBarTest under
     `BOTH` on three databases, and the `duckdb-lite` PCT lane.
   - **Recommended as the first real slice:** it builds the whole new path (an assert as a product Boolean plus the
     record layer, sides bound once) on a family where the product already agrees, before any rule changes.
3. **`assertEqWithinTolerance`** as `abs(e - a) <= d` (the predicate already exists): deletes `typedTolerance` and
   `databaseTolerance` (about 80 raw). SQLite's Decimal arithmetic must stay refused, so check the SQLite floor.

---

## Decisions for the user

**D1. What "the judges' separate comparison code" and "about 2,500 lines" mean.**
- Option A: the database judge's own comparison layer only (§1.5). About 2,200 to 2,300 raw (1,600 code) gross; about 0
  to -1,000 net, after the product's `==` grows.
- Option B: also the host judge's `Equality` and its compare code. This contradicts tenet 5 and leaves compatibility with
  no judge.
- **Recommend A,** with the "Done when" restated as "the database judge's comparison layer deleted (about 2,200 raw
  lines); the net stated".

**D2. Q2 (open since 2026-11-25): a `Number` or `Date`-declared value whose SQL type fixes its type** (`$p.weight == 1`
over a DOUBLE column).
- Options: a compile error (strict item 1; needs store column types at type-checking, which they are not today); compare
  at run time by the SQL-type-derived type (false); or a recorded row.
- **Recommend the run-time comparison in 6.2,** with the compile error deferred until column types reach the typer.

**D3. Order against step 4.** Options:
- step 4 before 6.5;
- 6.5 with a static gate for known-different types in helper asserts, deleted with step 4;
- refuse helper asserts that cross types.

**Recommend the gate,** so step 6 is not held by 6b item 1.

**D4. Q1 for asserts (ruled, not built): where it is built, and each door's answer.** Add the assert ids to
`EqualityClasses` with a kind of its own. Today `PureComparisons.answer` would turn an `assertEquals` marker into the
literal `false` statement, which is wrong for an assert.
- **Recommend:** lite refuses; both compatibility doors answer the call (the host's S127 false). B3 1a measured that this
  moves no verdict.
- Built after the literal ruling (6b item 1).

**D5. Run-time crossings per edition (v3 S-11).** Confirm the mechanism: two edition-table rows on tagged
comparisons. legend-engine: the database's promotion. legend-pure: compiled Pure's text rule (`1D == 1` true). Lite:
strict.

**D6. `assertSameElements` over values with no order** (instances, maps). Options:
- a lite-internal product function that the lowering writes as `SAME_IN_ANY_ORDER`;
- keep it a database-judge member;
- refuse.

**Recommend the internal function** (not user-visible, like the relational language's lite-only comparisons).

**D7. A DOUBLE comparable past 2^53.** Keep `refuseInexactRanks`, or use a DECIMAL comparable where the database has one
(the stage-B plan's caveat). **Recommend** keeping the refusal for now and measuring first.

**D8. Size pins that grow.** The product's `==` changes (instants, Decimal channels cast to one type, H2 list forms) may
grow lean-SQL families, and those pins only shrink. Ask before re-pinning, as for `acos`.

**D9. `Equality.java`'s ledger pin grows by about 3** for the Q4 tie-break. The pin moves only with the user's word.

**D10. Delete the measuring instruments** (`CanonicalForm`, `CanonicalDivergence`, about 610 raw) as part of step 6?
**Recommend yes:** they measure the byte channel the step retires.

**D11. JSON and TDS tolerance stay judge members** (G8): confirm. No product JSON equality is proposed.

**D12. AGENTS.md's "[CONVENTION until B3 step 3]" marker** on the equality rule names a step that is now step 6.
Amend it when the step lands.

## Not verified

- Line numbers quoted from the 5b/5c design for test files (`AssertBarTest :116-121`, ...) were written against
  another tree. The main-source lines in this sheet were read at `46f943fe`.
- That the product's `==` compares two date-time literals as written text (the 2026-11-23 measurement and the 5b/5c
  design say so); not traced through `CastPolicy` or the dialects here.
- The JSON lane's behaviour per database (a Decimal against a Float of the same digits, a date against its text): by
  reading the carrier only.
- `indexOf` over a tagged list: no tagged arm was found at `Scalars.java:1515-1560`; whether it is refused or wrong was
  not established.
- Whether the dual-golden gate at `AssertVerdicts.java:185-187` can reach the database judge at all (rule 4 suggests
  not).
- How many asserts live in helper functions, in core or PCT (it decides D3's cost).
- Whether any gate reads `CanonicalDivergence`'s census (`diagnostics/VerdictCensus` and two tests reference it).
- How much of `CanonicalRenderSql` (1,119 raw) and `CanonRider` (191) survive once the database judge plans plain
  comparisons: the side canon wrap (`planner/SidePlanner.java:114-127`) and the riders' kind reading
  (`DatabaseJudge.oneKind`) were not traced.
- The +1.4k to 2.3k growth estimate for the lowering is the stage-B plan's (2026-11-24), not re-measured.
- Whether `assertInstanceOf` needs a new product native.
- Whether the PCT compatibility lanes and the corpus stay byte-identical once 6.1 and 6.2 change the SQL of `==` inside
  their queries: depends on D5's rows; to be measured.
