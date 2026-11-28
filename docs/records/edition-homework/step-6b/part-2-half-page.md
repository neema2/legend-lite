# Step 6B part 2: the kinds that are not plain values (2026-11-28, written before it is built)

*The half page the plan of record asks for. Read by a read-only research pass over `claude/step-6b-next`
(2026-11-28; nothing built or run) and by the author. Stacked on the landing of slices 5 and 6 (`bc114f81e`).*

## The design, in one line (the user, 2026-11-28: "Yes that's right ... now extend to everything")

**Reduce each side to what it is compared by, then ONE instruction.** A plain value is compared by itself; a
date-time by its instant (done, slices 3 and 5); an object by its keys, or its identity where it has none; an
element of a mixed list by its type family and its value; a table is a list of rows, a row compared by its columns
matched by name. The lowering makes the reduction in one place per kind and emits the one "same values"
instruction (in order, in any order, or membership) over the two reduced sides, with their rows riding; each
dialect writes it over two values where its database holds lists, over the rows where it does not. It is what
slices 1 to 6 built for lists of one plain type, extended to every kind. What goes: the second implementation of
each kind's rule in `SideEqualities` (`instances`, `tagged`, `collections`, `values`, and `grids` as a comparison of
its own -- its two compile-time checks, column names and a defined row order, stay as checks).

## What is really there today (one correction to how part 2 was described)

For WHOLE sides of class instances, tables and mixed lists the test checker and the product already run ONE code:
`lowering/SideEqualities` (`pair` and its forms, `grids`), which `planner/RootComparison` calls for a query's
comparison and `testplan/DatabaseJudge` calls for an assert's (`JudgmentFacts.typedPair`, `tp.row`). What the
checker still has of its OWN is smaller than "its comparison code": its gates (which pair goes where, what is
"unjudged"), and four membership and grid helpers nothing else calls. The second code that step 6B is about is
`SideEqualities` (whole sides) beside `Equalities` (values where they stand).

## The pieces, in order

| | What | Why this order | Done when |
|---|---|---|---|
| **D** | Values typed only when the query runs (`Number`, `Any`, the abstract `Date`), and days and date-times under `assertContains`, take the path slices 5 and 6 built: the product's own comparison over sides bound once | 10 of the measure's 12 assert rows that are not right on DuckDB are these; the product's rule exists already | those rows right; `SideEqualities.contains` and `VerdictSql.typedContains` deleted |
| **B** | Two tables: the checker asks the planner's ONE entry for a comparison of two relations (`RootComparison`), with the values its failure record reads, instead of calling `grids` itself | the comparison is the same code already; only the entry and the record's values are missing | `VerdictSql.typedGridPair`, `gridColumn`, `gridCells`, `DatabaseJudge.gridColumnPair` deleted |
| **A** | Class instances (keyed, keyless, Pairs, an instance carried as its text) | real lowering work: instances in any order, many-valued instances on a database with no lists, the record's values cut down to the keys | `containsInstances` deleted; the measure's Pairs row right |
| **C** | Mixed lists | hardest: a bound column of a mixed value has to be turned back into what the mixed-list code reads, and the column that orders it may not be carried | `containsTagged` deleted |

After all four: `JudgmentFacts.TypedPair` and the checker's `tp.row` block go, and the ledger's pins come down below
where slice 6 found them.

## D, the first piece: what changes

1. **The assert's comparison is the test author's.** The product reads the kinds a comparison's sides hold only
   for a comparison the query's author wrote (a source position: `Equalities.heldApart`, `Equalities.member`,
   `HeldKinds`). The comparison the checker builds for an assert has none, so the reading is skipped. The assert's
   own position is carried to the built call (`VerdictQueries.sameSides`, `sameElements`, `containsOf`).
2. **The gates widen:** `DatabaseJudge.databaseVerdict` and `databaseContains` take sides declared `Number`, `Any`
   or `Date`; `SideEqualities.boundOnce` lets two sides be held in two kinds there (the product answers "never
   equal, both evaluated" for them).
3. **A membership's rows are read as its values are compared:** a day or a date-time by its instant, as the "same
   values" instruction's rows are (`SideEqualities.instants`), with the needle read the same way.

Not verified by reading, to probe first: what position an assert inside a helper function carries; whether the
checker's struct layout for an instance is the product's (`sqlTypeOf`), for A; whether the mixed list's ordering
column is needed to compare two lists, for C.

## Tests that judge each piece (run with `--test_filter`)

D: `exec/RunTimeComparisonTest`, `NumberFamiliesTest`, `DatesTypedByTheirColumnsTest`, `ContainsOverloadTest`,
`LiteralChannelTest`, `testplan/NumberKindsEdgeTest`, `AssertBarTest#theLastFamiliesAreTypedOrRefusedByName`.
B: `exec/RootComparisonTest`, `AssertBarTest#aTableComparisonIsOneOutcomeOnEveryDatabase`.
A: `AssertBarTest#instancesAreComparedByTheirKeysOrTheirIdentity`, `#aMetamodelPairOfSeveralSetsIsOneOutcome`,
`exec/StructValueTest`, `RootComparisonTest#anObjectWithNoKeyIsEqualToItselfAndToNoOther`.
C: `exec/TaggedListTest`, `TaggedListEdgeTest`, `TaggedSelectionTest`.
After every change that could move them: `//core:guardrails`, `//core:guardrails_edge`, `//core:census`.

## D, as built so far (the no-Bazel loop, 2026-11-28, about 09:20 UTC; not reviewed, guards not run yet)

- **Step 1, done:** the assert's source position rides to the comparison the database judge builds
  (`BodyDriver.ExecEnv.assertAt`, set in `AssertVerdicts.planDatabase`; `VerdictQueries.sameSides`, `sameElements`,
  `containsOf` stamp the built call).
- **Step 2, started:** `assertContains` over sides declared `Number` (each held in one number kind) takes the
  product's `contains` (`DatabaseJudge.databaseContains`'s gate; `SideEqualities.boundOnce(..., typedAtRunTime, ...)`).
  Probed on DuckDB and H2 under both judges: an Integer among Floats fails with the host's record, the same kind
  passes.
- **Found, to fix before D lands: the two judges disagree on a written number literal against a value typed at
  run time.** `assertContains([[2, 1]->cast(@Number)->sort()->at(0), 5], 1.0)`: the product, and so the database
  judge now, read the literal `1.0` as the Integer it is compared with (the literal rule; the user's ruling of
  2026-11-28 keeps that) and pass; the host judge compares a Float with an Integer and fails. Until D the database
  judge said "unjudged" there, so nothing showed it. The host judge has to read a written literal the same way
  (the judgment's facts say which side is a written literal), or a both-judges run of such an assert reports a
  disagreement.
- **Still to do in D:** the same for `assertEquals` / `assertNotEquals` / `assertSameElements` (the gate in
  `databaseVerdict`); the abstract `Date` and days and date-times under `assertContains` (the membership's rows read
  by their instants, the needle the same way); `Any`; then the measure, the judging tests named above, the guards.

## D, later the same day (about 10:00 UTC)

- **Sides declared `Number` are done for all four asserts** (`assertEquals`, `assertNotEquals`,
  `assertSameElements`, `assertContains`): each bound side is shown to the product as the kind its column holds,
  under the type it is declared (`VerdictQueries.heldAs`: a typed value of that kind beneath an upcast), so the
  product reads the kinds exactly as it reads an upcast the query writes -- through `let`s too, where the query's
  text names no kind. Both judges agree in every probe on DuckDB and H2 (two families fail with the host's record;
  an Integer beside a Decimal passes).
- **The measure** (the no-Bazel loop): DuckDB 198 of 229 right everywhere (194 at the landing), none failing, none
  wrong, none contradicting; H2 165. The four rows that moved are the `Number` rows of `assertContains`; the
  baseline file is this run's.
- **Guards and tests on the first cut** (Bazel, `//core:guardrails`, `guardrails_edge`, `core_tests_testplan`,
  `_exec`, `_compiler`, `_lowering`): pass. The judge ledger is UP (`DatabaseJudge` 907 to 929, `BodyDriver` +5,
  `AssertVerdicts` +1): it must be back under its pins when D lands, by deleting the old member's branches this
  path replaces.
- **Tried and taken back: the abstract `Date`.** A side declared `Date` is planned with two candidate kinds (the
  judge's rider lists a day and a date-time), so there is no ONE held kind to show the product. It needs the
  side's plan to say which kind each value holds, as a mixed list's does: with piece C, or by refining the rider
  where the query's text names the kind.
- **Left on the old path on purpose:** a side written as number literals against a value typed at run time (the
  host judge does not read a written literal as the product does: owed).

## Objects, maps, enumerations and the DuckDB fix (2026-11-28, about 13:00 to 15:30 UTC; the no-Bazel loop only)

*Not reviewed (the audit started on the first cut was stopped by the user's interrupt and not restarted), guards and
Bazel tests not run yet. The user, the same afternoon: mixed lists LAST, after the step 7 research is read.*

**One reduction for what an object is compared by** (`InstanceEquality.reduced`, over the leaves
`SideEqualities.pairedLeaves` names): the class it actually is, then its keys by value, or its identity where its
class declares no key. Every operation built on `equal` over objects reads it:

| Operation | Before | Now |
|---|---|---|
| `==`, `hasSameElements` of two lists of objects | a statement of its own (`SideEqualities.instances`); inside a row, the TEXT of the keys | the one "same values" instruction over the two lists, each object reduced, the sides' rows riding (`InstanceEquality.sameInstances`); the planner stands aside (`SideEqualities.instancesAsValues`) |
| `contains`, `in` | the text of the keys | the reduced value among the reduced list, the list's rows riding (`memberInstances`; `CarrierStrategies.membershipRule` reads a struct needle leaf by leaf) |
| `indexOf`, `removeDuplicates`, `isDistinct` | the text of the keys | the same reduction (`reducedBy`); the text only where a leaf has no typed reading |
| an assert of two sides of objects, `assertContains` of objects | the checker's own statements (`instances`, `containsInstances`) | the product's comparison over sides bound once (`SideEqualities.boundInstances`) |

Deleted: `SideEqualities.instances`, `containsInstances`, `VerdictSql.typedContainsInstances`.

**What it fixed, each measured before and after:**

- A Decimal key written at two scales (`1.0D`, `1.00D`): `indexOf` answered -1 (wrong) on DuckDB; right now.
- Two Pairs, an Integer against a Decimal: false on SQLite, refused on H2, "unjudged" by the database checker; right on
  all three, judged.
- A list of Pairs compared inside a list's function on DuckDB: refused; right now.
- `contains` of an object on H2: refused; right now (the rows ride). On SQLite an Integer against a Decimal inside
  Pairs: false; right now.
- **The class an object actually is** (the user: "Does it work for polymorphic at runtime too?"): a child under its
  parent's type was already compared rightly by `==`; `contains` of a child among a list typed its parent's answered
  FALSE (a fold on the two DECLARED classes, `InstanceEquality.equality`, there before this work). Two declared
  classes are now compared by the class each actually is, then keys or identity, where both layouts carry the class
  and the two declare the same keys (`SideEqualities.oneReadingOfTwoClasses`); the fold stays for two objects built
  where they stand.
- The host checker compared a classifier present on ONE side only (a list of built objects carries each one's, one
  built object read as a value carries none): `assertContains([^Q(id=2, ...)], ^Q(id=2, ...))` failed under the host
  checker alone. It compares the classifier where both sides carry one (`HostJudgments.asEqualSees`).

**Maps:** two single maps are compared as values, each as its entries sorted by key (`Equalities.sameMaps`,
`SideEqualities.entriesOf`, the reduction the statement form used); the planner stands aside
(`SideEqualities.mapsAsValues`). So a map read from the row, refused by name until now, is answered. SQLite, which
refused every comparison of two maps, answers the ones held as one map type.

**Enumerations:** two lists of one enumeration join the kinds compared as values (`SideEqualities.plainKind`).

**Two types the query's text names** (a list of whole numbers under `Number` against Floats under `Number`): the
planner stands aside for what `Equalities.heldApart` answers (`Equalities.apartByTheText`).

**DuckDB runs no sub-query inside a list's function**, which refused every comparison that is a statement of its own
there (two tables, date lists held two ways, a side read whole). One fix in the dialect, for every kind
(`sql/dialect/SubqueriesOutOfLambdas`): a sub-query that reads nothing of the lambda's element is read once outside
it -- `list_transform([{s0: (sub-query)}], __held -> list_filter(xs, x -> ... __held.s0 ...))[1]`. The one
difference: it is evaluated once even over an empty list, so a comparison whose side raises is raised there too
(to record for the user).

**Known and left:** on a database that carries a list as JSON text, two lists of objects holding exact numbers at
two scales, compared where the sides read the row, are refused by name (they answered false); `indexOf` over Pairs
there still compares texts (the database follow-up's row D8). `removeDuplicates` and `isDistinct` over a keyed class
no longer ask the layout for an identity (they are by the keys). A single-object pick for a side bound once is a
sub-query with `LIMIT 1` (H2 2.5 has no `ANY_VALUE`).

## The grid, and objects inside containers (2026-11-28, about 16:00 to 18:00 UTC; the no-Bazel loop)

**The user's questions that made it:** "we should check Pairs with objects and Pairs with value+object mixed. And Maps
as well? That's only key equality or key and value match. ... Do we have a full matrix of types and container types
that we should check each combination?" There was none: the measure's rows had been added as problems were found.

**The grid** (`EqualityMeasure.grid`): 13 kinds of value (Integer, Decimal, Float, String, Boolean, an enumeration, a
day, a date-time, a year, a keyed object, an object with no key, a child under its parent's type, a Pair) in 8
containers (a Pair's first, a Pair's second, a Pair in a Pair, a map's value, a map's key, `list([...])`, a computed
list, a computed list of Pairs), each as two comparisons: two writings of one value (equal) and two values (not).
The containers that build one value are asked of the other comparing functions too. 350 more rows; the measure is
600 comparisons in 20 places on three databases.

**What its first run showed on DuckDB:** every plain kind right in every place in a Pair and in a list; the failures
were objects inside containers, and smaller items below.

**Objects inside containers, the root cause and the fix.** A carrier that holds an object in a slot had no layout:
`LayoutTypes.sqlTypeOf` handed a Pair, a list or a map to the layoutless mapping, which raised "no SQL type for Pure
class" at the object. So such a value could be no statement's answer and no assert's side, and where it was compared
as a value the database compared every field (a keyed object in a Pair was "not equal" to one with the same keys).
`LayoutTypes.sqlTypeOf` now builds the carrier's shape over each slot's own type, a class by its layout. With it:

| | Before | Now (DuckDB, SQLite; H2 where it holds no list) |
|---|---|---|
| `pair(^Q(id=1, note='a'), 1) == pair(^Q(id=1, note='b'), 1)` | false | true: the object by its keys |
| the same as an assert, either checker | an internal error | judged, the two checkers agree, one record |
| a Pair holding an object with no key | by every field, by luck right | by the object's identity (`SideEqualities.leaves`: a slot declared a model class with no key) |
| `indexOf` of a Pair holding an object | by every field | by the reduction (`InstanceEquality.claims`) |
| `list([^Q(...), ...]) == list([...])` | by every field | as its instances (`InstanceEquality.instancesOf`); never by a side's rows: a List is one value |
| two maps to objects | an internal error | as values, entries sorted by key, each object reduced (`InstanceEquality.sameMapsOfInstances`) |
| an object as a map's key | an internal error | refused by name |

**Maps** (the user's question): keys AND values must match; the order the entries were written in is no part of it.
Probed: a value differs, a key differs, one more entry: false; the other order: true; whole-number keys: fine.

**Also built here:** an assert's membership over sides declared the abstract `Date` is the product's own (two days,
or a day and a date-time: `SideEqualities.heldDateKind` reads the kind off the query's text or the column;
`VerdictQueries.heldAs` shows it). Two date-times under `Date` stay with the old member until their rows ride read
as instants. The database checker judges two sides of model classes declared of two classes
(`SideEqualities.instancesOfTwoClasses`); the host checker compares each such object by the class it actually is
(`HostJudgments.asEqualSees`). The lowering asks the door's table in one place (`Lowerer.productsOwn`; the ratchet's
pin 6 to 3).

**Found by the grid, open:**
- `list([1, 1])->in([list([1, 1])])` raises a data error on DuckDB (a list among lists); `assertEquals` of two
  `list(...)` values is unjudged by the database checker.
- A map whose keys are Decimals written at two scales: the host checker says "not equal" (the product refuses the
  pair by name).
- A year inside a Pair or a map: answered in some places only.
- Reading a list of a child and a parent as a RESULT fails in exec ("a struct cell of 7 attributes under a slot of 6
  fields": the plan types the list by its declared class).
- On SQLite, `indexOf` over Pairs of plain values still compares texts.
