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
