# Slice 7: a door's comparison is its own function (2026-11-28, written before it is built)

*The half page the plan of record asks for. Read by an independent research pass over `claude/step-6b-slice-6`
(2026-11-28, read-only; file and line for each claim in its report, kept in the session's log) and by the author:
`lowering/Scalars.doorsRule` and `threeNumberTypesAndWrittenDatePrecision`, `lowering/EditionTable`,
`edge/UpstreamTies`, `edge/PureTable`, `edge/EngineTable`, `compiler/spec/CrossTypeRule` and its three
implementations, `Typer.compared`, `builtin/Pure`'s lite-owned functions.*

## The rule (the user, 2026-11-28)

"I thought part of this was fixing the doors to be passed in and nothing knowing or guessing about which mode/door
was?" AGENTS.md, tenet 6: no modes in the core; the edition is read before the lowering and "nothing after the typer
can see it".

## What the lowering still reads today, for a comparison

| Read | Where | What it decides |
|---|---|---|
| `Scalars.doorsRule(call, features, styled)` = the door's table, `EditionTable.lowersAs` | `Scalars.lower`; `Lowerer` four times (`==` as "same values", membership's rows, the two whole-side entries) | at both compatibility doors `==`, `eq`, `in`, `contains`, `indexOf`, `removeDuplicates`, `<`, `<=`, `>`, `>=` take the rule `THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION` (`edge/UpstreamTies`), lite's door none |
| `EditionTable.emptySideIsFalse(call)` | `Lowerer.written`, once | lite and legend-pure: an empty side is false as Pure says; legend-engine: only outside a store query (`call.outsideStore()`) |
| `NumberFamilies.TWO_FAMILIES` / `THREE_TYPES` | 22 mentions: `Scalars` 9, `Equalities` 9, `SideEqualities` 2, `HeldKinds` 1, `TaggedLists` 1 | lite's number rule or the doors'; some branches were added by slices 3, 5 and 6 to keep a door's answer as it was |

A guard pins the three counts as ceilings from this slice on (`ComparisonDoorReadsRatchetTest`: 6, 1, 22).

## The step

1. **Functions.** Each compared family gets the door's twin, declared with lite's own functions in `builtin/Pure`
   (as `hasSameElements` is): the upstream comparison (three number types, the written date precision) for both
   doors, and legend-engine's in-store equality (the database's own, no empty-side answer). Their rules are
   registered in `Scalars.RULES` like any function's; the rule is what `threeNumberTypesAndWrittenDatePrecision`
   dispatches to today. Two catalog pins move with a dated note (`NativeCatalogGovernanceTest`, `CatalogUpstreamDiffTest`'s
   count of functions upstream does not have).
2. **One place swaps them in**, before the lowering, for every comparison a door admits: the type-checker's hook
   (`Typer.compared`, extended from "two types that cross" to every comparison) for code it reads. NOT covered by
   that hook, each to be read and decided before building: code typed UNREAD (relational-mapping bodies, islands,
   planner frames: `Typer.java:295`), `Overloads`' derived shadow, and the calls the store resolver writes after
   typing (`resolver/Substitution`, `AssociationJoins`: join conditions) -- today the table catches all of these
   because it matches by the function's id wherever the call came from.
3. **The lowering forgets the door for a comparison:** `doorsRule` loses its compared rows, `emptySideIsFalse`
   and `NumberFamilies` as an argument go (each rule knows its own family), `UpstreamTies`' compared row and the
   `NamedRule` are deleted, and every place the lowering recognises `==` by its family (`isFamily(...AT_BOOLEAN_EQUAL)`:
   whole sides, instance equality, the positive-condition reading, the negation) is taught the twins or told
   why not. The guard's pins go to zero and an architecture rule forbids the read.

## How it is known to be right

Nothing a door answers may move, and no SQL a door writes: PCT at legend-pure's door (channel A and B), the
relational corpus and the engine lane at legend-engine's, the edge tests and the recorded edge SQL are byte for byte
as before; lite's own measure is line for line as before. Any difference stops the slice.

## Not in this slice

The other rows of a door's table (`round`, `substring`, the checks, the sort and extreme ties): the edition
program's own phases. Removing the branches slices 3, 5 and 6 added "for lite's rule alone" where legend-pure would
answer as lite does: each needs legend-pure's answer RUN first, so it is a list for the user, not a default.

## Size

The plan of record: one to two days. Not safe on the day slices 5 and 6 land; the choice is with the user
(close 6B without it and build it next, or build it now and close a day later).

## Where the swap must happen (a second read-only research pass, 2026-11-28, about 07:10 UTC)

**Decided by the code: one pass over the whole typed tree, after the store resolver and before the lowering**,
keyed on the comparison's family, swapping in the door's twin function. Not the type-checker's hook, and not a
stamp at each place a call is made. The three facts:

1. **The store resolver writes lite's `equal`, `in` and the orderings itself in more than ten files** (join keys,
   milestoning windows, the EXISTS it rewrites a user's own `contains` or `in` into, ordinals, subtype keys), found by
   name, with no source position. A twin swapped in by the type-checker would never reach these, and the resolver
   recognises comparisons by family in about seventy places that would all have to learn the twin.
2. **Code the type-checker does not read as a query** (relational-mapping bodies, islands, unmarked bodies) reaches
   the lowering today and is caught only because the door's table matches by the function's id.
3. **Everything such a pass needs is on the call already:** the "runs outside the store" mark is set before or in
   the resolver and survives every rebuild; a call lite wrote has no source position.

What the door's rule changes for two plain values of one type, so the risk is known: nothing in the number rule
(both rules end in the same writing); the one difference is the empty side at legend-engine's door inside a store
(a plain `=` there, where lite and legend-pure write "false when a side is empty") -- and only for a comparison
that is neither a positive condition nor a join or correlation the resolver built. So the pass gives legend-engine's
in-store comparisons their own function, and both doors' comparisons the "three number types" twin.

**The lowering's places that recognise a comparison by family** (each gets the twin, or a line saying why not):
`Lowerer` six (the positive condition, same values, empty sides, the whole answer and its negation, instance
equality, whole sides); `Scalars`' registrations for `==`/`eq`, the orderings, `indexOf`, `removeDuplicates`,
`contains`, `in`; `Equalities.membershipList`; `HeldKinds`; `InstanceEquality`, `Dedup` (by name);
`ValueCollectionOps`; `LambdaBinding`; `planner/RootComparison`.

**Not verified yet, to read before building:** the position (positive, join, or neither) of every comparison the
resolver writes; exactly where the resolver's phase ends in each planning entry (`ProductStatements`, `BodyPlanner`,
the edge's plans); whether the door's feature rules name `equal`.
