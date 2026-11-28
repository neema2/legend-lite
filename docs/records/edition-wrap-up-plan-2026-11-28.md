# The edition program: how it is wrapped up (2026-11-28)

*Written at the user's request the day slices 5 and 6 of step 6B went through their landing checks ("do we have it
written down somewhere?"). It is the order of what is left and what "done" means for each piece. The step numbers
are `edition-plan-to-finish-stage-b-2026-11-25.md`'s; step 6B's own plan is
`edition-homework/step-6b/PLAN-OF-RECORD-2026-11-27.md`.*

## Where it stands

| Step | What | State on 2026-11-28 |
|---|---|---|
| 1-5 | The rulings, the door's table, strict typing inside functions, legend-engine's meanings | Closed 2026-11-26, against their own tests |
| 6 "A" | One equality, first form | Landed |
| 6 "B" slices 1-3 | One "same values" instruction; each side written once; every plain kind | Landed (program branch `c7babd506`) |
| 6 "B" slices 5-6 | `in`, `contains`, `indexOf`, `hasSameElements` by the same rules; asserts of plain values as the product's own comparison; the three `map` fixes | In their landing checks (`claude/step-6b-slice-6`) |
| 6 "B" slice 7 | A door's comparison as its own function | Researched, half page written, not built |
| 6 "B" part 2 | Class instances, tables, mixed lists | Not started |
| 8 | Presentation at the edge | Two slices landed; the rest to be RE-SPECIFIED (below) |
| 9 | Test machinery out of the product | Slice 1 landed, slice 2 built |
| 10 | Clean up and lock | Not started |
| 11 | Land on `main` | Not started; the user's word |

## What is true today, and what is not yet (do not claim the second list)

**True:** for plain values a comparison answers the same wherever it stands (the measure, 229 comparisons each
asked in every place: DuckDB 194 right everywhere, none wrong, none contradicting; H2 164, none wrong); an assert of
plain values is the product's comparison, its SQL shorter than before.

**Not yet true:**
- "One comparison code." The test checker's own comparison code is still there and its ledger went UP this slice
  (the new path stands beside the old). It comes down only when part 2 moves the remaining kinds.
- "The lowering does not know the door." It asks in 29 places (pinned by `ComparisonDoorReadsRatchetTest`: 6, 1, 22).
- "Nothing after the type-checker sees the edition." Comparisons are slice 7; the rest of the door's table
  (`round`, `substring`, the raise checks, the sort and extreme ties) is the same work again.

## The order

1. **Land slices 5 and 6.**
2. **Part 2: class instances and tables first, then mixed lists** (order changed the same day, the user: "I feel
   like we know how to do those at least but still don't know how exactly to do step7"). Slice 7 is out for
   research in another account; part 2 does not wait for it. Instances and tables do not touch the door code at
   all. Mixed lists do pass the "two families or three types" value through, so they are written WITHOUT a new
   branch on it (`ComparisonDoorReadsRatchetTest` fails on one); slice 7 later changes how a rule is chosen, not
   what it computes, so the second touch is mechanical. Done: the judge's old member and `SideEqualities.values`
   deleted, the ledger's pins back under where they started, the measure at zero apart from database refusals
   listed by name.
3. **Slice 7: the doors, for comparisons**, built from the research's plan when it comes back
   (`edition-homework/step-6b/slice-7-half-page.md`, `slice-7-research-2026-11-28.md`). Done: the three pins at
   zero, an architecture rule that forbids the read, every door's recorded SQL and answers byte for byte as before.
4. **Step 8, re-specified first** (the user, 2026-11-28: "step 8 was ill specified and confused - it's supposed to
   take typed answers from lite only and do only final presentation work in java, instead it was building a bunch
   of rerun crap that made no sense, so it's not as decoupled as we thought, it needs all the typed lite work to
   come thru"). So step 8 does NOT run in parallel with the work above, and the handoff of 2026-11-27
   (`edition-homework/step8/HANDOFF-to-another-account-2026-11-27.md`) is not to be sent as it is. Its new
   statement, to write before any session starts it: Java at the edge receives the TYPED result lite computed and
   only writes it in legend-pure's or legend-engine's spelling; nothing is run again and no SQL is written for
   presentation; where lite's typed answer is not enough to write upstream's text, that is a gap in the typed
   answer, fixed in the product, not at the edge. What step 8's branch built that is not this is read and listed
   first, kept or dropped item by item.
5. **Step 9: test machinery out of the product.** After part 2, which deletes a share of it.
6. **The rest of the door's table into the type-checker's side, then step 10** (the old switch machinery deleted,
   the guards on: the rules AGENTS.md still marks "convention until the guards land").
7. **Step 11:** full rounds on every database, an outside audit, one pull request to `main`, the user's word.

## What makes a step done

A pinned number or a deleted file, never a description; the guard on before the claim is written into AGENTS.md;
every answer that moves, every test that goes from passing to refusing and every pin that goes up brought to the
user with its reason; one page of known gaps (`edition-homework/step-6b/for-the-user-2026-11-28.md` until a later
one replaces it).

## Kept so they are not lost

The lean-SQL follow-up (`database-recheck-2026-11-26/START_HERE.md`, researched, nothing built); about sixty
time-series words whose refusal check sits on rows a later filter drops; the `map` guard's gap inside nested
filters; Postgres at 169 of 210 on the measure; step 8's decisions D7 to D10
(`edition-homework/open-decisions-2026-11-26.md`), to be re-read under step 8's new statement.

## A lesson from 2026-11-28

The quick no-Bazel loop runs no guard. The first full round on slices 5 and 6 found about ten guard, ledger and
architecture failures at once, and one real SQL size regression in the asserts. From here `//core:guardrails`,
`//core:guardrails_edge` and `//core:census` run after each change that could move them, not at the end.
