# Re-audit: the fixes to the next batch, cd102a17..73e3a6c1 (/home/user/legend-lite)

I only read the code and ran git, building nothing. For what was run on these commits I relied on the coordinator's
list: `//pct:pct_h2_essential` 345/345, `AssertBarTest` 19/19, and the guardrails, guardrails_edge, census,
core_tests_root and core_edge_tests targets green.

**Blockers: none.**

## Each finding of the first audit

| # | Status | Notes |
|---|---|---|
| B1 | **Fixed** | Both pins deleted, with a dated comment. GATES.md says 82; the H2 record says 263 pass and 82 pinned, and its defect count went 23 to 21 (14 + 7, the list checks out). The four javadocs now name gate 7. The plan record's "135 dialect gaps" stays on purpose: the product's own Float `toString` on H2 is unchanged, and the H2 record says so. |
| S1 | **Fixed differently: sound; the record is partly wrong** | See section 1. |
| S2 | **Fixed differently: sound** | The departure from the research is recorded. `aGraphsSizeIsItsRootObjectsUnderEveryJudge` holds the root count. The to-many duplication case cannot be reached, because a graph fetch in a test body's execute is not planned under any judge; that is recorded as its own carried-forward row. The test reaches the children through `exists`, so it proves no duplication on a join, and it does not claim to. |
| N1 | **Recorded, not fixed; one word wrong** | The plan record says "a size that is NULL (which Pure's `size` never is)". The NULL in my finding was the EXPECTED size argument `n`, not `size(c)`. As written, the case it describes cannot happen. It should say "a size argument whose value is SQL NULL". |
| N2 | **Fixed** | `cw` deleted; DatabaseJudge's pin 828 to 827. |
| N3 | **Fixed** | Both messages fixed. |
| N4 | **Fixed** | `RunScope.planning` check added. |
| N5 | **Fixed** | The message and its golden line name all seven. |
| N6 | **Fixed** | The three links became `{@code}`. |
| N7 | Accepted note | Nothing to fix. |
| N8 | Not addressed | Low risk; no record. |

## 1. S1 and one row's cells (the coordinator's item 1)

**Is the construction sound?**
- `VerdictQueries.declared` resolves the exact `FunctionId` and throws if the platform does not declare it, so an overload is never picked by arity. That answers the S1 risk.
- The reason for not typing through the typer (only the parser and normalizer build a parsed node) is a real architecture rule.
- `native1` and `native2` still pick by arity. They are used only by older members of the class, outside this slice.

**Is keeping the refusal on one row's cells defensible? Yes.**
- At the base, all judges agreed on a wrong count of 1. Pure counts the cells.
- Now the database judge refuses by name, and so does BOTH, while the host judge still says 1.
- A refusal is better than a silent wrong agreement (G7), and it is pinned and owned by step 6.2.
- The cost is that the host-only mode still gives the wrong verdict. The record says so.

**Is every description truthful? No: two places are wrong, one is imprecise.**
1. **SHOULD-FIX: the plan record's S1 paragraph (about line 781 of `edition-plan-to-finish-stage-b-2026-11-25.md`)** says "the one such rewrite, a table's cells (rows times columns), concerns a side the judge refuses before it asks (a grid's envelope)". Two things in it are false:
   - It is not the one rewrite. The carried-forward row of the same record names a second: a row's `.values->size()` rewritten to the column count. That second rewrite is exactly the shape that moved.
   - The table's refusal is not the envelope's. `AssertBarTest` pins it as `RelationCountAsserts`' ModelException ("assertSize over a relation counts the relation as ONE value").
2. **SHOULD-FIX: the `VerdictQueries.sizeOf` javadoc** repeats "the judge refuses such a side before it asks (a grid's envelope read)". It has the same two errors.
3. **NOTE: the carried-forward row** says "the lowering refuses it by name". The pinned message is `DialectCapability: a tagged list reached the renderer`, so it is the dialect that refuses.

The `AssertBarTest` comment is accurate: it describes the move against the base, and says no judge gives 2 while another gives 1.

**Gap, unprobed:** the whole table's cells, `assertSize(<tds>.rows.values, n)`. That is the shape the typer rewrites to rows times columns (`TdsDesugars:742-753`). The records say it is refused before the judge asks, but no probe holds it. I could not read whether `<tds>.rows.values` is typed as a relation, which is what would send it to `RelationCountAsserts`. One more `AssertBarTest` row would settle it.

## 2. `assertNotSize` over a relation (the coordinator's item 2)

**Is the compatibility rewrite right? Yes.**
- `UpstreamTestBodies.asOne` replaces the relation with the literal `1`, kept only at multiplicity `[1]`.
- So `assertNotSize(1, n)` holds for every n except 1, which is upstream's answer.
- Its failure says `NOT_SIZE` about one value. No relation is printed, so there is no `assertEmpty`-style residue.

**Is the lite refusal right? Yes.** It suggests `assertNotEquals(<size>, $relation->size())`.

**Did anything else switch on the three-assert set? Nothing missed.** The only places that do are the two that were changed: `RelationCountAsserts.walk` and `UpstreamTestBodies.asOne`.

**Left to do:**
- **SHOULD-FIX: no test holds the compatibility rewrite for `assertNotSize`.** The `AssertBarTest` rows run with `NoEdgeRun.NONE`, lite's door, so they hold only the refusal. There is no `RelationCountAssertsTest` or edge test row either.
- **SHOULD-FIX: the S137 row needs the user's word.** It now says "`assertNotSize` held to the same rule 2026-11-26, to be confirmed by the user". A row the user has not ruled is a defect under the upstream-fidelity rule. It is not in the plan record's waiting-on-the-user list (6b). Add it there and bring it to the user.
- **NOTE: the S137 row's compatibility sentence was not extended.** It still says "the test planner refuses the three" and states no compatibility answer for `assertNotSize`.
- **NOTE: stale javadoc.** `TestPlanner.lifted`'s javadoc still lists three asserts.

## 3. `NativeCallStampGuardTest` 7 to 6 (the coordinator's item 3)

**A real refactor, but the "shrink" is cosmetic, and it slightly blinds the guard.**
- The guard counts construction SITES. Its rule is that a rising count is a new construction, and the pin moves up with a dated note.
- `declared` added a site, which would have made 8. Routing it, `native1` and `native2` through one helper, `built`, gave 6.
- The number of calls built without a stamp did not change. Every one is still a NEW call, never a copy of a call the class was given, so the guard's actual question (does a rebuild drop the stamp?) is still answered correctly today.
- But any future caller of `built` in this class, including one that rebuilds a call it was given, is now invisible to the guard.

**Recommended fix (NOTE):**
- Either say so in `built`'s javadoc ("new calls only; never a rebuild of a given call: that passes the stamp"),
- or make the dated note honest: "a construction added and three folded into one helper; the count of unstamped new calls is unchanged".

The note's word "a shrink" overstates what happened.

## 4. The H2 pins and counts (the coordinator's item 4)

Checked under B1 in the table above: correct and consistent. The plan record's earlier progress-log totals (229 pinned, 212 new) are dated log entries, so leaving them is fine.

## New problems in the fix diff

Nothing new is broken.
- `declared`'s `orElseThrow` follows the no-fallback rule.
- `comparedAsInAQuery` gets a `TypedNativeCall` with no cast now.
- I found no other change of behaviour.
