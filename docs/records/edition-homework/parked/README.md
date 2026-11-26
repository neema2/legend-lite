# Parked work, kept as patches (2026-11-26)

*Two pieces of the edition program are built and wait on the user's word. Their branches are local to the session
that built them, which a cloud container does not keep, so each is saved here as the text of its change. Apply with
`git apply` on the commit named, or read as the record of what was built.*

| File | What | Built on | Waits on |
|---|---|---|---|
| `parse-rows-d56c9d6b.patch.txt` | The plain parses as rows of legend-engine's table: `parseInteger` and `parseFloat` inside a store query are the database's own cast (`NamedRule.TEXT_CAST_TO_INTEGER`, `TEXT_CAST_TO_FLOAT`; `SqlFn.CAST_INT`, `CAST_FLOAT`, an arm in every dialect); the edge's pass that unguarded every parse in every compatibility style is deleted | `b5e484ba` (the session branch after step 5's start); `git diff b5e484ba d56c9d6b -- core pct spec` | `InterimEdgeSitesTest`'s ceiling 4 to 5 (it moves up only with the user's word); then two corpus size pins re-pinned down, and a round on the server and cloud lanes |
| `acos-asin-92e828cb.patch.txt` | `acos` and `asin` outside [-1, 1] raise as compiled legend-pure does (the user's ruling of 2026-11-25), within the domain as their own SQL functions; the two PCT error tests leave every database's expected failures; register S119 and the differences page | four commits on `b4c9bf0a` (`04260625`, `a7139a3e`, `9652b36a`, `81dde6ec`), each shown in the file | whether the product's SQL size pins may grow for this ruled check (41 grow, 10 shrink); then the list in the plan record's section 6b, item 7 |

The plan record (`docs/records/edition-plan-to-finish-stage-b-2026-11-25.md`, section 6b, items 7 and 12) has the
measurements for each.
