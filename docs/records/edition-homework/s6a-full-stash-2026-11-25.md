# S6a in full, kept aside (B2, saved at the handover of 2026-11-25)

The full S6a was built in B2's worktree on 2026-11-25 and kept as a git stash (`s6a-wip-2`) for the user's ruling on
legend-engine's door. At the handover of the orchestrator role it is saved here as a patch, so it outlives the
container. Documentation only: nothing in it is built on.

- **The patch:** `s6a-full-stash-2026-11-25.patch.txt` (the stash's tracked changes, then its one untracked file,
  `core/src/main/java/com/legend/edge/PureValueText.java` as it stood then). The `.txt` suffix keeps it from being read
  as source.
- **Its base:** `413960af9652035b0455ae3b42eaf6ab8446ccb7` ("B2b: the review of the CSV rulings' commit (e4bfa8677)").
  `git apply --check` passes on that commit (checked at the handover). It does not apply to the branch's head as is:
  the cell printer (`PureValueText`) and parts of the records landed since, and toCSV's Float cell is now a node.
- **To use it:** `git worktree add <dir> 413960af9`, then `git -C <dir> apply <this patch>`.

## What it does (28 files, +368/-458, then the new file)

- **Deletes the edge's value print passes** (`edge/PurePrintForms`): the Float text (legend-engine's in-query cast with
  its literal processors), the DateTime `+0000` composition (`edge/PureDateTimePrint` deleted) and the TDSNull joins,
  so both compatibility doors write the product's value texts in SQL. F31's parses (`UnguardedParses`) stay.
- **Lifts a last step's value print at the doors:** `PrintedRelations.liftValue` reads a last-step `x->toString()`
  (`x` a Float, with literal replaces) and the doors' printer (`PureRelationTextWriter`'s `VALUE` form) prints it in
  Java with legend-pure's Float print (`PureValueText`, `DecimalFormat("0.0")`); PCT's route (`PctPlanRoute`) asks it.
- **One value printer (G1):** `PureValueText`, the table printer's cell rules moved from `PureRelationText`.
- **The guards and tests that move with it:** `WriterCarriageGuardTest`, `JavaEvalLedgerTest`,
  `OpenClassMembersTest`, `EdgeSeparationTest`, `CompatSpellingGuardTest`, `AuditRound3Test`; `PrintedRelationsTest`,
  `PureRelationTextTest`, `FloatTextTest`, `EdgeSpellingHookTest`, `TypeInferenceIntegrationTest`,
  `PureRelationTextParityTest`, `PctPlanRoutePrintTest`; register S404's draft and the differences page.

## What was measured (docs/records/compat-b2-progress.md, "S6: the edge's value print passes, measured")

A temporary probe turned off the three passes (F31's parses kept) and every compatibility lane ran on it:
- **PCT channel A:** `testFloatToStringWithNegativeExponent` on DuckDB and SQLite (lite's six decimals print `0.0`).
- **The engine lanes:** that case on DuckDB PASS -> FAIL; on H2 `testFloatToStringWithPositiveExponent` FAIL -> PASS
  (Pure's `134210000.0` where legend-engine's H2 cast prints `1.3421E8`).
- **The corpus:** DuckDB +114 failing, H2 +109; the unordered registers -65 each. 112 / 107 are legend-engine's
  `TDSNull` (its empty TDS cell prints, joins and sorts as `TDSNull`: 76 a row's cells joined, 10 a cell's toString, 8
  both, 16 others, on DuckDB); 2 / 2 the validation showcase (an integer-held Float in a constraint message used
  further: legend-engine `1000000`, lite `1000000.0`).
- **DateTime:** no move anywhere.

## Why it is held

The user questioned whether legend-engine's door should give lite's text at all; the orchestrator held S6a's
legend-engine-door parts (the passes' deletion, which both doors share), and then B2b's design audit of S5a to S6a
removed the landed legend-pure-door lift too: it left a print used further to the passes, two printers at one door
(`(1.0E-5*1.0)->toString()` printed `0.00001` while `...->toString()->length()` read the cast's `1e-05`). The cell
printer `PureValueText` landed as a move. The held parts become edition rows under the v4 direction
(docs/records/edition-v4-direction-2026-11-25.md on `claude/compat-b3-nufdlk`).
