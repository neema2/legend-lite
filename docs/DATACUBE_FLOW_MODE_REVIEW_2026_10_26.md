# Flow mode review (2026-10-26)

The independent audit of branch `claude/datacube-flow-mode` (DataCube's flow mode -- a visual ETL canvas of boxes, each
a grid doing one step -- its page commands, its Python API and its example page: `29e25237..2395210b`, 131 files,
+24k lines, PR #75), run before the PR left draft (the owner, 2026-10-26: "Do we need an independent audit ... Should we
put up a PR in parallel right now or wait" -- a draft PR and the audit in parallel). Four reviewers read the diff
independently, each against the repository's own rules rather than the author's account of them, and each probed what
it found:

| Reviewer | Question | Judged by |
|---|---|---|
| P | Is every value the database's, through the platform's queries -- the Python paths too? | AGENTS.md's execution tenet and invariant 4; TENET_CHARTER C1-C5; WORLD_MAP; the owner's words for this feature |
| A | Is the canvas one API: the screen and Python making the same page commands? | DATACUBE_ONE_API_PLAN_2026_10_21 (L1-L12); the ledgers |
| G | Are the repository's written rules and guards kept? | AGENTS.md whole (guard files, invariant 4, common mistakes); every guard the diff moved |
| C | Are the tools right? | each tool's documented meaning and Alteryx's; every tool's real query planned by legend-lite's planner and run on DuckDB (a harness over `join-open.ts openRows`, as the box after it reads) |

A finding is listed as confirmed when a probe showed it or the code shows it directly. Each fix below is pinned by the
test named beside it; the C reviewer's probes are in the session scratchpad (`audit-q/`), re-run after the fixes.

## 1. Blocking -- fixed

| # | Finding | Fix | Pinned by |
|---|---|---|---|
| C1 | **A Sort box's order reached neither the box after it nor its Output**: every next box read `classLambda` (no order), so ids came back 1..5, not 2,1,5,4,3 | `query.ts handedOnLambda`: classLambda's rows in its sorts' order, uncapped -- what the box after it reads (`join-open.ts inputRows`) and its Output writes; a class's rows keep no order | `join-open.test.ts` "a Sort box's order reaches the box after it" (DuckDB) |
| B2 / C9 / C10 | **An Output had two writers**: the tab's (`toCsv`, its formula guard turning `+44 20 7946 0958` into `'+44 ...`) and Python's (`pandas.to_csv`: hidden columns only, renames, order and in-place names lost) | one writer, `flow-output.ts outputCsv`: in its sorts' order, the columns it hands on under their names (`handedOn`, `handedOnRows`), each value as it is (`export.ts` `defuse: false`); the tab's Write now and Run, and Python's `flow.run` through headless `gridOutput` -- the same text, BOM and CRLFs | `join-open.test.ts` "an Output's file" (DuckDB); `test_page.py` `test_an_output_writes_what_the_box_hands_on` |
| B1 | **Load values turned a Decimal's text into a double** (`12.10` became 12.1; `12345678901234.567` rounded) | `grid-gestures.ts pivot.pin`: a Decimal's values kept as their text, every digit | `grid-gestures.test.ts` "a Decimal's values kept as their text" |
| B7 | **A stopping box never judged let the run write**: only `testFails === true` stopped it; a refused grid, or a Cross Tab question the database refused (`.catch(() => undefined)`), wrote every Output | `layout/flow-board.ts runStoppers`: a stopping box stops the run unless it passed -- failed, or not judged, each said ("could not be judged (why)"); the refused question kept in its words | `flow-page.test.ts` "stops a run on a stopping box that fails or was never judged" |
| B3 | **A Join box was named one way by the screen, another by Python** (the palette's names; the grids' sources') | `flow-boxes.ts boxNamed`, `pageBoxName`: the canvas's rule -- its title unless it is a new grid's, else its tool's -- for the canvas, the Join window and headless `joinCube` on a flow | `flow-sheet.test.ts` "a box's name"; `test_page.py` `test_a_join_box_reads_both_live_and_is_set_again` |
| B4 | **A Select with no order named (Python's) put a retyped column last; the screen's window names every row** | `select-columns.ts selectCommands`: no order named, the grid's own -- the same commands either way | `flow-sheet.test.ts` "hands its columns on in its order" (fails without the fix) |
| B5 | **Python could not add a box straight onto a Join's L or R** (headless `rowsCube` had no port) | `rowsCube` takes `port` (`'false'`, `'left'`, `'right'`), as the canvas adds a box on one; `page.grid(input=..., port=...)` | `test_page.py` `test_a_join_box_hands_on_the_rows_each_side_pairs_with_none` |
| B6 | **A container moved from Python left its boxes behind** | `layout/flow.ts containerMoves`: the container and each box inside it by as much -- the canvas's drag and Python's `text.place` (headless gesture `flow.place`) | `flow-sheet.test.ts` "sets a container's frame"; `test_page.py` `test_a_container_frames_boxes` |
| C2-C4 | Data Cleansing's punctuation took out letters not ASCII; "largest first" and "last N" kept empties ahead of the largest; Count Records of no rows gave no row | see section 6 (the per-tool fixes) | section 6 |

## 2. Should fix -- fixed

| # | Finding | Fix | Pinned by |
|---|---|---|---|
| C11 | A Select swapping two names (a as b, b as a) was accepted, then the box after it refused | `join-open.ts handedOnRows`: renames that take a name another column has go through names of the product's own first | `join-open.test.ts` "a Select's swap" (DuckDB) |
| S1 | A layout made again on top (an undo, another writer's step) dropped a box's Output and its stopping | `page-rebase.ts rearranged` keeps them as the page has them; `page-between.ts` sets each by its own command (`setOutput`, `setStops`) | `flow-sheet.test.ts` "follows the page's renames" and "sets a Test box to stop a run ... undone" |
| S2 | Duplicate was the screen's alone; Python's `grid.copy()` of a box went after the box copied | `page-gestures.ts duplicateCommands` (gesture `flow.duplicate`): after the box it reads, as the canvas's Duplicate | `test_page.py` `test_a_box_duplicated_reads_what_it_reads` |
| S3 | Python's Fuzzy Match spelt the Join window's default distance | `within=None` leaves it to `join.ts` (`SIMILAR_WITHIN`) | existing `test_alike_text_matched_as_fuzzy_match` |
| S4 | A DateTime's and a RegEx's default names, and a format's "has a time", were spelt in Python and in the windows | `date-tools.ts dateToolName`, `formatHasTime`; `text-tool-columns.ts regexName` -- the readers supply them, Python sends none | `flow-sheet.test.ts` (the readers' defaults) |
| S5 | `setStops` took any box (one that checks nothing would now stop every run) | refused unless a Test (its shape, read without the checks' readers) or a Cross Tab with its columns fixed | `flow-sheet.test.ts` "sets a Test box to stop a run" |
| S6 / S8 | The Cross Tab's value questions were read twice (screen and headless), a missing column read as none; the lacking count said 501 for "more than 500" | `query.ts pivotValuesOf` (one reading, beside the questions; an answer without its column refused), `tooManyValues` (Load values refused past what a pivot shows), `cross-tab.ts lackingWords` "at least N more" | `flow-sheet.test.ts` "a Cross Tab's values as its questions answer them" |
| S9 | The Pure-text allowlist's two windows had no pinned counts | pinned: `multi-field-form.ts` 4, `pure-form.ts` 9 (dated) | `guardrails.test.ts` |
| S10 | The gestures floor (79) had not followed the list (83) | 83, dated | `one-api-gestures.test.ts` |
| S11 | The Pure and Custom Filter windows opened blank when the compiler could not print their function -- OK would have set another | refused to open, in the compiler's words; the Pure window says why its columns are missing | read; `verify_flow` |
| S12 | A timeseries of several grids, one a flow's box, was refused (its input not resolved) | resolved against the page first, on the tab (`timeseries-together.ts`) and headless (`join.ts sourcesOnHost`) | `test_page.py` `test_a_timeseries_of_a_box_and_another_grid` |
| S13 | Python's cleanse, impute and multi-field defaulted to beside their column; the windows start in place | `in_place=None`: in place on a flow's box, as the windows start; beside on a board's grid | `test_page.py` `test_tools_in_place_hand_their_columns_on_by_their_own_names` |

## 3. The startup's size

The fixes add 2,458 bytes gzipped to a grid-only page's startup (405,574 -> 408,032; the engine page 393,657 ->
396,140; a notebook's module 421,003 -> 423,483), the ceilings raised by that with dated notes
(`bundle-budget.test.ts`). A first cut added 9,421: the checks' readers fetched at start by `setStops`, and three new
lazy modules splitting the startup's shared chunks into 41 files from 33 (esbuild makes a chunk of each set of
importers; each file is gzipped alone). The Output writer and the Cross Tab's values now read only what the page has
at start and come with it; the Test's shape is read in `page-commands.ts`; the startup is 33 files again.

## 4. Notes -- fixed where small

- `pureText` and `customFilterText` read `true || ...` as a lambda named `true`: a name then `||` is an or (tested).
- The page gestures made every error a refusal: only the page reader's (`PageDocumentError`) are now (`pageOf`).
- The Join refusal named `flow.join`, which does not exist: `page.join(mine, theirs, how)`.
- A doc comment of `join-sources.ts` was above the wrong function; an undated `state-guardrail` pin is dated; the flow
  mode doc's stale rows (Filter, Profile, Comment and Container, Select and Dynamic Rename, `oneModel`, the Cross Tab's
  check) say what is built.

## 5. Stays, and why

- **Find Replace with a lookup that repeats a key repeats the row** (C13): the Join window counts repeated keys and
  warns before the box is made; one match per key needs a join form of its own (the first of theirs per key), recorded
  for a later step rather than built here.
- **Paste is the screen's** (the clipboard is the browser's); its commands (`addGrid`, `setInput`) are Python's too.
- **A box's words use the browser's locale** for its counts (`toLocaleString`): words, not values; Python reads values.
- **`Grid.inputs` in Python reads the source's sides from its document**: a read of the document's shape, not a rule.
- **The WIP commit `88e0db52` stays in the branch's history**: the PR is merged by squash, which drops its message;
  the branch's history is not rewritten.

## 6. The per-tool fixes (reviewer C)

Each re-run on DuckDB through the review's harness (each tool's commands, the box written, opened as the box after it
reads it, planned and run), before and after:

| # | Finding | Fix | Pinned by |
|---|---|---|---|
| C2 | Data Cleansing's "punctuation out" took out every letter not ASCII (`Côte d'Ivoire` → `Cte dIvoire`; `東京` → empty) | `text-tools.ts PUNCTUATION` `[^\p{L}\p{N}\s]`: any script's letters and digits kept | `flow-sheet.test.ts` (text tools) |
| C3 | "Largest first" (Unique), "last N" (Sample) and the Top N kept rows whose value is empty ahead of the largest (`DESC NULLS FIRST`) | a falling order led by a hidden `isEmpty` key, for these tools alone (the grid's own sort unchanged) | `flow-sheet.test.ts` (Unique, Sample); `value-filter.test.ts` (Top N) |
| C4 | Count Records of no rows gave no row (a grouping gives no group of none) | `query.ts`: a grouping by columns of one value alone is every row aggregated (`aggregate`, no key), each key its value beside; Count Records counts, the Test counts its rows -- 0 of none -- and judges a row of 0 rows (`checks.ts failingChecks`) | `join-open.test.ts` "Count Records of no rows" (DuckDB); `flow-sheet.test.ts` "judges a test of no rows" |
| C5 | Date Filter "between two days" on a DateTime refused (a protocol error), and would have dropped the last day after midnight | a date-time's `>= fromT00:00:00` and `< next dayT00:00:00` (`date-ranges.ts`), the gesture passing its column's type; a time of day refused and left out of the window's columns | `flow-sheet.test.ts` (Date Filter) |
| C6 | A Union of two columns of one name and two types was typed as the first's | refused in words (`join.ts appended`) | `join.test.ts`, `join-open.test.ts` |
| C7 | Text To Columns "into rows" dropped a row whose text is empty | `split(coalesce($x.c, ''))`: the row kept, one empty part | `flow-sheet.test.ts` |
| C8 | Test "repeats no value" counted repeated empties as repeats; a row lacking a count read as 0 | empties repeat nothing; a missing count thrown (invariant 4) | `flow-sheet.test.ts` (Test) |
| C12 | Generate Rows "each day" between two date-times overshot by a day | a date-time bound refused, its day first | `flow-sheet.test.ts` |
| C14 | Unique's and Sample's row numbers could tie, the kept row the database's pick (U and D, S and R could overlap) | ties broken by every other sortable column, rising (`unique.ts rowNumberBy`) | `flow-sheet.test.ts`: U [1,4,7], D [3,2,6,5], a partition |
| C15 | An in-place column went to the end of what a box hands on; a negative fill (-1) was not read back; the F port's comment said empties go nowhere | in its column's place (`box-columns.ts inTheirPlace`); the `minus` read back; the comment says F | `flow-sheet.test.ts` |
| -- | Append Fields' summary divided to count theirs; Profile computed its share, empty count and diagonal in JS, and gave no row of no rows | their rows counted by the database; Profile's one row a keyless `aggregate` (a row of 0s of no rows), its empties and share the database's, a column's own correlation 1 only where the database says it varies | `join-open.test.ts`; `flow-sheet.test.ts` (Profile) |
| -- | A Top N box's rank columns (`__all__topN`, and the new empties key) were handed on to the box after it | the product's own columns (`isMachinery`) left out of what a box hands on and of its Output | `join-open.test.ts` |

A falling Unique or Sample "last" saved before these fixes (without the empties key) is no longer read back as the
tool: only documents made on this branch before it lands.
