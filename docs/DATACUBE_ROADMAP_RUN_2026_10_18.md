# The DataCube roadmap run (2026-10-18)

The user, 2026-10-18: "run the whole roadmap in this session with each phase audited and stacked on next here, and do
not stop until whole roadmap is done; if you can't figure something out or don't know what to do, skip it, make a
note and we'll come back to it at the end."

This file is the run's ledger: the order, each phase's branch and state, and every item skipped with why. It is kept
on the newest branch of the stack and updated as each phase lands on its branch.

## How each phase is run

- **Its own branch, stacked on the one before**, pushed with no PR: CI is contended, and a branch with no PR runs no
  CI (the user, 2026-10-18). Nothing goes to `main`; nothing merges without the user.
- **A short design note first** where the phase has choices to make, then steps, one commit each, each with its
  tests (a fix with a test that fails without it).
- **Checked locally before the push:** the suites the phase touches (`//datacube/...` and `//python/...` for the
  DataCube line; `verify_app` as an unprivileged user).
- **Audited:** an independent reviewer that wrote none of it reads the phase's diff before the next phase starts;
  its findings are fixed, or answered here.

## The order

| # | Phase | Branch | State |
|---|---|---|---|
| — | #57: header sorting, pivot totals | `claude/epic-rubin-ay7upl` | merged |
| — | Command program stage 1a: the test foundation | `claude/datacube-command-tests` | done, audited |
| — | Command program stage 1: the grid half | `claude/datacube-commands` | done, audited |
| 1 | Command program stage 1: the page half (page parity tests first) | `claude/datacube-page-commands` | done, audited |
| 2 | Command program stage 2: one page state, page-level undo | `claude/datacube-page-state` | done, audited |
| 3 | Command program stage 3: the live channel between Python and an open page | `claude/datacube-live-commands` | done, audited |
| 4 | Graph mode: design note, then the G0 spike over main's Cypher island | `claude/datacube-graph-mode` | done, audited |
| 5 | Charts batch 4: the statistics aggregates and the kinds they unlock | `claude/datacube-charts-stats` | done, audited |
| 6 | Charts batch 5: maps | `claude/datacube-charts-maps` | done, audited |
| 7 | Charts batch 3's leftovers: Marimekko, waffle, Gantt | `claude/datacube-charts-more` | done, audited |
| 8 | The other line's small follow-ups (the identifier rule once; the pinned queries compiled in a test; a Python refusal that drops keys) | `claude/datacube-followups` | the first done, audited; the other two skipped |
| 9 | Dashboards on classes (the other line's F1-F5): page filters and cross-filtering, joins when preparing data, the model view, promote and publish, measures in the model | -- | skipped then; taken over 2026-10-19 (below) |
| 10 | The model plan's step 3: dashboards on classes (F1) | `claude/datacube-model-dashboards` | done, audited |
| 11 | The model plan's step 4: combining in prep (F2: joins, as a command API driven headless) | `claude/datacube-model-prep` | done, audited |

## Skipped, to come back to at the end

- **Previews (hosted per branch):** needs the user's Netlify account and the two repository secrets, and deleting the
  stale `preview` branch (the session's proxy refuses branch deletion).
- **"Get SQL out of clients":** its design waits on the user's go.
- **D5b (Snap's queries become Pure):** on the other DataCube line's own branch, and in the SQL plan's ground this
  line was told to leave alone (`engine.py` snap/frames/project-data, `_headless._query`, `warehouse.py`).

## Notes as the run goes

- **Phase 1, a choice for the user:** a chart with problems -- Python's `grid.chart(...)` refuses it in DataCube's
  words; the screen keeps it and shows the words on the chart (the homework's §8). One of the two should be the rule;
  left as it is until the user says which.
- **Phase 1, left to stage 2:** the screen tells `addChart`, `addGrid` and `setChart` once one page state holds a
  chart's spec and a grid's cube document as they are made.
- **Phase 1's audit** (an independent reviewer, six findings, each fixed with a test checked to fail without it): a
  `layout` command read as a saved page's sheet is (`readLayout`), so Python's `layout_from` refuses a bad shape in
  the page reader's words again; a cube kept for a detached chart has its id taken (`idsOf`, `freshId`); `addGrid`'s
  document read by `readCube`, from the commands' exact numbers; an editing grid's rename or move told as nothing; a
  grid given its made title back told as no title; the design note's `addChart` row. Answered, not changed: Python's
  refusal for a tile not on a sheet (`Sheet.layout`) and for a stack across sheets (`Page.stack`) are DataCube's words
  now, naming the tile's id and "move them there first" rather than Python's handle and `sheet.add(tile)`.
- **Phase 2** (`docs/DATACUBE_PAGE_STATE_2026_10_18.md`): one page state on a page of its own, Undo and Redo for every
  structural edit. Its audit: nine findings, eight fixed with a test each (checked to fail without it); one answered:
  an undo or a redo is told to a host following the page by commands as no command (no host follows it yet; the
  reverse channel will need it). Left to stage 2's next step (not built): one undo for a grid's own edits and the
  page's together.
- **Phase 3** (`docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md`): Python serves each change as the commands it was, and an
  open page applies them rather than opening the whole page again. Its audit: eight findings, all fixed with a test
  each (checked to fail without it). Answered, not changed: two views of one page can still differ after both edit
  without reporting (each keeps its own edits under Python's; the first `page.read()` after one reports serves the
  page whole). Next, not built: the reverse channel -- the open page's edits sent to Python as commands rather than its
  whole document.
- **Phase 4** (`docs/DATACUBE_GRAPH_MODE_2026_10_18.md`): main merged into the stack (one conflict, `docs/GATES.md`,
  both sections kept); the G0 spike -- Cypher planned by the tab's own planner and run on its DuckDB, and the island
  as a node of the tree DataCube sends -- and a design note proposed to the Cypher line, its questions for them. Its
  audit: seven findings, all taken (the headless roster, the note's claims on identities, `USE` and paths corrected
  against main, the spike's G1 case through `plan`). **For the user / the Cypher line:** the note's questions (an
  identity a drawing can key by within the study's decision C5; which graphs G1 lists); G1-G4 are not built.
- **Phase 5** (`docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md`, Batch 4): the quartiles (`percentile25`, `percentile75`)
  as aggregates of the grid and of a chart, and the box plot -- one number column's five figures per category, the
  database's -- with a Statistics sheet (box plots and a histogram) on the "more kinds" example page; Python's
  `GridView` reads a `Number` column's exact decimal as a `Decimal`. Its audit: seven findings, all taken with a test
  each (checked to fail without it): the box column read from the chart's own calculations too; a click on a batch 3
  kind's period or bin filters to its range (it threw on a range or box plot by month, and on a calendar's day);
  leaving a box plot keeps its median, and a pie, a lollipop, a KPI and a heatmap refuse a second value by name; the
  five figures compared by column and aggregate, another order and a value's own mark or axis refused in their own
  words; no "Show values" on a box plot; the doc corrected; a pivot cell's quartile planned on DuckDB. **Skipped, to
  come back to:** `corr`, `covar*` and `maxBy`/`minBy` (each reads a second column per measure, which a measure has no
  slot for -- the correlation heatmap, the trend line and the candlestick wait on it, the candlestick also on a
  `prices` sample); percentile reference lines (a percentile window, a new `WindowFunction`); the ECDF (a `cumeDist`
  show-as); the violin, error bars, hexbin and KPI change. Noticed, not changed: "Show values" also does nothing on a
  radar, parallel coordinates, a polar bar, a Sankey, a streamgraph and a calendar.
- **Phase 6** (`docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md`, Batch 5): a map (each country by its ISO code or name,
  coloured by a value) and a bubble map (a bubble at each place's latitude and longitude, the new `at` slot), on Natural
  Earth's 1:110m countries in the Equal Earth projection; the drawing and the shapes, and ECharts' map chart, are
  chunks of their own; Python writes both; a Maps sheet on the "more kinds" example page. **For the user:** the design's
  open question 2 (the map-preparation tool) taken for now as "prepared once and committed with how it was made"
  (`datacube/tools/maps/prepare.py`, standard library, the published file's sha256 pinned) -- say if a build-time run
  is wanted instead. Its audit: eight findings, all taken with a test each (checked to fail without it): no Open in
  grid on a map (Update would not change its places) and its pill says it does not regroup; a map's limit kept as the
  world's from a date across, and a chart leaving a map takes its own default again; no grain or bins on a map, a
  bubble map's name not one of its places, a group-level value reading a place allowed; a place with no value said;
  Python's `Chart.plot(at=...)`; the form's places listed once and no legend on a map; the ECharts code only the kinds
  and maps share given a budget. **Skipped, to come back to:** provinces and states (one file per country), the borders
  as each of 31 countries draws them (a deployment's default and a user's setting), the US with Alaska and Hawaii set in,
  a user's own shapes.
- **Phase 7** (`docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md`, batch 3's section): a Marimekko and a waffle (shares as
  cells, each cell's edges or range the database's: running totals, a lag, rounding) and a Gantt (two dates per row
  on a time axis); Python writes them; on the "more kinds" example page. Its audit: ten findings; nine taken with a test
  each (checked to fail without it): a whole of nothing divided by zero (DuckDB failed the cast, Postgres the
  division) -- now empty shares, said; a waffle's ranges meet (its start the lag of the range before); values below
  zero said on a waffle and a Marimekko; a Marimekko cell's click on a period filtered to the wrong one; its column
  names overlapped the axis's labels; a Gantt took a time of day; a map's limit spread overrode a kind's order; form
  options these kinds do not draw; a Gantt's start picked a sum. Answered, not changed: "At most" counts a Marimekko's
  cells, not its columns -- a cut chart says so in the panel's own words ("The first N by the chosen order"); a
  Marimekko's own limit is a choice left for later.
- **Phase 8** (branch `claude/datacube-followups`): the identifier rule once -- pure-protocol's `isIdentifier` (an
  identifier's letters, not `true` or `false`), which DataCube asks everywhere it asks whether a name is bare (a column
  reference, a frame source, a file's table name, a suggested package, the column editor's lambda head); a column named
  `true` is spelled `$x.'true'`. Its audit: six findings; four taken (the file's table name and the suggested package,
  the comments, the grammar test's assertion), each fix with a test checked to fail without it. **For the compiler
  line (core, not changed here):** the compiler's printer (`PureComposer.isUnquotedIdentifier`) and the table-model
  writer (`CatalogModel.ident`) still write `true` and `false` bare, so a column named `true` from a file fails before
  DataCube sees it, and Edit Column's print of `$x.'true'` reads back as a literal; quoting them where upstream does
  not needs a `docs/SEMANTICS_REGISTER.md` row. **Skipped, to come back to:** "the pinned queries compiled in a test"
  and "a Python refusal that drops keys" -- the run could not find which queries and which refusal the other line
  meant (no record of them beyond the one line), so nothing was guessed at.
- **Phase 9, skipped:** F1-F5 are steps 3-7 of the model plan (`docs/DATACUBE_MODEL_DESIGN_2026_10_10.md` §13), the
  other DataCube line's, in flight there in their order (`docs/IN_FLIGHT.md`, "Now: the model plan"); the user's
  ruling of 2026-10-14 puts page filters and cross-filtering on classes, as that plan's step 3; step 6 (promote and
  publish) is built on the Studio line's SDLC and Depot, its shape brought to that line first; step 7 (measures in the
  model) is BI and ETL plan B4. Building them here, without coordinating (this run messages no other session), would
  collide with that line. Where F1 would plug in, if the user wants this line to take it: a page filter as one more
  condition of each tile's query over the same class, given to the grid's controller (`cube.ts`, where each level's
  query is built from the snapshot) and to a chart's snapshot (`ui/chart-panel.ts`), never into a tile's own saved
  filter; a chart's click told as that page filter instead of its own grid's (`chart-render.ts` `onPick`); the page
  document and the page commands carry it, so Python, undo and the live channel follow.

- **Taken over, 2026-10-19 (the user: "we should take everything over from the other datacube lane, it's dead"):**
  the model plan from step 3 (`docs/IN_FLIGHT.md`, this run's entry). **Phase 10, step 3, dashboards on classes**
  (`claude/datacube-model-dashboards`; the design note and its "as built" are `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md`
  §16): a cube source over a class at the page's pin, **Add a Grid over <Class>**, page filters by a class's property
  (the page document's version 5, two page commands, a lens on every query a grid runs, the filter bar), a chart's
  click as one. Audited by a reviewer that wrote none of it: ten findings, all fixed, each with a test that fails
  without it (a filter id given twice after a reopen; a class grid opened before its file's grid; the page replaced
  while a class grid was added; a mark's calculated-column part; two quick clicks; Open in Grid's grid; the bar's
  failed fetch said; a lens that matched nothing refused; defaults made refusals; stale words). **Not built, named:**
  Python's pages over classes (step 3b: Python's pages are frame-only end to end -- load, read, serving -- so
  `page.grid(page.cls(...))` and `page.filter` wait on that; headless `pageFilter` is ready for them); the source
  picker's Classes section; re-pinning open class grids when the page re-pins (step 5).
- **Phase 11, step 4, combining in prep** (`claude/datacube-model-prep`; the design note, the study of upstream's
  Legend Cube, an Alteryx-like prep flow proposed, and its "as built" are `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md`
  §17): a join of two grids' results as a grid of its own -- any condition (four kinds, every pair), as of, keep my rows
  with a match or with none, append -- its source by value in the cube document, added by the page command every grid
  is (`addGrid`), so undo, save and the live channel follow; the model of several tables written by core's writer
  (the one core edit); Join… in the tab, saying what it will do before it is applied; headless `joinCube` and
  `joinQuestions` (Python's test drives them over two frames). The user's ask on 2026-10-20: no compiler rule copied in
  TypeScript -- join.ts writes every name as given and the compiler refuses what is wrong, in its words. Audited by a
  reviewer that wrote none of it: thirteen findings and three plausible ones, all fixed or named (§17.8). **Not built,
  named:** Python's `page.join` (step 4b, `engine.py`: the SQL plan's ground); live links and the flow view; Create a
  Class of a joined grid; a remote input's keys on reopen; the engine planner's booleans in a join; an append of a
  precise decimal (the compiler's line); remove duplicates as a step.

## At the end: what to come back to

- **For the user to decide:** the chart-with-problems rule (phase 1: refuse, as Python does, or keep and show, as the
  screen does); the map-preparation tool (phase 6: prepared and committed, taken for now); whether this line takes F1
  (phase 9).
- **For other lines:** the graph mode questions for the Cypher line (phase 4); `true`/`false` quoted by the compiler's
  printer and table-model writer (phase 8, core); D5b (the other DataCube line's, in the SQL plan's ground: asked of
  the user). F1-F5 are this run's since 2026-10-19 (phase 10 on).
- **Not built, named in each phase's note:** undo told to command-following hosts (phase 2); the reverse live channel
  (phase 3); corr, covariance, minBy/maxBy and the kinds waiting on them, percentile lines, the ECDF and the rest of
  batch 4 (phase 5); provinces, the borders by viewpoint, the US insets, a user's own shapes (phase 6); a Marimekko's
  own limit (phase 7); phase 8's other two follow-ups.
- **Skipped up front:** previews (Netlify secrets), "SQL out of clients" (waits on the user), D5b.

