# One keeper per page: homework before the build (2026-10-20)

Why: the user asked that Python and an open tab never get out of step on a page they both edit
(docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8's real gaps all came from two copies of one page drifting). The plan
proposed: the host keeps the page's one document; every write from either editor is versioned; a write against an old
version is re-applied as commands, or refused. Before building it, two questions: (1) which of the tab's changes are
commands already, and (2) where the line between the shared page and each editor's own view state falls. This note
answers both from the code as it is (claude/datacube-query-base at 1a388acb4) and says what changes in the plan.

## 1. Every way the tab changes the page

The tab already produces commands for most edits -- grid edits as `GridCommand`s (app.ts `onCommand`), structural
page edits as `PageCommand`s (page/cube-page.ts `onPageEdit`) -- but nothing sends them anywhere: the live report
(datacube-app.ts `liveChanged`) posts the whole document, 500 ms after a change settles.

| Change in the tab | Where | A command today? | Kind |
|---|---|---|---|
| Group, pivot, sort (menus, zones, header clicks), hide, heatmap, Column Properties, measures, add a calculated column | app.ts `#apply` | yes (`GridCommand`) | shared |
| Filter window's Apply | app.ts `#query(withFilter)` | no -- `filter` exists, unused here | shared: make it one |
| Calculated columns editor: edit, rename (renames carried through), remove | app.ts `#setCalc` (`#change`) | no -- `calculate` covers only adding | shared: new command |
| Settings window, a host's `applyConfiguration` | app.ts `#configure` | no -- `configure` exists, unused here | shared: make it one |
| Use a dimension | app.ts `useDimension` (`#query`) | no | shared: make it one |
| Chart click on a grid's chart (not a class's): the grid's filter and the chart's `selection` | cube-page.ts `#select` + app.ts `refilter` | no | shared: new command |
| Chart click on a class grid's chart | page-app.ts `onPick` -> `setPageFilter` | yes | shared |
| Add / remove / rename a tile, a sheet; move to sheet; add a chart; a chart's spec by hand | cube-page.ts `#tell` | yes (`PageCommand`) | shared |
| Layout drag; Undo Layout / Redo Layout | cube-page.ts `#tellLayout` | yes (`layout`, the sheet whole) | shared |
| Page filters set / removed | page-app.ts `#applyFilterCommand` | yes | shared |
| Page rename (name box) | datacube-app.ts `renamePage` | no -- `renamePage` exists, unused here | shared: make it one |
| Page-level Undo / Redo | page-app.ts `#move` | no -- the page put back whole | shared: whole document |
| Create a Class / Data Space / Association: the page's model | datacube-app.ts `current.model` | no | shared: whole document |
| Snap / Go Live | the plane toggle, then the page's state | no (the copy is in the document) | shared: the keeper's own operation |
| A following chart regrouped with its grid | cube-page.ts (followCube) | no -- derived | derived (see 2) |
| Expand / collapse a row, Collapse All, totals shown | app.ts `#change(tree)` | no | shared: new command (see 2) |
| Sheet shown, scroll, cell selection, a chart's editing grid, windows | screen only | not in the document | each editor's own |
| Settings saved for every grid (Settings > defaults) | `onSettingsChanged` | not the page's | the user's, not the page's |

Of about twenty kinds of change, eleven are commands already; seven could be with modest work (four have their
command kind already; open rows need theirs); two are whole documents by nature (page Undo/Redo, the page's model);
one is the keeper's own (Snap); one is derived; the rest are no part of the shared page.

Python's side, for the same question: every edit is a command except four, which serve the page whole today --
the page's model (page.py `_into`/`_keep`, which also change `_doc['model']` in place), a cube reopened over a frame
that changed (`_reopened`), a grid edit whose cube was reopened with notes, and the first change after `read()`.

## 2. The line: what the keeper versions

Today "changed since saved" compares `pageDefinitionText`: each cube's query and configuration, the views, sheets,
model and filters -- not the name (renaming is saving), not the open rows, not the copies. That is not the keeper's
line: a Snap changes nothing it compares, and the keeper must version it.

**Everything in the document is versioned** (the user, 2026-10-20). A first draft kept open rows in the document but
unversioned ("stored, last write wins"); the user's objection: a save or a share link captures the open rows, so
whose they are must be defined -- and a value both editors hold but neither orders is exactly what makes that
undefined. So there are two kinds of state, not three: the document (all of it versioned, all of it saved), and what
is no part of it (the sheet shown, scroll, cell selection, windows). Versioning open rows costs little under the
keeper's own rule: an expand conflicts with nothing, so a Python edit in flight is re-applied on top of it unseen; a
follower applies an open/close command rather than reopening. A second view of the same page mirrors the expansions
-- one page, one picture. Three questions stay apart: what is versioned (the document), what is saved (the
document), and what marks the page "changed since saved" (its definition: an expanded row does not).

| Field | Keeper | Why |
|---|---|---|
| `name` | versioned | a rename on either side is the page's |
| cube `source`, `query` (columns, calculated columns, filter, rows, pivot and its values, sorts, totals, row limit) | versioned | what the grid is |
| cube `configuration` (formats, kinds, aggregates, hidden, blurred, pinned, widths, title bar...) | versioned | saved with the page, set by Python's `configure` too; a width drag is an edit |
| cube `tree` (open rows, totals shown) | versioned | saved and shared with the page, so both editors must agree on it; an open/close command never conflicts |
| views: grid (id, cube, title), chart (id, cube, title, spec) | versioned | |
| a chart's `selection` | versioned | it is half of a filter that is versioned (its grid's), so the two move together |
| a following chart's spec | versioned, and derived | both editors must derive it by the one rule on every write (followCube), or the page ping-pongs between a stale and a fresh spec: Python's grid edits do not regroup a following chart's stored spec today |
| `sheets` (names, layouts) | versioned | |
| `model` (project, workspace, package, tables, classes, revision) | versioned | |
| `filters` | versioned | |
| `copies` | versioned, and the keeper's | what each grid reads; the keeper keeps a copy while some page it keeps reads it |
| `unknown` (fields a newer writer added) | versioned | carried |

The rule is the document itself, compared whole -- not `pageDefinitionText`, which stays "changed since saved".

## 3. What changes in the plan

1. **The keeper versions the whole document** (section 2): a Snap and an expanded row each bump it; neither marks
   the page changed since saved.
2. **Derived state is derived by every writer.** A following chart's spec is regrouped by the one rule whenever its
   grid changes, on Python's side too (headless gridApply returns it), so both editors write the same document.
3. **Python writes through one function.** `_doc['model']` is changed in place today; under the keeper every write
   is one versioned write, or it bypasses the version.
4. **Convert the seven to commands early.** Each one left is a whole-document write, which, against a concurrent change
   in Python, is refused and reopened rather than re-applied -- the edit is lost. The filter window, the settings
   window and the page rename already have their command kinds; the calculated columns editor, a dimension, a
   chart's click and opening or closing rows need theirs. Page Undo/Redo and the page's model stay whole documents: rare enough to accept a
   refusal under a concurrent change.
5. **Wire what exists.** Both kinds of command are produced already and consumed by nothing in the app: the keeper's
   client is that missing consumer, not new command plumbing.

## 4. The convergence test

Not spiked yet (planned as the build's first step). What exists makes it two halves and one seam: Python's half
drives the keeper with Python's edits and the tab's writes made by headless DataCube (the same functions the tab
runs); the tab's half drives the real page app (page-app.test.ts runs PageApp and CubeApp in a simulated DOM) against
a keeper of its own; the browser harness (datacube/test/python-engine) checks the seam on a few paths. Each checks,
after every step, that both editors hold the same document and that the copies held are exactly those
the kept pages read.

## 5. Order

The design note's build, with these changes folded in: (a) the keeper and its comparison, Python writing through it;
(b) the tab's commands sent to it, whole documents as the fallback; (c) the seven converted, following charts derived
by every writer; (d) Snap as the keeper's operation, the ownership code deleted; (e) `ll.show(df)` as a page of one
grid; (f) the two Snap fixes (a snapped grid opened over its copy everywhere; the copy typed by the live query); then
an independent audit.

## 6. As built: step 1 (2026-10-20)

Step (a) of section 5, and the Python half of the convergence test.

- **The keeper** (python/legend_lite/keeper.py, `PageKeeper`): each Python page's one document and its version, kept
  from the page's start (`Page.__init__`), shown or not. `write(key, base, doc, frames, commands)` moves it one version
  when `base` is its version, raises `Stale` when not, and makes no version for a document it has already; each version
  keeps its commands (or None, a change by no command) for the last 64, which `changes(key, since)` gives a page open at
  an older one. `referenced()`: the copies its pages read.
- **Python writes through it.** Every public call on a `Page`, `Tile`, `Grid`, `Chart` or `Sheet` is an operation
  (page.py `_operations`): the page as the keeper has it when the outermost call begins, its version pinned for the call
  (a write from the open page meanwhile is read by the next call), what it changed written when it ends -- or at each
  `_changed` inside it -- and let go when it raises. Written against a version the page moved on from, the call's
  commands are made again on the keeper's page by DataCube's own functions (`_rebased`: headless pageApply, gridApply),
  with the ids the page held at its version, so the ids the change gave out are its own; a change by no command (the
  page's model, a cube opened again over a changed frame, a load) or one that no longer applies is refused in words.
  `page.read()` is gone: a write from the open page is Python's page at once.
- **The engine's page.json** is the keeper's: GET gives the page, or since=v the commands since; POST is the open page's
  write against the version it is at -- read by DataCube's reader (headless pageRead), every frame it reads one this
  session serves -- answered `{"version": n}` (200), 409 when stale, 404, or 422. `serve_page`/`close_page` remain for a
  host that serves a page no Python page keeps.
- **Copies** (`Engine.sweep`, after every write by either editor and at a cell's end): a copy some kept page reads is
  the pages' (a tab's release keeps it); one no kept page reads any more is let go; one no page has read yet is its
  maker's. A collected page is forgotten at the session's next sweep, never in the collector.
- **The tab** (datacube-app.ts): its report is a versioned write whose answer, the new version, it adopts; its writes and
  follows run one at a time in order (`liveQueue`).
- **Tests**: test_keeper.py (the keeper alone; `Converges`: six seeds of fourteen steps interleaving Python's edits --
  sheets, groups, renames, Snap and Go Live -- with an open page's edits made by headless DataCube and written as the tab
  writes, some inside one Python call, asserting after every step that Python, the open page and the keeper hold one
  document and that the copies held are the ones read; a Python change refused when made again on top only for a real
  conflict); test_page.py's Live tests; the browser harness (a Python change after the open page's write arrives as its
  command, not a reopen).

Known limits, left to the steps after:

1. **The tab writes whole documents** (commands None). A tab write refused as stale is lost -- the follow then replaces
   it. Step (b): the tab sends its commands and makes them again on top when refused.
2. **Ids given out on both sides.** A Python change made again on top keeps the ids it gave out: when the open page gave
   out the same id meanwhile (a sheet added on each side at once), it is refused rather than renumbered.
3. **"Changed since saved"** on the open page now stays marked for a change made there after Python's change lands (the
   whole reopen used to clear it): right, since the person's change is saved nowhere, and noted because it is new.
4. Following charts are not yet derived by every writer (step (c)); the Snap ownership code (adopt, hand-over, claims)
   is still in place (step (d); deleted by the one API plan's Z17, 2026-10-22: one model, the keeper's).
5. **A page with no grid** (found by the 80-seed soak of section 7). Python holds one (`ll.Page` starts as one; Python
   may take off its last grid), but the open page cannot write one: DataCube's reader refuses a page with no cube
   (`PageNoCubeError`), and the tab writes nothing for a blank page (`pageNow` gives none). The last grid taken off in
   the tab -- or one taken off on each side at once -- leaves two pages: the tab blank, Python's with the grid. Step (b):
   a page with no grid is a page both sides write and open (the engine's write accepts one; the tab writes its blank
   page; opening one shows the blank page). test_keeper.py `EmptyPage` reproduces it, marked expected to fail.
6. **A copy dropped before every open page has moved past it** (found by section 7's two-thread test). A copy no
   current page reads is let go at once (`sweep`), but an open page that has not followed yet is at a version that
   reads it: its queries meet a table gone, and the commands that moved the page since may not apply over it -- the app
   then opens the page again whole (followLive), so nothing diverges, but the screen flashes an error and loses its
   view state. Step (d): a copy lives until every open page has followed past the version that dropped it (each
   follower says its version on every read and write). **Closed 2026-10-22** (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
   Z17), as planned: the keeper keeps the copies each version read, and each open page's version, said with its
   follows and writes and its tab's version poll (`follower`, `at`); a copy goes when no page reads it now and none
   open shows a version that reads it, or any since -- one silent for `FOLLOWED_FOR` (five minutes) keeps none.

## 7. Confidence (1b, 2026-10-20)

Asked whether the tests earn confidence for a change this size, the answer was measured, not argued.

- **Mutation pass.** Thirty deliberate breaks of step 1 -- the keeper's version check, its copies, its kept commands;
  the page's pin, pull, re-apply, discard, load; the engine's routes and copy sweep; the session's forgetting --
  each applied alone and the tests run (scratch harness: each break, the keeper/page/engine/show/notebook tests,
  restore). The baseline caught 22 of 30. The eight missed became tests (commit 20fa90bc9), and three new breaks
  were added for what those tests claim. Round 2 caught 10 of those 11; the one left -- the load's own last write
  taken out -- survived because the write was redundant: a public call inside `load` wrote the page as it stood, once
  per cube opened again, half loaded. `load` now runs as one call (`Page._call`), written once (commit fd933afdb), and
  the break is moot. So 32 of 32 that can be caught are. Two breaks of the tab's own live code, run through the
  browser harness: the tab ignoring the version its write made is caught; its follows not waiting for its write in
  flight is not -- a race no test here provokes, and the first target of step (b)'s tab half of the convergence test.
- **Intent, not only agreement.** The convergence test keeps a model of what every written edit said (a name, a
  sheet, the grids and charts, a grid's groups, Snap, titles) and checks the page against it after every step, in
  write order -- so "one editor's whole page wins" fails it, where agreement alone passed it. 14 Python edit kinds and
  9 of the open page's, with a pin that each went through somewhere; a refusal only for a real conflict (both gave
  out one id; the tile taken off on the other side).
- **The open page writes as step (b)'s tab will**: its edits as commands; refused, it follows what moved the page and
  makes them again on top. Two design inputs for step (b) came out of it: (1) the tab keeps trying for as long as the
  page keeps moving -- a fixed few tries starved behind Python in a loop (delayed, nothing lost); (2) a re-applied edit
  that made an id (a sheet, a tile) gets a fresh one, so a later pending edit naming the old id must be renamed to it.
- **Two threads.** Python's calls and the open page's writes at once, each forty edits, requiring that they met (a
  write refused for the other's): no deadlock, one page, every edit on it in version order. Run twenty times clean
  after the reopen fallback (limit 6).
- **Soak.** `LEGEND_LITE_KEEPER_SEEDS=80`: 2,400 interleaved steps clean (eight seeds in the suite).

## 8. As built: step 2 (2026-10-20)

The design changed with the user's question "are the exact same APIs used in the browser and in Python?": step 1 had
put the rule for making one writer's edits again on top of the other's in Python (page.py `_rebased`), and step (b) as
planned would have written it a second time for the tab. Instead the host settles every write, by one TypeScript rule.

- **One rule, in TypeScript** (datacube/src/page-rebase.ts `rebaseEdits`, headless `pageRebase`): one writer's edits,
  made against a version the page moved on from, applied in order to the page as it is now -- page commands by the page
  commands' own function, a grid's by the host's applier (only the host has a grid's base: its frame, or its copy). An
  id a command made that the page has held since is given out afresh and every later edit naming it follows; a sheet's
  arrangement keeps the tiles the other writer put on it; one edit that no longer applies refuses them all.
- **The host settles** (python/legend_lite/keeper.py `settle`): storage that never reads a page -- what a page reads
  (frames, copies, ids) comes with each write from DataCube (headless `pageReads`, `pageWritten`). A write at the version
  the page is at is kept as written, with no extra call into DataCube; behind it, one call makes its edits again on top
  (`pageRebase`), or the write is refused (a change by no command; edits that no longer apply). Python's page and the
  open page both write through it; Python's own re-apply loop is gone, and a handle whose id was given out afresh
  follows the rename.
- **The tab** (datacube/src/app/live-channel.ts): the version on screen, the edits made there since its last write (each
  grid's command and the page's, as applied), one queue of writes and follows. A change no command explains makes the
  next write the page whole; nothing done while following counts as the tab's own; a write the host made again on top
  reopens the page as the host made it; nothing is written when nothing was made (the browser harness caught such a
  write, behind a page Python had moved, reopening the page in place of following Python's command).
- **A page with no grid** is written and read live on both sides (known limit 5, settled).
- **A write asks DataCube a fixed number of times**, whatever the page holds (step 1 asked once per grid; pinned).
- Tests: page-rebase.test.ts (300 seeds of two writers' edits), live-channel.test.ts (the channel alone, and a real
  PageApp whose edits reach a keeper as commands it makes again on top of the other editor's), keeper storage and settle
  unit tests, the Python convergence and two-thread tests on the new protocol. Full suite 210/210.

Still open: known limit 6 (a replaced copy dropped before every open page followed past it; step (d): closed by the one
API plan's Z17, 2026-10-22); the seven tab
edits that are no commands yet stay whole-page writes, refused when they collide (step (c): proposed to drop, for its
cost against a rare case -- the user's decision pending); on a combined write the tab reopens the page (its view state
lost then).

## 9. Superseded in part by the one API program's S4 (2026-10-21)

The writes described in §6 and §8 changed with docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4 ("the host applies the
commands"): a write is the version it was made at and the edits made there, never the page; the keeper has DataCube
apply them to the page as it is (headless pageRebase) at every version, its own included, and keeps what they make. A
change no command explains no longer makes a write whole: the one write of a page whole is a restore (an undo or a redo,
the plan's N2; a page loaded, N1), kept only at the version it was made at. Each version keeps the ids the page held then,
which the rebase is told (a page filter the writer made is told from one it set). What a page on Python can hold is one
rule (headless grid.ts onHost: frames, and joins of them, until the plan's S5): a write that adds a grid over any other
source is refused as it is made. That plan's As built (S4) section is the current account.
