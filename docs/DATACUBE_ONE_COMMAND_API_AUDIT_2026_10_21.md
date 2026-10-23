# One command API: the full audit (2026-10-21)

The user's ask: find everywhere a change is not a command, and everywhere the browser and Python do something
different, so the tenet holds once and for all. The tenet: **DataCube logic is TypeScript APIs; the TS UI and headless
Python call exactly the same ones; every change to a saved page is a command, applied by the same function on both
sides.**

Method: four read-only audits (the grid, the page board, the host shell and the notebook widget, Python), each told
to cite `file:line` for every claim, plus a mechanical sweep of every state-changing call site. Checked against
`claude/datacube-query-base` at `5e3091d09`. **✓** marks a claim re-checked by hand against the code; the rest are
high-confidence findings with their lines, each re-checked when its fix is made. Paths: `ts/` is `datacube/src/`,
`py/` is `python/legend_lite/`.

This supersedes the inventory in docs/DATACUBE_PAGE_KEEPER_2026_10_20.md §1 (it was right in the large; it missed
the items marked *new* below, and its "Settings window" row was wrong: that window holds debug mode, history size and
row buffer, none of them saved).

## 0. The answer

Most of it is done: every grid edit made through a menu, a zone, a header or the Properties window is a `GridCommand`
applied by `applyGridCommands`, and every structural page edit Python makes is a `PageCommand` applied by headless
`pageApply`. What breaks the tenet comes from five root causes, not from a list of stray edits:

1. **R1. The page board is a second implementation of the page commands** ✓. Apart from page filters, the tab's
   page edits are performed by `CubePage`'s own code and then *described* as commands (`ts/page/page-state.ts:5-8`
   says so by design: the screen does not import the apply function). Only replay tests over a few scenarios hold
   the two together. Python's edits run `applyPageCommand`; the tab's run `CubePage`. Same edit, two codes.
2. **R2. The host keeps whole pages; commands are only annotations** ✓. At the current version the keeper stores the
   page exactly as the tab sent it (`py/keeper.py:113-137`, `py/engine.py:1447`); the commands are recorded, never
   applied or checked. Commands only matter when a write is stale. So nothing makes the tab's page and its commands
   agree.
3. **R3. Some edits are no command at all** (section 2): about twenty kinds across the grid, the page, the host and
   Python.
4. **R4. Derived state is stored, but only one side derives it** (section 3): a following chart, a chart's selection,
   the grid's tree from its settings, group-level column types.
5. **R5. Python keeps copies of TypeScript rules, and the two sides keep different rules for the same job** (sections
   5 and 6), plus capability gaps both ways (section 7).

## 1. The fix, by root cause, each with the guard that keeps it fixed

| Root cause | Fix | Guard (fails if it comes back) |
|---|---|---|
| R1 | Every page gesture builds a command; the page state is what `applyPageCommand` makes of it; the board is put at that page by `CubePage.sync` (it exists: undo, redo and follow already use it). The screen's own mutation code is deleted. | A guard test: the page state changes only through the apply functions (as `state-guardrail.test.ts` does for the grid's fields). Bundle budget moves with a dated note (the screen then downloads `page-commands.ts`). |
| R2 | The host keeps what the commands make: every write is applied by headless DataCube (the same `pageRebase` path stale writes take today, with no conflict), not taken as sent. The tab sends commands, not the page. | Keeper test: a write's document is the applied result; a write whose commands do not make the page it claims fails in the tab harness. |
| R3 | A command for each edit in section 2; then the "change no command made" path is deleted everywhere (the tab's `#told/#seen` guess and `edits: null`, the keeper's refusal for it, `pageWritten`'s null). Named exceptions only: a page born (new, load), and page undo/redo (whole, current version only). | A property test on the real PageApp and CubeApp: every gesture tells commands, and replaying them on the page before gives the page after. A static allowlist of the named exceptions. |
| R4 | One shared rule run inside the shared apply: a grid's commands applied to a page re-follow that grid's live charts and reconcile its charts' selections; the tree follows `configure`; open-level state saved. | Convergence test: Python, headless and the tab hold the same page after the same commands, charts included. |
| R5 | Each Python copy replaced by a headless call to the TypeScript rule; each different rule made one rule (decisions in section 8); a Python method for every command kind. | Python funnel test: `Page._doc` is only ever set from a headless answer. The coverage table (`docs/DATACUBE_COVERAGE_2026_10_18.md`) extended to fail when a command kind has no UI producer, no Python builder, or no JSON reader. |

## 2. Every change that is not a command

### The grid (`ts/app.ts`)

| Where | Action | Changes | Needs |
|---|---|---|---|
| ✓ app.ts:748-751 | Expand / collapse a row | `tree.open` | new `open` command |
| ✓ app.ts:2054-2056 | Collapse All | `tree.open`, expand level | new command |
| ✓ app.ts:3446-3451 *new* | Properties Apply with "show root aggregation" / "initially expand to level" | `tree.showTotals`, expand level, beyond the told `configure` | the tree rule inside `configure` |
| ✓ app.ts:1862-1882 | Calculated-columns editor: edit, rename (carried through rows, pivot, sorts, filter, settings, column order) | `query.derived`, `groupDerived` and every reference | new command; the rename rule moves out of the UI (it is split today: `snapshot.ts:803` and `config.ts:886-899`) |
| app.ts:3363-3368, column-editor.ts:444-460 | Delete a calculated column (written twice) | `query.derived` | new command |
| ✓ app.ts:3404-3421 | Filters window Apply | `query.filter` | existing `filter` |
| app.ts:2741-2749, cube-page.ts:1462-1478 | Click a chart mark on a grid's chart (not a class's) | `query.filter` and the view's `selection` | new command (filter and selection together) |
| cube-page.ts:490-524 *new* | Restoring a page whose filter lacks a chart's selection | `query.filter` | the same, or a consistent document |
| ✓ app.ts:3182-3185 | Use a dimension | `query.rows` (skips grouping's checks) | existing `group` |
| ✓ app.ts:2382-2387 *new* | `applyConfiguration` (host API; only tests call it) | `configuration`, by `withConfiguration`, not the `configure` command's stricter rule | delete; tests use `configure` |
| app.ts:919-921 *new* | public `change(update)` / `state` | anything | remove or narrow to commands |
| app.ts:4266-4296 | Grid Undo / Redo | the state whole | named exception (whole), see R3 |
| ✓ app.ts:3552, 1051-1053 *new* | An edit refused (expired sign-in) and run again after signing in | whatever it changed: its commands were dropped at the refusal | tell the kept commands on retry |
| app.ts:587 *new* | A grid made while the page bar is folded | `showTitleBar` at construction | in the `addGrid` document |

The edits that are commands are applied cleanly: CubeApp's state is made by `applyGridCommands` (app.ts:1002-1005)
and the same objects are told. Four timing cases tell commands that differ from what landed (app.ts:1033-1060,
state-owner.ts:397-430): a command refused on the committed state but kept on the pending one; a presentation command
told before a running transaction's; a change made on top of an undo in flight (only the change is told); Python's
commands mixed with the user's in `#untold` during a follow. Each matters once the host applies commands (R2).

### The page (`ts/page/`, `ts/app/datacube-app.ts`)

| Where | Action | Changes | Needs |
|---|---|---|---|
| ✓ page-app.ts:394-423 → datacube-app.ts:554-558 | Page name box | `name` | existing `renamePage` |
| datacube-app.ts:816-831 | Save / Save As under a name | `name` | `renamePage` |
| ✓ app.ts:3946-3970; page-app.ts:633-638 | Snap / Go Live in the tab | `copies` | existing `snap` / `live` (the screen follows Python's, page-app.ts:841-845, and sends none) |
| datacube-app.ts:1079-1169 | Create a Class / Association / Data Space | `model` | new model commands (`PageModel` into, pin, keepTables, keepClass already exist in `page-model.ts:57-89`) |
| page-app.ts:854-877 | Page Undo / Redo | anything | named exception (whole) |
| chart-panel.ts:208-218 ✓ *new* | Chart options form | `spec`: `setChart` is told, then the chart re-follows its grid, so the kept spec can differ from the told one | tell after following (part of R4) |
| cube-page.ts:1385-1451 *new* | Open in grid / close the editing grid | the chart's band shares reset | an editor tile must not touch the saved layout |
| cube-page.ts:1411-1435 *new* | Update a detached chart from its editing grid | a new kept cube; only `setChart` told | new command |
| page-app.ts:658-698 *new* | Open a saved page (restore) | views without a maker dropped, their click filters too, untold | creation; the drops said |
| datacube-app.ts:706-803, 662-687, 1225-1235 | Open…, open a source in place, New ▸ Blank Page, **on a page Python serves** | the whole page, replacing Python's | decision D1 |
| sources.ts:856-874, 1091-1119 *new* | The tab reopens a live page's cubes over frames as they are now | each cube rebuilt; the difference rides unannounced in the next write | a "rebuilt" grid command made by one side |
| datacube-app.ts:1376-1385 *new* | `ll.show(df)` page; its frame's columns change | the page replaced, the person's grid edits lost | `ll.show(df)` as a kept page of one grid (keeper step 5) |
| app.ts:2670-2700 *new* | Charts on a cube alone (embedded CubeApp) | the page whole; Undo Layout tells nothing (✓ cube-page.ts:372) | the same commands |

`stack` and `arrange` gestures are told as a whole `layout` (cube-page.ts:374), so a rebase keeps the screen's
positions instead of stacking or arranging again; under R1 the gesture's own command is applied.

### Python (`py/page.py`) ✓

| Where | Action | Today |
|---|---|---|
| page.py:1041-1079 `_into`/`_keep` | `create_class`, `create_association`, `create_data_space` | `doc['model']` written in Python, whole write (up to three per call) |
| page.py:1598-1621 `_reopened` (from `load`, `update`, and `_current` before `filter`, `calculate`, `chart`, `live`) | A cube reopened over a frame that changed | Python decides whether to take it, splices it in (`_with_cube`), whole write; inside `live()` the whole call becomes a whole write |
| page.py:277-296 `_kept` | A grid edit whose cube came back with notes or a changed source | whole write |
| page.py:1417-1453 `load` | A page from a document | whole write (creation: fine) |
| page.py:891-909 `Page()` | A new page | born whole (creation: fine) |
| engine.py:1456-1468 `serve_page` | Tests serve a page directly | reads worked out in Python, bypassing `pageReads` |

## 3. Derived state: stored, but derived by one side only (R4)

| State | Who derives it | Who does not | Consequence |
|---|---|---|---|
| A following chart's spec when its grid changes (`followCube`) | the tab (chart-panel.ts:152-162), silently | Python `Grid._apply`, headless `gridApply`, `rebaseEdits` | Python's and the keeper's documents hold stale chart specs |
| A chart's selection when its grid's filter no longer holds it | the tab (cube-page.ts:1125-1134) | Python, headless | stale selections in Python's documents |
| The tree's totals and expand level from `showRootAggregation` / `initialExpandToLevel` | the tab's Properties (✓ app.ts:3446) | ✓ headless keeps the tree (grid.ts:449) | Python's `configure({'showRootAggregation': True})` shows no total row |
| Group-level column types | the tab, from the run (cube-state.ts:75) | headless types row-level columns only (grid.ts:444, 454) | the two write different cube documents |
| Expand level and groups closed under it | nobody: `treeDocument` saves only `open` and `showTotals` (cube-document.ts:255-257) | | a reopened grid forgets its expand level |

## 4. Commands one side cannot read

- ✓ **Window, child-aggregate and explode columns.** The column editor and the Show As presets (running total,
  moving average, rank, % of total: `calc-presets.ts:139-140, 227-236`) send `calculate` with such a column; the JSON
  reader takes only `name`, `lambda`, `kind`, `type` (`grid-commands.ts:387-399`). A live tab's write with one is
  refused. Python can make none of them.
- The tab never sends `measures`; Python never sends `clickSort`, `clickTreeSort`, `addFilter`, `sorts` after a grid is
  made. Not wrong, but gaps (section 7).

## 5. Python's own copies of TypeScript rules (R5)

| Where | Rule | The TypeScript one |
|---|---|---|
| ✓ page.py:87-89 `_with_cube` | a cube put into a page | `page-rebase.ts:120 withCube` |
| ✓ page.py:1671-1673 `_ids` | the ids a page holds | `idsOf`; `pageReads().ids` |
| page.py:1041-1079 `_into`, `_keep` | the page's model record | `page-model.ts:57-89` |
| page.py:351-375 `_class` | the Create a Class action, place `'Python'` | `create-class.ts:65-72 action()` |
| page.py:1101-1126 `_facts` | association key uniqueness | `model-actions.ts:108-140` |
| page.py:1128-1164 | Create a Data Space orchestration; class path by simple name; no uniqueness preset | `model-actions.ts:167-255 createDataSpaceOf` |
| ✓ model.py (all) | the SDLC / model-writer client: project naming, workspace, save-fresh, commit messages | `model-changes/src` (the module calls itself its twin, held alike by shared cases) |
| page.py:1245-1252 | command order when `Page.grid` builds a grid | `draftCommands` has its own order |
| ✓ page.py:1301, 1312 | a join's input names; placed `near: None` | `join-flow.ts:27-34 inputOf`; placed `near: o.cube` (✓ :96) |
| page.py:1598-1621 | whether to take a reopened cube | none |
| page.py:277-296 | when a grid edit is a command or a whole write | none |
| page.py:130-176 `_value`, `_given_filter` | Python values to filter values; pre-checks and wording | `filterValueFrom`, `draftFrom` |
| page.py:1343-1349 | a stack needs two tiles | `page-commands.ts:356-361` (checks again) |
| ✓ page.py:1694-1701 | a Python page reads only frames | `frameTables` / `gridBase` |
| page.py:650-660, 1657-1668 | a snapped grid's base and refusal | inline in `headless/page.ts:236-242` |
| ✓ engine.py:92, 1104-1106, 1158-1160 | snap limit and its sentence | `snap.ts:70, 227-235` (kept separate by decision, 2026-10-15) |
| engine.py:1006-1061 `_claimed`, `sweep` | copy ownership | `HostSnaps` adopt / handOver (`engine-client/src/snap.ts:460-536`): two ownership models (keeper limit 6) |
| page.py:1388-1415 `save` | the saved-page record | `datacube-app.ts:819-836 saveTo` |

Python never calls two headless exports: `pageFilter` and `joinQuestions` (`entry.ts:60-62` still speaks of a
`page.filter` that does not exist).

## 6. Same job, different rule (what a person would see differ)

| Job | Browser | Python |
|---|---|---|
| ✓ What a page filter shows | grids show rows through the page's filters (`gridLens`) | headless `gridView` / `gridExport`, so `view()` and `to_pandas()`, ignore page filters (no lens in `headless/grid.ts`) |
| ✓ Rename a tile to its made title ("Grid 2") | stored as no title (cube-page.ts:326) | stored as the title |
| Default page name | first sheet's label, or `'page'` | `'Untitled page'` |
| Save under a name | the page is renamed to it | only the store record is named; the content keeps the old name |
| Page file | refused when the page has a model; compact; unknown fields dropped | allowed; indented; model kept |
| ✓ Empty page | written and served (the tab's blank page) | a shown page may not lose its last grid; an empty page cannot be shown or saved |
| Sources on a Python page | the tab offers files, remote, warehouse, saved queries | Python reads only frames, so it cannot save, show or rebase on such a page |
| A detached chart going live | not allowed (cube-page.ts:999) | allowed (page.py:747-763) |
| New page skeleton | `page.clear()`, name `''` | `pageNew`, which refuses a blank name |
| Frame columns change | `ll.show(df)` loses grid edits | `ll.Page` keeps them |
| ✓ Join | local engine only; not of frames | of frames |
| Ids held | the board's held set | `freshId(takenIds)`; the host records only the written page's ids (an id given and removed within one write may be given again) |
| Saved-page id | `crypto.randomUUID`, `current.cubeId` | `uuid4`, `page._saved`: saving one live page on both sides makes two records |
| "Nothing to save" | "There is nothing on this page to save: add a data source first." | "cannot read saved page: it has no cube -- in Python, page.grid(frame) adds one" |

## 7. Capability gaps

**The browser can, Python cannot:** replace or clear sorts, header and tree-column click sorts; add one condition to
a filter; edit, rename, delete calculated columns, group-level, window, child-aggregate and explode columns, the Show As
and period presets; expand / collapse rows, Collapse All; use a dimension; chart clicks (a grid's filter and selection;
a class grid's page filters) and any page filter at all; reorder and delete sheets; drill a chart; Copy of Grid; a
chart's Open in grid and Update; reopen a saved page by id.

**Python can, the browser cannot:** the `measures` command; a whole grid in one call; arrange tiles in any order by
bands; set a layout as JSON; `update(frame)`; read results as data (`view`, `to_pandas`, `relation`, `export`);
create classes against another SDLC or writer; data spaces and associations without the windows. These are an API's
conveniences, not missing semantics, except arrange-by-order.

## 8. Decisions for the user

- **D1.** Open…, open in place, New ▸ Blank Page on a page Python serves: today they replace Python's page whole.
  Recommended: they open a new page and leave the served one alone.
- **D2.** Empty pages: one rule. Recommended: allowed on both sides (Python's refusal goes).
- **D3.** Sources on a Python page. Recommended: the tab offers on a Python host only what Python's page can read
  (frames, joins of frames, copies), until Python reads more.
- **D4.** `model.py`, the Python twin of `model-changes/`. Recommended: Python asks the TypeScript library headless,
  and `model.py` goes.

The rest have an obvious single rule, taken without asking: page filters reach Python's views; the made-title rule
in the command; the browser's save rule (the page takes the saved name); one default page name, one page-file rule,
one detached-chart rule, both decided by the shared TypeScript code.

## 9. Order and size

1. **Commands complete** (R3, section 4, the telling cases): every edit in section 2 a command, window columns read,
   retry and undo told; model commands; Snap and rename from the screen. Guard: the gesture property test.
2. **Derived state by one rule** (R4). Guard: the convergence test with charts.
3. **The board produced by the commands** (R1). Guard: page-state guard test.
4. **The host applies the commands** (R2); the no-command path deleted; D1. Guard: keeper test.
5. **Python's copies gone, one rule each** (R5, section 6, D2-D4). Guard: Python funnel test.
6. **Capability gaps** (section 7): a Python method per command kind. Guard: the coverage table.

Each step audited and the full suite green before the next. Rough size: 6-9k lines changed with tests, a few thousand
deleted; steps 3 and 5 are the largest, step 3 the riskiest (the board's view state across `sync`).

## 10. Checked and found clean

Every name goes through `givenName`; `nextPageId`, `chartTitle`, `gridTitle`, `filtersFor`, `keptFor`, `copiesFor`,
`gridCopyOf` are shared; layout reading and placement are TypeScript's (`layoutTiles`, `layoutBands`, `bands.add`);
page reading and writing go through `pageRead`, `pageReads`, `pageOpenedLive`, `pageWritten`, `pageRebase`; data in
uses the same TypeScript functions on both sides; every chart rule is `gridChart` / `editChart`; Ad Hoc mode writes no
cube; `cubeDocument()` writes only the committed state; the widget and the tab carry identical engine routes; no
dialog writes the page document; the keeper reads no page itself.
