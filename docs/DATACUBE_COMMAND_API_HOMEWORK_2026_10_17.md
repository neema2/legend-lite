# DataCube command API: the homework (2026-10-17)

**Status:** the input to the next program. The user, 2026-10-17: "fix the stuff, get this PR out and then do the full
reshape with commands and everything else". PR neema2/legend-lite#47 lands first, with §6's fixes; the command API is
its own program after it (§9).

Read-only surveys of branch `claude/epic-rubin-ay7upl` at baf0bfd: every action on one grid, every page-level action,
every public Python method, and the state model, run in parallel. Items marked **checked** were confirmed in the code
by hand; the rest are the surveys' findings, each with file:line references in the session's record, not yet
re-checked.

## 1. The question and the answer

**The question:** can DataCube have one typed command API, `apply(page, command) → page | refusal`, where every screen
action builds a command and Python builds the same commands?

**The answer: yes, it's feasible, and nothing in PR neema2/legend-lite#47 stands in its way.** The command API is a real program, about
20 files and 5–8k changed lines, plus tests. It splits cleanly into stages.

## 2. The numbers

| Area | State-changing actions | Already in a DOM-free function | Logic inline in a handler (must be extracted) | Hard (I/O, live values, a live object) | Other |
|---|---|---|---|---|---|
| One grid | 119 | 65 | 46 | 3 (Snap/Live, Ad Hoc mode, sign-in) | 5 (undo, redo, reload; 2 view-only) |
| Page | 48 (of 62 listed) | 18 (8 already reached by Python) | 14 | 16 | 14 view/derived/read |

59 of the 119 grid actions only edit an editor's draft (Properties, Filters, Column, Settings). They reach the cube
through just 4 commits: Properties Apply, Filters Apply, Column OK, and Column Delete.

## 3. What already fits

- **The grid's undo already records states, not closures.**
  - The state owner (state-owner.ts) already runs a pure update, then an async check (compile and run), then commit or
    refuse.
  - `s => apply(s, cmd)` drops straight in.
  - A rule's refusal becomes a return value. That replaces today's try/catch-and-capture in `#edit`.
- **`grid-edits.ts` is the start of `apply`'s grid half**, and headless `gridEdit` is already a batch command.
- **Python already holds the page as exactly one document** (`Page._doc`), which is the shape `apply` wants.
- **The document formats need no migration step.** No older document is read (the user, 2026-10-17): a cube is
  version 2, a page version 4, a chart spec version 2, and an older one is refused by its version -- so a command
  never meets an older shape.

## 4. What's big, ranked

1. **The screen has no single page state.**
   - The page is spread across:
     - PageApp: the name;
     - CubePage: sheets, charts, ids and titles;
     - each sheet's BandBoard: the layout and tile titles;
     - each ChartPanel: the chart spec;
     - the host closure in datacube-app.ts: the saved identity and the model.
   - The page document is only assembled at save time.
   - A page-level `apply` needs one page state that these render from. This is the largest single step.
2. **60 actions have their rules inline in handlers** (46 on a grid, 14 on the page) and must move into DOM-free
   functions.
3. **Undo is three separate histories:**
   - each grid's;
   - each sheet's layout (50 steps);
   - nothing for charts, sheets, tiles, titles or page structure.

   Actions that cross scopes change a grid and the page without being atomic: a chart click that filters its grid,
   removing a grid, Open in Grid's Update.
4. **Engine facts live in committed state.** Compiler types land in the snapshot, and the edit rules read them. That's
   workable as long as types count as part of the state.
5. **Duplicated representations `apply` must keep in step:**
   - configuration vs its copy folded into the query (the query wins on open);
   - `showTotals` vs `showRootAggregation`;
   - a chart's selection vs its grid's filter;
   - a live chart's saved spec vs its grid.
6. **Ids and placement are allocated differently.**
   - Ids: Python's `_next` vs the screen's counters.
   - Placement: the screen sizes from the viewport and uses the shown sheet; Python uses 4 columns and the first sheet.
   - Commands must carry ids and the sheet explicitly.

## 5. What stays outside `apply`

These are effects that produce a value which a command then applies:
- opening sources and uploads;
- Save, Open and share;
- model actions (Create a Class, association, data space), which write to SDLC;
- Snap/Live, sign-in, Ad Hoc mode;
- auto-size (widths measured on screen);
- chart click and brush (the command carries the resulting conditions).

Undo and redo are operations around `apply`, not inside it. View state (selection, scroll, the shown sheet, the
layout lock, maximise) is not a command.

## 6. Does PR neema2/legend-lite#47 lock anything in?

**Mostly no.**
- The headless entry points and their JSON are internal (Python's public methods wrap them), so swapping them for
  one `apply` later is invisible to Python users.
- The document formats changed once, and nothing older is read: a cube is version 2 (no measures list); a cube of
  version 1, a page before version 4 and a chart spec of version 1 are refused by their versions (the user,
  2026-10-17: "you can remove ALL backward compatibility"; "you can break saved links"). Share links made before
  are refused too.
- Python's public API against `main`:
  - **added:** `Grid.column()`, `Page.grid(dimensions=)` and `Grid.calculate(kind=)`;
  - **removed:** `ll.planner()`, `ll.show(planner=, server=)`, `legend_lite.upstream` and `Engine(planner=)` (no
    remote planner in Python);
  - **changed meaning:** `Grid.measure()` (now a column's Aggregation, or a calculated column; a dimension's refused),
    `Grid.sort()` (now the menu's Add: replaces that column's sort), `Grid.group()` and `Grid.pivot()` (a column that
    is not a dimension refused: `Page.grid(rows=['year'])` needs `dimensions=['year']`), and `Grid.configure()` (a
    host's patch, merged and folded, with Column Properties' rules for a column's kind and Aggregation, where it wrote
    the document raw; a setting, a column or a name it has not is refused), `Grid.measure()` (its aggregate is
    required: no `'sum'` assumed), and `to_json()`/`save(path)` (a page with no grid is refused, as DataCube refuses
    it).
- Most public methods map one-to-one onto commands.

**Before merge, fix what #47 itself introduced or made worse:**

| # | Problem | Why it's #47's | Fix |
|---|---|---|---|
| 1 | The Query app didn't type-check (`measures: []`) | baf0bfd | **Done:** cbcd4d6 |
| 2 | `gridEdit` drops open groups on a regroup; the screen keeps them (**checked**) | gridEdit is new; the rule came from round 2 | **Done:** da1f546, kept as the screen keeps them |
| 3 | `Grid.column(aggregate=None)` can't clear an Aggregation or weight; the screen can | `column()` is new | **Done:** da1f546, a "keep" default, as `chart()` uses, so None clears |
| 4 | `Grid.configure` is a raw dictionary write (**checked**). After any `gridEdit`, which now writes the folded query, a `configure({'maxRows': …})` is overridden on open, and `columns` is replaced wholesale | configure is older, but #47 made the loss likely | **Done:** da1f546, through gridEdit's `configuration` part, by `config.ts withConfiguration` (moved out of app.ts `applyConfiguration`, which calls it), then folded |

**The round's independent audit (two read-only auditors, 2026-10-17, the reviewers' sessions being offline) found, and
#47 fixed before merge:** a saved list's conversion and Open in Grid's undone by the opener reading the folded query
back (folded now); a partial `appearance` from `configure` wiping the rest (merged now); Open in Grid's carriers
colliding on one (column, aggregate) pair; a saved live chart's values collapsing onto one Aggregation (a value its
grid carries as it is keeps its aggregate); `setSorts` refusing a pivot's filler count header; a sort of a measure
left out kept on open; `measure()` making a dimension a measure; `configure` setting what `column()` refuses;
`sort()`'s move-last rule held in Python (`grid-edits.ts addSort`, the menu's too); a grid measures list read at run
time (refused now); unknown `gridEdit` parts ignored (refused now); reopening's notes dropped (warned now).

**The clean audit (four independent auditors, 2026-10-17, main merged in) found, and #47 fixed before merge,** each
with a test that fails without it: an untouched Update after Open in Grid changing a chart's values (a histogram's
count, a copy the grid had, two counts both named "count"); a following chart folding one of two values of a column
onto the other, for good; seven ways a converted page opened showing other numbers, and cubes written then that
older readers refused -- all moot with the conversion gone (no older cube is read); `create_data_space` leaving out a
grid whose sample a class cannot hold; `configure` keeping settings and columns DataCube has not; a filter, a
calculated column or a chart checked against a Live frame changed in place but written into the stale cube; a
broken lambda escaping as a JavaScript exception; `measure()`'s Python-held `'sum'`; and the stand-in's test case that
could not fail.

**Known differences to record as follow-ups, not fix in #47:**
- `gridEdit` only compiles. The screen runs the change, so a pivot past 500 values is refused on screen but accepted
  by Python until its view runs (**checked**).
- A live chart's saved spec goes stale after a Python grid edit until a chart method touches it. The screen
  re-follows on every view (**checked**). A page-level `apply` fixes this properly.
- Open in Grid says nothing of a chart value it could not make a column (its notes are dropped); Update leaves that
  value as it is (since the clean audit: it took its column's Aggregation).
- `gridEdit` does not move the tree with the configuration as Properties Apply does (`withTotals`, `withExpandTo`), so
  `configure({'showRootAggregation': ...})` does not change the view (as before this round).

## 7. Screen bugs found along the way (already on main; not #47's)

- ~~Unticking **Exclude from horizontal pivot** in Column Properties does nothing~~: **fixed 2026-10-18 on #47**
  (e2e3ce8). `applyToSnapshot` now takes the exclusion from the configuration alone, as it takes the aggregate.
- **View ▸ Dimensions** skips the grouping rules (app.ts:2997 calls `useDimension` through `#query`, not `setGrouping`).
  (**checked**)
- The pivoted measure's parent checkbox makes N changes and N undo steps for one click (columns-panel.ts:368-372).
- Heatmap default colours are written twice (app.ts:2178, panel-column.ts:538).
- Filter ▸ Add Filter has no compile check before it runs; the Filters window has one.
- The "a column is on one axis only" rule lives only in pivot-panel.ts, so menu and Python edits can put a column on
  both axes.
- Removing a grid clears a frozen chart's chip, but the detached copy of the grid keeps those conditions in its filter.
- The save dialog's blank-name rule differs from `givenName`.
- ~~A header's click refuses a sort of a pivot total, while the header menu sorts by it.~~ **Fixed 2026-10-18**: a header
  click sorts by any column (grid-edits.ts clickSort), a total too, and a total's menu offers what a pivot cell's does.
- `setSorts` takes a pivot's header while pivoted, as the screen offers it; a header a pivoted level does not produce
  is left out of that level's query without a word (round 2's Python refused it; the screen never did).

**Found and fixed on #47 by the test-gap round (2026-10-18)**, each with a test that fails without its fix:
- unticking "Show grouped columns" left the query keeping them;
- a weighted average whose weight column is gone was kept on reopen, so the planner refused the whole cube;
- a cleared setting every grid has read as off;
- a grain or bins left on a chart with no column across;
- a layer reading a value before the chart's first;
- axis breaks that were not ranges, which threw at the drawing;
- option flags and `frozen` not checked as true or false;
- a drilled chart given another column across kept drawing the drill;
- a big integer's bin, clicked, threw;
- Open in grid offered on a frozen KPI with no trend, opening nothing.

These are commits e2e3ce8, 57b653a and f130b8e.

## 8. Python still holds copies of rules (the twins program's leftovers)

These remain because the screen's side of each rule is still inline in a DOM class, so there is no shared function to
call yet. Extracting them is the command program's first stage.

- **Id allocation and titles:** `_next` vs the screen's counters; "Chart N" titles; Page.sheet()'s id.
- **Page.remove's cascade.** It also misses the screen's two selection cleanups, and refuses to remove a page's last
  grid, which the screen allows.
- **Moving tiles:** Sheet.add and Tile.move_to (a same-sheet move goes to the bottom; the screen does nothing).
- **Placement:** always a bottom band with 4 columns; the screen goes beside the asking grid, sized by the viewport.
- **Page.name** writes each cube's name itself; the screen does it when it writes the document.
- **Copies of DataCube functions:**
  - `_tiles` / `_leaves` copy bands.ts's tiles()/leaves();
  - `_keep` re-implements keepTable/keepClass;
  - `create_data_space` matches class paths by their last segment, while the screen builds the full path.
- **Grid.calculate** calls `nameProblem` without the pivot's column names, so a clash with a pivot header passes.
- **Grid.chart** refuses a chart that has problems; the screen keeps it and shows the words.
- **Page(name)** defaults to 'Untitled page'; the screen keeps a new page unnamed.

## 9. A staged plan

**Stage 0, in neema2/legend-lite#47 before merge (small):**
- items 2–4 of §6 (done: da1f546);
- the PR description rewritten plainly: what the API is, the one behaviour change (a grid's measures list gone, saved
  ones converted on open) at the top, and the leftovers of §8 named as such, not "done".

Then review, the full local set, CI, merge.

**Stage 1a, the test foundation, first (the user, 2026-10-17: "write that into the homework doc as a part of the
command refactor"):** see §11. The tests come before the commands they protect, so each command lands against them.

**Stage 1, the command type and a pure page `apply` (about 6–10 files):**
- A `Command` union and `apply(PageState, Command)`.
- Its grid half is grid-edits.ts plus the extracted handler rules; its page half takes over §8's rules.
- Headless gets one `apply` entry; Python's methods become wrappers that only build commands.
- The screen builds the same command values and applies them onto today's holders.
- Each step's test: the screen's action and Python's call produce the identical command.

**Stage 2, one page state on the screen (large):**
- A page state owner that CubePage, BandBoard, ChartPanel and PageApp render from.
- Page-level undo, with combined entries for actions that change a grid and the page together.

**Stage 3, optional:** commands as the live channel between Python and an open page, instead of re-serving the whole
document and rebuilding every grid.

## 10. Cost of the Python bridge, for the record

A typical sequence makes about 10–12 synchronous calls:
`Page()`, `page.grid(rows=, measures=, filter=, sort=)`, `.group()`, `.sort()`, `.chart()`, `view()`, `save()`.

Each grid call:
- reopens the grid from the document;
- retypes it;
- re-reads every live frame;
- compiles.

A shown page re-serves the whole document on every change. Batching commands into one call (open, type and compile
once) is the natural fix, and gridEdit already works that way.

## 11. The test foundation: what the suite lacks, and the tests that close it

**Why.** DataCube's suite is broad and thin in one place. It has 129 unit and DOM test files (about 1,900 cases). 114
of its 127 source modules are reached by them, and the other 13 (the app shell, `app/datacube-app.ts` and
`app/sources.ts`, and the headless bridge) by the real-Chromium harnesses and Python's suite. But PR
neema2/legend-lite#47's audit found about twelve real bugs that every one of those tests missed. Nearly all of them
lived between pieces:
- the configuration against its copy folded into the query;
- the screen's glue against the headless glue;
- a chart's saved values against its grid;
- a page-level action against a grid's state.

Each piece had its tests; nothing checked that the pieces agree. These four close that gap, in this order:

1. **Parity: the screen and Python, one action, one result.** It is the program's core promise, and no test asserts it
   today.
   - **Before commands:** for each grid edit, the cube document the screen writes (its handler through the state owner,
     then `writeCube`) equals the one headless `gridEdit` writes for the same edit.
   - **With commands:** the screen's control and Python's call build the identical command value, and `apply` gives
     the identical page.
   - It would have caught #47's divergences in open groups, `configure` and the configuration read back
     (`fromSnapshot`).
2. **Round trips, as properties.** Random edits through `apply` (a seeded generator, in the spirit of
   `test/fuzz.test.ts`), then write, read, open and compare: the page comes back identical. Plus the invariants on
   duplicated representations:
   - `fromSnapshot(applyToSnapshot(s, c))` changes nothing a query reads (this alone would have caught #47's first
     blocker);
   - a chart's selection is in its grid's filter;
   - a live chart's saved spec is `followCube` of its grid.
3. **A corpus of real saved pages, one or more per version.** Pages of versions 1–4, with measures lists, pivots, live
   and frozen charts, sheets and stacks, kept as fixtures. Each opens with its expected notes and its figures, on the
   screen (a harness) and in Python (`Page.load`). Today the migration is tested on synthetic documents only.
4. **Line and branch coverage, measured.** Coverage over the Node tests (c8 or Node's own coverage), run once to find
   the dark corners, then kept as a report rather than a gate. Today nothing is measured.

**The rule from here** (the user's question, 2026-10-17): no fix without a test that fails without it; a fix found by
an audit or a review names its test in its commit.
