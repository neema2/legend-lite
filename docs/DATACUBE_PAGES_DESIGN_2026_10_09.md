# DataCube pages: every tile equal, easy layouts, the whole page saved (design, 2026-10-09)

**Status: agreed with the user, 2026-10-09** (§5 records the decisions). `datacube/` is the DataCube + Python line's since 2026-10-09
(`docs/IN_FLIGHT.md`). Builds on `docs/DATACUBE_DASHBOARDS_DESIGN_2026_09_28.md` (§4 layout, §5 the page model) and
`docs/BI_AND_ETL_PLAN_2026_09_29.md` §B2 (pages); it does not replace their page model, it brings forward the part of it
they left for later.

## 1. The goal (the user, 2026-10-09)

"We need to work on datacube full page save for sure, and also we need a big sprint on datacube layout -- right now it
still feels like the first grid is somehow still special, dragging tiles around still sometimes hangs/freezes, and we
don't have a good way to do basic/easy tiling/layout of common layouts like two side by side, or 2x2, 3x3, 1 top + n
bottom -- we need some sort of hover over layout manager maybe."

## 2. What the code does today (read 2026-10-09; file:line as of `1d0fe772a`)

- **The first grid is special.** The page belongs to the first cube's own app: `CubeApp#ensurePage` (`app.ts:2470`)
  builds the board inside that app and moves its parts into tile `'grid'` -- not removable, an `anchor` (never in a
  row), 12x24 at the top (`page/cube-page.ts:155-166`). Added grids are other `CubeApp`s (`compact`). Only `'grid'` is
  refreshed and reconciled (`cube-page.ts:327-338`); a chart opened elsewhere falls back to it (`:220`); auto layout
  always puts it on top (`below('grid', ...)`, `:482-489`); and it is the only grid saved (`views()`, `:359-390`, "plan
  B2"). Export leaves the others out (`:405`). A detached chart from another source's grid would be planned against
  the first grid's model (`#sourceOf`, `:455-469`).
- **Dragging can hang.** Each pointer move (`layout/board.ts:454-464`) re-plans the layout from scratch and repaints;
  the first move already resizes the neighbours (a lifted tile heals its hole, `layout/tile-layout.ts:264-299`), so
  every placeholder change re-lays out the DataCube grids and resizes every ECharts chart, synchronously, on each move
  -- nothing is throttled to the frame (likely the hang; the model itself is fast, 0.2 ms a step for 200 tiles). A
  gesture can also stick: no `lostpointercapture` handler, and the dragged tile has `pointer-events: none` until a
  `pointerup` or `pointercancel` it may never get. Escape does not cancel; a drag below the fold does not scroll. No
  browser test drags a tile at all (BI plan §B2's "done when" asks for one).
- **No easy layouts.** The page arranges itself only as "grid on top, the rest in rows of four below" until a hand
  move; a `beside` helper exists unused (`tile-layout.ts:747`); there is no arrange menu.
- **Save keeps one grid.** The page document (`datacube.page` v1) can list several cubes (`page-document.ts:74-81`),
  but `writePage` writes one, `'cube'` (`:93-113`), and the demo refuses to open a page of several ("not built yet",
  `demo/boot.ts:1079-1086`). Added grids have no saved source (`gridOver`, `boot.ts:1489-1522`); edits in them do not
  mark the page changed (`cube-page.ts:203-206`).

## 3. The proposal

### 3.1 Every tile equal

A **page** owns the board, not the first cube's app. Every grid is a tile holding its own `CubeApp` over its own
source, made the same way whether it is the first or the fifth: removable, movable, refreshed, saved alike. Removing
the last tile leaves an empty page that offers a source (the start screen's choices). Charts belong to the grid they
follow, by its tile id, never by "the" grid. The page, not a cube, carries Save, Share, Export and "changed".

**The page bar and each grid's header** (the user, 2026-10-09: "Great I love it"). The page has a thin bar of its own
-- a shell that is not a tile -- so page actions always have a home, an empty page included:

- **The page bar** carries only the page's things: its name and the page menu (New, Open, Save, Share, Arrange,
  Export of the page, Settings, locking the layout).
- **Each grid's tile header** carries only that grid's things: its source, Live or Snapped, and its own menu (Undo,
  Properties, Ad Hoc, the export of its rows) -- the same for the first grid and the fifth.
- **One grid alone looks like a maximised tile**: it fills the page with no frame, and its header shares the page bar's
  strip (the page menu on the left, the grid's source, pill and menu on the right) -- one strip, as a grid alone looks
  today. When a second tile arrives, the grid's part moves down into its own tile header. Neither menu ever changes
  what it holds.
- **An empty page** keeps its bar and shows the choices of a source where the tiles were.

Rejected: a page with no bar of its own (only tiles). Save, Open, Share and New would have no home on an empty page,
or would be repeated in every tile's menu, leaving it unclear whether a tile's Save saves the page or the grid.

A cube embedded alone (the Query app, a notebook's cube) stays a `CubeApp` with its own title bar, which looks the same
as one grid on a page.

### 3.2 Bands: one way of arranging, scrolling or filling the window

A page is a **stack of bands**, top to bottom. A band is divided into **columns**; a column can itself be divided --
across into stacked parts, or again into columns -- so a band is a small tree of rows and columns with proportions,
whose leaves are tiles. Every band has a **height**.

- **Scrolling** (the default for a page of many bands, as Power BI and Grafana pages scroll): the bands keep their
  heights, and when they are taller than the window the page scrolls.
- **Fit to window** (a page setting, **on for a new page**: the user, 2026-10-09): the bands share the window's height,
  and nothing scrolls -- with one band, the whole page is one divided screen, as a split window is. A page with more
  bands than the window holds at 160px each scrolls instead, so fitting never squeezes a band too short to read.

What the user asked for, in bands: two side by side is one band of two columns; 2x2, two bands of two; 3x3, three bands
of three; one on top and N below, a band of one and a band of N; one big tile on the left and two stacked on the right,
one band of two columns whose right column is divided across (the user, 2026-10-09: "as long as can still do two columns
one big on left and two split on right"). A row of small KPI boxes is a band of many columns above the bands of grids
and charts.

Why bands, against the 2026-09-28 design's ruling for a free 12-column grid (§4.1), and against two separate page modes
(a scrolling grid OR a divided screen, the user's question): bands give top-level scrolling and a divided screen with
**one** way of arranging -- one engine, one way to drag, one set of presets, one saved form -- where two modes would be
two engines and a lossy conversion between them. A band has nothing to collide, compact or cascade: a drag drops into an
edge and a divider moves one boundary, so no tile is ever shoved and no drag spreads across the page. What bands give
up is free placement (a small box anywhere, others flowing round it) and a tile spanning two bands (divide a column
inside one band instead). The free grid (`tile-layout.ts`) can return later as what one band holds -- a band whose
contents are a wall of small tiles on squares -- if a page of many small tiles needs it; the saved tree has room for it
(the 2026-09-28 design's `grid` node).

**Narrow windows** (the user, 2026-10-09: stacked): below a width the page shows every tile one under another, full
width, in reading order, and scrolls; the arrangement itself is unchanged, and comes back when the window widens.
Arranging pauses while stacked. (Stepping down a column at a time, 3x3 to 2 to 1, can come later.)

### 3.3 Arranging

1. **A layout picker.** A layout button in each tile's title bar (shown on hover) and **Arrange** in the page's menu
   open thumbnails of layouts, each the page's own tiles arranged that way. Hovering a thumbnail previews it on the
   page; clicking applies it. **Every picker arranges the whole page**: a tile's own puts *that* tile in the first slot
   (marked in the thumbnails), Arrange keeps the reading order; the others fill the slots in reading order. As built
   (the user, 2026-10-09, trying it: "default 8-9 shapes with option to see more and option to do custom"):
   - **the standard shapes first**, the same nine in the same order for any number of tiles: all side by side, all
     stacked, a grid as square as it goes, one large tile on each side with the rest beside it, two rows, two columns;
   - **More layouts**: every way to cut the tiles into rows (four to a row; up to four rows, three for six tiles or
     more -- "several on top, then one and one"; "one, several, one") and columns of near-equal size, under the
     headings Rows, One large, Columns;
   - **Custom**: rows built by hand, each row's count a step up or down, previewed as it is built;
   - "Fit to window" and "Even out" below them. A shape that looks the same as another is shown once.
   (A tile's picker first arranged only the band the tile was in; with four tiles it showed two of them and turned
   2x2 into "2, 1, 1", so every picker arranges the page. Empty slots offering "add a grid / a chart" are not built.)
2. **Drop zones.** Dragging a tile by its title bar over another tile shows that tile's zones -- its left, right, top
   and bottom edges divide it there; its centre swaps the two (and, with tabs, adds it as a tab) -- and between two bands
   a line that makes a new band there; the zone that will take it is highlighted, with an outline of the result. Escape
   cancels; letting go outside any zone cancels.
3. **Dividers.** Between columns and parts, a divider drags to resize (snapping at thirds, halves and quarters); a
   double click evens out its row. A band's bottom edge drags its height. A tile's title bar can maximise it (and
   restore).
4. **Smart placement.** A new grid or chart goes beside the tile it came from when its band has room for one more column
   at a readable width, otherwise into a new band below it -- so a page rarely needs arranging by hand.
5. **Edit and view.** In view mode the handles, zones and "add" buttons are gone, so nothing moves by accident; edit
   mode shows them. A page opened from a share link opens in view mode.

Every arrangement is one undoable step.

### 3.4 Smooth by construction

During a gesture nothing inside a tile re-lays out: the page shows an outline of the result (a light overlay), and the
change is applied once, on drop. Pointer handling owns its gesture whatever happens: `lostpointercapture` and
`pointercancel` end it, Escape cancels it, a second pointer is ignored. Content that must follow its pane's size (the
grid's rows, ECharts) does so once per animation frame, not once per observer callback. A tile being resized by a
divider is the one exception that follows live, and only its content, at most once a frame.

### 3.5 The whole page saved (`datacube.page` v2)

- **`cubes`**: one entry per grid, each its own source and view (the cube document as today: the source by identity,
  the query, the configuration, the open groups). A grid opened from a file, a table or a remote source gets its saved
  identity as a saved query's does today.
- **`views`**: a grid view per cube; a chart view names the cube it follows (a detached chart keeps its own query).
- **`layout`**: the bands, each its tree of rows and columns over the views' tile ids, with the page's fit setting.
- **Reopening** opens every cube through its source, each with its own planner and model (as New > Source does now),
  then lays them out. A v1 page reads as before: its one cube and charts, its 12-column grid read as bands (cut where no tile crosses:
  across into bands, then into columns and stacked parts; tiles no straight cut divides, side by side) -- as built,
  2026-10-09, where this said "laid out as one column": its arrangement is kept rather than lost. The page's layout is
  written as version 2 from then on.
- Save, Share (the same compressed JSON in the link), Export > Specification and "changed since saved" cover every
  tile.

### 3.6 Held by

- The bands' operations as pure functions (divide, swap, move between bands, remove, resize, presets, smart
  placement, the narrow stacking), with a fuzz test as the grid model has.
- **A browser test of a person's session**, in the pinned Chromium: open three grids from two sources, apply 2x2, drag
  a tile onto another's right half and one onto the line below a band, resize a divider and a band, switch fit to
  window and back, maximise and restore, remove the first grid, narrow the window and widen it, save, reopen -- the same
  tiles, sources, views and layout; and no long task over 50 ms during any drag (the browser's long-task timing).
- DataCube's existing page and chart tests, moved off "the" grid.

## 4. Order (the user, 2026-10-09: tabs wanted, "can be phased appropriately")

1. **The layout sprint:** bands with the fit-to-window setting; the picker, drop zones, dividers, maximise; smart
   placement; edit and view mode; the smooth gesture; stacking when narrow; every tile laid out, moved and arranged
   alike (no grid pinned on top), each grid with its own tile menu.
2. **The whole page saved and reopened** (several sources), with the page bar (§3.1): the page owns Save, Open, Share
   and "changed", so the first grid becomes removable here -- the two are one change, since saving is the page's.
3. **Tabs:** pages as sheets (several pages in one saved document, as Excel's sheets and Power BI's pages), and tabs in
   a pane (several tiles in one place, one shown; the drop zone's centre adds a tab). As decided (§5, 5-7; §7): the
   sheet tabs in the page's bar, as a browser's tabs, the page's name in a box before them; stacked tiles made by a
   drop on a tile's middle. Sheets land first (3a), stacked tiles second (3b).
4. **Python:** a page of several frames and their charts from `ll.show(...)`
   (`docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`), on the same document.

Later, when wanted: a band holding a free grid of small tiles (§3.2); the narrow layout stepping down a column at a time;
saved layouts as one's own templates; cross-filtering between tiles (the 2026-09-28 design's §5.4); the sheets shown
in turn on a timer, for a screen on a wall (as Grafana's playlists and Power BI's TV mode).

## 6. Phase 2, how it is built (2026-10-09, before the first edit)

The user's go: "Do it" (2026-10-09). What changes, in plain words, then where.

**The page is a thing of its own.** Today the first grid's app is the whole page: its title bar is the page's bar, its
menu holds Save and Share, and the board lives inside it. Phase 2 puts a **page** above the grids
(`datacube/src/page/page-app.ts`, new): the bar (the page's name, the page menu, the host's status), the board, and the
empty page. Every grid is a tile holding a compact `CubeApp`, made the same way whether it is the first or the fifth,
so the first one is removable. A cube embedded alone (the Query app, a notebook, the engine page) stays a `CubeApp`
with its own title bar and its own board, as now: the page is DataCube's own app (`demo/boot.ts`).

- **The bar.** The page menu holds the page's things only: New (a data source), Open, Save, Save As, Share, Export of
  the page, Arrange, Undo/Redo Layout, Edit Layout, Settings, and the host's own entries (Generated Pure & SQL, where
  the planner runs). Each grid's own menu (in its tile header) keeps the grid's things, as phase 1 made it.
- **One grid alone** fills the page with no frame, and its header -- its source, Live/Snapped, its menu -- sits at the
  right of the bar's strip: one strip, as a grid alone looks today. A second tile moves it back into the tile.
- **The empty page** (the last tile removed, or New > Blank Page) keeps the bar and shows the choices of a source.
- **Every grid knows its source.** A grid added over a file, a warehouse table or a remote file now writes its source
  down as a grid opened "in place" does, so every grid can be saved. Each grid has its own planner over its own model;
  none uses the page's shared one specially. Opening a source in place is: the page emptied, then that grid added.
- **Saving the page** writes one cube document per grid (each its own source and view), a grid view for each tile, and
  each chart naming the grid it follows; the layout as phase 1 writes it. **Reopening** opens each cube through its
  source -- a file asked for, a saved query rerun, a warehouse signed in to, a remote file's keys asked for -- each with
  its own planner, then lays them out as saved. "Changed since saved" and Share cover every grid.
- **A frozen chart whose grid is removed** keeps reading that grid's query, as today: the removed grid is kept off the
  page (no tile) while a chart still reads it, saved as a cube with no grid view, and reopened the same way. It goes
  once its last chart goes. (Today such a chart runs on the first grid's planner; with no first grid, it keeps its own.)
- The demo's test handles: `window.__dataCube` is the page's first grid (read when asked, so it is never a removed
  one); `window.__dataPage` is new, the page (its views, its document, its layout).

**As built (2026-10-09)**, where the plan above needed a decision:

- **The host's status line and the planner's word stay in the first grid's status bar**, with the planes offered
  there, not in the page's bar: a readout belongs in the status bar (an earlier ruling: the title bar says what is on
  screen). The page's bar holds its name, its menu and, for a grid alone, that grid's header. The page decides which
  grid is first (reading order): when another grid becomes first (the first removed, the page rearranged) the readout
  moves to it; the other grids have no slot for it.
- **Every grid is made by the host's maker**, a copy too: Copy of Grid, and the grid a detached chart's Update keeps,
  are made by the maker of the grid they copy, starting where that grid is now. So the page knows every grid it saves,
  and the host knows each grid's file (its handle, kept per page and grid when the page is saved or reopened) and the
  table it was read into -- dropped once no grid on the page reads it.
- **Opening a saved page is latest-wins**, as opening a source in place is (P2-330): an open overtaken while it waited
  (a file asked for, a sign-in) lands nothing, and what it read is dropped.
- **The bar's name** is the page's own once it is saved (or reopened), else its first grid's report title -- what a
  grid alone was called; the Properties' Report Title still names a page of one grid.
- **The bar folds** to a lip (the earlier ruling: the folds live in one column at the right): its fold just left of a
  lone grid's header, whose last control -- the drag zones' way back -- stays at the far right above the zone bar's own
  fold. The bar is folded while every grid on the page says its "show title bar" setting is off -- for a grid alone,
  that grid's setting, saved with it as a cube alone's was; folding or showing the bar by hand writes it on every grid,
  and a grid made while the bar is folded starts with it off, so the grids left later say the same. A tile's header puts
  the zones' way back last, after the grid's menu, on a cube alone's board too.
- **A grid's columns panel** starts open for a grid opened alone (in place, or a page of one reopened), folded for one
  added beside others -- as a source added beside others starts at the row limit (the user, 2026-10-01).
- **Exports**: each grid's own menu exports its rows with the page's charts where the board puts them (an export holds
  one table), once there is a chart -- with none, its rows alone, as before; an added grid on a cube alone's board does
  the same. The page's menu has Export > Page File, the page's document. Each grid's menu has Remove from Page (the
  one way to remove a grid alone, whose tile has no frame).
- **Settings** saved on one grid are in effect on every grid on the page.
- **A detached chart's query edited** (Open in grid, Update) gets a kept grid of its own, so two charts detached from
  one grid never share an edit.
- **The download budget** for a grid-only page rises from 352,000 to 368,000 bytes (measured 364,791): every page is
  a page of its own from the start, so its board loads at startup; the layouts are still fetched when first opened,
  ECharts when a chart first draws.

Files: `datacube/src/page/page-app.ts` (new), `page/cube-page.ts`, `app.ts` (a grid's changes reported to its page; the
page's entries out of a compact grid's menus), `page-document.ts` (several cubes written), `layout/band-board.ts` (the
lone tile shown frameless), `app.css`, `demo/boot.ts`, `demo/index.html` if its host windows move, the demo's browser
harnesses that open the title bar's menu (`demo/verify-*.mjs`), and their tests: `test/page-app.test.ts` (new) and a
browser check of a page of two sources saved and reopened, with its first grid removed.

## 5. Decisions (the user, 2026-10-09)

1. **Bands** -- one way of arranging that scrolls or fills the window -- rather than a free scrolling grid, a page of
   splits alone, or two page modes; with one big tile beside two stacked kept (§3.2).
2. **Tabs**, phased: after the layout sprint and the whole-page save (§4).
3. **Narrow windows stack** (§3.2).
4. **A page bar of its own, each grid its own header; one grid alone shares the page bar's strip** (§3.1).
5. **Sheet tabs in the page's bar, as a browser's tabs** (the user, 2026-10-09: "Maybe now there is no 'page title'
   it's just the first tab name as a tab, then a + for more like web browser?"), always shown, one tab and a + for a
   new page; **the page's name in a box right after the menu** ("Or after the menu hamburger then start still with
   single tab and + for more?"; "Do it"), not at the far right, where a lone grid's own header is.
6. **Stacked tiles, made by a drop on a tile's middle** ("If people like stacked tiles and not too much work let's go
   for it and when you drag you get a tab stack"): the middle no longer swaps; swapping stays on the arrow keys and in
   Arrange.
7. **What is looked at is not saved; what is on the page is** (this design's, §7): a page reopens on its first sheet,
   a stack on its first tab -- so switching a sheet or a tab never marks the page changed.

## 7. Phase 3, how it is built (2026-10-09, before the first edit)

The user's go: "Do it" (2026-10-09), on §5's 5 to 7. What changes, in plain words, then where.

### 7.1 The bar (3a)

The page's bar reads, left to right: **the menu; the page's name, in a box; the sheet tabs and a +; the free space; a
lone grid's header (its source, pill, menu); the fold.**

- **The name box** says the page's name (as saved), "Untitled page" before it has one, and a dot when the page has
  changed since it was saved. A click renames it in place (Enter keeps, Escape leaves it); a page not yet saved takes
  the name for its first Save. Renaming is a change of the page, saved by Save. The browser's tab title says the same
  name, as it does now. The old derived name (the first grid's report title, §6) becomes the first sheet's name.
- **The sheet tabs** are a browser's tabs: the one shown is raised; a click shows another; a double click renames one
  in place; the + adds a sheet after the last and shows it; a tab's right-click menu has Rename, Move Left, Move Right
  and Delete (a sheet with tiles asks first, and its tiles go with it); dragging a tab along the strip reorders. Many
  sheets shrink their tabs, then the strip scrolls, with a list of every sheet at its end. The tabs take the arrow keys
  as a tablist does (no page-wide shortcut: the browser keeps Ctrl+PgUp and Ctrl+PgDn for its own tabs).
- **Folded**, the bar is its lip as now: the tabs go with it (folding is for room; unfold to change sheet).

### 7.2 Sheets (3a)

- A page is **one or more sheets**, each a whole screen of tiles with its own bands, its own Arrange, fit and layout
  undo. A sheet's name is its own once given by hand; until then it is named after its first grid's report title (its
  source), else "Sheet N" -- so a page of one sheet looks as a page does today, its source's name in the one tab.
- **Every tile is on one sheet.** A new grid or chart goes on the sheet shown (a chart made from a grid, beside that
  grid, as now). A tile moves to another sheet from its menu (Move to Sheet: each other sheet, or a new one), or by
  dragging it by its header onto a sheet's tab.
- **A chart follows its grid on any sheet**: a sheet of charts over grids that live on another sheet. A grid on a sheet
  not shown keeps running -- its charts stay live -- and is drawn when its sheet is shown.
- **The empty page** (no tile on any sheet) shows the host's choices of a source, as now. **An empty sheet** of a page
  that has tiles says so, with a way to add a data source to it.
- **One grid alone on the sheet shown** shares the bar's strip, as a lone grid does now. The host's readout is in the
  first grid's status bar of the sheet shown (a sheet with no grid has none).
- A **cube alone** (the Query app, a notebook) has one sheet and no tabs: its board is as now.
- Locked (view mode), the sheets still switch; nothing moves.

### 7.3 The page saved, version 3 (3a)

`datacube.page` version 3 replaces `layout` by **`sheets`**: each `{ id, name?, layout }`, in order, `name` only when
given by hand, `layout` the bands as version 2 writes them. `cubes` and `views` are as in version 2; every view's tile
is in exactly one sheet's layout. Versions 1 and 2 read as one sheet. The share link carries the same JSON (its
compression dictionary, `p1`, is frozen; new words cost only a few bytes). Which sheet is shown is not saved (§5, 7): a
page reopens on its first sheet. A grid's export lays out the charts of its own sheet.

### 7.4 Stacked tiles (3b)

- **A stack** is one place in a band holding two tiles or more, one in front; its header is their tabs (each tile's
  title), with the front tile's own buttons at its right. A click on a tab brings it to the front.
- **Made by a drop on a tile's middle** (the zone that swapped): the dragged tile joins that tile's place, in front. A
  drop on a stack's middle or its tab strip adds to it. Dragging a tab out takes that tile anywhere else; a stack left
  with one tile is a plain tile again. A tab dragged along its strip reorders the stack.
- Arrange counts a stack as one place, and keeps it; the keyboard moves a stack as one; maximise shows the stack. An
  export shows each stack's front tile.
- **Saved** in its sheet's layout as a stack node of its tiles in order (version 3: a version 3 reader of 3a refuses
  it, and none is left by then). The front tile is not saved (§5, 7): a stack reopens on its first tab.
- `bands.ts` holds the stack as a node of the tree, under the same rules (`problems`: two tiles or more, each tile once,
  the front one of them), with the fuzz extended to stacks.

### 7.5 Where

3a: `datacube/src/page/page-app.ts` (the bar: name box, sheet tabs; an empty sheet), `src/page/cube-page.ts` (sheets: a
board per sheet, tiles by sheet, Move to Sheet, a drag onto a tab), `src/layout/band-board.ts` (a drag that ends outside
the board, on a tab), `src/ui/sheet-tabs.ts` (new: the strip), `src/page-document.ts` (version 3), `src/app.ts` (a
grid's Move to Sheet entry), `src/ui/menu.ts`, `src/app.css`, `demo/boot.ts` (the name box's rename, the changed dot),
the demo's browser harnesses that read the bar's title, and their tests: `test/sheet-tabs.test.ts` (new),
`test/page-app.test.ts`, `test/page-document.test.ts`, `test/share-link.test.ts`, and browser checks of a page of two
sheets -- a chart on one following a grid on the other -- saved, shared and reopened, and a tile dragged onto a tab.

**As built (3a, 2026-10-09)**, where the plan above needed a decision:

- **A tile moved to another sheet** (its menu's Move to Sheet -- a grid's own menu, a chart's right-click menu -- or a
  drag onto the tab) leaves the sheet shown as it is; the tile goes to the bottom of its new sheet, and the move is
  said. New ▸ Sheet in the page's menu adds a sheet as the + does.
- **Renaming the page is a change** ("changed since saved"): the page's definition leaves its name out (so a page
  saved under another name compares equal), so the host compares the name it was saved under as well.
- **The host's readout** is in the first grid's status bar of the sheet shown, and moves with the sheet shown.
- **Folding the bar** keeps its rule (folded while every grid on the page says its title bar is hidden), page-wide,
  across sheets.
- **The download budget** for a grid-only page rises from 368,000 to 378,000 bytes (measured 375,751; main before it
  367,729): the tabs, a board per sheet and the name box are on screen from the start.
- The browser checks that compared a page's `layout` now compare its `sheets` (with `layout` gone they compared
  nothing to nothing, and passed).

**As built (3b, 2026-10-09)**, where the plan above needed a decision:

- **A stack is made, and added to, by a drop on a tile's middle.** Its tab strip is its header, whose drop zone is the
  top edge, as any tile's: a drop there divides the place, so the strip is not a target of its own (the plan above
  named it as one).
- **A tab pressed brings its tile to the front at once**, as a browser's tab does -- not a change of the page (§5, 7);
  a tab moved along its strip is (the order is saved). A tile shown by the page (a chart's editing grid, a new tile)
  is brought to the front of its stack.
- **The download budget** rises from 378,000 to 381,000 bytes (measured 378,297; sheets before it 375,928).

3b: `src/layout/bands.ts` (the stack node), `src/layout/band-board.ts` (the stack's tab header), `src/page/cube-page.ts`,
`src/page-document.ts`, `src/export-model.ts` (the front tile), `src/app.css`, and `test/bands.test.ts` (the fuzz),
`test/band-board.test.ts`, and the browser session (`demo/verify-layout.mjs`): a stack made by a drag, a tab dragged
out, saved and reopened.
