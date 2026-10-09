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
- **Fit to window** (a page setting): the bands share the window's height, and nothing scrolls -- with one band, the
  whole page is one divided screen, as a split window is.

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

1. **A layout picker.** A layout button in each tile's title bar (shown on hover) and an **Arrange** button on the page
   open thumbnails of the common layouts: side by side, stacked, 2x2, 3x3, one on top and N below, one left and N
   right (and its mirror), one large and two small, and "even out". Hovering a thumbnail previews it on the page;
   clicking a slot puts *this* tile there and the others fill the remaining slots in reading order (a layout with
   fewer slots than tiles stacks the rest in its last slot's column; one with more leaves empty slots offering "add a
   grid / a chart"). The picker arranges the band the tile is in; "Arrange page" arranges every tile.
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
  then lays them out. A v1 page reads as before (its one cube and charts, laid out as one column).
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
3. **Tabs:** pages as sheets (several pages in one saved document, tabs along the bottom, as Excel's sheets and Power
   BI's pages), and tabs in a pane (several tiles in one place, one shown; the drop zone's centre adds a tab).
4. **Python:** a page of several frames and their charts from `ll.show(...)`
   (`docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`), on the same document.

Later, when wanted: a band holding a free grid of small tiles (§3.2); the narrow layout stepping down a column at a time;
saved layouts as one's own templates; cross-filtering between tiles (the 2026-09-28 design's §5.4).

## 5. Decisions (the user, 2026-10-09)

1. **Bands** -- one way of arranging that scrolls or fills the window -- rather than a free scrolling grid, a page of
   splits alone, or two page modes; with one big tile beside two stacked kept (§3.2).
2. **Tabs**, phased: after the layout sprint and the whole-page save (§4).
3. **Narrow windows stack** (§3.2).
4. **A page bar of its own, each grid its own header; one grid alone shares the page bar's strip** (§3.1).
