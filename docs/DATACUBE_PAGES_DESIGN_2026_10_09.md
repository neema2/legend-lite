# DataCube pages: every tile equal, easy layouts, the whole page saved (design, 2026-10-09)

**Status: proposed, for the user's decision** (§5). `datacube/` is the DataCube + Python line's since 2026-10-09
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

### 3.2 A layout of splits, filling the page

The page's layout is the tree the 2026-09-28 design already put in the page document (§5.2, "the JSON has the tree"),
with the node it left for later now first: **`split`** -- a row or a column of children with proportions -- over
**`tile`** leaves (and **`tabs`** next, §5). The page fills the window; tiles fill their panes.

Why splits, against that design's ruling for a scrolling grid (§4.1): what the user asks for -- two side by side, 2x2,
3x3, one on top and N below -- are splits; a split has nothing to collide, compact or cascade, so a drag or a resize
moves one divider and cannot spread across the page; and it saves as a small tree. The ruling's objection, that splits
do not reflow on a narrow screen, is answered by deriving (never storing) a single stacked column below a width, as the
grid's `fitToColumns` does today. The free grid (`tile-layout.ts`) stays a node type a scrolling dashboard of many small
tiles can use later; a page is a split tree until then.

### 3.3 Arranging, three ways

1. **A layout picker.** A small layout button in each tile's title bar (shown on hover) and an **Arrange** button on the
   page open thumbnails of the common layouts: side by side, stacked, 2x2, 3x3, one on top and N below, one left and N
   right, one large and two small, and "even out". Hovering a thumbnail previews it on the page; clicking a slot puts
   *this* tile there and the others fill the remaining slots in reading order (a layout with fewer slots than tiles
   stacks the rest in its last slot's column; one with more leaves empty slots offering "add a grid / a chart").
2. **Drop zones.** Dragging a tile by its title bar over another tile shows that tile's five zones -- its left, right,
   top and bottom halves split it there; its centre swaps the two -- with the zone that will take it highlighted and
   an outline of the result. Escape cancels; letting go outside any tile cancels.
3. **Dividers.** Between panes, a divider drags to resize (snapping at thirds, halves and quarters); a double click
   evens out its row or column. A tile's title bar can maximise it (and restore).

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
- **`layout`**: the split tree over the views' tile ids.
- **Reopening** opens every cube through its source, each with its own planner and model (as New > Source does now),
  then lays them out. A v1 page reads as before (its one cube and charts, laid out as one column).
- Save, Share (the same compressed JSON in the link), Export > Specification and "changed since saved" cover every
  tile.

### 3.6 Held by

- The tree's operations as pure functions (split, swap, remove, resize, presets, the narrow derivation), with a fuzz
  test as the grid model has.
- **A browser test of a person's session**, in the pinned Chromium: open three grids from two sources, apply 2x2, drag
  a tile onto another's right half, resize a divider, maximise and restore, remove the first grid, save, reopen -- the
  same tiles, sources, views and layout; and no long task over 50 ms during any drag (the browser's long-task timing).
- DataCube's existing page and chart tests, moved off "the" grid.

## 4. Order

1. Every tile equal, the split tree, the picker, drop zones, dividers, the smooth gesture (the layout sprint).
2. The whole page saved and reopened (v2), several sources.
3. Python: a page of several frames and their charts from `ll.show(...)`
   (`docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`), on the same document.

## 5. Decisions for the user

1. **Splits for pages (recommended), or keep the scrolling 12-column grid and add presets to it.** Splits make the
   asked-for layouts natural and dragging unable to cascade; the grid keeps free placement of many small tiles
   (dashboards of KPIs), which a split tree does less well.
2. **Tabs** (several tiles in one pane, one shown): with the first sprint, or after it.
3. **Narrow windows**: show the page as one stacked column below a width (recommended), or keep the layout and scroll.
