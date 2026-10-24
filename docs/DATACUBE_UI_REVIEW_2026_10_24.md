# DataCube UI review, 2026-10-24

The user asked for a deep dive on the whole UI, starting with tiles: the tile's title bar and what is in it, the chart
options panel (it cannot be closed except by choosing Options again), the page's ≡ menu, the page's title bar and the
grid's status bar. This is what the product looks like today, measured in a browser (the built site, 1400×900 and
1024×768, over the example pages), and what to change. Nothing here is built yet.

## What is there today

### Tiles

Every tile has a head: its **title** (drag to move, double-click to rename), then its kind's own controls, then
**⊞ Layouts**, **⤢ Maximise** and **× Remove** (`layout/band-board.ts:430-490`).

- **The × is still there.** It is invisible (opacity 0) until the tile is hovered or focused, hidden in view mode and
  on a narrow board, absent on a lone grid (the only tile on its sheet: its head is hidden, its controls move to the
  page bar) and replaced by a × per tab in a stack. A text tile can be removed only by its ×; a chart by its × or its
  right-click's Remove; a grid by its × or View ▸ Remove from Page.
- **Always visible, whatever the tile is doing:** a chart's state pill (**Dynamic**, **Pinned** or **Frozen**: it
  follows the grid; it keeps its own grouping and follows the filters; it keeps both as they are now), a grid's source
  name and its **Live/Snapped** pill. On the KPI dashboard that is six "Pinned" pills in a row, saying the same thing.
- **A grid tile on a dashboard carries four layers of chrome:** the tile head, the drag zones (Row Groups / Column
  Labels), the columns panel's strip and its status bar. On the joins example, two grids spend about a third of the
  height on chrome.
- **Hover-only controls have no touch equivalent.** On an iPad nothing hovers: the × and ⤢ show only once the tile
  has focus.
- **Each kind has its own right-click.** A grid's is the full menu (now with View ▸ Move to Sheet, Remove from Page);
  a chart's and a KPI's are Options..., Show Grid, Move to Sheet ▸, Remove; a text tile has none.

### The chart options panel

Right-click a chart → **Options...** floats a 220px form over the chart's left edge, inside the tile. It has no
heading and no ×; Escape and a click elsewhere do nothing; the only way to close it is Options... again, which shows
no checked state (`page/cube-page.ts:1242-1259`, `ui/chart-panel.ts:175-249`). It covers the chart it edits, and in a
small tile the form is cut off at the tile's bottom with no scroll (1024×768: the last field is clipped).

### The page's ≡ menu

`page/page-app.ts:1267-1298`: **Create ▸** (Data Source…, Sheet, Association…, Data Space…, Blank Page) · **Arrange…**,
**Undo**, **Redo**, **✓ Edit Layout** · **Save**, **Save As…**, **Open…**, **Share…**, **Export ▸** (Page File (JSON))
· **View ▸** (Generated Pure & SQL…) · **Settings...**

- **Blank Page sits under Create**, beside Sheet and Data Source, though it replaces the whole page.
- **The groups are not by purpose:** file actions sit in the middle, Undo/Redo sit with layout, and View ▸ and Export ▸
  have one entry each.
- **There is no Text Box** in the page's Create, though every grid's Create has one.
- **Settings... opens the first grid's settings,** which on a page of several grids is not obviously which one.
- **"Settings..." uses three dots** where every other entry uses "…".

### The page's title bar

≡ · the page name (a bordered box, "Untitled page" in italics until named, a • when changed) · the sheet tabs (each a
fixed width, truncated: "Reference l…", "Drill and br…" with half the bar empty) and + · a spacer · the fold chevron ·
and, for a lone grid, its source name and Live/Snapped pill at the far right
(`page/page-app.ts:216-279`).

- **The name box looks like a tab,** beside the tabs it is not one of.
- **The lone grid's source and pill drift to the far right of the page bar,** away from the grid they describe; on a
  page of several grids the same information is in each tile's head.
- **The Snapped pill's dashed outline reads as a focus ring,** not as a state or a button.
- **The header comments at `page-app.ts:6-13` still describe a ≡ in each grid's head,** which went on 2026-10-23.

### The grid's status bar

`app.ts:1436-1539`: **⚙ Properties** and **⧨ Filter** as underlined blue links (both also in the right-click) · "3
rows × 20 cols in 124ms" · the receipt chip ("this tab (+1)") · a note · the truncation warning · the selection's
statistics · a 192×14 progress box · the planner's word ("local").

- **"3 rows" counts the top level's groups,** not the data's rows (the start page has 200,000).
- **"this tab (+1)" and "local" mean nothing to a person** who has not read the receipts design.
- **The progress box is always there,** an empty bordered rectangle that looks like an input when idle.
- **An error runs into the bar as truncated red text** (the joins example: "…sales does not exist! Did you mean…").
- **The link styling is the one place the app looks like a 2005 web page.**

## Found on the way (not on the list)

1. **A bug: opening "Joining tables" after another example fails.** Open… → Chart gallery (or Chart examples) →
   Open… → Joining tables: both grids say *Catalog Error: Table with name sample_sales does not exist! Did you mean
   "sample_sales_2"?* Opening Joining tables first, or Chart examples after it, works. The new page's copy of the sales
   file is named `sample_sales_2` (the old page's still held `sample_sales` when it loaded), then the old page lets
   its copy go, and the new grids still query `sample_sales`.
2. **A grouped grid shows a blank column for every text column.** The Data sheet grouped by region shows nine empty
   columns (order_id, sold, city, channel…): a text column's default aggregate is `unique`, blank whenever a group
   holds more than one value. Upstream DataCube's default, and the first thing a person sees on the start page.
3. **The columns panel lists every pivot leaf** (notional 2021 … 2025, Total; pnl 2021 …) on the start page: 25
   checkboxes for 3 measures.
4. **Glyphs from four sets:** ⊞ ⤢ × ✎ ⚙ ⧨ ▸ ⌃, each from a different font, at different weights and baselines.

## What to change

### 1. The tile: content first, chrome on demand

- **Always visible: the title, and what is unusual about the tile.** A chart's state shows only when it is not the
  default (a small 📌 for Pinned, ❄ for Frozen; nothing for Dynamic); a filter chip shows while a click-filter holds;
  a grid's Snapped shows as a small mark, Live as nothing. The source name goes to the tooltip and the status bar.
- **Everything else on hover or focus,** and on touch when the tile is tapped (a selected tile shows its head's
  controls until another is tapped): the state menu, ⊞, ⤢, ×, ✎.
- **View mode (Edit Layout off) is the reading mode:** no ×, no ⊞, no drag zones, no columns strip, a thin status bar;
  Edit Layout brings them back. The page menu's Edit Layout already exists; it becomes the switch for this.
- **One right-click shape for every tile:** Options… (or Properties… for a grid), then the kind's own entries, then
  View ▸ Maximise, Move to Sheet ▸, Remove — the grid's, the chart's, the KPI's and the text tile's alike.

### 2. The chart options panel: a pane that closes

- **A heading with a ×**, Escape closes it, a click on the page outside it closes it. Options… only opens it (or
  brings it forward): it is no longer a toggle.
- **Out of the chart it edits:** a pane docked at the page's right edge (Power BI's Format pane, the grid's own
  Properties pattern), the chart left whole beside it and its marks highlighted — or, at least, a popover anchored to
  the tile that may overflow it and scrolls within the window, so a small tile never clips it.

### 3. The page's ≡ menu, grouped by purpose

In the order of the menus' shared skeleton (below, "The menus"):

1. **New Page** (was Create ▸ Blank Page), **Open…**, **Save**, **Save As…**, **Share…**, **Export Page File…** (one
   entry until a second format arrives)
2. **Undo *step***, **Redo *step***
3. **Edit Layout** (**Done Editing Layout** while on), **Arrange…**
4. **Create ▸**: Data Source…, Sheet, Text Box, then Association…, Data Space…
5. **Settings…**: the app's, not the first grid's (a grid's own window is its Properties…)
6. **View ▸**: Hide the Bar (Show the Bar while hidden), Generated Pure & SQL…, Keyboard Shortcuts…

### 4. The page's title bar

- **The page name as a title,** not a box: plain text, renamed by click, with "Unsaved changes" (or a •) beside it.
- **Tabs sized to their names,** truncated only when the bar runs out, the ▾ list for the rest.
- **The lone grid's source and state in its own status bar,** as every grid's — the page bar holds the page's things.
- **One pill style** for states (filled, quiet), never a dashed outline.

### 5. The grid's status bar, a readout a person reads

- **Left: what the grid shows** — "200,000 rows · 3 groups" (the leaf count the grid already queries), and the
  selection's Sum · Average · Count when cells are selected (Excel's status bar), as today.
- **Right: chips that say something** — "Filtered" (only while a filter is on; click to edit), "Snapped 2 min ago" or
  "Live", the query's time on hover, a warning icon for a truncated result or an error (click for the details).
- **The progress bar only while something runs:** a thin bar along the grid's top edge, not a box in the status bar.
- **Properties and Filter leave the bar** (both are in the right-click; the bar is a readout, not a toolbar) — or stay
  as icon buttons, not links.
- **Receipts and the planner's word behind one "ⓘ" chip** that opens the Receipts window.

### 6. The rest

- **Fix the example-switch bug** (found 1).
- **A grouped grid's text columns at group level:** show the count of values in muted text ("3 values"), or hide a
  `unique` column at group level until expanded — instead of a blank (found 2).
- **The columns panel:** pivot leaves folded under their measure by default (found 3).
- **One icon set,** inline SVG at one weight, for the tile head, the status bar and the bars (found 4).

## The menus: one design language, two scopes

The user, 2026-10-24: the page's ≡ and a grid's right-click are different and stay different, but should share a
design language -- not converge. Today there are five menus (the page's ≡, a grid's right-click, a chart's or KPI's
right-click, a sheet tab's right-click; a text tile has none), each with its own habits.

### What differs, on purpose

- **The page's ≡ is the page's scope:** the file (new, open, save, share, export), the sheets and the layout, the
  page's history, the app's settings. It never lists what is done to a column or a grid.
- **A right-click is the scope of what is under the pointer:** the cell and its column first, then its grid, then the
  tile it sits in. It never lists the file (save, open).

### What they share

**1. One skeleton, in one order.** Each menu uses the sections its scope has, always in this order, a line between:

| # | Section | The page's ≡ | A grid's right-click | A chart's, a KPI's, a text tile's |
|---|---|---|---|---|
| 1 | Out: what leaves the app | New Page, Open…, Save, Save As…, Share…, Export | Export ▸, Email ▸ | -- |
| 2 | Edit: history and copies | Undo *step*, Redo *step* | Edit ▸ (Undo *step*, Redo *step*, copies) | -- |
| 3 | The data | -- | Sort ▸, Filter ▸, Pivot ▸, Join ▸, Extended Columns ▸ | Show Grid |
| 4 | The look | Edit Layout / Done Editing Layout, Arrange… | Resize ▸, Pin ▸, Hide *column*, Conditional Formatting ▸ | -- |
| 5 | Create ▸ | Data Source…, Sheet, Text Box, ─ Association…, Data Space… | Data Source…, Copy of This Grid, Visualization, Text Box | the same as the grid's |
| 6 | Its window | Settings… (the app's) | Properties… | Properties… (a chart's, today Options...); Edit Text… |
| 7 | View ▸: how it is shown, and where | Hide/Show the Bar, Generated Pure & SQL…, Keyboard Shortcuts… | Expand All, Collapse All, ─ Hide/Show Controls, Keyboard Shortcuts…, ─ Move to Sheet ▸, Remove from Page | Maximise / Restore, ─ Move to Sheet ▸, Remove from Page |

A sheet tab's right-click is the same shape at its size: Rename…, Move Left, Move Right, ─ Delete Sheet.

**2. The same words for the same things, in every menu.**

- **Shared entries:** Create ▸, View ▸, Export ▸, Undo/Redo, Move to Sheet ▸, Remove from Page, Keyboard Shortcuts…,
  Data Source…, Properties….
- **A scope's own window is "Properties…".** Today it is "Properties..." on a grid and "Options..." on a chart, and
  "Settings..." on the page opens the *first grid's* settings. "Settings…" becomes the app's alone, in the page's ≡.
- **Remove and Delete mean different things.** Remove takes a thing off the page and can be undone (Remove from Page,
  for every tile; today a chart says "Remove"). Delete destroys it (Delete Sheet; today "Delete").

**3. The same marks.**

- **"…" means the entry opens a window or asks for more,** and is always the one character "…". Today 8 entries use
  "..." and 12 use "…".
- **▸ means a submenu,** and a submenu has two entries or more. Today the page's Export ▸ and View ▸ have one each:
  until a second arrives, the entry stands alone ("Export Page File…").
- **An on/off toggle says what it will do, and flips** (the user, 2026-10-24: a missing ✓ is not clear -- an
  unchecked toggle looks like any other entry): Hide Controls / Show Controls, Hide the Bar / Show the Bar, Edit Layout
  / Done Editing Layout, Maximise / Restore, Add Data Bars / Remove Data Bars, Values First in Column Headers / Measures
  First in Column Headers. Today Edit Layout and Measures First in Column Headers carry a ✓ instead, and Data Bars has
  no verb.
- **✓ marks the current pick among several**, never an on/off: a chart's Dynamic / Pinned / Frozen, the sheet shown
  in the sheet list, the icon set chosen, Pin Left / Pin Right.
- **A line separates sections, and sets apart a destructive entry,** which comes last in its group: Remove from Page,
  Delete Sheet, New Page (which replaces this one).
- **Shortcuts are shown right-aligned** where one exists: Undo Ctrl+Z, Copy Ctrl+C, Properties… Ctrl+E, Save Ctrl+S.
  MenuItem has no field for one today.

**4. The same rules.**

- **Title Case for commands** (Copy Selected Cells, Add New Column…). Sentence case only where an entry is a value
  rather than a command (a column's name, "% of parent row total").
- **An entry that cannot apply here is shown disabled, with why on hover, never left out** (the grid's rule since
  P2-211). It is left out only where the host can never offer it: no pages service, no Open….
- **An entry names its target when the menu could act on more than one:** Hide notional, Copy Column notional, Undo
  Sort by notional.
- **At most two levels of submenu** (Conditional Formatting ▸ Color Scales ▸ is the deepest), and about a dozen
  entries at the top.

### The two menus, after

```
The page's ≡                              A grid's right-click (its order as people know it)
New Page                                  Export ▸
Open…                                     Email ▸
Save                         Ctrl+S       Edit ▸   Undo Sort by notional  Ctrl+Z ...
Save As…                                  ─────────
Share…                                    Sort ▸
Export Page File…                         Filter ▸
─────────                                 Join ▸ (always there: greyed, saying why, where nothing joins)
Undo Move tile               Ctrl+Z       Pivot ▸
Redo                                      Extended Columns ▸
─────────                                 ─────────
Edit Layout                               Resize ▸
Arrange…                                  Pin ▸
─────────                                 Hide notional
Create ▸                                  Conditional Formatting ▸
─────────                                 ─────────
Settings…                                 Create ▸
View ▸                                    Properties…                 Ctrl+E
                                          View ▸
```

## Suggested order

| Wave | What | Why first |
|---|---|---|
| 1 | The example-switch bug; the chart options pane closes (×, Escape, click outside) and never clips; the status bar's idle box, labels and error chip; the tile's always-visible chrome cut to the title and what is unusual | Each is small, each is felt on every page |
| 2 | View mode vs Edit Layout; the menus' design language (the skeleton, words, marks and rules above) for every menu; the title bar (name, tabs, the lone grid's state) | Structural, touches every tile and every menu |
| 3 | The options pane docked at the page's edge; grouped text columns; the columns panel; one icon set | Larger design work |

Screenshots of each state (the start page, the chart examples and the gallery's KPI dashboard, the chart options at
1400 and 1024 wide, the page menu, the joins example's error, the status bar) were taken with a scratch Playwright
script over the built site (`bazel build //datacube:shots`, then the runfiles' `demo/harness.mjs` serve).
