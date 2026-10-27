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
   **Fixed 2026-10-25** (`app/sources.ts` readFile and borrow, `app/join-sources.ts` leafTable). The cause was that
   the gallery and the join example use the same sales sample. A join reopened looks up a table already read from the
   same file, and it found the old page's `sample_sales` before the new page's own `sample_sales_2`; the old page then
   dropped it. The join now takes the opening page's own copy first. A table it does take from a grid stays until the
   join's grid is made, even if that grid's page goes, because the join borrows it. `demo/verify-join.mjs` opens the
   Chart gallery before the join example.
2. **A grouped grid shows a blank column for every text column.** The Data sheet grouped by region shows nine empty
   columns (order_id, sold, city, channel…): a text column's default aggregate is `unique`, blank whenever a group
   holds more than one value. Upstream DataCube's default, and the first thing a person sees on the start page.
   **Kept as it is (the user, 2026-10-25).** Showing a muted count ("3 values") where `unique` comes back blank, or
   making distinct count the default, were weighed; the user chose to keep upstream's `unique` for now.
3. **The columns panel lists every pivot leaf** (notional 2021 … 2025, Total; pnl 2021 …) on the start page: 25
   checkboxes for 3 measures.
   **Fixed 2026-10-25** (`ui/columns-panel.ts`). Every measure a pivot spread now arrives folded under its own row,
   whatever the block's size, so the start page lists its 3 measures. Folding or unfolding one by hand sticks. A
   measure's box hides or shows the whole block, and it is part-ticked when only some of its columns show, so a
   folded block still says it is partly hidden. Tested in `test/columns-panel.test.ts`; `demo/verify-features.mjs`
   checks the real page.
4. **Glyphs from four sets:** ⊞ ⤢ × ✎ ⚙ ⧨ ▸ ⌃, each from a different font, at different weights and baselines.
   **Fixed 2026-10-25** (`ui/tile-icons.ts` chromeIcon, closeMark). The page's chrome is now drawn in the one set the
   tile states and the status bar's tools already used: one stroke on one 16-unit square, in the text's colour, sized
   to the text of its button. That covers the tile header's layouts, maximise / restore and remove; the page's and a
   lone grid's ≡; the sheet list's caret and each tab's ×; the menus' submenu chevron; the columns panel's fold and
   collapse chevrons; and every close or take-off ×, in windows, chips, the status bar and the editors. A button whose
   glyph was its only name is now named for a screen reader. The grid's tree carets followed (▸ ▾, also in the JSON
   field picker and Ad Hoc's member picker), then -- found by the branch's audit, 2026-10-25 -- the columns selector's
   move arrows (› ‹) and Column Properties' fold carets, which were typed in the stylesheet. A guard in
   `test/guardrails.test.ts` fails on a typed ×, ≡, ⊞, ⤢, ⤡, ▸, ▾, › or ‹, spaced or not.

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

### 1a. One tile header for every kind (agreed with the user, 2026-10-25)

This replaces §1's first bullet: a quiet state is shown too, so every tile reads the same way.

Every tile -- a grid, a chart, a KPI, a text box -- has the same header, in the same places:

```
[state] Title                                   ⊞ ⤢ ×
```

- **Left of the title: the tile's state, always shown, as an icon.** Quiet (muted) for the usual state, accented for an
  exception, so a page of ordinary tiles is calm and the one that is different stands out.

  | Tile | States (quiet first) | Its menu, on a click of the icon |
  |---|---|---|
  | A grid | ● Live (quiet) · 📷 Snapped (amber, its time on hover) | Snap Again / Go Live (Snap when live) |
  | A chart or a KPI | 🔗 Dynamic (grey) · 📌 Pinned (accent) · ❄ Frozen (blue) | Dynamic / Pinned / Frozen, ✓ on the current one (a pick among several) |
  | A text box | ✎ (quiet when viewing, accented while editing) | none: a click edits; Done ends it |

- **Right: the tile's actions, ⊞ (layout: move and resize) ⤢ (maximise) ×** (remove from page), shown on hover, on
  keyboard focus, and on a tap on a touch screen -- never permanently, so the header stays quiet.
- **The source's name leaves the header:** it is the title's tooltip and the status bar's words. The Live/Snapped
  pills go.
- **View mode (Edit Layout off):** the state icons stay, and stay clickable (Go Live is no layout change); a text
  box's ✎ goes (no editing in view mode); ⊞ and × go (nothing moves, nothing is removed); ⤢ stays.
- **A lone grid** (the only tile on its sheet) has no header at all: its state icon goes in its sheet's tab, its status
  bar says it in words ("Live", "Snapped 2 min ago"), and there is nothing to lay out, maximise or remove.
- **A maximised tile** keeps a slim header with ⤡ (restore) in place of ⤢.

**Built 2026-10-25.** `ui/tile-icons.ts` draws the states (inline SVG, coloured by `app.css .dc-tile-state[data-state]`;
each carries its word for a screen reader and a test); `layout/band-board.ts` puts a tile's `state` before its title,
says its `about` on the title's hover and flips ⤢/⤡; `page/cube-page.ts` gives a chart its state menu (Dynamic / Pinned /
Frozen, ✓ on the current one, what each does on hover) and a text box its pencil; `app.ts` gives a grid in a tile its
state (`#plane`, one description for its pill and its icon) with Snap, or Go Live and Snap Again, and its source and
plane in words in its status bar (`.dc-status-source`); `ui/sheet-tabs.ts` holds a lone grid's state in its sheet's tab.
A grid alone on a page of its own (no tile, Studio's query app) keeps its title bar's pill as it was. The zones' way
back, while folded, stays among a tile's buttons (a lone grid's, at the page bar's right).

### 2. The chart options panel: a pane that closes

- **A heading with a ×**, Escape closes it, a click on the page outside it closes it. Options… only opens it (or
  brings it forward): it is no longer a toggle.
- **Out of the chart it edits:** a pane docked at the page's right edge (Power BI's Format pane, the grid's own
  Properties pattern), the chart left whole beside it and its marks highlighted — or, at least, a popover anchored to
  the tile that may overflow it and scrolls within the window, so a small tile never clips it.

**Built 2026-10-25, the popover** (`ui/chart-panel.ts` `openForm`, `closeForm`, `#placeForm`; tested in
`test/cube-page-shell.test.ts` "a chart's right-click menu"). Options… opens the options or brings them forward;
they have a heading with ×, and Escape or a press anywhere else closes them. They float over the page beside their
chart: to its right if there is room, else to its left, else over it. They sit above tiles and windows and under
menus, as tall as the window allows, so a small tile never clips them. The pane docked at the page's edge is still to
come.

**Built 2026-10-25, the window** (the user: "a floating dialog like Properties and filters", opening "near the chart
but not covering, and not half off the screen"). It replaces the popover, and the docked pane and "Focus to edit" are
no longer planned. A chart's Properties… opens its options as a window with the grid windows' own frame: a title bar
it is dragged by, titled "Properties — *the chart's name*", a ×, and eight resize grips (`ui/chart-panel.ts`
`openForm`, `ui/window.ts` `makeWindow`). It floats where the page's grid windows float and shares their stack, so the
window touched last is on top.

- **Where it opens** (`ui/window.ts` `besideWindow`): to the chart's right where it fits, else its left, else below,
  else above. Where no side fits at its size, it is made smaller to fit the side with the most room. It covers the
  chart only when no side has room even for its minimum size, as with a chart filling the screen. It is placed beside
  the chart every time it opens, because the chart may have moved; the size a person left it at is kept.
- **Never off the screen:** it is fixed to the browser's window and kept wholly inside it, however it is dragged or
  resized, and when the browser's window shrinks (`viewport`).
- **How it closes:** its × or Escape closes it (not Escape typed in a field, as with the page's other windows). A
  press elsewhere leaves it open, as with any window, and Properties… again brings it forward. It also closes when its
  chart leaves the page: the chart removed, or its sheet hidden.

Tested in `test/window.test.ts` (placement), `test/window-dom.test.ts` (kept on the screen),
`test/cube-page-shell.test.ts` ("a chart's right-click menu") and, in a browser, `demo/verify-charts.mjs` (the KPI
check: the window neither covers its chart nor leaves the screen).

**Its fields in the Properties window's design, 2026-10-25** (the user: "nice and sleek like we upgraded Properties,
Filters, Join"). They were one long column of "Label:" and box-cornered dropdowns, with two plain headings.

- **Sections:** they now sit in the Properties window's own sections (`ui/chart-form.ts`): Data, More values, Small
  multiples, Layout, Reference lines, Labels and legend, and Look and interaction. Each has a small uppercase title
  with a caret that folds it, and the chart remembers which are folded. A section the chart's kind has nothing for is
  left out.
- **Shared styling:** the section titles, controls and focus ring use the Properties window's own CSS rules
  (`app.css`, shared, not copied). The fold helper is shared too: `ui/form.ts` `foldable`, which moved there from the
  column panel.
- **Kind and values as cards:** the chart's kind sits in a card at the top, like the Join window's two grids. Each
  value after the first is a card with a quiet × at its corner. "+ Add a value" is a sky link, like the Join window's
  "+ Add a condition".
- **Segmented choices:** each short choice is shown with all its options in view (`ui/chart-form.ts` `segmented`, a radio
  group to the keyboard and to screen readers): Orientation, Stack, Legend, Labels, Turn labels, Zoom slider, and a
  KPI's or gauge's "Good when". The chosen option is drawn like a chosen Join kind: sky fill, sky edge, bold text.

Tested in `test/chart-kinds.test.ts` ("a chart's options in the Properties window's design") and, in a browser,
`demo/verify-charts.mjs` (the section titles' type and the chosen option's sky colours).

**More of a square, 2026-10-25** (the user: "more of a square vs long thin scroll?"). A bar chart's options were about
2,100px tall in a 300px column: three or four windowfuls of scrolling, with Data and Layout never in sight together.

- **Two columns where there is room:** what the chart reads on the left (Data, More values, Small multiples), how it
  is drawn on the right (Layout, Reference lines, Labels and legend, Look and interaction), each column at least
  216px wide (`ui/chart-form.ts` `columns`, `app.css` `.dc-chartpanel-columns`). The columns are fixed, so folding a
  section never moves another across. The window opens about 588px wide beside its chart where that fits, and
  narrower where it does not, one column under the other below about 490px (`ui/chart-panel.ts` `CHART_WINDOW`,
  placed by `besideWindow`).
- **No taller than it needs:** once its form is painted, the window shrinks to fit it and is placed beside the chart
  again at that height (`ui/chart-panel.ts` `#fit`, through `ui/window.ts` `moveWindow`). This happens once, as it
  opens: a window that changed height with every option would jump under the pointer. It never overrides a size a
  person gave it.
- **The rarer sections start folded:** Small multiples, Reference lines, and Look and interaction (`ui/chart-form.ts`
  `FOLDED_TO_START`). One that holds a setting starts open, marked as changed. Once opened, the chart keeps it open.
- **Its kind as a header row:** "Chart" and its dropdown on one line in the card at the top. The section titles darken
  on hover and show a focus ring.

At 1400×900 and at 1024×768, a bar chart's options open in two columns beside the chart, about 590 by 720–740 and 500
by 720–740, with nothing to scroll. Tested in `test/chart-kinds.test.ts` (sections folded to start, kept, and opened by
a setting), `test/cube-page-shell.test.ts` (the window's size and place) and, in a browser, `demo/verify-charts.mjs`
(two columns at 500px or wider, and no empty space under the form).

**The independent review, 2026-10-25.** Two reviewers who wrote none of it read the chart window, its form and
Settings… under View ▸ (`c2fba9d27` to `b53f576c2`). They found no blocker. Everything they found is fixed, each fix
with a test that fails without it:

- **The window's focus:** opened from its chart's right-click, the menu closing put the focus back on the tile, so
  Escape no longer closed the window. A menu entry that moves the focus now keeps it there (`ui/menu-view.ts`).
- **Its size:** only a size given with a grip is kept for the next opening. A press on its title bar, or the
  browser's window shrinking, kept a squeezed size for good (`ui/window.ts` `WindowChange`).
- **Its fit:** a window dragged or resized before its form painted is no longer fitted over where it was put.
- **Its place:** beside the chart's whole tile, never over the tile's own title and buttons; kept off the page's
  scrollbars (`ui/window.ts` `screenOf`).
- **Raising:** a window already on top is not raised again. Every press raised it, and after about a thousand it
  rose over the menus.
- **The form:** a rarer section opens, marked as changed, only for a setting one of its fields shows, never for one
  it could not take off (smooth lines kept on bars, a band, the second value's target). The segmented choices take
  Home and End, show their words on a hover, and are ringed when reached by the keyboard, as are the checkboxes.
- **Settings…** in a lone grid's View ▸ is set apart from the view's own entries.

### 3. The page's ≡ menu, grouped by purpose

In the order of the menus' shared skeleton (below, "The menus"):

1. **New Page** (was Create ▸ Blank Page), **Open…**, **Save**, **Save As…**, **Share…**, **Export Page File…** (one
   entry until a second format arrives)
2. **Undo *step***, **Redo *step***
3. **Edit Layout** (**Done Editing Layout** while on), **Arrange…**
4. **Create ▸**: Data Source…, Sheet, Text Box, then Association…, Data Space…
5. **View ▸**: Hide the Bar (Show the Bar while hidden), Generated Pure & SQL…, Keyboard Shortcuts…, Settings… (the
   app's, not the first grid's; a grid's own window is its Properties…)

**Built 2026-10-25** (`page/page-app.ts` `#menuGroups`, tested in `test/page-app.test.ts` "the page's ≡, grouped by
purpose"). An entry that cannot apply is greyed with why on hover: Arrange… and Sheet "Edit Layout first", Settings… and
Keyboard Shortcuts… "Open a data source first", Export Page File… the page's save refusal. Two notes: Settings… still
opens through the first grid, but its window holds the app's settings (kept for every grid), so it is the app's in all
but where its code lives; and "Show the Bar" is never seen in the ≡, which folds away with the bar (the lip where it
was brings it back). No shortcuts are shown yet: the page binds no keys of its own (a grid's Ctrl+Z is the grid's).

### 4. The page's title bar

- **The page name as a title,** not a box: plain text, renamed by click, with "Unsaved changes" (or a •) beside it.
- **Tabs sized to their names,** truncated only when the bar runs out, the ▾ list for the rest.
- **The lone grid's source and state in its own status bar,** as every grid's — the page bar holds the page's things.
- **One pill style** for states (filled, quiet), never a dashed outline.

**Built 2026-10-25** (`app.css` `.dc-page-name`, `.dc-sheet-tab`; checked in `demo/verify-charts.mjs` over the gallery's
tabs). The page's name is its title in plain bold words, with a box only on hover to show it renames there. Each sheet
tab is as wide as its name, cut short only when the strip runs out of room; they were all 160px, and the gallery's
read "Bars and li…" with half the bar empty. The ▾ list of every sheet, the unsaved dot, the lone grid's state in its
tab and the state icons were in place already.

### 5. The grid's status bar, a readout a person reads

- **Left: what the grid shows** — "200,000 rows · 3 groups" (the leaf count the grid already queries), and the
  selection's Sum · Average · Count when cells are selected (Excel's status bar), as today.
- **Right: chips that say something** — "Filtered" (only while a filter is on; click to edit), "Snapped 2 min ago" or
  "Live", the query's time on hover, a warning icon for a truncated result or an error (click for the details).
- **The progress bar only while something runs:** a thin bar along the grid's top edge, not a box in the status bar.
- **Properties and Filter leave the bar** (both are in the right-click; the bar is a readout, not a toolbar) — or stay
  as icon buttons, not links.
- **Receipts and the planner's word behind one "ⓘ" chip** that opens the Receipts window.

**Built 2026-10-25** (`app.ts` `#renderStatusBar`, `#tool`, `#joinTool`, `#chip`, `timingText`; the icons in
`ui/tile-icons.ts` `toolIcon`; tested in `test/app-dom.test.ts` "the bar says what you are looking at"). The user chose
icon buttons for **Properties, Filter, Add Column and Join** at the left; Join is the host's (`joinMenu`) and is absent
where the host makes no joins. Join is greyed, with the host's reason as its tooltip, when nothing can be joined. That
is asked when the pointer or the focus reaches it, not on every paint, because the host builds its page's document to
answer. Then the readout (below) and the selection's figures. The right holds a chip only while it applies: the source with
Live/Snapped (a grid in a tile), Filtered (opens the filter), Top N (× clears it), the page's filter, a note, and the
truncation warning. Then the receipt as an ⓘ (its words hidden but still its name) and the host's word. Progress is a
2px bar on the grid's top edge while something runs. The readout keeps `data-rows` (rows on screen) and `data-view` (a
count of landed views) for scripts, because its text no longer changes with each query's time.

**The error chip, built 2026-10-25** (`app/datacube-app.ts` sayError, tested in `test/datacube-app-live.test.ts`). A page's
error is a red chip in the bar: ⚠ and its first words, the whole of it on hover. A click opens "What went wrong" with
the whole message and a button that shows the generated Pure and SQL. Its words stay the element's text, because the
scripts that check the status line for an error read it all.

Changed from the plan above, by the user's decision (2026-10-25: "just show stats/num rows of the last query"):
the readout is the last query's own figures, "3 rows × 20 cols in 109ms", the same line a host is told. It shows no
groups wording, no total and no summed count. The planner's word stays the host's own readout, because clicking it is
how a person picks where the planner runs.

### 6. The rest

- **Fix the example-switch bug** (found 1).
- **A grouped grid's text columns at group level:** show the count of values in muted text ("3 values"), or hide a
  `unique` column at group level until expanded — instead of a blank (found 2).
- **The columns panel:** pivot leaves folded under their measure by default (found 3).
- **One icon set,** inline SVG at one weight, for the tile head, the status bar and the bars (found 4).

### 7. The text box, a Markdown editor (agreed with the user, 2026-10-25)

The user chose GitHub's Markdown with a toolbar. **Built 2026-10-25** (`page-text.ts`, still a chunk fetched with the
first text tile; tested in `test/page-text.test.ts` and `test/page-text-dom.test.ts`).

- **What it reads:** every heading level, including underlined ones; lists nested by indent and task lists; quotes;
  fenced code, kept exactly as typed; tables with column alignment; rules; bold, italic, both, strikethrough, code,
  links (in brackets, in angle brackets, or bare). A `_` inside a word stays part of the word, so `trade_id` is not
  italic.
- **What it never does:** run HTML or fetch an image. Typed markup shows as text, and an image is its words linked to
  it. Links are only http(s) and mailto, as before.
- **The editor:** a toolbar (Bold, Italic, Strikethrough, Inline code, Heading, the three lists, Quote, Link, Code
  block, Table, Horizontal rule) that writes the marks around the selection, with Ctrl+B, Ctrl+I and Ctrl+K as
  shortcuts. The words and a live preview sit side by side, one above the other when the tile is narrow. OK, Cancel,
  Escape and Ctrl+Enter work as before, and a link to GitHub's syntax guide sits in its bar. The toolbar's icons are
  drawn as the tiles' are, but kept in the editor's chunk.
- **Exports** write each block as lines: list marks with a task's `[x]`, nested items indented, code line by line,
  quotes after `>`, table rows with cells between `|`, and `---` for a rule (`cube-page.ts textLines`).

### 8. Files in the text box (the user, 2026-10-25: the end of the roadmap)

Now that the text box reads and edits Markdown, it should also open files. Open or drag-and-drop one file or several
onto it and preview them in the tile.

| Order | Format | How |
|---|---|---|
| 1 | Markdown, plain text, CSV, JSON | `page-text.ts`, as today |
| 2 | PDF | Mozilla's pdf.js, vendored through Bazel as the other dependencies are, in a chunk fetched with the first PDF |
| 3 | Images, audio, video | The browser's own `<img>`, `<audio>` and `<video>` over the file's bytes; no library |
| 4 | Word `.docx` | Converted to the text box's own blocks; the document's HTML is never run |
| 5 | Email `.eml`, Outlook `.msg` | The mail parsed; sender, subject and body shown as text; attachments listed; no remote image ever fetched (a tracking pixel) |
| 6 | PowerPoint `.pptx` | The weakest in a browser: each slide's text and images, not a faithful rendering, which may need a server |

Several files make a list in the tile (tabs or a side list), one shown at a time.

**Decided: a file is pointed to, never copied into the page** (the user, 2026-10-25: "pointing to file if we can that
would be ideal"). A page keeps each file's name, size, format and fingerprint (its SHA-256), never its bytes, the way a
grid over a local file is kept today (`file-handles.ts`):

- **Chrome and Edge:** opening or dropping a file gives the page a handle to it, kept in this browser (IndexedDB, by
  the page and the fingerprint), never in the page's document or anything shared. Reopened, the file is read as it is
  now; after a reload the browser asks once, a click with no file dialog.
- **Firefox and Safari, a moved or deleted file, or a page opened on another machine:** the text box says the file is
  missing and offers to pick it again; the fingerprint says whether what was picked is the same file.
- **Python:** a text box's files are read and written through the page's one command (`setTextFiles`), Python
  fingerprinting a file it is given by path; the browser tab then finds it as above.

**Step 1 (built 2026-10-25):** Markdown, plain text, CSV and JSON (`text-files.ts`, the one rule; the command
`setTextFiles`; `page-text.ts` `fileElement`; `cube-page.ts`; Python's `Text.files`). Open Files… on the text box's
right-click, or a drop onto it; one tab a file beside the box's own words; × takes a file off; Markdown drawn as the box
draws its own words, text and JSON as they are (JSON indented), CSV as a table of its first 200 rows and 100 columns,
the rest counted. Up to 12 files a box, each up to 64 MB (it is read whole once, to fingerprint it); the first 2 MB of
one are drawn, and that is said. A file of a kind a box does not show, or too big, is refused by its name and size
before it is read.

The branch's audit (2026-10-25) found and fixed: a tab's × and Replace matched files by object, so a step that drew
equal files again left the × doing nothing; a file read through a kept handle, or picked again after it went missing,
had its handle kept nowhere (Save As now takes the handles the page it was saved as kept, and a file picked again is
kept at once); a file changed since it was added was said to have changed only the first time; a file that could not
be read (a folder dropped, a file changed on disk) failed silently; a double-click in a preview opened the editor over
it; an export wrote the box's Markdown source while a file's tab was in front; a JSON file over 2 MB was called no JSON;
and the Markdown reader took time in the square of a line's length (a 120 KB line, 13 s) and overflowed on deep
nesting. Left as designed: two editors adding files at once each send the whole list, so one list wins, as with
`setText`.

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

### Built so far (step 1, 2026-10-25)

The marks and words are now the same in every menu:

- **"…" is one character everywhere.** Every menu entry that opens a window or asks for more ends with "…", never
  "...". A guard in `test/guardrails.test.ts` fails on a menu label written with three dots.
- **Keys are shown right-aligned, only where the key does what the entry does:** Undo Ctrl+Z, Redo Ctrl+Shift+Z,
  Copy Selected Cells Ctrl+C, Properties… Ctrl+E. A screen reader is told the key too (`aria-keyshortcuts`). The
  page's ≡ shows none: its Undo has no key of its own, and there is no Ctrl+S.
- **A chart's menu** is Show Grid; Properties… (once "Options..."); then View ▸ with Maximise / Restore, Move to
  Sheet ▸ and Remove from Page (once a bare "Remove").
- **A text box has a right-click:** Create ▸, Edit Text…, Open Files… (§8), then the same View ▸. While its words are
  being edited, its editor keeps the browser's own menu (copy, paste). On a locked page, View ▸ Remove from Page is
  offered off, as a tile's × is hidden there (the audit, 2026-10-25) -- a grid's and a chart's too.
- **On/off entries say what they will do and flip,** with no check mark: Edit Layout / Done Editing Layout, Measures
  First / Values First in Column Headers, Add / Remove Heatmap, Add / Remove Data Bars.
- **A sheet tab's menu** says Rename… and Delete Sheet. Each disabled entry there says why on hover ("Edit Layout
  first", "It is the first sheet", "A page keeps one sheet at least"), as does a locked Move to Sheet.

**Step 2, 2026-10-25:** a chart's and a text box's Create ▸, in the grid's own words (`ui/menu.ts` createItems). On a
chart, Copy of This Grid and Visualization act on the grid the chart reads; on a text box, or on a chart whose grid is
gone, they are offered disabled and say why. Data Source… and Text Box are placed beside the tile.

"Settings…" stays in the page's ≡, not in a grid's Properties: its four settings (Debug Mode, Reload, Max History Stack
Size, Row Buffer) are the app's, apply to every grid on the page at once and are kept by the host for this browser, so a
tab in one grid's Properties would mislead. Only the way it opens goes through the first grid. **Moved under View ▸,
2026-10-25** (the user), beside Keyboard Shortcuts…, in the page's ≡ and in a lone grid's title-bar ≡ alike
(`page/page-app.ts`, `app.ts`; tested in `test/page-app.test.ts` and `test/shell.test.ts`).

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
View ▸ (… Keyboard Shortcuts…, Settings…) Create ▸
                                          Properties…                 Ctrl+E
                                          View ▸
```

## Suggested order

| Wave | What | Why first |
|---|---|---|
| 1 | The example-switch bug; the chart options pane closes (×, Escape, click outside) and never clips; the status bar's idle box, labels and error chip; the tile's always-visible chrome cut to the title and what is unusual | Each is small, each is felt on every page |
| 2 | View mode vs Edit Layout; the menus' design language (the skeleton, words, marks and rules above) for every menu; the title bar (name, tabs, the lone grid's state) | Structural, touches every tile and every menu |
| 3 | The options pane docked at the page's edge; grouped text columns; the columns panel; one icon set | Larger design work |
| 4 | Files in the text box (§8): Markdown and text first, then PDF, media, Word, email, PowerPoint | Added by the user, 2026-10-25 |

**Where it stands, 2026-10-25.** Built: numbers right-aligned; the page's ≡ grouped by purpose (§3); one tile header
(§1a); the grid's status bar (§5); the text box as a Markdown editor (§7). Built since: the chart options as a popover that closes (§2); the example-switch bug fixed (found 1); the status bar's error as a chip; the title bar (§4); the columns panel (found 3); the menus' marks and words (step 1
of the menus); one icon set for the page's chrome (found 4). Grouped text columns (found 2) kept as they
are (the user). Built since: Create ▸ on a chart's and a text box's right-click; the tree carets drawn; files in the
text box, step 1 (§8); then the branch's deep audit and its fixes (above, and the notes in each section). Then the
chart options as a window beside their chart (§2), and Settings… under View ▸ (§3), with the independent review's
fixes (§2). Decided, nothing to build: a
group's row count shows only once the group is open ("AMER (5)", "500+" past the most rows), never on a closed group
(the user, 2026-10-25). Known and left: a join being opened when the page Python serves is left keeps the
table it borrowed from a grid until the tab closes (rare; no test reaches it cheaply).

Screenshots of each state (the start page, the chart examples and the gallery's KPI dashboard, the chart options at
1400 and 1024 wide, the page menu, the joins example's error, the status bar) were taken with a scratch Playwright
script over the built site (`bazel build //datacube:shots`, then the runfiles' `demo/harness.mjs` serve).
