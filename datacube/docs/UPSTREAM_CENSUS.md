# Upstream DataCube: the full census — 2026-09-25

Every feature of real Legend DataCube, read file by file, and what ours
has for each. This replaces the sampled first pass in
`FEATURE_CENSUS.md` as the list of what is missing. That document keeps
the fix history.

## Method

- **Spec:** legend-studio `c5b2f2c78`. Every source file was read in
  full, with nothing skipped, in:
  - `legend-data-cube` (141 files);
  - `legend-application-data-cube` (57 files);
  - `-bootstrap` and `-deployment`.

  Every DataCube reference was also traced into the eleven other
  packages that touch it: `legend-graph`, `legend-application`,
  `legend-application-query`, `legend-query-builder`,
  `legend-application-studio`, `legend-application-repl`,
  `legend-extension-dsl-data-product`, `legend-extension-dsl-data-space`,
  `legend-application-marketplace`, `legend-lego` and `legend-art`. The
  server side of save/load was read in legend-engine 4.145.0
  (`ApplicationQuery.java`).
- **Ours:** legend-lite `main` + local `4093e73fc`, `dbefb4a8d`,
  `datacube/src`. Each claim below cites the file, and a line where it
  matters.
- **Evidence:**
  - **C** = read in code at the cited place;
  - **H** = passes in `verify_features` (137/137);
  - **C!** = read in code and should be confirmed in the browser before
    a fix is claimed.
- **Status:**
  - ✅ = matches;
  - ⚠️ = partial or different;
  - ❌ = missing;
  - ➕ = ours only;
  - 🚫 = an upstream placeholder (greyed out, marked "WIP"), so not a gap.

## Headline: what we are missing

Grouped by size of consequence, not by file.

**1. Save/load is not the upstream feature at all.** This is the
biggest gap, and it is structural.

- Upstream saves a `PersistentDataCube` whose `content` is a
  `DataCubeSpecification`: the query as **Pure text**, the whole
  **configuration**, and a typed **source**. It lives in the engine
  query store, under `/pure/v1/query/dataCube`.
- Ours writes one `localStorage` slot in our own `SavedView` format. It
  keeps the snapshot, the open paths, and the column order and number
  formats. `loadView` restores neither the order nor the formats
  (`app.ts:1968-1995`). No other part of the configuration is saved at
  all: titles, colours, fonts, pins, widths, hidden columns, display
  names, links, heatmaps, grid lines and highlight rows are all lost.
  **C!**
- Our "DataCube Specification" export writes that same `SavedView`
  JSON, not upstream's shape (`app.ts:1922`).
- We cannot open an upstream spec either, because we have no Pure-text
  → cube reader (upstream `DataCubeSnapshotBuilder`, G12).

**2. The HTTP surface.** Upstream DataCube talks to exactly 18 engine
endpoints (§J). legend-lite serves none of them under their real paths.
This is already the planned API program (`ENGINE_API_CONTRACT.md`).

**3. Only one window at a time.**
- Upstream DataCube is a desktop of floating, non-modal windows: the
  Filter editor, the Properties editor, any number of column editors,
  Settings, Documentation and alerts, all open together.
- Ours has ONE overlay, and opening anything replaces it
  (`app.ts:2159`, `#showOverlay`). You cannot, for example, keep the
  filter open while changing a column's format.

**4. The filter editor is a draft of upstream's, not a match.** Five
differences, each seen in `filter-editor.ts`:
1. It applies live on every change; there is no Cancel / Apply / OK.
2. `+` inserts a blank default condition instead of a clone.
3. The group button wraps a single node in an AND group, instead of
   making an OR of the node and its clone.
4. The operator list is not filtered by the column's type.
5. The value is a single text box. Upstream has typed editors instead:
   - a number field that evaluates arithmetic;
   - a date picker with Date / Date Time / Today / Now modes;
   - a boolean checkbox;
   - a list popover with typed items;
   - a same-type column picker.

**5. The grid lacks what users do without thinking.** None of these is
in ours:
- clicking a header to sort, with sort arrows and multi-sort index;
- dragging a column edge to resize;
- cell and header tooltips (`Value = …`, `Column = …`,
  `Group Value = … (n)`, the pivot `[ key = value ]` tooltips);
- the scroll position readout (`start-end/total`);
- the "Loading…" and "0 rows" overlays.

**6. Settings, alerts and debugging.** None of these exists in ours:
- the Settings window (debug mode, history size, row buffer, warnings,
  retry failed fetches, reload, and the host's own settings);
- typed alert windows with action buttons;
- the execution-error alert with its debug info (query code, execute
  input, *Download Execute Input*);
- the code-check alert that shows the query with the error range marked.

Ours reports everything on the status line.

**7. Properties → Apply can wedge the cube.** `#applyDraft` calls
`#refreshOr(null)` (`app.ts:2079`), so a draft the planner refuses stays
as the current snapshot, and every later query repeats the refusal.
Upstream compiles the whole query before publishing it ("Query
Validation Failure: Can't safely apply changes"). **C!**

**8. Everything the other apps do to open a cube.** None of these
exists in ours:
- `/:dataCubeId` and `?sourceData=` URLs;
- the sources: Legend Query, UDF, Freeform TDS and Lakehouse;
- launching from Legend Query, the Query Builder, Studio, the REPL,
  data products, the marketplace and the AI chat.

The rest are smaller parity rows, listed in full below.

**Two earlier claims corrected by this pass:**
- **Upstream has NO pivot total column, and that is an upstream BUG
  (user ruling 2026-09-25), not a spec to match.** The configuration
  carries it:
  - `pivotStatisticColumnName` (default "Total");
  - `pivotStatisticColumnPlacement` (left/right; unset = hidden);
  - a per-measure `pivotStatisticColumnFunction` (default SUM, from the
    client-side operator set: sum, avg, median, count, min, max, var(_s),
    std(_s)).

  No UI sets the placement, and no query or grid code renders it. Ours
  must SHIP it: keep the three fields byte-compatible, and render the
  column. It is a gap row below (§B), not a non-feature.
  `FEATURE_CENSUS.md` §2 removed our fields in local commit `4093e73fc`;
  they come back with the feature.
- **Upstream's Email needs no host.** It downloads a `.eml` draft
  (`X-Unsent: 1`) with the CSV or Excel file attached. Ours needs a host
  `email` callback, so in the demo it is disabled. Upstream's way works
  in any browser.

---

## A. Grid menu (right-click and header menu)

Upstream: `DataCubeGridMenuBuilder.tsx`. Ours: `src/ui/menu.ts`,
`app.ts #wireContextMenu / #onMenuAction`.

| Entry / behaviour | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Same menu from a header (the header's own menu, `getMainMenuItems`) | ✓ | right-click anywhere in the grid, header included | ✅ | C |
| **Every Export entry asks first**: "Confirm you want to proceed with export" + attestation text, Decline / Accept | ✓ | no confirmation | ❌ | C `menu.ts:365` |
| Export › Excel (Grid) / CSV (Grid) | ✓ `.xlsx`, CSV of loaded rows | `.xls` SpreadsheetML, CSV of the view | ✅ | H |
| Export › **CSV** (full result, engine `execute?serializationFormat=CSV`, streamed) | ✓ | ❌ only the rows on screen | ❌ | C |
| Export › HTML / Plain Text / PDF / DataCube Specification | 🚫 | ➕ (the specification is OUR format, see headline 1) | ➕⚠️ | H |
| Export file name `"<title> - EEE MMM dd yyyy HH_mm_ss.<ext>"` | ✓ | `<title>.<ext>` | ⚠️ | C `app.ts:1879` |
| Email › Excel / CSV attachment (downloads `.eml`, no host needed) | ✓ | needs a host `email` callback; disabled in the demo | ⚠️ | C `app.ts:173`, `menu.ts:385` |
| Email › HTML / Plain Text / PDF / Specification | 🚫 | ➕ (same gate) | ➕ | C |
| Copy › Plain Text / Selected Rows / Selected Column | ✓ (Rows disabled from a header) | ✓ | ✅ | H |
| Sort › Ascending, Descending, Clear, Add Asc/Desc (disabled if already), Clear All | ✓ | ✓ (Add not disabled when already sorted that way) | ✅ | H |
| Filter › "Add Filter: col = 'v'" (strings QUOTED), More Filters on col… (typed operator set), is null / is not null on a blank, Filters…, Clear All | ✓ | ✓ unquoted strings | ✅ | H |
| Filter on the TREE column filters that level's group column | ✓ | ✓ (by path) | ✅ | C `app.ts:1266` |
| Pivot › Vertical / Add / Remove / Clear All; Horizontal / Add / Clear All | ✓ (items only for dimension columns) | ✓ (shown disabled for measures) + ➕ Remove Horizontal | ✅ | H |
| Pivot › Exclude Column X from / Include Column X in Horizontal Pivot | ✓ | ✓ | ✅ | H |
| Extended Columns › Add New / Extend Column X / Edit / Delete | ✓ | ✓ | ✅ | H |
| Resize › Auto-size, Minimize, Auto-size All, Minimize All, Size Grid to Fit | ✓ | ✓ | ✅ | H |
| Pin › Pin Left / Right (each **checked** when active, disabled if already) / Unpin / Remove All | ✓ | ✓ no checkmark, not disabled | ⚠️ | C `menu.ts:592` |
| Hide | ✓ | ✓ | ✅ | H |
| Collapse All (tree column only) | ✓ | ✓ (everywhere; disabled when nothing is open) | ✅ | H |
| Heatmap / Show Plot… / Show TreeMap… | 🚫 | ➕ working | ➕ | H |
| **Properties…** from a header opens **Column Properties on that column** (pivot result → its measure); disabled while the editor is open | ✓ | opens the editor on its last tab | ⚠️ | C `app.ts:1371` |
| Menu hidden while scrolling | ✓ | — | ❌ | C |
| Multidimensional menu: Zoom Out, Export, Email, Resize | ✓ WIP | — (Essbase mode, `FEATURE_CENSUS.md` §10) | ❌ | C |

## B. Grid rendering and interaction

Upstream: `DataCubeGridConfigurationBuilder.tsx`, `DataCubeGrid.tsx`,
`DataCubeGridClientEngine.ts`, `DataCubeGridControllerState.ts`. Ours:
`src/grid/*`, `treeview.ts`, `format.ts`, `style.ts`.

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| **Click a header to sort**; multi-sort always on; sort arrow + index in the header | ✓ | ❌ (sort only from the menu / editor) | ❌ | C `grid.ts` has no header click handler |
| **Drag a column edge to resize** (unless fixed width) | ✓ | ❌ (width from the editor / Resize menu) | ❌ | C no resize handle in `grid/` |
| Drag a header to reorder, persisted | ✓ | ✓ | ✅ | H |
| Drag to the row-group panel ("drag here to group") | ✓ ag-grid panel always shown | ✓ drag zones (can be folded) | ✅ | H |
| Side bar "Columns" tool panel: show/hide, drag to groups, drag into the grid | ✓ | ✓ `ColumnsToolPanel` | ✅ | H |
| Cell tooltip `Value = …` / `Missing Value`; header `Column = Display (name)`; tree `Group Value = v (count)`; pivot headers `[ Pivot = v ]`, `Column = X ~ [ … ]`; 1.5 s delay | ✓ | ❌ | ❌ | C no titles in `grid/` |
| Scroll readout `start-end/total` above the grid while scrolling | ✓ | ❌ | ❌ | C |
| "0 rows" and "Loading..." overlays; loading row; `#ERR` row on a failed fetch + error colours | ✓ | status line only | ⚠️ | C |
| Cell range selection, copy (Ctrl+C) | ✓ | ✓ + keyboard, ARIA treegrid ➕ | ✅ | H |
| Column selection by clicking a header (to copy a whole column) | ✓ | via the menu entry only | ⚠️ | C |
| Tree column: pinned left, header blank, min 200, leaf count "(n)" when *Show leaf count* | ✓ | ✓ | ✅ | H |
| Grand total (root aggregation) row, auto-expanded | ✓ | ✓ | ✅ | H |
| Initial expand level; expanded paths SAVED and restored; user expand/collapse recorded | ✓ | ✓ live; ⚠️ not saved in a view beyond `expanded` | ⚠️ | C |
| Lazy drill-down: each expand fetches only the next level, filtered to the group | ✓ | ✓ per level | ✅ | C `tree.ts` |
| **Max depth → detail rows** (groupBy dropped at the leaf level) | ✓ | ❌ (known gap) | ❌ | H |
| Pivot: nested header groups, 5-colour rotation, leaves in configuration order, values ordered client-side by each key's direction | ✓ | ✓ | ✅ | H, C `grid.css:704` |
| Pivot result columns cannot be pinned or moved | ✓ | not enforced | ⚠️ | C |
| **Pivot total column** per measure (name, left/right placement, per-measure function), per row including subtotals and the grand total | ❌ upstream BUG (config only) | ❌ fields removed in `4093e73fc` | ❌ | C |
| Number format: scale (%, bp, k, m, b, t, auto) → grouping/decimals → parens → scale unit | ✓ | ✓ | ✅ | C `format.ts` |
| **Unit**: glued to the value, or a **prefix when it starts with `_`** (`_$` → `$1,234`) | ✓ | always a suffix, with a space (`1,234 _$`) | ❌ | C `format.ts:294` |
| Missing-value text on number AND text columns | ✓ | ✓ (`nullText`) | ✅ | C `format.ts:254` |
| Zero / negative colours only on number cells; error colours on failed rows | ✓ | ✓ zero/negative; no error rows | ⚠️ | C |
| Font case on text columns only | ✓ | ✓ | ✅ | C |
| Blur, unblurred on hover | ✓ | ✓ | ✅ | C `grid.css` |
| Link cells: http(s) URL → link, label from a URL parameter | ✓ | ✓ | ✅ | H |
| Alternate rows: STANDARD (native odd-row stripe) vs CUSTOM (colour every N rows), mutually exclusive | ✓ | one checkbox plus colour and N | ⚠️ | C `panel-general.ts:333` |
| Pagination toggle (slice per page of 500), status-bar switch | ✓ default ON | ❌ (virtual windowing + row limit) | ⚠️ | C |
| Large-dataset warning with pagination off (> 1000 rows: Enable Pagination / Dismiss) | ✓ | ❌ | ❌ | C |
| Auto-size all columns after every fetch | ✓ | explicit only | ⚠️ | C |
| Refetch only when a data-affecting part changes (styling and reorder never refetch) | ✓ | every configuration change goes through `#refresh` | ⚠️ | C `app.ts:1555` |
| Cache toggle (whole source into the in-browser DuckDB, with a warning) | ✓ WIP | ➕ snap mode | ✅ | C `snap.ts` |
| Drill-through (the rows behind a number, on double-click) | — | ➕ | ➕ | H |

## C. Filter editor

Upstream: `DataCubeFilterEditor.tsx`, `DataCubeFilterEditorState.tsx`,
`DataCubeFilterEditorUtils.ts`. Ours: `src/ui/filter-editor.ts`.

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Non-modal window "Filter" (750×400) beside other windows | ✓ | single overlay | ⚠️ | C |
| **Cancel / Apply / OK** — edits batched, published once, only if changed | ✓ | ❌ applies on every change | ❌ | C `filter-editor.ts:478-482` |
| Empty state: "No filter is specified…" + Create New Filter → root AND + one default condition | ✓ | ✓ | ✅ | C `:546` |
| Tree rendering: gutters, indentation, and/or words, selected node/group highlight | ✓ | ✓ | ✅ | C |
| Group row "All of" / "Any of" | ✓ | ✓ | ✅ | C `:626` |
| **`+` inserts a CLONE** of the condition (the match is unchanged); on a group, a default condition | ✓ | inserts a default condition on the FIRST column | ❌ | C `:515-521` |
| **Remove flattens** a group left with one child into its parent | ✓ | leaves single-child groups | ⚠️ | C `:258` |
| **Layer** = an **OR** group of the node + its clone | ✓ | an AND group holding the node alone | ⚠️ | C `:327` |
| NOT toggle + NOT tag | ✓ | ✓ | ✅ | C |
| Column dropdown; switching column keeps a compatible operator, else the first compatible, and resets the value | ✓ | keeps the operator whatever the type | ❌ | C `:644` |
| **Operator list filtered by column type** | ✓ | all 31 for every column | ❌ | C `:647-653` |
| Number value accepts ARITHMETIC (`1e6*3` → Enter), ↑/↓ step, `#ERR` | ✓ | text box | ❌ | C `:668` |
| Date value: mode dropdown Date / Date Time / Today / Now + a native date or datetime (seconds) picker | ✓ | text, `today()` / `now()` typed by hand | ⚠️ | C |
| Boolean value: checkbox | ✓ | text | ❌ | C |
| List value (in / not in): popover of typed items, add / remove, "a, b (+N)" summary | ✓ | comma-separated text | ⚠️ | C |
| Column-compare value: dropdown of SAME-TYPE columns | ✓ | every column | ⚠️ | C `:659` |
| Calculated columns filterable (row stage), not group stage | ✓ | ✓ | ✅ | C `app.ts:2049` |
| Case-insensitive in / not in | ✗ (declared, not built) | ➕ | ➕ | C |

## D. Calculated (extended) columns

Upstream: `DataCubeExtendManagerState.tsx`, `DataCubeColumnEditorState.tsx`,
`DataCubeColumnEditor.tsx`, `DataCubeCodeEditor.tsx`. Ours:
`src/ui/calc-editor.ts`, `calc.ts`.

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Leaf level (measure / dimension) and group level | ✓ | ✓ row and group stages + kind | ✅ | H |
| Entry points: Extended Columns in the menu | ✓ | ✓ | ✅ | H |
| **One window per column, several open at once**; Edit on a column already open focuses its window | ✓ | one window listing every calculated column | ⚠️ | C `app.ts:2027` |
| Name defaults to `col_{N+1}`; a live ✓/✗ for uniqueness | ✓ | ✓ default; checked on save | ⚠️ | C |
| **Value Type** (Text / Number / Date) declared; OK disabled unless the compiled type matches | ✓ | ❌ (the type is learned after running) | ❌ | C |
| **Live compile** (500 ms debounce) with **error markers in the editor** (offset for the hidden prefix) | ✓ | validated on Save; message shown in the form | ⚠️ | C, `calc-editor.test.ts` |
| Pure syntax highlighting (Monaco) | ✓ | plain textarea | ⚠️ | C |
| Engine autocomplete | ✓ | ➕ our own in-scope completion list (engine completion skipped by ruling) | ➕ | C |
| Reset (for an existing column) | ✓ | ❌ | ❌ | C |
| Delete from the editor | ✓ | ✓ Remove | ✅ | C |
| Update / delete check the WHOLE query first; refused → nothing changes | ✓ | ✓ run, and revert on refusal | ✅ | C `app.ts:1219` |
| Rename carries through the cube | ✓ | ✓ | ✅ | H |
| Open editors recompile when the cube changes | ✓ | — (one editor) | ⚠️ | C |
| Window (`over()`) and reducing extends accepted from a query | ✓ | ❌ | ❌ | C |

## E. Properties editor

Upstream: `DataCubeEditor*.tsx` + states. Ours: `src/ui/editor.ts`,
`panel-*.ts`, `columns-selector.ts`.

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Tabs: Columns, Horizontal Pivots, Vertical Pivots, Dimensions, Sorts, General Properties, Column Properties; Cancel / Apply / OK | ✓ | ✓ | ✅ | H |
| **Apply compiles the whole query first**; failure = code-check alert, nothing applied | ✓ | runs it; a refusal leaves the refused draft in place | ❌ | C! `app.ts:2079` |
| Two-pane selector: search on both sides with count, multi-select, drag (multi-row), reorder, double-click, add/remove all | ✓ | ✓ | ✅ | H |
| Columns tab: Show hidden columns?, hidden greyed, Extended badges | ✓ | ✓ | ✅ | C |
| Horizontal Pivots: sort direction per key | ✓ | ✓ | ✅ | H |
| Sorts: direction per column; pivot names shown `a / b / col` | ✓ | ✓ | ✅ | C |
| **General** › Report Title; root aggregation; leaf count; tree Sort; expand-to level | ✓ | ✓ | ✅ | H |
| General › Row Limit + truncation warning | ✓ | ✓ | ✅ | H |
| General › Show Selection Stats | 🚫 disabled | ➕ working | ➕ | H |
| General › Grid lines H/V + colour | ✓ | ✓ | ✅ | H |
| General › Highlight rows: Standard XOR Custom (colour every N) | ✓ | not exclusive (see B) | ⚠️ | C |
| **Font family list**: Arial, Roboto, Roboto Condensed \| Georgia, Roboto Serif, Times New Roman \| JetBrains Mono, Roboto Mono, Ubuntu Mono | ✓ | Arial, Roboto, Helvetica, Verdana, Tahoma, Georgia, Times New Roman, Courier New, Monospace | ⚠️ (a saved spec's fonts won't match) | C `panel-general.ts:57` |
| **Font size list** 4…72 (21 sizes) | ✓ | number field 6–48 | ⚠️ | C `:128` |
| **Underline is a VARIANT** (solid, dashed, dotted, double, wavy); underline XOR strikethrough | ✓ | boolean underline; not exclusive | ⚠️ | C `:137` |
| Case (toggle + caret), alignment segmented buttons | ✓ | dropdown / toggle group | ✅ | C |
| Default colours 4×2; Use Default Styling (disabled when already default) | ✓ | ✓ (always enabled) | ✅ | H |
| ➕ Keep grouped columns in the grid; Show drag zones / title bar | — | ➕ | ➕ | C |
| **Column Properties** › choose column (A–Z; type badge; Extended badges) | ✓ | ✓ (`Derived` badge) | ✅ | C |
| Column › Show advanced settings? → Column Kind (ADV) | ✓ | ✓ kind always shown | ✅ | C |
| Column › **kind locked while the column is a vertical or horizontal pivot** (tooltip) | ✓ | ❌ | ❌ | C `panel-column.ts:231` |
| Column › switching kind resets exclusion from the pivot | ✓ | ❌ | ❌ | C |
| Column › Display name; Aggregation (**only ops compatible with the type**); Exclude from horizontal pivot | ✓ | ✓ ops not filtered by type | ⚠️ | C `:258` |
| Column › aggregation parameters (join-strings delimiter) | ✗ not editable (TODO) | ➕ weight for weighted average | ➕ | C |
| Column › number section only for NUMBER columns; link section only for TEXT | ✓ | shown for every column | ⚠️ | C |
| Column › Decimals, commas, parens, Scale, Unit, Missing Value Format, Blur, Hide, Pin, Width (Any / Fixed / Range), fonts, colours, Use Default Styling | ✓ | ✓ | ✅ | H |
| Column › per-column Pivot sort direction | via the H-Pivots tab | ➕ also here | ➕ | C |
| Column › Heatmap | — | ➕ | ➕ | C |
| **Default column config**: numeric = measure, decimals 0 (Integer) / 2, commas ON, **negative in parens ON**, right-aligned; others = dimension, unique, excluded from the pivot | ✓ | measure/dimension ✓; parens OFF by default | ⚠️ | C `format.ts:84` |
| Dimensions tab (+ "Enable multidimensional grid mode", experimental notice, convert vertical pivots into a dimension) | ✓ WIP | named dimensions as presets; no grid mode | ⚠️ | C `panel-dimensions.ts` |

## F. Query semantics

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Pipeline: extend → filter → select → [sort → pivot → cast] → [groupBy → sort] → extend → sort → limit | ✓ | ✓ | ✅ | C `serialize.ts` |
| Filter ops: = != < <= > >= in, not in, null / not null, contains / starts / ends (± not, ± case-insensitive), `*_value in column` | ✓ | ✓ same set (+ ➕ ci in/not in) | ✅ | C |
| TODAY only for date columns, NOW only for datetime columns | ✓ | both offered for any date | ⚠️ | C |
| Aggregates: sum, avg, min, max, var / var-s, std / std-s, count (numeric only), unique, joinStrings | ✓ | ✓ + ➕ median, weighted avg | ✅ | C `panel-column.ts:65` |
| Aggregates: first, last | ✓ | ❌ (planner: first → ANY_VALUE, no last) | ❌ | C |
| **Carried columns after a pivot use their OWN aggregate** | ✓ | hard-coded `unique` | ❌ | C `serialize.ts:895` |
| Filler `count()` when a groupBy has no aggregates | ✓ | grand-total fix handles the case | ✅ | H |
| Row limit applies at every level | ✓ | ✓ | ✅ | C |
| Pagination via `slice(start, end)` | ✓ | ❌ | ❌ | C |
| **Open a cube FROM Pure query text** (validate the composition, rebuild or validate the configuration, clear errors) | ✓ `DataCubeSnapshotBuilder` | ❌ — no reader | ❌ | C |
| Pure-text view of the cube's query (partial code for View/Edit Query, code-check alerts) | ✓ | ➕ host menu *Generated Pure & SQL…* | ✅ | C |

## G. Shell and services

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Title bar: title, host header slot, hamburger (Undo, Redo, Settings…, then host items) | ✓ | ✓ Undo, Redo, Properties, Zones, Title Bar, Save/Load View, dimensions, host items | ✅ | H |
| Status bar: Properties, Filter (disabled in multidimensional mode), task progress with per-task tooltip, rows, truncation warning, Pagination / Cache switches | ✓ | ✓ Properties, Filter (on-state ➕), rows + timing ➕, truncation, selection stats; ❌ task progress, Pagination | ⚠️ | C `app.ts:898` |
| **Floating windows**: drag, 8-edge resize, bring to front, remember place, **many at once** | ✓ | one at a time (drag, resize, remember ✓) | ⚠️ | C `app.ts:2159` |
| **Settings window**: Debug mode, dev protocol version, Reload, cache warnings ×2, **Max history size**, large-dataset warning, **Row buffer**, **Refresh group node data**, **Retry failed data fetches**; Restore defaults; host-supplied settings and persistence | ✓ | ❌ | ❌ | C |
| **Alerts**: typed windows (error / info / success / warning), action buttons | ✓ | status line | ❌ | C |
| **Execution-error alert**: Show debug info → query code + execute input; Download Execute Input | ✓ | status line text | ❌ | C |
| **Code-check alert**: the query, with the error range marked | ✓ | ❌ | ❌ | C |
| Documentation panel + (?) hints (row limit, kind, unit, missing value, link, extended levels) | ✓ | ❌ | ❌ | C |
| Undo / redo with max history; patch changes grouped | ✓ | ✓ + keyboard ➕ | ✅ | H |
| Colour picker: hex + ALPHA, palette of 18×11, transparent swatch, Reset / Cancel / OK | ✓ | native picker + clear | ⚠️ | C `form.ts:199` |
| Number input: ↑/↓ step, Esc selects all, invalid reverts | ✓ | partial | ⚠️ | C |
| Debug mode: log every snapshot diff, query, SQL, timings | ✓ | ❌ | ❌ | C |
| Telemetry events | ✓ | ❌ (not needed) | — | — |
| Embedding options: layout / task managers, host header and menu, settings + onSettingsChanged, onNameChanged, grid licence | ✓ | ✓ `hostMenu`, `hostStatus`, `storage`, `email`, `download`, `dimensions`, `configuration` | ⚠️ | C `app.ts:117` |

## H. The application: save, load, delete, routes

Upstream: `legend-application-data-cube` (A1–A4). Ours: the demo host
(`demo/boot.ts`) + `app.ts` saved views.

| Feature | Upstream | Ours | St | Ev |
|---|---|---|---|---|
| Route `/:dataCubeId?` (deep link to a saved cube) | ✓ | ❌ | ❌ | C |
| `?sourceData=<url-safe base64 raw source>` opens the New dialog on that source (how every other app launches DataCube) | ✓ | ❌ (`?remote=<url>` only) | ❌ | C `boot.ts:259` |
| Header buttons: Load DataCube / New DataCube / Save DataCube | ✓ | Save View / Load View in the hamburger | ⚠️ | C |
| **Load dialog**: search by name or ID (debounced), Mine Only, Sort by Last Viewed / Created / Updated, recently viewed first (MRU of 10), "Found N / 50+ matches", cards (name, relative updated time, owner), Copy ID, Manage › Delete…, OK | ✓ | one slot, no dialog | ❌ | C `app.ts:1968` |
| **Save dialog**: Name, "keep report name in sync", Advanced › Auto-enable caching; Save (owner only) / Save As / Save (new) | ✓ | one slot, the name = title | ❌ | C `app.ts:1946` |
| **What a save holds**: the whole `DataCubeSpecification` (query as Pure, full configuration, source, options) | ✓ | snapshot + open paths + order + formats (the last two not reloaded) | ❌ | C! `app.ts:1946-1995` |
| **Delete**: typed confirmation `<user>_<yyyyMMdd>` | ✓ | ❌ | ❌ | C |
| Owner, created / updated / last opened; owner-only update/delete | ✓ | ❌ | ❌ | C |
| Hamburger: View Source, Edit Source Query, Reset to Latest Save, Edit Latest Saved Query, Update Info…, Delete DataCube…, docs, About (environment, version, servers) | ✓ | host *Data…*, *Generated Pure & SQL…* | ❌ | C `boot.ts:394` |
| Release notes pop-up and log | ✓ | ❌ (not needed) | — | — |
| Window title `⊞ <name> - <session time>`; leave-page guard | ✓ | ❌ | ❌ | C |
| First launch opens New DataCube; empty state "Create a new DataCube to start" | ✓ | the demo opens a sample cube | ⚠️ | C |
| New DataCube dialog: choose the source type (Legend Query default, Lakehouse Consumer, UDF, Freeform TDS, Lakehouse Producer, Local File) | ✓ | the Data… upload dialog | ⚠️ | C |
| OIDC login, token refresh | ✓ | ❌ (server phase) | ❌ | C |

## I. Sources

| Source | Upstream | Ours | St |
|---|---|---|---|
| **Legend Query** `{_type:"legendQuery", queryId, parameterValues}`: query-store search picker, read-only code, typed/enum parameter editors (enum from the system model, then depot), saved values win when the name and type match; TDS queries auto-converted to relation; header "Parameters:" chips; View Source → edit parameters (locked while caching) | ✓ | ❌ (no saved-query store) | ❌ |
| **User-Defined Function** `{functionPath, runtime, model: pointer}`: project → version → function → runtime wizard (depot); no parameters | ✓ | ❌ | ❌ |
| **Freeform TDS expression** `{query, runtime, mapping?, model pointer}`: project / version / runtime / mapping + Pure editor, compiled live | ✓ | ⚠️ our source IS a Pure expression over a model, but not this typed, saved source | ⚠️ |
| **Local file** `{fileName, fileFormat:"csv", _ref, columnNames}`: CSV into the browser DuckDB; preview; a **saved cube asks for the file again** and checks its columns | ✓ | ➕ CSV + Parquet + inferred model; ❌ no re-upload prompt on load | ⚠️ |
| **Lakehouse Consumer / Producer** (data products, ingest, Iceberg, Snowflake) | ✓ | ❌ (GS infrastructure) | ❌ |
| Remote https/s3 Parquet, CSV, Iceberg | — | ➕ | ➕ |
| Execution planes: engine (+ in-browser cache) | ✓ | ➕ three (tab / legend-lite / legend-engine) | ➕ |

## J. The exact engine HTTP surface DataCube uses

`legend-graph V1_EngineServerClient`. `{q}` = `engine.queryUrl ?? engine.url`.
legend-lite today serves `/lsp`, `/engine/{execute,plan,sql,diagram}`
and `/health`. **None of these 18 are served:**

| Call | Method + path |
|---|---|
| current user | GET `/server/v1/currentUser` |
| parse value spec | POST `/pure/v1/grammar/grammarToJson/valueSpecification` (text/plain; `sourceId`, `lineOffset`, `columnOffset`, `returnSourceInformation`; 400 = ParserError) |
| parse lambda | POST `/pure/v1/grammar/grammarToJson/lambda` |
| parse model | POST `/pure/v1/grammar/grammarToJson/model` |
| render value spec | POST `/pure/v1/grammar/jsonToGrammar/valueSpecification` (`renderStyle`; Accept text/plain) |
| return type | POST `/pure/v1/compilation/lambdaReturnType` |
| relation type | POST `/pure/v1/compilation/lambdaRelationType` (400 = EngineError with source info) |
| TDS → relation | POST `/pure/v1/compilation/autofix/transformTdsToRelation/lambda` |
| completion | POST `/pure/v1/codeCompletion/completeCode` (skipped by ruling) |
| execute | POST `/pure/v1/execution/execute` (+ `serializationFormat=CSV`, streamed) |
| plan | POST `/pure/v1/execution/generatePlan` |
| query store | POST `{q}/pure/v1/query/search`; GET `{q}/pure/v1/query/{id}` |
| DataCube store | POST `{q}/pure/v1/query/dataCube/search`; GET `…/dataCube/batch?queryIds=`; GET `…/dataCube/{id}`; POST `…/dataCube`; PUT `…/dataCube/{id}`; DELETE `…/dataCube/{id}` |

engine 4.145.0 adds `GET …/dataCube/events` and `…/dataCube/stats`. The
stored record is `{id, name, description, content, owner, createdAt,
lastUpdatedAt, lastOpenAt}`. The sources also call depot:
`getProjects`, versions, `getVersionEntity`, `getVersionEntities`,
`getDependencyEntities` and `getProject`.

## K. Launch points in the other applications

| Where | What | Ours |
|---|---|---|
| Legend Query | Action "Launch Legend DataCube..." (saved queries only) → `<dataCube>?sourceData={legendQuery, queryId}`; the query's usage panel shows and copies that URL; route `/edit/:queryId/cube` embeds a cube in Legend Query | ❌ |
| Query Builder | `QueryBuilderDataCubeEngine` + a modal viewer over the current query, parameters and mapping/runtime | ❌ |
| Studio | "Data Cube (BETA)" on the Function editor and the Service execution editor, and in the explorer's context menu (functions, services); relation queries only | ❌ |
| REPL | Its own `/api/dataCube/*` backend; **Publish** turns the REPL source into a Freeform TDS source, POSTs a `PersistentDataCube` to the query store, and shows a link `<dataCube>/<id>` | ❌ |
| Data products / Marketplace / Legend AI chat | "Open in Datacube" per access point (not parameterised ones); the AI chat's "Open in DataCube" translates its SQL into a data-product accessor query | ❌ |

## L. Ours beyond upstream

- Working versions of upstream's greyed-out "WIP" menu entries:
  - HTML, plain-text, PDF and specification export;
  - heatmaps;
  - plot and treemap.
- Selection statistics.
- Drill-through.
- Median and weighted average.
- Case-insensitive in / not in.
- Sources: Parquet, remote https/s3 and Iceberg.
- Three execution planes, with the planner in the tab as WebAssembly.
- Snap mode.
- An ARIA treegrid with keyboard navigation.
- Undo covering every action, with keyboard shortcuts.
- A per-column heatmap and pivot direction.
- Configuration options upstream lacks: keep grouped columns, fold the
  zones and the title bar.

## Proposed order (what to build, biggest first)

1. **Save/load as the upstream feature.** Needs the API program:
   - the spec as `DataCubeSpecification`, with the query as Pure text,
     the full configuration and a typed source;
   - the Pure-text → cube reader;
   - the query-store DataCube endpoints on legend-lite;
   - Load / Save / Save As / Delete dialogs;
   - `/:id` and `?sourceData=`.

   It also fixes today's `SavedView` defects (configuration not saved,
   order and formats not reloaded).
2. **The engine HTTP surface of §J** — `ENGINE_API_CONTRACT.md`, the
   agreed next program.
3. **Pivot totals** (upstream bug, user ruling): computed in the database
   as the measure over the row's whole slice with the pivot key dropped
   — correct for every aggregate, the same as our row subtotals;
   placement and name from the upstream fields; the per-measure
   function read as the total's aggregate.
4. **Two correctness defects that need no program:**
   - Properties Apply must not keep a refused draft (headline 7);
   - carried columns after a pivot must use their own aggregate.
5. **Several windows at once.** Filter, Properties and column editors
   open together, as upstream.
6. **Filter editor parity (§C).**
   - Cancel / Apply / OK.
   - Clone on `+`.
   - OR layering; flattening on remove.
   - Operators filtered by type.
   - Typed value editors: number, date modes, boolean, list, same-type
     column.
7. **Grid basics (§B):**
   - click-to-sort with indicators;
   - edge-drag resize;
   - tooltips;
   - the scroll readout;
   - overlays.
8. **Shell (§G):**
   - the Settings window;
   - alerts;
   - the execution-error and code-check alerts with debug info;
   - documentation hints.
9. **Small parity rows:**
   - the unit prefix `_`;
   - the font family and size lists;
   - the underline variant;
   - Standard XOR Custom highlight;
   - the pinned checkmark;
   - Properties-from-header;
   - export confirmation and timestamped file names;
   - `.eml` email without a host;
   - the default of negative-in-parens;
   - the kind lock and its pivot reset;
   - type-filtered aggregates;
   - TODAY / NOW by type;
   - first / last (a core leg).
10. **Calculated-column editor parity (§D):**
   - value type;
   - live compile with markers;
   - Reset;
   - one window per column.
11. **Pagination** (a decision to make first: upstream defaults it ON,
    and our virtual windowing may make it unnecessary), and detail rows
    at the leaf level (also an Essbase prerequisite).
12. **Essbase mode** (`FEATURE_CENSUS.md` §10), then the **server phase**:
    sources (Legend Query, UDF, Freeform, local-file re-prompt), launch
    points (§K), and OIDC.
