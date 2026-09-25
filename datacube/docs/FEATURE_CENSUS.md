# DataCube feature census — 2026-09-25

What upstream Legend DataCube has, what ours has, and what stands between
ours and a shippable product. Every row carries its evidence; nothing
here is from memory.

- **Spec:** legend-studio `c5b2f2c78` (2026-09-14) —
  `packages/legend-data-cube` 0.3.121 (the component, ~34k lines) and
  `packages/legend-application-data-cube` (the app around it: sources,
  save/load). Read as spec only.
- **Ours:** legend-lite `main` @ `cef2288a4`, `datacube/`.
- **Evidence codes:** **H** = passes in `bazel run //datacube:verify_features`
  (89/89 on this date, 2 known gaps) · **P** = observed in a headless
  browser probe written for this census (driving the built site the way a
  user would) · **N** = proven in Node against `src/` (config → snapshot →
  Pure) · **C** = read in code, not exercised.
- **Status:** ✅ works · ⚠️ present but broken or partial · ❌ missing ·
  ➕ ours only · 🚫 upstream placeholder (greyed-out "WIP" entry).

## Headline

The component is in far better shape than "unfinished" suggests: 89
browser-checked features pass, and ours exceeds upstream in export,
charts, drill-through, file sources and execution planes. What is wrong
is concentrated in two places:

1. **Calculated columns look broken because, in the most common flows,
   they are.** A new column in a grouped cube renders blank; a calculated
   column can never be grouped or pivoted on; a column pivot silently
   drops calculated measures; a refused expression deletes what the user
   typed (§1).
2. **Eight editor controls do nothing.** They write the configuration,
   and nothing between the configuration and the query reads them — the
   worst one being Column Properties → Aggregation, which leaves the query
   byte-identical (§2).

Both are ship-blockers, both are small and local, and both are
invisible to the harness because it checks each control's *presence*
and a handful of *outcomes*, not every control's outcome.

---

## 1. Calculated columns (user item 1)

> **Update 2026-09-25 — C1–C6 FIXED**, each first added as a failing
> check in `verify_features` (95/95 after):
> - one lookup, `rowColumns` (`src/snapshot.ts`), for every column that
>   exists before aggregation — grouping, the menu's types, the pivot's
>   measures, the Filters dialog, the sidebar, named dimensions and the
>   editor's pivot selectors all read it (C2, C3, C4);
> - a new column defaults to **measure**, named `col_N`, as upstream (C1);
> - the editor awaits the planner: a refusal keeps the form, the text
>   and the planner's message in it (C5);
> - right-click **Extended Columns** › Add New Column… / Extend Column
>   X… (seeded `$x.X`, X's kind) / Edit Column X… / Delete Column X;
>   gone from the hamburger; **Layout** gone from the right-click menu
>   (C6);
> - found on the way: a **Boolean** column had no value filters at all
>   (source ones too); now `=` / `!=`, upstream's set.
>
> Still open: C7 (`-0`, default precision), C8, D1.

Upstream's model (`DataCubeExtendManagerState`, `DataCubeColumnEditorState`):
leaf-level columns (`extend` before `groupBy`, declared **measure** or
**dimension**) and group-level columns (`extend` after `groupBy`); the
editor type-checks the expression against the engine
(`getReturnType`) before applying; a new column defaults to **MEASURE**
(`DataCubeColumnEditorState.tsx:302`); entry points are in the grid's
right-click menu under **Extended Columns**.

Ours has the same two stages (`derived` / `groupDerived`,
`src/snapshot.ts`), a Pure expression editor with completion
(`src/ui/calc-editor.ts`, `src/calc.ts`), and a correct pipeline order in
`src/serialize.ts` (row `extend` → `filter` → `select` → `pivot`/`groupBy`
→ group `extend` → `sort` → `limit`). What works is covered by eight
harness checks (**H**). What fails:

| # | Finding | Evidence | Cause | Fix direction |
|---|---|---|---|---|
| C1 | **A new calculated column renders blank in any grouped cube.** `$x.notional * 1.1` under a row group shows empty cells; the query aggregates it with `uniqueValueOnly()`. | P (P1) | The editor defaults kind to **dimension** (`calc-editor.ts:183, :214`); a numeric dimension collapses to its unique value. Upstream defaults to MEASURE. | Default to measure when the expression is numeric (or always, as upstream); the kind radio remains the override. |
| C2 | **A calculated column can never be grouped or pivoted on**, even when declared a dimension. The menu shows a bare, disabled "Vertical Pivot"; drag zones refuse it. | P (P4) | `#isDimension` (`app.ts:549`) looks the column up in `snapshot.columns`, which holds source columns only; a derived column is never found → not a dimension. | Resolve kind over source ∪ row-stage derived columns (one lookup, used by menu, drag zones and grid). |
| C3 | **A column pivot silently drops calculated measures.** With `uplift` declared a measure, pivoting on `year` aggregates only `notional` and `pnl`; the query is identical to the no-calc cube. | P (F9) | The pivot's measure set is `measureLike()` (`serialize.ts:565`, used at :615/:630/:638), which reads `snapshot.columns` — source columns only. Same root as C2/C4. | Include row-stage derived measures in the pivot's aggregate set. |
| C4 | **The filter menu offers the wrong operators for a calculated column.** A Boolean calc offers `contains`, `starts with`, `< true`… | P (P4 menu) | Column type for the menu comes from `snapshot.columns` (`app.ts:1212`); a derived column gets no type → falls back to `String`. | Same lookup fix as C2, using the learned `DerivedColumn.type`. |
| C5 | **A refused expression deletes the column and the text the user typed.** The error ("relation has no column 'nope'") appears only on the page status line; the editor list is empty afterwards. | P (F6), H passes only because it checks the message appears | The snapshot reverts on refusal (by design, `calc-editor.ts:16-21`), and the draft goes with it. | Keep the draft in the editor and show the planner's refusal *inside the form*; revert only the running query. Upstream validates before applying (`getReturnType`); ours can do the same by planning the candidate snapshot before committing it. |
| C6 | **Entry points are in the wrong place.** Only the hamburger has "Calculated Columns..."; the right-click menu has none. | P (P11) | — | User direction: move to right-click as upstream's **Extended Columns** submenu — *Add New Column…*, *Extend Column ‹col›…* (prefilled with `$x.‹col›`, kind inherited), *Edit Column ‹col›…*, *Delete Column ‹col›* (last two only on a calculated column). Remove `view.calc` from the hamburger. |
| C7 | Group-stage ratios display as `0`, `0.01`, `-0`. | P (P10) | Default 2-decimal number format; negative zero printed as `-0`. | Never print `-0`; a group-stage calc without a format should get a sensible precision (upstream seeds format from the Pure type). |
| C8 | Filtering on a float calc uses exact `==` on the displayed row's value. Works (1 row) but is fragile across formats. | P (F3) | — | Low priority; same as for source floats. |

Also noted while probing (not calc-specific):

| # | Finding | Evidence |
|---|---|---|
| D1 | The date filter's menu label shows a raw JS date: `Add Filter: trade_date = Tue Feb 09 2021 00:00:00 GMT-0500 (Eastern Standard Time)` while the cell shows `Feb 09, 2021`. The generated filter is correct (`%2021-02-09`). | P (F12) |

**Layout out of the right-click menu (user direction).** Remove the
`Layout` submenu (`menu.ts:540`). The hamburger keeps *Hide/Show Drag
Zones* and *Hide Title Bar*, and a folded title bar already leaves a lip
that restores it (H: "folding the title bar leaves a lip that restores
it"). The H check "the grid's menu restores a bar the hamburger went
with" tests exactly the entry being removed and must be rewritten
against the lip.

## 2. Controls that do nothing

> **Update 2026-09-25 — CLOSED.** Wired: Aggregation + its weight
> (grouping, pivot, calculated columns, read back into the editor),
> Initially expand to level (upstream's open-by-default, a user's
> close sticks, and a host's startup configuration now applies),
> Display as link + label parameter (http/https only). Found by the
> new guardrail and wired: **Show leaf count** ("EMEA (1234)", not yet
> under a column pivot). Removed until their feature exists: grid mode
> (returns with Essbase mode) and the pivot total name/function (our
> pivot has no total column — a feature, with an open decision:
> upstream applies a function ACROSS the row's pivot cells client-side;
> our subtotals are the measure with the key dropped, in the database;
> they differ for average and count). Guardrail:
> `test/config-readers.test.ts` fails when a configuration field has no
> reader outside the panels that write it.

Found by listing every `CubeConfiguration` field and its readers outside
`config.ts` and the editor panels, then reading `applyToSnapshot`
(`config.ts:467`) — the one place configuration reaches the query. It
carries only `kind`, `excludedFromPivot`, grouped-column visibility,
`maxRows` and `treeColumnSort`.

| Control (panel) | Field | Readers | Evidence | Upstream |
|---|---|---|---|---|
| **Aggregation** (Column Properties) | `aggregateFn` | none | **N**: `notional` set to measure + `max` → query byte-identical to default, still `sum()` | works |
| Weight column for weighted avg (Column Properties) | `aggregationParameters` | none | C | n/a (ours only; upstream uses parameters for `joinStrings`) |
| Pivot total's aggregate (Column Properties) | `pivotStatisticColumnFunction` | none | C | works |
| Pivot total column name (General) | `pivotStatisticColumnName` | none | C | works |
| Initially expand to level (General) | `initialExpandToLevel` | none | C | works |
| Grid mode: dimensional (General) | `gridMode` | none | C | WIP (see §10) |
| Display as link (Column Properties) | `displayAsLink` | none | C | works |
| Link label parameter (Column Properties) | `linkLabelParameter` | none | C | works |

Every one of these is a shipped control that silently lies. Either wire
it or remove it before shipping; `gridMode` should be removed until §10
lands.

A guardrail belongs with the fix: a test that fails when a configuration
field has no reader on the path to the query or the grid, so the class of
defect cannot recur.

## 3. Grid right-click menu

Upstream: `DataCubeGridMenuBuilder.tsx` (standard grid) and
`generateDimensionalMenuBuilder` (multidimensional grid). Ours:
`src/ui/menu.ts`.

| Entry | Upstream | Ours | Evidence |
|---|---|---|---|
| Sort: Ascending / Descending / Add Asc / Add Desc / Clear / Clear All | ✅ | ✅ | H |
| Filter: `Add Filter: col = value`, *More Filters on col…* (type-aware), is null / is not null on a blank cell, *Filters…*, *Clear All Filters* | ✅ | ✅ (⚠️ D1, C4) | H, P |
| Pivot: Vertical (set/add/remove/clear) | ✅ | ✅ (⚠️ C2) | H |
| Pivot: Horizontal (set/add/clear) | ✅ | ✅ | H |
| Pivot: *Exclude Column X from Horizontal Pivot* / *Include Column X…* | ✅ | ❌ in menu (the setting exists in Column Properties) | P (menu listing) |
| **Extended Columns**: Add New / Extend Column / Edit / Delete | ✅ | ✅ (fixed 2026-09-25) | H |
| Resize: Auto-size / Auto-size All | ✅ | ✅ | H |
| Resize: *Minimize Column*, *Minimize All Columns*, *Size Grid to Fit Screen* | ✅ | ❌ | P, C |
| Pin: Left / Right / Unpin / Remove All Pinnings | ✅ | ✅ | H |
| Hide | ✅ | ✅ | H |
| Collapse All (tree column) | ✅ | ✅ | H |
| Copy: selected cells as plain text | ✅ | ✅ | H |
| Copy: *Selected Rows as Plain Text* | ✅ | ❌ | P |
| Copy: selected column as plain text | ✅ | ✅ | H |
| Export: CSV (Grid), Excel (Grid) | ✅ | ✅ | H |
| Export: CSV (full result, server-side) | ✅ | ❌ — only the grid's rows | C |
| Export: HTML, Plain Text, PDF, DataCube Specification | 🚫 | ➕ ✅ | H |
| Email: Excel/CSV attachment (`.eml`) | ✅ | ⚠️ entries exist, gated on a host `email` option; the demo supplies none | C |
| Email: HTML / Plain Text / PDF / Specification | 🚫 | ➕ entries exist (same gate) | C |
| Heatmap add / remove | 🚫 | ➕ ✅ | H |
| Plot / Treemap | 🚫 | ➕ ✅ | C (`chart.ts`, tests) |
| Properties… | ✅ | ✅ | H |
| Layout: drag zones / title bar | — | removed 2026-09-25 (user direction); hamburger + title-bar lip keep them | H |
| Zoom Out (multidimensional grid) | ✅ WIP | ❌ — §10 | C |

## 4. Properties editor

Tabs match upstream's `DataCubeEditorTab` order (`src/ui/editor.ts`): H
"every editor tab shows a panel".

| Area | Upstream | Ours | Evidence |
|---|---|---|---|
| General: report title, tree column sort, row limit, truncation warning, show root aggregation, leaf count, selection stats, grid lines, fonts, default colours, alternate rows | ✅ | ✅ | C (readers exist), H for grand total / selection stats |
| General: initially expand to level, pivot total name | ✅ | ⚠️ dead — §2 | C |
| General: pivot total *placement* (left/right/hidden) | ✅ `pivotStatisticColumnPlacement` | ❌ no field | C |
| General: grid mode | ✅ WIP | ⚠️ dead — §2 | C |
| Columns / Vertical Pivots / Horizontal Pivots / Sorts selectors (drag, search, reorder) | ✅ | ✅ | H |
| Column Properties: kind, display name, number format, scale (bp/%/k/m/b/t/auto), unit, missing-value text, visibility, blur, pin, width (fixed/range), font, four-slot colours | ✅ | ✅ | H (kind, display name), C |
| Column Properties: aggregation, pivot total function, link | ✅ | ⚠️ dead — §2 | N, C |
| Column Properties: pivot sort direction, exclude from pivot | ✅ | ✅ | C |
| Dimensions tab (named hierarchies) | ✅ WIP | ✅ (presets for row grouping) | C (`panel-dimensions.ts`, tests) |
| Apply / OK / Cancel semantics | ✅ | ✅ | C |

## 5. Query operations

| Area | Upstream | Ours | Evidence |
|---|---|---|---|
| Filter operators (31: comparisons, in/not in, null, contains/starts/ends ± negation ± case-insensitive, *value in column* comparisons) | ✅ | ✅ same set | C (`snapshot.ts FilterOperator`) |
| Filter tree: nested AND/OR groups, NOT | ✅ | ✅ | H |
| Relative date values `TODAY` / `NOW` | ✅ | ❌ | C |
| Aggregates: sum, avg, count, min, max, unique, var/var_sample, std/std_sample, joinStrings | ✅ | ✅ | C |
| Aggregates: **first**, **last** | ✅ | ❌ | C |
| Aggregates: median, weighted average | ❌ | ➕ | C |
| Multi-column sort | ✅ | ✅ | H |
| Row limit + truncation warning | ✅ | ✅ | C |
| Bottom-level group → its detail rows | ✅ (drops `groupBy` at max depth) | ❌ — H known gap | H |

## 6. Grid

| Area | Upstream | Ours | Evidence |
|---|---|---|---|
| Row-group tree, expand/collapse, grand total, leaf count | ✅ | ✅ | H |
| Column pivot with nested headers | ✅ | ✅ | H |
| Cell range selection + stats | ✅ | ✅ | H |
| Keyboard navigation, screen-reader treegrid | ag-Grid default | ✅ ARIA APG treegrid | H |
| Pagination toggle | ✅ | ❌ (ours windows the tree; no page UI) | C |
| Panel reorder reaching the grid after a long session | — | ⚠️ H known gap (unexplained) | H |

## 7. Shell

| Area | Upstream | Ours | Evidence |
|---|---|---|---|
| Undo / Redo (history) | ✅ | ✅ | H |
| Floating, draggable, resizable windows | ✅ | ✅ | H |
| Status bar: Properties, Filter, row count, truncation warning, selection stats | ✅ | ✅ | H |
| Settings panel: debug mode, max history size, row buffer, large-dataset warning, retry failed fetches, reload | ✅ | ❌ | C |
| Title-bar menu: View Source, Edit Source Query, Reset to Latest Save, Update Info, Delete, Documentation, About | ✅ (app) | ⚠️ host menu in the demo offers *Data…* and *Generated Pure & SQL…* only | C |
| Embedding options: host menu items, header renderer, settings, callbacks | ✅ `DataCubeOptions` | ✅ `hostMenu`, `storage`, `email`, `dimensions` | C |

## 8. Sources, saving, execution

| Area | Upstream | Ours | Evidence |
|---|---|---|---|
| Local file | CSV only | ➕ CSV + Parquet, schema inferred, Pure model written | C (`upload.ts`, `infer.ts`), H via the sweep's own file |
| Remote object storage (https/s3 Parquet, CSV, Iceberg) | ❌ | ➕ | C (`remote.ts`) |
| Legend Query / Freeform TDS / User-Defined Function / Lakehouse sources | ✅ | ❌ — model-backed source via a Pure model only | C |
| Save / Save As / Load / Delete DataCube, shared catalogue with search | ✅ (server) | ⚠️ Save View / Load View to browser `localStorage` only; no delete, no sharing | H (save/load), C |
| In-browser cache of a remote source | ✅ WIP toggle | ✅ snap mode (`snap.ts`) | C |
| Execution planes | engine only (+ cache) | ➕ three: plan in the tab (WebAssembly), plan on a legend-lite server, run on legend-engine (`engine-remote.ts`, `runner.ts`) | C |

## 8b. Saving, loading, and the other applications (added 2026-09-25)

The first pass of this census read the application package for its
menu labels only, and rated save/load one row. It is the biggest
missing feature area, so here it is traced properly. Upstream sources:
`legend-graph` (`PersistentDataCube`, `V1_EngineServerClient` §DataCube),
`legend-application-data-cube` (builder store, sources, routes), and
the DataCube entry points in `legend-application-query`,
`legend-query-builder`, `legend-application-studio`,
`legend-application-repl`, `legend-extension-dsl-data-product`,
`legend-lego` (AI chat).

**What a saved DataCube IS upstream.** A `PersistentDataCube` — id,
name, description, owner, created / updated / last-opened — whose
`content` is a `DataCubeSpecification`: the cube's `query` as Pure
text, its `configuration`, a typed `source` (JSON with `_type`), and
`options`. It lives in the engine's query store beside saved queries,
`/pure/v1/query/dataCube`: search (term, mine-only, sorted), batch get,
get, create, update, delete.

| Upstream feature | Ours | Evidence |
|---|---|---|
| Save / Save As (name + description) to a shared server store | ⚠️ *Save View* writes ONE fixed `localStorage` slot (`VIEW_KEY`, `app.ts:1797`); each save overwrites the last | C |
| Load DataCube: search by name, "mine only", recently viewed, copy id | ⚠️ *Load View* loads that one slot; no list, no search | C |
| Update Info (rename, description), Delete DataCube, Reset to Latest Save | ❌ | C |
| A saved cube has a URL (`/:dataCubeId`), so it can be shared as a link | ❌ | C |
| New cube from a URL (`?sourceData=<source JSON>`) — how other apps open DataCube | ❌ (only `?remote=<file url>` in the demo) | C |
| Owner, created / updated / last-opened timestamps | ❌ | C |
| The saved record carries the SOURCE, so reopening it re-derives the data | ⚠️ the snapshot's source expression is saved; an uploaded file is not, and nothing asks for it again (upstream stores name + format and prompts for the file) | C |
| View Source; Edit Source Query (jump to Legend Query); Edit Latest Saved Query | ❌ (the demo host menu has *Generated Pure & SQL…*) | C |
| A server-side store to put any of this in | ❌ legend-lite's server serves `/lsp`, `/engine/{execute,plan,sql,diagram}`, `/health` only (`LegendHttpServer.java:47-56`); no query store, no DataCube store | C |

**Sources a saved cube can be built on** (the typed `source`):

| Upstream source | What it references | Ours |
|---|---|---|
| **Legend Query** | a saved query by `queryId` + parameter values; loads its lambda, mapping and runtime | ❌ — there is no saved-query store to reference |
| **User-Defined Function** | a function returning a relation, by project/version + path | ❌ |
| **Freeform TDS expression** | Pure text + runtime + model | ⚠️ equivalent in spirit: our cube's source IS a Pure expression over a model, but it is not a selectable, saved source kind |
| **Local file** | CSV (upstream TODO: Parquet, Arrow, Excel) | ✅ CSV + Parquet (but not re-prompted on reload, above) |
| **Lakehouse producer / consumer** | a data product's access point | ❌ |
| Remote object storage (https / s3 Parquet, CSV, Iceberg) | — | ➕ |

**Entry points from the other applications** (each opens DataCube on
the query in hand):

| Where | What the user gets | Ours |
|---|---|---|
| Legend Query app | Open an existing saved query in DataCube (embedded viewer, and a link into the DataCube app with `sourceData`) | ❌ |
| Query Builder | a *Data Cube* button: the current query (lambda, mapping, runtime, parameters) in an embedded cube | ❌ |
| Studio | *Data Cube (BETA)* on the Function editor, the Service execution editor, and the explorer's context menu | ❌ (Studio Lite has no such entry) |
| REPL | *Publish*: saves the cube to the store and hands back a link and an id | ❌ |
| Data products / marketplace, AI chat | *Open in DataCube* on an access point or a generated query | ❌ |

**What this means for the plan.** Save/load is not a DataCube-page
feature; it is a **server** feature with a page on top: a store (the
query store's DataCube half, and the saved-query half that a Legend
Query source points at), a URL scheme (`/:id`, `?sourceData=`), and
typed sources. It belongs in the server phase, and it should be built
on the same server as server-side DuckDB rather than as a second
browser-only mechanism.

## 9. Ours beyond upstream

Plain-text, HTML and PDF export; specification export; heatmaps; plot and
treemap; drill-through (the rows behind a number, `drill.ts`); Parquet
and remote-storage sources; the in-tab WebAssembly planner; median and
weighted average; three execution planes; undo covering every action;
an ARIA treegrid. Upstream has most of the first four as greyed-out
placeholders.

## 10. Essbase mode (user item 2)

**Checked against Oracle's documentation, the reading is right: it is
Smart View ad hoc analysis.** Its operations
([Analyzing Plan Data in Smart View](https://docs.oracle.com/en/cloud/saas/planning-budgeting-cloud/sv-tutorial-adhocanalysis/index.html),
[Member Options](https://docs.oracle.com/en/applications/enterprise-performance-management/smart-view/25.200/uugsv/opt_memb.html),
[Ad Hoc Behavior](https://docs.oracle.com/en/applications/enterprise-performance-management/smart-view/23.200/uugsv/smart_view_behavior_options_ad_hoc.html)):

- **Dimensions on three axes:** rows, columns, and the **POV** — every
  dimension not on the grid is pinned to one member (initially its top,
  "All"), shown in a POV bar.
- **Zoom In** on a member (double-click by default), with a level:
  *Next Level*, *All Levels*, *Bottom Level*, *Same Level*, *Sibling
  Level*, *Same Generation*. **Zoom Out** to the parent.
- **Keep Only / Remove Only** on selected members (multi-select).
- **Pivot** a dimension between rows and columns; **Pivot to POV**.
- **Member Selection** dialog: search the hierarchy, pick members,
  "descendants inclusive".
- **Suppress** rows/columns that are missing, zero, or repeated members.
- **Display:** member name / alias / both; indentation (subitems,
  totals, none); ancestor position (top or bottom).
- Planning apps add **Submit Data** (write-back) — out of scope for a
  read-only cube.

**Upstream's version** is `DataCubeGridMode.MULTIDIMENSIONAL`, badged WIP:
named dimensions (a name + ordered columns), each dimension one grid
column starting at `ALL`, double-click drills a member one level
(`retrieveDrilldownData`), *Zoom Out* on the right-click menu
(`retrieveDrilloutData`), vertical pivots ignored in that mode. No POV,
no Keep/Remove Only, no zoom levels, no member selection, no
suppression. So matching upstream is not the bar; Smart View is.

**Ours today:** named dimensions exist as presets for row grouping
(`dimensions.ts`, `panel-dimensions.ts`, drill down/up one level), and
a `gridMode: 'dimensional'` setting that nothing reads (§2).

**What the mode needs**, mapped onto machinery that already exists:

| Smart View operation | Maps to |
|---|---|
| Hierarchies | Named dimensions (have) |
| Rows / columns axes | Row groups and column pivots (have) |
| POV bar | A per-dimension member filter shown as a bar; "All" = no filter (new UI, existing filter tree) |
| Zoom In (next level) / double-click | `drillDown` on the dimension, scoped to that member (extend) |
| Zoom In: all levels / bottom level | Expand to depth n / to the leaf column (extend) |
| Zoom In: same level / sibling / same generation | Members at a level with/without the parent filter (new) |
| Zoom Out | `drillUp` (have, per dimension) |
| Keep Only / Remove Only | `in` / `not in` on the member's column at its level (existing filter operators) |
| Pivot / Pivot to POV | Move a dimension between rows, columns, POV (existing zone moves + new POV zone) |
| Member Selection | Searchable hierarchy picker writing an `in` filter (new dialog) |
| Suppress missing / zero | Filter empty/zero aggregate rows (new, post-aggregation) |
| Suppress repeated members | Render option on the flattened grid (new) |
| Name / alias, indentation, ancestor position | Display options (new) |

Prerequisites from this census: C2 (calculated dimensions must be
groupable, or they cannot sit in a hierarchy) and the bottom-level gap
in §5 (Zoom In to the leaf needs detail rows).

## 11. Proposed order

1. **Calculated columns, ship-blocking:** C1, C2, C3, C4, C5; move the
   entry points to the right-click menu (C6); remove `Layout` from the
   right-click menu and rewrite its harness check. Add harness checks for
   each finding first so every fix is proven in the browser.
2. **Dead controls (§2):** wire Aggregation, weight, pivot total
   function/name, initial expand level, links; remove `gridMode` until
   Essbase mode lands; add the no-reader guardrail.
3. **Small parity gaps:** `first`/`last`, `TODAY`/`NOW`, Selected Rows
   copy, Minimize / Size to Fit, Exclude/Include from horizontal pivot in
   the menu, pivot total placement, D1 date label, C7 `-0`.
4. **Bottom-level detail rows** (H known gap) — also an Essbase
   prerequisite.
5. **Essbase mode (§10).**
6. **Server phase:** the store and URL scheme of §8b (saved DataCubes
   and saved queries, `/:id`, `?sourceData=`), typed sources (Legend
   Query, function, file, lakehouse), the entry points from the query
   builder and Studio, full-result server CSV export, email — built on
   the same server as the server-side DuckDB work.
