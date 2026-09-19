# DataCube feature census, and the phase plan

Counted from `legend-studio`, not remembered. Four sources: the
editor's tabs, the configuration model, the grid context menu, and the
snapshot's query shape.

## The surface

**Editor tabs (7)** — General Properties, Column Properties, Columns,
Vertical Pivots, Dimensions, Horizontal Pivots, Sorts.

**Cube configuration — 34 settings.** Grid lines (horizontal,
vertical, colour, mode), a full font set (family, size, bold, italic,
underline, strikethrough, case), text alignment, four foreground and
four background colours (normal / negative / zero / error),
alternating rows (on, standard-mode, colour, count), selection stats,
truncation warning, root aggregation, leaf count, tree-column sort
direction, pivot statistic column, pivot layout, dimensions.

**Per-column configuration — 39 settings.** name, type, kind,
displayName, decimals, displayCommas, negativeNumberInParens,
numberScale, missingValueDisplayText, unit, the same font and colour
sets per column, isSelected, hideFromView, blur, fixedWidth, minWidth,
maxWidth, pinned, displayAsLink, linkLabelParameter,
aggregationParameters, excludedFromPivot, pivotSortDirection,
pivotStatisticColumnFunction.

**Context menu — 66 actions.** Sort (asc/desc/add/clear), export
(HTML, plain text, PDF, Excel, CSV, specification), email with each of
those as an attachment, copy (selected rows, selected column), filter
(more filters on a column, clear all), pivot (vertical and horizontal,
add/remove/clear), extended columns (add, edit, delete), resize
(auto-size, minimize, fit screen), pin (left, right, unpin, clear),
hide, collapse all, heatmap (add/remove), plot, treemap, properties,
zoom.

**Snapshot query shape.**

    configuration
    sourceColumns
    leafExtendedColumns      extend BEFORE aggregation
    filter
    selectColumns
    pivot
    groupBy
    groupExtendedColumns     extend AFTER aggregation
    sortColumns
    limit
    dimensionalTree

## What is already built here

Filter (31 operators, nested tree editor), pivot including
multi-dimension and multi-measure, vertical pivots to arbitrary depth,
subtotals and grand total, the single tree column, sorts with a total
order, the row cap with truncation warning, snap mode, saved views,
drill-through, CSV/TSV export, column order/visibility/width, and the
formatter with currency/percent/scale/parens.

## The gap that matters most

**`groupExtendedColumns` — derived columns computed AFTER
aggregation.** This is how a variance percentage, a ratio of two
measures, or a contribution-to-total is expressed, and it is
structurally different from `leafExtendedColumns` (which this
implementation has as `derived`): those are computed per source row
and then aggregated, these are computed from the aggregates
themselves. Neither can express the other. It is also the feature the
research named as the second most-complained-about gap in spreadsheet
pivots.

## Phases — all six complete

Ordered by dependency and by risk, not by visibility. Status added
after the work; each phase's reasoning is in its commit.

**Phase 1 — query completeness.** groupExtendedColumns; sort by a
pivoted column; pivotSortDirection and treeColumnSortDirection;
excludedFromPivot; column kind (dimension vs measure).

**Phase 2 — column properties.** displayName, decimals,
displayCommas, negativeNumberInParens, numberScale,
missingValueDisplayText, unit; fixedWidth / minWidth / maxWidth;
pinned; hideFromView; isSelected; blur; displayAsLink.

**Phase 3 — appearance.** Grid lines and mode; the font set; text
alignment; the four foreground and four background colours applied by
value (normal / negative / zero / error); alternating rows.

**Phase 4 — interaction.** The context menu; cell selection and
clipboard copy; selection statistics; resize and pin actions;
collapse all.

**Phase 5 — export and sharing.** Excel, HTML, plain text; the
specification export; email attachments.

**Phase 6 — advanced.** Heatmap; plot and treemap; the dimensional
tree mode.

Deliberately not planned: PDF export, which needs a rendering
pipeline out of proportion to its value here, and email, which is a
host concern rather than a grid one. Both are recorded so their
absence is a decision rather than an oversight.


## Status after the phases

All six are done: 5,864 lines of product code, 362 tests, 22 browser
checks, tsc clean under strict with noUncheckedIndexedAccess and
exactOptionalPropertyTypes.

**Phase 1, query completeness.** groupExtendedColumns (proved against
a real engine: a per-row margin averaged gives 0.500 where the same
margin from the aggregates gives 0.108); sorting by a pivoted column,
which already worked and now has proof; column kind; excludedFromPivot;
treeColumnSort.

**Phase 2, column properties.** Seven number scales with suffixes and
an auto scale chosen per value; decimals; separators; units; font
case; width bounds as clamps; pinning; display names; blur.

**Phase 3, appearance.** Four foreground and four background colours
resolved by value state, cube and column merged field by field; grid
lines by custom property; alternating rows with a band size.

**Phase 4, interaction.** Cell selection as an anchored rectangle,
selection statistics excluding blanks, clipboard copy through the
existing exporter; the context menu built and tested as data, with a
keyboard-operable renderer.

**Phase 5, export.** HTML as a standalone document; Excel as
SpreadsheetML so numbers arrive as numbers; the specification export
is the saved-view JSON that already existed.

**Phase 6, advanced.** Per-column heatmaps with an optionally fixed
scale; named dimensions drilled as a unit, composed onto the existing
row tree rather than a second mode.

## Phase 7 — the product

The audit that closed phase 6 found the honest problem: most of
phases 4 to 6 were built, tested and **unreachable**. `grep -c
contextmenu src/grid/grid.ts` returned 0; `MenuView`, `buildMenu`,
`heatColour`, `toHtml`, `toSpreadsheetML`, `drillQuery` and
`selectionStats` each had exactly one user, the file that defined
it. Good unit tests on all of them, and no user could reach any.

So phase 7 is the editor and the assembly.

**One configuration.** It existed four times over -- ColumnLayout,
GridOptions.formats, GridOptions.appearance, and the snapshot. Now
`config.ts` is authoritative and those four are projections. A
column's settings live under its NAME, which is what makes them
survive being hidden, reordered, pivoted and brought back.

**The seven panels**, in DataCube's tab order, with its labels, over
one draft: nothing reaches the cube until Apply, so a half-built
pivot never issues a query and Cancel is a discard rather than an
undo log.

**Drag to pivot**, which is ag-Grid's row group panel
(`rowGroupPanelShow: 'always'`). DataCube deliberately disables the
matching pivot panel because of ag-Grid restrictions that do not
apply here, so both zones exist -- the column zone being the one
place this goes beyond DataCube rather than matching it. The drag
SOURCE is a Columns tool panel, because a pivoted cube has no
dimension header to drag: the row dimensions collapse into one tree
column with a blank header.

**A floating filter**, which DataCube does NOT have -- grepping
legend-data-cube for `floatingFilter` returns nothing. It writes
into the same FilterNode tree the editor edits, so the two cannot
drift, and a filter too complex for a box disables the box rather
than blanking it. It is a strip of labelled boxes over the
dimensions in play rather than a box per leaf: in an aggregating
cube no leaf is ever a source column.

**A reachability guardrail**, so the phase-6 failure cannot recur:
each module must be imported by `src/app.ts` and called, by name.

531 tests, 45 browser checks against real DuckDB-WASM.

## Still open

- The app has only ever run against the demo SQL shim. `/engine/plan`
  is proven in isolation; the end-to-end path through legend-lite
  needs a model fixture and a running server.
- Differential tests against legend-engine, which is what would make
  backwards compatibility a proven property rather than a design
  intent. This was step 2 of the original plan and remains the
  largest gap.
- The server plane: warehouse dialects, parameterised plans, grouping
  sets. Still unsized.
- On the engine side: the TypedRelationOp sealed classification, and
  whether SubQueryLift has pivot's "tested only in the wrong shape"
  hole.

## Deliberately not built

PDF export (a rendering pipeline out of proportion to its value),
email (a host concern), plot and treemap (a charting surface, not a
grid feature), and leaf counts (they need a count aggregate in every
level query, and a display toggle that rewrites the user's SQL is the
wrong shape -- it belongs as an explicit measure).
