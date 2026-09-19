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

## Phases

Ordered by dependency and by risk, not by visibility.

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
