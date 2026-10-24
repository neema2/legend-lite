# DataCube charts and dashboards: what the ECharts gallery shows we need (design, 2026-10-09)

The DataCube + Python line. The order (§3) and the maps (§4) are agreed with the user (2026-10-09); batches 2-5 were
widened with the user on 2026-10-14 (§3.1-§3.3: every chart we can realistically draw, the spec's version 2, the
calculated columns packaged as presets, an example page per batch); §6 lists what is still open. This builds on `docs/DATACUBE_DASHBOARDS_DESIGN_2026_09_28.md` (the chart spec, the page model and its
linking rules, milestones M0-M5) rather than replacing it: batch 1 here is that design's M3 and M4, never built.

## 0. The ask

The user, 2026-10-09: "all the charts -- look at echarts examples"; then "definitely look through all the examples on
their example page -- what are important features are there for BI/visualization/dashboarding that we need to add to
Datacube?" And on maps: "yes long/lat", world regions "that would only download to browser when used/needed", and
borders "configurable by where the user is".

## 1. What was read

All 377 examples of the gallery, from its source repository: their list by category, and each one's source, counted for
the chart kinds and features it uses (`docs/datacube-charts-homework-2026-10-09/`: `examples.md`, `census.txt`, and the
two scripts that made them). The kinds, by how many examples use each: line 59, bar 57, scatter 43, pie 22, graph 16,
custom 16, heatmap 14, gauge 12, then effectScatter, lines, pictorialBar, sankey, sunburst, tree and treemap (7 each),
candlestick and map (6), radar (5), chord, funnel and parallel (4), boxplot (3), themeRiver (2). The features: a label
or axis formatter 95, a value-coloured scale (visualMap) 45, the toolbox (save as image, data view, switch kind) 44,
dimming the rest on hover 39, zoom and pan (dataZoom) 34, a second value axis 31, timed updates 26, reference lines 13,
pinned highest and lowest 7, shaded bands 3, several grids 14 and the matrix 13 (small multiples), large-data modes 15,
brush selection 6, a crosshair shared across charts 4, drill-down 3. (3D is a separate gallery, echarts-gl: not read.)

## 2. What DataCube has

Seven kinds -- bar, line, area, scatter, pie, heatmap, treemap (`datacube/src/chart-spec.ts`) -- and six options:
orientation, stack (none, stacked, 100%), sort, top N, labels, legend position. A chart is a view of its grid's query,
regrouped by its own encoding; the database aggregates, the chart only draws (`chart-option.ts`, the one file that
builds the ECharts option; `chart-render.ts`, the one that draws). A click on a mark filters the chart's own grid (a
chart's selection, `page/cube-page.ts`). A chart exports as a picture. ECharts 6.1.0 is registered piece by piece
(`echarts/core`) and loads only when a chart first draws (`test/bundle-budget.test.ts` holds it out of the grid's
startup). `chart-option.ts`'s house rules: the dataviz palette, bars at most 24 px wide, one value axis.

## 3. What to add, in order (agreed)

Each batch lands on its own (an audit, the local gate, CI, the user's OK), and each reaches Python the moment it lands:
`ll.Page` writes the same chart spec and page document, and `datacube/test/python-vocabulary.test.ts` holds Python's
words to DataCube's. *(Amended 2026-10-16, PR neema2/legend-lite#47: Python keeps no list of the chart's words any
more, so the test went with its pins -- DataCube's reader, asked headless by `gridChart`, refuses a word it does not
know and names the words it takes; a chart's version comes from the chart DataCube starts from.)*

### Batch 1: the dashboard around the charts (the dashboards design's M3 and M4)

The largest gaps are not in the charts.

**Landing (2026-10-10):** the KPI lands first. Page filters and cross-filtering were built on the batch's branch, matching
columns by name; the user then ruled that what a filter reaches comes from the Pure model, not a name (DataCube's own
links, below, included), and that nothing reaches across tiles without one: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
(revision 3: prep makes classes, dashboards are built on them). They land rebuilt on it, on dashboards over classes,
with its step 3 (its §13: dashboards on classes).

1. **Page filters.** A filter for the whole page -- a date range, a pick-list of values, a number range -- shown in the
   page's bar, reaching every tile whose source has the column (the dashboards design §5.4: "all tiles except
   excluded"; each tile says what filters it). Saved in the page document (its version 4: `filters`), never in a
   tile's own cube, so a grid exported alone is still the grid alone.
2. **Cross-filtering.** A click (and, batch 2, a brush) on one tile filters the others on the page, not only its own
   grid; the tile clicked highlights its mark rather than filtering itself. Within one source first; then across
   sources through declared links -- a column of one source named as a column of another, suggested by name and type,
   confirmed by the user (§5.4's links; a tile not linked says so).
3. **A KPI tile.** One number, large, in the grid's format; under it how it stands against a target (a percentage,
   good or bad by which way is good), and an optional small trend line; its change against the period before is batch
   4's (it needs the period before computed). As built, a chart kind (`mark: 'kpi'`), not a tile kind of its own: it
   is a view of its grid's query as every chart is, so it has a chart's every way -- placed beside its grid, on any
   sheet, stacked, following the grid's measure or frozen, saved, and Python's `grid.chart('kpi', ...)` -- with none
   of it written twice; its options carry the target and which way is good.
   **As built, 2026-10-23 (the user: "make them look nicer, more sleek"):** a stat tile. Its number is a headline,
   the grid's own format scaled from 100,000 ("$35.7m"; format.ts `headlineFormat`, a percentage to one place), in
   full on a hover. Under it: the difference from its target, coloured and arrowed, then in words; a meter of how much
   of the target is reached, the target at its end; its latest period's change from the one before ("▲ 3.3% Dec vs
   Nov 2025"), the database's -- the trend's query carries the measure shown as `percentChange`, the lag window below
   (chart-spec.ts `kpiChange`); and the trend in the quiet hue, its latest point marked. Sized by its own tile (a CSS
   container), it sheds its lesser lines when short. The gallery's last sheet, **KPI dashboard**, lays out a strip of
   eight on top, then the trend against target, the channel mix and the target's ring, then the top products, the
   regions by channel and the map. Found on the way: every chart's text named the font `inherit`, which a canvas
   refuses -- drawing in whatever font it had last, its sizes lost (a gauge's 30px number at 10px); the theme now
   carries the page's font by name (chart-option.ts `ChartTheme.font`).

### Batches 2-5: every chart we can realistically draw (revised with the user, 2026-10-14)

The user, 2026-10-14: "do all the charting you can -- like every possible combination of charts and visualizations
that we can realistically do ... anything BI/visualization/PowerBI/tableau/superset ideas ... does not have to conform
to current charts model in datacube". Then, before any new query machinery: "check Extended Columns or Calculated
Columns first -- that might have the starting point or most of what you want". It did (§3.2), and the plan below is
built on it. Agreed in conversation the same day: the spec's version 2 (§3.1), a chart's own calculations kept in its
spec (§3.2, option (a)), the presets shared by the chart and the grid (§3.2), an example page per batch (§3.3), and the
batches in this order.

#### 3.1 The chart spec, version 2: a slot per role

Version 1 is fixed slots -- one column across (`x`), one `split` (a series per value), the measures (`y`), six options --
enough for seven kinds and no more: a bubble needs a size, a bar-and-line chart a kind and an axis per measure, small
multiples a facet, a reference line a layer. Version 2 is a SUPERSET of version 1, every new field optional, so a
version 1 spec reads as a version 2 spec unchanged (the reader moves the number; nothing else is rewritten) and every
chart drawn today draws the same:

| Slot | What it holds | What uses it |
|---|---|---|
| `x` | the column across; with a **grain** (a date's day, week, month, quarter, year) or **bins** (a number's width) | every axis chart; a time axis |
| `y[]` | each measure: its column and aggregate -- or `fn: 'value'`, a GROUP-LEVEL column already computed per group --, its own `mark` (bar, line, area), its `axis` (left, right; else by unit, §6), and a **show as** (§3.2) | bar and line together; running totals, % of total |
| `split` | the colour slot: one series per value (the name kept: Python and saved pages say it) | as today |
| `size` | a measure sizing each mark | bubble, scatter |
| `label` | what a mark says on it: its value, the category, another measure | data labels |
| `tooltip[]` | further measures a hover lists, beside the plotted ones | every kind |
| `facet` | a column per small chart (rows, or columns, or both): small multiples on shared axes | small multiples, the matrix |
| `layers[]` | reference lines (a constant, a target, the average, min or max of a measure -- the database's figure, a group-level window over every row of the chart, before its top N; the median comes with batch 4's percentile), bands (between two values, or a date range), thresholds (colour by value: below a target red) and pinned extremes | the analytics layer |
| `drill[]` | the path drilled so far (the grid's row groupings, one level per click) | drill-down |
| `calcs[]` | the chart's own calculated columns, written in Pure as the grid's are (the column editor's own types, `DerivedColumn`) | anything a preset does not say |
| `options` | grows: smooth, step, log axis, axis breaks, label position and rotation, zoom (slider), brush, sampling, hover dimming, decal fills, the toolbox | §3 batch 2 item 12 |

A new KIND stays a new `mark`; a reader that does not know one refuses it by name (`page-document.ts`, as now). The
spec stays plain JSON, holding no functions: formats come from the grid at draw time, never stored.

A page this DataCube saves writes its charts as version 2, whether or not they use a slot version 2 adds, so a DataCube
from before batch 2 refuses it ("its version 2 is not one this DataCube reads"). Accepted (the DataCube + Python line's
review, 2026-10-14): there is one deployed DataCube, and it reads every version before its own.

**Planners.** Every preset's query is planned by legend-lite and run on DuckDB in `test/chart-queries.test.ts`. Not yet
planned through legend-engine (a running 4.145, as D5b's probes did): the forms the presets lean on --
`firstDayOfWeek/Month/Quarter/Year` and `datePart` (a grain), `adjust(..., DurationUnit)` (a period's end), a bin's
nested `if` and `floor` over a division (its edge), `if(..., |[], ...)` (a change from zero), and the group-level `lag`,
`rank` and framed windows (show-as, statistics). Until they are, a chart using one on a page planned by legend-engine is
unchecked there; the probe is a step before batch 2 lands on a deployment that plans with the engine.

#### 3.2 Calculations: the calculated columns DataCube already has, packaged

What the column editor (`ui/column-editor.ts`; `snapshot.ts` `DerivedColumn`, `WindowSpec`, `ChildAggregate`; `calc.ts`)
can already say, read 2026-10-14:

- **Row level**: any Pure expression over the source row, typed by the compiler. A time grain is
  `$x.tradeDate->firstDayOfMonth()` (and `firstDayOfWeek`, `firstDayOfQuarter`, `firstDayOfYear`, `year`,
  `monthNumber`, `quarterNumber`, `dayOfWeekNumber` -- every one a native of the compiler); bins are
  `floor($x.notional / 1000000) * 1000000`.
- **Window**: a running or moving sum, average, min, max, count (a partition, an order, a frame of the last N rows); rank,
  dense rank, row number, percent rank, cumulative distribution, ntile; lag and lead; first and last value. That is
  running totals, moving averages, rank (a bump chart), the period before (lag), an ECDF, a Pareto's cumulative share.
- **Group level**: an expression over the grouped measures (`$x.profit / $x.revenue`); windows at this level run over
  the grouped rows, so a running total runs along a chart's categories.

The gaps, and what each step closes:

1. **A chart cannot plot a group-level column today**: `chartSnapshot` drops `groupDerived` and the windows, and a
   chart's measure is a column and an aggregate only. Version 2's `fn: 'value'` plots one, and the chart's query keeps
   the group-level columns it plots (with the measures they read, under the names they read them by).
2. **Presets**, one library (`calc-presets.ts`) turning a choice into ordinary calculated columns of the existing
   types: **grain** (day, week, month, quarter, year), **bins** (a width, or one from the column's range), and **show
   as** -- running total, % of total, difference from the previous, % change from the previous, moving average over N,
   rank. Nothing new is queried: a preset IS a `DerivedColumn` the planner already plans.
3. **Where a chart's calculation lives (option (a), the user's choice):** in the chart's spec -- the grid stays as the
   person left it; a button promotes it into the grid. A preset is kept as its WORDS (`x: {column, grain: 'month'}`,
   `showAs: {kind: 'runningTotal'}`) and expanded to calculated columns by `chart-spec.ts` when the query is built, so
   Python writes the same words and never a copy of the expansion (one-platform rule 1: DataCube's semantics are
   DataCube's TypeScript); a calculation in Pure is a `calcs[]` entry, as the grid keeps one.
4. **The grid gets the presets too**: its column header menu (beside "Extend Column X…") offers grain, bins and show-as,
   each adding the columns to the cube, editable afterwards as Pure; a new column may start from a preset.
5. **The editor's vocabulary** (`CALC_FUNCTIONS`) gains the date functions, each with an example the build compiles.
6. **Batch 4's aggregates** -- `percentile` (quartiles), `corr`, `covarSample`/`covarPopulation`, `maxBy`/`minBy`
   (first and last by time) -- are natives of the compiler already; DataCube's `AggregateFn` gains them (Python takes
   DataCube's; the quartiles are built, the two-column ones are not -- see Batch 4). A trend line needs no regression function: its slope is the covariance over the variance, its intercept
   the mean of y less the slope times the mean of x -- two aggregates and a group-level column.

#### 3.3 An example page per batch

Each batch ships a saved page of examples, simple to complex, one sheet (tab) per family: opened from the Open window
("Examples"), over a sample DataCube makes itself (`samples.ts`, from a fixed seed: a page names its sample and opening
rebuilds it, as a saved page over a sample does today), so it needs no file and no network. Each page is BUILT by
TypeScript (`src/examples/`, its own lazily loaded chunk) rather than hand-written JSON, so it moves with the spec; a
node test reads each page back and finds every chart drawable (`chartProblems` empty), and the browser test opens it
and draws every tile. A sample with what a dashboard needs: `sales` -- a date, a region, a country (its ISO code and
name), a city with latitude and longitude, a channel, a category, a product, units, revenue, cost, profit, a target, a
discount -- and, for batch 4, `prices` (a symbol's daily open, high, low, close and volume).

### Batch 2: the charts made dashboard-grade

Each step a commit of its own, in this order:

0. This note, and the branch announced in `docs/IN_FLIGHT.md`.
1. **The spec's version 2** (§3.1): the slots, the reader (version 1 read unchanged), Python's words
   (`page.py`, `python-vocabulary.test.ts`). *(Amended 2026-10-16: Python's words are DataCube's reader's alone,
   asked headless; the list and its test are gone, PR neema2/legend-lite#47.)*
2. **A chart plots group-level columns and windows** (§3.2 gap 1).
3. **The presets** (`calc-presets.ts`): grain, bins, show as, used by the chart's editor; the date functions in the
   editor's vocabulary.
4. **The grid's presets**: its header menu and the column editor's start (§3.2 item 4).
5. **Bar and line together**: a kind per measure; one value axis when the measures share a unit, two when they do not,
   each labelled in its series' colour (§6, decided).
6. **The grid's own number formats** on every axis, label and tooltip (currency, percent, thousands and millions), from
   the column's format in the grid, so a chart never says 15054092 where the grid says $15.1M.
7. **Reference lines and colour by value**: a constant, a target, the average, minimum or maximum as a line (the
   database's figure: a group-level window over every row of the chart -- before its top N, across every series); a
   shaded band or period; the highest and lowest pinned; a series coloured by thresholds (red below target). Not on a
   100% stack (its axis is shares), refused by name. The median comes with batch 4's percentile.
8. **Label and tooltip**: a hover lists every measure asked for; a label from any column.
9. **A real time axis, zoom and pan**: a date across is a time scale (gaps are gaps), a slider under a long series, and
   point thinning (ECharts' sampling) so a long series stays quick.
10. **Drill-down and drill-through**: a double-click on a bar goes one level down the grid's own row groupings (region,
    then desk), with the path shown and a way back up -- only from a column the grid groups by, never from a period or
    a bin; drill-through is a click, which filters the grid to the rows behind the mark.
11. **Brush to filter**: drag across a date range, or around a region of a scatter, to filter the chart's own grid --
    today's click-to-filter for a range (two conditions, from and to; four on a scatter's two columns), taken by value
    whatever order the chart draws in. A period's or a bin's end is the DATABASE's (its `adjust()`, its next edge:
    chart-spec.ts AXIS_UPPER), compared, never computed in the browser.
12. **The cheaper options**: smooth and step lines, a log axis, axis breaks, label position and rotation, dimming the
    rest on hover, hiding a series from its legend (ECharts' own: a click on its legend entry), pattern fills for
    readers who cannot tell colours apart (ECharts' decal), switching a chart's kind in place (the toolbox's bars or
    line, kept), the toolbox (save as a picture, the chart's data, its CSV), and a crosshair shared across a page's
    charts of one column across -- the tooltip relayed, nothing else (ECharts' `connect` would pass on a brush or a zoom
    too). The options form leaves startup: a chunk of its own (ui/chart-form.ts), fetched when a chart's options are
    first shown.
13. **The notebook widget fetches its chart code lazily** (today ECharts is inside its one module, budget 461,746 bytes
    gzipped, and grows with every piece registered): measured before and after, and asked of the user before it is
    built, since it changes the widget's build. **Done** (the user said yes, 2026-10-14): the module is built with lazy
    chunks (`widget-chunks/`); the loader fetches each over the kernel's channel the first time the cube needs it and
    imports it from a blob URL with its chunk specifiers rewritten (a static import to the chunk's own URL, a dynamic
    one to the loader's fetch). What a cube fetches before it shows went from 604,278 bytes gzipped to 281,797; the
    chart's code and ECharts come with the first chart. Python's hash of the module covers the chunks.
14. **Its example page** (§3.3): sheets for combinations, reference lines and thresholds, time and zoom, drill and brush,
    formats and labels, the options.

### Batch 3: new kinds over the query DataCube already runs

Drawing work over rows the database already groups, using version 2's slots and the presets; the rarer kinds in chunks
of their own, fetched with their first chart:

- **Comparing**: bubble (a scatter sized by a measure), waterfall (a profit-and-loss bridge), Pareto (bars and their
  cumulative share, the running-total preset), lollipop, dumbbell (two measures per category), diverging bar, bullet,
  polar bar and rose, radial bar.
- **Over time**: stacked area and streamgraph (themeRiver), a range band (min to max), slope chart, bump chart (the rank
  preset), calendar heatmap, a sparkline grid, Gantt and timeline (a start and an end per row).
- **Part of a whole**: funnel, half donut, two-ring donut, sunburst (the row groups as rings), Marimekko, waffle.
- **Flow and hierarchy**: Sankey (two groupings as from and to, a measure as the flow), chord, tree and org chart from
  the row groupings.
- **One value**: gauge, progress ring (against the KPI's target).
- **Many measures**: radar (one shape per value, a measure per spoke), parallel coordinates, a scatter matrix.
- **Layout**: small multiples on shared axes (`facet`), and the matrix (a chart per cell).
- **Its example page**: a sheet per family.

**Step 1, built 2026-10-14** (stacked on batch 2, to land with it): seventeen kinds -- lollipop, dumbbell, range,
waterfall, Pareto, funnel, rose, polar bar and radial bars, radar, parallel coordinates, Sankey, chord, streamgraph,
calendar, gauge and progress ring, sunburst and tree (the grid as shown, as the treemap) -- and the pie in two rings
(its split outside) and as half a ring; a line of ranks is a bump chart (its axis runs from the first), a line by year a
slope chart. The database computes each kind's own figures: a waterfall's starts and ends (a running total along the
chart's own order, which now breaks ties by the category so the window and the rows agree), a Pareto's cumulative share,
a two-ring pie's inner totals (calc-presets.ts); the page only places them. Every kind refuses by name what it cannot
draw (chart-spec.ts chartProblems; Python's `_drawable` the same). The drawing (chart-kinds.ts) and the rarer ECharts
charts (chart-render-kinds.ts) are chunks of their own, fetched with the first such chart: ECharts is now registered a
chart at a time (`echarts/lib/...`), since the `echarts/charts` index imported by both chunks put every chart in the
first chart's download. Its example page, "Charts: more kinds", has a sheet per family; each chart's query is planned
and run on DuckDB (chart-queries.test.ts) and every sheet drawn in a browser (verify_charts). A grid's tree (treemap,
sunburst, tree) is now valued by its first MEASURE, not its first number column (an id is empty on a group's row).

**Step 2, built 2026-10-14**: the bubble -- a scatter's `size`, each point sized between the least and the most its
rows say (ECharts' visual map places them; a point with no size is not drawn, and counted) -- and small multiples
(`facet`: a chart per value of a column, a row of them, or a matrix of both) of bars, lines and areas, which make the
sparkline grid too. The query groups by the facet's columns and asks for each panel's most times the most panels drawn
(12); every window is a panel's own (a share is of the panel's total, an average line the panel's average). Each panel
is drawn by the chart's own drawing from its rows and laid out as one picture: the value axes shared (each runs to the
most any panel shows, found by comparing; a stack's panels keep their own scale, said), a series the same colour and
in the same place in every panel, one legend, one hover, a click saying the panel's facet values too. Refused: a facet
on another kind, with a brush or a slider, or coloured by value.

**Step 3, built 2026-10-14**: the scatter matrix (`matrix`): two to five values, each pair plotted against each other
from the cube's rows (as a scatter, not groups), each value's name on the diagonal, a colour per value of the split.

Batch 3 is complete but for the kinds named above as not planned yet (Marimekko, waffle, Gantt) -- **built
2026-10-18** (the roadmap run's phase 7, `docs/DATACUBE_ROADMAP_RUN_2026_10_18.md`), each with the layout the database
computes:

- **A Marimekko** (`mark: 'marimekko'`, its split needed): each column across as wide as its share of the whole, each
  cell up it as tall as its share of the column. The database gives each cell its four edges, shares between 0 and 1
  (`calc-presets.ts marimekkoColumns`: a running total through the cell less the running total within its column, over
  the whole, is where its column starts; that plus the column's total, where it ends; the running total within the
  column, over the column's total, where the cell ends) -- in the order across, which it must be ordered by.
- **A waffle** (`mark: 'waffle'`): a hundred cells, ten by ten, each category the cells from where the category before
  it ended (the database's lag of the same column) to its running share through it, rounded by the database
  (`waffleColumns`) -- a running total rounded, so the ranges meet and add up to a hundred exactly. The chart colours
  each cell by the range that holds it.
- **A Gantt** (`mark: 'gantt'`): each row a bar from a start to an end on a time axis -- two values, each the min or max
  of a date column (the database's least and greatest), run from the earliest start.

Each draws one value (a Gantt two), takes no show-as (it computes its own figures), and is on the "more kinds" example
page with its family. Their audit: a whole of nothing gives empty shares (never a division by zero, which failed the
query), said by the chart; a waffle or a Marimekko of values below zero says so rather than drawing a wrong whole; a
Marimekko cell knows its category (a click on a period filters to it) and its columns are named as the axis names a
period; a Gantt takes dates or instants, never a time of day, and says a row that ends before it starts; the form shows
none of the options these kinds do not draw.

**Its audit (2026-10-14)** found and fixed: a live chart of one series (a waterfall, a lollipop) took the cube's
second grouping as its split and was refused with no way to take it off -- following the cube, such a kind takes no
split, and a kind of several values (a dumbbell, a radar) keeps its own; a split polar bar's hover read the first
series' row; a matrix of small multiples shifted its panels after an empty pair; a calendar of a date-time without the
day grain; a waterfall cut by "At most" called its last bar the total ("Total shown" now); Python's refusals caught up
with DataCube's; the notebook's loader asks the next open cube's channel when one does not answer in 30 seconds.
**A saved pie with a split changes look**: it was drawn as one ring from each category's first row (a split silently
dropped); it is now two rings, the split outside. `options.half` and `options.ring` are checked words (true or false);
a DataCube from before batch 3 draws a full pie or a dial for them, as it reads version 2. Named then, not planned yet: Marimekko and waffle (each needs its shares laid out as cells, a layout
to design so the page does not compute), Gantt (a start and an end per row: dates as measures) -- built since, above.

**The rules written once (2026-10-15, before batches 2 and 3 landed; the review of 2d24022).** The DataCube + Python
line's review found Python's chart rules (`page.py _drawable`) a copy of `chartProblems`, held to it only by the shared
cases, and drifted: a Windows fix to the calendar's check had changed DataCube's side alone. With the user: one
rulebook, DataCube's, which Python asks. `readChart` is the one reader of a saved chart (the page's, and Python's).
`editChart` is the one change by hand: every field of the panel's options form goes through it, and so does Python
(`chartFrom`: DataCube's own new chart, `defaultChart`, or the chart as it was, changed by `editChart`). In it a slot
told as null is taken away, a kind takes what it needs and leaves what it has no place for (`asMark`, once the form's
own), a new column across takes its own order and its grain or bins go with the old one (`acrossChanged`, shared with
`followCube`, so a Pareto keeps its order on a date either way), and a hand on what `followCube` would set again
freezes a live chart (`freezesOn`: a KPI's measure, but not its trend; nothing of a chart of rows). The headless
bundle's `gridChart` judges that chart, and keeps it, as the panel draws and keeps it -- a live chart regrouped by its
grid -- against the grid's columns as the compiler types them now, calculated ones included (step 0). Python keeps
what it makes, or refuses in DataCube's words; it passes what it is told as given, turning only its `(column, fn)`
shorthand into a measure. Rules only Python had (no weighted average on a chart; bins a positive number, a count a whole
one; a measure's and a facet's keys) moved into DataCube, whose reader now refuses any key a chart has no place for (a
slot, an option, a measure's, a facet's, a layer's by its kind, bins', an order's, a drill's, a calculated column's) by
name, as it refuses a word it does not know. A chart told its grouping and told to follow the grid (Python's
`frozen=False`) is refused where the grid would replace what it was told (`followRefusal`): the panel never meets it, as
a hand on the grouping freezes the chart. A chart of the grid as shown (treemap, sunburst, tree) asks nothing
across and nothing to plot, but a slot it does not draw (a layer, a facet, a grain) is refused, and the panel now asks
before drawing one too. Sharing the rule found two bugs of the panel's: changing the column across kept the old
column's order (a date ran from the largest), and a live chart with a split was taken for regrouped on every refresh
(the regrouped copy's keys came in another order), so its form was painted again each time. To make DataCube's code
always there to ask, the package is one package, two things: the wheel alone the compiler; its `datacube` extra
everything DataCube needs, V8 included (the headless design's §10, extended).
The audit of the other twins is `docs/PYTHON_ASKS_DATACUBE_2026_10_15.md`.

### Batch 4: new aggregates, and the kinds they unlock

The rule holds -- the database computes, the chart draws. The aggregates (§3.2 item 6) join `AggregateFn` (Python
takes DataCube's); then:

**Built (2026-10-18, the roadmap run's phase 5, `docs/DATACUBE_ROADMAP_RUN_2026_10_18.md`):** the quartiles
`percentile25` and `percentile75` (Pure's `percentile(0.25)` and `(0.75)`, continuous; offered on every number column by
the compiler's own answers, `generated/offer-facts.ts`) as aggregates of the grid and of a chart; the **box plot**
(`mark: 'boxplot'`): one number column's five figures -- `min`, `percentile25`, `median`, `percentile75`, `max`, in that
order (`BOX_FNS`) -- per category, drawn by ECharts' boxplot series, ordered by its median, drillable, one series; a
chart told the kind takes its first value's column's five figures (`editChart`), and leaving it for a kind of one value
keeps its median. The histogram needed nothing new -- the bins preset with a count, as batch 2 built it -- and has an
example of its own beside the box plots, on the Statistics sheet of the "more kinds" example page. The phase's audit
also made a click on a batch 3 kind's period or bin (a range or a box plot by month, a calendar's day) filter to its
range, and a pie, a lollipop, a KPI and a heatmap refuse a second value by name (they draw their first alone). Python writes a box plot as any chart
(`grid.chart('boxplot', x=...)`), and an Integer's quartile -- a `Number` the database may answer as an exact decimal --
reaches `GridView` as a `Decimal`.

**Not built (left for a later pass):** `corr`, `covarSample`/`covarPopulation` and `maxBy`/`minBy` read a SECOND column
per measure (a measure names one; `wavg`'s weight is the only second column a measure has), so the correlation
heatmap, the trend line and the candlestick wait on a measure with two columns; the candlestick also on the `prices`
sample. Percentile lines need a percentile window (a new `WindowFunction`, in the window form and Python's words); the
ECDF a `cumeDist` show-as; the violin, error bars and hexbin were not started (the KPI change was built 2026-10-23, the
lag window below, §3 item 3).

| Kind | What the query computes |
|---|---|
| Histogram | the bins preset as the grouping, a count |
| Box plot | quartiles (`percentile` at 0.25, 0.5, 0.75), min, max |
| Violin | a density: fine bins and a count, per group |
| ECDF | the cumulative distribution window (`cumeDist`) |
| Candlestick, OHLC | open and close (`minBy`/`maxBy` by time), high and low, per period |
| Error bars | the standard deviation (an aggregate DataCube has) |
| Percentile lines | `percentile` over every row: one more query, as the average line is |
| Correlation heatmap | `corr` for each pair of measures |
| Hexbin density | two bins presets and a count |
| Trend line, forecast | slope and intercept from `covarPopulation`, `variancePopulation` and two means; the forecast extends the line |
| KPI change | the same measure for the period before (the lag window) |

And its example page.

### Batch 5: maps (§4), with its example page

**Built (2026-10-18, the roadmap run's phase 6, `docs/DATACUBE_ROADMAP_RUN_2026_10_18.md`):** two kinds on Natural
Earth's 1:110m countries, in the Equal Earth projection, zoomed and panned. A **map** (`mark: 'map'`) colours each
country its column across names -- an ISO code exactly (alpha-3 or alpha-2), else one of Natural Earth's names, case
aside -- by one value, and says the values it found no country for, and a country two rows named (the first drawn). A
**bubble map** (`mark: 'bubblemap'`) puts a bubble at each place its column across names, where a new slot says --
`at: {lat, lon}`, two number columns -- sized by one value; its query groups by the place's name, latitude and
longitude (never an average of positions), and a row with no position is said. Both draw one series of one value,
ask for 250 places (`MAP_LIMIT`), and are placed by their own columns: they do not follow the grid's grouping. A click
filters to the place, as a bar's does. Drawn by `chart-maps.ts`, the shapes in its chunk; ECharts' map chart and its
geo component are a chunk of their own (`chart-render-maps.ts`, handed the shapes to register); both are fetched with
the first map. Its audit: a map is not offered Open in grid (Update would not change its places) and its pill says it
does not regroup; its limit stays the world's whatever is across, and a chart leaving it takes its own default again; a
map has no grain or bins, and a bubble map's name is not one of its places; a place with no value is said. Python writes them as any chart
(`grid.chart('map', x=...)`, `grid.chart('bubblemap', x=..., at={'lat': ..., 'lon': ...})`). On the Maps sheet of the
"more kinds" example page.

**Not built (left for a later pass):** provinces and states (one file per country, item 1); the borders as each of
31 countries draws them (item 4: Natural Earth's views, a deployment's default and a user's setting); the US with
Alaska and Hawaii set in (item 5); a user's own shapes (item 6).

### After the batches

Program step F1 rebuilds page filters and cross-filtering on classes (its own line); this work's clicks, brushes and
drills filter a chart's own grid, built to feed F1 when it lands. Left for later: network graphs, bar races and
timelines (storytelling, of little use in BI), word clouds (a third-party plugin), 3D. Not adopted: ECharts' own data
transforms (filter, sort, aggregate in the chart), since the database does that.

## 4. Maps (agreed)

What a map is in practice: regions coloured by a measure (revenue by country, headcount by state: a choropleth), and
bubbles placed by latitude and longitude. Both offline: everything a map draws ships with DataCube.

1. **The shapes** are Natural Earth's (public domain), prepared at build time and fetched by the browser only when a
   map draws. Sizes as published (measured 2026-10-09, `github.com/nvkelso/natural-earth-vector`, `geojson/`):

   | Shapes | Published | What a browser fetches |
   |---|---|---|
   | World countries, 1:110m | 0.8 MB | one file, about 100-200 KB once simplified and compressed |
   | World countries, 1:50m | 3.1 MB | not needed for a tile |
   | States and provinces, the whole world (1:10m) | 41 MB | never as one: one file per country |
   | One country's states or provinces | (part of the 41 MB) | tens to a few hundred KB, only for the country drawn |

2. **Prepared at build time**: simplified to a tile's size and split by country, keeping the borders neighbouring
   regions share identical (simplified one by one, neighbours part and leave slivers between them). The tool is open
   (§6, open 2); not Node.
3. **Matching the data to the shapes**: a column of ISO codes ("USA", "GBR") matches exactly; names ("United States",
   "US") through Natural Earth's own name fields. Rows that match no region are said ("3 regions not found: ..."), as
   the treemap says what it leaves out.
4. **Borders by who is looking**: Natural Earth publishes country borders as 31 countries draw them (Argentina,
   Bangladesh, Brazil, China, Egypt, France, Germany, Greece, India, Indonesia, Israel, Italy, Japan, Korea, Morocco,
   Nepal, the Netherlands, Pakistan, Palestine, Poland, Portugal, Russia, Saudi Arabia, Spain, Sweden, Taiwan, Turkey,
   Ukraine, the UK, the US, Vietnam), and an ISO view. Each is prepared the same way and fetched only when used. The
   deployment sets the default; a user may choose another in settings; a deployment may let it follow the browser's
   region setting (an India-English browser, India's view) -- the browser's setting, never where the person is. A page
   shared draws in the viewer's view, not its author's, and a map says which view it draws. The views are of country
   borders; the provinces come in one version, by who controls an area.
5. **Projections**: the world in a projection that keeps areas honest; the US with Alaska and Hawaii set in below.
6. **Bubbles by latitude and longitude** on the same shapes, and **your own shapes** (sales territories, a floor
   plan) from a file the user gives.
7. **Not built**: street-level maps (background tiles from a tile server on the internet, with its limits and fees).
8. A click on a region filters, as a click on a bar does; a map zooms and pans.

## 5. Across every batch

- **The spec stays ours, plain JSON** (the dashboards design §6). A new kind is a new `mark`; an older DataCube
  opening a page with a kind it does not know refuses it by name (`page-document.ts`'s reader already does). New page
  parts (filters, links) move the page document's version, read back from every version before it.
- **Download**: the chart chunk gets a budget of its own beside the grid's (`bundle-budget.test.ts`), raised on purpose
  with each batch, and the rarer kinds -- maps first -- go in chunks of their own, fetched with their first chart.
- **Accessible**: each kind says what it shows in words (`describe`), and pattern fills are one option away.
- **Tests**: each kind's option tested in node, drawn as SVG (`chart-option` tests); each batch's interactions in
  Chromium (`verify_*`); Python's words read against DataCube's (`python-vocabulary.test.ts`; *amended 2026-10-16:
  retired with Python's lists, DataCube's reader judging each word Python sends, PR neema2/legend-lite#47*).
- **Examples**: each batch's example page (§3.3), every chart on it read back drawable in node and drawn in Chromium.

## 6. Decided, and open

**Decided (the user, 2026-10-09):** the work happens batch by batch, this note landing with the first ("we should do
everything on branch here -- definitely start"); and a second value axis, as recommended ("yes on 2 axis"): only for a
bar-and-line chart whose two measures differ in unit, each axis labelled in its series' colour, one axis otherwise --
`chart-option.ts`'s rule changes to say so.

**Decided (the user, 2026-10-14):** batches 2-5 widened to every chart we can realistically draw (§3); the spec's
version 2, a slot per role (§3.1); calculations from the calculated columns DataCube already has, packaged as presets,
not new query machinery (§3.2); a chart's own calculations kept in its spec, the grid untouched (option (a)); the
presets shared by the chart and the grid; an example page per batch, a sheet per family (§3.3). Batch 1's page filters
and cross-filtering are not ported from the old `datacube-charts` branch: program step F1 rebuilds them on classes.
Batch 2 is built on the session branch `claude/epic-rubin-ay7upl`, from main.

**Decided (the user, 2026-10-23: "frozen charts ... don't freeze filters, seems to only freeze pivots"):** a chart has
three states, one word each, set from its pill's menu (`chart-spec.ts` `chartState`, Python's `chart.state`):
**Dynamic** follows its grid -- its grouping, its filters and the page's clicks; **Pinned** keeps its own grouping (a
hand on its axes pins it, as "frozen" did before) and still follows the grid's filters and clicks; **Frozen** keeps its
grouping and the grid's filter as it was when frozen (`ChartSpec.kept`), so a filter put on the grid after, or a click
on another chart, leaves it as it was. A kept filter naming a column the cube has lost is said
(`chartProblems`), never dropped. The same day, one gallery page of every kind (`examples/charts-gallery.ts`) leads the
Open window's examples, each sheet fitting its window.

**Open:**

1. **Regions beyond countries and their provinces** (UK regions, EU regions): not planned; Natural Earth's provinces
   cover most countries, and a user's own shapes cover the rest.
2. **The map-preparation tool**: a pinned Python tool run at build time, or files prepared once and committed with how
   they were made. To be chosen when batch 5 starts, with the measured sizes. **Taken for now, for the user to confirm
   (the roadmap run's phase 6):** prepared once and committed -- `datacube/tools/maps/prepare.py` (standard library
   only) reads the published 1:110m countries of release 5.1.2, its sha256 pinned, keeps the geometry with every
   coordinate rounded to 2 decimals (a shared border's vertices stay the same on both sides: no simplification, so no
   slivers) and five fields, and writes `datacube/src/maps/world-110m.ts`: 183 KB, 60 KB gzipped, fetched only with
   the first map. A build-time run would need Natural Earth's file at build time, which a cloud session's network
   may refuse; the committed file needs nothing. Provinces (41 MB published) will need simplification, and so a real
   tool, when they come.
3. **The notebook widget's chart code fetched lazily** (batch 2 item 13): measured, asked, built (2026-10-14).
