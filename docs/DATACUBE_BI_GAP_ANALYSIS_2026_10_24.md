# DataCube against Power BI and Tableau: a gap analysis (2026-10-24)

**Asked by the user (2026-10-24):** "gap analysis of features in DataCube vs Power BI/Tableau". This is an analysis, not
a plan: it says where DataCube stands, what it lacks, what it already does better, and an order to close the gaps in.
The plan it feeds is `docs/BI_AND_ETL_PLAN_2026_09_29.md` (tracks A, B and F); where a gap is already one of that plan's
items, the item is named.

**How it was read.** DataCube's column is from the code on `main` at 819a2e81a (each row names the file, or the doc
that records it). The Power BI and Tableau columns are their generally available features as of mid-2026: Power BI
Desktop and the Power BI service (Fabric), Tableau Desktop, Tableau Cloud/Server, Tableau Prep and Tableau Pulse.
Preview features and partner add-ons are left out unless named.

**Statuses:** **Have** (built, tested), **Partial** (built, but short of what users of the incumbents expect),
**Missing**, **Won't** (deliberately not copied: the reason is given). **Size** is a rough effort for DataCube: S (days),
M (one or two weeks), L (a month), XL (more).

---

## 1. The short version

**Where DataCube is strong.** Its grid is a real pivot whose every figure the database computes: DuckDB in the
browser, Postgres through the warehouse, and the database a Legend model maps to, in the SQL the compiler renders for
it (DuckDB, Postgres, Snowflake, BigQuery, Databricks, Trino, ClickHouse, MySQL/MariaDB, SQLite, H2). Charts follow the grid as it is pivoted, or keep a grouping of their own, and Show Grid takes any chart
back to a grid. Pages hold any number of grids and charts across sheets. Thirty-two chart kinds, with presets that are
dashboard-grade. A Python API that drives exactly what the screen drives. A governed semantic model (Pure classes,
mappings, data spaces) reviewed and versioned like code. All of it can run entirely in a browser, with no server.

**Where it is behind.** Five gaps decide whether a Power BI or Tableau user can switch:

1. **Interactivity on a page:** slicer and parameter controls, drill-through to a detail page, tooltips that show a
   chart, bookmarks. Today a page filters only by a chart's click and by filters on class properties.
2. **Tile kinds:** a page has grids and charts only. No text, title, image, button or filter-control tile.
3. **Share and govern as a service:** folders, permissions, version history, comments, embedding. Pages are files and
   links today.
4. **Operate:** scheduled delivery (PDF or Excel at 8am), data alerts, caching, usage metrics.
5. **Augmented analytics:** trend lines, forecasting, clustering, "explain this change", natural-language questions.

**The order recommended (§4):** page interactivity and tile kinds first (they make a dashboard feel like a
dashboard), then grid formatting (data bars, icons, in-cell sparklines), then the analytics layer, then the service
(share, schedule, alert), then AI. Each tier stands on the one before.

---

## 2. The comparison, area by area

### 2.1 Connect to data

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Files (CSV, Excel, Parquet, JSON) | Yes | Yes | **Have** | Files, and remote files by URL (`cube-document.ts`, sources `file`, `remoteFile`) |
| Databases, live | DirectQuery | Live connection | **Have** | Every query is SQL the database runs: the tab's DuckDB, Postgres through the warehouse (`docs/DATACUBE_ON_POSTGRES.md`), a class's own database through its mapping |
| Extracts (a copy for speed) | Import mode | Hyper extracts | **Have** | **Snap**: a grid's rows copied into the tab's DuckDB, Live again on demand |
| Semantic model / published data source | Semantic models | Published data sources | **Have** | Pure classes and mappings; a grid over a class (`class`), a saved query (`savedQuery`), a data space |
| Warehouse tables with grants | (via Fabric) | (via connections) | **Have** | `warehouseTable` sources; the warehouse's grant model |
| Python data frames | Python visuals only | TabPy only | **Have** | `frame` sources from Python; a page is drivable from Python |
| Joins between sources | Relationships, merges | Relationships, joins, blending | **Partial** | Joins by key with a match summary (`join.ts`); no star-schema relationship model across many tables (the class model is that, for class sources) |
| Composite and mixed models | Composite models | Blending | **Partial** | A page holds grids over different sources; they do not filter one another unless both are class sources (page filters) |
| Incremental refresh, aggregations | Yes | Incremental extracts | **Missing** | Snap is all-or-nothing. Size M (needs plan F3's scheduler) |

### 2.2 Model and calculate

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Calculated columns | DAX | Calculated fields | **Have** | Pure expressions, row-level and group-level (`derived`, `groupDerived`), with an editor and presets |
| Measures with a format | DAX measures | Measures | **Have** | Per-column aggregation and format (Column Properties); currency, percent, scale |
| Context-independent aggregates | `CALCULATE`, `ALL` | LOD (`FIXED`, `INCLUDE`, `EXCLUDE`) | **Partial** | Group-level columns and show-as windows cover "% of total" and running totals; no general FIXED/EXCLUDE expression. Size L |
| Table calculations | Visual calculations | Running total, rank, % of total, moving average, difference | **Have** | Show-as presets, computed by the database (`calc-presets.ts`): running total, % of total, difference, % change, moving average, rank |
| Hierarchies and drill paths | Yes | Yes | **Partial** | A chart drills through the grid's row groups; no named hierarchy declared in the model (plan B4). Size M |
| Bins, date grains | Yes | Yes | **Have** | Bins by width or count, day/week/month/quarter/year, computed by the database |
| Parameters, what-if | What-if and field parameters | Parameters, parameter actions | **Missing** | Plan B3. Size L (bound into queries as Pure parameters) |
| Groups and sets | Groups | Groups, sets, set actions | **Missing** | Size M |
| Calculation groups | Yes | No | **Missing** | Low priority |
| Metrics declared once, certified | Semantic model measures, endorsement | Pulse metrics, certified data sources | **Partial** | The model holds classes and derived properties; certified metrics with impact analysis are plan B4. Size L |

### 2.3 The grid (tables and matrices)

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Pivot: rows, columns, values | Matrix | Crosstab | **Have** | The core of DataCube: drag to group, pivot, aggregate per column |
| Subtotals and totals | Yes | Yes | **Have** | Group totals, a grand total, pivot totals as columns (with heatmaps and sorts of their own) |
| Sort, filter, search | Yes | Yes | **Have** | Header sorts with chains, a filter editor, quick Filter ▸ from a cell's right-click |
| Conditional formatting: colour scales | Yes | Yes | **Have** | Heatmaps per column, negative and zero colours, alternating rows (`config.ts`) |
| Conditional formatting: data bars, icon sets, rules | Yes | Partly | **Missing** | Size M |
| Sparklines in cells | Yes | (via workarounds) | **Missing** | Plan B1 lists them. Size M |
| Ad hoc analysis on raw rows | Yes | Yes | **Have** | Start Ad Hoc Analysis from the Dimensions tab |
| Drill to underlying rows | "See records" | View data | **Partial** | Show Grid takes a chart to its rows; no "show the rows behind this cell" in one click. Size S |
| Very large results | Paged | Paged | **Have** | The grid draws the rows in view; the database answers each level as it opens |

### 2.4 Visuals

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Core chart kinds | About 30 built in | About 24 in Show Me | **Have** | 32 kinds (`CHART_MARKS`): bars, lines, areas, scatter, pies, treemaps, sunbursts, Sankeys, chords, heatmaps, calendars, box plots, waterfalls, Paretos, funnels, gauges, KPIs, maps and more |
| Combo, dual axis, small multiples | Yes | Yes | **Have** | A measure's own mark and axis; facets by row and column |
| Reference lines and bands | Analytics pane | Analytics pane | **Have** | Lines, average/min/max, bands, thresholds, extremes, computed by the database |
| KPI cards | Card, KPI, new card | Big numbers | **Have** | Stat tiles with target, meter, period change and trend (2026-10-23) |
| Maps | Map, filled map, Azure Maps | Geocoded maps, layers, spatial files, density | **Partial** | Choropleth by country code and bubbles by latitude/longitude. No geocoding of names, layers, density or custom shapes. Size L |
| Trend lines, forecast, clustering | Analytics pane, forecast | Trend lines, forecast, clustering | **Missing** | The charts design doc lists the trend line (needs a two-column measure). Size L |
| Bullet, candlestick, error bars, violin | Some built in, more via AppSource | Some via workarounds | **Missing** | Candlestick and error bars wait on a two-column measure (charts design doc). Size M each |
| Decomposition tree, key influencers | Yes (AI visuals) | Explain Data | **Missing** | See §2.8 |
| Custom visuals | AppSource and an SDK | Extensions API | **Missing** | Size XL; low priority while 32 kinds stand |
| Viz in tooltip | Report page tooltips | Viz in tooltip | **Missing** | Size M |
| Annotations | Text boxes, shapes | Point and area annotations | **Missing** | Size M |
| Theming | Theme JSON | Workbook formatting | **Partial** | Light and dark themes from tokens, a reference palette validated for colour blindness; no user theme editor. Size S |
| Accessible charts | Yes | Yes | **Partial** | ARIA descriptions, a pattern fill option (decal), a data view in the toolbox; no keyboard navigation of marks. Size M |

### 2.5 Interactivity on a page

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Cross-filtering by a click | Yes | Filter actions | **Have** | A chart's click filters its grid (and every chart that follows it); page filters across grids over one class |
| Highlight rather than filter | Yes (default) | Highlight actions | **Partial** | Brushing filters; no highlight mode that dims the rest of other charts. Size M |
| Slicers and filter controls | Slicers | Filter cards | **Missing** | Filters exist in the grid and as page filters, but no control on the page to set them. The most visible gap. Size M |
| Drill down, up, through | All three | All three | **Partial** | Drill down and up in charts; no drill-through to a detail sheet with the clicked context carried. Size M |
| Bookmarks, stories | Bookmarks, buttons | Stories | **Missing** | A page's state is already a document of commands, so a bookmark is a saved state. Size M |
| URL actions, buttons, navigation | Buttons, page navigation | URL and go-to-sheet actions | **Missing** | Size S |
| Dynamic visibility | Bookmarks and selection pane | Dynamic zone visibility | **Missing** | Low priority |
| Undo and redo of every edit | Desktop only | Yes | **Have** | Every edit is a command; page-level undo |
| Cross-highlight across sources | Relationships | Blending | **Missing** | Plan B3's declared links between sources. Size L |

### 2.6 Pages and layout

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Several pages or sheets | Pages | Sheets, dashboards | **Have** | Sheets, each a layout of bands |
| Free and tiled layout | Free canvas | Tiled and floating | **Partial** | Bands, splits, arrange, fit to window, layout undo; no floating tiles. Size M |
| Text, title, image, shape tiles | Yes | Yes | **Missing** | A page has grid and chart tiles only (`page-document.ts` `PageView`). Size S to M |
| Phone and tablet layouts | Mobile layout | Device layouts | **Partial** | A narrow page stacks tiles; long press opens the right-click menu on touch; no layout authored per device. Size M |
| Templates and example pages | Template apps | Accelerators | **Have** | Example pages in the Open window: the chart gallery (with a KPI dashboard), chart examples, more kinds, joins |

### 2.7 Prepare data (Power Query, Tableau Prep)

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Join, union | Yes | Yes | **Partial** | Joins (left, inner, anti) with a match summary; no union. Size S |
| Pivot and unpivot columns | Yes | Yes | **Partial** | Pivoting is the grid's; unpivot needs a native (plan §8). Size M |
| Clean: split, trim, replace, fill, change type | Yes | Yes | **Partial** | Possible as calculated columns in Pure; no point-and-click step list. Size L |
| A flow of steps on a canvas | Power Query steps | Prep flows | **Missing** | Plan track C (C2, the canvas). Size XL |
| Write the result to a table | Dataflows | Prep outputs | **Missing** | Plan C1 (materialize a cube), F1. Size M |
| Create a model from data | No | No | **Have** | Create a Class, Association or Data Space from a grid, committed through SDLC review |

### 2.8 Augmented analytics and AI

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Natural-language questions | Q&A, Copilot | Tableau Agent, Pulse | **Missing** | A separate line (`nlq/`), not wired into DataCube. Size L to wire |
| Generated summaries and narratives | Smart narrative, Copilot | Pulse insights | **Missing** | Size M |
| Explain a change, key drivers | Key influencers, decomposition tree | Explain Data | **Missing** | "Why did P&L move" is plan B7. Size L |
| Anomaly detection | Yes | Pulse | **Missing** | Size M |

### 2.9 Share, collaborate, govern

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Save and open | Workspaces | Projects | **Partial** | Pages saved as documents, a saved-page store, open from a file or the store (`page-document.ts`) |
| Share a link | Yes | Yes | **Have** | A link that carries the cube (`share/link.ts`) |
| Folders, permissions, owners | Workspaces, roles | Projects, permissions | **Missing** | Plan B5. Size M |
| Version history | Fabric Git integration | Revision history | **Partial** | Models are versioned and reviewed in SDLC; pages are not. Size M |
| Comments | Yes | Yes | **Missing** | Size M |
| Row-level security | RLS, OLS | User filters, virtual connections | **Partial** | Live queries carry the viewer's database grants, so a viewer sees only what the database lets them read; no RLS in the model. Size L |
| Lineage and impact | Lineage view | Catalog | **Missing** | Plan F4 (lineage over the typed tree). Size L |
| Certification, endorsement | Yes | Yes | **Missing** | Plan B4. Size M |
| Embedding | Power BI Embedded | Embedding API | **Partial** | A notebook widget, and the headless API; no embed component with the host's sign-in. Size M |

### 2.10 Operate and deliver

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Export data | Excel, CSV | Crosstab, CSV | **Have** | CSV, Excel, HTML, PDF and text (`export*.ts`; plan track A made them correct) |
| Export a page | PDF, PowerPoint | PDF, image, PowerPoint | **Partial** | A chart saves as an image (toolbox); no whole-page PDF or PowerPoint. Size M |
| Paginated, pixel-perfect reports | Paginated reports | No | **Missing** | Low priority |
| Scheduled delivery | Subscriptions | Subscriptions | **Missing** | Plan B6 (needs F3, a scheduler). Size M |
| Data alerts | Alerts on cards | Data-driven alerts | **Missing** | Plan B6. Size M |
| Caching, refresh | Scheduled refresh, caching | Extract refresh | **Partial** | Snap holds a copy; no scheduled refresh or result cache. Size M |
| Usage metrics | Yes | Admin views | **Missing** | Size S |

### 2.11 Build on it (programmability)

| Capability | Power BI | Tableau | DataCube | Notes |
|---|---|---|---|---|
| Drive the tool from code | REST API, semantic link in notebooks | REST API, Hyper API | **Have** | `legend_lite` in Python: grids, charts, pages, Show Grid, saves, every gesture as a command, the same rules as the screen |
| In a notebook | Fabric notebooks | No | **Have** | The notebook widget, a live channel between the page and Python |
| The document as text | PBIP (preview to GA) | TWB (XML) | **Have** | Pages and cubes are JSON documents, written by DataCube's own writers |
| Open source | No | No | **Have** | The whole stack |

---

## 3. Where DataCube already does better

These are the reasons to choose DataCube even before the gaps close, and they should shape how the gaps are closed.

1. **The grid is the source.** A chart follows the grid as it is pivoted, keeps its own grouping when pinned or frozen,
   and Show Grid takes any chart back to the grid it reads, or makes one of its own. Neither incumbent has that round
   trip: their charts are built on shelves and never return to a table you can pivot.
2. **One model, governed like code.** Metrics and relationships live in Pure classes and mappings, reviewed and
   versioned through SDLC, and the same model serves Legend's other tools. Power BI and Tableau each keep their own
   semantic layer, edited in the tool.
3. **The database computes everything.** Bins, quantiles, running totals, reference lines, the KPI's period change:
   every figure is SQL the database runs. Numbers agree across tiles, exports and Python because there is one place
   they come from.
4. **Python is the same tool.** Every gesture is a command that the screen and Python apply through one function, with
   the same rules and refusals. Power BI's and Tableau's APIs manage content; they do not build an analysis.
5. **No server needed.** The planner and DuckDB run in the browser, so the playground, the notebook and a file-based
   page need nothing behind them.
6. **Many databases, one meaning.** A grid over a Legend class runs on whatever database its mapping names, because
   the compiler renders the one query in that database's SQL; the same model answers the same way on each.

---

## 4. Closing the gaps: a recommended order

Each tier is worth shipping on its own; each stands on the one before.

### Tier 1: a page that behaves like a dashboard (about six weeks)

| Gap | Size | Why first |
|---|---|---|
| **Filter controls** on the page: a dropdown, list, range or date control bound to a column, reaching every grid over the source (and, through declared links, other sources) | M | The most visible gap. Page filters exist underneath; this is their control |
| **Text, title and image tiles** | S | Every dashboard has a title and a note. A new `PageView` kind |
| **Drill-through** to a detail sheet, carrying the clicked mark's conditions | M | The second question users ask of a dashboard: "show me the detail behind this" |
| **Show the rows behind a cell or mark** in one click | S | Show Grid's sibling, for a single value |
| **Parameters** (a value or a list bound into queries) | L | Plan B3; makes what-if and "top N, N chosen by the reader" possible |
| **"Other" bucket** for a cut top-N | S | Today the chart says "the first N"; the incumbents show the rest as "Other" |

### Tier 2: a grid and charts that answer at a glance (about four weeks)

| Gap | Size |
|---|---|
| Data bars, icon sets and rule-based colours in grid cells | M |
| Sparklines in cells (a trend per row) | M |
| Highlight mode for clicks (dim the rest, do not filter) | M |
| Bookmarks: a named page state, a button to return to it | M |
| Viz in tooltip | M |
| Annotations on charts | M |

### Tier 3: the analytics layer (about six weeks)

| Gap | Size |
|---|---|
| A measure over two columns (`corr`, `covar`, `maxBy`/`minBy`), unlocking trend lines, candlesticks and error bars | M (core) |
| Trend lines and a simple forecast, computed by the database | L |
| Geocoded maps (names to places), map layers, density | L |
| Groups and sets | M |
| Context-independent aggregates (Tableau's FIXED/EXCLUDE) as Pure | L |

### Tier 4: the service (plan B5, B6; about two months, needs F3 and F5)

Folders, owners and permissions; page version history; comments; scheduled delivery as PDF or Excel; data alerts;
result caching; usage metrics; an embed component with the host's sign-in.

### Tier 5: augmented analytics

Natural-language questions through the `nlq/` line, generated summaries of a page, "explain this change", anomaly
flags on KPIs. Best built on Tier 3's analytics and plan B4's certified metrics, so the answers are the governed ones.

---

## 5. Deliberately not copied

| Incumbent feature | Why not |
|---|---|
| A second formula language (DAX, Tableau's calculation language) | Pure is the language: one meaning in the model, the grid, Python and every database. Presets give the common calculations a point-and-click form instead |
| A proprietary extract format (VertiPaq, Hyper) | Snap copies into DuckDB, a database the compiler already renders to; the copy answers the same queries |
| Charts computed in the client | Every figure is the database's (the execution tenet), so tiles, exports and Python agree |
| A marketplace of third-party visuals | A support and security burden; the chart spec makes a new kind a mapping, not a plug-in |
| Paginated reports | A different product; exports cover the need today |

---

## 6. Open questions for the user

1. **Who switches first?** Analysts who live in pivots (Excel, DataCube's heritage) need Tier 2 sooner; dashboard
   consumers need Tier 1 and Tier 4. Which audience leads?
2. **Filter controls across sources:** only within one source (simple, Tier 1), or through declared links between
   sources (plan B3, more work)?
3. **The service (Tier 4):** where it lives (beside the warehouse, as plan §9 recommends) decides how soon it can start.
4. **AI:** wire the `nlq/` line into DataCube now, or wait for certified metrics (plan B4) so its answers are governed?
