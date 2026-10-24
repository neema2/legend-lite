# DataCube against Power BI, Tableau and Excel: Pure as the semantic layer, and the program to catch up

Homework for the user's ask of 2026-10-24: *"build all the gaps to BI tools and Excel -- serious deep dive and serious
homework/research of everything we want to build to catch up to those tools -- remember that Pure is a semantic layer
language, so we should leverage that as a much better DAX thing that can be much more powerful -- you saw the start of
that with the grid from object v0 feature we built."*

Branch `claude/datacube-bi-parity`, on `claude/datacube-bi-gaps-graph` (which holds the area-by-area comparison,
`docs/DATACUBE_BI_GAP_ANALYSIS_2026_10_24.md`, and the graph brainstorm). This document does not repeat that
comparison: it adds what the comparison could not -- **what legend-lite's compiler supports today as a semantic layer,
with evidence**, the **design that makes Pure the semantic layer** (and where it beats DAX, and where it does not), the
**complete catalogue** of Excel and BI features with what each needs, and the **ordered program** this line builds.

Three research tracks fed it (2026-10-24): what core runs end to end today (tests and corpus cited); the incumbents'
current features (Power BI through its August 2026 notes, Tableau through "August 2026", Microsoft 365 Excel, and the
semantic layers -- dbt MetricFlow, Cube, LookML, Malloy, AtScale, Apache Ossie); and DataCube's grid against Excel,
feature by feature. Claims marked *unverified* were not confirmed from a primary source.

---

## 1. The thesis, in one table

Every BI tool has a semantic layer bolted onto a table store: relationships drawn between tables, a formula language
for measures (DAX, Tableau calculations, LookML's SQL snippets, MetricFlow's YAML), metadata in a side file. **Pure is
already a typed, object-oriented semantic language**, with the compiler that lowers it to SQL on about ten databases.
The concepts the incumbents bolted on are Pure's own constructs:

| The incumbents' concept | Where they have it | Pure's construct | legend-lite today (evidence: §2) | DataCube today |
|---|---|---|---|---|
| Table, column | everywhere | Class, property (typed, with multiplicity) | Supported | A grid over a class (`Class.all()->project`), to-one primitive properties only |
| Relationship, cardinality, filter direction | DAX relationships, LookML `relationship`, MetricFlow entities, Tableau relationships | **Association**, both ends named, multiplicities on each (`[0..1]`, `[*]`) | Supported: to-one LEFT JOIN, to-many EXISTS in filters, correlated aggregates, multi-hop | Not offered: the class tree (model plan step 5) |
| "No double counting across a fan-out" | LookML symmetric aggregates, Malloy aggregate locality, Tableau multi-fact | An aggregate over a to-many **end** (`$c.trades.notional->sum()`) lowers to a correlated aggregate, never a fan-out | Supported (`ResolveNavigationTest.java:691`) | Not offered |
| Calculated column | DAX calculated column, Tableau row-level calc | **Derived property** (`margin() { $this.pnl / $this.notional }`) | Supported, inlined (`DerivedProps.java:38`, `Typer.java:1362`) | Calculated columns typed in the grid; the model's derived properties not offered |
| Parameterized calculation | DAX UDF with parameters (GA June 2026), Tableau parameters | **Qualified property** (`grossAmountIn(fxRate: Float[1])`) | Supported (stress `06-trading.pure:69`); three corpus failures | Not offered |
| Measure | DAX measure, dbt metric, Cube measure, LookML measure | A **function over the instances** (`grossNotional(t: Trade[*]): Float[1] { $t.notional->sum() }`), stereotyped as a measure (§3.4) | Functions callable and inlined; no measure element; a user aggregate inside `groupBy` unproven | Measures are a column's aggregation, chosen per grid; nothing named in the model |
| User-defined function | DAX UDFs (no recursion, no overloads, no return type), Excel LAMBDA | **Function**, typed in and out, overloads, generics, higher-order (lambdas are values), tested | Supported, inlined so SQL sees through (`UserCallInliner.java:33`); recursion refused | Not offered |
| Calculation group (one transform, any measure) | DAX calculation groups, Excel Show Values As, Power BI visual calculations | A **function from a measure to a measure** -- or DataCube's own show-as presets, which already are one transform over any measure | Window functions supported (`WindowFunctionTest.java`) | Six show-as presets, ungrouped-by-pivot only |
| Time intelligence, calendars | DAX calendars (preview), MetricFlow time spine, Cube `time_shift`, Looker `period_over_period` | Date functions; **milestoning** for as-of (business, processing, bitemporal) | Date functions supported (`DuckDBIntegrationTest.java:2560-3486`); milestoning supported with known failures | Period grains (day..year); no YTD, prior period, calendars |
| Hierarchy | DAX hierarchies, Tableau hierarchies | Properties in order, marked by a **profile** (tagged values) | Profiles kept in the compiled model, readable as model JSON | Dimensions configured per grid (`Geography: region, desk`) |
| Format, description, folder, synonyms | TMDL properties, LookML `value_format`, `description` | **Tagged values** (`{doc.doc = '...'}`) | Kept; readable through `grammarToJson`; not queryable from Pure | DataCube writes `doc.doc`; reads none |
| Certified, approved | Power BI endorsement, Tableau certified, Pulse | **Stereotype** + **SDLC review** of the model project | Stereotypes kept; SDLC is Studio's line | Not shown |
| Data quality rules | Power Query profiling, dbt tests | **Constraints** on classes | Checked only by `graphFetchChecked`, defects computed in SQL | Not shown |
| Units, currency | Power BI dynamic format strings, AtScale | **Measure / Unit** types | **Not supported**: a unit-typed property is "Unknown type" | Currency as a column format only |
| Perspective, curated view | DAX perspectives, LookML explores, Cube views | **Data space** (execution contexts, its elements) | Parsed and checked; queried through `->from(mapping, runtime)` the client builds | Classes listed per project |
| Model as code, review, lineage | TMDL/PBIP, LookML, dbt YAML | `.pure` files in an SDLC project; lineage (F4) | Native | The page's project in the model home (§6 of the model design) |
| Pre-aggregation | Cube, Looker aggregate awareness, AtScale | **AggregationAware** mappings | Supported (`resolver/AggregationAwareRouting.java`) | Not used |

Where the incumbents are honestly ahead (and this design does not pretend otherwise):

- **Implicit context.** DAX evaluates a measure in whatever filter context a visual gives it, and `CALCULATE` edits
  that context -- powerful and notoriously hard to read. The Pure form (§3.4) makes the context **explicit**: a measure
  is a function of the set of instances in a cell. That is easier to read, test and compose, but a person writing
  "sales ignoring the region filter" writes it as a different grain (§3.8), not a context edit.
- **Interactive engine speed.** VertiPaq and Hyper answer a click in milliseconds from memory. legend-lite always asks
  the database; the answer for speed is DuckDB in the tab (Snap), AggregationAware routing, and caching (Tier 4).
- **Breadth of authoring UI.** Years of polish: format panes, field wells, Show Me. DataCube's command layer means each
  new control is one gesture, the same for Python, but the controls have to be built (§4).

---

## 2. What legend-lite's compiler supports today (evidence)

From the core research track. "Corpus" is legend-engine's core_relational suite run by `//spec:corpus_duckdb` (its
failures listed in `spec/src/test/resources/rcorpus/duckdb-fail-roster.txt`); "stress" is
`core/src/test/resources/stress/*.pure`, run by `StressServiceSuitesTest.java`.

| Feature | Status | Evidence | Known limits |
|---|---|---|---|
| Derived properties in project, filter, groupBy | Supported | Stress F30 (`94-fanout-services.pure:8-35`), F32 (`:78-100`); `StackRatchetWitnessTest.java:51-53,193` | No `orElse` (use `coalesce`, `LegendLiteGapTest.java:92`) |
| Qualified properties with parameters | Supported | `stress/06-trading.pure:69`, used at `94-fanout-services.pure:30-31`, through a hop at `:8210` | Corpus roster 61-62, 102; overload edge S42 (`SEMANTICS_REGISTER.md:65`) |
| Association navigation | Supported | `AssociationIntegrationTest.java:221,234` (to-one), `:315` (to-many EXISTS), `:295` (row expansion), `:601` (groupBy), `:469` and `ResolveNavigationTest.java:664,677` (multi-hop), `:691` (aggregate over to-many in project) | XStore and ModelJoin association mappings refused (`LegendLiteGapTest.java:70-77`); `count()` of an empty navigation is 0 (S22, deliberately not the engine's 1) |
| Milestoning | Supported | `ResolveTemporalContextTest.java:171` (`.all(date)` and a dated hop), `:194`, `:119` (bitemporal); `allVersions`, `allVersionsInRange` (`GetAllChecker.java:53`) | Roster 21, 26-29, 53, 85-86, 92 |
| Units of measure, currency | **Not supported** | A unit-typed property: "Unknown type" (`LegendLiteGapTest.java:54-63`) | `29-money.pure` excluded from stress |
| Constraints | Partial | `graphFetchChecked` only, defects in SQL (`GraphFetchCheckedIntegrationTest.java:115,205,290`) | Not in project/filter; pre/post-conditions never evaluated (S94); `###DataQualityValidation` parse-only |
| Enumerations, enumeration mappings | Supported | `EnumIntegrationTest.java:318,420,515,621,725` | An optional enum query parameter refused |
| Inheritance, `subType` | Supported | `InheritanceIntegrationTest.java:180,201`; `Substitution.java:879` | `subType` over a partial-membership navigation refused |
| Stereotypes, tagged values, profiles | Partial | Kept (`KnowledgeLayer.java:471`; `BazelSmokeTest.java:103-111`); read as model JSON (`PureV1Api.java:103`; Query reads `doc.doc` and a data space's `certified`) | Not queryable from Pure (no metamodel table) |
| User functions, query parameters | Supported | `UserFunctionIntegrationTest.java:711,821,883,898,1185`; parameters bound at run (`PureV1ApiTest.java:223`) | Optional enum, BYTE, LATEST_DATE, STRICT_TIME parameters refused |
| Relation API | Supported | windows, groupBy, pivot, `olapGroupBy`, `asOfJoin`, join, project, distinct, select, concatenate, rename (each with its checker test) | pivot and asOfJoin refused on some dialects, named |
| Aggregates | Supported | sum..max, stdDev/variance, median/percentile, joinStrings, mode; **two-column** corr, covar, wavg, maxBy, minBy (`TypeInferenceIntegrationTest.java:3604-3801`; `lowering/Aggregates.java:81-91`) | A two-column measure is a DataCube UI gap, not a compiler gap |
| Data spaces | Partial | Parsed, checked (`ModelIntegrity.java:112-117`); created by a route | No "query a data space" entry; the client builds `->from` |
| Graph fetch | Supported | `AssociationIntegrationTest.java:1760,1792` | m2m2r with a derived property (roster 3-4) |
| Measures in the model | Partial | A model function used as a group key (stress F32); AggregationAware routing (`stress/57-aggregation-store.pure`) | No measure element; a user aggregate inside `groupBy` (`y|$y->my::agg()`) untested |
| Date functions | Supported | now, today, firstDayOf{Week,Month,Quarter,Year}, dateDiff, adjust, datePart, timeBucket, month/quarterNumber | No `lastDayOfMonth`; no prior-period helper (build from `adjust` or `lag`) |

The five strongest: class queries with derived and qualified properties and correct to-one/to-many semantics, all in
one SQL statement; milestoning lowered to SQL; a broad Relation API with a rich aggregate set; user functions inlined so
SQL sees through them; one query rendered for about ten databases. The five gaps that matter for a semantic layer: no
measure element; no units; metadata not queryable; constraints not in ordinary queries; a data space not a query entry.

---

## 3. The design: Pure as the semantic layer

### 3.1 Rules

1. **The model is the one place.** A measure, a dimension, a hierarchy, a format, a description lives in the model's
   Pure, reviewed in its project. DataCube, Python, a REST caller, a Pure query and (later) Excel read the same
   definition. Nothing the grid offers by name is re-defined in TypeScript.
2. **Offered by name, ad hoc still possible.** A grid over a class offers the class's measures and dimensions by name;
   a column typed into the grid stays possible and is marked local, as plan B4 says.
3. **Compare and rearrange, never compute** (`docs/WORLD_MAP.md`). DataCube may take a measure's Pure apart into the
   aggregate columns and the group-stage expression a grid's query needs; every figure is the database's.
4. **No second language.** There is no DataCube formula language; a calculated column is Pure.
5. **Explicit grain.** A measure is a function of the set of instances it is evaluated over. A grid's cell, a chart's
   bar, a KPI's whole page: each is a set, and the measure is applied to it. What DAX does with filter context and
   `CALCULATE`, this design does with sets and grains (§3.8).

### 3.2 One profile for what the incumbents keep in side files

A platform profile, declared once (a platform declaration, `docs/WORLD_MAP.md`: declarations ship, bodies do not):

```pure
Profile meta::legend::bi::BI
{
  stereotypes: [measure, dimension, timeDimension, hierarchy, certified, hidden];
  tags: [label, format, description, folder, synonyms, unit, goodDirection, level, fiscalYearStart, defaultAggregate];
}
```

`doc.doc` stays the description where it is already written (DataCube's Create a Class writes it; Query reads it).
`format` is DataCube's format words (`'$#,##0.0,,"M"'`, `'0.0%'` -- §4.4's format codes), read by the grid as if the
person had set them. `goodDirection` is `up` or `down` (Pulse's favourability, the KPI's colours). `unit` is a label
until core types units (§3.10).

### 3.3 Entities, navigation and the class tree (model plan step 5, brought forward)

A grid over a class today shows its to-one primitive properties. The class tree is the column chooser as Legend
Query's explorer is: the class's properties, and through each class-typed property and Association end, the related
class's, to any depth.

- **A to-one hop is a column**: `customer > region` is `$x.customer.region`, a LEFT JOIN (supported). Its name in the
  grid is the path (`customer.region`), its label the related property's label.
- **A to-many end is an aggregate, never a fan-out by default**: `trades > notional` is offered as `count of trades`,
  `sum of trades' notional` and so on -- `$x.trades.notional->sum()`, a correlated aggregate (supported). This is the
  symmetric-aggregate guarantee by construction: a customer grid summing trade notional can never double count,
  because the aggregate is per customer before any grouping. Explicitly asked, the end is a row per related instance,
  marked (the model design's §5.1: "a customer's values repeat, and totals count them again").
- **Derived and qualified properties** are in the tree with the class's own: a derived property is a column; a
  qualified property with parameters asks its arguments (a literal, or a page parameter, §3.7).
- **Measures and dimensions declared on the class** (§3.4, §3.5) are at the top of the tree, by label, with their
  format and description.

### 3.4 Measures: functions over the instances

A measure is a Pure function whose one parameter is the instances of a class, stereotyped as a measure:

```pure
function <<meta::legend::bi::BI.measure>>
  {meta::legend::bi::BI.format = '$#,##0', meta::legend::bi::BI.goodDirection = 'up',
   doc.doc = 'Gross notional booked, both sides'}
trading::measures::grossNotional(t: trading::Trade[*]): Float[1]
{
  $t.notional->sum()
}

// filtered (MetricFlow's filter on a simple metric; Cube's filtered measure)
function <<meta::legend::bi::BI.measure>> {meta::legend::bi::BI.format = '$#,##0'}
trading::measures::buyNotional(t: trading::Trade[*]): Float[1]
{
  $t->filter(x | $x.side == trading::Side.BUY).notional->sum()
}

// a ratio: measures composed (DAX measures referencing measures; MetricFlow ratio and derived)
function <<meta::legend::bi::BI.measure>> {meta::legend::bi::BI.format = '0.00%'}
trading::measures::margin(t: trading::Trade[*]): Float[1]
{
  trading::measures::pnl($t) / trading::measures::grossNotional($t)
}

// across an association: per trade's counterparty, never fanned out
function <<meta::legend::bi::BI.measure>>
trading::measures::counterparties(t: trading::Trade[*]): Integer[1]
{
  $t.counterparty.id->distinct()->count()
}
```

**Why this is a better DAX.** It is typed in and out (DAX UDFs have no return type); overloads and generics work
(DAX UDFs have none); recursion is refused explicitly; it composes by ordinary calls; it is tested by ordinary Pure
tests against mapping test data; it is reviewed in a project with lineage; and its meaning does not depend on a hidden
filter context -- the set it is given *is* the context, so `margin` of a cell, of a whole page and of a KPI's period
are the same function applied to different sets. A measure in the model reaches every consumer: a grid, a chart, a
KPI, Python (`grid.model_measure('grossNotional')`), a hand-written Pure query (`Trade.all()->groupBy(...)` calling it), and
the REST routes.

**How a grid plans it.** A grid over `Trade` grouped by `region` needs, per group, `grossNotional` of the group's
trades. The planner takes the measure's body apart -- rule 3, rearranging, never computing:

1. The body is inlined (G½ does this for every user call).
2. Each **aggregate over the set** -- `$t.<path>-><agg>()`, optionally after `$t->filter(<pred>)` -- becomes one
   aggregate column of the group stage: map `x | <path of x>` (with the predicate, `x | if(<pred>, |<path>, |[])`),
   reduce `y | $y-><agg>()`. These are the Relation API's `~name : x | ... : y | ...` aggregate columns the grid already
   writes for every measure.
3. What remains -- arithmetic, `if`, other measures' results -- is a **group-stage expression** over those columns:
   DataCube's existing `groupDerived` columns ("Ratio of totals" is exactly `sum(a) / sum(b)`).
4. A shape that does not decompose (an aggregate of an aggregate, a window inside a measure) is refused in words,
   naming the measure and the part -- never approximated (AGENTS.md invariant 4).

**Built 2026-10-24 in DataCube** (`datacube/src/model-measures.ts`; the header menu's Add Model Measure, Python's
`grid.model_measure(name)`, headless `gridModelMeasure`), each step as a command a person could make: an aggregate a
row-level calculated column with its Aggregation, the rest a group-level column over them (hidden machinery named
`<measure>__1`, `__2`...), a called measure inlined, a path through a to-one hop a related column (§3.3), the format
from `BI.format`. Two findings the planned SQL showed: **a count is not the grid's count** -- DataCube's count
aggregation counts rows and reads no value, so `$t.customer.region->count()` and `$t->filter(p)->count()` are each a
sum of `if(<has a value, and p>, |1, |0)`, a group with none 0 as Pure's count of nothing is; and **a path through a
to-many end** only sums or counts (each instance's own figure, then summed), as an average of averages is no average.
Proved on DuckDB (`test/model-join.test.ts`: the compiler's own elements read, five measures planned, rows checked)
and from Python over a project's saved measures (`python/tests/test_model.py`).

Where the decomposition lives is the first decision (§7, D1): **in DataCube's TypeScript, shared headless with
Python**, so a grid can use model measures as soon as it can read them -- or **in core**, as a groupBy that accepts a
measure over class instances, so every Pure query gets the same semantics. Recommended: build it in DataCube first
(it is the grid's query building, and it is small), with the core form as the follow-up the compiler line owns; the
two must agree, and the DataCube tests become the core form's acceptance tests.

**Reading measures from the model.** The class source already reads the project's files at its pin through the host's
compiler (`reading.elements`). It gains: the model's functions stereotyped `measure` whose parameter is the class (or a
supertype), with their tagged values and their body as the protocol's value specification (`grammarToJson`). Nothing is
parsed by DataCube.

**MetricFlow's five metric types, each as Pure.** Simple: `$t.x->sum()`. Ratio: `a($t) / b($t)`. Derived: an
expression of measures. Cumulative: a measure shown as a running total or to-date (§3.6, §3.8), not a different kind of
measure. Conversion (a base event followed by a conversion event for the same entity within a window): a measure over
an association between two event classes, `$t->filter(x | $x.converted->isNotEmpty())`-shaped; offered later.

### 3.5 Dimensions and hierarchies

A property stereotyped `dimension` is offered for grouping first; `hidden` keeps a key out of the tree. A hierarchy is
a tagged order of properties (`{BI.hierarchy = 'Geography', BI.level = '1'}` on `region`, `'2'` on `desk`), which
becomes the grid's **dimensions** (DataCube's existing `dimensions` option: `Geography: region, desk`) read from the
model instead of configured per grid. Drilling down a hierarchy is the grid's existing expand.

### 3.6 Time intelligence

- **The time dimension.** A date property stereotyped `timeDimension` is the default axis of a trend chart, a KPI's
  period, and the page's date filter. `fiscalYearStart` (a month) on it names the fiscal calendar.
- **Grains**: day, week, month, quarter, year exist (`calc-presets.ts`, the compiler's `firstDayOf*`). Fiscal quarter
  and fiscal year are the same functions on a date shifted by the fiscal start (`adjust`), named.
- **To-date and prior period**, as show-as transforms over any measure (§3.8): YTD, QTD, MTD (a running total
  partitioned by the period, ordered by the date), **same period last year** and **prior period** (a `lag` by the
  grain over a complete axis), **year over year %** (the change of the two), **rolling N** (a moving window). All
  windows; the database computes them.
- **The time spine.** A prior-period comparison over a sparse axis is wrong if a month has no rows (the lag skips it).
  MetricFlow joins to a time spine; this design asks core for a **date range relation** (one row per day, week or month
  between two dates, DuckDB `range`, Postgres `generate_series`) as a native (§5, C3), joined left to the grouped rows.
  Until then, a show-as over time is marked when the axis has gaps.
- **Milestoning is the as-of.** A milestoned class read on a page takes the page's business date
  (`Trade.all(%2026-09-30)`), a page parameter (§3.7): "this dashboard as of the end of Q3" is one control, and a
  bitemporal class answers "as we knew it then". No incumbent has this in the semantic layer.

### 3.7 Parameters

A page parameter is a named, typed value (a date, a number, a string, a list, an enumeration) with a control on the
page (§4.6). It binds into: a qualified property's argument; a measure function's extra parameter
(`topCustomers(t, n: Integer[1])`); a milestoned class's date; a filter's value; a Top N's N. Field parameters (Power
BI's "swap the measure or dimension in a visual") are a parameter whose values are measures or dimensions by name.

### 3.8 Grain: what DAX does with CALCULATE

| The ask | DAX | This design |
|---|---|---|
| % of grand total | `DIVIDE([m], CALCULATE([m], ALL(t)))` | The measure at the grand total's grain: the same aggregate over every row the grid's filter keeps, one row joined to each group row (`DerivedColumn.grain`, `at: 'all'`; **built 2026-10-24**) |
| % of parent row / column | `CALCULATE([m], ALLSELECTED(...))` per level | The measure at the parent's grain: the same aggregate over the parent's rows (`at: 'parent'`; **built 2026-10-24** for rows). Not a window over the level's figures: a sum of the groups' averages is no average, and the grid's level queries are each parent's, so a window there was the parent's whole at every depth below the first -- the shipped "% of total" was % of parent there |
| Ignore one filter | `CALCULATE([m], REMOVEFILTERS(region))` | Evaluate on the set before that filter: the page filter is a lens, so the lens without that condition |
| A fixed grain (Tableau FIXED) | -- | A measure grouped by named keys and joined back (a window partitioned by those keys) |
| Running total, rank, moving average | visual calculations, `WINDOW` | Show-as transforms (exist; extended in §4.2) |
| One transform for every measure (calculation group) | calculation group items over `SELECTEDMEASURE()` | Show-as transforms take any measure; a model-declared transform is a Pure function from a measure's column to a column, offered by name (later) |

### 3.9 Metadata and governance

- **Read**: labels, formats, descriptions, folders and the certified stereotype from the model JSON, shown in the
  tree, the header tooltip, the KPI's description line and the grid's About.
- **Certified**: a measure or class stereotyped `certified` shows a badge; the badge is only true for a model at a
  released revision (the model design's §7: publishing needs a released model).
- **Impact**: which pages use a measure, from the saved pages' sources and the lineage plan (F4); later.
- **Queryable metadata** (Power BI's `INFO.*`): core's metamodel tables lack tagged values and stereotypes (§5, C4).

### 3.10 Units and currency

Core refuses a unit-typed property today (§2). Until it types them: a measure's `unit` tag is shown and the grid's
currency format is read from it; currency conversion is a qualified property with a date and a rate class
(`notionalIn(ccy: String[1], on: StrictDate[1])`, a navigation to the rate row), the page's currency a parameter.
Typed units and a converting `Measure` are core's (§5, C2).

### 3.11 Data quality

A class's constraints shown as a column of defects (`graphFetchChecked` computes them in SQL today) and as a page's
"rows failing a rule" count; constraints evaluated in ordinary projections are core's (§5, C5).

### 3.12 Many consumers

Have: the tab, Python (one API, the same commands), the REST routes. Next: an MCP server over the model (Power BI and
Tableau both shipped one in 2026), so an agent asks for `grossNotional by region` by name. Later: Excel's PivotTables
over the model through XMLA/MDX, as Cube and AtScale serve Excel -- the strongest single reason a bank would put its
BI model in Pure (§7, D6).

---

## 4. The catalogue: everything to build

Status: **Have**, **Partial**, **Missing**. Where: **DC** (DataCube alone, `datacube/`, headless shared with Python),
**core** (a compiler change, announced in `docs/IN_FLIGHT.md` first), **both**. Size: S (a day or less), M (days), L
(a week or more). Phase: §6.

### 4.1 The grid, against Excel

| Feature | Reference | Status | Where | Size | Phase |
|---|---|---|---|---|---|
| Menu copies as tab-separated text (paste fills columns) | Excel | **Built 2026-10-24** (were CSV with a byte-order mark) | DC | S | P1 |
| Copies headed by labels; the tree column by what it groups by | Excel | **Built 2026-10-24** (`__tree`, internal names) | DC | S | P1 |
| Pinned columns stacked at their edge | Excel freeze panes, ag-grid | **Built 2026-10-24** (overlapped at `left: 0`) | DC | S | P1 |
| Ctrl+Arrow to the data edge, Ctrl+A select all | Excel | **Built 2026-10-24** | DC | S | P1 |
| Double-click a cell to see the rows behind it | Excel pivot, Power BI "show records" | **Built 2026-10-24** (was Enter/Space only, a CSV overlay) | DC | M | P1 |
| The rows behind a cell as a grid (sortable, exportable, a tile) | Excel drill-down sheet | **Partial 2026-10-24**: a grid with its caption and Copy; as a tile of its own, P4's drill-through sheet | DC | M | P1 |
| Expand all / collapse all, expand to a level | Excel pivot | **Built 2026-10-24**: Expand All beside Collapse All | DC | S | P1 |
| Keyboard shortcut help | Excel, Sheets | **Built 2026-10-24** (Edit > Keyboard Shortcuts, `?`) | DC | S | P1 |
| Find in the grid (Ctrl+F): a quick filter across text columns | Excel find, Sheets | Missing | DC (a `contains` filter, the database's) | M | P1 |
| Drag to select with the mouse | Excel | **Built 2026-10-24** | DC | M | P1 |
| Multi-range selection (Ctrl+click) | Excel | Missing | DC | M | later |
| Subtotals at the bottom of each group; grand total at the bottom | Excel pivot layout | Partial: top only | DC | M | P2 |
| Freeze rows | Excel | Missing (header sticky) | DC | M | later |
| Paste into the grid, edit cells, fill handle | Excel | Missing (the grid reads a query; editing is a source's -- a held table or a write-back action) | DC | L | later |
| Cell comments and notes | Excel | Missing | DC + page document | M | P4 |
| Formula bar, cell references | Excel | Won't (a grid is a query, not a sheet; calculated columns are Pure) | -- | -- | -- |

### 4.2 Pivot and analysis

| Feature | Reference | Status | Where | Size | Phase |
|---|---|---|---|---|---|
| Show as: % of grand total at every depth; % of row total, % of column total | Excel Show Values As, PIVOTBY `relative_to` | **Built 2026-10-24** for the grand total (the measure at the grand total's grain, any aggregate); row and column totals wait for show-as under a pivot | DC | M | P2 |
| Show as: % of parent row, % of parent column | Excel, Power BI `COLLAPSE` | **Built 2026-10-24** for the parent row (the measure at the parent's grain: an average's share is of the parent's average); parent column waits for a pivot | DC | M | P2 |
| Show as under a horizontal pivot | Excel | Missing (offered only unpivoted) | DC | M | P2 |
| Show as: difference from / % difference from a chosen item | Excel | Missing | DC | M | P2 |
| Show as: index, % running total | Excel | **Built 2026-10-24**: % running total, and rank from the smallest; index waits for a pivot (it reads a column total) | DC | S | P2 |
| Top / Bottom N filter by a measure, within each group | Excel Top 10, Tableau Top N | **Built 2026-10-24** (each parent's groups ranked by the database, ties kept) | DC (a rank window, filtered) | M | P2 |
| "Other" bucket for what a Top N cuts | Power BI, Tableau | Missing | DC | M | P2 |
| Value checklist filter (distinct values, counts, search) | Excel autofilter | **Built 2026-10-24** (Filter Values of X...) | DC (values the database's) | M | P2 |
| Relative dates: last N days/weeks/months, this/last month/quarter/year, YTD/QTD/MTD | Excel date filters, Power BI relative date slicer | **Built 2026-10-24** (Filter > Date Range, eighteen ranges; their ends the database's) | DC | M | P2 |
| A measure over two columns: corr, covariance, weighted average, max by, min by | DAX, Excel | **Built 2026-10-24** (`snapshot.ts SECOND_COLUMN`): Column Properties' Aggregation offers correlation, sample and population covariance, max by and min by beside the weighted average, each with its second column (the panel names it by its role: weight, paired, ordering; a number, as the compiler's `rowMapper` takes); written `rowMapper($x.a, $x.b)` reduced by legend-lite's natives (CORR, COVAR_*, ARG_MAX/ARG_MIN), a pivot cell's each its own; offered by the compiler's answers (max by keeps any column's type); Python's `grid.column(..., aggregate='corr', weight=...)` | DC | M | P2 |
| Distinct count, count of blanks | Excel | **Built 2026-10-24**, as a calculated column and its Aggregation make them -- no shortcut of their own (the user, 2026-10-24: a Count menu entry, then Column Properties buttons, were taken out, the menu too long and the machinery already there): a count of values or of blanks is a Formula `if($x.c->isEmpty(), \|0, \|1)` (or the other way) summed, COUNTIF; a text column's distinct count is a number column's Aggregation `distinctCount` with the text column its counted column (COUNT(DISTINCT); 1 or 0 on a row, one type per column); a number column's distinct count is its own Aggregation; Python's `grid.calculate` + `grid.column(..., aggregate='distinctCount', weight=...)` | DC | S | P2 |
| One column, two types: the values on its rows and a figure of another type on its group rows (a text column's distinct count over its own values) | This DataCube's own (the user, 2026-10-24) | **Design, not built.** Tried the same day as an Aggregation writing the count as text and dropped: a number as text sorts as text ("10" before "9"), takes no number format, colour or chart, and exports and reaches Python as text. First-class, it is a column of a row type and a group type, each the compiler's (the offer facts already know an aggregate's result type); the queries are fine as they are (each level its own), but the grid's merged table holds one type per column (`treeview.ts`), so it would carry both, and every reader -- the grid's format, sort and colours, the exports, Python's frames -- ask the type of the cell it reads | DC | L | later |
| Groups (bucket members by hand) and sets | Tableau groups and sets, Power BI groups | **Built 2026-10-24** for groups (`snapshot.ts ValueGroups`, query.ts groupsLambda): a column's values ticked and named into groups in the Group Values window (Extended Columns ▸), the rest under a name or, on a text column, as themselves; its expression built at query time from the Filter Values window's own test, so the database does the bucketing; Edit Groups reopens it; Python's `grid.group_values`. Sets (a member in or out, by a condition) later | DC (a `case` calculated column, Pure) | M | P3 |
| Fixed-grain aggregates (Tableau FIXED/INCLUDE/EXCLUDE) | Tableau LOD | **Partial 2026-10-24**: a measure at its parent's or the grand total's grain (`DerivedColumn.grain`, joined in the level's query); grain by any chosen keys is P5 | DC (a join to the measure at the coarser grain) | L | P5 |

### 4.3 Conditional formatting and number formats

| Feature | Reference | Status | Where | Size | Phase |
|---|---|---|---|---|---|
| Data bars | Excel, Power BI | **Built 2026-10-24** (`datacube/src/conditional.ts`): from the zero line to the value, a negative bar leftward in red, against the column's range at the cell's depth -- measured over the whole column as the heatmap is, never the window on screen; Conditional Formatting ▸ Data Bars, the column's Properties, Python's `grid.data_bars`; drawn in the HTML export | DC | M | P3 |
| Three-colour scale with a midpoint | Excel | **Built 2026-10-24**: the heatmap's `mid` colour; Color Scales ▸ offers Excel's (green-yellow-red, red-yellow-green, blue-white-red) and the two-colour one; Python's `grid.color_scale` | DC | S | P3 |
| Icon sets (arrows, flags, traffic lights) by thresholds | Excel, Power BI | **Built 2026-10-24**: arrows, traffic lights, symbols, flags, by the third of the range (Excel's 33%/67%), reversed for a cost; each icon labelled for a reader that does not see colour. Thresholds of one's own: later | DC | M | P3 |
| Rule-based formatting (value > 100: red; text contains; top 10%) | Excel | **Built 2026-10-24**: greater/less than, at least/most, between, equal, contains, starts with, top/bottom N (ties in), above/below average, blank -- the first rule that holds drawing one of Excel's styles or one's own colours; the menu's one-click rules, the Properties' rule of a value of one's own, Python's `grid.highlight`; a rule's colours in every export. Top N% later | DC | M | P3 |
| Custom number format codes (`#,##0.0,,"M"`, `0.0%`, `[Red]`) | Excel, Power BI | **Built 2026-10-24** (`datacube/src/format-code.ts`, `ColumnFormat.code`): a code's four sections (positive; negative; zero; text), places, grouping, scaling commas, percent, quoted and escaped literals, `[$€-407]` currency, a section's colour drawn by the grid and the HTML export; the workbook gets the code itself; the column's Properties' Format Code, Python's `grid.format_code`, a model measure's `BI.format` kept whole. Judged when set and when a saved page opens; a condition (`[>100]`), an exponent, a fraction or an elapsed time refused in words | DC | M | P3 |
| Date formats (`yyyy-mm-dd`, `dd MMM yyyy`, ...) | Excel | **Built 2026-10-24**, as format codes: `yyyy yy mmmm mmm mm m dddd ddd dd d hh h mm ss AM/PM`, `m` a minute after an hour or before a second (Excel's rule), in UTC as stored | DC | S | P3 |
| Sparklines in cells (line, column, win/loss) | Excel, Power BI | Missing | DC (a group-level series query) | M | P3 |

### 4.4 Charts

| Feature | Reference | Status | Where | Size | Phase |
|---|---|---|---|---|---|
| Chart from a selection | Excel | Missing (charts the whole query) | DC | M | P3 |
| Trend line (linear regression) | Excel, Tableau, Power BI | Missing | DC (`regr_slope`-style aggregates or two-column aggregates; the database's) | M | P5 |
| Forecast | Excel Forecast Sheet, Tableau | Missing | core or DC | L | later |
| Highlight a click (dim the rest) instead of filtering | Power BI | Missing | DC | M | P4 |
| Viz in tooltip | Tableau, Power BI | Missing | DC | M | later |
| Annotations | Tableau | Missing | DC + page document | M | later |
| Donut centre value, card with several callouts | Power BI (2025-26) | Partial (KPI strip; ring totals) | DC | S | P4 |

### 4.5 Page and interactivity

| Feature | Reference | Status | Where | Size | Phase |
|---|---|---|---|---|---|
| Text, title and image tiles | Power BI, Tableau | **Built 2026-10-24** for text (`page-text.ts`, a `text` page view): Create ▸ Text Box in every menu adds a text tile, edited in place (double-click; OK or Ctrl+Enter, Escape), its words in a small Markdown -- headings, **bold**, *italic*, `code`, lists, http(s) links -- drawn as text, never HTML; page commands `addText`, `setText` (Python's `page.text`, `sheet.text`, `Text.text`); saved, merged with Python's edits, exported to HTML and PDF (not yet into the workbook). Images later | DC (a page view kind) | S | P4 |
| Slicer tiles: list, dropdown, buttons, range, date (relative) | Power BI slicers, Excel slicers and timelines | Partial: a filter bar for class grids | DC | M | P4 |
| Filter controls reaching relation grids (not only classes) | Power BI | Missing | DC | M | P4 |
| Page parameters with controls | Tableau parameters, Power BI what-if | Missing | both (§3.7) | L | P4 |
| Bookmarks (a named page state) | Power BI | Missing | DC | M | P4 |
| Drill-through to a detail sheet carrying the clicked conditions | Power BI | Missing | DC | M | P4 |
| Field parameters (swap a visual's measure) | Power BI | Missing | DC | M | P5 |

### 4.6 The semantic layer

| Feature | Status | Where | Size | Phase |
|---|---|---|---|---|
| The class tree: to-one hops as columns, through class-typed properties and associations | **Built 2026-10-24**: a related column (`DerivedColumn.related`) projected with the class's own (query.ts relatedSource); the header menu's Add Related Column offers one hop, Python's `grid.related('a.b')` any depth, each path judged against the model's class tree (class-tree.ts) | DC (core supports the queries) | M | P5 |
| A to-many end offered as aggregates (count, sum, ...) per instance | **Built 2026-10-24**: count, sum, average, min, max -- a LEFT JOIN to the related rows grouped by their key, so a region's total of customers' trade sums is exact; never a row per instance. Distinct count waits for core (C9) | DC (correlated aggregate, supported) | M | P5 |
| Derived properties offered; qualified properties with arguments | Missing | DC | M | P5 |
| Labels, formats, descriptions read from tagged values | **Partial 2026-10-24**: a model measure's `BI.format` (an Excel format code a number, currency or percent format, else none -- never guessed) and its `doc.doc` (the menu's hint) | DC (model JSON) | M | P5 |
| Measures declared in the model, offered by name (§3.4) | **Built 2026-10-24** in DataCube (§3.4's "Built"): the header menu's Add Model Measure, Python's `grid.model_measure`; a project's own profile named `BI` read until the platform ships `meta::legend::bi::BI` (D3, C10); the core form is C1 | DC first, core form after | L | P5 |
| Dimensions and hierarchies from the model | Missing | DC | M | P5 |
| Certified badge | Missing | DC | S | P5 |
| Time intelligence: YTD/QTD/MTD, prior period, same period last year, YoY % | Missing | DC (windows) | M | P5 |
| A page's as-of date for milestoned classes | Missing | DC (core supports `.all(date)`) | M | P5 |
| Time spine | Missing | core (C3) | M | core |
| Units and currency | Missing | core (C2) | L | core |
| An MCP server over the model | Missing | server | M | later |
| Excel PivotTables through XMLA/MDX | Missing | server | L | later |

### 4.7 Service and AI (the gap analysis's Tiers 4 and 5)

Folders, permissions, version history, comments, scheduled delivery, alerts, caching, usage, embedding; natural
language over the governed model, summaries, "explain this change". Not this program's phases: plan B5, B6 and the
`nlq/` line. Written down so nothing is lost.

---

## 5. What core is asked for (each announced in `docs/IN_FLIGHT.md` before an edit)

| # | Ask | Why | Owner |
|---|---|---|---|
| C1 | A groupBy aggregate that is a **user function over class instances** (`y | $y->trading::measures::grossNotional()`), its body decomposed or inlined as the planner would | Model measures usable from any Pure query, not just DataCube's grids (§3.4) | Compiler line |
| C2 | **Units and currency**: a unit-typed property compiled, converted, lowered | §3.10 | Compiler line |
| C3 | A **date range relation** native (one row per period between two dates) | The time spine (§3.6) | Compiler line |
| C4 | **Tagged values and stereotypes** in the metamodel tables | Queryable metadata, `INFO.*`-style (§3.9) | Compiler line |
| C5 | **Constraints evaluated in projections** (a defects column) | Data quality on a page (§3.11) | Compiler line |
| C6 | `orElse`; optional enum parameters; `lastDayOfMonth` | Small gaps a semantic layer meets at once | Compiler line |
| C7 | A **data space as a query entry** | A perspective a client names rather than unpacks | Compiler line |
| C8 | **Unused projected columns pruned**: a `select` or `groupBy` after a `project` keeps the LEFT JOINs (and per-instance aggregate subqueries) of columns it never reads (probed 2026-10-24) | A class's grid could then project its whole class tree and pay only for what each query reads | Compiler line |
| C9 | **`distinct()->count()` over a to-many navigation** in a projection (refused today: "aggregate count over an expression containing a to-many navigation is not supported yet") | A related column's distinct count | Compiler line |
| C10 | **The `meta::legend::bi::BI` profile as a platform declaration** (D3): `stereotypes: [measure, dimension, timeDimension, hierarchy, certified, hidden]; tags: [format, goodDirection, ...]` (§3.2) | A project cannot declare a `meta::` element -- its store refuses the path (legend-sdlc's rule; found 2026-10-24, Python's end-to-end test) -- so until then a project declares its own profile named `BI`, which DataCube reads as the platform's | Compiler line |

This line does not message other sessions: these are recorded here and in the line's IN_FLIGHT entry for the compiler
line to read.

---

## 6. The program

Each phase lands on its own branch off the last (`claude/datacube-*`), with tests that fail without each change, the
local DataCube suite, and the user's OK before any merge. The order puts what a person feels every minute first, then
the analysis a pivot user expects, then what makes a page a dashboard, then the semantic layer -- which is the
largest, and the differentiator, and stands on the grid and page work below it.

| Phase | Contents | Exit |
|---|---|---|
| **P1 The grid feels like Excel** | Copies (built), pinned columns (built), Ctrl+Arrow and Ctrl+A, double-click to the rows behind a cell, those rows as a grid, expand all and expand to a level, keyboard help, find, drag-select | Each with a jsdom test; one browser check of copy, pin, drill |
| **P2 The pivot answers like Excel** | Show-as: % of row/column/parent, under a pivot, difference from an item, index; subtotals and grand total at the bottom; Top/Bottom N with Other; value checklist filter; relative date ranges; two-column measures | Each planned in the tab on DuckDB, rows checked |
| **P3 Formats and in-cell visuals** | Data bars, three-colour scales, icon sets, rules, format codes, date formats, sparklines, chart from a selection, groups | jsdom and browser screenshots |
| **P4 A page is a dashboard** | Text tiles, slicer tiles (list, buttons, range, relative date), filters reaching relation grids, page parameters, bookmarks, drill-through, highlight mode, cell notes | Page commands, undo, Python, saved and reopened |
| **P5 Pure is the semantic layer** | The class tree (hops, to-many aggregates), derived and qualified properties, metadata read, model measures by name (§3.4), dimensions and hierarchies, certified, time intelligence, a page's as-of date, field parameters, fixed grain, trend lines | A model with measures, dimensions and a milestoned class, planned in the tab, rows checked; Python asks the same |
| **P6 Core asks** | C1-C7, as the compiler line takes them | That line's gates |

This session builds from P1 down, committing each piece as it passes.

---

## 7. Decisions for the user

| # | Question | Recommendation |
|---|---|---|
| D1 | Where does a model measure's decomposition live: DataCube's TypeScript (shared with Python), or core? | DataCube first, small and testable now; core's form (C1) after, with DataCube's tests as its acceptance tests |
| D2 | A measure as a **function over instances** (§3.4) or as a new element kind (`Measure trading::grossNotional { ... }`)? | The function: it is Pure already, compiles today, composes, and needs no grammar; the profile carries what an element kind would |
| D3 | The profile's path: `meta::legend::bi::BI` (a platform declaration) or a user-owned profile per project? | Platform: one meaning across projects, as `doc.doc` is. **Found 2026-10-24:** a project cannot declare it -- its store refuses a `meta::` path, legend-sdlc's rule -- so until core ships it (C10) DataCube also reads a project's own profile named `BI` (model-measures.ts) |
| D4 | A to-many end in the tree: aggregates only by default, the row-per-instance form marked? | Yes (the model design's §5.1 decided it; this keeps it) |
| D5 | Editing cells (paste, fill, write-back): a held table's edit, or a write-back action (Power BI's translytical flows)? | Later, and as actions, not as a spreadsheet: a grid is a query |
| D6 | Excel through XMLA/MDX: on the road map? | Yes, after P5: it is the shortest path into every bank's Excel, and Cube and AtScale prove the shape |

---

## 8. Sources

The incumbents' features, from the research track's primary sources: Microsoft Learn (DAX UDFs, calculation groups,
visual calculations, field parameters, INFO functions, Direct Lake, the Power BI update archive through August 2026),
SQLBI (calendar-based time intelligence), Tableau Help and release pages (Pulse, composable data sources, viz
extensions, 2025.3 to 2026.2), Salesforce developer docs (Tableau Semantics), Microsoft Support (GROUPBY, PIVOTBY,
PERCENTOF, Show Values As, cube functions), dbt docs (MetricFlow metric types; Apache 2.0 since October 2025), Cube
docs (multi-stage measures, pre-aggregations, MDX and DAX APIs), Looker docs (symmetric aggregates), Malloy docs
(aggregate locality), AtScale's 2025 announcements, Apache Ossie's incubation notice (June 2026). legend-lite's support:
the test and corpus paths cited in §2. DataCube's grid: `datacube/src` as of `819a2e81a`.
