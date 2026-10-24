# A DataCube for graphs: an editor and a viewer for Cypher (a brainstorm, 2026-10-24)

**Asked by the user (2026-10-24):** "brainstorm about what a 'DataCube' style experience would be for graph/Cypher --
what people would like as a visual editor and viewer for graph queries and graph output". This is a brainstorm, not
an agreed plan: it gathers what such a tool could be, what people like and dislike in the tools that exist, and a
proposed order. It extends DataCube's graph-mode note (`docs/DATACUBE_GRAPH_MODE_2026_10_18.md`, phases G0 to G4) and
stands on the Cypher line's plan (`docs/CYPHER_PLAN_2026_10_15.md`).

---

## 1. The idea in one paragraph

DataCube's promise for tables is: point it at data, explore without writing a query, and every gesture is a query the
database runs, which you can see, save, share and drive from Python. **The same promise for graphs:** point it at a
Legend model, see its graph (classes are node types, associations and edge classes are relationship types), ask
graph questions by drawing and clicking, and get back a picture, tables and charts that stay linked. Every gesture
compiles to Cypher, then to Pure and SQL, run by the database the model already maps to. **No graph database, no copy
of the data, no second model.** That last part is what nobody else offers.

---

## 2. What it stands on (main, today)

- **Cypher over the class model** (`#Cypher{...}#`, `compiler/spec/CypherIslands.java`): a label is a class, a
  relationship type an association or a class-typed property, a relationship with properties an edge class
  (`<<graph.Edge>>`). The island becomes an ordinary Relation expression, typed once, lowered to SQL for any dialect,
  judged against Neo4j's own answers (over 2,800 cases passing on DuckDB at the plan's last count, zero wrong
  answers, everything else refused by name).
- **A graph by name:** `USE <data space>` picks the graph a query sees; without it, the whole model.
- **Identities:** a returned node or relationship carries an `@id` built from its class's key, so a drawing can tell
  nodes apart without the physical key leaking (with the gaps the graph-mode note lists: an association edge named from
  either end has two ids; `startNode`/`endNode` refused; composite keys refused).
- **Paths:** named paths, bounded variable-length patterns and unbounded walks have landed; `shortestPath` and the
  algorithms (PageRank, components, centrality, communities) are planned, computed in the database through a recursion
  native (plan PR4 to PR7).
- **G0, done:** a Cypher query planned in the tab's WebAssembly planner and run on its DuckDB, with no server
  (`datacube/test/cypher-spike.test.ts`).
- **The rest of DataCube:** grids, 32 chart kinds, pages, cross-filtering, Python, and ECharts' graph series (shipped,
  unused).

---

## 3. Who would use it, and for what

| Who | The question | What they need to see |
|---|---|---|
| A risk or credit analyst | "Who are we really exposed to?" Ownership chains from a counterparty up to its ultimate parent, and every trade along the way | A hierarchy, with totals rolled up each level |
| An operations or KYC analyst | "Are these accounts connected?" Shared addresses, directors, phone numbers | A neighbourhood around two or three entities, and the paths between them |
| A data owner | "What uses this table?" Lineage from a store through mappings and classes to services and pages | A left-to-right flow, impact highlighted |
| An engineer on a model | "Does my model say what I think?" The classes, associations and their cardinalities, with real counts | The schema, drawn, with instance counts on each edge |
| A quant or data scientist | "Give me the subgraph, then let me compute on it" | The same view, driven from Python, with the result as a frame or a NetworkX graph |
| An executive consuming a dashboard | "Show me the network that matters" | A graph tile on a page beside KPIs and charts, filtered by the page |

The point of the table: **most graph questions end in a table or a number**, not just a picture. A tool that only
draws is a toy; one that only returns rows is SQL. The DataCube way is both, linked.

---

## 4. What people like, and dislike, in the tools that exist

| Tool | Loved for | Complained about |
|---|---|---|
| **Neo4j Browser** | Type Cypher, see a graph, a table or text of the same result; click a node to expand | A hairball past a few hundred nodes; the picture and the query drift apart; no dashboards |
| **Neo4j Bloom** | Search phrases instead of Cypher ("Person who works at Firm"); styling rules by label and property; saved "scenes" | Hard to get from a picture back to a table or a number; a separate product |
| **NeoDash** | Dashboards of Cypher reports: graph, table, bar, map, KPI tiles with parameters | Every tile is hand-written Cypher; no exploring without writing |
| **Linkurious, KeyLines/ReGraph** | Investigation: expand, pin, group into "combos", a timeline that plays edges over time, saved investigations | Commercial, heavy; the data must be in a graph database |
| **Graphistry** | Huge graphs on a GPU; histograms of properties that filter the picture as you drag | The picture is the whole tool; little modelling or query help |
| **Gephi** | Layouts and statistics (modularity, centrality) applied to a picture | Desktop, file-based, not live |
| **TigerGraph GraphStudio, Memgraph Lab** | A schema view, and drawing a pattern to build a query | Tied to their database |
| **Arrows.app** | Drawing a pattern is the clearest way to explain one | A drawing only; it runs nothing |

**What to take:** three views of one result (graph, table, text); styling rules; expand by click; search phrases;
grouping into combos; histograms that filter; a timeline; dashboards of graph tiles.
**What to avoid:** the hairball; losing the way back to a table; a query the user can no longer see; needing a graph
database and a copy of the data.

---

## 5. The editor: ways to ask without writing Cypher

Every way below writes Cypher, shown live beside it and editable both ways, the way DataCube's grid writes Pure.

### 5.1 Start from the schema

The model's graph is drawn: each class a node type (with its instance count), each association or edge class a
relationship type (with its count, and its cardinality). Click **Person** to start: "Show 25 Persons". Click the
**worksAt** edge between Person and Firm: "Show Persons and the Firms they work at". The schema is a menu.

### 5.2 Draw the pattern

A canvas where the user drags node types from the schema and connects them, as in Arrows.app:

```
 (p:Person) ──worksAt──▶ (f:Firm) ──locatedIn──▶ (c:City {name: "London"})
     │                                                  
     └── age > 40                                       
```

- Each node and edge has filter chips, the grid's filter editor and Filter ▸ menu reused ("name starts with", "in",
  "between").
- An edge can be optional (a dashed line: `OPTIONAL MATCH`), negated (a crossed line: `WHERE NOT`), or a range of hops
  (a slider: `*1..3`).
- Ticking a node or edge returns it; ticking a property returns that column.
- The Cypher pane updates as you draw; typing in it redraws the pattern when the pattern can be drawn (and says so
  when it cannot).

### 5.3 Search phrases

Typed, with completion from the schema, as Bloom does: "Person worksAt Firm locatedIn City London". The phrase is the
pattern, read left to right; the completions are the model's own names. Later, natural-language questions through the
`nlq/` line become one more way to fill the canvas.

### 5.4 Expand by clicking

On any node in a result: **Expand ▸** with each relationship type and its count before you click ("employees (42)",
"subsidiaries (7)"). Expanding adds to the picture and to the query (`MATCH ... OPTIONAL MATCH ...`), so the query
always says what is on the screen. Undo takes the expansion back.

### 5.5 Find paths

Pick two nodes (or two searches): **Paths ▸** shortest, all up to N hops, only through these relationship types,
avoiding these nodes. The answer is drawn and listed, each path a row.

### 5.6 Aggregate: the graph's pivot

DataCube's grid groups rows; the graph's version groups nodes. **Group by ▸ Firm.city**: every Firm collapses into one
node per city, sized by how many firms, with the relationships between them summed ("120 employees work across these
two cities"). Ungroup returns them. The same grouping, as rows, is a grid: one row per city, the counts as columns.
This is how the hairball gets tamed without hiding data.

### 5.7 Algorithms as menu items

**Analyse ▸** degree, PageRank, connected components, shortest-path distances, communities (the deterministic ones,
plan PR6 and PR7). Each answer becomes a property of the nodes, computed in the database, which the user can then size
by, colour by, filter on, sort in the table, or chart. Algorithms whose answers are random (Louvain, random walks) are
refused by name, as the plan decides.

---

## 6. The viewer: one result, linked views

### 6.1 Three views, one selection

| View | Shows | DataCube twin |
|---|---|---|
| **Graph** | Nodes and relationships, drawn | A chart |
| **Nodes** | One row per node, its label and properties, as a grid (pivot it, chart it) | A grid |
| **Relationships** | One row per relationship, its ends and properties | A grid |
| **Paths** (when asked) | One row per path, its length and steps | A grid |
| **Cypher** | The query, editable | Pure, in the source editor |

Select in any view and the others follow: a node selected in the picture is highlighted in the table, and a filter on
the table thins the picture. **Show Table** is Show Grid's twin.

### 6.2 Styling rules (Column Properties for graphs)

- Colour by label, or by a property (a sequential ramp for a number, the categorical palette for a category).
- Size by a property or an algorithm's answer (PageRank, degree).
- An icon per label, a caption (which property names the node).
- Edge width by a property (notional, count), edge colour by type, arrows on or off.
- A legend, always, for colour and size.
- Saved with the page, and set from Python the same way.

### 6.3 Layouts for the question asked

| Layout | For |
|---|---|
| Force-directed (default) | Exploring an unknown neighbourhood |
| Hierarchical, top-down or left-right | Ownership chains, org charts, lineage |
| Radial | One entity and its circles of neighbours |
| Circular, grouped | Communities, after Analyse ▸ communities |
| Geographic | Nodes with a latitude and longitude (DataCube's maps already draw places) |
| Timeline | Relationships with dates, played over time (trades, payments) |
| Flow (Sankey) | Money or data moving between groups |

### 6.4 Taming the hairball

- **Start small:** 25 nodes, not 25,000; the rest offered as "N more" chips on the nodes they hang from, like
  DataCube's "the first N" note.
- **Group by** (5.6) as the main way to see everything at once.
- **Histograms that filter** (from Graphistry): a strip of each numeric property's distribution; drag across one to keep
  only those nodes.
- **Degree and distance filters:** hide leaves, or keep only nodes within N hops of the selection.
- **Pin, hide, focus:** pin the nodes you care about; hide the noise; focus fades everything not on a path or near the
  selection.
- **Level of detail:** zoomed out, communities are blobs with counts; zoomed in, they open.

### 6.5 Inspect a node

A card with its label, properties and relationship counts; **Open as grid** opens its class's rows filtered to it,
and the same for its neighbours ("Open its 42 employees as a grid"). From there, every DataCube tool applies.

### 6.6 Time

A time slider for relationships with dates: watch a network form, see which links were active on a day, compare two
periods. On a page, the slider is a page filter like any other.

### 6.7 Scale

ECharts' graph series draws a few thousand nodes well; past that, a WebGL renderer (sigma.js, deck.gl or
Cosmograph-like) for the picture, with the database grouping and sampling before anything reaches the browser. The
rule stays DataCube's: the database computes, the browser draws.

---

## 7. DataCube's ideas, carried over

| In DataCube | For graphs |
|---|---|
| A grid over a source | A graph over a model or a data space (`USE`) |
| Group rows, pivot | Group nodes into meta-nodes (5.6) |
| Filter ▸ on a cell | Filter on a node or edge property, from its card or the canvas |
| Drill down a chart | Expand a node (5.4) |
| Calculated column | A computed property of nodes, in Pure |
| Charts follow the grid | Charts follow the graph: degree distribution, a bar of nodes by label, a histogram of a property, each re-drawn as the graph changes |
| Freeze / Pin | Keep a graph as it is while the page changes |
| Show Grid | Show Table (6.1), Open as grid (6.5) |
| Snap | Snap a subgraph: its nodes and relationships copied into the tab, explored offline |
| A page of tiles, cross-filtering | A graph tile beside grids, charts and KPIs; selecting nodes filters the page, and a page filter thins the graph |
| Undo, redo, commands | Every expansion, grouping and style a command, undoable, replayable |
| Python drives the same page | `page.graph(model, cypher=...)`, `g.expand(node, 'employees')`, `g.paths(a, b, max_hops=3)`, `g.group_by('city')`, `g.nodes` / `g.relationships` as frames, `g.to_networkx()` |

---

## 8. What only Legend can do

1. **A graph over the data you already have.** The relational tables a model maps stay where they are; the graph is
   the model's reading of them. No ETL into a graph database, no second copy to keep in sync, no second set of grants.
2. **Data spaces as curated graphs.** A data space already scopes a model for a purpose; as a graph (`USE`), it is a
   ready-made "perspective" (Bloom's term) that the model's owners govern and review.
3. **The model as a graph.** Point the viewer at the model itself: classes, associations, mappings, stores, services,
   data spaces, and who depends on whom. Impact analysis and lineage (the BI plan's F4) get a picture for free.
4. **One query, everywhere.** A graph tile's Cypher is a Pure expression, so the same question runs on DuckDB in the
   browser, Postgres, or Snowflake, and from Python, with the same answer, checked against Neo4j by the Cypher line's
   harness.
5. **Graph answers in a pivot.** Every graph result is also rows, so the full grid (group, pivot, chart, export) applies
   to it. In graph databases' own tools, that is an afterthought.

---

## 9. A proposed order

The graph-mode note's G1 to G4 stand; this adds what the brainstorm found. Each phase is useful on its own.

| Phase | What | Needs |
|---|---|---|
| **G1** | Create ▸ Data Source ▸ **Cypher** over a model or a data space; the result a grid, so pivots, charts, pages and Python work unchanged | G0 (done) |
| **G2** | A **graph tile**: the result drawn (ECharts' graph series), linked to its nodes and relationships grids; styling rules; force and hierarchical layouts | Identities as a drawing needs them (the note's questions 1 to 3, for the Cypher line) |
| **G3** | **Expand by click** with counts; selection filters the page, and page filters thin the graph; Show Table; Open as grid | G2 |
| **G4** | **Start from the schema** (5.1) and **draw the pattern** (5.2), with the Cypher pane two-way | G2 |
| **G5** | **Group by** into meta-nodes (5.6), histograms that filter, "N more" chips | G3 |
| **G6** | **Paths** (5.5) and **Analyse ▸** (5.7), as the Cypher line lands `shortestPath` and the algorithms | Plan PR4 to PR7 |
| **G7** | Search phrases (5.3); timeline and geographic layouts; Python's `page.graph` API with the same commands | G4, G5 |
| **G8** | Scale: a WebGL renderer, the database grouping and sampling before drawing; the model itself as a graph (8.3) | G5 |

A first demo worth showing could be G1 plus G2 on the example pages' sales data, or a small ownership model: a graph
tile beside a KPI strip, a click on a firm filtering the page.

---

## 10. Open questions

1. **Identities** (the graph-mode note's question 1, still open with the Cypher line): may a client key a drawing by
   `@id`, and will one association edge carry one id whichever end names it?
2. **Which renderer** past a few thousand nodes: stay on ECharts and group harder, or add WebGL early?
3. **Cypher or SQL/PGQ** as the visible language: Cypher is what graph users know; SQL/PGQ is the standard. The plan
   lists SQL/PGQ as a later second spelling.
4. **Writes:** read-only, like the rest of DataCube today? (Recommended: yes, until the BI plan's F1.)
5. **The first audience:** risk and ownership chains (hierarchical layouts, roll-ups), or investigations
   (neighbourhoods, paths)? The answer decides whether G4 (drawing patterns) or G5 (grouping) comes first.
6. **A name:** "graph mode" inside DataCube, or its own app beside Query, DataCube and Studio on the playground?
