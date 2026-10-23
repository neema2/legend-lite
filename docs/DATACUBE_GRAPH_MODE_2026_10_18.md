# DataCube's graph mode: a design note, and the G0 spike

Phase 4 of the DataCube roadmap run (`docs/DATACUBE_ROADMAP_RUN_2026_10_18.md`), on branch `claude/datacube-graph-mode`,
stacked on phase 3 with main merged in (main carries the `#Cypher{...}#` island, neema2/legend-lite#58).

**A proposal to the Cypher line, not yet agreed.** The compiler half of a graph mode is the Cypher session's
(`docs/CYPHER_PLAN_2026_10_15.md`); this line does not message other sessions, so the note is written for them to
read and answer, and the run builds only what stands on what main already has: the G0 spike.

## What exists

- **The island** (main, `compiler/spec/CypherIslands.java`): `#Cypher{...}#` inside any Pure query is translated to an
  ordinary Relation expression over the class model, which is the graph (decision C2: a label is a class, a
  relationship type a property or association end holding a class), then typed once and lowered to SQL like any query.
  Judged against Neo4j's snapped answers on DuckDB and Postgres (`//core:cypher_parity_test`).
- **The tab's planner** is the same compiler, in WebAssembly (`//wasm:planner`): it plans whatever core plans.
- **DataCube** runs a Pure relation as a grid's source; pivots, charts, Python and the page all stand on that.
- **Identities, inside the compiler, not out of Cypher** (main, `docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md` W2): a
  mapped instance's identity is its class's key, `'<class>:<key>'` (the mapping's `~primaryKey`, else the main table's
  PRIMARY KEY), a Pure native (`meta::legend::lite::elementId`); Cypher compares nodes by it. Cypher's own `id(n)` and
  `elementId(n)` are refused, by the study's decision C5 (the physical key must not leak). A node or a relationship a
  query RETURNs comes back as a value carrying it (`"@id": "test::Person:1"`, `"@id": "firm:test::Person:1>test::Firm:1"`).
  For a drawing, gaps remain: one association edge has two identities, by which end names it (`firm:...` and
  `employees:...`); `startNode` and `endNode` are refused; a composite key is refused, and a class with no key has no
  identity.
- **A graph by name** (main, the study's decision C2 and §12.45): `USE <data space>` names the graph a query sees -- any
  DataSpace, its elements the labels (one with none: every class); stereotyped `<<graph::graph.Open>>`, it is read open
  (a label no class has: no rows; a property none declares: null), else closed. Without `USE`, the whole class model is
  the graph, closed.
- **Paths, partly:** named paths, bounded variable-length patterns and unbounded walks have landed; `shortestPath`
  (the plan's PR4) and the algorithms (PR6, PR7) are to come.
- **Missing here:** a Cypher source in DataCube; a network chart (ECharts has one, unused).

## The phases

| | What | Whose | Stands on |
|---|---|---|---|
| G0 | A Cypher query planned in the tab and run on its DuckDB, answering as the island's integration test holds | this line | main |
| G1 | A Cypher query as a grid's source: New > Data Source > Cypher over a model, its result a relation, so pivots, charts, the page and Python work on it unchanged | this line | G0 |
| G2 | A network view: nodes and relationships drawn (ECharts' graph series), each node's label and properties | both | a returned node's and relationship's identity as a drawing needs it (the gaps above), within C5 |
| G3 | Click a node to expand its neighbours (variable-length patterns have landed); a selection filters the grid, as a chart's click does | this line | G2 |
| G4 | Paths and algorithms (shortest path, PageRank) as the compiler gains them | the Cypher line | its plan |

## G0, built here

`datacube/test/cypher-spike.test.ts` (`//datacube:cypher_spike_test`): the island's own integration model (people,
firms, cities; a person's firm an association, a firm's city a class-typed property), its rows seeded into the tab's
DuckDB, Cypher queries planned by the tab's planner (`LitePlanner.planText`, the WebAssembly build of main's compiler)
and run on DuckDB, their rows the island's integration test's (derived there by hand from Cypher's semantics): a
match, a hop along an association, a hop back counted by group, nodes compared by identity. And the island as a node
of the tree DataCube sends (`{_type: 'classInstance', type: 'Cypher', value: {cypher}}`, planned by `plan`, as every
grid's query is), grouped and sorted in Pure as a grid's query composes with its source, its rows computed for the
data. It proves the tab runs Cypher today, with no server, so G1 is DataCube's work alone: a source picker entry, a
source editor, and the Cypher node built into the grid's query tree -- the tab's grammar parse (`LitePlanner.parse`)
refuses lite's islands (it reads the engine's surface), so the source is built as a node, not parsed from text.

## Questions for the Cypher line

1. **Identities, within C5:** a drawing needs to tell nodes apart and to join a relationship to its ends, without the
   physical key leaking. Is the `@id` a returned node and relationship carry meant to be read by a client, as the
   handle a drawing keys by? And one association edge named from either end -- should it carry one identity?
   `startNode`/`endNode` -- planned?
2. **A graph to pick:** G1's source picker would list a model's data spaces, each a graph by `USE` (open or closed as
   its stereotype says), and offer the whole model, closed, as one more. Is that the reading?
3. **Refusals:** the island's refusals (not yet supported, syntax) arrive as compile errors placed in the source; G1
   would show them in the source editor as Pure's are. Anything else a source editor should know?
