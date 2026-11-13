# legend-engine plans, in and out: the design (2026-11-12)

> **Status: the shape is accepted.** The user, 2026-11-12, asked: "we still want to be able to load legend-engine plans
> and also us be able to generate legend-engine plans that engine can read - the nice thing about the java stuff is
> that we still dont have to do all the crazy floating stuff in SQL both ways". On the design below they said: "i
> thought that was perfect ... that was the exact right spirit".
>
> **Phase:** D (compatibility to the edge), built after D1 makes the edge (`PlanStyle`, `com.legend.edge`). It touches
> nothing in A/B/C. A/B/C is what makes it possible: a lite plan is pure data, SQL and values, so it can be translated
> to and from another plan format without running anything.

## 1. What it is for

- **Export (lite to engine).** A query written and planned in lite becomes a plan a real legend-engine can execute.
  - Lite-authored queries can be deployed to an engine server.
  - Existing Legend UIs and tools that ask an engine's `generatePlan` get a plan they can run.
- **Import (engine to lite).** A plan legend-engine generated runs on lite.
  - An engine's plan for a query can run where lite runs (the tab, Python, the server).
  - DataCube pointed at an engine can use the engine's planner and run the SQL locally. Today's TypeScript
    `UpstreamPlanner` does a small part of this.

## 2. What exists today (read 2026-11-12, at 549c018d7)

- **Export, half there.** `/api/pure/v1/execution/generatePlan` (`server/PureV1Api.java`, `generatePlan` and the writer
  around :920-975) writes legend-engine's plan format, measured against engine 4.145.0:
  - a `simple` plan;
  - a `relationalTdsInstantiation` root with `tdsColumns`;
  - one `sql` node with its `connection`, `resultColumns`, `sqlQuery` and the engine's `sqlComment`
    (`-- "executionTraceID" : "${execID}"`);
  - `templateFunctions` (`PlanSupportFunctions`) and the `pure` serializer;
  - a folded constant as engine's `constant` node;
  - a keyed pivot marked `isResultColumnsDynamic` (S110).

  Two gaps:
  - Its `sqlQuery` is **lite's own product statement** (`plan.sql()`).
  - **No one has checked that a real engine executes it.** It was built so plans can be shown, and compared in the
    corpus.
- **Import, TypeScript only and one shape.** `datacube/src/planner.ts` (`UpstreamPlanner`, `sqlNode` :198-219) reads
  only:
  - a `relationalTdsInstantiation` over exactly one `sql` node;
  - written for the database it runs on (DuckDB);
  - with no `${...}` placeholder.

  Everything else is refused by name. Java has no reader for engine plans.

## 3. The design

### 3.1 Both directions are TRANSLATIONS AT PLANNING, at the edge

- **Neither direction changes exec.**
  - An engine plan becomes a lite plan before anything runs.
  - A lite plan becomes an engine plan without being run.
  - Exec still sends only a lite plan's statements, with A3's three forms and nothing else (G4).
- **Both live at the edge**:
  - `com.legend.edge` on the Java side (served by `EdgeServer`);
  - the lazily loaded TypeScript compatibility module in the browser (D-1's middle option).

  The edge depends on the product, never the reverse (G5). No product class knows engine plans exist.

### 3.2 Export: a lite plan to an engine plan

1. **Plan with `PlanStyle.UPSTREAM`.**
   - The SQL then has engine's semantics and spellings where they differ from the product's: null order, division,
     the raised errors' texts.
   - Today `generatePlan` writes the product statement. The export switches to the upstream style, so engine running
     the plan gives engine's own answers.
2. **Write engine's plan format** from the lite plan's records, as `generatePlan` does today:
   - the node types;
   - `resultColumns` and the TDS result type;
   - the connection;
   - the template functions.
3. **Parameters** are written as engine's FreeMarker placeholders (the forms engine's own planner writes for a
   parameter), so engine binds them at execution as it always does.
4. **What does not export is refused by name.** For example:
   - a lite plan with a step engine has no node for (a planned load, a keyed region engine would plan otherwise);
   - a database engine does not support.

### 3.3 Import: an engine plan to a lite plan

1. **Read engine's plan JSON** at the edge. The first scope is relational and TDS:
   - `relationalTdsInstantiation`;
   - `sql` nodes;
   - `constant` nodes.
2. **Template placeholders become bound parameters at planning.**
   - A `${param}` the plan's parameters name becomes a `?` with its declared type, in placeholder order (the
     invariant 3 contract).
   - The `executionTraceID` comment is dropped: lite binds its own execution id (S304).
   - Any other FreeMarker construct is refused by name. Exec never evaluates a template.
3. **Refused by name, never run in part:**
   - **Java in the plan.** Engine plans for graph-fetch and model-to-model mapping carry generated Java
     (`globalImplementationSupport`, the Java platform implementation). G4: a plan is SQL and data only, never Java.
   - **A node type outside the scope.**
   - **SQL written for a database other than the one it would run on.**
4. **The result** is an ordinary lite plan. The engine's SQL is sent as written, in its dialect for that database.
   Lite's exec, sessions and writers run it like any plan.

### 3.4 Printing: the serializer's job, in Java, at the edge (the user's float point)

- **The rule.** Formatting a RESULT is the serializer's job, never the SQL's, in both directions:
  - **Export:** legend-engine formats results itself, in Java (its TDS serializer). So an exported plan carries lean
    SQL and typed result columns, and no float printing.
  - **Import:** lite runs the imported plan to typed rows. The edge's Java presenter writes them as engine would
    (TDS JSON, engine's Float and date forms). G6 allows exactly this: "Small Java formatting ... only at the edge".
- **What the long Float expression in SQL is for.** It exists for two things, and plans in and out need neither:
  1. the DATABASE judge, which compares values inside SQL on the test road;
  2. a Pure function that prints INSIDE a query (`toString` in a filter or a concatenation).
- **The one place SQL still prints.**
  - Printing inside a query must stay SQL: the database computes the filter.
  - On export it uses ENGINE's own spelling (engine does it in SQL too), not lite's canon.
  - On import it is whatever engine wrote.

## 4. Verification

- **Export.** For a set of queries (PCT's relational suites and the corpus's TDS queries):
  1. export the lite plan;
  2. execute it on the **real engine runner** (`docs/result-boundary-homework/README.md` §7, "How to ask it again":
     the engine runner's jars are the engine; `.github/workflows/engine-verdict.yml`);
  3. compare its answer with lite's answer for the same query through the edge.
- **Import.** For the same queries:
  1. take the plan engine generates;
  2. import it and run it on lite;
  3. compare with engine's own execution of that plan.
- **Each difference** is fixed, or posted to the user and ledgered as a `docs/SEMANTICS_REGISTER.md` row (the
  upstream-fidelity tenet).
- **Guards**, each with a planted violator:
  - no product class depends on the import or export code (G5);
  - an imported plan that holds Java or an unknown template construct is refused;
  - exec holds no FreeMarker or template code (a dependency rule).

## 5. Defaults this design takes

These were recommended in the message the user accepted. A change is the user's call.

1. **Engine plan format: 4.145.0**, the version every upstream reading in this program is pinned to. A newer engine's
   format is a measured change, not a silent one.
2. **Import scope first: relational and TDS** (`relationalTdsInstantiation`, `sql`, `constant`).
   - Graph-fetch and model-to-model plans are refused by name: they carry Java.
   - A later scope is the user's to open.
3. **Verification is the engine runner's**, by round trip in both directions (§4), never a reading of the engine's
   source alone.

## 6. Order

1. After D1 makes the edge and the `PlanStyle` hook:
   - **D3.1:** move `generatePlan`'s writer to the edge, planning with `UPSTREAM`.
   - **D3.2:** the export round trip on the engine runner.
2. **D3.3:** the Java import translator (relational/TDS, placeholders to bound parameters, refusals by name), with the
   import round trip.
3. **D3.4:** the browser.
   - The TypeScript compatibility module (D-1) carries import and export for a UI pointed at a real engine.
   - `UpstreamPlanner`'s one-shape reader becomes that module's import.
