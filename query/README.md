# Legend Query (light)

Legend Query rebuilt on legend-lite: find data (data spaces, classes, services), build a query in
the form or as Pure text, run it, save it. One browser app, plain TypeScript and DOM, speaking only
legend-engine's upstream APIs. Design and parity plan: `docs/QUERY_APP_DESIGN_2026_09_30.md`;
the upstream feature census: `query/docs/UPSTREAM_QUERY_CENSUS.md`.

## Run it

```sh
bazel run //query:serve                 # http://127.0.0.1:8100/demo/index.html
bazel run //query:serve -- --port 9000
```

Where queries run is the page's config (`?config=<file>`, default `config.json`):

| config | where queries run | saved queries |
|---|---|---|
| `config.json` | **in the browser**: legend-lite's planner (WebAssembly, in a worker) writes the SQL, DuckDB-WASM in the tab runs it, seeded from `demo/models/trading-seed.sql`. No server. | this browser (IndexedDB) |
| `config-warehouse.json` | the warehouse's DuckDB, as the user you sign in as (DataCube's Live plane) | this browser |
| `config-server.json` | **legend-lite's server** (`bazel run //core:server -- 8090 --query-store DIR`): it plans and executes | the server's store |

It looks and lays out as upstream Legend Query does (legend-studio's tokens, type, icons and
panels; dark by default, its legacy-light behind the sun/moon switch): `/` opens the query builder
-- pick a data space, a context, an entity -- and `/setup` holds every other way to start (a
mapping's classes, a service, a saved query).

The demo model is `demo/models/trading.pure` (classes, enumerations, a mapping, a service, a data
space) with a runtime per plane (`runtime-duckdb.pure`, `runtime-h2.pure`).

## Results

A table query's rows show in the plain grid (sort, copy, filter by a cell's value into the query), or
-- with **Grid | DataCube** in the results bar -- in DataCube over the same query: grouping, pivots,
formats and charts are DataCube's, planned on the same worker. It opens with its controls hidden;
right-click the grid, **Show Controls**. An Objects (graph fetch) query shows JSON.

## Check it

```sh
bazel test //query:tests //query:typecheck_test   # builder round-trips, loader, types
bazel run //query:verify                          # end to end, in Chromium: every step in the browser AND on the server
```

`bazel test //query:verify_test` runs the same end to end on the Chromium Bazel fetches (CI's `ui` lane, Linux);
`bazel run //query:verify` by hand needs Playwright's Chromium once (`bazel run //datacube:install_browser`). Either
starts legend-lite's server and a warehouse itself (with Bazel's JDK), on free ports.

## Layout

```
src/backend/   the engine calls (HTTP, or the browser plane: planner worker + DataCube's engines),
               the saved-query stores, DataCube's planner seam (cube-planner.ts)
src/model/     the model graph the screens read (display only)
src/builder/   the query as state; build.ts (state -> protocol JSON), load.ts (JSON -> state)
src/app/       routes, the session, run, save, the results cube (cube.ts)
src/ui/        one file per panel; app.css
demo/          the page, configs, models, serve.mjs, verify.mjs
```
