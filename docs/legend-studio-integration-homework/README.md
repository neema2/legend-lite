# legend-studio integration: the spikes

Evidence for [`../LEGEND_STUDIO_INTEGRATION_PROPOSAL_2026_11_18.md`](../LEGEND_STUDIO_INTEGRATION_PROPOSAL_2026_11_18.md).
Each script drives legend-lite's planner module (WebAssembly) from Node with the requests legend-studio's
DataCube sends, and prints what came back. They are probes, not tests: nothing here is a gate.

Run on 2026-11-18 against legend-lite `main` at `4dfbb5b76` (plus the uncommitted protocol-info routes) and
legend-studio at `c40833edd` (2026-10-22).

## Running them

```bash
bazel build //wasm:planner
```

```bash
node docs/legend-studio-integration-homework/01-engine-routes.mjs bazel-bin/wasm/planner wasm/corpus/model.pure
```

- **Node:** 25 runs the module with no flag. Node 22 needs `--experimental-wasm-exnref`. Node 20 cannot run it.
- **Scripts 08 and 09** also take the path of a legend-studio checkout whose `packages/legend-graph/lib` is
  built, because they call legend-studio's own protocol classes.

| Script | Arguments | What it shows |
|---|---|---|
| `01-engine-routes.mjs` | planner dir, a `.pure` model | Grammar both ways, `lambdaRelationType` and `generatePlan` answered through `pureV1OrError`; the plan's shape and where the SQL sits; cold and warm timings |
| `02-execute-halves.mjs` | planner dir, a `.pure` model | The in-tab execute halves accept a model only as Pure text the host serves; the five DataCube routes that answer 404 |
| `03-studio-model-exact.mjs` | planner dir | The exact model legend-studio's DataCube synthesizes for a DuckDB-backed source. Refused. `PATCH='{"postProcessorWithParameter":[]}'` supplies the first missing field and reveals the second refusal |
| `04-studio-model-connection-pointer.mjs` | planner dir | The same model with the connection as its own element and that field present: 11 of 12 DataCube-shaped queries plan |
| `05-aggregates-pivot-errors.mjs` | planner dir | Each aggregate alone (`last` fails); the cast-less pivot's plan; the error bodies for a compile error and a parse error |
| `06-text-model-held.mjs` | planner dir | The same model as Pure text: `generatePlan` works with no change; the planner's own held-model exports and their timings; the cast-less pivot refused by name |
| `07-nullable-arithmetic.mjs` | planner dir | Arithmetic on a nullable column is a compile error; with `toOne()` it plans |
| `08-studio-lambda-roundtrip.mjs` | planner dir, legend-studio root | Each query re-serialized by legend-studio's own protocol classes plans to the same SQL (8 of 8) |
| `09-studio-plan-deserializer.mjs` | planner dir, legend-studio root | legend-lite's `generatePlan` answer read by legend-studio's `V1_deserializeExecutionPlan` and DataCube's own extraction logic |

## What was not run

- **No SQL was executed.** No DuckDB was available in this environment, so no query's rows were checked, on
  either DuckDB version.
- **No legend-engine was running**, so nothing was compared with the engine's own answers.
- **No browser.** The module was loaded in Node only.
- **The lambdas in 08 start as legend-lite's parse** of Pure text and are then passed through legend-studio's
  serializer. They are not the output of DataCube's own query builder.
