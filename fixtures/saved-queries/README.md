# Saved queries: the shared fixture

Each file is one saved query **exactly as legend-lite's `GET /api/pure/v1/query/{id}` answers it**
(upstream legend-engine's `Query` shape), created through `POST /api/pure/v1/query` on a server
started with `--query-store` -- not hand-written. Query writes these records; DataCube reads them
as a source. Both test against this directory, so the reading cannot drift from the writing.

The records compile against the demo project `demo:trading:0.0.0`, whose model is
`query/demo/models/trading.pure` plus a runtime (`runtime-duckdb.pure` or `runtime-h2.pure`). A
record carries no model: a consumer maps its `groupId:artifactId:versionId` to model text from its
own configuration (`projects[].models` in both apps' `config.json`).

How to read one:

1. `executionContext` gives the mapping and runtime:
   - `explicitExecutionContext`: its `mapping` and `runtime`.
   - `dataSpaceExecutionContext`: the data space's execution context named `executionKey` (else its
     `defaultExecutionContext`); that context's mapping and default runtime.
   - anything else is refused.
2. `content` is the lambda as Pure text, WITHOUT `->from(...)`: parse it, then wrap its last
   expression in `->from(mapping, runtime)`.
3. `defaultParameterValues[].content` is each value as Pure text: parse `|<content>`.

| file | context resolves to | parameters | runs to | a DataCube source? |
|---|---|---|---|---|
| `explicit-context.json` | `TradingMapping`, `Runtime` | -- | 6 rows | yes |
| `data-space-context.json` | `TradingDataSpace` `Production`: `TradingMapping`, `Runtime` | -- | 5 rows | yes |
| `default-parameter-values.json` | `TradingMapping`, `Runtime` | `minQty` = `1000000` | 6 rows | yes, its value bound first |
| `graph-fetch.json` | `TradingMapping`, `Runtime` | -- | 4 objects (JSON) | **no**: objects, not rows |

Timestamps (`createdAt`, `lastUpdatedAt`, `lastOpenAt`) are incidental, and written as 0 so the bytes do not
depend on the clock. Bazel makes the records (`make.mjs`, which also re-runs every record to the counts above);
regenerate with `bazel run //fixtures/saved-queries:update_generated`, and `//:generated` fails when they are
stale.
