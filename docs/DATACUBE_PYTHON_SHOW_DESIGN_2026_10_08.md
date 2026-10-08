# `datacube.show(df)`: DataCube on a Python dataframe (design, 2026-10-08)

The DataCube + Python line, step 3 (`docs/IN_FLIGHT.md`, the sixth line). Agreed with the user on 2026-10-08, decision by
decision, below. Steps 1 and 2 are on main: the compiler as a native library with Python bindings (`bc8107c4e`), and
dataframes as Legend tables in duckdb-python through the one model writer (`0a20eb895`).

## What a developer types

```python
import legend_lite as ll
cube = ll.show(df)          # opens DataCube on the frame in the browser; returns at once
df.loc[0, "qty"] = 5         # an in-place change: the cube shows it on its next query
cube.update(new_df)          # a NEW frame (rebinding `df` does not reach the cube)
```

Later, in a notebook: `DataCube(df)`, the same thing over the notebook's own channel.

## The shape

**DataCube is only the UI.** It runs in its remote-run mode (`datacube/src/runner.ts` `RemoteRun`), the way the Query
app already embeds it (`query/src/app/cube.ts`): one call per change, query in, rows out. The page loads no compiler and
no DuckDB-WASM, so it is small, starts at once, and needs no WebAssembly GC.

**Python is a small Legend engine on the developer's machine.** It answers upstream's public `pure/v1` slice that
DataCube's remote client (`engine-client/src/engine-remote.ts`) calls:

| Call | Answered by |
|---|---|
| `grammar/grammarToJson/lambda` (parse) | the native library |
| `grammar/jsonToGrammar/lambda` (print) | the native library |
| `compilation/lambdaRelationType` (a query's types) | the native library |
| `execution/execute` | the native compiler plans; duckdb-python runs the SQL over the frames |

Python plans AND runs: the browser never sends SQL, and only SQL legend-lite's own compiler wrote is ever run.

**No Legend protocol logic lives in Python.** The native library already answers the first three exactly as
legend-lite's Java server does (measured 2026-10-08 over the planner corpus: parse 69 of 69, print 67 of 67 in both
styles, types 67 of 67, refusal messages 13 of 13, byte for byte). The few shapes Python still needs -- a refusal's HTTP
answer, the execute answer's metadata -- are built by the compiler's boundary (`planner.Wasm`), not written in Python.
Python carries the web server and the data.

**It also serves DataCube's built pages,** with a page configuration naming itself as the engine, and the frame's model,
runtime and table (Frames writes them, `python/legend_lite/frames.py`).

## Rows: Arrow, and JSON

Upstream's execute answers in Arrow when asked: `?serializationFormat=ARROW_IPC` (`RelationalResultToArrowIPCSerializer`,
in the pinned legend-engine 4.145.0), with the SQL that ran in the schema's metadata (`legend.activities`, beside
`legend.columns`). It is chosen between the engine and the browser, not by the database: an engine converts JDBC rows
from any database. For Python it is native: duckdb-python produces Arrow, so JSON would be the extra conversion.

DataCube's client reads BOTH formats, and each engine connection DECLARES its format when it is set up -- never tried and
fallen back from at runtime (DataCube's rule, held by `test/guardrails.test.ts`: a silent fallback once hid three bugs).
Python's engine declares Arrow; legend-engine and legend-lite's server stay JSON until chosen otherwise (legend-lite's
server gains Arrow after the database owner's execution-plan step 4: a new binary result kind).

## Live, Snapped, and when the grid refreshes

- **Live** (the default): each query reads the frame as it is then (re-read as Arrow at each query). ONE meaning, in
  every mode.
- **Snapped**: copied into duckdb-python once (Frames' `mode="snapped"`). There is no Snap into the tab: the page has no
  DuckDB.
- **Refresh**: the grid re-queries when it is used. In IPython and notebooks, after a command or cell finishes, Python
  tells the page to re-query, so a change shows by itself. At the plain `>>>` prompt the next click shows it.
- **A half-made change**: a frame changed at the same moment the cube reads it can give one odd query (pandas is not
  thread-safe); the next query is right. Accepted (the user, 2026-10-08): hand edits rarely meet a read; a frame a
  program keeps changing belongs in Snapped mode. A notebook-only snapshot at each cell's end is the fix to reach for if
  it bites, measured first.

## Lifecycle

`show()` does not block: the server runs in a background thread of the developer's process (a thread, not an event
loop: Jupyter runs its own). In a plain script, if a cube is still open when the script ends, Python waits there and
says so ("DataCube is still open at ...: press Ctrl-C to finish"). In the REPL (`python -i`), IPython and notebooks the
process stays alive anyway.

## Safety

Loopback only. A one-time token in the link, sent with every request. A page is refused unless its `Host` is local
(DNS rebinding; the warehouse's rule). No cross-origin access. The browser never sends SQL.

## What changes outside Python

1. The compiler's boundary (`wasm/src/main/java/planner/Wasm.java`): the refusal answer and the execute metadata.
2. DataCube's remote client (`engine-client/src/engine-remote.ts`): read Arrow as well as JSON, its format declared.
3. DataCube's page: a start for "an engine at this address", chosen once as the page loads -- the Studio line's files,
   agreed with it before the first edit.

## How it is held

- **In the browser**, as `live_snap_test` is (`datacube/test/live-snap/`): the Python server serves the page in the pinned
  Chromium; the page runs the cube's cases in remote-run mode against Python and again in the tab on the same rows,
  and they must agree. Node only drives the browser.
- **Conformance**: the same requests to the Python server, legend-lite's Java server and legend-engine.
- **Measured before designing**: DataCube in remote-run mode against legend-lite's Java server gives the tab's rows for
  all 58 engine operations of `datacube/demo/engine-cases.mjs` (2026-10-08); the only feature that mode refuses is Snap
  into the tab, by design.

## Order

1. The boundary's builders; the Python server; its Python tests.
2. Arrow in DataCube's remote client.
3. DataCube's start for an engine at an address (with the Studio line).
4. The browser test.
5. `show()`: opening the browser, the handle, the wait at a script's end, the notebook refresh.

Windows follows with the package step (the library does not build there yet: the loader knows no `.dll`, and the
shared library has not been built on Windows).
