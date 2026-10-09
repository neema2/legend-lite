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
app already embeds it (`query/src/app/cube.ts`): one call per change, query in, rows out. The page loads no compiler
module (about 5 MB) and no DuckDB-WASM: its script is the grid and the remote client, about 290 KB gzipped at startup
(measured 2026-10-08, held under a budget by `datacube/test/bundle-budget.test.ts`), and it needs no WebAssembly GC.

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

**One copy of the answers (step 1, 2026-10-08).** The boundary does not write them again: it calls legend-lite's own
server code. `PureV1Api` (the pure/v1 answers) was plan-side code in a server library, its one database call being
execute's run; it is now its own plan-side library, `//core:pure_v1`, and that run is handed in by the caller
(`PureV1Api.Runner`: the server passes its driver). The path-to-endpoint table moved into it too (`PureV1Api.route`),
so legend-lite's server and Python's engine route the same way. Python's engine sends every call but execute to
`route` through one native entry (`lite_pure_v1`); for execute it takes `PureV1Api.arrowPlan` -- the SQL and the Arrow
schema metadata, for the models it serves only -- runs the SQL in duckdb-python, and writes the Arrow.

**It also serves DataCube's built pages,** with a page configuration naming itself as the engine, and the frame's model,
runtime and table (Frames writes them, `python/legend_lite/frames.py`).

## Rows: Arrow, and JSON

Upstream's execute answers in Arrow when asked: `?serializationFormat=ARROW_IPC` (`RelationalResultToArrowIPCSerializer`,
in the pinned legend-engine 4.145.0), with the SQL that ran in the schema's metadata (`legend.activities`, beside
`legend.columns`). It is chosen between the engine and the browser, not by the database: an engine converts JDBC rows
from any database. For Python it is native: duckdb-python produces Arrow, so JSON would be the extra conversion.

**Measured against legend-engine 4.145.0 (2026-10-08; the answer recorded,
`core/src/test/resources/upstream-api/e8-arrow-groupby-sort.json` and its bytes).** Status 200, labelled
`Content-Type: application/json` with `x-legend-response-format: FormatNotSet`; the body is one zstd frame around an
Arrow IPC stream; the schema metadata is `legend.builder` (the JSON answer's builder), `legend.activities` (the activities
WITHOUT their `_type`) and `legend.columns`. An empty result is the schema with no batches; a refusal is the JSON error,
as in the JSON format. Python's engine answers the same way. Two upstream defects, not copied (`docs/SEMANTICS_REGISTER.md`
S28): its Arrow columns are typed from the JDBC metadata and decimals rounded to its scale (on H2 a fractional `sum`
arrives whole: 135.2 as 135, recorded in `upstream-api/e8-arrow-fractional-sum.json`), and it needs `--add-opens=java.base/java.nio=ALL-UNNAMED` on its JVM or every Arrow answer
is a 500. So a reader types each column by `legend.builder`, never by its Arrow type. No upstream client reads this
format yet (legend-engine has no test of it; pylegend does not use it): DataCube's is the first. A browser reads zstd
only through a decoder of its own: the pinned Chromium (153) refuses `DecompressionStream('zstd')` (measured
2026-10-08), so DataCube's client carries fzstd (pure JavaScript, MIT, no dependencies; agreed with the user), and fflate,
already DataCube's, reads no zstd.

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
- **A Live frame whose columns change** is written a new model at its next query. The page still holds the old one,
  so that query is refused, saying to read the engine's model again; today that is a reload of the page (it reads
  `cube.json` again), and `show()` (step 5) makes the page re-read it by itself.
- **One query at a time**: the engine holds the frames still while a query is planned and run (a Live frame read,
  its SQL run), so queries take turns, and Frames' own calls wait for one; the answer is compressed outside.
- **A half-made change**: a frame changed at the same moment the cube reads it can give one odd query (pandas is not
  thread-safe); the next query is right. Accepted (the user, 2026-10-08): hand edits rarely meet a read; a frame a
  program keeps changing belongs in Snapped mode. A notebook-only snapshot at each cell's end is the fix to reach for if
  it bites, measured first.

## Lifecycle

`show()` does not block: the server runs in a background thread of the developer's process (a thread, not an event
loop: Jupyter runs its own). In a plain script, if a cube it opened in the browser is still open when the script
ends, Python waits there and says so ("DataCube is still open at ...: press Ctrl-C to finish"): a person is looking at
it, from a terminal, an IDE's run window (PyCharm's Stop ends it) or anywhere else. A script that opened no browser --
a test or a CI job, with `browser=False` or no browser to open -- ends, and says the cube ended with it, so nothing an
unattended run starts can hang. The engine answers on its own compiler threads, which keep serving through Python's
exit (not `concurrent.futures`' pool, which Python shuts down before an exit handler runs: found by the audit of
`show()`). In the REPL (`python -i`), IPython and notebooks the process stays alive anyway.

## Safety

Loopback only. A one-time token in the link, sent with every request. A page is refused unless its `Host` is local
(DNS rebinding; the warehouse's rule). No cross-origin access. The browser never sends SQL. The token rides in the
link's fragment, which a browser never sends to a server (nor in a Referer), and it stays in the address bar and the
browser's history while the page is open, so a reload works -- as the warehouse's launch key does; the engine's
lifetime is the token's. `show()` also prints the link (and a cube's repr is it): in a notebook it is in the cell's
output, saved with the notebook, a token dead once the kernel ends.

## What changes outside Python

1. The compiler's boundary (`wasm/src/main/java/planner/Wasm.java`): three calls into `PureV1Api` (`route`, `arrowPlan`,
   `refused`), which became a plan-side library of its own (`//core:pure_v1`; above, "One copy of the answers").
2. DataCube's remote client (`engine-client/src/engine-remote.ts`): read Arrow as well as JSON, its format declared.
3. DataCube's page of one cube on an engine (`datacube/demo/engine.html`, `engine.ts`; `make-dist.mjs` and the
   `_BUNDLES` entry): a page of its own in remote-run mode, as Query's results run, so the demo's `boot.ts` and
   `planners.ts` are untouched -- the Studio line's files, agreed with it before the first edit (2026-10-08).

## How it is held

- **In the browser**, as `live_snap_test` is (`datacube/test/live-snap/`): the Python server serves the page in the pinned
  Chromium; the page runs the cube's cases in remote-run mode against Python and again in the tab on the same rows,
  and they must agree. Node only drives the browser.
- **Conformance**: the same requests to the Python server, legend-lite's Java server and legend-engine.
- **Measured before designing**: DataCube in remote-run mode against legend-lite's Java server gives the tab's rows for
  all 58 engine operations of `datacube/demo/engine-cases.mjs` (2026-10-08); the only feature that mode refuses is Snap
  into the tab, by design.

## Order

Built so far (2026-10-08, branch `datacube-show`): 1, the engine (it also serves a site, so the page and the API are one
origin); 2, DataCube's remote client reading Arrow, declared per engine (`serializationFormat: 'ARROW_IPC'`, with the
engine's token as `authorization`), held by `//datacube:python_engine_test`: in the pinned Chromium, every case of the
cube corpus through DataCube's remote-run path on Python's engine and again in the tab on the same rows, the same SQL,
columns, types and rows; and legend-engine's own recorded Arrow answer read by the same reader. 3, DataCube's page of
one cube on an engine (`datacube/demo/engine.html`, agreed with the Studio line: a page of its own in remote-run mode,
as Query's results run, so `boot.ts` is untouched and the page loads neither DuckDB-WASM nor the compiler): the link
names the frame (`?table=`) and carries the token in its fragment; the page asks the engine `cube.json` (with the
token) for the frame's model, runtime and source, and opens the cube over `RemoteRun`; held by the same browser test
(the real page, from the engine's site, shows the frame's rows). 5, `show()` (`python/legend_lite/datacube.py`): one
engine per process, started by the first `show`; a frame Live by default, named `frame`, `frame_2`, ... unless given;
the browser opened on the cube's link; `cube.update(frame)`, `cube.refresh()`, `cube.close()` (the last one stops the
engine). The page FOLLOWS its frame: about once a second (while the tab is shown) it asks the engine the frame's
version (`version.json`), which moves after a notebook cell for Live cubes (IPython's `post_run_cell`), an update, a
refresh, a Live frame's new columns (noticed at its next query) or a close; then it reads `cube.json` again -- the same
model re-runs the view as it stands, a new one opens the cube again, a closed frame stops it following. A brief call,
nothing held open: a held request per tab (a long poll, the first build) would use up the browser's six connections to
one origin with a few cube tabs (the audit). A plain script that opened a cube in the browser waits at its end until
Ctrl-C. Held by `//python:engine_test` (show's cases, a real script that opened a browser waiting while the engine
still answers, and ones that opened none ending at once) and the browser test (an update shows on the open page by
itself; a new column opens it again). To try
it: `bazel run //python:repl` (the repository's Python and pinned packages, the library and the site), then
`cube = ll.show(trades)`. 6, the package (`//python:wheel`, `legend-lite` 0.1.0, one wheel per platform): the modules,
the compiler's library (`_native/`) and DataCube's engine page alone (`_site/`: no DuckDB-WASM, no compiler module),
16.5 MB; settled with the user (2026-10-08): macOS 14 and up (the library's minimum pinned in its build, where it had
followed the build machine's SDK), Python 3.12 and up (what it is tested on), Linux labelled by the glibc the library is
measured to need. Held by `//python:wheel_test`: the tag against the library's own header, and the wheel installed
into a fresh environment offline, `show()` and a query run from it alone. Left for later: the notebook widget,
publishing to PyPI (the user's decision), Windows.

1. The boundary's builders; the Python server; its Python tests.
2. Arrow in DataCube's remote client.
3. DataCube's page of one cube on an engine (with the Studio line).
4. The browser test.
5. `show()`: opening the browser, the handle, the wait at a script's end, the notebook refresh.

Windows follows with the package step (the library does not build there yet: the loader knows no `.dll`, and the
shared library has not been built on Windows).
