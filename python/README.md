# legend_lite

The Legend compiler in Python: legend-lite's compiler as a native library (`//native:compiler`),
with bindings on Python's standard library alone (ctypes, json, decimal).

```python
import legend_lite as ll
tree = ll.parse("|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")
ll.relation_type(model_text, tree)          # the compiler's columns and types
ll.plan(model_text, tree, "trades::RT").sql  # the compiler's SQL
ll.print_tree(tree)                          # back to Pure text
```

It works on **protocol trees** (upstream's V1 lambda JSON as Python dicts), the way DataCube builds
its queries; Pure text is one way to make one. Numbers stay exact: decimals as `Decimal`, integers
as `int`. A refusal is a `LegendError` (`.kind`, `.message`).

The installed package carries the library in `legend_lite/_native/`, where the bindings look; `LEGEND_LITE_LIBRARY`
points them elsewhere (a build of this repository: `bazel build //native:compiler` makes
`bazel-bin/native/libcompiler.dylib` or `.so`).

One isolate per process. Each thread attaches itself on first use and stays attached (a few
kilobytes once it has exited), so call from a thread pool rather than a thread per request. Not
across `os.fork`: start child processes with multiprocessing's `spawn` method.

## Dataframes as Legend tables

```python
frames = ll.Frames()                                   # one duckdb-python database
trades = frames.register("trades", df)                 # a pandas or polars frame, an Arrow table, ...
trades.execute("->filter(x|$x.desk == 'FX')->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")  # pyarrow.Table
trades.model, trades.runtime, trades.accessor          # the model the compiler plans it against
```

A frame is anything Arrow reads, or a function returning one. It is handed to duckdb-python as Arrow,
and becomes an ordinary DuckDB table or view named as registered. The compiler reads its catalog and
writes its model with the same writer DataCube uses for a table (`table_model`), so a frame and a
DataCube table are planned alike.

- **Live** (the default): every query reads the frame as it is then, including changes made in place;
  if its columns changed, its model is written again. Re-reading is a conversion to Arrow at each
  query: a few milliseconds per million rows of a pandas or polars frame (pandas text kept as Python
  objects, `dtype=object`, costs about ten times more), nothing for an Arrow table.
- **Snapped** (`mode="snapped"`): copied into DuckDB once. Faster to query, and it never changes.

A frame's name is a plain identifier, one name whatever its case (as in DuckDB). Registering it again
replaces its table, and the old handle stops working; `unregister(name)` and `close()` take tables
out. Frames never replaces a table it did not make, and it sets the connection's session as the
planner's SQL expects (UTC), on a connection it opens or one it is given. A pandas frame's index is
not a column (`reset_index()` keeps it as one).

## DataCube on a dataframe

```python
import legend_lite as ll
cube = ll.show(df)          # DataCube opens in the browser; show() returns at once
df.loc[0, 'qty'] = 5         # Live: the cube's next query sees it (in a notebook, it re-queries after the cell)
cube.update(new_df)          # a new frame: the open page shows it by itself
cube.close()
```

One engine per process, started by the first `show` and stopped when the last cube closes. In IPython and notebooks
each Live cube is told to query again after every cell; at a plain `>>>` prompt the next click shows a change in the
rows (or `cube.refresh()`), and a frame given new columns is noticed at its next query and the page opens the cube
again over them. A plain script that opened a cube in the browser waits at its end, saying so, until Ctrl-C (or an
IDE's Stop); one that opened none (`browser=False`, or no browser to open: a test, a CI job) ends, and says so. `show(df, browser=False)` opens nothing: the link is `cube.url`, which
`show` also prints (it carries the engine's token).

**Install it** (into PyCharm's environment, a virtualenv, anywhere; Python 3.12 and up, macOS 14 and up or Linux):
`bazel build //python:wheel`, then `pip install 'bazel-bin/python/legend_lite-0.1.0-<platform>.whl[pandas]'` (pip
fetches duckdb and pyarrow; `[polars]` for polars). The wheel carries the compiler's library and DataCube's page; `//python:wheel_test` installs it into a
fresh environment, offline, and runs `show()` and a query from it alone.

To try it with nothing installed: `bazel run //python:repl` -- the repository's Python and pinned packages, the
compiler's library and DataCube's site, with `ll`, `pd` and a sample `trades` DataFrame ready.

## An engine for DataCube

```python
engine = ll.Engine(frames)     # legend-engine's pure/v1 API at engine.url, from a background thread
engine.authorization           # "Bearer <token>": every request carries it
engine.close()
```

The engine answers the calls DataCube's remote client makes, as legend-engine answers them: parse, print and a
query's types by legend-lite's own server code (`PureV1Api`, through the native library), and execute in
upstream's Arrow format (`?serializationFormat=ARROW_IPC`: one zstd frame around an Arrow IPC stream, its schema
carrying the builder, the SQL that ran and the columns) with the rows DuckDB computes over the frames, Live ones
read as they are then. Only queries over the models the frames' tables were written with are run, and only SQL
the compiler wrote. It listens on 127.0.0.1 alone and answers only requests that carry its token and name this
machine as their Host; it sends no cross-origin header. Design: `docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`.

`Engine(frames, site=...)` also serves a site's files (DataCube's built pages, `//datacube:dist`) at its origin, and
`cube.json?table=<name>` (with the token): a frame's model, runtime and source as they are now, which DataCube's
`engine.html` opens a cube over (`<url>/engine.html?table=<name>#token=<token>`).

Frames and the engine need duckdb and pyarrow; the compiler itself needs neither, and `import legend_lite`
loads them only when `Frames` or `Engine` is first used.

Tests: `//python:bindings_test` (the compiler, on Python's standard library alone), `//python:frames_test`
(the frames, against pandas) and `//python:engine_test` (the engine over HTTP, and `show()`), on the repository's own
Python (3.12); `//datacube:python_engine_test`, DataCube itself against the engine in the pinned Chromium.
