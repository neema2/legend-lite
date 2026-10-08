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

The library is found at `LEGEND_LITE_LIBRARY`. A package built to install will carry it in
`legend_lite/_native/`, where the bindings look otherwise; nothing builds that package yet, so set
the variable (`bazel build //native:compiler` makes `bazel-bin/native/libcompiler.dylib` or `.so`).

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

Frames need duckdb and pyarrow; the compiler itself needs neither, and `import legend_lite` loads them
only when `Frames` is first used.

Tests: `//python:bindings_test` (the compiler, on Python's standard library alone) and
`//python:frames_test` (the frames, against pandas), on the repository's own Python (3.12).
