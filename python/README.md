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

Tests: `//python:bindings_test`, on the repository's own Python (3.12).
