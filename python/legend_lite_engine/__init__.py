"""legend-lite-engine: legend-engine's routes for legend-lite in Python, OPT-IN (K2, the user's "product root, edge
opt-in", 2026-11-18; the [engine] extra: ``pip install legend-lite[engine]``). It carries the engine library alone
(``_native/libcompiler_engine.*``: //native:engine, legend-engine's entry points over the product they depend on), which
``legend_lite.edge`` finds beside this module and loads on first use, its own isolate. The product -- ``legend_lite`` and
its library -- never loads it."""
