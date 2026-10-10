# native

legend-lite's compiler as a **native shared library**, for hosts that load native code (first
Python: `python/legend_lite`). It is the browser's compiler: the same boundary (`//wasm:boundary`,
`planner.Boundary` over `//core`) that `//wasm:planner` compiles to WebAssembly through the tab's adapter, compiled
here by GraalVM's native-image (`//native:compiler` → `libcompiler.dylib` on macOS, `libcompiler.so` on
Linux; no JVM).

- `src/main/java/com/legend/nativelib/Compiler.java`: Python's adapter, the C entry points, each a delegation to one
  `planner.Boundary` operation -- `lite_plan_json`, `lite_plan_text`, `lite_database_from_catalog`, ..., and
  legend-engine's `pure/v1` through `lite_pure_v1` (Python's grammar and a query's relation type ask it there:
  docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5) -- each returning the boundary's answer in the encoding the
  tab's adapter answers in (`planner.Folded`: `OK\n<result>`, or `ERR\n<kind>\n<message>`, the kind the engine's
  errorType or the server's status)
  as a UTF-8 C string freed with `lite_free`. A Java error the planner does not answer itself (its
  heap exhausted, say) comes back as an `ERR` answer too, never an abort of the host process.
  `lite_unfreed` counts the answers not yet freed, so a host can check that it frees every one.
  Every calling OS thread must be attached to the isolate (`graal_attach_thread`).
- `src/main/resources/META-INF/native-image/.../reachability-metadata.json`: the resources the image
  carries (the Pure prelude, the engine handlers).
- `//python:bindings_test`: the planner differential corpus through the library and the Python
  bindings, answer for answer against the JVM's (`//wasm:jvm_answers`), as `//wasm:differential_test`
  holds the WebAssembly build. The `warehouse` lane runs it (`gates/BUILD.bazel`), beside the
  warehouse's own image.

Linux and macOS for now: the bindings load a `.dylib` or a `.so`; Windows (a `.dll`, the warehouse
image's MSVC toolchain) is the next platform.
